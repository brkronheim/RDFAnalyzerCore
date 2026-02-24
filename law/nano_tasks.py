"""
Law tasks that replicate the behaviour of generateSubmissionFilesNANO.py.

Fixed assumptions:
  - stage-out  is always active (outputs xrdcp'd to EOS after each job)
  - eos-sched  is always active (submission dir lives on EOS)
  - stage-in   is a freely-settable BoolParameter (default False)

All parallelism is delegated entirely to law's branch dispatch mechanism.
No manual Python threading is used anywhere in this file.

Workflow
--------
  PrepareNANOSample  (LocalWorkflow, one branch per sample line in sampleConfig)
      → per branch: opens a Rucio client, queries each DAS path sequentially,
        merges and size-splits the file list, then writes
        condorSub_{name}/samples/{sample_name}/job_{i}/
        with submit_config.txt / floats.txt / ints.txt
      → output: branch_outputs/sample_{N}.json  (list of created job dirs)

  BuildNANOSubmission  (Task)
      → requires all PrepareNANOSample branches
      → copies exe / aux / x509 into shared_inputs/
      → creates sequential symlinks  job_0 … job_{N-1}
      → writes condor_runscript.sh + condor_submit.sub

  SubmitNANOJobs  (Task)
      → requires BuildNANOSubmission
      → runs condor_submit, saves cluster ID + job count to submitted.txt

  MonitorNANOJobs  (Task)
      → requires SubmitNANOJobs
      → blocking poll loop: checks condor_q / condor_history every
        --poll-interval seconds
      → verifies each expected EOS output file via xrdfs stat
      → resubmits held / failed jobs individually (up to --max-retries times)
        using per-job condor submit files placed in resubmissions/
      → persists state in monitor_state.json (restart-safe)
      → writes all_outputs_verified.txt when every output is confirmed on EOS

Usage
-----
  source env.sh

  law run PrepareNANOSample \\
      --submit-config analyses/vjetStitch/cfg/submit_config.txt \\
      --name stitch22 --x509 /path/to/x509 --exe build/bin/vjetStitch

  law run BuildNANOSubmission  [same params]
  law run SubmitNANOJobs       [same params]
  law run MonitorNANOJobs      [same params]
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore
import requests
from requests.exceptions import ChunkedEncodingError, RequestException
import urllib3

law.contrib.load("htcondor")

# ---------------------------------------------------------------------------
# Make sure core/python is importable regardless of how the script is invoked
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

from submission_backend import (  # noqa: E402
    read_config,
    get_copy_file_list,
    write_submit_files,
)
from validate_config import validate_submit_config  # noqa: E402


# ---------------------------------------------------------------------------
# Hard-coded assumptions
# ---------------------------------------------------------------------------
STAGE_OUT = True   # always stage outputs to EOS
EOS_SCHED = True   # submission dir lives on EOS
WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))
EOS_BASE = WORKSPACE  # root of the submission dir tree

# ===========================================================================
# Utility functions
# ===========================================================================

# Rucio needs the default configuration → taken from CMS cvmfs defaults
if "RUCIO_HOME" not in os.environ:
    os.environ["RUCIO_HOME"] = "/cvmfs/cms.cern.ch/rucio/current"


def _get_proxy_path() -> str:
    try:
        subprocess.run("voms-proxy-info -exists -valid 0:20", shell=True, check=True)
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "VOMS proxy expired or missing: "
            "run `voms-proxy-init -voms cms -rfc --valid 168:0`"
        )
    return subprocess.check_output(
        "voms-proxy-info -path", shell=True, text=True
    ).strip()


def _get_rucio_client(proxy=None):
    from rucio.client import Client  # import lazily – may not be available at import time

    try:
        if not proxy:
            proxy = _get_proxy_path()
        return Client()
    except Exception as e:
        raise RuntimeError(f"Cannot create Rucio client: {e}") from e


def _query_rucio(directory, file_split_gb, WL, BL, site_override, client):
    """Return a dict {group_idx: 'url1,url2,...'}."""
    if not directory or not isinstance(directory, str) or not directory.startswith("/"):
        print(f"Warning: invalid DAS path: {directory!r} – skipping")
        return {}

    max_retries = 5
    backoff = 0.5
    rucio_output = None
    for attempt in range(1, max_retries + 1):
        try:
            rucio_output = list(client.list_replicas([{"scope": "cms", "name": directory}]))
            break
        except (ChunkedEncodingError, RequestException, urllib3.exceptions.ProtocolError) as e:
            if attempt < max_retries:
                print(f"Warning: Rucio attempt {attempt}/{max_retries} failed: {e}. Retrying in {backoff:.1f}s…")
                time.sleep(backoff)
                backoff *= 2
            else:
                print(f"Error: Rucio failed for {directory!r} after {max_retries} attempts: {e}")
                return {}
        except Exception as e:
            print(f"Error: unexpected Rucio error for {directory!r}: {e}")
            return {}

    if not rucio_output:
        print(f"Warning: no replicas found for {directory!r}")
        return {}

    groups: dict[int, str] = {}
    group_counts: dict[int, int] = {}
    group_sizes: dict[int, float] = {}
    running_size = 0.0
    group = 0

    for filedata in rucio_output:
        fname = filedata["name"]
        states = filedata.get("states", {})
        size_gb = filedata.get("bytes", 0) * 1e-9
        redirector = "root://xrootd-cms.infn.it/"
        running_size += size_gb
        if running_size > file_split_gb:
            group += 1
            running_size = size_gb

        for site, state in states.items():
            if state == "AVAILABLE" and "Tape" not in site:
                clean_site = site.replace("_Disk", "")
                if clean_site in WL:
                    redirector = "root://xrootd-cms.infn.it//store/test/xrootd/" + clean_site + "/"
                    break
                elif clean_site in BL or "T3" in clean_site or "Tape" in clean_site:
                    continue
                else:
                    redirector = "root://xrootd-cms.infn.it//store/test/xrootd/" + clean_site + "/"

        url = redirector + fname
        if group in groups:
            groups[group] += "," + url
            group_counts[group] += 1
            group_sizes[group] += round(size_gb, 1)
        else:
            groups[group] = url
            group_counts[group] = 1
            group_sizes[group] = round(size_gb, 1)

    return groups


def _get_sample_list(config_file):
    """Parse the sample config and return (sampleDict, baseDirectoryList, lumi, WL, BL)."""
    config_dict: dict[str, dict] = {}
    base_dirs = []
    lumi = 1.0
    WL: list[str] = []
    BL: list[str] = []
    with open(config_file) as fh:
        for line in fh:
            line = line.split("#")[0].strip().split()
            inner: dict[str, str] = {}
            for pair in line:
                parts = pair.split("=")
                if len(parts) == 2:
                    inner[parts[0]] = parts[1]
            if "name" in inner:
                config_dict[inner["name"]] = inner
            elif "prefix_cern" in inner:
                base_dirs.append(inner["prefix_cern"])
            elif "lumi" in inner:
                lumi = float(inner["lumi"])
            elif "WL" in inner:
                WL = inner["WL"].split(",")
            elif "BL" in inner:
                BL = inner["BL"].split(",")
    return config_dict, base_dirs, lumi, WL, BL


def _normalize_config_paths(config_dict):
    """Strip directory components from .txt path values (so job dirs are self-contained)."""
    normalized = dict(config_dict)
    for key, value in config_dict.items():
        if isinstance(value, str) and ".txt" in value and os.path.sep in value:
            normalized[key] = os.path.basename(value)
    return normalized


def _append_unique_lines(file_path, lines):
    existing = []
    if os.path.exists(file_path):
        with open(file_path) as fh:
            existing = [ln.rstrip("\n") for ln in fh]
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    seen: set[str] = set()
    deduped: list[str] = []
    for ln in existing:
        if ln in seen:
            continue
        seen.add(ln)
        deduped.append(ln)
    for line in lines:
        if not line or line in seen:
            continue
        seen.add(line)
        deduped.append(line)
    with open(file_path, "w") as fh:
        for ln in deduped:
            fh.write(ln + "\n")


def _copy_file(src: str, dst: str) -> None:
    """Copy src to dst, skipping if sizes and mtimes match."""
    if os.path.exists(dst):
        if (os.path.getsize(dst) == os.path.getsize(src)
                and int(os.path.getmtime(dst)) == int(os.path.getmtime(src))):
            return
        os.remove(dst)
    shutil.copy2(src, dst)


def _copy_dir(src: str, dst: str) -> None:
    """Copy whole directory tree src → dst (skip if dst already exists)."""
    if not os.path.exists(dst):
        shutil.copytree(src, dst, dirs_exist_ok=True)


def _eos_file_exists(eos_path: str, timeout: int = 30) -> bool:
    """Return True if *eos_path* can be stat'd on eosuser.cern.ch via xrdfs."""
    try:
        r = subprocess.run(
            ["xrdfs", "root://eosuser.cern.ch/", "stat", eos_path],
            capture_output=True,
            timeout=timeout,
        )
        return r.returncode == 0
    except Exception:
        return False


def _condor_q_cluster(cluster_id: str) -> dict[int, int]:
    """
    Query condor_q for all jobs still in the queue for *cluster_id*.
    Returns {proc_id: JobStatus}  (1=Idle, 2=Running, 5=Held).
    Returns {} if the cluster is gone or condor_q fails.
    """
    try:
        r = subprocess.run(
            ["condor_q", str(cluster_id), "-json"],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return {}
        ads = json.loads(r.stdout)
        return {ad["ProcId"]: ad["JobStatus"] for ad in ads if "ProcId" in ad}
    except Exception as e:
        print(f"Warning: condor_q failed for cluster {cluster_id}: {e}")
        return {}


def _condor_history_exit(cluster_id: str) -> dict[int, int]:
    """
    Query condor_history for jobs that have left the queue.
    Returns {proc_id: ExitCode}  (0 = success, non-zero = failure).
    """
    try:
        r = subprocess.run(
            ["condor_history", str(cluster_id), "-json"],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return {}
        ads = json.loads(r.stdout)
        return {
            ad["ProcId"]: ad.get("ExitCode", 1)
            for ad in ads
            if "ProcId" in ad
        }
    except Exception as e:
        print(f"Warning: condor_history failed for cluster {cluster_id}: {e}")
        return {}


def _submit_single_job(
    job_index: int,
    main_dir: str,
    exe_relpath: str,
    x509loc: str | None,
    stage_inputs: bool,
    max_runtime: int,
    request_memory: int,
    shared_dir_name: str,
    config_dict: dict,
    include_aux: bool,
) -> str | None:
    """
    Create a per-job condor submit file for *job_index* in *main_dir*/resubmissions/
    and submit it.  Returns the new cluster ID string, or None on failure.
    """
    job_dir   = os.path.join(main_dir, f"job_{job_index}")
    resub_dir = os.path.join(main_dir, "resubmissions")
    Path(resub_dir).mkdir(parents=True, exist_ok=True)

    copy_list   = get_copy_file_list(config_dict)
    copy_bnames = sorted({os.path.basename(p) for p in copy_list})
    skip_xfer   = {"floats.txt", "ints.txt", "submit_config.txt"}

    transfer_files = [
        os.path.join(job_dir, "submit_config.txt"),
        os.path.join(job_dir, "floats.txt"),
        os.path.join(job_dir, "ints.txt"),
        os.path.join(main_dir, shared_dir_name, exe_relpath),
    ]
    if include_aux:
        transfer_files.append(os.path.join(main_dir, shared_dir_name, "aux"))
    if x509loc:
        transfer_files.append(
            os.path.join(main_dir, shared_dir_name, os.path.basename(x509loc))
        )
    for name in copy_bnames:
        if name not in skip_xfer:
            transfer_files.append(os.path.join(job_dir, name))

    filtered     = [p for p in transfer_files if "$(" in p or os.path.exists(p)]
    transfer_str = ",".join(filtered)
    log_base     = os.path.join(main_dir, "condor_logs")
    Path(log_base).mkdir(parents=True, exist_ok=True)

    sub_content = f"""universe = vanilla
Executable     = {main_dir}/condor_runscript.sh
Should_Transfer_Files = YES
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
Notification = never
transfer_input_files = {transfer_str}
MY.WantOS = "el9"
+RequestMemory={request_memory}
+RequestCpus=1
+RequestDisk=20000
+MaxRuntime={max_runtime}
max_transfer_input_mb = 10000
WhenToTransferOutput = On_Exit
transfer_output_files = submit_config.txt

Output = {log_base}/resub_{job_index}_$(Cluster)_$(Process).stdout
Error  = {log_base}/resub_{job_index}_$(Cluster)_$(Process).stderr
Log    = {log_base}/resub_{job_index}.log
queue 1
"""
    sub_path = os.path.join(resub_dir, f"resub_job{job_index}.sub")
    with open(sub_path, "w") as fh:
        fh.write(sub_content)

    try:
        r = subprocess.run(
            ["condor_submit", sub_path],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            print(f"Error resubmitting job {job_index}: {r.stderr.strip()}")
            return None
        for line in r.stdout.splitlines():
            if "cluster" in line.lower():
                parts = line.split()
                for k, part in enumerate(parts):
                    if part.lower().rstrip(".") == "cluster" and k + 1 < len(parts):
                        return parts[k + 1].rstrip(".")
    except Exception as e:
        print(f"Error resubmitting job {job_index}: {e}")
    return None


# ===========================================================================
# Shared parameter mixin
# ===========================================================================

class NANOMixin:
    """Parameters shared by all NANO submission tasks."""

    submit_config = luigi.Parameter(
        description="Path to the submit config file (key=value format)",
    )
    name = luigi.Parameter(
        description="Submission name; output dir will be condorSub_{name}",
    )
    size = luigi.IntParameter(
        default=30,
        description="GB of data per condor job (default: 30)",
    )
    x509 = luigi.Parameter(
        description="Path to the VOMS x509 proxy file",
    )
    exe = luigi.Parameter(
        description="Path to the compiled C++ executable to run on workers",
    )
    stage_in = luigi.BoolParameter(
        default=False,
        description="If True, xrdcp input files to the worker node before running",
    )
    root_setup = luigi.Parameter(
        default="",
        description="Optional shell command to set up ROOT on the worker (e.g. 'source thisroot.sh')",
    )
    max_runtime = luigi.IntParameter(
        default=3600,
        description="Maximum condor job runtime in seconds (default: 3600)",
    )
    no_validate = luigi.BoolParameter(
        default=False,
        description="Skip submit-config validation",
    )

    # ---- derived helpers ---------------------------------------------------

    def _resolve(self, path_value):
        """Resolve a path relative to the submit-config's parent directory."""
        if not path_value:
            return path_value
        if os.path.isabs(path_value):
            return path_value
        config_base = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(self.submit_config)), "..")
        )
        if os.path.exists(path_value):
            return os.path.abspath(path_value)
        return os.path.abspath(os.path.join(config_base, path_value))

    @property
    def _main_dir(self):
        """Absolute path to the condor submission directory."""
        base = os.path.join(EOS_BASE, f"condorSub_{self.name}")
        return base

    @property
    def _config_dict(self):
        return read_config(self.submit_config)

    @property
    def _sample_config(self):
        return self._resolve(self._config_dict["sampleConfig"])

    @property
    def _exe_path(self):
        return self._resolve(self.exe)

    @property
    def _exe_relpath(self):
        return os.path.basename(self._exe_path)

    @property
    def _x509_src(self):
        return self._resolve(self.x509)

    @property
    def _x509_loc(self):
        return "x509"

    @property
    def _shared_dir(self):
        return os.path.join(self._main_dir, "shared_inputs")


# ===========================================================================
# Task 1 – PrepareNANOSample
# ===========================================================================

class PrepareNANOSample(NANOMixin, law.LocalWorkflow):
    """
    LocalWorkflow: one branch per sample line in the sampleConfig file.

    Law dispatches branches in parallel; no manual threading is used here.
    Each branch opens its own Rucio client, queries each DAS path for that
    sample sequentially (multiple DAS entries are merged into one URL list),
    splits the list by size into groups, then creates per-job subdirectories
    under condorSub_{name}/samples/{sample_name}/job_{i}/ and writes a JSON
    manifest as its law output.
    """

    task_namespace = ""

    # ------------------------------------------------------------------ law

    def create_branch_map(self):
        sample_list, _, _, _, _ = _get_sample_list(self._sample_config)
        return {i: key for i, key in enumerate(sorted(sample_list.keys()))}

    def output(self):
        return law.LocalFileTarget(
            os.path.join(
                self._main_dir, "branch_outputs", f"sample_{self.branch}.json"
            )
        )

    # ------------------------------------------------------------------ run

    def run(self):
        if not self.no_validate and self.branch == 0:
            errors, warnings = validate_submit_config(self.submit_config, mode="nano")
            if warnings:
                for w in warnings:
                    self.publish_message(f"Config warning: {w}")
            if errors:
                raise RuntimeError(
                    "Submit config validation failed:\n" + "\n".join(errors)
                )

        sample_key = self.branch_data
        config_dict = self._config_dict
        sample_list, _, lumi, WL, BL = _get_sample_list(self._sample_config)
        sample = sample_list[sample_key]

        name = sample["name"]
        das = sample["das"]
        xsec = float(sample["xsec"])
        typ = sample["type"]
        norm = float(sample.get("norm", 1))
        kfac = float(sample.get("kfac", 1))
        extra_scale = float(sample.get("extraScale", 1))
        site_override = sample.get("site", "")

        # Query Rucio --------------------------------------------------------
        try:
            client = _get_rucio_client()
        except Exception as e:
            raise RuntimeError(f"Cannot open Rucio client: {e}") from e

        das_entries = [d.strip() for d in das.split(",") if d.strip()]
        if not das_entries:
            self.publish_message(f"No DAS entries for sample {name}; creating empty output.")
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            self.output().dump([], formatter="json")
            return

        # Query Rucio sequentially for each DAS entry and merge URLs into a
        # single deduplicated list.  Law's branch dispatch provides all the
        # inter-sample parallelism; no threading is used here.
        all_urls: list[str] = []
        seen_urls: set[str] = set()
        last_groups: dict[int, str] = {}   # retains size-split groups for single-DAS case

        for das_entry in das_entries:
            partial = _query_rucio(das_entry, self.size, WL, BL, site_override, client)
            last_groups = partial            # for the single-entry fast path below
            for gkey in sorted(partial.keys()):
                for url in partial[gkey].split(","):
                    url = url.strip()
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_urls.append(url)

        if len(das_entries) == 1:
            # _query_rucio already produced correct size-capped groups; reuse them.
            groups = last_groups if last_groups else ({0: ",".join(all_urls)} if all_urls else {})
        else:
            # Multiple DAS entries: merged into one group (no size re-split needed
            # since cross-DAS entries are typically complementary datasets).
            groups = {0: ",".join(all_urls)} if all_urls else {}

        if not groups:
            self.publish_message(f"No files found for sample {name}; skipping.")
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            self.output().dump([], formatter="json")
            return

        # Copy-list files that should travel with every job ---------------
        copy_list = get_copy_file_list(config_dict)

        save_dir = self._resolve(config_dict["saveDirectory"])
        Path(save_dir).mkdir(parents=True, exist_ok=True)

        sample_base = os.path.join(self._main_dir, "samples", name)

        norm_scale = str(extra_scale * kfac * lumi * xsec / norm)
        created_dirs: list[str] = []

        for sub_idx, group_key in enumerate(sorted(groups.keys())):
            job_dir = os.path.join(sample_base, f"job_{sub_idx}")
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            # Copy auxiliary config files (floats.txt, ints.txt, etc.) ----
            for fpath in copy_list:
                src = self._resolve(fpath)
                dst = os.path.join(job_dir, os.path.basename(src))
                Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dst)

            # Build job-specific submit config ----------------------------
            job_config = _normalize_config_paths(dict(config_dict))
            job_config["saveFile"] = os.path.join(
                save_dir, f"{name}_{sub_idx}.root"
            )
            job_config["metaFile"] = os.path.join(
                save_dir, f"{name}_{sub_idx}_meta.root"
            )
            job_config["fileList"] = groups[group_key]
            job_config["batch"] = "True"
            job_config["type"] = typ
            job_config["sampleConfig"] = os.path.basename(self._sample_config)
            job_config["floatConfig"] = "floats.txt"
            job_config["intConfig"] = "ints.txt"

            if self.stage_in:
                original_list = job_config["fileList"]
                local_inputs = [
                    f"input_{i}.root"
                    for i, item in enumerate(original_list.split(","))
                    if item.strip()
                ]
                job_config["__orig_fileList"] = original_list
                job_config["fileList"] = ",".join(local_inputs)

            # Always stage-out: store original EOS paths, use local basenames
            base, ext = os.path.splitext(job_config["saveFile"])
            job_config["__orig_saveFile"] = job_config["saveFile"]
            job_config["saveFile"] = os.path.basename(job_config["saveFile"])
            basem, extm = os.path.splitext(job_config["metaFile"])
            job_config["__orig_metaFile"] = job_config["metaFile"]
            job_config["metaFile"] = os.path.basename(job_config["metaFile"])

            # Write floats / ints -----------------------------------------
            float_file = os.path.join(job_dir, "floats.txt")
            _append_unique_lines(float_file, [
                "normScale=" + norm_scale,
                "sampleNorm=" + str(norm),
            ])
            int_file = os.path.join(job_dir, "ints.txt")
            _append_unique_lines(int_file, ["type=" + typ])

            with open(os.path.join(job_dir, "submit_config.txt"), "w") as fh:
                for k, v in job_config.items():
                    fh.write(f"{k}={v}\n")

            created_dirs.append(job_dir)

        # Write the branch manifest -----------------------------------------
        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        self.output().dump(created_dirs, formatter="json")
        self.publish_message(f"Sample {name}: {len(created_dirs)} job dir(s) created.")


# ===========================================================================
# Task 2 – BuildNANOSubmission
# ===========================================================================

class BuildNANOSubmission(NANOMixin, law.Task):
    """
    Collects all per-sample job directories created by PrepareNANOSample,
    creates sequential symlinks job_0 … job_{N-1} in the main dir,
    copies the shared executable / aux / x509 into shared_inputs/,
    and writes condor_runscript.sh + condor_submit.sub.
    """

    task_namespace = ""

    def requires(self):
        return PrepareNANOSample.req(self)

    def output(self):
        return {
            "submit": law.LocalFileTarget(
                os.path.join(self._main_dir, "condor_submit.sub")
            ),
            "runscript": law.LocalFileTarget(
                os.path.join(self._main_dir, "condor_runscript.sh")
            ),
        }

    def run(self):
        # ---- collect all job dirs from branch outputs --------------------
        job_dirs: list[str] = []
        branch_coll = self.input()  # SiblingFileCollection or similar
        # law returns the workflow's outputs as a dict keyed by branch index
        for branch_idx in sorted(branch_coll.keys()):
            manifest = branch_coll[branch_idx].load(formatter="json")
            job_dirs.extend(manifest)

        if not job_dirs:
            raise RuntimeError("No job directories were created by PrepareNANOSample.")

        # Sort for determinism: alphabetically by absolute path
        job_dirs.sort()
        n_jobs = len(job_dirs)
        self.publish_message(f"Total jobs to submit: {n_jobs}")

        # ---- shared inputs dir ------------------------------------------
        shared_dir = self._shared_dir
        exe_path = self._exe_path
        exe_relpath = self._exe_relpath
        x509_src = self._x509_src

        Path(shared_dir).mkdir(parents=True, exist_ok=True)
        _copy_file(exe_path, os.path.join(shared_dir, exe_relpath))

        aux_src = self._resolve("aux")
        aux_exists = bool(aux_src and os.path.exists(aux_src))
        if aux_exists:
            _copy_dir(aux_src, os.path.join(shared_dir, "aux"))
        else:
            self.publish_message("Warning: 'aux' directory not found; skipping.")

        if x509_src:
            shutil.copy2(x509_src, os.path.join(shared_dir, self._x509_loc))

        # ---- create sequential job_N symlinks ----------------------------
        for i, job_dir_abs in enumerate(job_dirs):
            link_path = os.path.join(self._main_dir, f"job_{i}")
            if os.path.islink(link_path):
                os.unlink(link_path)
            elif os.path.exists(link_path):
                shutil.rmtree(link_path)
            os.symlink(job_dir_abs, link_path)

        # ---- write condor files -----------------------------------------
        config_dict = self._config_dict
        copy_list = get_copy_file_list(config_dict)
        copy_basenames = sorted({os.path.basename(p) for p in copy_list})
        skip_transfer = {"floats.txt", "ints.txt", "submit_config.txt"}
        extra_transfer_files = [
            os.path.join(self._main_dir, "job_$(Process)", name)
            for name in copy_basenames
            if name not in skip_transfer
        ]

        main_dir = self._main_dir
        submit_path = write_submit_files(
            main_dir,
            n_jobs,
            exe_relpath,
            stage_inputs=self.stage_in,
            stage_outputs=STAGE_OUT,
            root_setup=self.root_setup,
            x509loc=self._x509_loc,
            want_os="el9",
            max_runtime=str(self.max_runtime),
            request_memory=2000,
            request_cpus=1,
            request_disk=20000,
            extra_transfer_files=extra_transfer_files,
            include_aux=aux_exists,
            shared_dir_name="shared_inputs",
            eos_sched=EOS_SCHED,
            config_file="submit_config.txt",
        )

        self.publish_message(
            f"Condor submission ready.\n"
            f"  {n_jobs} job(s) prepared.\n"
            f"  Submit with:  condor_submit {submit_path}"
        )


# ===========================================================================
# Task 3 – SubmitNANOJobs
# ===========================================================================

class SubmitNANOJobs(NANOMixin, law.Task):
    """
    Submits the prepared condor job batch and records the cluster ID + job
    count in submitted.txt so that MonitorNANOJobs can pick them up.
    """

    task_namespace = ""

    def requires(self):
        return BuildNANOSubmission.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "submitted.txt")
        )

    def run(self):
        submit_file = self.input()["submit"].path

        if not os.path.exists(submit_file):
            raise RuntimeError(f"condor_submit.sub not found: {submit_file}")

        result = subprocess.run(
            ["condor_submit", submit_file],
            capture_output=True, text=True, check=True,
        )
        stdout = result.stdout.strip()
        self.publish_message(stdout)

        # Extract cluster ID from condor_submit output
        cluster_id = ""
        for line in stdout.splitlines():
            if "cluster" in line.lower():
                parts = line.split()
                for i, p in enumerate(parts):
                    if p.lower().rstrip(".") == "cluster" and i + 1 < len(parts):
                        cluster_id = parts[i + 1].rstrip(".")
                        break

        # Count total jobs from the "queue N" line in the submit file
        n_jobs = 0
        sub_text = Path(submit_file).read_text()
        for line in sub_text.splitlines():
            stripped = line.strip()
            if stripped.lower().startswith("queue"):
                try:
                    n_jobs = int(stripped.split()[1])
                except (IndexError, ValueError):
                    pass

        with self.output().open("w") as fh:
            fh.write(f"cluster_id={cluster_id}\n")
            fh.write(f"n_jobs={n_jobs}\n")
            fh.write(f"submit_file={submit_file}\n")
            fh.write(stdout + "\n")

        self.publish_message(
            f"Submitted cluster {cluster_id} with {n_jobs} job(s)."
        )


# ===========================================================================
# Task 4 – MonitorNANOJobs
# ===========================================================================

class MonitorNANOJobs(NANOMixin, law.Task):
    """
    Blocking monitoring task that waits for all condor jobs to complete,
    verifies that each expected ROOT output file exists on EOS, and
    resubmits held or failed jobs up to --max-retries times each.

    Per-job status machine
    ----------------------
    submitted  → running  → done           (output confirmed on EOS)
               → held/failed → resubmitting → done
                                           → perm_fail  (retries exhausted)

    State is saved to condorSub_{name}/monitor_state.json after every poll
    cycle so the task can be safely interrupted and restarted.
    """

    task_namespace = ""

    max_retries   = luigi.IntParameter(
        default=3,
        description="Maximum resubmission attempts per failed job (default: 3)",
    )
    poll_interval = luigi.IntParameter(
        default=120,
        description="Seconds between condor_q polls (default: 120)",
    )

    def requires(self):
        return SubmitNANOJobs.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "all_outputs_verified.txt")
        )

    # ---- helpers -----------------------------------------------------------

    @property
    def _state_file(self) -> str:
        return os.path.join(self._main_dir, "monitor_state.json")

    def _load_state(self) -> dict:
        if os.path.exists(self._state_file):
            with open(self._state_file) as fh:
                try:
                    return json.load(fh)
                except json.JSONDecodeError:
                    pass
        return {"jobs": {}}

    def _save_state(self, state: dict) -> None:
        with open(self._state_file, "w") as fh:
            json.dump(state, fh, indent=2)

    def _discover_jobs(self) -> dict[int, dict]:
        """
        Walk job_N symlinks and read each submit_config.txt to obtain
        the expected EOS output paths.
        Returns {job_idx: {dir, orig_save, orig_meta}}.
        """
        info: dict[int, dict] = {}
        i = 0
        while True:
            link = os.path.join(self._main_dir, f"job_{i}")
            if not os.path.exists(link):
                break
            real_dir = os.path.realpath(link)
            cfg = read_config(os.path.join(real_dir, "submit_config.txt"))
            info[i] = {
                "dir":       real_dir,
                "orig_save": cfg.get("__orig_saveFile", ""),
                "orig_meta": cfg.get("__orig_metaFile", ""),
            }
            i += 1
        return info

    def _status_summary(self, state: dict) -> dict[str, int]:
        counts: dict[str, int] = {}
        for jinfo in state["jobs"].values():
            s = jinfo["status"]
            counts[s] = counts.get(s, 0) + 1
        return counts

    # ---- run ---------------------------------------------------------------

    def run(self):
        # Parse submitted.txt -------------------------------------------
        sub_info: dict[str, str] = {}
        with self.input().open("r") as fh:
            for line in fh:
                line = line.strip()
                if "=" in line:
                    k, v = line.split("=", 1)
                    sub_info[k.strip()] = v.strip()

        initial_cluster = sub_info.get("cluster_id", "")
        if not initial_cluster:
            raise RuntimeError("No cluster_id found in submitted.txt")

        job_info = self._discover_jobs()
        if not job_info:
            raise RuntimeError(
                f"No job_N symlinks found in {self._main_dir}. "
                "Run BuildNANOSubmission first."
            )

        config_dict = self._config_dict
        aux_src     = self._resolve("aux")
        aux_exists  = bool(aux_src and os.path.exists(aux_src))

        # Initialise / restore per-job state ----------------------------
        state = self._load_state()
        for idx in job_info:
            key = str(idx)
            if key not in state["jobs"]:
                state["jobs"][key] = {
                    "status":  "submitted",
                    "retries": 0,
                    "cluster": initial_cluster,
                    "proc":    idx,
                }
        self._save_state(state)

        self.publish_message(
            f"Monitoring {len(job_info)} job(s) in cluster {initial_cluster}."
        )

        # Main poll loop ------------------------------------------------
        while True:
            # Gather all distinct cluster IDs that are still active
            active_clusters: set[str] = set()
            for jstate in state["jobs"].values():
                if jstate["status"] in ("submitted", "running", "resubmitting"):
                    active_clusters.add(str(jstate["cluster"]))

            # Snapshot: live queue status and history exit codes
            live_status: dict[tuple, int] = {}    # (cluster, proc) → JobStatus
            history_exit: dict[tuple, int] = {}   # (cluster, proc) → ExitCode
            for cid in active_clusters:
                for proc, jstatus in _condor_q_cluster(cid).items():
                    live_status[(cid, proc)] = jstatus
                for proc, exit_code in _condor_history_exit(cid).items():
                    history_exit[(cid, proc)] = exit_code

            # Update each job's status -----------------------------------
            for idx in sorted(job_info.keys()):
                key    = str(idx)
                jstate = state["jobs"][key]
                cid    = str(jstate["cluster"])
                proc   = jstate["proc"]
                status = jstate["status"]

                if status in ("done", "perm_fail"):
                    continue

                condor_js   = live_status.get((cid, proc))   # None if not in queue
                exit_code   = history_exit.get((cid, proc))  # None if not in history

                if condor_js == 2:   # Running
                    jstate["status"] = "running"
                    continue

                if condor_js == 5:   # Held → resubmit
                    self._try_resubmit(
                        idx, jstate, job_info, config_dict, aux_exists, reason="held"
                    )
                    continue

                if condor_js is None and exit_code is not None:
                    # Job has left the queue
                    if exit_code != 0:
                        self._try_resubmit(
                            idx, jstate, job_info, config_dict, aux_exists,
                            reason=f"exit code {exit_code}"
                        )
                        continue
                    # exit_code == 0: analysis finished, check EOS output

                # Job either completed cleanly (exit 0) or is running and
                # has already left the queue – check EOS file.
                if condor_js is None:  # no longer in queue
                    orig_save = job_info[idx]["orig_save"]
                    if orig_save and _eos_file_exists(orig_save):
                        jstate["status"] = "done"
                        self.publish_message(f"Job {idx}: output verified on EOS.")
                    elif exit_code == 0:
                        # Condor says success but file not on EOS yet – keep waiting
                        pass

            self._save_state(state)

            summary   = self._status_summary(state)
            done      = summary.get("done", 0)
            perm_fail = summary.get("perm_fail", 0)
            total     = len(job_info)
            self.publish_message(
                f"Poll: {done}/{total} verified, "
                f"{perm_fail} permanently failed, "
                f"{total - done - perm_fail} in-flight."
            )

            if done + perm_fail == total:
                break

            time.sleep(self.poll_interval)

        # Write completion file -----------------------------------------
        summary   = self._status_summary(state)
        done      = summary.get("done", 0)
        perm_fail = summary.get("perm_fail", 0)

        with self.output().open("w") as fh:
            fh.write(f"done={done}\n")
            fh.write(f"perm_fail={perm_fail}\n")
            fh.write(f"total={len(job_info)}\n")
            if perm_fail:
                failed = [
                    k for k, j in state["jobs"].items()
                    if j["status"] == "perm_fail"
                ]
                fh.write("perm_failed_jobs=" + ",".join(failed) + "\n")

        if perm_fail:
            self.publish_message(
                f"WARNING: {perm_fail} job(s) permanently failed. "
                f"Details in {self._state_file}"
            )
        else:
            self.publish_message(
                f"All {done} output(s) verified on EOS. Monitoring complete."
            )

    def _try_resubmit(
        self,
        idx: int,
        jstate: dict,
        job_info: dict,
        config_dict: dict,
        aux_exists: bool,
        reason: str,
    ) -> None:
        """Attempt to resubmit job *idx*; update jstate in place."""
        retries = jstate["retries"]
        if retries >= self.max_retries:
            self.publish_message(
                f"Job {idx}: permanently failed ({reason}, "
                f"{retries} retries exhausted)."
            )
            jstate["status"] = "perm_fail"
            return

        self.publish_message(
            f"Job {idx} failed ({reason}) – resubmitting "
            f"(attempt {retries + 1}/{self.max_retries})."
        )
        new_cluster = _submit_single_job(
            idx,
            self._main_dir,
            self._exe_relpath,
            self._x509_loc,
            self.stage_in,
            self.max_runtime,
            2000,
            "shared_inputs",
            config_dict,
            aux_exists,
        )
        if new_cluster:
            jstate.update(
                cluster=new_cluster,
                proc=0,
                retries=retries + 1,
                status="resubmitting",
            )
        else:
            jstate["status"] = "perm_fail"
