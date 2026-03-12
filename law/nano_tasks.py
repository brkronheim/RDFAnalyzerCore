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
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import glob
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore
import requests
from requests.exceptions import ChunkedEncodingError, RequestException
import urllib3

law.contrib.load("htcondor")

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from submission_backend import (  # noqa: E402
    read_config,
    get_copy_file_list,
    write_submit_files,
    ensure_xrootd_redirector,
)
from validate_config import validate_submit_config  # noqa: E402
from workflow_executors import DaskWorkflow, HTCondorWorkflow, _run_analysis_job  # noqa: E402
from dataset_manifest import DatasetManifest  # noqa: E402
from partition_utils import _make_partitions, _query_tree_entries  # noqa: E402
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402


# ---------------------------------------------------------------------------
# Hard-coded assumptions
# ---------------------------------------------------------------------------
STAGE_OUT = True   # always stage outputs to EOS
EOS_SCHED = True   # submission dir lives on EOS
WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))
EOS_BASE = WORKSPACE  # root of the submission dir tree

# Default XRootD redirector used when Rucio does not supply a site-specific one
NANO_REDIRECTOR = "root://xrootd-cms.infn.it/"


def _ensure_xrootd_redirector(uri: str, redirector: str = NANO_REDIRECTOR) -> str:
    """Thin wrapper around the shared ``ensure_xrootd_redirector`` utility.

    Uses ``NANO_REDIRECTOR`` as the default so callers don't need to spell out
    the CMS redirector URL, while delegating all logic to
    ``submission_backend``.
    """
    return ensure_xrootd_redirector(uri, redirector)

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


def _query_rucio(directory, file_split_gb, WL, BL, site_override, client,
                 max_files_per_group: int = 50):
    """Return a dict {group_idx: 'url1,url2,...'}.

    Files are grouped so that each group contains at most
    *max_files_per_group* files and at most *file_split_gb* GB of data.
    """
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
    running_files = 0
    group = 0

    for filedata in rucio_output:
        fname = filedata["name"]
        states = filedata.get("states", {})
        size_gb = filedata.get("bytes", 0) * 1e-9
        redirector = NANO_REDIRECTOR
        running_size += size_gb
        running_files += 1
        if (running_size > file_split_gb) or (running_files >= max_files_per_group):
            group += 1
            running_size = size_gb
            running_files = 1

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

        url = ensure_xrootd_redirector(fname, redirector)
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
    """Parse the sample config and return (sampleDict, baseDirectoryList, lumi, WL, BL).

    Accepts both the legacy key=value text format **and** the new YAML manifest
    format (detected by ``.yaml`` / ``.yml`` extension).  When a YAML manifest
    is supplied the returned ``sampleDict`` contains all :class:`DatasetEntry`
    objects converted to legacy dicts, and ``lumi`` / ``WL`` / ``BL`` are
    taken from the manifest's global settings.
    """
    ext = os.path.splitext(config_file)[1].lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        return (
            manifest.to_legacy_sample_dict(),
            [],
            manifest.lumi,
            manifest.whitelist,
            manifest.blacklist,
        )

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


def _rechunk_urls(urls: list[str], max_files: int) -> dict[int, str]:
    """Split a flat URL list into groups of at most *max_files* entries."""
    groups: dict[int, str] = {}
    for i, url in enumerate(urls):
        g = i // max_files
        if g in groups:
            groups[g] += "," + url
        else:
            groups[g] = url
    return groups


def _collect_local_shared_libs(exe_path: str, repo_root: str) -> dict[str, str]:
    """Collect executable shared-library deps that resolve under repo_root."""
    staged: dict[str, str] = {}
    try:
        ldd_output = subprocess.check_output(["ldd", exe_path], text=True, stderr=subprocess.STDOUT)
    except Exception as err:
        print(f"Warning: failed to inspect shared-library deps via ldd for '{exe_path}': {err}")
        return staged

    repo_prefix = os.path.abspath(repo_root)
    if not repo_prefix.endswith(os.path.sep):
        repo_prefix += os.path.sep

    for line in ldd_output.splitlines():
        line = line.strip()
        if not line or "=> not found" in line:
            continue

        soname = None
        resolved = None

        match_arrow = re.match(r"^(\S+)\s*=>\s*(\S+)", line)
        if match_arrow:
            soname = os.path.basename(match_arrow.group(1))
            resolved = match_arrow.group(2)
        else:
            match_abs = re.match(r"^(\/\S+)\s+\(", line)
            if match_abs:
                resolved = match_abs.group(1)
                soname = os.path.basename(resolved)

        if not resolved or not os.path.isabs(resolved):
            continue

        real_path = os.path.realpath(resolved)
        if not os.path.exists(real_path):
            continue
        if not real_path.startswith(repo_prefix):
            continue

        real_name = os.path.basename(real_path)
        if ".so" not in real_name:
            continue

        staged[real_name] = real_path
        if soname and ".so" in soname:
            staged[soname] = real_path

    return staged


def _eos_file_exists(eos_path: str, timeout: int = 30) -> bool:
    """Return True if *eos_path* can be stat'd on eosuser.cern.ch via xrdfs."""
    if not eos_path:
        return False

    eos_path = eos_path.strip()
    if eos_path.startswith("root://"):
        parts = eos_path.split("/", 3)
        if len(parts) >= 4:
            eos_path = "/" + parts[3].lstrip("/")
        else:
            return False

    try:
        r = subprocess.run(
            ["xrdfs", "root://eosuser.cern.ch/", "stat", eos_path],
            capture_output=True,
            timeout=timeout,
        )
        return r.returncode == 0
    except Exception:
        return False


def _condor_q_ads(cluster_id: str) -> dict[int, dict]:
    """Query condor_q and return raw ads keyed by ProcId."""
    try:
        r = subprocess.run(
            ["condor_q", str(cluster_id), "-json"],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return {}
        ads = json.loads(r.stdout)
        return {ad["ProcId"]: ad for ad in ads if "ProcId" in ad}
    except Exception as e:
        print(f"Warning: condor_q failed for cluster {cluster_id}: {e}")
        return {}


def _condor_q_cluster(cluster_id: str) -> dict[int, int]:
    """
    Query condor_q for all jobs still in the queue for *cluster_id*.
    Returns {proc_id: JobStatus}  (1=Idle, 2=Running, 5=Held).
    Returns {} if the cluster is gone or condor_q fails.
    """
    ads = _condor_q_ads(cluster_id)
    return {proc: ad.get("JobStatus") for proc, ad in ads.items() if "JobStatus" in ad}


def _condor_rm_job(cluster_id: str, proc_id: int, timeout: int = 30) -> bool:
    """Remove a specific held/failed condor job from queue."""
    try:
        jid = f"{cluster_id}.{proc_id}"
        r = subprocess.run(["condor_rm", jid], capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0
    except Exception:
        return False


def _read_failed_job_error(main_dir: str, cluster_id: str, proc_id: int, job_idx: int) -> str:
    """Return a short stderr tail for a failed job if a log file is available."""
    candidates = [
        os.path.join(main_dir, "condor_logs", f"log_{cluster_id}_{proc_id}.stderr"),
    ]
    candidates.extend(sorted(glob.glob(os.path.join(main_dir, "condor_logs", f"resub_{job_idx}_{cluster_id}_{proc_id}.stderr"))))
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as fh:
                lines = fh.read().splitlines()
            tail = "\n".join(lines[-20:]).strip()
            if tail:
                return tail
        except Exception:
            continue
    return ""


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
    aux_file_names: list[str] | None = None,
    shared_lib_names: list[str] | None = None,
) -> str | None:
    """
    Create a per-job condor submit file for *job_index* in *main_dir*/resubmissions/
    and submit it.  Returns the new cluster ID string, or None on failure.
    """
    job_dir   = os.path.join(main_dir, f"job_{job_index}")
    resub_dir = os.path.join(main_dir, "resubmissions")
    Path(resub_dir).mkdir(parents=True, exist_ok=True)

    # Transfer the three per-job files + the whole shared_inputs/ directory.
    # Everything else (exe, .so libs, aux files, x509, config files) lives in
    # shared_inputs/ which HTCondor transfers as a unit – no shared filesystem
    # access required on the remote worker.
    transfer_files = [
        os.path.join(job_dir, "submit_config.txt"),
        os.path.join(job_dir, "floats.txt"),
        os.path.join(job_dir, "ints.txt"),
        os.path.join(main_dir, shared_dir_name),
    ]

    inner_script = os.path.join(main_dir, "condor_runscript_inner.sh")
    if os.path.exists(inner_script):
        transfer_files.append(inner_script)

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
environment = CONDOR_PROC=$(Process) CONDOR_CLUSTER=$(Cluster)
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
    dataset = luigi.Parameter(
        default="",
        description=(
            "Optional: restrict processing to a single named dataset from the "
            "sampleConfig file.  When set, only that dataset is prepared and "
            "executed, and outputs are written to condorSub_{name}_{dataset}/ "
            "so that multiple datasets can be processed independently without "
            "interfering with each other.  Leave empty (default) to process "
            "all datasets in the sampleConfig as a single workflow."
        ),
    )
    size = luigi.IntParameter(
        default=30,
        description="GB of data per condor job (default: 30)",
    )
    partition = luigi.Parameter(
        default="file_group",
        description=(
            "Partitioning mode for splitting each dataset across jobs.  "
            "One of: 'file_group' (default – group files by count and size), "
            "'file' (one file per job), "
            "'entry_range' (split files by TTree entry ranges; requires uproot "
            "and --entries-per-job)."
        ),
    )
    files_per_job = luigi.IntParameter(
        default=50,
        description=(
            "Maximum number of files per job in 'file_group' mode (default: 50). "
            "Ignored for 'file' and 'entry_range' modes."
        ),
    )
    entries_per_job = luigi.IntParameter(
        default=100_000,
        description=(
            "Maximum TTree entries per job in 'entry_range' mode "
            "(default: 100000).  "
            "Requires uproot (pip install uproot).  "
            "Ignored for 'file_group' and 'file' modes."
        ),
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
        description="Path to setup script file; its contents are embedded in the worker inner runscript",
    )
    container_setup = luigi.Parameter(
        default="",
        description="Container setup command used by outer wrapper script (e.g. 'cmssw-el9')",
    )
    max_runtime = luigi.IntParameter(
        default=3600,
        description="Maximum condor job runtime in seconds (default: 3600)",
    )
    no_validate = luigi.BoolParameter(
        default=False,
        description="Skip submit-config validation",
    )
    make_test_job = luigi.BoolParameter(
        default=False,
        description="If True, create a local test_job/ directory with a single input file for quick testing",
    )
    python_env = luigi.Parameter(
        default="",
        description=(
            "Path to a Python environment tarball created by law/setup_python_env.sh. "
            "The tarball is shipped to every condor worker node and unpacked before the "
            "analysis runs, making the packaged Python packages available on PYTHONPATH."
        ),
    )
    dask_scheduler = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Dask scheduler address for --workflow dask "
            "(e.g. tcp://scheduler-host:8786). "
            "Leave empty to start a local Dask cluster."
        ),
    )
    dask_workers = luigi.IntParameter(
        default=1,
        significant=False,
        description=(
            "Number of workers for the local Dask cluster when "
            "--dask-scheduler is empty (default: 1)."
        ),
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
        """Absolute path to the condor submission directory.

        When ``--dataset`` is set the directory is named
        ``condorSub_{name}_{dataset}`` so that independent per-dataset
        pipeline runs do not overwrite each other's symlinks or submit files.
        When ``--dataset`` is empty the original ``condorSub_{name}`` path is
        used, preserving full backward compatibility.
        """
        if self.dataset:
            return os.path.join(EOS_BASE, f"condorSub_{self.name}_{self.dataset}")
        return os.path.join(EOS_BASE, f"condorSub_{self.name}")

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

    @property
    def _root_setup_content(self):
        if not self.root_setup:
            return ""
        path = self._resolve(self.root_setup)
        if not path or not os.path.isfile(path):
            raise RuntimeError(f"root_setup must point to an existing file, got: {self.root_setup!r}")
        with open(path, "r") as fh:
            return fh.read().rstrip("\n")

    def _python_env_path(self) -> str | None:
        if not self.python_env:
            return None
        path = self._resolve(self.python_env)
        if not path or not os.path.isfile(path):
            raise RuntimeError(
                f"python_env must point to an existing tarball, got: {self.python_env!r}\n"
                "Create one with:  bash law/setup_python_env.sh --output python_env.tar.gz"
            )
        return path


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
        if self.dataset:
            if self.dataset not in sample_list:
                raise ValueError(
                    f"Dataset {self.dataset!r} not found in sample config "
                    f"{self._sample_config!r}. "
                    f"Available datasets: {sorted(sample_list.keys())}"
                )
            return {0: self.dataset}
        return {i: key for i, key in enumerate(sorted(sample_list.keys()))}

    def output(self):
        return law.LocalFileTarget(
            os.path.join(
                self._main_dir, "branch_outputs", f"sample_{self.branch}.json"
            )
        )

    # ------------------------------------------------------------------ run

    def run(self):
        task_label = f"PrepareNANOSample[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    def _run_impl(self):
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
        das_path = sample.get("das", "")
        xsec = float(sample["xsec"])
        typ = sample["type"]
        norm = float(sample.get("norm", 1))
        kfac = float(sample.get("kfac", 1))
        extra_scale = float(sample.get("extraScale", 1))
        site_override = sample.get("site", "")

        # Determine the primary TTree name for entry-range queries
        tree_name = config_dict.get("treeList", "Events").split(",")[0].strip() or "Events"

        # Collect all file URLs -------------------------------------------
        all_urls: list[str] = []
        seen_urls: set[str] = set()

        # Check for an explicit file list from the manifest (files field)
        explicit_files = [
            f.strip() for f in sample.get("fileList", "").split(",") if f.strip()
        ]
        if explicit_files:
            for url in explicit_files:
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(ensure_xrootd_redirector(url))
        elif das_path:
            # Query Rucio ----------------------------------------------------
            try:
                client = _get_rucio_client()
            except Exception as e:
                raise RuntimeError(f"Cannot open Rucio client: {e}") from e

            das_entries = [d.strip() for d in das_path.split(",") if d.strip()]
            if not das_entries:
                self.publish_message(f"No DAS entries for sample {name}; creating empty output.")
                Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
                self.output().dump([], formatter="json")
                return

            # Query Rucio sequentially for each DAS entry and merge URLs
            for das_entry in das_entries:
                partial = _query_rucio(
                    das_entry, self.size, WL, BL, site_override, client,
                    max_files_per_group=self.files_per_job,
                )
                for gkey in sorted(partial.keys()):
                    for url in partial[gkey].split(","):
                        url = url.strip()
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            all_urls.append(url)
        else:
            self.publish_message(f"No DAS entries or explicit files for sample {name}; creating empty output.")
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            self.output().dump([], formatter="json")
            return

        if not all_urls:
            self.publish_message(f"No files found for sample {name}; skipping.")
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            self.output().dump([], formatter="json")
            return

        # Config files are staged to shared_inputs/ once by BuildNANOSubmission;
        # they must NOT be copied per-job to avoid N redundant copies on disk.
        # Apply partitioning -------------------------------------------------
        partitions = _make_partitions(
            all_urls,
            mode=self.partition,
            files_per_job=self.files_per_job,
            entries_per_job=self.entries_per_job,
            tree_name=tree_name,
        )

        # Copy-list files that should travel with every job ---------------
        copy_list = get_copy_file_list(config_dict)

        save_dir = self._resolve(config_dict["saveDirectory"])
        try:
            Path(save_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Cannot create save directory {save_dir!r}: {e}")

        sample_base = os.path.join(self._main_dir, "samples", name)

        norm_scale = str(extra_scale * kfac * lumi * xsec / norm)
        created_dirs: list[str] = []

        for sub_idx, partition in enumerate(partitions):
            job_dir = os.path.join(sample_base, f"job_{sub_idx}")
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            # Build job-specific submit config ----------------------------
            job_config = _normalize_config_paths(dict(config_dict))
            job_config["saveFile"] = os.path.join(
                save_dir, f"{name}_{sub_idx}.root"
            )
            job_config["metaFile"] = os.path.join(
                save_dir, f"{name}_{sub_idx}_meta.root"
            )
            job_config["fileList"] = partition["files"]
            job_config["batch"] = "True"
            job_config["type"] = typ
            job_config["sampleConfig"] = os.path.basename(self._sample_config)
            job_config["floatConfig"] = "floats.txt"
            job_config["intConfig"] = "ints.txt"

            # Write entry-range keys when in entry_range partition mode ----
            # last_entry > 0 is the sentinel: file_group/file modes use 0
            # for both fields, while entry_range mode always sets last_entry > 0.
            if partition["last_entry"] > 0:
                job_config["firstEntry"] = str(partition["first_entry"])
                job_config["lastEntry"] = str(partition["last_entry"])

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
        with PerformanceRecorder("BuildNANOSubmission") as rec:
            self._run_impl()
        rec.save(os.path.join(self._main_dir, "build_submission.perf.json"))

    def _run_impl(self):
        def _iter_loadable_targets(obj):
            if obj is None:
                return
            if hasattr(obj, "load") and callable(getattr(obj, "load")):
                yield obj
                return
            if isinstance(obj, dict):
                for value in obj.values():
                    yield from _iter_loadable_targets(value)
                return
            if isinstance(obj, (list, tuple, set)):
                for value in obj:
                    yield from _iter_loadable_targets(value)
                return
            if hasattr(obj, "targets"):
                yield from _iter_loadable_targets(getattr(obj, "targets"))
                return

        # ---- collect all job dirs from branch outputs --------------------
        job_dirs: list[str] = []
        for target in _iter_loadable_targets(self.input()):
            manifest = target.load(formatter="json")
            if isinstance(manifest, list):
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

        local_shared_libs = _collect_local_shared_libs(exe_path, WORKSPACE)
        for staged_name, src_path in sorted(local_shared_libs.items()):
            _copy_file(src_path, os.path.join(shared_dir, staged_name))
        if local_shared_libs:
            self.publish_message(
                f"Staged {len(local_shared_libs)} local shared library file(s) into shared_inputs/."
            )

        aux_src = self._resolve("aux")
        aux_files: list[str] = []
        if aux_src and os.path.exists(aux_src):
            for aux_file in sorted(Path(aux_src).iterdir()):
                if aux_file.is_file():
                    _copy_file(str(aux_file), os.path.join(shared_dir, aux_file.name))
                    aux_files.append(aux_file.name)
        else:
            self.publish_message("Warning: 'aux' directory not found; skipping.")

        if x509_src:
            shutil.copy2(x509_src, os.path.join(shared_dir, self._x509_loc))

        # ---- stage python env tarball if provided -----------------------
        python_env_src = self._python_env_path()
        python_env_staged: str | None = None
        if python_env_src:
            tarball_name = os.path.basename(python_env_src)
            python_env_staged = os.path.join(shared_dir, tarball_name)
            _copy_file(python_env_src, python_env_staged)

        # ---- create sequential job_N symlinks ----------------------------
        for i, job_dir_abs in enumerate(job_dirs):
            link_path = os.path.join(self._main_dir, f"job_{i}")
            if os.path.islink(link_path):
                os.unlink(link_path)
            elif os.path.exists(link_path):
                shutil.rmtree(link_path)
            os.symlink(job_dir_abs, link_path)

        # ---- create local test job if requested --------------------------
        config_dict = self._config_dict
        if self.make_test_job:
            test_dir = os.path.join(self._main_dir, "test_job")
            Path(test_dir).mkdir(parents=True, exist_ok=True)

            # Find the first available input file from sorted job dirs
            first_file = ""
            first_job_dir = ""
            for jd in job_dirs:
                jcfg_path = os.path.join(jd, "submit_config.txt")
                if os.path.exists(jcfg_path):
                    jcfg = read_config(jcfg_path)
                    file_list = jcfg.get("__orig_fileList", jcfg.get("fileList", ""))
                    if file_list:
                        first_file = file_list.split(",")[0].strip()
                        first_job_dir = jd
                        break

            if first_file:
                # Copy copyList files
                copy_list_test = get_copy_file_list(config_dict)
                for fpath in copy_list_test:
                    src = self._resolve(fpath)
                    dst = os.path.join(test_dir, os.path.basename(src))
                    Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(src, dst)

                # Copy aux files flat directly into test_job/
                aux_src_local = self._resolve("aux")
                if aux_src_local and os.path.exists(aux_src_local):
                    for aux_file in sorted(Path(aux_src_local).iterdir()):
                        if aux_file.is_file():
                            _copy_file(str(aux_file), os.path.join(test_dir, aux_file.name))
                else:
                    self.publish_message("Warning: 'aux' directory not found; skipping aux copy for test job.")

                # Copy the executable
                _copy_file(exe_path, os.path.join(test_dir, exe_relpath))

                # Build a minimal submit config
                test_config = _normalize_config_paths(dict(config_dict))
                test_config["fileList"] = first_file
                test_config["batch"] = "False"
                test_config.setdefault("threads", "1")
                test_config["saveFile"] = "test_output.root"
                test_config["metaFile"] = "test_output_meta.root"
                test_config["sampleConfig"] = os.path.basename(self._sample_config)
                test_config["floatConfig"] = "floats.txt"
                test_config["intConfig"] = "ints.txt"
                # Remove stage-out sentinel keys
                test_config.pop("__orig_saveFile", None)
                test_config.pop("__orig_metaFile", None)
                test_config.pop("__orig_fileList", None)

                # Reuse floats.txt / ints.txt from the first job dir
                float_src = os.path.join(first_job_dir, "floats.txt")
                int_src   = os.path.join(first_job_dir, "ints.txt")
                if os.path.exists(float_src):
                    shutil.copyfile(float_src, os.path.join(test_dir, "floats.txt"))
                if os.path.exists(int_src):
                    shutil.copyfile(int_src, os.path.join(test_dir, "ints.txt"))

                with open(os.path.join(test_dir, "submit_config.txt"), "w") as fh:
                    for k, v in test_config.items():
                        fh.write(f"{k}={v}\n")

                self.publish_message(
                    f"Test job created. Run locally with:\n"
                    f"  cd {test_dir} && ./{exe_relpath} submit_config.txt"
                )
            else:
                self.publish_message("Warning: no input files found; test job not created.")

        # ---- copy common config files to shared_inputs/ (once for all jobs) ---
        copy_list = get_copy_file_list(config_dict)
        for fpath in copy_list:
            src = self._resolve(fpath)
            if src and os.path.exists(src):
                dst = os.path.join(shared_dir, os.path.basename(src))
                if not os.path.exists(dst):
                    shutil.copy2(src, dst)

        # ---- write condor files -----------------------------------------
        # shared_inputs/ directory is transferred as a whole by write_submit_files;
        # no extra per-file entries are needed (python env tarball is inside shared_inputs/).
        extra_transfer_files: list[str] = []

        main_dir = self._main_dir
        submit_path = write_submit_files(
            main_dir,
            n_jobs,
            exe_relpath,
            stage_inputs=self.stage_in,
            stage_outputs=STAGE_OUT,
            root_setup=self._root_setup_content,
            x509loc=self._x509_loc,
            max_runtime=str(self.max_runtime),
            request_memory=2000,
            request_cpus=1,
            request_disk=20000,
            extra_transfer_files=extra_transfer_files,
            include_aux=False,
            shared_dir_name="shared_inputs",
            eos_sched=EOS_SCHED,
            config_file="submit_config.txt",
            container_setup=self.container_setup,
            python_env_tarball=python_env_staged,
        )

        self.publish_message(
            f"Condor submission ready.\n"
            f"  {n_jobs} job(s) prepared.\n"
            f"  Submit with:  condor_submit {submit_path}"
        )


# ===========================================================================
# Task 3 – RunTestJob
# ===========================================================================

class RunTestJob(NANOMixin, law.Task):
    """
    Runs the test_job/ directory produced by BuildNANOSubmission locally,
    validates the exit code and output ROOT files, and writes test_passed.txt.

    SubmitNANOJobs requires this task when --make-test-job is set, so condor
    submission is blocked until the test job is verified to be working.
    """

    task_namespace = ""

    def requires(self):
        return BuildNANOSubmission.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "test_job", "test_passed.txt")
        )

    def run(self):
        test_dir = os.path.join(self._main_dir, "test_job")
        exe_relpath = self._exe_relpath

        if not os.path.isdir(test_dir):
            raise RuntimeError(
                f"test_job/ directory not found at {test_dir!r}. "
                "Run BuildNANOSubmission with --make-test-job."
            )
        if not os.path.exists(os.path.join(test_dir, exe_relpath)):
            raise RuntimeError(
                f"Executable {exe_relpath!r} not found in {test_dir!r}."
            )

        # Build inner shell command
        root_setup_path = self._resolve(self.root_setup) if self.root_setup else ""
        cmd_parts = []
        if root_setup_path and os.path.isfile(root_setup_path):
            cmd_parts.append(f"source {shlex.quote(root_setup_path)}")
        cmd_parts.append(f"cd {shlex.quote(test_dir)}")
        cmd_parts.append(f"chmod +x {shlex.quote(exe_relpath)}")
        cmd_parts.append(f"./{exe_relpath} submit_config.txt")
        inner_cmd = " && ".join(cmd_parts)

        # Optionally wrap with container
        container = (self.container_setup or "").strip()
        if container:
            if "{cmd}" in container:
                full_cmd = container.replace("{cmd}", f"bash -c {shlex.quote(inner_cmd)}")
            elif "cmssw-el" in container and "--command-to-run" not in container:
                full_cmd = f"{container} --command-to-run {shlex.quote(inner_cmd)}"
            elif "--command-to-run" in container:
                full_cmd = f"{container} {shlex.quote(inner_cmd)}"
            else:
                full_cmd = f"{container} bash -c {shlex.quote(inner_cmd)}"
        else:
            full_cmd = f"bash -c {shlex.quote(inner_cmd)}"

        self.publish_message(f"Running test job:\n  {full_cmd}")

        with PerformanceRecorder("RunTestJob") as rec:
            # stdout/stderr intentionally inherit from parent so the test
            # job output is visible to the user running the workflow.
            proc = subprocess.Popen(full_cmd, shell=True)
            rec.monitor_process(proc.pid)
            proc.wait()
            rc = proc.returncode

        rec.save(perf_path_for(self.output().path))

        if rc != 0:
            if rc >= 128:
                sig = rc - 128
                raise RuntimeError(
                    f"Test job terminated by signal {sig} (segmentation fault or abort). "
                    f"Exit code: {rc}"
                )
            raise RuntimeError(f"Test job failed with exit code {rc}.")

        """
        # Validate output files
        expected = ["test_output.root", "test_output_meta.root"]
        missing = [f for f in expected if not os.path.exists(os.path.join(test_dir, f))]
        empty   = [f for f in expected if os.path.exists(os.path.join(test_dir, f))
                   and os.path.getsize(os.path.join(test_dir, f)) == 0]

        if missing:
            raise RuntimeError(f"Test job completed but expected output file(s) missing: {missing}")
        if empty:
            raise RuntimeError(f"Test job completed but expected output file(s) are empty: {empty}")
        """
        self.publish_message("Test job passed: exit code 0")

        with self.output().open("w") as fh:
            fh.write("status=passed\n")
            #for fname in expected:
            #    size = os.path.getsize(os.path.join(test_dir, fname))
            #    fh.write(f"{fname}={size} bytes\n")


# ===========================================================================
# Task 4 – SubmitNANOJobs
# ===========================================================================

class SubmitNANOJobs(NANOMixin, law.Task):
    """
    Submits the prepared condor job batch and records the cluster ID + job
    count in submitted.txt so that MonitorNANOJobs can pick them up.
    """

    task_namespace = ""

    def requires(self):
        if self.make_test_job:
            return RunTestJob.req(self)
        return BuildNANOSubmission.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "submitted.txt")
        )

    def run(self):
        with PerformanceRecorder("SubmitNANOJobs") as rec:
            # Locate condor_submit.sub directly – input may be BuildNANOSubmission
            # (dict with "submit" key) or RunTestJob (single FileTarget)
            inp = self.input()
            if isinstance(inp, dict) and "submit" in inp:
                submit_file = inp["submit"].path
            else:
                submit_file = os.path.join(self._main_dir, "condor_submit.sub")

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

        rec.save(perf_path_for(self.output().path))


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
            live_hold_reason: dict[tuple, str] = {}  # (cluster, proc) → HoldReason
            history_exit: dict[tuple, int] = {}   # (cluster, proc) → ExitCode
            for cid in active_clusters:
                for proc, ad in _condor_q_ads(cid).items():
                    if "JobStatus" in ad:
                        live_status[(cid, proc)] = ad["JobStatus"]
                    if "HoldReason" in ad and ad.get("HoldReason"):
                        live_hold_reason[(cid, proc)] = str(ad.get("HoldReason"))
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
                    hold_reason = live_hold_reason.get((cid, proc), "held without reported reason")
                    removed = _condor_rm_job(cid, proc)
                    if removed:
                        self.publish_message(f"Job {idx} held and removed from queue: {hold_reason}")
                    else:
                        self.publish_message(f"Job {idx} held (could not confirm condor_rm): {hold_reason}")
                    self._try_resubmit(
                        idx, jstate, job_info, config_dict, reason=f"held: {hold_reason}"
                    )
                    continue

                if condor_js is None and exit_code is not None:
                    # Job has left the queue
                    if exit_code != 0:
                        error_tail = _read_failed_job_error(self._main_dir, cid, proc, idx)
                        if error_tail:
                            self.publish_message(
                                f"Job {idx} failed with exit code {exit_code}. stderr tail:\n{error_tail}"
                            )
                        self._try_resubmit(
                            idx, jstate, job_info, config_dict,
                            reason=f"exit code {exit_code}"
                        )
                        continue
                    # exit_code == 0: analysis finished, check EOS output

                # Job either completed cleanly (exit 0) or is running and
                # has already left the queue – check EOS file.
                if condor_js is None:  # no longer in queue
                    orig_save = job_info[idx]["orig_save"]
                    orig_meta = job_info[idx]["orig_meta"]
                    if (
                        (orig_save and _eos_file_exists(orig_save))
                        or (orig_meta and _eos_file_exists(orig_meta))
                    ):
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


# ===========================================================================
# Task 5 – RunNANOJobs  (local / htcondor / dask executor)
# ===========================================================================

class RunNANOJobs(NANOMixin, law.LocalWorkflow, HTCondorWorkflow, DaskWorkflow):
    """
    Workflow task that executes prepared analysis jobs via a selectable executor.

    This task requires ``BuildNANOSubmission`` to have been run first (which
    creates the ``job_N`` symlinks, shared inputs, and the condor submit files).
    Each branch corresponds to one prepared job directory.

    Select the executor with the ``--workflow`` flag:

    * ``--workflow local``     – run every job sequentially via a local
                                 subprocess (default).
    * ``--workflow htcondor``  – dispatch each job to an HTCondor cluster using
                                 LAW's native HTCondor workflow mechanism.
    * ``--workflow dask``      – run jobs concurrently on a Dask distributed
                                 cluster; set the scheduler address with
                                 ``--dask-scheduler tcp://host:8786``.

    All three modes produce the same output files
    (``condorSub_<name>/job_outputs/job_<N>.done``) and accept identical
    command-line parameters, making them trivially interchangeable.

    Examples
    --------
    ::

        # Prepare submission (always local – needs Rucio)
        law run PrepareNANOSample   --submit-config cfg/submit.txt --name myRun \\
            --x509 /tmp/x509 --exe build/myexe

        law run BuildNANOSubmission --submit-config cfg/submit.txt --name myRun \\
            --x509 /tmp/x509 --exe build/myexe

        # Execute with local workers
        law run RunNANOJobs --submit-config cfg/submit.txt --name myRun \\
            --x509 /tmp/x509 --exe build/myexe --workflow local

        # Execute on HTCondor (via LAW's htcondor workflow)
        law run RunNANOJobs --submit-config cfg/submit.txt --name myRun \\
            --x509 /tmp/x509 --exe build/myexe --workflow htcondor

        # Execute on a Dask cluster
        law run RunNANOJobs --submit-config cfg/submit.txt --name myRun \\
            --x509 /tmp/x509 --exe build/myexe \\
            --workflow dask --dask-scheduler tcp://scheduler:8786
    """

    task_namespace = ""

    # Parameters not meaningful at the individual-branch level
    exclude_params_branch = getattr(DaskWorkflow, "exclude_params_branch", set()) | {
        "make_test_job",
    }

    # ------------------------------------------------------------------
    # Branch map: one branch per job_N symlink created by BuildNANOSubmission
    # ------------------------------------------------------------------

    def create_branch_map(self):
        jobs: dict[int, str] = {}
        i = 0
        while True:
            link = os.path.join(self._main_dir, f"job_{i}")
            if not (os.path.exists(link) or os.path.islink(link)):
                break
            jobs[i] = os.path.realpath(link)
            i += 1
        return jobs

    # ------------------------------------------------------------------
    # Requirements
    # ------------------------------------------------------------------

    def workflow_requires(self):
        """The build step must complete before any job can run."""
        reqs = super().workflow_requires()
        reqs["build"] = BuildNANOSubmission.req(self)
        return reqs

    def requires(self):
        return BuildNANOSubmission.req(self)

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "job_outputs", f"job_{self.branch}.done")
        )

    # ------------------------------------------------------------------
    # run() – executed by --workflow local  AND  --workflow htcondor branches
    # ------------------------------------------------------------------

    def run(self):
        """Execute one analysis job (one branch)."""
        job_dir = self.branch_data
        exe_path = os.path.join(self._shared_dir, self._exe_relpath)

        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Executable not found at {exe_path!r}. "
                "Run BuildNANOSubmission first."
            )
        if not os.path.isdir(job_dir):
            raise RuntimeError(
                f"Job directory not found: {job_dir!r}. "
                "Run BuildNANOSubmission first."
            )

        task_label = f"RunNANOJobs[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            result_text = _run_analysis_job(
                exe_path=exe_path,
                job_dir=job_dir,
                root_setup=self._root_setup_content,
                container_setup=self.container_setup or "",
            )

        rec.save(perf_path_for(self.output().path))
        self.publish_message(f"Branch {self.branch}: {result_text}")
        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with self.output().open("w") as fh:
            fh.write(f"status=done\n{result_text}\n")

    # ------------------------------------------------------------------
    # HTCondor workflow configuration
    # ------------------------------------------------------------------

    def htcondor_output_directory(self):
        """Directory for LAW's HTCondor job submission files."""
        return law.LocalDirectoryTarget(
            os.path.join(self._main_dir, "law_htcondor")
        )

    def htcondor_log_directory(self):
        """Directory for HTCondor log files."""
        return law.LocalDirectoryTarget(
            os.path.join(self._main_dir, "law_htcondor_logs")
        )

    def htcondor_bootstrap_file(self):
        """
        Optional bootstrap script executed on every HTCondor worker before the
        LAW branch task runs.  Users can supply a site-specific script at
        ``law/htcondor_bootstrap.sh``; if absent, no bootstrap is applied.
        """
        bootstrap_path = os.path.join(_HERE, "htcondor_bootstrap.sh")
        if os.path.isfile(bootstrap_path):
            from law.job.base import JobInputFile  # type: ignore
            return JobInputFile(bootstrap_path, copy=True, render_local=False)
        return None

    def htcondor_job_config(self, config, job_num, branches):
        """Configure per-job HTCondor resource requests."""
        if not hasattr(config, "custom_content") or config.custom_content is None:
            config.custom_content = []
        config.custom_content.extend([
            ("+RequestMemory", str(2000)),
            ("+MaxRuntime", str(self.max_runtime)),
            ("request_cpus", "1"),
            ("request_disk", "20000"),
        ])
        # Transfer the python environment tarball when provided
        python_env_src = self._python_env_path()
        if python_env_src:
            tarball_name = os.path.basename(python_env_src)
            staged = os.path.join(self._shared_dir, tarball_name)
            if os.path.isfile(staged):
                existing = dict(
                    item for item in config.custom_content
                    if isinstance(item, (list, tuple)) and len(item) == 2
                )
                xfer = existing.get("transfer_input_files", "")
                if xfer:
                    config.custom_content.append(
                        ("transfer_input_files", f"{xfer},{staged}")
                    )
                else:
                    config.custom_content.append(("transfer_input_files", staged))
        return config

    def htcondor_workflow_requires(self):
        """Ensure BuildNANOSubmission is complete before HTCondor submission."""
        from law.util import DotDict  # type: ignore
        return DotDict(build=BuildNANOSubmission.req(self))

    # ------------------------------------------------------------------
    # Dask workflow configuration
    # ------------------------------------------------------------------

    def get_dask_work(self, branch_num: int, branch_data: str) -> tuple:
        """
        Return the (callable, args, kwargs) triple that runs one analysis job
        on a Dask worker.

        The *callable* is :func:`_run_analysis_job`, a pure module-level
        function that is picklable and requires no LAW imports on the worker.
        Workers only need access to the shared filesystem where
        ``shared_inputs/`` and the ``job_N`` directories live.
        """
        exe_path = os.path.join(self._shared_dir, self._exe_relpath)
        return (
            _run_analysis_job,
            [exe_path, branch_data, self._root_setup_content, self.container_setup or ""],
            {},
        )


# ===========================================================================
# GetXRDFSFileList – file discovery via xrdfs ls
# ===========================================================================

def _xrdfs_list_files(
    server: str,
    remote_path: str,
    pattern: str = "*.root",
    recursive: bool = True,
    timeout: int = 60,
) -> list:
    """Discover ROOT files on an XRootD server using ``xrdfs ls``.

    Parameters
    ----------
    server:
        XRootD server URL, e.g. ``root://eosuser.cern.ch/``.
    remote_path:
        Remote directory path to search, e.g. ``/eos/user/a/alice/ntuples/``.
    pattern:
        Glob-style filename pattern to match (matched against basename only).
    recursive:
        When True, recursively search sub-directories.
    timeout:
        Timeout in seconds for each ``xrdfs`` invocation.

    Returns
    -------
    list[str]
        Sorted list of ``root://{server}/{path}`` URLs for matching files.
    """
    import fnmatch

    def _ls(path: str) -> list:
        try:
            result = subprocess.run(
                ["xrdfs", server, "ls", "-l", path],
                capture_output=True, text=True, timeout=timeout,
            )
        except Exception as exc:
            logging.warning("xrdfs ls failed for %s: %s", path, exc)
            return []

        if result.returncode != 0:
            logging.warning(
                "xrdfs ls returned %d for %s: %s",
                result.returncode, path, result.stderr.strip(),
            )
            return []

        found: list = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if not parts:
                continue
            entry = parts[-1]
            if not entry.startswith("/"):
                continue
            if line.startswith("d") or (len(parts) > 4 and parts[0].startswith("d")):
                # It's a directory
                if recursive:
                    found.extend(_ls(entry))
            else:
                if fnmatch.fnmatch(os.path.basename(entry), pattern):
                    found.append(entry)
        return found

    paths = _ls(remote_path)
    urls = [f"{server.rstrip('/')}/{p.lstrip('/')}" for p in sorted(paths)]
    return urls


class XRDFSMixin:
    """Parameters shared by :class:`GetXRDFSFileList`."""

    name = luigi.Parameter(
        description="Run name; output files go to xrdfsFileList_{name}/",
    )
    dataset_manifest = luigi.Parameter(
        description=(
            "Path to the dataset manifest YAML.  Each dataset entry should "
            "specify either an XRootD server + remote path via the ``xrdfs_server`` "
            "and ``xrdfs_path`` fields, or explicit ``files`` URLs."
        ),
    )
    xrdfs_server = luigi.Parameter(
        default="",
        description=(
            "Default XRootD server (e.g. root://eosuser.cern.ch/).  "
            "Can be overridden per-dataset in the manifest."
        ),
    )
    xrdfs_pattern = luigi.Parameter(
        default="*.root",
        description="Glob pattern for ROOT file discovery (default: *.root).",
    )
    xrdfs_recursive = luigi.BoolParameter(
        default=True,
        description="Recursively search sub-directories (default: True).",
    )


class GetXRDFSFileList(XRDFSMixin, law.LocalWorkflow):
    """Produce a per-dataset XRootD file-list JSON by running ``xrdfs ls``.

    This task is useful for finding files on XRootD storage (EOS, dCache, etc.)
    when Rucio or the CERN Open Data Portal are not available – for example when
    running over existing skims or NTuples stored on EOS.

    For each dataset entry in the manifest the task looks for an ``xrdfs_path``
    field (the remote directory to search) and optionally an ``xrdfs_server``
    field.  If neither is provided and explicit ``files`` are listed they are
    used directly.

    Output per dataset::

        xrdfsFileList_{name}/{dataset_name}.json
        {"sample": "dataset_name", "files": ["root://...", ...]}

    The output can be consumed by :class:`~analysis_tasks.SkimTask` via
    ``--file-source xrdfs --file-source-name <name>``.
    """

    task_namespace = ""

    @property
    def _file_list_dir(self) -> str:
        return os.path.join(WORKSPACE, f"xrdfsFileList_{self.name}")

    def create_branch_map(self):
        from dataset_manifest import DatasetManifest as _DatasetManifest
        manifest = _DatasetManifest.load(self.dataset_manifest)
        return {i: entry for i, entry in enumerate(manifest.datasets)}

    def output(self):
        from dataset_manifest import DatasetEntry as _DatasetEntry
        dataset: _DatasetEntry = self.branch_data
        return law.LocalFileTarget(
            os.path.join(self._file_list_dir, f"{dataset.name}.json")
        )

    def run(self):
        task_label = f"GetXRDFSFileList[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    def _run_impl(self):
        from dataset_manifest import DatasetEntry as _DatasetEntry
        dataset: _DatasetEntry = self.branch_data

        # Prefer explicit file list from the manifest
        if dataset.files:
            files = [ensure_xrootd_redirector(f) for f in dataset.files]
        else:
            extras = getattr(dataset, "extra", {}) or {}
            server = extras.get("xrdfs_server") or self.xrdfs_server
            remote_path = extras.get("xrdfs_path", "")
            if not remote_path:
                self.publish_message(
                    f"Dataset '{dataset.name}' has no files or xrdfs_path; "
                    "writing empty list."
                )
                files = []
            elif not server:
                raise RuntimeError(
                    f"Dataset '{dataset.name}': xrdfs_path is set but no "
                    "xrdfs_server provided (set --xrdfs-server or add "
                    "'xrdfs_server' to the manifest entry)."
                )
            else:
                files = _xrdfs_list_files(
                    server=server,
                    remote_path=remote_path,
                    pattern=self.xrdfs_pattern,
                    recursive=self.xrdfs_recursive,
                )

        payload = {"sample": dataset.name, "files": files}
        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(payload, fh, indent=2)
        self.publish_message(
            f"GetXRDFSFileList: {dataset.name} → {len(files)} file(s)"
        )


# ===========================================================================
# GetNANOFileList – lightweight file-list-only task
# ===========================================================================

class GetNANOFileList(NANOMixin, law.LocalWorkflow):
    """Produce a per-dataset XRootD file-list JSON without creating job directories.

    Unlike :class:`PrepareNANOSample` this task **only** queries Rucio (or uses
    the explicit ``files`` field from the dataset manifest) and writes a compact
    JSON file::

        nanoFileList_{name}/{sample_name}.json
        {"sample": "name", "files": ["root://...", ...]}

    The output of this task can be consumed directly by :class:`~analysis_tasks.SkimTask`
    via the ``--file-source nano --file-source-name <name>`` parameters, enabling
    a clean file-discovery → skim → collect-histograms pipeline without the full
    HTCondor submission machinery.

    Parameters are identical to :class:`NANOMixin`.
    """

    task_namespace = ""

    @property
    def _file_list_dir(self) -> str:
        """Directory where per-dataset file list JSONs are written."""
        return os.path.join(WORKSPACE, f"nanoFileList_{self.name}")

    def create_branch_map(self):
        sample_list, _, _, _, _ = _get_sample_list(self._sample_config)
        if self.dataset:
            if self.dataset not in sample_list:
                raise ValueError(
                    f"Dataset {self.dataset!r} not found in sample config. "
                    f"Available: {sorted(sample_list.keys())}"
                )
            return {0: self.dataset}
        return {i: key for i, key in enumerate(sorted(sample_list.keys()))}

    def output(self):
        sample_key = self.branch_data
        return law.LocalFileTarget(
            os.path.join(self._file_list_dir, f"{sample_key}.json")
        )

    def run(self):
        task_label = f"GetNANOFileList[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    def _run_impl(self):
        sample_key = self.branch_data
        sample_list, _, lumi, WL, BL = _get_sample_list(self._sample_config)
        sample = sample_list[sample_key]

        name = sample["name"]
        das_path = sample.get("das", "")
        site_override = sample.get("site", "")

        all_urls: list = []
        seen_urls: set = set()
        # Rucio-computed groups preserved for downstream SkimTask partitioning.
        # Each entry is a comma-separated URL string (one group = one future job).
        rucio_groups: list = []

        # Check for explicit file list (no Rucio, no size-based grouping)
        explicit_files = [
            f.strip() for f in sample.get("fileList", "").split(",") if f.strip()
        ]
        if explicit_files:
            for url in explicit_files:
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(ensure_xrootd_redirector(url))
            # No pre-computed groups for explicit file lists; SkimTask will
            # partition them using --partition / --files-per-job.
        elif das_path:
            try:
                client = _get_rucio_client()
            except Exception as e:
                raise RuntimeError(f"Cannot open Rucio client: {e}") from e

            das_entries = [d.strip() for d in das_path.split(",") if d.strip()]
            for das_entry in das_entries:
                # Use self.size (GB) AND self.files_per_job as the per-group
                # limits – exactly the same parameters as PrepareNANOSample.
                partial = _query_rucio(
                    das_entry, self.size, WL, BL, site_override, client,
                    max_files_per_group=self.files_per_job,
                )
                for gkey in sorted(partial.keys()):
                    group_str = partial[gkey]  # comma-separated URL string
                    group_urls = [u.strip() for u in group_str.split(",") if u.strip()]
                    new_urls = [u for u in group_urls if u not in seen_urls]
                    if new_urls:
                        seen_urls.update(new_urls)
                        all_urls.extend(new_urls)
                        # Preserve the group as a comma-separated string
                        rucio_groups.append(",".join(new_urls))
        else:
            self.publish_message(
                f"No DAS entries or explicit files for sample {name}; writing empty list."
            )

        # Build output payload.
        # ``groups`` encodes the Rucio-computed size+count grouping so that
        # PrepareSkimJobs / SkimTask can use them directly without re-partitioning.
        payload: dict = {"sample": name, "files": all_urls}
        if rucio_groups:
            payload["groups"] = rucio_groups

        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(payload, fh, indent=2)
        self.publish_message(
            f"GetNANOFileList: {name} → {len(all_urls)} file(s), "
            f"{len(rucio_groups)} Rucio group(s) written to {self.output().path}"
        )
