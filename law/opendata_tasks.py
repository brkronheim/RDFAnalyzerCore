"""
Law tasks for CERN Open Data batch submission.

Mirrors the four-task structure of nano_tasks.py but uses the CERN Open Data
Portal API instead of Rucio for file discovery.

Fixed assumptions:
  - stage-out  is always active (outputs xrdcp'd to EOS after each job)
  - stage-in   is a freely-settable BoolParameter (default False)

All parallelism is delegated entirely to law's branch dispatch mechanism.

Workflow
--------
  PrepareOpenDataSample  (LocalWorkflow, one branch per sample in sampleConfig)
      → per branch: fetches file lists from CERN Open Data API, groups files
        by count (--files), then writes
        condorSub_{name}/samples/{sample_name}/job_{i}/
        with submit_config.txt / floats.txt / ints.txt
      → output: branch_outputs/sample_{N}.json  (list of created job dirs)

  BuildOpenDataSubmission  (Task)
      → requires all PrepareOpenDataSample branches
      → copies exe / aux / x509 into shared_inputs/
      → creates sequential symlinks  job_0 … job_{N-1}
      → writes condor_runscript.sh + condor_submit.sub

  SubmitOpenDataJobs  (Task)
      → requires BuildOpenDataSubmission
      → runs condor_submit, saves cluster ID + job count to submitted.txt

  MonitorOpenDataJobs  (Task)
      → requires SubmitOpenDataJobs
      → blocking poll loop: checks condor_q / condor_history every
        --poll-interval seconds
      → verifies each expected EOS output file via xrdfs stat
      → resubmits held / failed jobs individually (up to --max-retries times)
      → persists state in monitor_state.json (restart-safe)
      → writes all_outputs_verified.txt when every output is confirmed

Usage
-----
  source law/env.sh

  law run PrepareOpenDataSample \\
      --submit-config analyses/myAnalysis/cfg/submit_config.txt \\
      --name myRun --exe build/analyses/myAnalysis/myanalysis

  law run BuildOpenDataSubmission   [same params]
  law run SubmitOpenDataJobs        [same params]
  law run MonitorOpenDataJobs       [same params]
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import glob
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore
import requests
from requests.exceptions import RequestException

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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STAGE_OUT = True   # always stage outputs
WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))
EOS_BASE  = WORKSPACE

# Default XRootD redirector for CERN Open Data files
OPENDATA_REDIRECTOR = "root://eospublic.cern.ch/"


# ===========================================================================
# Utility functions
# ===========================================================================


def _ensure_xrootd_redirector(uri: str, redirector: str = OPENDATA_REDIRECTOR) -> str:
    """Thin wrapper around the shared ``ensure_xrootd_redirector`` utility.

    Defaults the redirector to :data:`OPENDATA_REDIRECTOR` for CERN Open Data
    files and delegates all logic to the shared implementation in
    ``submission_backend``.
    """
    return ensure_xrootd_redirector(uri, redirector)


def _load_file_indices(recid):
    """Fetch _file_indices metadata for a CERN Open Data record.

    Tries cernopendata-client first, falls back to the REST API.
    Raises RuntimeError if both fail.
    """
    client_error = None
    if shutil.which("cernopendata-client"):
        result = subprocess.run(
            [
                "cernopendata-client",
                "get-metadata",
                "--recid",
                str(recid),
                "--output-value=_file_indices",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        if result.returncode == 0 and stdout:
            try:
                return json.loads(stdout)
            except json.JSONDecodeError as exc:
                client_error = f"Invalid JSON from cernopendata-client: {exc}"
        else:
            client_error = stderr or "Empty output from cernopendata-client"
    else:
        client_error = "cernopendata-client not found on PATH"

    api_error = None
    try:
        response = requests.get(
            f"https://opendata.cern.ch/api/records/{recid}", timeout=30
        )
    except Exception as exc:
        api_error = f"HTTP request failed: {exc}"
    else:
        if response.ok:
            try:
                payload = response.json()
            except ValueError as exc:
                api_error = f"Invalid JSON from CERN Open Data API: {exc}"
            else:
                metadata = payload.get("metadata", {})
                file_indices = metadata.get("_file_indices")
                if file_indices:
                    return file_indices
                api_error = "CERN Open Data API response missing metadata._file_indices"
        else:
            api_error = f"CERN Open Data API returned HTTP {response.status_code}"

    raise RuntimeError(
        f"Failed to fetch metadata for recid {recid}. "
        f"Client error: {client_error}. API error: {api_error}."
    )


def _process_metadata(recid, sample_names):
    """Return {sample_name: [uri, ...]} for the given recid.

    sample_names maps Open Data key → analysis sample name.
    """
    result = _load_file_indices(recid)
    file_dict: dict[str, list[str]] = {}
    for entry in result:
        for data in entry.get("files", []):
            uri = _ensure_xrootd_redirector(data["uri"])
            key = data["key"].split("_file_index")[0]
            if key not in sample_names:
                continue
            mapped = sample_names[key]
            file_dict.setdefault(mapped, []).append(uri)
    return file_dict


def _parse_opendata_config(config_file):
    """Parse a sample config file for Open Data submissions.

    Accepts both the legacy key=value text format **and** the new YAML manifest
    format (detected by ``.yaml`` / ``.yml`` extension).  When a YAML manifest
    is provided, ``recids`` are collected from the ``das`` field of each entry
    (comma-separated record IDs stored there by convention for open-data
    samples).

    Returns:
        samples    dict[str, dict]  – {name: legacy_dict}
        recids     list[str]        – record IDs to query
        lumi       float            – luminosity
    """
    ext = os.path.splitext(config_file)[1].lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        samples = manifest.to_legacy_sample_dict()
        recids: list[str] = []
        for entry in manifest.datasets:
            if entry.das:
                for r in entry.das.split(","):
                    r = r.strip()
                    if r and r not in recids:
                        recids.append(r)
        return samples, recids, manifest.lumi

    samples: dict[str, dict] = {}
    recids: list[str] = []
    lumi = 1.0

    with open(config_file) as fh:
        for line in fh:
            line = line.split("#")[0].strip().split()
            inner: dict[str, str] = {}
            for pair in line:
                parts = pair.split("=")
                if len(parts) == 2:
                    inner[parts[0]] = parts[1]
            if "lumi" in inner:
                lumi = float(inner["lumi"])
            if "recids" in inner:
                recids += [r.strip() for r in inner["recids"].split(",") if r.strip()]
            if "name" in inner and "das" in inner:
                samples[inner["name"]] = inner

    return samples, recids, lumi


def _append_unique_lines(file_path, lines):
    """Ensure each line appears exactly once in file_path."""
    existing: list[str] = []
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


def _normalize_config_paths(config_dict):
    """Strip directory components from .txt path values."""
    normalized = dict(config_dict)
    for key, value in config_dict.items():
        if isinstance(value, str) and ".txt" in value and os.path.sep in value:
            normalized[key] = os.path.basename(value)
    return normalized


def _copy_file(src: str, dst: str) -> None:
    """Copy src to dst, skipping if sizes and mtimes match."""
    if os.path.exists(dst):
        if (os.path.getsize(dst) == os.path.getsize(src)
                and int(os.path.getmtime(dst)) == int(os.path.getmtime(src))):
            return
        os.remove(dst)
    shutil.copy2(src, dst)


def _copy_dir(src: str, dst: str) -> None:
    """Copy whole directory tree src → dst, syncing any existing destination."""
    shutil.copytree(src, dst, dirs_exist_ok=True)


def _collect_local_shared_libs(exe_path: str, repo_root: str) -> dict[str, str]:
    """Collect shared-library deps of exe that resolve under repo_root."""
    staged: dict[str, str] = {}
    try:
        ldd_output = subprocess.check_output(
            ["ldd", exe_path], text=True, stderr=subprocess.STDOUT
        )
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
        soname = resolved = None
        match_arrow = re.match(r"^(\S+)\s*=>\s*(\S+)", line)
        if match_arrow:
            soname   = os.path.basename(match_arrow.group(1))
            resolved = match_arrow.group(2)
        else:
            match_abs = re.match(r"^(\/\S+)\s+\(", line)
            if match_abs:
                resolved = match_abs.group(1)
                soname   = os.path.basename(resolved)
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
    """Return True if eos_path can be stat'd on eosuser.cern.ch."""
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
    """Return raw condor_q ads keyed by ProcId."""
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


def _condor_rm_job(cluster_id: str, proc_id: int, timeout: int = 30) -> bool:
    """Remove a specific job from the condor queue."""
    try:
        r = subprocess.run(
            ["condor_rm", f"{cluster_id}.{proc_id}"],
            capture_output=True, text=True, timeout=timeout,
        )
        return r.returncode == 0
    except Exception:
        return False


def _condor_history_exit(cluster_id: str) -> dict[int, int]:
    """Return {proc_id: ExitCode} for jobs that have left the queue."""
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


def _read_failed_job_error(main_dir: str, cluster_id: str, proc_id: int, job_idx: int) -> str:
    """Return a short stderr tail for a failed job if a log file is available."""
    candidates = [
        os.path.join(main_dir, "condor_logs", f"log_{cluster_id}_{proc_id}.stderr"),
    ]
    candidates.extend(
        sorted(glob.glob(
            os.path.join(main_dir, "condor_logs", f"resub_{job_idx}_{cluster_id}_{proc_id}.stderr")
        ))
    )
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path) as fh:
                lines = fh.read().splitlines()
            tail = "\n".join(lines[-20:]).strip()
            if tail:
                return tail
        except Exception:
            continue
    return ""


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
    shared_lib_names: list[str] | None = None,
) -> str | None:
    """Create a per-job condor submit file for resubmission and run condor_submit."""
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
    if shared_lib_names:
        for name in sorted(set(shared_lib_names)):
            transfer_files.append(os.path.join(main_dir, shared_dir_name, name))

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

class OpenDataMixin:
    """Parameters shared by all Open Data submission tasks."""

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
    files = luigi.IntParameter(
        default=30,
        description="Number of ROOT files per condor job (default: 30)",
    )
    partition = luigi.Parameter(
        default="file_group",
        description=(
            "Partitioning mode for splitting each dataset across jobs.  "
            "One of: 'file_group' (default – group files by count), "
            "'file' (one file per job), "
            "'entry_range' (split files by TTree entry ranges; requires uproot "
            "and --entries-per-job).  "
            "In 'file_group' mode the --files parameter controls the max files "
            "per job."
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
    exe = luigi.Parameter(
        description="Path to the compiled C++ executable to run on workers",
    )
    x509 = luigi.Parameter(
        default="",
        description="Path to the VOMS x509 proxy file (optional for Open Data)",
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
        description="Container setup command used by outer wrapper script (e.g. 'cmssw-el8')",
    )
    max_runtime = luigi.IntParameter(
        default=1200,
        description="Maximum condor job runtime in seconds (default: 1200)",
    )
    no_validate = luigi.BoolParameter(
        default=False,
        description="Skip submit-config validation",
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
        return self._resolve(self.x509) if self.x509 else None

    @property
    def _x509_loc(self):
        return "x509" if self.x509 else None

    @property
    def _shared_dir(self):
        return os.path.join(self._main_dir, "shared_inputs")

    @property
    def _root_setup_content(self):
        if not self.root_setup:
            return ""
        path = self._resolve(self.root_setup)
        if not path or not os.path.isfile(path):
            raise RuntimeError(
                f"root_setup must point to an existing file, got: {self.root_setup!r}"
            )
        with open(path) as fh:
            return fh.read().rstrip("\n")

    def _python_env_path(self) -> str | None:
        """Return the resolved path to the Python environment tarball, or None."""
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
# Task 1 – PrepareOpenDataSample
# ===========================================================================

class PrepareOpenDataSample(OpenDataMixin, law.LocalWorkflow):
    """
    LocalWorkflow: one branch per sample in the sampleConfig file.

    Each branch fetches CERN Open Data metadata for all record IDs defined
    in the sample config, filters the file list for its own sample, splits
    into groups of ``--files`` files, and creates per-job subdirectories
    under condorSub_{name}/samples/{sample_name}/job_{i}/.
    The branch outputs a JSON manifest listing the created job directories.
    """

    task_namespace = ""

    def create_branch_map(self):
        samples, _, _ = _parse_opendata_config(self._sample_config)
        if self.dataset:
            if self.dataset not in samples:
                raise ValueError(
                    f"Dataset {self.dataset!r} not found in sample config "
                    f"{self._sample_config!r}. "
                    f"Available datasets: {sorted(samples.keys())}"
                )
            return {0: self.dataset}
        return {i: key for i, key in enumerate(sorted(samples.keys()))}

    def output(self):
        return law.LocalFileTarget(
            os.path.join(
                self._main_dir, "branch_outputs", f"sample_{self.branch}.json"
            )
        )

    def run(self):
        if not self.no_validate and self.branch == 0:
            errors, warnings = validate_submit_config(
                self.submit_config, mode="opendata"
            )
            if warnings:
                for w in warnings:
                    self.publish_message(f"Config warning: {w}")
            if errors:
                raise RuntimeError(
                    "Submit config validation failed:\n" + "\n".join(errors)
                )

        sample_key  = self.branch_data
        config_dict = self._config_dict
        samples, recids, lumi = _parse_opendata_config(self._sample_config)
        sample = samples[sample_key]

        name        = sample["name"]
        xsec        = float(sample.get("xsec", 1.0))
        typ         = sample.get("type", "1")
        norm        = float(sample.get("norm", 1.0))
        kfac        = float(sample.get("kfac", 1.0))
        extra_scale = float(sample.get("extraScale", 1.0))

        # Build the das→name mapping for all samples (needed by _process_metadata)
        sample_names = {s.get("das", s["name"]): s["name"] for s in samples.values()}

        # Fetch file lists for all recids and collect URIs for this sample
        file_list: list[str] = []
        for recid in recids:
            try:
                partial = _process_metadata(recid, sample_names)
            except Exception as e:
                self.publish_message(
                    f"Warning: failed to fetch metadata for recid {recid}: {e}"
                )
                continue
            file_list.extend(partial.get(name, []))

        if not file_list:
            self.publish_message(f"No files found for sample {name}; skipping.")
            Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
            self.output().dump([], formatter="json")
            return

        copy_list = get_copy_file_list(config_dict)
        save_dir  = self._resolve(config_dict["saveDirectory"])
        try:
            Path(save_dir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Cannot create save directory {save_dir!r}: {e}")

        sample_base = os.path.join(self._main_dir, "samples", name)
        norm_scale  = str(extra_scale * kfac * lumi * xsec / norm)
        created_dirs: list[str] = []

        # Determine TTree name for entry-range queries
        tree_name = config_dict.get("treeList", "Events").split(",")[0].strip() or "Events"

        # Apply partitioning
        partitions = _make_partitions(
            file_list,
            mode=self.partition,
            files_per_job=self.files,
            entries_per_job=self.entries_per_job,
            tree_name=tree_name,
        )

        for sub_idx, partition in enumerate(partitions):
            job_dir = os.path.join(sample_base, f"job_{sub_idx}")
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            # Copy auxiliary config files --------------------------------
            for fpath in copy_list:
                src = self._resolve(fpath)
                dst = os.path.join(job_dir, os.path.basename(src))
                Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dst)

            # Build per-job submit config --------------------------------
            job_config = _normalize_config_paths(dict(config_dict))
            job_config["saveFile"] = os.path.join(save_dir, f"{name}_{sub_idx}.root")
            job_config["metaFile"] = os.path.join(save_dir, f"{name}_{sub_idx}_meta.root")
            job_config["fileList"] = partition["files"]
            job_config["batch"]    = "True"
            job_config["type"]     = typ
            job_config["sampleConfig"] = os.path.basename(self._sample_config)
            job_config["floatConfig"]  = "floats.txt"
            job_config["intConfig"]    = "ints.txt"

            # Write entry-range keys when in entry_range partition mode ----
            if partition["first_entry"] > 0 or partition["last_entry"] > 0:
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

            # Always stage-out
            job_config["__orig_saveFile"] = job_config["saveFile"]
            job_config["saveFile"]        = os.path.basename(job_config["saveFile"])
            job_config["__orig_metaFile"] = job_config["metaFile"]
            job_config["metaFile"]        = os.path.basename(job_config["metaFile"])

            # Write floats.txt / ints.txt --------------------------------
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

        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        self.output().dump(created_dirs, formatter="json")
        self.publish_message(f"Sample {name}: {len(created_dirs)} job dir(s) created.")


# ===========================================================================
# Task 2 – BuildOpenDataSubmission
# ===========================================================================

class BuildOpenDataSubmission(OpenDataMixin, law.Task):
    """
    Collects all per-sample job directories from PrepareOpenDataSample,
    creates sequential symlinks job_0 … job_{N-1} in the main dir,
    copies the shared executable / aux / x509 into shared_inputs/,
    and writes condor_runscript.sh + condor_submit.sub.
    """

    task_namespace = ""

    def requires(self):
        return PrepareOpenDataSample.req(self)

    def output(self):
        return {
            "submit":    law.LocalFileTarget(
                os.path.join(self._main_dir, "condor_submit.sub")
            ),
            "runscript": law.LocalFileTarget(
                os.path.join(self._main_dir, "condor_runscript.sh")
            ),
        }

    def run(self):
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

        # Collect all job dirs from branch outputs -----------------------
        job_dirs: list[str] = []
        for target in _iter_loadable_targets(self.input()):
            manifest = target.load(formatter="json")
            if isinstance(manifest, list):
                job_dirs.extend(manifest)

        if not job_dirs:
            raise RuntimeError("No job directories were created by PrepareOpenDataSample.")

        job_dirs.sort()
        n_jobs = len(job_dirs)
        self.publish_message(f"Total jobs to submit: {n_jobs}")

        # Shared inputs dir ----------------------------------------------
        shared_dir  = self._shared_dir
        exe_path    = self._exe_path
        exe_relpath = self._exe_relpath
        x509_src    = self._x509_src

        Path(shared_dir).mkdir(parents=True, exist_ok=True)
        _copy_file(exe_path, os.path.join(shared_dir, exe_relpath))

        local_shared_libs = _collect_local_shared_libs(exe_path, WORKSPACE)
        for staged_name, src_path in sorted(local_shared_libs.items()):
            _copy_file(src_path, os.path.join(shared_dir, staged_name))
        if local_shared_libs:
            self.publish_message(
                f"Staged {len(local_shared_libs)} local shared library file(s) into shared_inputs/."
            )

        aux_src    = self._resolve("aux")
        aux_exists = bool(aux_src and os.path.exists(aux_src))
        if aux_exists:
            _copy_dir(aux_src, os.path.join(shared_dir, "aux"))
        else:
            self.publish_message("Warning: 'aux' directory not found; skipping.")

        if x509_src:
            shutil.copy2(x509_src, os.path.join(shared_dir, self._x509_loc))

        # Handle Python environment tarball --------------------------------
        python_env_src = self._python_env_path()
        python_env_staged: str | None = None
        if python_env_src:
            tarball_name = os.path.basename(python_env_src)
            python_env_staged = os.path.join(shared_dir, tarball_name)
            _copy_file(python_env_src, python_env_staged)
            self.publish_message(f"Staged Python environment tarball: {tarball_name}")

        # Sequential job symlinks ----------------------------------------
        for i, job_dir_abs in enumerate(job_dirs):
            link_path = os.path.join(self._main_dir, f"job_{i}")
            if os.path.islink(link_path):
                os.unlink(link_path)
            elif os.path.exists(link_path):
                shutil.rmtree(link_path)
            os.symlink(job_dir_abs, link_path)

        # Write condor files ---------------------------------------------
        config_dict    = self._config_dict
        copy_list      = get_copy_file_list(config_dict)
        copy_basenames = sorted({os.path.basename(p) for p in copy_list})
        skip_transfer  = {"floats.txt", "ints.txt", "submit_config.txt"}
        extra_transfer_files = [
            os.path.join(self._main_dir, "job_$(Process)", name)
            for name in copy_basenames
            if name not in skip_transfer
        ]
        extra_transfer_files.extend(
            os.path.join(self._main_dir, "shared_inputs", name)
            for name in sorted(local_shared_libs.keys())
        )
        if python_env_staged:
            extra_transfer_files.append(python_env_staged)

        submit_path = write_submit_files(
            self._main_dir,
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
            include_aux=aux_exists,
            shared_dir_name="shared_inputs",
            eos_sched=False,
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
# Task 3 – SubmitOpenDataJobs
# ===========================================================================

class SubmitOpenDataJobs(OpenDataMixin, law.Task):
    """
    Submits the prepared condor job batch and records the cluster ID and job
    count in submitted.txt so that MonitorOpenDataJobs can pick them up.
    """

    task_namespace = ""

    def requires(self):
        return BuildOpenDataSubmission.req(self)

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

        cluster_id = ""
        for line in stdout.splitlines():
            if "cluster" in line.lower():
                parts = line.split()
                for i, p in enumerate(parts):
                    if p.lower().rstrip(".") == "cluster" and i + 1 < len(parts):
                        cluster_id = parts[i + 1].rstrip(".")
                        break

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
# Task 4 – MonitorOpenDataJobs
# ===========================================================================

class MonitorOpenDataJobs(OpenDataMixin, law.Task):
    """
    Blocking monitoring task that waits for all condor jobs to complete,
    verifies each expected output file on EOS, and resubmits held or failed
    jobs up to --max-retries times each.

    State is persisted to monitor_state.json so the task can be safely
    interrupted and restarted.
    """

    task_namespace = ""

    max_retries = luigi.IntParameter(
        default=3,
        description="Maximum resubmission attempts per failed job (default: 3)",
    )
    poll_interval = luigi.IntParameter(
        default=120,
        description="Seconds between condor_q polls (default: 120)",
    )

    def requires(self):
        return SubmitOpenDataJobs.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "all_outputs_verified.txt")
        )

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
        """Walk job_N symlinks and read each submit_config.txt."""
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

    def run(self):
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
                "Run BuildOpenDataSubmission first."
            )

        config_dict = self._config_dict
        aux_src     = self._resolve("aux")
        aux_exists  = bool(aux_src and os.path.exists(aux_src))
        shared_lib_names = (
            [
                name for name in os.listdir(self._shared_dir)
                if ".so" in name
                and os.path.isfile(os.path.join(self._shared_dir, name))
            ]
            if os.path.isdir(self._shared_dir)
            else []
        )

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

        while True:
            active_clusters: set[str] = set()
            for jstate in state["jobs"].values():
                if jstate["status"] in ("submitted", "running", "resubmitting"):
                    active_clusters.add(str(jstate["cluster"]))

            live_status: dict[tuple, int] = {}
            live_hold_reason: dict[tuple, str] = {}
            history_exit: dict[tuple, int] = {}
            for cid in active_clusters:
                for proc, ad in _condor_q_ads(cid).items():
                    if "JobStatus" in ad:
                        live_status[(cid, proc)] = ad["JobStatus"]
                    if ad.get("HoldReason"):
                        live_hold_reason[(cid, proc)] = str(ad["HoldReason"])
                for proc, exit_code in _condor_history_exit(cid).items():
                    history_exit[(cid, proc)] = exit_code

            for idx in sorted(job_info.keys()):
                key    = str(idx)
                jstate = state["jobs"][key]
                cid    = str(jstate["cluster"])
                proc   = jstate["proc"]
                status = jstate["status"]

                if status in ("done", "perm_fail"):
                    continue

                condor_js = live_status.get((cid, proc))
                exit_code = history_exit.get((cid, proc))

                if condor_js == 2:
                    jstate["status"] = "running"
                    continue

                if condor_js == 5:
                    hold_reason = live_hold_reason.get((cid, proc), "held without reported reason")
                    removed = _condor_rm_job(cid, proc)
                    if removed:
                        self.publish_message(f"Job {idx} held and removed: {hold_reason}")
                    else:
                        self.publish_message(f"Job {idx} held (could not confirm condor_rm): {hold_reason}")
                    self._try_resubmit(
                        idx, jstate, job_info, config_dict, aux_exists,
                        reason=f"held: {hold_reason}",
                    )
                    continue

                if condor_js is None and exit_code is not None:
                    if exit_code != 0:
                        error_tail = _read_failed_job_error(self._main_dir, cid, proc, idx)
                        if error_tail:
                            self.publish_message(
                                f"Job {idx} failed (exit {exit_code}). stderr tail:\n{error_tail}"
                            )
                        self._try_resubmit(
                            idx, jstate, job_info, config_dict, aux_exists,
                            reason=f"exit code {exit_code}",
                        )
                        continue

                if condor_js is None:
                    orig_save = job_info[idx]["orig_save"]
                    orig_meta = job_info[idx]["orig_meta"]
                    if (
                        (orig_save and _eos_file_exists(orig_save))
                        or (orig_meta and _eos_file_exists(orig_meta))
                    ):
                        jstate["status"] = "done"
                        self.publish_message(f"Job {idx}: output verified on EOS.")
                    elif exit_code == 0:
                        # Condor reports success but no EOS output found; treat as
                        # a stage-out failure and retry within max-retries logic.
                        self.publish_message(
                            f"Job {idx}: condor reports success (exit 0) but EOS "
                            "outputs are missing; attempting resubmission."
                        )
                        self._try_resubmit(
                            idx, jstate, job_info, config_dict, aux_exists,
                            reason="missing_output",
                        )
                        continue

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

        summary   = self._status_summary(state)
        done      = summary.get("done", 0)
        perm_fail = summary.get("perm_fail", 0)

        with self.output().open("w") as fh:
            fh.write(f"done={done}\n")
            fh.write(f"perm_fail={perm_fail}\n")
            fh.write(f"total={len(job_info)}\n")
            if perm_fail:
                failed = [k for k, j in state["jobs"].items() if j["status"] == "perm_fail"]
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
        """Attempt to resubmit job idx; update jstate in place."""
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

        shared_lib_names = (
            [
                name for name in os.listdir(self._shared_dir)
                if ".so" in name
                and os.path.isfile(os.path.join(self._shared_dir, name))
            ]
            if os.path.isdir(self._shared_dir)
            else []
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
            shared_lib_names=shared_lib_names,
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
# Task 5 – RunOpenDataJobs  (local / htcondor / dask executor)
# ===========================================================================

class RunOpenDataJobs(OpenDataMixin, law.LocalWorkflow, HTCondorWorkflow, DaskWorkflow):
    """
    Workflow task that executes prepared Open Data analysis jobs via a
    selectable executor.

    This task requires ``BuildOpenDataSubmission`` to have been run first (which
    creates the ``job_N`` symlinks and shared inputs).  Each branch corresponds
    to one prepared job directory.

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

        # Prepare and build
        law run PrepareOpenDataSample   --submit-config cfg/submit.txt --name myRun \\
            --exe build/myexe
        law run BuildOpenDataSubmission --submit-config cfg/submit.txt --name myRun \\
            --exe build/myexe

        # Execute with local workers
        law run RunOpenDataJobs --submit-config cfg/submit.txt --name myRun \\
            --exe build/myexe --workflow local

        # Execute on HTCondor (via LAW's htcondor workflow)
        law run RunOpenDataJobs --submit-config cfg/submit.txt --name myRun \\
            --exe build/myexe --workflow htcondor

        # Execute on a Dask cluster
        law run RunOpenDataJobs --submit-config cfg/submit.txt --name myRun \\
            --exe build/myexe --workflow dask --dask-scheduler tcp://scheduler:8786
    """

    task_namespace = ""

    # Parameters not meaningful at the individual-branch level
    exclude_params_branch = getattr(DaskWorkflow, "exclude_params_branch", set())

    # ------------------------------------------------------------------
    # Branch map
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
        reqs["build"] = BuildOpenDataSubmission.req(self)
        return reqs

    def requires(self):
        return BuildOpenDataSubmission.req(self)

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._main_dir, "job_outputs", f"job_{self.branch}.done")
        )

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(self):
        """Execute one analysis job (one branch)."""
        job_dir = self.branch_data
        exe_path = os.path.join(self._shared_dir, self._exe_relpath)

        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Executable not found at {exe_path!r}. "
                "Run BuildOpenDataSubmission first."
            )
        if not os.path.isdir(job_dir):
            raise RuntimeError(
                f"Job directory not found: {job_dir!r}. "
                "Run BuildOpenDataSubmission first."
            )

        result_text = _run_analysis_job(
            exe_path=exe_path,
            job_dir=job_dir,
            root_setup=self._root_setup_content,
            container_setup=self.container_setup or "",
        )

        self.publish_message(f"Branch {self.branch}: {result_text}")
        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with self.output().open("w") as fh:
            fh.write(f"status=done\n{result_text}\n")

    # ------------------------------------------------------------------
    # HTCondor workflow configuration
    # ------------------------------------------------------------------

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(
            os.path.join(self._main_dir, "law_htcondor")
        )

    def htcondor_log_directory(self):
        return law.LocalDirectoryTarget(
            os.path.join(self._main_dir, "law_htcondor_logs")
        )

    def htcondor_bootstrap_file(self):
        bootstrap_path = os.path.join(_HERE, "htcondor_bootstrap.sh")
        if os.path.isfile(bootstrap_path):
            from law.job.base import JobInputFile  # type: ignore
            return JobInputFile(bootstrap_path, copy=True, render_local=False)
        return None

    def htcondor_job_config(self, config, job_num, branches):
        if not hasattr(config, "custom_content") or config.custom_content is None:
            config.custom_content = []
        config.custom_content.extend([
            ("+RequestMemory", str(2000)),
            ("+MaxRuntime", str(self.max_runtime)),
            ("request_cpus", "1"),
            ("request_disk", "20000"),
        ])
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
        from law.util import DotDict  # type: ignore
        return DotDict(build=BuildOpenDataSubmission.req(self))

    # ------------------------------------------------------------------
    # Dask workflow configuration
    # ------------------------------------------------------------------

    def get_dask_work(self, branch_num: int, branch_data: str) -> tuple:
        """
        Return the (callable, args, kwargs) triple for Dask job submission.
        Workers need access to the shared filesystem where shared_inputs/ and
        job directories live.
        """
        exe_path = os.path.join(self._shared_dir, self._exe_relpath)
        return (
            _run_analysis_job,
            [exe_path, branch_data, self._root_setup_content, self.container_setup or ""],
            {},
        )
