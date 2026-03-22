"""
Shared utility functions for dataset ingestion tasks.

This module consolidates helpers that were previously duplicated across
``nano_tasks.py``, ``opendata_tasks.py``, and ``analysis_tasks.py``.
All ingestion task modules should import from here instead of defining
their own copies.
"""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path


# ---------------------------------------------------------------------------
# File utilities
# ---------------------------------------------------------------------------


def collect_local_shared_libs(exe_path: str, repo_root: str) -> dict[str, str]:
    """Return ``{staged_name: real_path}`` for shared-library deps of *exe_path*.

    Only libraries whose resolved real path starts with *repo_root* are
    returned, so system libraries (glibc, libstdc++, …) are silently ignored.
    Both the resolved basename and the declared soname are added as keys so
    that the loader can find the library under either name.
    """
    staged: dict[str, str] = {}
    try:
        ldd_output = subprocess.check_output(
            ["ldd", exe_path], text=True, stderr=subprocess.STDOUT
        )
    except Exception as err:
        print(
            f"Warning: failed to inspect shared-library deps via ldd "
            f"for '{exe_path}': {err}"
        )
        return staged

    repo_prefix = os.path.abspath(repo_root)
    if not repo_prefix.endswith(os.path.sep):
        repo_prefix += os.path.sep

    for line in ldd_output.splitlines():
        line = line.strip()
        if not line or "=> not found" in line:
            continue
        soname = resolved = None
        m = re.match(r"^(\S+)\s*=>\s*(\S+)", line)
        if m:
            soname = os.path.basename(m.group(1))
            resolved = m.group(2)
        else:
            m = re.match(r"^(\/\S+)\s+\(", line)
            if m:
                resolved = m.group(1)
                soname = os.path.basename(resolved)
        if not resolved or not os.path.isabs(resolved):
            continue
        real_path = os.path.realpath(resolved)
        if not os.path.exists(real_path) or not real_path.startswith(repo_prefix):
            continue
        real_name = os.path.basename(real_path)
        if ".so" not in real_name:
            continue
        staged[real_name] = real_path
        if soname and ".so" in soname:
            staged[soname] = real_path

    return staged


def copy_file(src: str, dst: str) -> None:
    """Copy *src* to *dst*, skipping if sizes and mtimes already match."""
    if os.path.exists(dst):
        if (
            os.path.getsize(dst) == os.path.getsize(src)
            and int(os.path.getmtime(dst)) == int(os.path.getmtime(src))
        ):
            return
        os.remove(dst)
    shutil.copy2(src, dst)


def copy_dir(src: str, dst: str) -> None:
    """Copy directory tree *src* → *dst*, merging into any existing destination."""
    shutil.copytree(src, dst, dirs_exist_ok=True)


def append_unique_lines(file_path: str, lines: list[str]) -> None:
    """Ensure each entry in *lines* appears exactly once in *file_path*.

    Pre-existing duplicate lines in the file are also de-duplicated (first
    occurrence wins).  The file is written back atomically after merging.
    """
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


def normalize_config_paths(config_dict: dict) -> dict:
    """Strip directory components from staged config-file path values.

    Config files are copied flat into ``shared_inputs/`` before being
    transferred to condor workers, so job-local configs should reference
    only the basename of staged ``.txt`` / ``.yaml`` / ``.yml`` files.
    """
    normalized = dict(config_dict)
    for key, value in config_dict.items():
        if (
            isinstance(value, str)
            and os.path.sep in value
            and value.lower().endswith((".txt", ".yaml", ".yml"))
        ):
            normalized[key] = os.path.basename(value)
    return normalized


def rechunk_urls(urls: list[str], max_files: int) -> dict[int, str]:
    """Split a flat URL list into groups of at most *max_files* entries.

    Returns ``{group_index: 'url1,url2,...'}`` suitable for feeding into
    job-directory preparation routines.
    """
    groups: dict[int, str] = {}
    for i, url in enumerate(urls):
        g = i // max_files
        if g in groups:
            groups[g] += "," + url
        else:
            groups[g] = url
    return groups


# ---------------------------------------------------------------------------
# EOS / XRootD helpers
# ---------------------------------------------------------------------------


def eos_file_exists(eos_path: str, timeout: int = 30) -> bool:
    """Return ``True`` if *eos_path* can be stat'd on eosuser.cern.ch via xrdfs."""
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


def eos_files_exist_batch(
    eos_paths: list[str],
    server: str = "root://eosuser.cern.ch/",
    timeout: int = 60,
) -> dict[str, bool]:
    """Check existence of multiple EOS files using directory-level ``xrdfs ls``.

    Files are grouped by their parent directory so that a single ``xrdfs ls``
    call is made per parent directory, reducing round-trips to the XRootD
    server significantly.

    Parameters
    ----------
    eos_paths:
        List of EOS paths to check.  Each path may be in ``root://...`` URL
        form or a plain ``/eos/...`` path.
    server:
        XRootD server URL used for ``xrdfs ls`` calls.
    timeout:
        Timeout in seconds for each ``xrdfs ls`` invocation.

    Returns
    -------
    dict[str, bool]
        Mapping from each original path string to ``True`` / ``False``.
    """
    result: dict[str, bool] = {}
    by_dir: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for path in eos_paths:
        if not path:
            result[path] = False
            continue
        clean = path.strip()
        if clean.startswith("root://"):
            parts = clean.split("/", 3)
            clean_path = "/" + parts[3].lstrip("/") if len(parts) >= 4 else ""
        else:
            clean_path = clean
        if not clean_path:
            result[path] = False
            continue
        parent = os.path.dirname(clean_path)
        by_dir[parent].append((path, clean_path))

    for parent_dir, items in by_dir.items():
        try:
            r = subprocess.run(
                ["xrdfs", server, "ls", parent_dir],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if r.returncode != 0:
                for orig_path, _ in items:
                    result[orig_path] = eos_file_exists(orig_path, timeout)
                continue
            existing = set(r.stdout.splitlines())
            for orig_path, clean_path in items:
                result[orig_path] = clean_path in existing
        except Exception:
            for orig_path, _ in items:
                result[orig_path] = eos_file_exists(orig_path, timeout)

    return result


# ---------------------------------------------------------------------------
# HTCondor utilities
# ---------------------------------------------------------------------------


def condor_q_ads(cluster_id: str) -> dict[int, dict]:
    """Query condor_q and return raw classads keyed by ProcId."""
    try:
        r = subprocess.run(
            ["condor_q", str(cluster_id), "-json"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return {}
        ads = json.loads(r.stdout)
        return {ad["ProcId"]: ad for ad in ads if "ProcId" in ad}
    except Exception as e:
        print(f"Warning: condor_q failed for cluster {cluster_id}: {e}")
        return {}


def condor_q_cluster(cluster_id: str) -> dict[int, int]:
    """Return ``{proc_id: JobStatus}`` for jobs still in the condor queue.

    Status codes: 1=Idle, 2=Running, 5=Held.
    Returns an empty dict when the cluster has left the queue or condor_q fails.
    """
    ads = condor_q_ads(cluster_id)
    return {
        proc: ad.get("JobStatus")
        for proc, ad in ads.items()
        if "JobStatus" in ad
    }


def condor_rm_job(cluster_id: str, proc_id: int, timeout: int = 30) -> bool:
    """Remove a specific held/failed condor job from the queue."""
    try:
        r = subprocess.run(
            ["condor_rm", f"{cluster_id}.{proc_id}"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return r.returncode == 0
    except Exception:
        return False


def condor_history_exit(cluster_id: str) -> dict[int, int]:
    """Return ``{proc_id: ExitCode}`` for jobs that have left the queue.

    An ``ExitCode`` of 0 means the job completed successfully; any non-zero
    value indicates failure.
    """
    try:
        r = subprocess.run(
            ["condor_history", str(cluster_id), "-json"],
            capture_output=True,
            text=True,
            timeout=120,
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


def read_failed_job_error(
    main_dir: str, cluster_id: str, proc_id: int, job_idx: int
) -> str:
    """Return a short stderr tail for a failed job, if a log file is available."""
    candidates = [
        os.path.join(main_dir, "condor_logs", f"log_{cluster_id}_{proc_id}.stderr"),
    ]
    candidates.extend(
        sorted(
            glob.glob(
                os.path.join(
                    main_dir,
                    "condor_logs",
                    f"resub_{job_idx}_{cluster_id}_{proc_id}.stderr",
                )
            )
        )
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


def submit_single_job(
    job_index: int,
    main_dir: str,
    exe_relpath: str,
    x509loc: str | None,
    stage_inputs: bool,
    max_runtime: int,
    request_memory: int,
    shared_dir_name: str,
    config_dict: dict,
    include_aux: bool = False,
    shared_lib_names: list[str] | None = None,
) -> str | None:
    """Create a per-job condor submit file for resubmission and run condor_submit.

    Returns the new cluster ID string on success, or ``None`` on failure.

    The generated submit file transfers the three per-job config files
    (``submit_config.txt``, ``floats.txt``, ``ints.txt``) together with the
    whole ``shared_inputs/`` directory so that the worker node has access to
    the executable and all supporting files without requiring a shared
    filesystem.
    """
    job_dir = os.path.join(main_dir, f"job_{job_index}")
    resub_dir = os.path.join(main_dir, "resubmissions")
    Path(resub_dir).mkdir(parents=True, exist_ok=True)

    transfer_files = [
        os.path.join(job_dir, "submit_config.txt"),
        os.path.join(job_dir, "floats.txt"),
        os.path.join(job_dir, "ints.txt"),
        os.path.join(main_dir, shared_dir_name),
    ]

    inner_script = os.path.join(main_dir, "condor_runscript_inner.sh")
    if os.path.exists(inner_script):
        transfer_files.append(inner_script)

    filtered = [p for p in transfer_files if "$(" in p or os.path.exists(p)]
    transfer_str = ",".join(filtered)
    log_base = os.path.join(main_dir, "condor_logs")
    Path(log_base).mkdir(parents=True, exist_ok=True)

    sub_content = (
        f"universe = vanilla\n"
        f"Executable     = {main_dir}/condor_runscript.sh\n"
        f"Should_Transfer_Files = YES\n"
        f"on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)\n"
        f"Notification = never\n"
        f"transfer_input_files = {transfer_str}\n"
        f"environment = CONDOR_PROC=$(Process) CONDOR_CLUSTER=$(Cluster)\n"
        f"+RequestMemory={request_memory}\n"
        f"+RequestCpus=1\n"
        f"+RequestDisk=20000\n"
        f"+MaxRuntime={max_runtime}\n"
        f"max_transfer_input_mb = 10000\n"
        f"WhenToTransferOutput = On_Exit\n"
        f"transfer_output_files = submit_config.txt\n"
        f"\n"
        f"Output = {log_base}/resub_{job_index}_$(Cluster)_$(Process).stdout\n"
        f"Error  = {log_base}/resub_{job_index}_$(Cluster)_$(Process).stderr\n"
        f"Log    = {log_base}/resub_{job_index}.log\n"
        f"queue 1\n"
    )
    sub_path = os.path.join(resub_dir, f"resub_job{job_index}.sub")
    with open(sub_path, "w") as fh:
        fh.write(sub_content)

    try:
        r = subprocess.run(
            ["condor_submit", sub_path],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if r.returncode != 0:
            print(f"Error resubmitting job {job_index}: {r.stderr.strip()}")
            return None
        for line in r.stdout.splitlines():
            if "cluster" in line.lower():
                parts = line.split()
                for k, part in enumerate(parts):
                    if (
                        part.lower().rstrip(".") == "cluster"
                        and k + 1 < len(parts)
                    ):
                        return parts[k + 1].rstrip(".")
    except Exception as e:
        print(f"Error resubmitting job {job_index}: {e}")
    return None


# ---------------------------------------------------------------------------
# Backward-compatible private-name aliases (used by existing callers)
# ---------------------------------------------------------------------------

_collect_local_shared_libs = collect_local_shared_libs
_copy_file = copy_file
_copy_dir = copy_dir
_append_unique_lines = append_unique_lines
_normalize_config_paths = normalize_config_paths
_rechunk_urls = rechunk_urls
_eos_file_exists = eos_file_exists
_eos_files_exist_batch = eos_files_exist_batch
_condor_q_ads = condor_q_ads
_condor_q_cluster = condor_q_cluster
_condor_rm_job = condor_rm_job
_condor_history_exit = condor_history_exit
_read_failed_job_error = read_failed_job_error
_submit_single_job = submit_single_job
