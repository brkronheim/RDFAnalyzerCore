"""
Shared utility functions for dataset ingestion tasks.

This module consolidates helpers that were previously duplicated across
``rucio_tasks.py``, ``opendata_tasks.py``, and ``analysis_tasks.py``.
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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from submission_backend import (
    condor_machine_requirements as _condor_machine_requirements,
)


# ---------------------------------------------------------------------------
# File utilities
# ---------------------------------------------------------------------------


def collect_local_shared_libs(exe_path: str, repo_root: str) -> dict[str, str]:
    """Return local shared-library deps and ROOT dictionary sidecars.

    Only libraries whose resolved real path starts with *repo_root* are
    returned, so system libraries (glibc, libstdc++, …) are silently ignored.
    Both the resolved basename and the declared soname are added as keys so
    that the loader can find the library under either name. ROOT PCM/rootmap
    files and referenced headers beside a library are staged with it.
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

        library_stem = real_name.split(".so", 1)[0]
        library_dir = os.path.dirname(real_path)
        rootmap_path = os.path.join(library_dir, f"{library_stem}.rootmap")
        for sidecar_name in (
            f"{library_stem}_rdict.pcm",
            f"{library_stem}.rootmap",
        ):
            sidecar_path = os.path.join(library_dir, sidecar_name)
            if os.path.isfile(sidecar_path):
                staged[sidecar_name] = sidecar_path

        if os.path.isfile(rootmap_path):
            try:
                pending_headers: list[str] = []
                with open(rootmap_path, encoding="utf-8") as rootmap_fh:
                    for rootmap_line in rootmap_fh:
                        key, _, header_name = rootmap_line.strip().partition(" ")
                        if key != "header" or not header_name:
                            continue
                        pending_headers.append(header_name)

                # ROOT's rootmap names only the dictionary header. Stage its
                # adjacent quoted-include dependencies too, since Cling parses
                # that header on the worker when it loads the PCM.
                seen_headers: set[str] = set()
                local_library_dir = os.path.realpath(library_dir)
                while pending_headers:
                    header_name = pending_headers.pop()
                    if os.path.dirname(header_name) not in ("", "."):
                        continue
                    header_path = os.path.join(library_dir, header_name)
                    if not os.path.isfile(header_path):
                        continue
                    header_path = os.path.realpath(header_path)
                    if (header_path in seen_headers
                            or os.path.dirname(header_path) != local_library_dir):
                        continue
                    seen_headers.add(header_path)
                    staged[os.path.basename(header_path)] = header_path

                    with open(header_path, encoding="utf-8") as header_fh:
                        for header_line in header_fh:
                            include_match = re.match(
                                r'^\s*#\s*include\s*"([^"/][^"]*)"',
                                header_line,
                            )
                            if include_match:
                                pending_headers.append(include_match.group(1))
            except OSError as err:
                print(f"Warning: failed to read rootmap '{rootmap_path}': {err}")

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
    """Stat an XRootD path, defaulting plain paths to eosuser.cern.ch."""
    if not eos_path:
        return False
    server, eos_path = _remote_output_location(eos_path, "root://eosuser.cern.ch/")
    if not eos_path:
        return False
    try:
        r = subprocess.run(
            ["xrdfs", server, "stat", eos_path],
            capture_output=True,
            timeout=timeout,
        )
        return r.returncode == 0
    except Exception:
        return False


def _remote_output_location(path: str, server: str) -> tuple[str, str]:
    clean = path.strip()
    if clean.startswith("root://"):
        parts = urlsplit(clean)
        if not parts.netloc:
            return server, ""
        return f"root://{parts.netloc}/", "/" + parts.path.lstrip("/")
    return server, clean


def _xrdfs_listing_info(line: str) -> tuple[str, dict] | None:
    """Parse long listings with owner/group fields or date-first formatting."""
    date = r"(?P<mtime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
    for fields in (rf"(?P<size>\d+)\s+{date}", rf"{date}\s+(?P<size>\d+)"):
        match = re.search(rf"\s{fields}\s+(?P<path>/.*)$", line)
        if match:
            mode = line.split(None, 1)[0]
            if not mode.startswith(("-", "d", "l")):
                return None
            return match["path"], dict(size=int(match["size"]),
                mtime=match["mtime"], mode=mode)
    return None


def eos_file_info_batch(
    eos_paths: list[str],
    server: str = "root://eosuser.cern.ch/",
    timeout: int = 60,
) -> dict[str, dict | None]:
    """Batch size/mtime checks by (server, directory), with four bounded workers.

    None means absent/unreadable. An empty dict means present but without a
    trustworthy signature: callers must not reuse an integrity receipt for it.
    Unknown listing formats fall back to an ordinary directory listing. Failed
    listings retain the existing per-file stat fallback without caching it.
    """
    result: dict[str, dict | None] = {}
    by_dir: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)

    for path in eos_paths:
        if not path:
            result[path] = None
            continue
        host, clean_path = _remote_output_location(path, server)
        if not clean_path:
            result[path] = None
            continue
        parent = os.path.dirname(clean_path)
        by_dir[(host, parent)].append((path, clean_path))

    def check_directory(entry):
        (host, parent_dir), items = entry
        found: dict[str, dict | None] = {}
        try:
            r = subprocess.run(
                ["xrdfs", host, "ls", "-l", parent_dir],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if r.returncode != 0:
                raise OSError("directory listing failed")
            lines = [line for line in r.stdout.splitlines() if line.strip()]
            parsed = [_xrdfs_listing_info(line) for line in lines]
            if any(record is None for record in parsed):
                plain = subprocess.run(["xrdfs", host, "ls", parent_dir],
                    capture_output=True, text=True, timeout=timeout)
                if plain.returncode != 0:
                    raise OSError("plain directory listing failed")
                existing = {line.strip(): {} for line in plain.stdout.splitlines()}
            else:
                existing = dict(parsed)
            for orig_path, clean_path in items:
                info = existing.get(clean_path)
                if info and info.get("mode", "-").startswith("d"):
                    info = None
                elif info and info.get("mode", "-").startswith("l"):
                    info = {}  # A link's metadata does not fingerprint its target.
                found[orig_path] = info
        except Exception:
            for orig_path, clean_path in items:
                url = host.rstrip("/") + "/" + clean_path
                found[orig_path] = {} if eos_file_exists(url, timeout) else None
        return found

    if by_dir:
        with ThreadPoolExecutor(max_workers=min(4, len(by_dir))) as pool:
            for found in pool.map(check_directory, by_dir.items()):
                result.update(found)
    return result


def eos_files_exist_batch(
    eos_paths: list[str],
    server: str = "root://eosuser.cern.ch/",
    timeout: int = 60,
) -> dict[str, bool]:
    """Check output paths using the shared directory-level XRootD batch."""
    return {path: info is not None
            for path, info in eos_file_info_batch(eos_paths, server, timeout).items()}


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


def condor_event_log_exit(
    main_dir: str, cluster_id: str, proc_id: int, job_idx: int,
    job_dir: str = "", log_cache: dict | None = None,
) -> tuple[int, str] | None:
    """Read exact-job normal termination when scheduler history is unavailable.

    Only generated event-log paths are considered, never stdout or mere output
    presence. The latest matching event must be a complete termination record.
    Callers must exclude live jobs and still validate all expected outputs.
    Cache parsed shared cluster logs by file identity/mtime/size, not per job.
    """
    if not str(cluster_id).isdigit() or int(proc_id) < 0:
        return None
    base = Path(main_dir) / "condor_logs"
    candidates = [base / f"log_{cluster_id}.log", base / f"resub_{job_idx}.log"]
    match = re.fullmatch(r"job_(\d+_retry\d+_part\d+)", Path(job_dir).name)
    if match:
        candidates.append(base / f"resub_{match.group(1)}.log")
    header = re.compile(r"^(\d{3}) \((\d+)\.(\d+)\.(\d+)\)")
    target = (str(int(cluster_id)), int(proc_id))
    evidence = []
    for path in dict.fromkeys(candidates):
        try:
            stat = path.stat()
            signature = (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size)
            cached = (log_cache or {}).get(str(path))
            if cached is not None and cached[0] == signature:
                exits = cached[1]
            else:
                exits = {}
                current = None
                code = None
                body = []
                with path.open(errors="replace") as stream:
                    for line in stream:
                        event = header.match(line)
                        if event:
                            current = (str(int(event[2])), int(event[3]))
                            # A newer/incomplete record invalidates prior success.
                            exits.pop(current, None)
                            code = event[1]
                            body = []
                        elif line.strip() == "...":
                            if current is not None and code == "005":
                                normal = re.search(
                                    r"(?m)^\s*\(1\) Normal termination \(return value (\d+)\)\s*$",
                                    "".join(body),
                                )
                                if normal:
                                    exits[current] = int(normal[1])
                            current = None
                            body = []
                        elif current is not None:
                            body.append(line)
                if log_cache is not None:
                    log_cache[str(path)] = (signature, exits)
            if target in exits:
                evidence.append((exits[target], str(path)))
        except OSError:
            continue
    if not evidence or len({item[0] for item in evidence}) != 1:
        return None
    return evidence[0]


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
        f"Requirements = {_condor_machine_requirements()}\n"
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
