"""Read, split, and regenerate the concrete inputs for Condor jobs."""

from __future__ import annotations

from native_config_io import job_config_path
from pathlib import Path
from typing import Optional
import math
import os
import re
import shutil
import subprocess
import time

from job_sizing import EntryCache
from submission_backend import condor_config_return_remap
from submission_backend import ensure_condor_machine_exclusion
from submission_backend import materialize_retry_submit
from submission_backend import read_config
from submission_backend import write_config


def _safe_dataset_submission_name(dataset_name: str) -> str:
    """Return a filesystem-safe directory name for a dataset submission bundle."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(dataset_name)).strip("._") or "dataset"


def _dataset_submission_dir(run_dir: str, dataset_name: str) -> str:
    """Return the dataset-specific concrete Condor submission directory."""
    return os.path.join(run_dir, "condor_submissions", _safe_dataset_submission_name(dataset_name))


def _group_prepared_skim_jobs_by_dataset(payload: list[dict]) -> dict[str, list[tuple[int, dict]]]:
    """Group prepared skim job entries by dataset while preserving order."""
    grouped: dict[str, list[tuple[int, dict]]] = {}
    for global_index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            continue
        dataset_name = str(entry.get("dataset_name", "")).strip()
        if not dataset_name:
            continue
        grouped.setdefault(dataset_name, []).append((global_index, entry))
    return grouped


def _discover_jobs_in_submission_dir(submission_dir: str) -> dict[int, dict[str, str]]:
    """Discover prepared concrete skim jobs under a dataset submission directory."""
    info: dict[int, dict[str, str]] = {}
    idx = 0
    while True:
        job_dir = os.path.join(submission_dir, f"job_{idx}")
        if not os.path.exists(job_dir):
            break
        cfg = read_config(str(job_config_path(job_dir)))
        save = cfg.get("__orig_saveFile", cfg.get("saveFile", ""))
        meta = cfg.get("__orig_metaFile", cfg.get("metaFile", ""))
        info[idx] = {
            "dir": os.path.realpath(job_dir),
            "save": save,
            "meta": meta,
            "tree_name": str(cfg.get("saveTree", "Events") or "Events"),
        }
        idx += 1
    return info


def _skim_job_file_list(job_dir: str) -> list[str]:
    """Return the concrete input files configured for a prepared skim job."""
    config_path = str(job_config_path(job_dir))
    if not os.path.isfile(config_path):
        return []
    config = read_config(config_path)
    raw_file_list = config.get("__orig_fileList", config.get("fileList", ""))
    return [item.strip() for item in str(raw_file_list or "").split(",") if item.strip()]


def _split_skim_file_list(file_list: list[str]) -> list[list[str]]:
    """Split a multi-file skim into two balanced file groups."""
    if len(file_list) <= 1:
        return [file_list]
    midpoint = (len(file_list) + 1) // 2
    return [file_list[:midpoint], file_list[midpoint:]]


def _split_skim_partition(job_dir: str, metadata_cache: str = "") -> list[dict]:
    """Bisect bounded entry ranges before considering whole-file bisection."""
    files = _skim_job_file_list(job_dir)
    config_path = str(job_config_path(job_dir))
    config = read_config(config_path) if os.path.isfile(config_path) else {}
    first, last = 0, 0
    if "firstEntry" in config and "lastEntry" in config:
        first, last = int(config["firstEntry"]), int(config["lastEntry"])
        if first < 0 or last < 0:
            raise ValueError("Retry entry bounds must be nonnegative")
        if (first or last) and last <= first:
            raise ValueError("Retry entry interval must be increasing")
    if last <= first and len(files) == 1:
        last = int(config.get("plannedInputEntries", 0))
        if not last and metadata_cache and os.path.isfile(metadata_cache):
            last = EntryCache(metadata_cache).get(files[0], str(config.get("treeList", "Events"))) or 0
        first = 0
    if last > first:
        if last - first > 1:
            middle = first + (last - first) // 2
            return [{"files": files, "first_entry": a, "last_entry": b}
                    for a, b in ((first, middle), (middle, last))]
        return [{"files": files, "first_entry": first, "last_entry": last}]
    return [{"files": part} for part in _split_skim_file_list(files)]


def _append_output_part_suffix(path: str, part_index: int) -> str:
    """Add a split-part suffix while preserving the output file extension."""
    if not path:
        return path
    base, extension = os.path.splitext(path)
    return f"{base}_part{part_index}{extension}"


def _prepare_split_skim_job(
    source_job_dir: str,
    submission_dir: str,
    job_index: int,
    retry_attempt: int,
    part_index: int,
    file_list: list[str],
    first_entry: Optional[int] = None,
    last_entry: Optional[int] = None,
) -> dict[str, str]:
    """Create an isolated per-file-group directory for a split retry."""
    resub_dir = os.path.join(submission_dir, "resubmissions")
    target_job_dir = os.path.join(
        resub_dir,
        f"job_{job_index}_retry{retry_attempt}_part{part_index}",
    )
    Path(resub_dir).mkdir(parents=True, exist_ok=True)
    config = read_config(job_config_path(source_job_dir))
    normalized_file_list = ",".join(file_list)
    config["fileList"] = normalized_file_list
    if "__orig_fileList" in config:
        config["__orig_fileList"] = normalized_file_list
    config.pop("plannedInputEntries", None)
    if first_entry is not None and last_entry is not None:
        config["firstEntry"] = str(first_entry)
        config["lastEntry"] = str(last_entry)
        config["plannedInputEntries"] = str(last_entry - first_entry)

    for key in ("saveFile", "metaFile", "__orig_saveFile", "__orig_metaFile"):
        if config.get(key):
            config[key] = _append_output_part_suffix(str(config[key]), part_index)

    from tempfile import TemporaryDirectory
    target = Path(target_job_dir)
    if not target.exists():
        with TemporaryDirectory(prefix=".split-", dir=resub_dir) as temporary:
            staged = Path(temporary) / "job"
            shutil.copytree(source_job_dir, staged,
                ignore=lambda directory, names: {"submit_config.txt", "submit_config.yaml", "submit_config.runtime.yaml"}
                if Path(directory) == Path(source_job_dir) else set())
            write_config(config, staged / "submit_config.yaml")
            try:
                staged.rename(target)
            except OSError:
                if not target.is_dir():
                    raise
    if read_config(job_config_path(target)) != config:
        raise RuntimeError(f"Existing retry config in {target} describes a different partition; use a new attempt")

    return {
        "dir": os.path.realpath(target_job_dir),
        "save": str(config.get("__orig_saveFile", config.get("saveFile", ""))),
        "meta": str(config.get("__orig_metaFile", config.get("metaFile", ""))),
        "tree_name": str(config.get("saveTree", "Events") or "Events"),
    }


def _condor_job_elapsed_runtime(ad: dict) -> float:
    """Return wall time for the current run, using the schedd's clock when available."""
    # RemoteWallClockTime is cumulative across executions and can lag while a
    # job is running. Prefer the current start date so a stuck first execution
    # is detected and an earlier eviction does not penalize a fresh execution.
    for attribute in ("JobCurrentStartDate", "EnteredCurrentStatus"):
        try:
            started = float(ad.get(attribute, 0) or 0)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(started) or started <= 0:
            continue
        try:
            now = float(ad.get("ServerTime", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            now = 0
        if not math.isfinite(now) or now <= 0:
            now = time.time()
        return max(0.0, now - started)

    # Compatibility with ads that do not expose a current-run timestamp.
    try:
        elapsed = float(ad.get("RemoteWallClockTime", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return elapsed if math.isfinite(elapsed) and elapsed > 0 else 0.0


def _condor_job_exceeded_runtime(
    ad: dict,
    fallback_max_runtime: Optional[int] = None,
    grace_factor: float = 1.5,
) -> bool:
    """Return whether a running Condor job exceeded its assigned runtime."""
    try:
        assigned_runtime = float(ad.get("MaxRuntime", 0) or 0)
        if assigned_runtime <= 0 and fallback_max_runtime:
            assigned_runtime = float(fallback_max_runtime)
        elapsed_runtime = _condor_job_elapsed_runtime(ad)
    except (TypeError, ValueError, OverflowError):
        return False

    return (
        math.isfinite(assigned_runtime)
        and assigned_runtime > 0
        and elapsed_runtime > assigned_runtime * grace_factor
    )


def _submit_single_skim_job(
    job_index: int,
    submission_dir: str,
    max_runtime: int,
    request_memory: int,
    extra_transfer_files: Optional[list[str]] = None,
    config_file: Optional[str] = None,
    source_job_dir: Optional[str] = None,
    log_label: Optional[str] = None,
) -> Optional[str]:
    """Submit a single prepared skim job for monitor-driven resubmission."""
    job_dir = source_job_dir or os.path.join(submission_dir, f"job_{job_index}")
    config_file = config_file or job_config_path(job_dir).name
    resub_dir = os.path.join(submission_dir, "resubmissions")
    Path(resub_dir).mkdir(parents=True, exist_ok=True)
    job_reference = os.path.relpath(job_dir, submission_dir).replace(os.sep, "/").rstrip("/") + "/"
    resub_label = log_label or f"resub_{job_index}"

    submit_template_path = os.path.join(submission_dir, "condor_submit.sub")
    log_base = os.path.join(submission_dir, "condor_logs")
    Path(log_base).mkdir(parents=True, exist_ok=True)

    template = Path(submit_template_path)
    existing = template.read_text() if template.is_file() else ""
    if existing:
        sub_content = existing
        sub_content = sub_content.replace("job_$(RDFJobIndex)/", job_reference).replace("$(RDFJobIndex)", str(job_index))
        sub_content = sub_content.replace("job_$(Process)/", job_reference)
        sub_content = "".join(
            line if line.lstrip().lower().startswith("transfer_output_remaps")
            else line.replace("$(Process)", str(job_index))
            for line in sub_content.splitlines(keepends=True)
        )
        sub_content = re.sub(r"(?mi)^[ \t]*queue\b.*$", "queue 1", sub_content)
        sub_content = re.sub(
            r"(?mi)^[ \t]*Output\s*=.*$",
            f"Output = {log_base}/{resub_label}_$(Cluster)_$(Process).stdout",
            sub_content,
        )
        sub_content = re.sub(
            r"(?mi)^[ \t]*Error\s*=.*$",
            f"Error = {log_base}/{resub_label}_$(Cluster)_$(Process).stderr",
            sub_content,
        )
        sub_content = re.sub(
            r"(?mi)^[ \t]*Log\s*=.*$",
            f"Log = {log_base}/{resub_label}.log",
            sub_content,
        )
        sub_content = re.sub(
            r"(?mi)^[ \t]*\+RequestMemory\s*=.*$",
            f"+RequestMemory={request_memory}",
            sub_content,
        )
        sub_content = re.sub(
            r"(?mi)^[ \t]*\+MaxRuntime\s*=.*$",
            f"+MaxRuntime={max_runtime}",
            sub_content,
        )

        extra_inputs = [path for path in (extra_transfer_files or []) if os.path.exists(path)]
        if extra_inputs:
            match = re.search(r"(?mi)^(?P<indent>[ \t]*transfer_input_files\s*=\s*)(?P<value>.*)$", sub_content)
            if match:
                existing = [entry.strip() for entry in match.group("value").split(",") if entry.strip()]
                for path in extra_inputs:
                    if path not in existing:
                        existing.append(path)
                replacement = f"{match.group('indent')}{','.join(existing)}"
                sub_content = (
                    sub_content[:match.start()] + replacement + sub_content[match.end():]
                )
    else:
        transfer_files = [
            os.path.join(job_dir, config_file),
        ]
        transfer_files.extend(extra_transfer_files or [])
        inner_script = os.path.join(submission_dir, "condor_runscript_inner.sh")
        if os.path.exists(inner_script):
            transfer_files.append(inner_script)

        transfer_str = ",".join(
            path for path in transfer_files if "$(" in path or os.path.exists(path)
        )

        sub_content = f"""universe = vanilla
Executable     = {submission_dir}/condor_runscript.sh
Should_Transfer_Files = YES
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
Notification = never
transfer_input_files = {transfer_str}
environment = "CONDOR_PROC={job_index} CONDOR_CLUSTER=$(Cluster)"
+RequestMemory={request_memory}
+RequestCpus=1
+RequestDisk=20000
+MaxRuntime={max_runtime}
max_transfer_input_mb = 10000
WhenToTransferOutput = On_Exit
transfer_output_files = {config_file}

        Output = {log_base}/{resub_label}_$(Cluster)_$(Process).stdout
Error  = {log_base}/{resub_label}_$(Cluster)_$(Process).stderr
Log    = {log_base}/{resub_label}.log
queue 1
"""

    if not re.search(r"(?mi)^[ \t]*transfer_output_remaps\s*=", sub_content):
        remap = condor_config_return_remap(log_base, config_file)
        sub_content = re.sub(r"(?mi)^[ \t]*queue\b", lambda match: f"{remap}\n{match[0]}", sub_content)
    sub_content = materialize_retry_submit(sub_content, submission_dir, os.path.join(job_dir, config_file))
    sub_content = ensure_condor_machine_exclusion(sub_content)
    submit_filename = (
        f"resub_job{job_index}.sub"
        if log_label is None
        else f"{resub_label}.sub"
    )
    sub_path = os.path.join(resub_dir, submit_filename)
    with open(sub_path, "w") as fh:
        fh.write(sub_content)

    try:
        result = subprocess.run(
            ["condor_submit", sub_path],
            capture_output=True,
            text=True,
            timeout=420,
        )
        if result.returncode != 0:
            return None
        for line in result.stdout.splitlines():
            if "cluster" not in line.lower():
                continue
            parts = line.split()
            for idx, part in enumerate(parts):
                if part.lower().rstrip(".") == "cluster" and idx + 1 < len(parts):
                    return parts[idx + 1].rstrip(".")
    except Exception:
        return None

    return None
