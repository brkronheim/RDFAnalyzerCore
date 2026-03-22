"""
Law tasks for running analysis jobs locally and deriving MC stitching weights.

Provides three tasks that extend the typical analysis workflow:

  SkimTask  (LocalWorkflow)
      → Runs the analysis executable locally to produce skimmed ROOT files.
      → One branch per dataset entry from a dataset manifest YAML.
      → Output: per-dataset ``.done`` marker files in
        ``skimRun_{name}/job_outputs/``.  Actual skim/meta ROOT files are
        written to ``skimRun_{name}/outputs/{dataset_name}/``.

  HistFillTask  (LocalWorkflow)
      → Runs the analysis executable locally to fill analysis histograms.
      → One branch per dataset entry from a dataset manifest YAML.
      → Optionally depends on SkimTask: when ``--skim-name`` is set the task
        requires the matching SkimTask to have completed first, and the skim
        output directory is automatically wired as the input ``fileList`` for
        each dataset job.
      → Output: per-dataset ``.done`` marker files in
        ``histRun_{name}/job_outputs/``.

  StitchingDerivationTask  (Task)
      → Derives binwise MC stitching scale factors from counter histograms
        written by the framework's ``CounterService``.
      → For each group of stitched MC samples the task reads
        ``counter_intWeightSignSum_{sample}`` histograms from the merged meta
        ROOT files using **uproot** (preferred), with a PyROOT fallback for
        environments where uproot is unavailable.
      → Computes the net signed counts ``C_n(k) = P_n(k) − N_n(k)`` and the
        per-bin scale factor

            b_n(k) = C_n(k) / Σ_m C_m(k)

        This is the exact formula described in the stitching derivation: each
        sample's scale factor equals its share of the total signed counts in
        that stitch bin, ensuring correct normalisation across overlapping
        phase-space samples (e.g. HT-binned W+jets).
      → Reads group/sample configuration from a YAML file (``--stitch-config``).
      → Output: ``stitchWeights_{name}/stitch_weights.json`` — a
        **correctionlib** ``CorrectionSet`` JSON with one ``Correction`` per
        group.  Each correction is evaluated as
        ``cset[group_name].evaluate(sample_name, stitch_id)`` → scale factor.

Mathematical background
-----------------------
For a set of MC samples that overlap in a region of phase space, sample *n*
contributes P_n positive-weight events and N_n negative-weight events, each of
magnitude a_n, so its net signed count is C_n = P_n - N_n.  Requiring that the
combined normalised distribution equals the true cross-section leads to the
per-sample scale factor

    b_n = C_n / Σ_m C_m

For the integer-binned (stitch_id) case the same formula is applied
independently in each histogram bin k:

    b_n(k) = C_n(k) / Σ_m C_m(k)

where k is the integer stitch_id value stored per event by
``counterIntWeightBranch`` and counted by ``CounterService``.

Usage
-----
::

    source law/env.sh

    # Run skim pass (one job per dataset entry in the manifest)
    law run SkimTask \\
        --exe build/analyses/myAnalysis/myanalysis \\
        --submit-config analyses/myAnalysis/cfg/skim_config.txt \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --name mySkimRun

    # Fill histograms (requires SkimTask outputs when --skim-name is set)
    law run HistFillTask \\
        --exe build/analyses/myAnalysis/myanalysis \\
        --submit-config analyses/myAnalysis/cfg/hist_config.txt \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --name myHistRun \\
        --skim-name mySkimRun

    # Derive binwise stitching scale factors → correctionlib JSON
    law run StitchingDerivationTask \\
        --stitch-config analyses/myAnalysis/cfg/stitch_config.yaml \\
        --name myStitch

Stitch config YAML format
--------------------------
::

    groups:
      wjets_ht:
        meta_files:
          wjets_ht_0: /path/to/merged/wjets_ht_0_meta.root
          wjets_ht_1: /path/to/merged/wjets_ht_1_meta.root
          wjets_ht_2: /path/to/merged/wjets_ht_2_meta.root

Each key under ``meta_files`` is the ``sampleName`` used by CounterService
when booking the ``counter_intWeightSignSum_{sampleName}`` histogram.  This
matches the ``sample`` / ``type`` key in the analysis submit_config.txt.

Correctionlib output format
----------------------------
One ``Correction`` per stitching group is written to the output
``CorrectionSet``.  Each correction has two inputs:

* ``sample_name`` (string) – the sample identifier matching the keys under
  ``meta_files`` in the stitch config.
* ``stitch_id``   (int)    – the integer bin produced by
  ``counterIntWeightBranch`` at event level.

The correction returns the stitching scale factor ``b_n(k)``.  Usage from
C++ (via CorrectionManager) or Python::

    # Python
    cset = correctionlib.CorrectionSet.from_file("stitch_weights.json")
    sf = cset["wjets_ht"].evaluate("wjets_ht_0", stitch_id)
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Optional, cast

import luigi  # type: ignore
import law  # type: ignore

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402
from dataset_manifest import DatasetManifest, DatasetEntry  # noqa: E402
from submission_backend import (  # noqa: E402
    ensure_xrootd_redirector,
    get_copy_file_list,
    read_config,
    write_submit_files,
)
from workflow_executors import DaskWorkflow, HTCondorWorkflow, _run_analysis_job  # noqa: E402
from partition_utils import _make_partitions  # noqa: E402
from output_schema import (  # noqa: E402
    ArtifactResolutionStatus,
    DatasetManifestProvenance,
    IntermediateArtifactSchema,
    OutputManifest,
    ProvenanceRecord,
    SkimSchema,
    check_cache_validity,
    write_cache_sidecar,
)

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))

import re as _re  # noqa: E402  - used by _collect_skim_shared_libs


# ===========================================================================
# Shared-library staging helper (mirrors _collect_local_shared_libs in
# nano_tasks.py but kept here so analysis_tasks has no circular import)
# ===========================================================================


def _collect_skim_shared_libs(exe_path: str, repo_root: str) -> dict:
    """Return {staged_name: real_path} for local shared-library deps of *exe_path*.

    Only libraries whose resolved path starts with *repo_root* are included,
    so system libraries (glibc, libstdc++, …) are ignored.
    """
    staged: dict = {}
    try:
        ldd_output = subprocess.check_output(
            ["ldd", exe_path], text=True, stderr=subprocess.STDOUT
        )
    except Exception:
        return staged

    repo_prefix = os.path.abspath(repo_root)
    if not repo_prefix.endswith(os.path.sep):
        repo_prefix += os.path.sep

    for line in ldd_output.splitlines():
        line = line.strip()
        if not line or "=> not found" in line:
            continue
        soname = resolved = None
        m = _re.match(r"^(\S+)\s*=>\s*(\S+)", line)
        if m:
            soname = os.path.basename(m.group(1))
            resolved = m.group(2)
        else:
            m = _re.match(r"^(\/\S+)\s+\(", line)
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


def _find_dataset_entry(
    dataset_manifest_path: str, name: str
) -> Optional[DatasetEntry]:
    """Return the :class:`~dataset_manifest.DatasetEntry` named *name*, or ``None``."""
    if not dataset_manifest_path or not os.path.isfile(dataset_manifest_path):
        return None
    try:
        manifest = DatasetManifest.load(dataset_manifest_path)
        for entry in manifest.datasets:
            if entry.name == name:
                return entry
    except Exception:
        pass
    return None


def _resolve_analysis_config_path(
    template_config_path: str,
    path_value: str,
) -> str:
    """Resolve config references relative to the analysis cfg or its parent.

    Analysis submit-config templates often live under ``analysis/cfg/`` while
    referenced payload files are written as ``cfg/foo.yaml`` relative to the
    analysis root.  Resolve both layouts consistently.
    """
    if not path_value:
        return path_value
    if os.path.isabs(path_value):
        return path_value

    template_dir = os.path.dirname(os.path.abspath(template_config_path))
    analysis_dir = os.path.abspath(os.path.join(template_dir, ".."))
    candidates = [
        os.path.abspath(path_value),
        os.path.join(template_dir, path_value),
        os.path.join(analysis_dir, path_value),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

    return os.path.abspath(os.path.join(template_dir, path_value))


def _copy_analysis_config_files(
    template_config_path: str,
    config_dict: dict[str, str],
    destination_dir: str,
) -> set[str]:
    """Copy referenced config payload files into *destination_dir*.

    Returns the basenames that were staged locally so callers can rewrite
    config values to point at the copied files.
    """
    copied: set[str] = set()
    for path_value in get_copy_file_list(config_dict):
        src = _resolve_analysis_config_path(template_config_path, path_value)
        if src and os.path.isfile(src):
            dst = os.path.join(destination_dir, os.path.basename(src))
            if not os.path.exists(dst):
                shutil.copy2(src, dst)
            _rewrite_staged_analysis_config(
                template_config_path=template_config_path,
                source_config_path=src,
                staged_config_path=dst,
            )
            copied.add(os.path.basename(src))
    return copied


def _analysis_aux_dir(template_config_path: str) -> str:
    """Return the analysis-local aux directory for a submit-config template."""
    return _resolve_analysis_config_path(template_config_path, "aux")


def _copy_analysis_aux_dir(template_config_path: str, destination_dir: str) -> bool:
    """Copy the analysis aux directory into *destination_dir*/aux when present."""
    aux_src = _analysis_aux_dir(template_config_path)
    if not aux_src or not os.path.isdir(aux_src):
        return False

    aux_dst = os.path.join(destination_dir, "aux")
    Path(aux_dst).mkdir(parents=True, exist_ok=True)
    for aux_entry in sorted(Path(aux_src).iterdir()):
        if aux_entry.is_file():
            shutil.copy2(str(aux_entry), os.path.join(aux_dst, aux_entry.name))
    return True


def _bundle_analysis_aux_dir(template_config_path: str, destination_dir: str) -> Optional[str]:
    """Create a tar.gz archive containing the analysis aux/ directory."""
    aux_src = _analysis_aux_dir(template_config_path)
    if not aux_src or not os.path.isdir(aux_src):
        return None

    archive_path = os.path.join(destination_dir, "aux_bundle.tar.gz")
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(aux_src, arcname="aux")
    return archive_path


def _bundle_analysis_config_files(
    template_config_path: str,
    config_dict: dict[str, str],
    destination_dir: str,
) -> Optional[str]:
    """Create a tar.gz archive containing shared cfg/ payload files."""
    payload_paths: list[str] = []
    for path_value in get_copy_file_list(config_dict):
        src = _resolve_analysis_config_path(template_config_path, path_value)
        if src and os.path.isfile(src):
            payload_paths.append(src)
    if not payload_paths:
        return None

    archive_path = os.path.join(destination_dir, "cfg_bundle.tar.gz")
    with tempfile.TemporaryDirectory(prefix="skim_cfg_bundle_") as tmpdir:
        cfg_stage_dir = os.path.join(tmpdir, "cfg")
        Path(cfg_stage_dir).mkdir(parents=True, exist_ok=True)
        for src in payload_paths:
            staged_path = os.path.join(cfg_stage_dir, os.path.basename(src))
            shutil.copy2(src, staged_path)
            _rewrite_staged_analysis_config(
                template_config_path=template_config_path,
                source_config_path=src,
                staged_config_path=staged_path,
            )
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(cfg_stage_dir, arcname="cfg")
    return archive_path


def _normalize_shared_config_references(
    template_config_path: str,
    job_config: dict[str, str],
) -> None:
    """Rewrite referenced payload files to the runtime cfg/ layout."""
    staged_payloads = {
        os.path.basename(
            _resolve_analysis_config_path(template_config_path, path_value)
        )
        for path_value in get_copy_file_list(job_config)
        if path_value
    }
    for key, val in list(job_config.items()):
        if key.startswith("__"):
            continue
        if isinstance(val, str) and (
            val.endswith(".txt") or val.endswith(".yaml") or val.endswith(".yml")
        ):
            basename = os.path.basename(val)
            if basename in staged_payloads:
                job_config[key] = os.path.join("cfg", basename)


def _resolve_runtime_transfer_path(path_value: str) -> str:
    """Prefer a transferred HTCondor copy in the current directory when present."""
    local_candidate = os.path.join(os.getcwd(), os.path.basename(path_value))
    if os.path.exists(local_candidate):
        return local_candidate
    return path_value


def _copy_tree_contents(src_dir: str, dst_dir: str) -> None:
    """Copy all files/directories under src_dir into dst_dir."""
    if not os.path.isdir(src_dir):
        return
    Path(dst_dir).mkdir(parents=True, exist_ok=True)
    for entry in sorted(Path(src_dir).iterdir()):
        dst_path = os.path.join(dst_dir, entry.name)
        if entry.is_dir():
            shutil.copytree(str(entry), dst_path, dirs_exist_ok=True)
        elif entry.is_file():
            shutil.copy2(str(entry), dst_path)


def _extract_tar_archive(archive_path: str, destination_dir: str) -> None:
    """Extract a tar archive using the safest available tarfile mode."""
    with tarfile.open(archive_path, "r:gz") as archive:
        try:
            archive.extractall(destination_dir, filter="data")
        except TypeError:
            archive.extractall(destination_dir)


def _materialize_file_source_runtime_job(
    *,
    shared_dir: str,
    source_job_dir: str,
    exe_relpath: str,
) -> tuple[str, str]:
    """Create a worker-local runtime directory with cfg/ and aux/ materialized."""
    runtime_shared_dir = _resolve_runtime_transfer_path(shared_dir)
    runtime_job_dir = _resolve_runtime_transfer_path(source_job_dir)
    if not os.path.isdir(runtime_shared_dir):
        raise RuntimeError(
            f"Shared inputs directory not available at runtime: {runtime_shared_dir!r}."
        )
    if not os.path.isdir(runtime_job_dir):
        raise RuntimeError(
            f"Per-job directory not available at runtime: {runtime_job_dir!r}."
        )

    sandbox_dir = tempfile.mkdtemp(prefix="skim_runtime_")

    cfg_bundle = os.path.join(runtime_shared_dir, "cfg_bundle.tar.gz")
    if os.path.isfile(cfg_bundle):
        _extract_tar_archive(cfg_bundle, sandbox_dir)

    aux_bundle = os.path.join(runtime_shared_dir, "aux_bundle.tar.gz")
    if os.path.isfile(aux_bundle):
        _extract_tar_archive(aux_bundle, sandbox_dir)

    submit_config_src = os.path.join(runtime_job_dir, "submit_config.txt")
    if not os.path.isfile(submit_config_src):
        raise RuntimeError(
            f"Per-job submit config not found at runtime: {submit_config_src!r}."
        )
    shutil.copy2(submit_config_src, os.path.join(sandbox_dir, "submit_config.txt"))

    job_cfg_overlay = os.path.join(runtime_job_dir, "cfg")
    if os.path.isdir(job_cfg_overlay):
        _copy_tree_contents(job_cfg_overlay, os.path.join(sandbox_dir, "cfg"))

    for staged_name in sorted(os.listdir(runtime_shared_dir)):
        src_path = os.path.join(runtime_shared_dir, staged_name)
        if not os.path.isfile(src_path):
            continue
        if staged_name in {"cfg_bundle.tar.gz", "aux_bundle.tar.gz", "x509"}:
            continue
        if staged_name.endswith((".tar.gz", ".tgz")) and staged_name != exe_relpath:
            continue
        shutil.copy2(src_path, os.path.join(sandbox_dir, staged_name))

    exe_path = os.path.join(sandbox_dir, exe_relpath)
    if not os.path.isfile(exe_path):
        raise RuntimeError(
            f"Executable not found in runtime sandbox: {exe_path!r}."
        )
    return sandbox_dir, exe_path


def _run_prepared_skim_job(
    shared_dir: str,
    source_job_dir: str,
    exe_relpath: str,
    root_setup: str = "",
    container_setup: str = "",
) -> str:
    """Run a prepared skim job after materializing a worker-local sandbox."""
    runtime_job_dir = None
    try:
        runtime_job_dir, exe_path = _materialize_file_source_runtime_job(
            shared_dir=shared_dir,
            source_job_dir=source_job_dir,
            exe_relpath=exe_relpath,
        )
        return _run_analysis_job(
            exe_path=exe_path,
            job_dir=runtime_job_dir,
            root_setup=root_setup,
            container_setup=container_setup,
        )
    finally:
        if runtime_job_dir and os.path.isdir(runtime_job_dir):
            shutil.rmtree(runtime_job_dir, ignore_errors=True)


def _rewrite_submit_transfer_files(
    submit_path: str,
    banned_suffixes: Optional[list[str]] = None,
    banned_exact_paths: Optional[list[str]] = None,
    appended_paths: Optional[list[str]] = None,
) -> None:
    """Rewrite transfer-input entries in a generated Condor submit file."""
    banned_suffixes = banned_suffixes or []
    banned_exact = set(banned_exact_paths or [])
    appended_paths = appended_paths or []

    with open(submit_path) as fh:
        lines = fh.readlines()

    rewritten: list[str] = []
    for line in lines:
        stripped = line.lstrip()
        if not stripped.lower().startswith("transfer_input_files ="):
            rewritten.append(line)
            continue

        prefix, value = line.split("=", 1)
        items = [item.strip() for item in value.strip().split(",") if item.strip()]
        kept = [
            item
            for item in items
            if item not in banned_exact
            and not any(item.endswith(suffix) for suffix in banned_suffixes)
        ]
        for path in appended_paths:
            if path not in kept:
                kept.append(path)
        rewritten.append(f"{prefix}= {','.join(kept)}\n")

    with open(submit_path, "w") as fh:
        fh.writelines(rewritten)


def _skim_shared_input_files(shared_dir: str) -> list[str]:
    """Return concrete shared-input files to transfer one-by-one for skim jobs."""
    return sorted(
        os.path.join(shared_dir, entry)
        for entry in os.listdir(shared_dir)
        if os.path.isfile(os.path.join(shared_dir, entry))
    )


def _output_files_exist_batch(output_paths: list[str]) -> dict[str, bool]:
    """Check a mixed list of local and XRootD output paths."""
    result: dict[str, bool] = {}
    eos_paths: list[str] = []

    for path in output_paths:
        if not path:
            result[path] = False
        elif path.startswith("root://"):
            eos_paths.append(path)
        else:
            result[path] = os.path.exists(path)

    if eos_paths:
        from nano_tasks import _eos_files_exist_batch  # lazy import to avoid circular deps

        result.update(_eos_files_exist_batch(eos_paths))

    return result


def _submit_single_skim_job(
    job_index: int,
    main_dir: str,
    max_runtime: int,
    request_memory: int,
    shared_dir_name: str,
    config_file: str = "submit_config.txt",
) -> Optional[str]:
    """Submit a single prepared skim job for monitor-driven resubmission."""
    job_dir = os.path.join(main_dir, f"job_{job_index}")
    resub_dir = os.path.join(main_dir, "resubmissions")
    Path(resub_dir).mkdir(parents=True, exist_ok=True)

    transfer_files = [
        os.path.join(job_dir, config_file),
    ]
    shared_dir = os.path.join(main_dir, shared_dir_name)
    transfer_files.extend(_skim_shared_input_files(shared_dir))
    inner_script = os.path.join(main_dir, "condor_runscript_inner.sh")
    if os.path.exists(inner_script):
        transfer_files.append(inner_script)

    transfer_str = ",".join(
        path for path in transfer_files if "$(" in path or os.path.exists(path)
    )
    log_base = os.path.join(main_dir, "condor_logs")
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
transfer_output_files = {config_file}

Output = {log_base}/resub_{job_index}_$(Cluster)_$(Process).stdout
Error  = {log_base}/resub_{job_index}_$(Cluster)_$(Process).stderr
Log    = {log_base}/resub_{job_index}.log
queue 1
"""

    sub_path = os.path.join(resub_dir, f"resub_job{job_index}.sub")
    with open(sub_path, "w") as fh:
        fh.write(sub_content)

    try:
        result = subprocess.run(
            ["condor_submit", sub_path],
            capture_output=True,
            text=True,
            timeout=60,
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


def _rewrite_staged_analysis_config(
    template_config_path: str,
    source_config_path: str,
    staged_config_path: str,
) -> None:
    """Rewrite staged YAML config payloads so aux references stay valid locally."""
    if not staged_config_path.lower().endswith((".yaml", ".yml")):
        return

    aux_src = _analysis_aux_dir(template_config_path)
    aux_files: set[str] = set()
    if aux_src and os.path.isdir(aux_src):
        aux_files = {
            aux_entry.name
            for aux_entry in Path(aux_src).iterdir()
            if aux_entry.is_file()
        }
    if not aux_files:
        return

    try:
        import yaml  # type: ignore
    except ImportError:
        return

    with open(staged_config_path) as fh:
        payload = yaml.safe_load(fh)

    def _rewrite(node):
        if isinstance(node, list):
            return [_rewrite(item) for item in node]
        if isinstance(node, dict):
            rewritten: dict[object, object] = {}
            for key, value in node.items():
                if isinstance(key, str) and key.lower().endswith("file") and isinstance(value, str):
                    basename = os.path.basename(value.strip())
                    if basename in aux_files:
                        rewritten[key] = os.path.join("aux", basename)
                        continue

                    resolved_value = _resolve_analysis_config_path(source_config_path, value.strip())
                    if (
                        resolved_value
                        and os.path.isfile(resolved_value)
                        and os.path.basename(resolved_value) in aux_files
                    ):
                        rewritten[key] = os.path.join(
                            "aux",
                            os.path.basename(resolved_value),
                        )
                        continue
                rewritten[key] = _rewrite(value)
            return rewritten
        return node

    rewritten_payload = _rewrite(payload)
    if rewritten_payload == payload:
        return

    with open(staged_config_path, "w") as fh:
        yaml.safe_dump(rewritten_payload, fh, sort_keys=False)


# ===========================================================================
# Helpers: provenance collection and manifest construction
# ===========================================================================


def _build_task_provenance(
    submit_config_path: Optional[str] = None,
    dataset_manifest_path: Optional[str] = None,
) -> ProvenanceRecord:
    """Collect a :class:`~output_schema.ProvenanceRecord` for the current task.

    Gathers whichever provenance fields are available in the current
    environment:

    * **framework_hash** – HEAD commit hash of the repository containing
      this file, obtained via ``git rev-parse HEAD`` (silently omitted when
      git is unavailable or the working tree is not inside a git repository).
    * **config_mtime** – UTC modification timestamp of *submit_config_path*
      in ISO 8601 format (omitted when the file does not exist).
    * **dataset_manifest_hash** – SHA-256 hex digest of *dataset_manifest_path*
      via :meth:`~dataset_manifest.DatasetManifest.file_hash` (omitted when
      the file does not exist or the hash cannot be computed).

    Parameters
    ----------
    submit_config_path:
        Path to the submit_config.txt template for the current task.
    dataset_manifest_path:
        Path to the dataset manifest YAML file for the current task.

    Returns
    -------
    ProvenanceRecord
        Best-effort provenance snapshot; fields that cannot be determined
        are left as ``None``.
    """
    framework_hash: Optional[str] = None
    try:
        result = subprocess.run(
            ["git", "-C", _HERE, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            framework_hash = result.stdout.strip() or None
    except Exception:
        pass

    config_mtime: Optional[str] = None
    if submit_config_path and os.path.isfile(submit_config_path):
        mtime = os.path.getmtime(submit_config_path)
        config_mtime = datetime.datetime.fromtimestamp(
            mtime, tz=datetime.timezone.utc
        ).isoformat()

    dataset_manifest_hash: Optional[str] = None
    if dataset_manifest_path and os.path.isfile(dataset_manifest_path):
        try:
            dataset_manifest_hash = DatasetManifest.file_hash(dataset_manifest_path)
        except Exception:
            pass

    return ProvenanceRecord(
        framework_hash=framework_hash,
        config_mtime=config_mtime,
        dataset_manifest_hash=dataset_manifest_hash,
    )


def _build_skim_manifest(
    dataset: DatasetEntry,
    submit_config_path: str,
    dataset_manifest_path: str,
    skim_output_file: str,
) -> OutputManifest:
    """Build an :class:`~output_schema.OutputManifest` for a skim artifact.

    Constructs the manifest with an :class:`~output_schema.IntermediateArtifactSchema`
    recording the artifact as a ``"preselection"`` kind, combined with
    :class:`~output_schema.DatasetManifestProvenance` capturing the dataset
    selection context and the provenance fields collected by
    :func:`_build_task_provenance`.

    Parameters
    ----------
    dataset:
        The :class:`~dataset_manifest.DatasetEntry` processed in this branch.
    submit_config_path:
        Path to the submit_config.txt template file.
    dataset_manifest_path:
        Path to the dataset manifest YAML file.
    skim_output_file:
        Absolute path to the produced skim ROOT file.

    Returns
    -------
    OutputManifest
        Manifest suitable for passing to :func:`~output_schema.write_cache_sidecar`.
    """
    prov = _build_task_provenance(
        submit_config_path=submit_config_path,
        dataset_manifest_path=dataset_manifest_path,
    )

    manifest_hash: Optional[str] = None
    if os.path.isfile(dataset_manifest_path):
        try:
            manifest_hash = DatasetManifest.file_hash(dataset_manifest_path)
        except Exception:
            pass

    dataset_manifest_prov = DatasetManifestProvenance(
        manifest_path=dataset_manifest_path,
        manifest_hash=manifest_hash,
        resolved_entry_names=[dataset.name],
    )

    artifact = IntermediateArtifactSchema(
        artifact_kind="preselection",
        output_file=skim_output_file,
    )

    return OutputManifest(
        intermediate_artifacts=[artifact],
        framework_hash=prov.framework_hash,
        config_mtime=prov.config_mtime,
        dataset_manifest_provenance=dataset_manifest_prov,
    )


def _dataset_job_metadata(dataset: DatasetEntry) -> dict[str, str]:
    """Return per-dataset manifest metadata to inject into a job config.

    The skim workflow now materializes manifest metadata directly into every
    per-job submit config so analysis code can query sample attributes without
    reparsing the manifest. ``dtype`` carries the framework-level ``mc`` /
    ``data`` classification, while ``type`` is reserved for the legacy numeric
    sample code and is derived from ``sample_type`` or ``stitch_id`` when
    available.
    """
    metadata: dict[str, str] = {
        "sample": dataset.name,
    }

    def _store(key: str, value: object) -> None:
        if value is None:
            return
        if isinstance(value, list):
            if not value:
                return
            metadata[key] = ",".join(str(item) for item in value)
            return
        metadata[key] = str(value)

    _store("name", dataset.name)
    _store("year", dataset.year)
    _store("era", dataset.era)
    _store("campaign", dataset.campaign)
    _store("process", dataset.process)
    _store("group", dataset.group)
    _store("stitch_id", dataset.stitch_id)
    _store("sample_type", dataset.sample_type)
    _store("xsec", dataset.xsec)
    _store("filter_efficiency", dataset.filter_efficiency)
    _store("kfac", dataset.kfac)
    _store("extra_scale", dataset.extra_scale)
    _store("sum_weights", dataset.sum_weights)
    _store("dtype", dataset.dtype)
    _store("parent", dataset.parent)
    _store("das", dataset.das)

    if dataset.filter_efficiency is not None:
        metadata["filterEfficiency"] = str(dataset.filter_efficiency)
    if dataset.extra_scale is not None:
        metadata["extraScale"] = str(dataset.extra_scale)
    if dataset.sum_weights is not None:
        metadata["norm"] = str(dataset.sum_weights)

    legacy_type = None
    if dataset.sample_type is not None:
        legacy_type = dataset.sample_type
    elif dataset.stitch_id is not None:
        legacy_type = dataset.stitch_id
    elif dataset.dtype:
        legacy_type = dataset.dtype
    if legacy_type is not None:
        metadata["type"] = str(legacy_type)

    return metadata


def _resolve_output_destination_base(
    template_config_path: str,
    config_dict: dict[str, str],
) -> str:
    """Resolve the configured final saveDirectory for staged skim outputs."""
    save_dir = str(config_dict.get("saveDirectory", "") or "").strip()
    if not save_dir:
        return ""
    if os.path.isabs(save_dir):
        return save_dir
    template_dir = os.path.dirname(os.path.abspath(template_config_path))
    return os.path.abspath(os.path.join(template_dir, save_dir))


def _rewrite_job_output_destinations(config_path: str, final_output_dir: str) -> None:
    """Rewrite saveDirectory/saveFile/metaFile in a per-job config copy."""
    config_dict = read_config(config_path)
    config_dict["saveDirectory"] = final_output_dir
    config_dict["saveFile"] = os.path.join(final_output_dir, "skim.root")
    config_dict["metaFile"] = os.path.join(final_output_dir, "meta.root")

    with open(config_path, "w") as fh:
        for key, value in config_dict.items():
            if not key.startswith("__"):
                fh.write(f"{key}={value}\n")


# ===========================================================================
# Helper: write a job submit_config.txt from a template + dataset overrides
# ===========================================================================

def _write_job_config(
    job_dir: str,
    template_config_path: str,
    dataset: DatasetEntry,
    output_dir: str,
    extra_overrides: Optional[dict[str, str]] = None,
    stage_shared_inputs_locally: bool = True,
) -> None:
    """Create a ``submit_config.txt`` inside *job_dir* for *dataset*.

    Reads the template at *template_config_path*, overrides the dataset-
    specific keys (``fileList``, ``saveFile``, ``metaFile``,
    ``saveDirectory``) and injects manifest metadata (including ``dtype``,
    ``stitch_id``, ``sample_type``, ``process``, ``group``, etc.) into the
    main per-job config. ``type`` is reserved for the legacy numeric sample
    code and is derived from ``sample_type`` or ``stitch_id`` when present.
    Referenced auxiliary config files (``*.txt``, ``*.yaml``) are copied from
    the template directory into *job_dir* and the final config is written.

    Parameters
    ----------
    job_dir:
        Directory to write ``submit_config.txt`` into (created if absent).
    template_config_path:
        Path to the user-provided submit config template.
    dataset:
        :class:`~dataset_manifest.DatasetEntry` whose files and metadata
        are used to fill in the dataset-specific fields.
    output_dir:
        Directory where the analysis executable should write its output
        ROOT files for this dataset.
    extra_overrides:
        Additional key-value overrides applied after the standard ones.
    """
    template_config = read_config(template_config_path)
    template_dir = os.path.dirname(os.path.abspath(template_config_path))

    job_config = dict(template_config)

    # ---- file list ----------------------------------------------------------
    if dataset.files:
        file_list = [f.strip() for f in dataset.files if f and f.strip()]
    elif dataset.das:
        file_list = [dataset.das.strip()]
    else:
        file_list = []

    # Ensure each entry is accessible via XRootD redirector when appropriate.
    # Do not modify local filenames (input_*.root) or other non-EOS paths.
    normalized_files = []
    for f in file_list:
        if not f:
            continue
        if f.startswith("/") or f.startswith("store/") or f.startswith("root://"):
            normalized_files.append(ensure_xrootd_redirector(f))
        else:
            normalized_files.append(f)

    if normalized_files:
        job_config["fileList"] = ",".join(normalized_files)

    # ---- output paths -------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    job_config["saveFile"] = os.path.join(output_dir, "skim.root")
    job_config["metaFile"] = os.path.join(output_dir, "meta.root")
    job_config["saveDirectory"] = output_dir

    # ---- sample identity / manifest metadata --------------------------------
    job_config.update(_dataset_job_metadata(dataset))

    # Data samples do not carry MC-only generator weights.
    if str(dataset.dtype or "").lower() == "data":
        job_config.pop("counterWeightBranch", None)

    # ---- caller-supplied overrides ------------------------------------------
    if extra_overrides:
        job_config.update(extra_overrides)

    # ---- copy referenced auxiliary files ------------------------------------
    if stage_shared_inputs_locally:
        copied_payloads = _copy_analysis_config_files(
            template_config_path=template_config_path,
            config_dict=template_config,
            destination_dir=job_dir,
        )
        _copy_analysis_aux_dir(template_config_path, job_dir)
        for key, val in list(job_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt") or val.endswith(".yaml") or val.endswith(".yml")
            ):
                basename = os.path.basename(val)
                if basename in copied_payloads:
                    job_config[key] = basename
    else:
        _normalize_shared_config_references(template_config_path, job_config)

    # ---- write the final config file ----------------------------------------
    config_path = os.path.join(job_dir, "submit_config.txt")
    with open(config_path, "w") as fh:
        for key, val in job_config.items():
            if not key.startswith("__"):
                fh.write(f"{key}={val}\n")


def _plan_file_source_jobs(
    *,
    dataset_manifest_path: str,
    file_list_dir: str,
    partition: str,
    files_per_job: int,
    entries_per_job: int,
    jobs_dir: str,
    outputs_dir: str,
) -> list[dict[str, object]]:
    """Build deterministic per-job metadata from file-list JSON outputs."""
    job_plans: list[dict[str, object]] = []

    for json_path in sorted(Path(file_list_dir).glob("*.json")):
        with open(json_path) as fh:
            payload = json.load(fh)

        dataset_name = payload.get("sample", json_path.stem)
        all_files = payload.get("files", [])
        groups = payload.get("groups", [])

        # Prefer pre-computed group partitions when available (e.g. from GetNANOFileList).
        # If the file list is empty but groups exist, we still want to create jobs.
        if groups:
            partitions = [
                {"files": g, "first_entry": 0, "last_entry": 0}
                for g in groups
                if g
            ]
        else:
            if not all_files:
                continue
            partitions = _make_partitions(
                urls=all_files,
                mode=partition,
                files_per_job=files_per_job,
                entries_per_job=entries_per_job,
            )

        dataset_entry = _find_dataset_entry(dataset_manifest_path, dataset_name)
        dtype = dataset_entry.dtype if dataset_entry else "mc"

        for sub_idx, part in enumerate(partitions):
            job_plans.append({
                "dataset_name": dataset_name,
                "job_dir": os.path.join(jobs_dir, dataset_name, f"job_{sub_idx}"),
                "out_dir": os.path.join(outputs_dir, dataset_name, f"job_{sub_idx}"),
                "files": part["files"],
                "first_entry": part.get("first_entry", 0),
                "last_entry": part.get("last_entry", 0),
                "dtype": dtype,
            })
            

    return job_plans


# ===========================================================================
# Shared parameter mixin for SkimTask and HistFillTask
# ===========================================================================

class AnalysisMixin:
    """Parameters shared by :class:`SkimTask` and :class:`HistFillTask`."""

    submit_config = luigi.Parameter(
        description=(
            "Path to the submit_config.txt template file for this analysis pass. "
            "Dataset-specific keys (fileList, saveFile, metaFile, sample, type) "
            "are filled in automatically from the dataset manifest; all other "
            "keys are forwarded verbatim to the job."
        ),
    )
    exe = luigi.Parameter(
        description=(
            "Path to the compiled analysis executable. "
            "It is invoked as ``<exe> submit_config.txt`` inside each job directory."
        ),
    )
    name = luigi.Parameter(
        description=(
            "Run name used to namespace output directories. "
            "SkimTask writes to ``skimRun_<name>/``; "
            "HistFillTask writes to ``histRun_<name>/``."
        ),
    )
    dataset_manifest = luigi.Parameter(
        description=(
            "Path to the dataset manifest YAML file. "
            "One workflow branch is created per dataset entry in the manifest."
        ),
    )

    # ---- derived directory helpers -----------------------------------------

    @property
    def _jobs_dir(self) -> str:
        """Directory tree holding per-dataset job subdirectories."""
        return os.path.join(self._run_dir, "jobs")

    @property
    def _outputs_dir(self) -> str:
        """Directory tree holding per-dataset analysis output ROOT files."""
        return os.path.join(self._run_dir, "outputs")

    @property
    def _job_outputs_dir(self) -> str:
        """Directory holding per-branch ``.done`` marker files."""
        return os.path.join(self._run_dir, "job_outputs")


# ===========================================================================
# Additional parameter mixin for SkimTask and PrepareSkimJobs
# ===========================================================================

class SkimMixin:
    """Extra parameters for :class:`PrepareSkimJobs` and :class:`SkimTask`.

    Adds file-discovery chaining, per-job file partitioning, worker-environment
    staging, and multi-executor (local / HTCondor / Dask) control on top of
    the base :class:`AnalysisMixin` parameters.
    """

    # ---- file-source chaining ---------------------------------------------

    file_source = luigi.Parameter(
        default="",
        description=(
            "Source for input file lists.  One of: '' (use dataset manifest directly), "
            "'nano' (use GetNANOFileList output), 'opendata' (use GetOpenDataFileList "
            "output), 'xrdfs' (use GetXRDFSFileList output).  "
            "When set, :class:`PrepareSkimJobs` is automatically required and "
            "splits files into per-job chunks before HTCondor/Dask dispatch."
        ),
    )
    file_source_name = luigi.Parameter(
        default="",
        description=(
            "Name of the file-list task run to chain from (required when "
            "--file-source is set)."
        ),
    )
    files_per_job = luigi.IntParameter(
        default=50,
        description=(
            "Maximum number of files per job when splitting from a file list "
            "(default: 50).  Also used as the per-group file limit passed to "
            "the Rucio query in GetNANOFileList."
        ),
    )
    partition = luigi.Parameter(
        default="file_group",
        description=(
            "Partitioning mode for splitting file lists into per-job chunks: "
            "'file_group' (default – group by file count), "
            "'file' (one file per job), "
            "'entry_range' (split by TTree entry ranges, requires uproot)."
        ),
    )
    entries_per_job = luigi.IntParameter(
        default=100_000,
        description=(
            "Maximum TTree entries per job in 'entry_range' mode (default: 100000). "
            "Requires uproot.  Ignored for 'file_group' and 'file' modes."
        ),
    )

    # ---- worker environment -----------------------------------------------

    x509 = luigi.Parameter(
        default="",
        description=(
            "Path to the VOMS x509 proxy file.  Copied into "
            "``skimRun_{name}/shared_inputs/x509`` for use by HTCondor workers."
        ),
    )
    stage_in = luigi.BoolParameter(
        default=False,
        description=(
            "If True, xrdcp input files to the worker node before running the "
            "analysis (same as NANOMixin stage_in)."
        ),
    )
    root_setup = luigi.Parameter(
        default="",
        description=(
            "Path to a ROOT environment setup script whose contents are "
            "embedded in the worker inner runscript."
        ),
    )
    container_setup = luigi.Parameter(
        default="",
        description=(
            "Container setup command used by the outer wrapper script "
            "(e.g. 'cmssw-el9')."
        ),
    )
    python_env = luigi.Parameter(
        default="",
        description=(
            "Path to a Python environment tarball created by "
            "``law/setup_python_env.sh``.  Staged to every HTCondor worker "
            "node alongside the executable."
        ),
    )
    make_test_job = luigi.BoolParameter(
        default=True,
        description=(
            "If True (default), run a single-file local test job before dispatching "
            "the full SkimTask workflow.  This catches analysis runtime errors early "
            "before submitting potentially hundreds of jobs.  Set --no-make-test-job "
            "to skip the test and submit immediately."
        ),
    )

    # ---- multi-executor control -------------------------------------------

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
    max_runtime = luigi.IntParameter(
        default=3600,
        significant=False,
        description="Maximum HTCondor job runtime in seconds (default: 3600).",
    )

    # ---- derived helpers --------------------------------------------------

    def _resolve_skim(self, path_value: str) -> str:
        """Resolve *path_value* relative to the submit-config's parent directory."""
        return _resolve_analysis_config_path(self.submit_config, path_value)

    @property
    def _shared_dir(self) -> str:
        """Shared inputs directory containing the exe, libs, and x509 proxy."""
        return os.path.join(self._run_dir, "shared_inputs")

    @property
    def _exe_relpath(self) -> str:
        """Basename of the compiled analysis executable."""
        return os.path.basename(os.path.abspath(self.exe))

    @property
    def _root_setup_content(self) -> str:
        """Return the content of the root_setup script, or empty string."""
        if not self.root_setup:
            return ""
        path = self._resolve_skim(self.root_setup)
        if not path or not os.path.isfile(path):
            raise RuntimeError(
                f"root_setup must point to an existing file, got: {self.root_setup!r}"
            )
        with open(path) as fh:
            return fh.read().rstrip("\n")

    def _python_env_path(self) -> Optional[str]:
        """Return the resolved path to the Python environment tarball, or None."""
        if not self.python_env:
            return None
        path = self._resolve_skim(self.python_env)
        if not path or not os.path.isfile(path):
            raise RuntimeError(
                f"python_env must point to an existing tarball, "
                f"got: {self.python_env!r}\n"
                "Create one with:  bash law/setup_python_env.sh --output python_env.tar.gz"
            )
        return path

    def _file_list_dir(self) -> str:
        """Return the directory containing per-dataset file-list JSONs."""
        prefix_map = {
            "nano": "nanoFileList",
            "opendata": "openDataFileList",
            "xrdfs": "xrdfsFileList",
        }
        prefix = prefix_map.get(self.file_source, "")
        if not prefix:
            return ""
        return os.path.join(WORKSPACE, f"{prefix}_{self._effective_file_source_name}")

    @property
    def _effective_file_source_name(self) -> str:
        """Return the configured file-source name or fall back to the run name."""
        return str(self.file_source_name or self.name)

    @property
    def _test_job_dir(self) -> str:
        """Directory for the local pre-submission test job."""
        return os.path.join(self._run_dir, "test_job")


# ===========================================================================
# PrepareSkimJobs – set up shared_inputs/ and per-job directories
# (analogous to BuildNANOSubmission in nano_tasks.py)
# ===========================================================================

class PrepareSkimJobs(AnalysisMixin, SkimMixin, law.Task):
    """Create per-job directories and ``shared_inputs/`` for :class:`SkimTask`.

    This task mirrors the role of ``BuildNANOSubmission`` in the NANO workflow:
    it prepares **all** job directories and the shared executable/library
    staging area **before** any HTCondor or Dask branches are dispatched.

    Workflow
    --------
    When ``--file-source`` is set the task reads the file-list JSON(s) written
    by :class:`~nano_tasks.GetNANOFileList`,
    :class:`~opendata_tasks.GetOpenDataFileList`, or
    :class:`~nano_tasks.GetXRDFSFileList` from the appropriate
    ``{prefix}FileList_{file_source_name}/`` directory.  Each JSON encodes a
    flat ``files`` list and, when available, pre-computed Rucio size groups
    (``groups`` key).  The task splits the files into per-job partitions and
    writes one ``submit_config.txt`` / ``floats.txt`` / ``ints.txt`` set per
    job directory under ``skimRun_{name}/jobs/{dataset}/job_{i}/``.

    When ``--file-source`` is empty the manifest datasets are used directly
    (one job dir per dataset entry).

    In both cases the shared inputs directory
    ``skimRun_{name}/shared_inputs/`` is populated with:

    * the analysis executable (``self.exe``),
    * local shared libraries that resolve under the repository root,
    * the VOMS x509 proxy (when ``--x509`` is set),
    * the Python environment tarball (when ``--python-env`` is set).

    Output
    ------
    * ``skimRun_{name}/prep_submission.json`` – JSON list of
      ``{"dataset_name": ..., "job_dir": ..., "out_dir": ...}`` records used
      by :class:`SkimTask` to build its branch map.
    """

    task_namespace = ""

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        """Declare LAW dependency on the file-list task when ``--file-source`` is set.

        For ``--file-source xrdfs`` the dependency on
        :class:`~nano_tasks.GetXRDFSFileList` is declared automatically because
        both tasks share the same ``dataset_manifest`` parameter.

        For ``--file-source nano`` and ``--file-source opendata`` the skim
        submit config is reused directly when it contains the corresponding
        ``sampleConfig`` payload, allowing the file-list step to be chained
        automatically from :class:`SkimTask`.
        """
        if self.file_source == "xrdfs":
            from nano_tasks import GetXRDFSFileList  # lazy import to avoid circular deps
            return GetXRDFSFileList.req(
                self,
                name=self._effective_file_source_name,
            )
        if self.file_source == "nano":
            from nano_tasks import GetNANOFileList  # lazy import to avoid circular deps
            return GetNANOFileList.req(
                self,
                name=self._effective_file_source_name,
            )
        if self.file_source == "opendata":
            from opendata_tasks import GetOpenDataFileList  # lazy import to avoid circular deps
            return GetOpenDataFileList.req(
                self,
                name=self._effective_file_source_name,
            )
        return None

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._run_dir, "prep_submission.json")
        )

    def run(self):
        with PerformanceRecorder("PrepareSkimJobs") as rec:
            self._run_impl()
        rec.save(os.path.join(self._run_dir, "prep_submission.perf.json"))

    def _run_impl(self):  # noqa: C901 - intentionally comprehensive
        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Analysis executable not found: {exe_path!r}. "
                "Build the analysis before running PrepareSkimJobs."
            )

        # ---- set up shared_inputs/ ----------------------------------------
        shared_dir = self._shared_dir
        Path(shared_dir).mkdir(parents=True, exist_ok=True)

        # Copy executable
        shutil.copy2(exe_path, os.path.join(shared_dir, self._exe_relpath))

        # Copy local shared libraries
        local_libs = _collect_skim_shared_libs(exe_path, WORKSPACE)
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(shared_dir, lib_name)
            if (not os.path.exists(lib_dst)
                    or os.path.getsize(lib_dst) != os.path.getsize(lib_src)):
                shutil.copy2(lib_src, lib_dst)
        if local_libs:
            self.publish_message(
                f"Staged {len(local_libs)} local shared lib(s) into {shared_dir}/"
            )

        # Copy x509 proxy
        if self.x509:
            x509_src = os.path.abspath(self.x509)
            if os.path.isfile(x509_src):
                shutil.copy2(x509_src, os.path.join(shared_dir, "x509"))
            else:
                self.publish_message(
                    f"Warning: x509 file not found: {x509_src!r}; skipping."
                )

        # Copy Python env tarball
        python_env_src = self._python_env_path()
        if python_env_src:
            tarball_name = os.path.basename(python_env_src)
            shutil.copy2(python_env_src, os.path.join(shared_dir, tarball_name))

        # Bundle shared configs and aux once for remote worker materialization.
        if os.path.isfile(self.submit_config):
            template_config = read_config(self.submit_config)
            _bundle_analysis_config_files(self.submit_config, template_config, shared_dir)
            _bundle_analysis_aux_dir(self.submit_config, shared_dir)

        # ---- determine datasets and file groups ---------------------------
        job_entries: list = []

        if self.file_source:
            fl_dir = self._file_list_dir()
            if not os.path.isdir(fl_dir):
                raise RuntimeError(
                    f"File list directory not found: {fl_dir!r}.\n"
                    f"Run the file list task first:\n"
                    f"  law run Get{self.file_source.capitalize()}FileList "
                    f"--name {self.file_source_name!r} ..."
                )

            planned_jobs = _plan_file_source_jobs(
                dataset_manifest_path=self.dataset_manifest,
                file_list_dir=fl_dir,
                partition=self.partition,
                files_per_job=self.files_per_job,
                entries_per_job=self.entries_per_job,
                jobs_dir=self._jobs_dir,
                outputs_dir=self._outputs_dir,
            )

            for plan in planned_jobs:
                job_dir = str(plan["job_dir"])
                out_dir = str(plan["out_dir"])
                dataset_name = str(plan["dataset_name"])
                Path(job_dir).mkdir(parents=True, exist_ok=True)

                manifest_entry = _find_dataset_entry(self.dataset_manifest, dataset_name)
                if manifest_entry is not None:
                    transient = DatasetEntry.from_dict(manifest_entry.to_dict())
                    transient.files = [
                        f.strip()
                        for f in str(plan["files"]).split(",")
                        if f.strip()
                    ]
                    transient.das = None
                else:
                    transient = DatasetEntry(
                        name=dataset_name,
                        files=[
                            f.strip()
                            for f in str(plan["files"]).split(",")
                            if f.strip()
                        ],
                        dtype=str(plan["dtype"]),
                    )

                extra_overrides: dict[str, str] = {}
                if int(plan.get("last_entry", 0)) > 0:
                    extra_overrides["firstEntry"] = str(plan["first_entry"])
                    extra_overrides["lastEntry"] = str(plan["last_entry"])


                _write_job_config(
                    job_dir=job_dir,
                    template_config_path=self.submit_config,
                    dataset=transient,
                    output_dir=out_dir,
                    extra_overrides=extra_overrides,
                    stage_shared_inputs_locally=False,
                )

                job_entries.append({
                    "dataset_name": dataset_name,
                    "job_dir": job_dir,
                    "out_dir": out_dir,
                })

        else:
            # Standard manifest mode: one job dir per dataset entry
            manifest = DatasetManifest.load(self.dataset_manifest)
            for entry in manifest.datasets:
                job_dir = os.path.join(self._jobs_dir, entry.name)
                Path(job_dir).mkdir(parents=True, exist_ok=True)
                out_dir = os.path.join(self._outputs_dir, entry.name)
                _write_job_config(
                    job_dir=job_dir,
                    template_config_path=self.submit_config,
                    dataset=entry,
                    output_dir=out_dir,
                    stage_shared_inputs_locally=False,
                )
                job_entries.append({
                    "dataset_name": entry.name,
                    "job_dir": job_dir,
                    "out_dir": out_dir,
                })

        # ---- write the branch-map manifest --------------------------------
        Path(self._run_dir).mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(job_entries, fh, indent=2)

        self.publish_message(
            f"PrepareSkimJobs: {len(job_entries)} job dir(s) prepared.\n"
            f"  shared_inputs/ ready at {shared_dir}"
        )

        # ---- create local test job (one file) for pre-flight validation -----
        if self.make_test_job and job_entries:
            self._create_test_job(exe_path, job_entries)

    def _create_test_job(self, exe_path: str, job_entries: list) -> None:
        """Create a minimal single-file ``test_job/`` directory for pre-flight testing.

        The first available input file from the first job entry is used.  The test
        job writes outputs to ``test_job/`` itself (not EOS/shared storage) so that
        it runs entirely locally without staging.

        Parameters
        ----------
        exe_path:
            Absolute path to the analysis executable.
        job_entries:
            List of ``{"dataset_name", "job_dir", "out_dir"}`` dicts produced by
            :meth:`_run_impl`.
        """
        test_dir = self._test_job_dir
        Path(test_dir).mkdir(parents=True, exist_ok=True)

        exe_relpath = self._exe_relpath

        # Find the first input file from the first job entry
        first_file = ""
        first_job_dir = ""
        for entry in job_entries:
            jcfg_path = os.path.join(entry["job_dir"], "submit_config.txt")
            if os.path.exists(jcfg_path):
                jcfg = read_config(jcfg_path)
                file_list = jcfg.get("__orig_fileList", jcfg.get("fileList", ""))
                if file_list:
                    first_file = file_list.split(",")[0].strip()
                    first_job_dir = entry["job_dir"]
                    break

        if not first_file:
            self.publish_message(
                "Warning: no input files found in job entries; test job not created."
            )
            return

        # Copy the analysis executable into the test job dir
        dst_exe = os.path.join(test_dir, exe_relpath)
        if not os.path.exists(dst_exe):
            shutil.copy2(exe_path, dst_exe)

        # Copy local shared libraries
        local_libs = _collect_skim_shared_libs(exe_path, WORKSPACE)
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(test_dir, lib_name)
            if not os.path.exists(lib_dst):
                shutil.copy2(lib_src, lib_dst)

        # Copy auxiliary config files referenced in the template
        if os.path.isfile(self.submit_config):
            template_config = read_config(self.submit_config)
            _copy_analysis_config_files(self.submit_config, template_config, test_dir)
            _copy_analysis_aux_dir(self.submit_config, test_dir)

        # Reuse floats.txt / ints.txt from the first job dir if available
        for aux_name in ("floats.txt", "ints.txt"):
            src = os.path.join(first_job_dir, aux_name)
            if os.path.exists(src):
                dst = os.path.join(test_dir, aux_name)
                if not os.path.exists(dst):
                    shutil.copy2(src, dst)

        # Build a minimal submit config: single file, local outputs, single thread
        if os.path.isfile(self.submit_config):
            test_config = dict(read_config(self.submit_config))
        else:
            test_config = {}
        test_config["fileList"] = first_file
        test_config["saveFile"] = "test_output.root"
        test_config["metaFile"] = "test_output_meta.root"
        test_config["saveDirectory"] = test_dir
        test_config.setdefault("threads", "1")
        # Remove any stage-out sentinel keys
        for key in ("__orig_saveFile", "__orig_metaFile", "__orig_fileList"):
            test_config.pop(key, None)

        # Resolve config-file references to their basenames (files are already copied)
        for key, val in list(test_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt") or val.endswith(".yaml") or val.endswith(".yml")
            ):
                basename = os.path.basename(val)
                if os.path.exists(os.path.join(test_dir, basename)):
                    test_config[key] = basename

        with open(os.path.join(test_dir, "submit_config.txt"), "w") as fh:
            for k, v in test_config.items():
                if not k.startswith("__"):
                    fh.write(f"{k}={v}\n")

        self.publish_message(
            f"[PrepareSkimJobs] Test job created in {test_dir}/\n"
            f"  Input file: {first_file}\n"
            f"  Run manually with:  cd {test_dir} && ./{exe_relpath} submit_config.txt"
        )


# ===========================================================================
# RunSkimTestJob – pre-flight single-file local test
# ===========================================================================

class RunSkimTestJob(AnalysisMixin, SkimMixin, law.Task):
    """Run a single-file local test before dispatching the full :class:`SkimTask`.

    This task mirrors :class:`~nano_tasks.RunTestJob` in the NANO workflow.  It
    runs the analysis executable with a single input file and validates the exit
    code, catching analysis runtime errors **before** expensive batch or
    multi-dataset dispatch.

    In **file-source mode** (``--file-source`` set) the test job directory is
    prepared by :class:`PrepareSkimJobs`, which is declared as a formal LAW
    prerequisite.

    In **standard manifest mode** (``--file-source`` empty) the task creates the
    test job directory itself using the first file from the first dataset entry in
    the manifest.

    Output
    ------
    * ``skimRun_{name}/test_job/test_passed.txt`` – written on success
    """

    task_namespace = ""

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        """Require PrepareSkimJobs in file-source mode (creates the test_job/ dir)."""
        if self.file_source:
            return PrepareSkimJobs.req(self)
        return None

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._test_job_dir, "test_passed.txt")
        )

    def run(self):
        test_dir = self._test_job_dir

        if not os.path.isdir(test_dir):
            # Standard manifest mode: PrepareSkimJobs did not create the test_job/
            # dir because no file-source mode was used.  Build it from scratch.
            self._prepare_standard_test_job(test_dir)

        config_path = os.path.join(test_dir, "submit_config.txt")
        if not os.path.isfile(config_path):
            raise RuntimeError(
                f"Test job config not found: {config_path!r}. "
                "Ensure PrepareSkimJobs has completed (file-source mode) or "
                "that the dataset manifest contains at least one dataset with files."
            )

        exe_relpath = self._exe_relpath
        exe_in_test = os.path.join(test_dir, exe_relpath)
        if not os.path.isfile(exe_in_test):
            # Try the absolute path directly (in case shared_inputs is on PATH)
            exe_in_test = os.path.abspath(self.exe)

        if not os.path.isfile(exe_in_test):
            raise RuntimeError(
                f"Executable not found for test job: {exe_in_test!r}. "
                "Build the analysis before running RunSkimTestJob."
            )

        self.publish_message(
            f"[RunSkimTestJob] Running single-file test in {test_dir} …"
        )

        with PerformanceRecorder("RunSkimTestJob") as rec:
            result = _run_analysis_job(
                exe_path=exe_in_test,
                job_dir=test_dir,
                root_setup=self._root_setup_content,
                container_setup=self.container_setup or "",
            )
        rec.save(os.path.join(test_dir, "test_job.perf.json"))

        self.publish_message(
            f"[RunSkimTestJob] Test passed: {result}"
        )

        with self.output().open("w") as fh:
            fh.write(f"status=passed\n{result}\n")

    def _prepare_standard_test_job(self, test_dir: str) -> None:
        """Create a minimal test job directory in standard (non-file-source) mode.

        Reads the first dataset from the manifest, takes the first file, copies
        the executable and config files, and writes a minimal ``submit_config.txt``.
        """
        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Analysis executable not found: {exe_path!r}. "
                "Build the analysis before running RunSkimTestJob."
            )

        manifest = DatasetManifest.load(self.dataset_manifest)
        first_dataset = next(iter(manifest.datasets), None)
        if first_dataset is None:
            raise RuntimeError(
                f"Dataset manifest '{self.dataset_manifest}' contains no datasets."
            )

        first_file = ""
        if first_dataset.files:
            first_file = first_dataset.files[0]
        elif first_dataset.das:
            first_file = first_dataset.das

        if not first_file:
            raise RuntimeError(
                f"Dataset '{first_dataset.name}' has no files or DAS path; "
                "cannot create a test job."
            )

        Path(test_dir).mkdir(parents=True, exist_ok=True)

        # Copy executable
        exe_relpath = self._exe_relpath
        dst_exe = os.path.join(test_dir, exe_relpath)
        if not os.path.exists(dst_exe):
            shutil.copy2(exe_path, dst_exe)

        # Copy local shared libraries
        local_libs = _collect_skim_shared_libs(exe_path, WORKSPACE)
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(test_dir, lib_name)
            if not os.path.exists(lib_dst):
                shutil.copy2(lib_src, lib_dst)

        # Copy auxiliary config files referenced in template
        if os.path.isfile(self.submit_config):
            template_config = read_config(self.submit_config)
            _copy_analysis_config_files(self.submit_config, template_config, test_dir)
            _copy_analysis_aux_dir(self.submit_config, test_dir)
            test_config = dict(template_config)
        else:
            test_config = {}

        # Override dataset-specific keys
        test_config["fileList"] = first_file
        test_config["saveFile"] = "test_output.root"
        test_config["metaFile"] = "test_output_meta.root"
        test_config["saveDirectory"] = test_dir
        test_config.update(_dataset_job_metadata(first_dataset))
        if str(first_dataset.dtype or "").lower() == "data":
            test_config.pop("counterWeightBranch", None)
        test_config.setdefault("threads", "1")
        for key in ("__orig_saveFile", "__orig_metaFile", "__orig_fileList"):
            test_config.pop(key, None)

        # Resolve referenced config files to basenames
        for key, val in list(test_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt") or val.endswith(".yaml") or val.endswith(".yml")
            ):
                basename = os.path.basename(val)
                if os.path.exists(os.path.join(test_dir, basename)):
                    test_config[key] = basename

        with open(os.path.join(test_dir, "submit_config.txt"), "w") as fh:
            for k, v in test_config.items():
                if not k.startswith("__"):
                    fh.write(f"{k}={v}\n")

        self.publish_message(
            f"[RunSkimTestJob] Test job prepared in {test_dir}/\n"
            f"  First dataset: {first_dataset.name}, input: {first_file}"
        )


class BuildSkimSubmission(AnalysisMixin, SkimMixin, law.Task):
    """Write concrete Condor submission files for prepared skim jobs."""

    task_namespace = ""

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        return PrepareSkimJobs.req(self)

    def output(self):
        return {
            "submit": law.LocalFileTarget(os.path.join(self._run_dir, "condor_submit.sub")),
            "runscript": law.LocalFileTarget(os.path.join(self._run_dir, "condor_runscript.sh")),
        }

    def run(self):
        with PerformanceRecorder("BuildSkimSubmission") as rec:
            self._run_impl()
        rec.save(os.path.join(self._run_dir, "build_submission.perf.json"))

    def _run_impl(self) -> None:
        prep_manifest = os.path.join(self._run_dir, "prep_submission.json")
        if not os.path.isfile(prep_manifest):
            raise RuntimeError(
                f"Prepared skim manifest not found: {prep_manifest!r}. "
                "Run PrepareSkimJobs first."
            )

        with open(prep_manifest) as fh:
            payload = json.load(fh)
        if not isinstance(payload, list) or not payload:
            raise RuntimeError("No prepared skim jobs found in prep_submission.json.")

        job_dirs = sorted(
            str(entry["job_dir"])
            for entry in payload
            if isinstance(entry, dict) and entry.get("job_dir")
        )
        if not job_dirs:
            raise RuntimeError("No prepared skim job directories were discovered.")

        template_config = read_config(self.submit_config)
        final_output_base = _resolve_output_destination_base(
            self.submit_config,
            template_config,
        )
        bookkeeping_outputs_dir = os.path.join(self._run_dir, "outputs")

        Path(self._run_dir).mkdir(parents=True, exist_ok=True)
        for idx, entry in enumerate(payload):
            job_dir_abs = str(entry["job_dir"])
            link_path = os.path.join(self._run_dir, f"job_{idx}")
            if os.path.islink(link_path):
                os.unlink(link_path)
            elif os.path.exists(link_path):
                shutil.rmtree(link_path)

            Path(link_path).mkdir(parents=True, exist_ok=True)
            _copy_tree_contents(job_dir_abs, link_path)

            if final_output_base and entry.get("out_dir"):
                rel_out_dir = os.path.relpath(str(entry["out_dir"]), bookkeeping_outputs_dir)
                final_output_dir = os.path.join(final_output_base, rel_out_dir)
                _rewrite_job_output_destinations(
                    os.path.join(link_path, "submit_config.txt"),
                    final_output_dir,
                )

        shared_dir = self._shared_dir
        if not os.path.isdir(shared_dir):
            raise RuntimeError(
                f"Shared inputs directory not found: {shared_dir!r}. "
                "Run PrepareSkimJobs first."
            )

        python_env_staged = None
        python_env_src = self._python_env_path()
        if python_env_src:
            candidate = os.path.join(shared_dir, os.path.basename(python_env_src))
            if os.path.isfile(candidate):
                python_env_staged = candidate

        x509_loc = "x509" if os.path.isfile(os.path.join(shared_dir, "x509")) else None
        shared_transfer_files = _skim_shared_input_files(shared_dir)

        submit_path = write_submit_files(
            self._run_dir,
            len(job_dirs),
            self._exe_relpath,
            stage_inputs=self.stage_in,
            stage_outputs=True,
            root_setup=self._root_setup_content,
            x509loc=x509_loc,
            max_runtime=str(self.max_runtime),
            request_memory=2000,
            request_cpus=1,
            request_disk=20000,
            extra_transfer_files=shared_transfer_files,
            include_aux=False,
            shared_dir_name="shared_inputs",
            eos_sched=False,
            config_file="submit_config.txt",
            container_setup=self.container_setup,
            python_env_tarball=python_env_staged,
        )
        _rewrite_submit_transfer_files(
            submit_path,
            banned_suffixes=["/floats.txt", "/ints.txt"],
            banned_exact_paths=[os.path.join(self._run_dir, "shared_inputs")],
        )

        self.publish_message(
            f"Skim condor submission ready.\n"
            f"  {len(job_dirs)} job(s) prepared.\n"
            f"  Submit with: condor_submit {submit_path}"
        )


class SubmitSkimJobs(AnalysisMixin, SkimMixin, law.Task):
    """Submit the concrete skim Condor batch and record the cluster ID."""

    task_namespace = ""

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        reqs = {"build": BuildSkimSubmission.req(self)}
        if self.make_test_job:
            reqs["test"] = RunSkimTestJob.req(self)
        return reqs

    def output(self):
        return law.LocalFileTarget(os.path.join(self._run_dir, "submitted.txt"))

    def run(self):
        with PerformanceRecorder("SubmitSkimJobs") as rec:
            build_target = self.input()["build"]
            submit_file = build_target["submit"].path
            if not os.path.exists(submit_file):
                raise RuntimeError(f"condor_submit.sub not found: {submit_file}")

            result = subprocess.run(
                ["condor_submit", submit_file],
                capture_output=True,
                text=True,
                check=True,
            )
            stdout = result.stdout.strip()
            self.publish_message(stdout)

            cluster_id = ""
            for line in stdout.splitlines():
                if "cluster" not in line.lower():
                    continue
                parts = line.split()
                for idx, part in enumerate(parts):
                    if part.lower().rstrip(".") == "cluster" and idx + 1 < len(parts):
                        cluster_id = parts[idx + 1].rstrip(".")
                        break

            n_jobs = 0
            for line in Path(submit_file).read_text().splitlines():
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
                f"Submitted skim cluster {cluster_id} with {n_jobs} job(s)."
            )

        rec.save(perf_path_for(str(self.output().path)))


class MonitorSkimJobs(AnalysisMixin, SkimMixin, law.Task):
    """Monitor concrete skim Condor jobs with NANO-style resubmission."""

    task_namespace = ""

    max_retries = luigi.IntParameter(
        default=3,
        description="Maximum resubmission attempts per failed skim job (default: 3)",
    )
    poll_interval = luigi.IntParameter(
        default=120,
        description="Seconds between condor_q polls (default: 120)",
    )

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        return SubmitSkimJobs.req(self)

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._run_dir, "all_outputs_verified.txt")
        )

    @property
    def _state_file(self) -> str:
        return os.path.join(self._run_dir, "monitor_state.json")

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

    def _discover_jobs(self) -> dict[int, dict[str, str]]:
        info: dict[int, dict[str, str]] = {}
        idx = 0
        while True:
            link = os.path.join(self._run_dir, f"job_{idx}")
            if not os.path.exists(link):
                break
            real_dir = os.path.realpath(link)
            cfg = read_config(os.path.join(real_dir, "submit_config.txt"))
            # Prefer the original remote output paths (used for staging to EOS)
            save = cfg.get("__orig_saveFile", cfg.get("saveFile", ""))
            meta = cfg.get("__orig_metaFile", cfg.get("metaFile", ""))
            info[idx] = {
                "dir": real_dir,
                "save": save,
                "meta": meta,
            }
            idx += 1
        return info

    def _status_summary(self, state: dict) -> dict[str, int]:
        counts: dict[str, int] = {}
        for job_state in state["jobs"].values():
            status = job_state["status"]
            counts[status] = counts.get(status, 0) + 1
        return counts

    def run(self):
        from nano_tasks import (  # lazy import to avoid circular deps
            _condor_history_exit,
            _condor_q_ads,
            _condor_rm_job,
            _read_failed_job_error,
        )

        sub_info: dict[str, str] = {}
        with self.input().open("r") as fh:
            for line in fh:
                line = line.strip()
                if "=" in line:
                    key, value = line.split("=", 1)
                    sub_info[key.strip()] = value.strip()

        initial_cluster = sub_info.get("cluster_id", "")
        if not initial_cluster:
            raise RuntimeError("No cluster_id found in submitted.txt")

        job_info = self._discover_jobs()
        if not job_info:
            raise RuntimeError(
                f"No job_N symlinks found in {self._run_dir}. "
                "Run BuildSkimSubmission first."
            )

        state = self._load_state()
        for idx in job_info:
            key = str(idx)
            if key not in state["jobs"]:
                state["jobs"][key] = {
                    "status": "submitted",
                    "retries": 0,
                    "cluster": initial_cluster,
                    "proc": idx,
                }
        self._save_state(state)

        self.publish_message(
            f"Monitoring {len(job_info)} skim job(s) in cluster {initial_cluster}."
        )

        while True:
            active_clusters = set()
            for job_state in state["jobs"].values():
                if job_state["status"] in ("submitted", "running", "resubmitting"):
                    active_clusters.add(str(job_state["cluster"]))

            live_status: dict[tuple[str, int], int] = {}
            live_hold_reason: dict[tuple[str, int], str] = {}
            history_exit: dict[tuple[str, int], int] = {}
            for cluster_id in active_clusters:
                for proc, ad in _condor_q_ads(cluster_id).items():
                    if "JobStatus" in ad:
                        live_status[(cluster_id, proc)] = ad["JobStatus"]
                    if ad.get("HoldReason"):
                        live_hold_reason[(cluster_id, proc)] = str(ad.get("HoldReason"))
                for proc, exit_code in _condor_history_exit(cluster_id).items():
                    history_exit[(cluster_id, proc)] = exit_code

            output_paths_to_check: list[str] = []
            for idx in sorted(job_info.keys()):
                job_state = state["jobs"][str(idx)]
                if job_state["status"] in ("done", "perm_fail"):
                    continue
                cluster_id = str(job_state["cluster"])
                proc = job_state["proc"]
                if live_status.get((cluster_id, proc)) is None:
                    if job_info[idx]["save"]:
                        output_paths_to_check.append(job_info[idx]["save"])
                    if job_info[idx]["meta"]:
                        output_paths_to_check.append(job_info[idx]["meta"])

            output_exists = _output_files_exist_batch(output_paths_to_check)

            for idx in sorted(job_info.keys()):
                key = str(idx)
                job_state = state["jobs"][key]
                cluster_id = str(job_state["cluster"])
                proc = job_state["proc"]
                status = job_state["status"]
                if status in ("done", "perm_fail"):
                    continue

                condor_status = live_status.get((cluster_id, proc))
                exit_code = history_exit.get((cluster_id, proc))

                if condor_status == 2:
                    job_state["status"] = "running"
                    continue

                if condor_status == 5:
                    hold_reason = live_hold_reason.get((cluster_id, proc), "held without reported reason")
                    removed = _condor_rm_job(cluster_id, proc)
                    if removed:
                        self.publish_message(f"Job {idx} held and removed from queue: {hold_reason}")
                    else:
                        self.publish_message(f"Job {idx} held (could not confirm condor_rm): {hold_reason}")
                    self._try_resubmit(idx, job_state, reason=f"held: {hold_reason}")
                    continue

                if condor_status is None and exit_code is not None and exit_code != 0:
                    error_tail = _read_failed_job_error(self._run_dir, cluster_id, proc, idx)
                    if error_tail:
                        self.publish_message(
                            f"Job {idx} failed with exit code {exit_code}. stderr tail:\n{error_tail}"
                        )
                    self._try_resubmit(idx, job_state, reason=f"exit code {exit_code}")
                    continue

                if condor_status is None:
                    save_path = job_info[idx]["save"]
                    meta_path = job_info[idx]["meta"]
                    if (
                        (save_path and output_exists.get(save_path, False))
                        or (meta_path and output_exists.get(meta_path, False))
                    ):
                        job_state["status"] = "done"
                        self.publish_message(f"Job {idx}: output verified.")

            self._save_state(state)

            summary = self._status_summary(state)
            done = summary.get("done", 0)
            perm_fail = summary.get("perm_fail", 0)
            total = len(job_info)
            self.publish_message(
                f"Poll: {done}/{total} verified, {perm_fail} permanently failed, "
                f"{total - done - perm_fail} in-flight."
            )

            if done + perm_fail == total:
                break

            time.sleep(self.poll_interval)

        summary = self._status_summary(state)
        done = summary.get("done", 0)
        perm_fail = summary.get("perm_fail", 0)
        with self.output().open("w") as fh:
            fh.write(f"done={done}\n")
            fh.write(f"perm_fail={perm_fail}\n")
            fh.write(f"total={len(job_info)}\n")
            if perm_fail:
                failed = [
                    key for key, job_state in state["jobs"].items()
                    if job_state["status"] == "perm_fail"
                ]
                fh.write("perm_failed_jobs=" + ",".join(failed) + "\n")

        if perm_fail:
            self.publish_message(
                f"WARNING: {perm_fail} skim job(s) permanently failed. Details in {self._state_file}"
            )
        else:
            self.publish_message(f"All {done} skim output(s) verified. Monitoring complete.")

    def _try_resubmit(self, idx: int, job_state: dict, reason: str) -> None:
        retries = job_state["retries"]
        if retries >= self.max_retries:
            self.publish_message(
                f"Job {idx}: permanently failed ({reason}, {retries} retries exhausted)."
            )
            job_state["status"] = "perm_fail"
            return

        self.publish_message(
            f"Job {idx} failed ({reason}) - resubmitting "
            f"(attempt {retries + 1}/{self.max_retries})."
        )
        new_cluster = _submit_single_skim_job(
            idx,
            self._run_dir,
            self.max_runtime,
            2000,
            "shared_inputs",
            config_file="submit_config.txt",
        )
        if new_cluster:
            job_state.update(
                cluster=new_cluster,
                proc=0,
                retries=retries + 1,
                status="resubmitting",
            )
        else:
            job_state["status"] = "perm_fail"

class SkimTask(AnalysisMixin, SkimMixin, law.LocalWorkflow, HTCondorWorkflow, DaskWorkflow):
    """
    Run the analysis executable locally to produce skimmed ROOT files.

    One branch is created per dataset entry in the ``--dataset-manifest``.
    Each branch sets up a dedicated job directory under
    ``skimRun_{name}/jobs/{dataset_name}/``, writes a dataset-specific
    ``submit_config.txt`` derived from the ``--submit-config`` template, and
    runs the analysis executable.

    The analysis executable is invoked as::

        <exe> submit_config.txt

    inside the job directory.  The ``fileList``, ``saveFile``, ``metaFile``,
    ``saveDirectory``, ``sample``, and ``type`` keys are overridden
    automatically; all other keys in the template are forwarded unchanged.

    Outputs
    -------
    * ``skimRun_{name}/job_outputs/{dataset_name}.done``   – completion marker
    * ``skimRun_{name}/outputs/{dataset_name}/skim.root``  – skimmed tree
    * ``skimRun_{name}/outputs/{dataset_name}/meta.root``  – meta/counter file
    """

    task_namespace = ""

    # Parameters not meaningful at branch level (forwarded by the workflow
    # task but consumed only at workflow-scheduling time).
    exclude_params_branch = (
        getattr(law.LocalWorkflow, "exclude_params_branch", set())
        | getattr(HTCondorWorkflow, "exclude_params_branch", set())
        | getattr(DaskWorkflow, "exclude_params_branch", set())
        | {"dask_scheduler", "dask_workers", "max_runtime", "make_test_job"}
    )
    cache_branch_map_default = False
    reset_branch_map_before_run = True

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    # ------------------------------------------------------------------
    # Branch map: prefer the authoritative prep_submission.json produced by
    # PrepareSkimJobs, with deterministic fallbacks before that file exists.
    # ------------------------------------------------------------------

    def _fallback_branch_entries(self) -> list[dict[str, str]]:
        """Return deterministic branch metadata before prep_submission.json exists."""
        if self.file_source:
            fl_dir = self._file_list_dir()
            if os.path.isdir(fl_dir):
                return [
                    {
                        "dataset_name": str(entry["dataset_name"]),
                        "job_dir": str(entry["job_dir"]),
                        "out_dir": str(entry["out_dir"]),
                    }
                    for entry in _plan_file_source_jobs(
                        dataset_manifest_path=self.dataset_manifest,
                        file_list_dir=fl_dir,
                        partition=self.partition,
                        files_per_job=self.files_per_job,
                        entries_per_job=self.entries_per_job,
                        jobs_dir=self._jobs_dir,
                        outputs_dir=self._outputs_dir,
                    )
                ]

            manifest = DatasetManifest.load(self.dataset_manifest)
            return [
                {
                    "dataset_name": entry.name,
                    "job_dir": os.path.join(self._jobs_dir, entry.name, "job_0"),
                    "out_dir": os.path.join(self._outputs_dir, entry.name, "job_0"),
                }
                for entry in manifest.datasets
            ]

        manifest = DatasetManifest.load(self.dataset_manifest)
        return [
            {
                "dataset_name": entry.name,
                "job_dir": os.path.join(self._jobs_dir, entry.name),
                "out_dir": os.path.join(self._outputs_dir, entry.name),
            }
            for entry in manifest.datasets
        ]

    def _prepared_branch_entries(self) -> list[dict[str, str]]:
        """Return branch metadata from PrepareSkimJobs, or a deterministic fallback."""
        prep_json = os.path.join(self._run_dir, "prep_submission.json")
        if os.path.isfile(prep_json):
            with open(prep_json) as fh:
                payload = json.load(fh)
            if isinstance(payload, list):
                return [
                    {
                        "dataset_name": str(entry["dataset_name"]),
                        "job_dir": str(entry["job_dir"]),
                        "out_dir": str(entry["out_dir"]),
                    }
                    for entry in payload
                    if isinstance(entry, dict)
                ]
        return self._fallback_branch_entries()

    def create_branch_map(self):
        return {
            i: entry
            for i, entry in enumerate(self._prepared_branch_entries())
        }

    # ------------------------------------------------------------------
    # Workflow requirements
    # ------------------------------------------------------------------

    def workflow_requires(self):
        """Require prepared jobs before any executor dispatch begins."""
        reqs = super().workflow_requires()
        reqs["prep"] = PrepareSkimJobs.req(self)
        if self.make_test_job:
            reqs["test"] = RunSkimTestJob.req(self)
        return reqs

    def requires(self):
        reqs = [PrepareSkimJobs.req(self)]
        if self.make_test_job:
            reqs.append(RunSkimTestJob.req(self))
        if len(reqs) == 1:
            return reqs[0]
        return reqs

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def output(self):
        branch_data = self.branch_data
        if isinstance(branch_data, DatasetEntry):
            return law.LocalFileTarget(
                os.path.join(self._job_outputs_dir, f"{branch_data.name}.done")
            )
        branch_entry = cast(dict[str, str], branch_data)
        dataset_name = str(branch_entry["dataset_name"])
        if not self.file_source:
            return law.LocalFileTarget(
                os.path.join(self._job_outputs_dir, f"{dataset_name}.done")
            )
        return law.LocalFileTarget(
            os.path.join(
                self._job_outputs_dir, f"{dataset_name}_b{self.branch}.done"
            )
        )

    # ------------------------------------------------------------------
    # complete() – adds cache-validity check in standard mode
    # ------------------------------------------------------------------

    def complete(self):
        """Return True only when the `.done` marker and a valid cache sidecar exist.

        Cache validity is checked only in standard manifest mode (one job per
        dataset).  In file-source mode there is no canonical single skim file
        per dataset, so only the ``.done`` marker is checked.
        """
        if not self.is_branch():
            prep_json = os.path.join(self._run_dir, "prep_submission.json")
            if not os.path.exists(prep_json):
                return False
            return super().complete()
        if not super().complete():
            return False
        if self.file_source:
            return True

        branch_data = self.branch_data
        if isinstance(branch_data, DatasetEntry):
            dataset = branch_data
            dataset_name = dataset.name
            skim_file = os.path.join(self._outputs_dir, dataset.name, "skim.root")
        else:
            branch_entry = cast(dict[str, str], branch_data)
            dataset_name = str(branch_entry["dataset_name"])
            skim_file = os.path.join(str(branch_entry["out_dir"]), "skim.root")
            dataset = _find_dataset_entry(self.dataset_manifest, dataset_name)
            if dataset is None:
                return True

        current_prov = _build_task_provenance(
            submit_config_path=self.submit_config,
            dataset_manifest_path=self.dataset_manifest,
        )
        status = check_cache_validity(
            skim_file, current_provenance=current_prov, strict=True
        )

        if status == ArtifactResolutionStatus.COMPATIBLE:
            return True

        done_path = str(self.output().path)
        if os.path.exists(done_path):
            os.remove(done_path)
        self.publish_message(
            f"[SkimTask] Cache invalidated for '{dataset_name}' "
            f"(status={status.value}): the .done marker has been removed "
            "and the artifact will be regenerated."
        )
        return False

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(self):
        branch_data = self.branch_data

        if isinstance(branch_data, DatasetEntry):
            dataset = branch_data
            exe_path = os.path.abspath(self.exe)
            if not os.path.isfile(exe_path):
                raise RuntimeError(
                    f"Analysis executable not found: {exe_path!r}. "
                    "Build the analysis before running SkimTask."
                )
            if not dataset.files and not dataset.das:
                raise RuntimeError(
                    f"Dataset '{dataset.name}' has no files or DAS path defined "
                    "in the dataset manifest."
                )

            job_dir = os.path.join(self._jobs_dir, dataset.name)
            Path(job_dir).mkdir(parents=True, exist_ok=True)
            out_dir = os.path.join(self._outputs_dir, dataset.name)
            _write_job_config(
                job_dir=job_dir,
                template_config_path=self.submit_config,
                dataset=dataset,
                output_dir=out_dir,
            )

            task_label = f"SkimTask[{dataset.name}]"
            with PerformanceRecorder(task_label) as rec:
                result = _run_analysis_job(exe_path=exe_path, job_dir=job_dir)

            Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
            rec.save(perf_path_for(str(self.output().path)))

            skim_file = os.path.join(out_dir, "skim.root")
            if os.path.isfile(skim_file):
                mf = _build_skim_manifest(
                    dataset=dataset,
                    submit_config_path=self.submit_config,
                    dataset_manifest_path=self.dataset_manifest,
                    skim_output_file=skim_file,
                )
                write_cache_sidecar(skim_file, mf)
                self.publish_message(
                    f"[SkimTask] Cache sidecar written for '{dataset.name}': "
                    f"{skim_file}"
                )

            self.publish_message(f"Skim done for '{dataset.name}': {result}")
            with self.output().open("w") as fh:
                fh.write(f"status=done\ndataset={dataset.name}\n{result}\n")

        else:
            branch_entry = cast(dict[str, str], branch_data)
            dataset_name = str(branch_entry["dataset_name"])
            job_dir = str(branch_entry["job_dir"])
            out_dir = str(branch_entry["out_dir"])

            if not os.path.isdir(self._shared_dir):
                raise RuntimeError(
                    f"Shared inputs directory not found: {self._shared_dir!r}.\n"
                    "Ensure PrepareSkimJobs has completed successfully."
                )
            if not os.path.isdir(job_dir):
                raise RuntimeError(
                    f"Job directory not found: {job_dir!r}.\n"
                    "Ensure PrepareSkimJobs has completed successfully."
                )

            if not self.file_source:
                dataset = _find_dataset_entry(self.dataset_manifest, dataset_name)
                if dataset is not None and not dataset.files and not dataset.das:
                    raise RuntimeError(
                        f"Dataset '{dataset_name}' has no files or DAS path defined "
                        "in the dataset manifest."
                    )

            task_label = f"SkimTask[{dataset_name}/b{self.branch}]"
            with PerformanceRecorder(task_label) as rec:
                result = _run_prepared_skim_job(
                    shared_dir=self._shared_dir,
                    source_job_dir=job_dir,
                    exe_relpath=self._exe_relpath,
                    root_setup=self._root_setup_content,
                    container_setup=self.container_setup or "",
                )

            Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
            rec.save(perf_path_for(str(self.output().path)))

            if not self.file_source:
                dataset = _find_dataset_entry(self.dataset_manifest, dataset_name)
                skim_file = os.path.join(out_dir, "skim.root")
                if dataset is not None and os.path.isfile(skim_file):
                    mf = _build_skim_manifest(
                        dataset=dataset,
                        submit_config_path=self.submit_config,
                        dataset_manifest_path=self.dataset_manifest,
                        skim_output_file=skim_file,
                    )
                    write_cache_sidecar(skim_file, mf)
                    self.publish_message(
                        f"[SkimTask] Cache sidecar written for '{dataset_name}': "
                        f"{skim_file}"
                    )

            self.publish_message(
                f"Skim done for '{dataset_name}' branch {self.branch}: {result}"
            )
            with self.output().open("w") as fh:
                fh.write(
                    f"status=done\ndataset={dataset_name}\n"
                    f"branch={self.branch}\n{result}\n"
                )

    # ------------------------------------------------------------------
    # HTCondor workflow configuration (mirrors RunNANOJobs)
    # ------------------------------------------------------------------

    def htcondor_output_directory(self):
        """Directory for LAW's HTCondor job submission files."""
        return law.LocalDirectoryTarget(
            os.path.join(self._run_dir, "law_htcondor")
        )

    @property
    def _htcondor_submit_file(self) -> str:
        """Persistent HTCondor submit description used for debugging."""
        return os.path.join(self._run_dir, "condor_submit.sub")

    @property
    def _htcondor_runscript_path(self) -> str:
        """Persistent HTCondor worker wrapper used for debugging."""
        return os.path.join(self._run_dir, "condor_runscript.sh")

    def _ensure_htcondor_runscript(self) -> str:
        """Write the static HTCondor runscript used by remote skim jobs."""
        runscript_path = self._htcondor_runscript_path
        Path(self._run_dir).mkdir(parents=True, exist_ok=True)

        runscript = """#!/usr/bin/env bash
set -euo pipefail

export LAW_JOB_TASK_MODULE="$1"
export LAW_JOB_TASK_CLASS="$2"
export LAW_JOB_TASK_PARAMS="$( echo "$3" | base64 --decode )"
export LAW_JOB_TASK_BRANCHES="$( echo "$4" | base64 --decode )"
export LAW_JOB_TASK_BRANCHES_CSV="${LAW_JOB_TASK_BRANCHES// /,}"
export LAW_JOB_TASK_N_BRANCHES="$( echo "${LAW_JOB_TASK_BRANCHES}" | tr " " "\n" | wc -l | tr -d " " )"
export LAW_JOB_WORKERS="$5"
export LAW_JOB_AUTO_RETRY="$6"
export LAW_JOB_DASHBOARD_DATA="$( echo "$7" | base64 --decode )"

export LAW_HTCONDOR_JOB_NUMBER="${LAW_HTCONDOR_JOB_PROCESS:-${_CONDOR_PROCNO:-0}}"
LAW_HTCONDOR_JOB_NUMBER="$((LAW_HTCONDOR_JOB_NUMBER + 1))"

if [ -f "htcondor_bootstrap.sh" ]; then
    source "./htcondor_bootstrap.sh" ""
fi

branch_param="branch"
workflow_param=""
if [ "${LAW_JOB_TASK_N_BRANCHES}" != "1" ]; then
    branch_param="branches"
    workflow_param="--workflow=local"
fi

cmd="law run ${LAW_JOB_TASK_MODULE}.${LAW_JOB_TASK_CLASS} ${LAW_JOB_TASK_PARAMS} --${branch_param}=${LAW_JOB_TASK_BRANCHES_CSV} ${workflow_param} --workers=${LAW_JOB_WORKERS}"

echo "running condor_runscript.sh for job number ${LAW_HTCONDOR_JOB_NUMBER}"
echo "cmd: ${cmd}"
eval "${cmd}"
"""

        current = None
        if os.path.isfile(runscript_path):
            with open(runscript_path) as fh:
                current = fh.read()
        if current != runscript:
            with open(runscript_path, "w") as fh:
                fh.write(runscript)
        os.chmod(runscript_path, os.stat(runscript_path).st_mode | 0o110)
        return runscript_path

    def htcondor_create_job_file_factory(self, **kwargs):
        """Store concrete submit artifacts in the skim run directory."""
        Path(self._run_dir).mkdir(parents=True, exist_ok=True)
        kwargs.setdefault("dir", self._run_dir)
        kwargs.setdefault("mkdtemp", False)
        kwargs.setdefault("cleanup", False)
        kwargs.setdefault("file_name", os.path.basename(self._htcondor_submit_file))
        return super().htcondor_create_job_file_factory(**kwargs)

    def htcondor_group_wrapper_file(self):
        """Use a persistent runscript instead of LAW's transient group wrapper."""
        from law.job.base import JobInputFile  # type: ignore

        return JobInputFile(
            self._ensure_htcondor_runscript(),
            copy=True,
            render_local=False,
            render_job=False,
            share=True,
        )

    def htcondor_wrapper_file(self):
        """Use a persistent runscript instead of LAW's transient wrapper."""
        return self.htcondor_group_wrapper_file()

    def htcondor_job_file(self):
        """Execute the same persistent runscript as the remote job file."""
        return self.htcondor_group_wrapper_file()

    def htcondor_log_directory(self):
        """Directory for HTCondor log files."""
        return law.LocalDirectoryTarget(
            os.path.join(self._run_dir, "law_htcondor_logs")
        )

    def htcondor_bootstrap_file(self):
        """Optional bootstrap script executed on every HTCondor worker."""
        bootstrap_path = os.path.join(_HERE, "htcondor_bootstrap.sh")
        if os.path.isfile(bootstrap_path):
            from law.job.base import JobInputFile  # type: ignore
            return JobInputFile(bootstrap_path, copy=True, render_local=False)
        return None

    def htcondor_job_config(self, config, job_num, branches):
        """Configure per-job HTCondor resource requests.

        The ``shared_inputs/`` directory and per-branch prepared job
        directories are transferred explicitly so workers execute the same
        payload layout as local mode.
        """
        def _iter_branch_numbers(values):
            for value in values:
                if isinstance(value, (list, tuple, set)):
                    yield from _iter_branch_numbers(value)
                else:
                    yield value

        if not hasattr(config, "custom_content") or config.custom_content is None:
            config.custom_content = []
        config.custom_content.extend([
            ("+RequestMemory", str(2000)),
            ("+MaxRuntime", str(self.max_runtime)),
            ("request_cpus", "1"),
            ("request_disk", "20000"),
        ])
        if os.path.isdir(self._shared_dir):
            existing = {
                k: v
                for item in config.custom_content
                if isinstance(item, (list, tuple)) and len(item) == 2
                for k, v in [item]
            }
            xfer_items = [item for item in existing.get("transfer_input_files", "").split(",") if item]
            xfer_items.append(self._shared_dir)
            branch_map = self.create_branch_map()
            for branch in _iter_branch_numbers(branches):
                branch_data = branch_map.get(branch)
                if isinstance(branch_data, dict):
                    job_dir = str(branch_data.get("job_dir", ""))
                    if job_dir and os.path.isdir(job_dir):
                        xfer_items.append(job_dir)
            deduped_items: list[str] = []
            for item in xfer_items:
                if item not in deduped_items:
                    deduped_items.append(item)
            config.custom_content.append(
                ("transfer_input_files", ",".join(deduped_items))
            )
        return config

    def htcondor_workflow_requires(self):
        """Ensure prepared jobs and optional pre-flight validation exist first."""
        from law.util import DotDict  # type: ignore
        reqs = DotDict(prep=PrepareSkimJobs.req(self))
        if self.make_test_job:
            reqs["test"] = RunSkimTestJob.req(self)
        return reqs

    def dask_workflow_requires(self):
        """Ensure prepared jobs and optional pre-flight validation exist first."""
        from law.util import DotDict  # type: ignore
        reqs = DotDict(prep=PrepareSkimJobs.req(self))
        if self.make_test_job:
            reqs["test"] = RunSkimTestJob.req(self)
        return reqs

    # ------------------------------------------------------------------
    # Dask workflow configuration (mirrors RunNANOJobs)
    # ------------------------------------------------------------------

    def get_dask_work(self, branch_num: int, branch_data) -> tuple:
        """Return the (callable, args, kwargs) triple for Dask dispatch.

        Dict-style branch data runs via the same prepared-job sandbox used by
        local and HTCondor execution.  DatasetEntry support is retained for
        unit tests that patch branch_data directly.
        """
        if isinstance(branch_data, DatasetEntry):
            exe_path = os.path.abspath(self.exe)
            job_dir = os.path.join(self._jobs_dir, branch_data.name)
            return (
                _run_analysis_job,
                [exe_path, job_dir, self._root_setup_content,
                 self.container_setup or ""],
                {},
            )
        branch_entry = cast(dict[str, str], branch_data)
        return (
            _run_prepared_skim_job,
            [
                self._shared_dir,
                str(branch_entry["job_dir"]),
                self._exe_relpath,
                self._root_setup_content,
                self.container_setup or "",
            ],
            {},
        )


# ===========================================================================
# Task 2 – HistFillTask
# ===========================================================================

class HistFillTask(AnalysisMixin, law.LocalWorkflow):
    """
    Run the analysis executable locally to fill analysis histograms.

    Structurally identical to :class:`SkimTask` but targets the histogram-fill
    pass of the analysis.  When ``--skim-name`` is provided the task declares
    :class:`SkimTask` as a dependency and automatically wires the skim output
    directory as the ``fileList`` for each dataset job.

    One branch is created per dataset entry in the ``--dataset-manifest``.
    Outputs land in ``histRun_{name}/``.

    Parameters
    ----------
    skim_name:
        Optional name of a previously run :class:`SkimTask`.  When set, the
        task requires that ``SkimTask --name <skim_name>`` has completed and
        uses the skimmed ROOT files from
        ``skimRun_{skim_name}/outputs/{dataset_name}/skim.root`` as the
        per-dataset input file list.
    """

    task_namespace = ""

    skim_name = luigi.Parameter(
        default="",
        description=(
            "Name of a completed SkimTask run whose outputs should be used "
            "as input files for this histogram fill pass.  "
            "When set, HistFillTask requires SkimTask --name <skim_name> to "
            "have completed first and reads skim output ROOT files from "
            "``skimRun_{skim_name}/outputs/{dataset}/skim.root``.  "
            "Leave empty to use the ``fileList`` from the submit_config template."
        ),
    )

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"histRun_{self.name}")

    @property
    def _skim_run_dir(self) -> str:
        """Root directory of the referenced SkimTask run (when skim_name is set)."""
        return os.path.join(WORKSPACE, f"skimRun_{self.skim_name}")

    # ---- dependency --------------------------------------------------------

    def workflow_requires(self):
        reqs = super().workflow_requires()
        if self.skim_name:
            reqs["skim"] = SkimTask.req(
                self,
                name=self.skim_name,
                dataset_manifest=self.dataset_manifest,
                submit_config=self.submit_config,
                exe=self.exe,
            )
        return reqs

    def requires(self):
        if self.skim_name:
            return SkimTask.req(
                self,
                name=self.skim_name,
                dataset_manifest=self.dataset_manifest,
                submit_config=self.submit_config,
                exe=self.exe,
            )
        return None

    # ---- output ------------------------------------------------------------

    def create_branch_map(self):
        manifest = DatasetManifest.load(self.dataset_manifest)
        return {i: entry for i, entry in enumerate(manifest.datasets)}

    def output(self):
        dataset: DatasetEntry = self.branch_data
        return law.LocalFileTarget(
            os.path.join(self._job_outputs_dir, f"{dataset.name}.done")
        )

    # ---- run ---------------------------------------------------------------

    def run(self):
        dataset: DatasetEntry = self.branch_data

        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Analysis executable not found: {exe_path!r}. "
                "Build the analysis before running HistFillTask."
            )

        # ---- determine input files -----------------------------------------
        extra_overrides: dict[str, str] = {}
        if self.skim_name:
            skim_file = os.path.join(
                self._skim_run_dir, "outputs", dataset.name, "skim.root"
            )
            if not os.path.isfile(skim_file):
                raise RuntimeError(
                    f"Skim output not found for dataset '{dataset.name}': "
                    f"{skim_file!r}.  Run SkimTask --name {self.skim_name!r} first."
                )

            # ---- validate skim cache before consuming it -------------------
            current_prov = _build_task_provenance(
                submit_config_path=self.submit_config,
                dataset_manifest_path=self.dataset_manifest,
            )
            cache_status = check_cache_validity(
                skim_file, current_provenance=current_prov
            )
            if cache_status == ArtifactResolutionStatus.COMPATIBLE:
                self.publish_message(
                    f"[HistFillTask] Skim cache compatible for '{dataset.name}': "
                    f"reusing {skim_file}"
                )
            elif cache_status == ArtifactResolutionStatus.STALE:
                self.publish_message(
                    f"[HistFillTask] WARNING: Skim cache stale for '{dataset.name}': "
                    "provenance has changed but schema is current.  "
                    f"Proceeding with existing skim at {skim_file}.  "
                    "Re-run SkimTask to regenerate with current provenance."
                )
            else:
                # MUST_REGENERATE – the skim file exists but the sidecar is
                # absent or records an incompatible schema version.  Warn and
                # proceed; the file can still be consumed, but provenance
                # cannot be verified.
                self.publish_message(
                    f"[HistFillTask] WARNING: Skim cache unverifiable for "
                    f"'{dataset.name}' (status={cache_status.value}): "
                    "no valid cache sidecar found.  "
                    f"Proceeding with {skim_file}.  "
                    "Re-run SkimTask to write a provenance sidecar."
                )

            extra_overrides["fileList"] = skim_file
        elif not dataset.files and not dataset.das:
            raise RuntimeError(
                f"Dataset '{dataset.name}' has no files or DAS path defined "
                "in the dataset manifest and no --skim-name was provided."
            )

        # ---- set up job directory ------------------------------------------
        job_dir = os.path.join(self._jobs_dir, dataset.name)
        Path(job_dir).mkdir(parents=True, exist_ok=True)

        out_dir = os.path.join(self._outputs_dir, dataset.name)
        _write_job_config(
            job_dir=job_dir,
            template_config_path=self.submit_config,
            dataset=dataset,
            output_dir=out_dir,
            extra_overrides=extra_overrides,
        )

        # ---- run the analysis executable -----------------------------------
        task_label = f"HistFillTask[{dataset.name}]"
        with PerformanceRecorder(task_label) as rec:
            result = _run_analysis_job(exe_path=exe_path, job_dir=job_dir)

        Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
        rec.save(perf_path_for(self.output().path))

        self.publish_message(f"Histogram fill done for '{dataset.name}': {result}")
        with self.output().open("w") as fh:
            fh.write(f"status=done\ndataset={dataset.name}\n{result}\n")


# ===========================================================================
# Task 3 – StitchingDerivationTask
# ===========================================================================

def _read_intweight_sign_hist(
    meta_file: str,
    sample_name: str,
) -> list[float]:
    """Read ``counter_intWeightSignSum_{sample_name}`` from *meta_file*.

    **uproot is the preferred reader** because it requires no ROOT installation
    and runs in any Python environment.  PyROOT is used as a fallback for
    environments (e.g. CMSSW) where uproot is unavailable.

    Parameters
    ----------
    meta_file:
        Path to a merged meta ROOT file containing CounterService histograms.
    sample_name:
        The sample name key embedded in the histogram name, i.e. the value
        of the ``sample`` / ``type`` config key used during the analysis run.

    Returns
    -------
    list[float]
        Bin contents of ``counter_intWeightSignSum_{sample_name}`` ordered by
        bin index (bin k corresponds to stitch_id = k).

    Raises
    ------
    RuntimeError
        If the histogram is not found in the file, or neither uproot nor
        PyROOT is available.
    """
    hist_name = f"counter_intWeightSignSum_{sample_name}"

    # ---- uproot (preferred) -----------------------------------------------
    try:
        import uproot  # type: ignore

        with uproot.open(meta_file) as root_file:
            # uproot key strings include cycle numbers, e.g. "histName;1"
            keys_no_cycle = {k.split(";")[0]: k for k in root_file.keys()}
            if hist_name not in keys_no_cycle:
                raise RuntimeError(
                    f"Histogram '{hist_name}' not found in '{meta_file}'. "
                    f"Available histograms: {sorted(keys_no_cycle)}"
                )
            return root_file[keys_no_cycle[hist_name]].values().tolist()

    except ImportError:
        pass  # fall through to PyROOT

    # ---- PyROOT fallback --------------------------------------------------
    try:
        import ROOT  # type: ignore

        tfile = ROOT.TFile.Open(meta_file, "READ")
        if not tfile or tfile.IsZombie():
            raise RuntimeError(f"Cannot open ROOT file: '{meta_file}'")
        hist = tfile.Get(hist_name)
        if not hist:
            tfile.Close()
            raise RuntimeError(
                f"Histogram '{hist_name}' not found in '{meta_file}'."
            )
        values = [hist.GetBinContent(b) for b in range(1, hist.GetNbinsX() + 1)]
        tfile.Close()
        return values

    except ImportError:
        raise RuntimeError(
            "Neither uproot nor ROOT (PyROOT) is importable. "
            "Install uproot (``pip install uproot``) to read counter histograms "
            "without a full ROOT installation."
        )


def _derive_stitch_weights(
    group_name: str,
    meta_files: dict[str, str],
) -> dict[str, list[float]]:
    """Compute per-bin stitching scale factors for one group.

    For each stitch bin *k*, the scale factor for sample *n* is::

        b_n(k) = C_n(k) / Σ_m C_m(k)

    where ``C_n(k)`` is the net signed count (positive minus negative weight
    events) in bin *k* of sample *n*'s
    ``counter_intWeightSignSum_{sample_name}`` histogram.

    Bins where all samples have ``C_m(k) = 0`` (no events contribute) are
    assigned a scale factor of 0 for every sample.

    Parameters
    ----------
    group_name:
        Human-readable name for the group (used only in error messages).
    meta_files:
        Mapping from ``sample_name`` to absolute path of the merged meta
        ROOT file for that sample.

    Returns
    -------
    dict[str, list[float]]
        Mapping from ``sample_name`` to its array of per-bin scale factors.
        The arrays have the same length as the histogram (up to the last
        non-zero bin across all samples; trailing zero-only bins are trimmed
        to keep the correctionlib output compact).
    """
    import numpy as np  # type: ignore

    sample_names = list(meta_files.keys())

    # ---- read C_n(k) for every sample in the group -------------------------
    sign_counts: dict[str, "np.ndarray"] = {}
    ref_len: Optional[int] = None

    for sample_name, meta_file in meta_files.items():
        if not os.path.isfile(meta_file):
            raise RuntimeError(
                f"[StitchingDerivationTask] Meta file not found for sample "
                f"'{sample_name}' in group '{group_name}': '{meta_file}'"
            )
        values = _read_intweight_sign_hist(meta_file, sample_name)
        arr = np.array(values, dtype=np.float64)

        if ref_len is None:
            ref_len = len(arr)
        elif len(arr) != ref_len:
            raise RuntimeError(
                f"[StitchingDerivationTask] Histogram length mismatch in "
                f"group '{group_name}': sample '{sample_name}' has "
                f"{len(arr)} bins but previous samples had {ref_len} bins. "
                "All samples in a group must use the same stitch variable "
                "with the same binning."
            )
        sign_counts[sample_name] = arr

    assert ref_len is not None  # guaranteed: meta_files is non-empty

    # ---- Σ_m C_m(k) for each bin k ----------------------------------------
    total = np.zeros(ref_len, dtype=np.float64)
    for arr in sign_counts.values():
        total += arr

    # ---- b_n(k) = C_n(k) / Σ_m C_m(k); guard against division by zero ----
    # np.where evaluates both branches before masking, so suppress the
    # expected divide-by-zero warning for bins where total = 0.
    scale_factors: dict[str, "np.ndarray"] = {}
    with np.errstate(invalid="ignore", divide="ignore"):
        for sample_name in sample_names:
            c_n = sign_counts[sample_name]
            scale_factors[sample_name] = np.where(total != 0.0, c_n / total, 0.0)

    # ---- trim trailing all-zero bins to keep the correctionlib file compact -
    # Find the highest bin index where at least one sample has a non-zero total
    non_zero_mask = total != 0.0
    if np.any(non_zero_mask):
        last_active = int(np.where(non_zero_mask)[0][-1])
        n_used = last_active + 1
    else:
        # All bins are zero; keep at least one bin so correctionlib is valid
        n_used = 1

    return {
        name: arr[:n_used].tolist()
        for name, arr in scale_factors.items()
    }


def _build_correctionlib_cset(
    all_scale_factors: dict[str, dict[str, list[float]]],
    description: str = "Stitching scale factors derived from counter_intWeightSignSum histograms",
) -> "correctionlib.schemav2.CorrectionSet":
    """Build a correctionlib :class:`CorrectionSet` from derived scale factors.

    One :class:`~correctionlib.schemav2.Correction` is created per group.
    Each correction takes two inputs:

    * ``sample_name`` (string) – identifies which sample's factors to return
    * ``stitch_id``   (int)    – integer bin from ``counterIntWeightBranch``

    and returns the stitching scale factor ``b_n(stitch_id)``.

    The internal structure is a ``Category`` node (keyed by ``sample_name``)
    containing per-sample ``Binning`` nodes (keyed by integer ``stitch_id``
    value).  Out-of-range stitch IDs are clamped to the nearest edge.

    Parameters
    ----------
    all_scale_factors:
        ``{group_name: {sample_name: [b_n(0), b_n(1), ...]}}``
    description:
        Top-level description string for the ``CorrectionSet``.

    Returns
    -------
    correctionlib.schemav2.CorrectionSet
    """
    try:
        import correctionlib.schemav2 as cs  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "correctionlib is required to write stitching scale factors as a "
            "correction JSON.  Install it with: pip install correctionlib"
        ) from exc

    corrections = []

    for group_name, sample_factors in all_scale_factors.items():
        # All arrays in a group have the same trimmed length
        n_bins = max(len(v) for v in sample_factors.values())
        # Integer edges: stitch_id=k maps to interval [k, k+1)
        edges = [float(k) for k in range(n_bins + 1)]

        cat_items = []
        for sample_name, factors in sample_factors.items():
            # Pad to n_bins if a sample's array is shorter (shouldn't normally
            # happen after trimming, but defensively handled)
            padded = factors + [0.0] * (n_bins - len(factors))
            binning = cs.Binning(
                nodetype="binning",
                input="stitch_id",
                edges=edges,
                content=padded,
                flow="clamp",
            )
            cat_items.append(cs.CategoryItem(key=sample_name, value=binning))

        correction = cs.Correction(
            name=group_name,
            description=(
                f"Binwise stitching scale factors for group '{group_name}'. "
                f"Evaluate: cset[\"{group_name}\"].evaluate(sample_name, stitch_id). "
                "b_n(k) = C_n(k) / sum_m C_m(k) where C_n(k) is the net signed "
                "event count (positive minus negative weight events) for sample n "
                "in stitch bin k."
            ),
            version=1,
            inputs=[
                cs.Variable(
                    name="sample_name",
                    type="string",
                    description="Sample identifier (matches meta_files key in stitch config)",
                ),
                cs.Variable(
                    name="stitch_id",
                    type="int",
                    description=(
                        "Integer stitching bin ID stored per event by "
                        "counterIntWeightBranch"
                    ),
                ),
            ],
            output=cs.Variable(
                name="weight",
                type="real",
                description="Stitching scale factor b_n(k) = C_n(k) / sum_m C_m(k)",
            ),
            data=cs.Category(
                nodetype="category",
                input="sample_name",
                content=cat_items,
            ),
        )
        corrections.append(correction)

    return cs.CorrectionSet(
        schema_version=2,
        description=description,
        corrections=corrections,
    )


class StitchingDerivationTask(law.Task):
    """
    Derive binwise MC stitching scale factors from counter histograms.

    This task reads the ``counter_intWeightSignSum_{sample}`` histograms
    written by the framework's :class:`CounterService` from the merged meta
    ROOT files of a set of stitched MC samples and computes the binwise
    scale factor

        b_n(k) = C_n(k) / Σ_m C_m(k)

    where ``C_n(k) = P_n(k) − N_n(k)`` is the net signed count (positive
    minus negative weight events) for sample *n* in stitch bin *k*.  See the
    module docstring for the full mathematical derivation.

    ROOT files are read with **uproot** (preferred); PyROOT is used as a
    fallback in environments where uproot is unavailable.

    The groups to process and the path to each sample's meta ROOT file are
    specified in a YAML configuration file (``--stitch-config``).  Results are
    written as a **correctionlib** ``CorrectionSet`` JSON (``stitch_weights.json``)
    so they can be consumed directly by the analysis framework's
    :class:`CorrectionManager`.

    Stitch config YAML format
    -------------------------
    ::

        groups:
          wjets_ht:
            meta_files:
              wjets_ht_0: /path/to/merged/wjets_ht_0_meta.root
              wjets_ht_1: /path/to/merged/wjets_ht_1_meta.root
              wjets_ht_2: /path/to/merged/wjets_ht_2_meta.root

    Each key under ``meta_files`` must match the ``sample`` / ``type`` value
    used in the analysis config that produced the meta ROOT file, because
    CounterService embeds that value in the histogram name
    (``counter_intWeightSignSum_{sample}``).

    Output: correctionlib JSON (``stitch_weights.json``)
    -----------------------------------------------------
    One ``Correction`` per group with two inputs:

    * ``sample_name`` (string) – sample identifier
    * ``stitch_id``   (int)    – integer stitch bin value from the event stream

    Usage from Python::

        import correctionlib
        cset = correctionlib.CorrectionSet.from_file("stitch_weights.json")
        sf = cset["wjets_ht"].evaluate("wjets_ht_0", stitch_id)

    Usage from C++ via CorrectionManager: load the JSON file via
    ``correctionlibConfig`` and call ``evaluate`` with the sample name and
    ``counterIntWeightBranch`` value.
    """

    task_namespace = ""

    stitch_config = luigi.Parameter(
        description=(
            "Path to the YAML stitching configuration file that lists the "
            "sample groups and meta ROOT file paths.  "
            "See the task docstring for the expected file format."
        ),
    )
    name = luigi.Parameter(
        description=(
            "Run name used to namespace the output directory. "
            "Output is written to ``stitchWeights_{name}/``."
        ),
    )

    @property
    def _output_dir(self) -> str:
        return os.path.join(WORKSPACE, f"stitchWeights_{self.name}")

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._output_dir, "stitch_weights.json")
        )

    def run(self):
        import yaml  # type: ignore

        config_path = os.path.abspath(self.stitch_config)
        if not os.path.isfile(config_path):
            raise RuntimeError(
                f"Stitch configuration file not found: '{config_path}'"
            )

        with open(config_path) as fh:
            config = yaml.safe_load(fh)

        if "groups" not in config or not isinstance(config["groups"], dict):
            raise RuntimeError(
                f"Stitch config '{config_path}' must contain a top-level "
                "'groups' mapping.  See the task docstring for the format."
            )

        all_scale_factors: dict[str, dict[str, list[float]]] = {}

        with PerformanceRecorder("StitchingDerivationTask") as rec:
            for group_name, group_cfg in config["groups"].items():
                if "meta_files" not in group_cfg or not isinstance(
                    group_cfg["meta_files"], dict
                ):
                    raise RuntimeError(
                        f"Group '{group_name}' in '{config_path}' must contain a "
                        "'meta_files' mapping of sample_name → path."
                    )

                meta_files: dict[str, str] = {
                    str(k): str(v) for k, v in group_cfg["meta_files"].items()
                }

                self.publish_message(
                    f"Processing group '{group_name}' "
                    f"({len(meta_files)} sample(s): {list(meta_files)})…"
                )

                scale_factors = _derive_stitch_weights(group_name, meta_files)
                all_scale_factors[group_name] = scale_factors

                self.publish_message(
                    f"  group '{group_name}': scale factors computed for "
                    f"{len(scale_factors)} sample(s)."
                )

        # ---- build and write correctionlib JSON ----------------------------
        Path(self._output_dir).mkdir(parents=True, exist_ok=True)

        cset = _build_correctionlib_cset(all_scale_factors)
        json_path = self.output().path
        with open(json_path, "w") as fh:
            fh.write(cset.model_dump_json(indent=2))

        rec.save(os.path.join(self._output_dir, "stitch_derivation.perf.json"))
        self.publish_message(
            f"Stitch weights (correctionlib) written to: {json_path}"
        )


# ===========================================================================
# Task 4 – CollectHistogramsTask
# ===========================================================================

class CollectHistogramsTask(law.Task):
    """Merge per-dataset histogram ROOT files produced by a SkimTask run.

    Requires a completed :class:`SkimTask` (identified by ``--skim-name``) and
    collects all ``skim.root`` files from its output directories, then merges
    them into a single ``histograms.root`` using ``hadd``.

    When the SkimTask was run in **file-source mode** (``--file-source`` set),
    branch output directories follow the pattern
    ``skimRun_{skim_name}/outputs/{dataset}/branch_*/skim.root``, which this
    task discovers automatically.

    Output
    ------
    * ``histCollect_{name}/histograms.root`` – merged histogram file
    * ``histCollect_{name}/collect.done``    – completion marker

    Parameters
    ----------
    name:
        Run name; output goes to ``histCollect_{name}/``.
    skim_name:
        Name of the completed :class:`SkimTask` run to collect from.
    dataset_manifest:
        Path to the dataset manifest YAML (forwarded to :class:`SkimTask`).
    """

    task_namespace = ""

    name = luigi.Parameter(
        description=(
            "Run name used to namespace the output directory. "
            "Output is written to ``histCollect_{name}/``."
        ),
    )
    skim_name = luigi.Parameter(
        description="Name of the completed SkimTask run to merge histograms from.",
    )
    dataset_manifest = luigi.Parameter(
        description="Path to the dataset manifest YAML (forwarded to SkimTask).",
    )
    submit_config = luigi.Parameter(
        default="",
        description=(
            "Path to the submit_config.txt template (forwarded to SkimTask "
            "for dependency resolution; not used directly by this task)."
        ),
    )
    exe = luigi.Parameter(
        default="",
        description=(
            "Path to the analysis executable (forwarded to SkimTask "
            "for dependency resolution; not used directly by this task)."
        ),
    )

    @property
    def _output_dir(self) -> str:
        return os.path.join(WORKSPACE, f"histCollect_{self.name}")

    @property
    def _skim_outputs_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.skim_name}", "outputs")

    def requires(self):
        kwargs: dict = dict(
            name=self.skim_name,
            dataset_manifest=self.dataset_manifest,
        )
        if self.submit_config:
            kwargs["submit_config"] = self.submit_config
        if self.exe:
            kwargs["exe"] = self.exe
        return SkimTask.req(self, **kwargs)

    def output(self):
        return {
            "histogram": law.LocalFileTarget(
                os.path.join(self._output_dir, "histograms.root")
            ),
            "done": law.LocalFileTarget(
                os.path.join(self._output_dir, "collect.done")
            ),
        }

    def run(self):
        # Discover all skim.root files under the skim outputs directory
        skim_files = sorted(
            str(p)
            for p in Path(self._skim_outputs_dir).rglob("skim.root")
            if p.is_file()
        )

        if not skim_files:
            raise RuntimeError(
                f"No skim.root files found under '{self._skim_outputs_dir}'. "
                f"Ensure SkimTask --name {self.skim_name!r} has completed successfully."
            )

        self.publish_message(
            f"CollectHistogramsTask: merging {len(skim_files)} skim file(s) "
            f"into {self._output_dir}/histograms.root"
        )

        Path(self._output_dir).mkdir(parents=True, exist_ok=True)
        merged_path = self.output()["histogram"].path

        # Use hadd to merge histogram ROOT files
        cmd = ["hadd", "-f", merged_path] + skim_files
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"hadd failed (exit {result.returncode}):\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )

        self.publish_message(
            f"CollectHistogramsTask: merged histogram written to {merged_path}"
        )

        with self.output()["done"].open("w") as fh:
            fh.write(
                f"status=done\nskim_name={self.skim_name}\n"
                f"files_merged={len(skim_files)}\n"
            )
