"""
Law tasks for running analysis jobs locally and deriving MC stitching weights.

Provides workflow helpers that extend the typical analysis workflow:

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

    StitchingApplyTask  (SkimTask)
            → Runs a counter pass, derives correctionlib stitching weights, and
                runs a second application pass with the derived JSON staged once in
                the shared worker archive.

    StitchingValidationTask  (Task)
            → Validates every application metadata file, including file-source
                partitions, for the derived correction and stitched histograms.

  StitchingDerivationTask  (Task)
      → Derives binwise MC stitching scale factors from counter histograms
        written by the framework's ``CounterService``.
      → For each group of stitched MC samples the task reads
        ``counter_intWeightSignSum_{sample}`` histograms from the merged meta
        ROOT files using **uproot** (preferred), with a PyROOT fallback for
        environments where uproot is unavailable.
            → Computes the net signed counts ``C_n(k) = P_n(k) − N_n(k)`` and treats
                negative net bins as zero when deriving the per-bin scale factor

            b_n(k) = max(C_n(k), 0) / Σ_m max(C_m(k), 0)

        Each sample's scale factor equals its share of the nonnegative net
        signed counts in that stitch bin, ensuring physical normalisation
        across overlapping phase-space samples (e.g. HT-binned W+jets).
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

    b_n = max(C_n, 0) / Σ_m max(C_m, 0)

For the integer-binned (stitch_id) case the same formula is applied
independently in each histogram bin k:

    b_n(k) = max(C_n(k), 0) / Σ_m max(C_m(k), 0)

where k is the integer stitch_id value stored per event by
``counterIntWeightBranch`` and counted by ``CounterService``.

Usage
-----
::

    source core/python/law/env.sh

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
                inclusive_samples:
                    - wjets_inclusive_axis0
                    - wjets_inclusive_axis1

Each key under ``meta_files`` is the ``sampleName`` used by CounterService
when booking the ``counter_intWeightSignSum_{sampleName}`` histogram.  This
matches the ``sample`` / ``type`` key in the analysis submit_config.txt.
``inclusive_sample`` accepts one sample name, while ``inclusive_samples`` (or
the plural form nested under a cross-section era) accepts a flat or nested
collection of samples. Their signed counters are summed before the inclusive
bin fractions are calculated. Inclusive cross sections must be configured in
the stitch YAML; Core does not provide process-specific defaults.

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
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import luigi  # type: ignore
import law  # type: ignore
import yaml
from law.util import DotDict  # type: ignore
from law.workflow.base import BaseWorkflow, BaseWorkflowProxy  # type: ignore

if TYPE_CHECKING:
    import correctionlib.schemav2

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402
from dataset_manifest import DatasetManifest, DatasetEntry  # noqa: E402
from derivation_correctionlib import (  # noqa: E402
    _build_correctionlib_cset as _shared_build_correctionlib_cset,
    _derive_stitch_cross_sections as _shared_derive_stitch_cross_sections,
    _derive_stitch_weights as _shared_derive_stitch_weights,
    _normalize_sample_names as _shared_normalize_sample_names,
    _read_intweight_sign_hist as _shared_read_intweight_sign_hist,
    _read_weight_sum as _shared_read_weight_sum,
    _read_weight_sign_sum as _shared_read_weight_sign_sum,
)
from stitching_plotting import plot_ratio_comparison as _plot_ratio_comparison  # noqa: E402
from submission_backend import (  # noqa: E402
    ensure_xrootd_redirector,
    get_copy_file_list,
    read_config,
    write_config,
    write_submit_files,
)
from workflow_executors import DaskWorkflow, HTCondorWorkflow, _run_analysis_job  # noqa: E402
from partition_utils import _make_partitions  # noqa: E402
from output_schema import (  # noqa: E402
    ArtifactResolutionStatus,
    CutflowSchema,
    DatasetManifestProvenance,
    HistogramAxisSpec,
    HistogramSchema,
    IntermediateArtifactSchema,
    MetadataSchema,
    OutputManifest,
    ProvenanceRecord,
    RegionDefinition,
    SkimSchema,
    check_cache_validity,
    write_cache_sidecar,
)

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

_MC_GENERATOR_CONFIG_KEYS = (
    "counterWeightBranch",
    "genWeightBaseWeightColumn",
    "genWeightNominalColumn",
    "genWeightLheScaleBranch",
    "genWeightPsBranch",
    "genWeightPdfBranch",
    "genWeightEnableLheScale",
    "genWeightEnablePs",
    "genWeightEnablePdf",
    "genWeightEnablePdfAs",
)


def _remove_mc_generator_config_for_data(config: dict[str, str]) -> None:
    """Remove generator-weight settings from data job configurations."""
    if str(config.get("dtype", "")).lower() != "data":
        return
    for key in _MC_GENERATOR_CONFIG_KEYS:
        config.pop(key, None)

from ingestion_utils import (  # noqa: E402
    collect_local_shared_libs as _collect_skim_shared_libs,
    condor_history_exit as _condor_history_exit,
    condor_q_ads as _condor_q_ads,
    condor_rm_job as _condor_rm_job,
    eos_files_exist_batch as _eos_files_exist_batch_util,
    read_failed_job_error as _read_failed_job_error,
)


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


def _manifest_lumi_for_dataset(
    dataset_manifest_path: str, dataset: DatasetEntry
) -> Optional[float]:
    """Return manifest luminosity when the manifest is available."""
    if not dataset_manifest_path or not os.path.isfile(dataset_manifest_path):
        return None
    try:
        manifest = DatasetManifest.load(dataset_manifest_path)
    except (OSError, ValueError, TypeError):
        return None
    return manifest.lumi_for(dataset.year, dataset.era)


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
    candidates = [os.path.abspath(path_value)]

    current_dir = template_dir
    visited: set[str] = set()
    while current_dir not in visited:
        visited.add(current_dir)
        candidates.append(os.path.join(current_dir, path_value))

        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            break
        current_dir = parent_dir

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


def _symlink_or_copy_path(src_path: str, dst_path: str) -> None:
    """Create *dst_path* from *src_path*, preferring symlinks and falling back to copies."""
    if os.path.exists(dst_path) or os.path.islink(dst_path):
        return

    src_abs = os.path.abspath(src_path)
    try:
        os.symlink(src_abs, dst_path)
    except OSError:
        if os.path.isdir(src_abs):
            shutil.copytree(src_abs, dst_path, dirs_exist_ok=True)
        else:
            Path(os.path.dirname(dst_path)).mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_abs, dst_path)


def _ensure_local_analysis_runtime_layout(
    template_config_path: str,
    config_dict: dict[str, str],
    destination_dir: str,
    include_aux: bool = True,
) -> None:
    """Populate local job directories with referenced cfg/ and aux payloads."""
    cfg_dir = os.path.join(destination_dir, "cfg")
    Path(cfg_dir).mkdir(parents=True, exist_ok=True)

    for path_value in get_copy_file_list(config_dict):
        src = _resolve_analysis_config_path(template_config_path, path_value)
        if not src or not os.path.isfile(src):
            continue
        dst = os.path.join(cfg_dir, os.path.basename(src))
        _symlink_or_copy_path(src, dst)

    if include_aux:
        aux_src = _analysis_aux_dir(template_config_path)
        if aux_src and os.path.isdir(aux_src):
            aux_dst = os.path.join(destination_dir, "aux")
            _symlink_or_copy_path(aux_src, aux_dst)


def _shared_inputs_archive_name() -> str:
    """Return the canonical filename for the skim shared-input archive."""
    return "shared_inputs.tar.gz"


def _build_shared_inputs_archive(shared_dir: str, archive_path: str) -> str:
    """Create a tar.gz archive with materialized cfg/aux and shared runtime files."""
    cfg_bundle = os.path.join(shared_dir, "cfg_bundle.tar.gz")
    aux_bundle = os.path.join(shared_dir, "aux_bundle.tar.gz")
    excluded_files = {
        "cfg_bundle.tar.gz",
        "aux_bundle.tar.gz",
        os.path.basename(archive_path),
    }

    with tempfile.TemporaryDirectory(prefix="skim_shared_inputs_") as tmpdir:
        Path(os.path.join(tmpdir, "cfg")).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(tmpdir, "aux")).mkdir(parents=True, exist_ok=True)

        if os.path.isfile(cfg_bundle):
            _extract_tar_archive(cfg_bundle, tmpdir)
        if os.path.isfile(aux_bundle):
            _extract_tar_archive(aux_bundle, tmpdir)

        with tarfile.open(archive_path, "w:gz") as archive:
            for entry_name in ("cfg", "aux"):
                entry_path = os.path.join(tmpdir, entry_name)
                if os.path.isdir(entry_path):
                    archive.add(entry_path, arcname=entry_name)
            for entry in sorted(Path(shared_dir).iterdir()):
                if not entry.is_file() or entry.name in excluded_files:
                    continue
                archive.add(str(entry), arcname=entry.name)

    return archive_path


def _normalize_shared_config_references(
    template_config_path: str,
    job_config: dict[str, str],
) -> None:
    """Rewrite referenced payload files to the runtime cfg/ layout."""
    staged_payloads = set()
    for path_value in get_copy_file_list(job_config):
        if not path_value:
            continue
        source_path = _resolve_analysis_config_path(template_config_path, path_value)
        if os.path.isfile(source_path):
            staged_payloads.add(os.path.basename(source_path))
    for key, val in list(job_config.items()):
        if key.startswith("__"):
            continue
        if isinstance(val, str) and (
            val.endswith(".txt")
            or val.endswith(".yaml")
            or val.endswith(".yml")
            or val.endswith(".json")
            or val.endswith(".json.gz")
            or val.endswith(".onnx")
        ):
            basename = os.path.basename(val)
            if basename in staged_payloads:
                job_config[key] = os.path.join("cfg", basename)


def _resolve_runtime_transfer_path(path_value: str) -> str:
    """Resolve a runtime path, preferring local originals before transferred copies."""
    if path_value and os.path.exists(path_value):
        return path_value
    local_candidate = os.path.join(os.getcwd(), os.path.basename(path_value))
    if os.path.exists(local_candidate):
        return local_candidate
    return path_value


def _runtime_loose_cfg_files(exclude_names: Optional[set[str]] = None) -> list[str]:
    """Discover loose config payload files transferred into the current work dir."""
    exclude = set(exclude_names or set())
    discovered: list[str] = []
    for entry_name in sorted(os.listdir(os.getcwd())):
        if entry_name in exclude:
            continue
        entry_path = os.path.join(os.getcwd(), entry_name)
        if not os.path.isfile(entry_path):
            continue
        if not entry_name.lower().endswith((".txt", ".yaml", ".yml")):
            continue
        discovered.append(entry_path)
    return discovered


def _copy_runtime_loose_cfg_files(destination_cfg_dir: str, exclude_names: Optional[set[str]] = None) -> None:
    """Copy transferred loose config files into the runtime cfg/ directory."""
    for src_path in _runtime_loose_cfg_files(exclude_names=exclude_names):
        dst_path = os.path.join(destination_cfg_dir, os.path.basename(src_path))
        if os.path.exists(dst_path):
            continue
        shutil.copy2(src_path, dst_path)


def _materialize_runtime_staged_inputs(runtime_cfg_path: str, sandbox_dir: str) -> None:
    """Link staged worker input files into the runtime sandbox when present."""
    config_dict = read_config(runtime_cfg_path)
    file_list = str(config_dict.get("fileList", "") or "")
    if not file_list:
        return

    staged_names = [
        entry.strip()
        for entry in file_list.split(",")
        if re.fullmatch(r"input_\d+\.root", entry.strip())
    ]
    if not staged_names:
        return

    missing: list[str] = []
    for staged_name in staged_names:
        src_path = os.path.join(os.getcwd(), staged_name)
        dst_path = os.path.join(sandbox_dir, staged_name)
        if os.path.exists(dst_path):
            continue
        if not os.path.exists(src_path):
            missing.append(staged_name)
            continue
        try:
            os.symlink(src_path, dst_path)
        except OSError:
            shutil.copy2(src_path, dst_path)

    if not missing:
        return

    original_file_list = str(config_dict.get("__orig_fileList", "") or "")
    if original_file_list:
        config_dict["fileList"] = original_file_list
        write_config(config_dict, runtime_cfg_path)


def _skim_shared_loose_input_files(shared_dir: str) -> list[str]:
    """Return shared input files that should be transferred individually."""
    excluded = {
        "cfg_bundle.tar.gz",
        "aux_bundle.tar.gz",
        _shared_inputs_archive_name(),
    }
    return sorted(
        os.path.join(shared_dir, entry)
        for entry in os.listdir(shared_dir)
        if os.path.isfile(os.path.join(shared_dir, entry)) and entry not in excluded
    )


def _shared_archive_file_basenames(archive_path: str) -> set[str]:
    """Return file basenames already contained in a shared-input archive."""
    if not os.path.isfile(archive_path):
        return set()

    with tarfile.open(archive_path, "r:gz") as archive:
        return {
            os.path.basename(member.name)
            for member in archive.getmembers()
            if member.isfile() and os.path.basename(member.name)
        }


def _copy_job_submission_files(
    source_dir: str,
    destination_dir: str,
    shared_archive_file_names: Optional[set[str]] = None,
) -> None:
    """Copy per-job payloads without expanding shared configuration overlays.

    ``PrepareSkimJobs`` keeps a local ``cfg/`` overlay so local execution can
    use the same prepared job directory.  Remote skim jobs receive the
    equivalent payload through ``shared_inputs.tar.gz`` instead, so copying
    that overlay into every concrete Condor job would duplicate potentially
    large correction/configuration files.  Keep job-specific files (the
    submit config, redirector map, and any future per-job payloads) while
    omitting directories and files represented by the shared archive.
    """
    shared_names = set(shared_archive_file_names or set())
    per_job_names = {"submit_config.txt", "site_redirectors.json"}
    for entry in sorted(Path(source_dir).iterdir()):
        if entry.name in {"cfg", "aux"}:
            continue
        if entry.name in shared_names and entry.name not in per_job_names:
            continue

        destination = os.path.join(destination_dir, entry.name)
        if entry.is_dir():
            shutil.copytree(str(entry), destination, dirs_exist_ok=True)
        elif entry.is_file():
            shutil.copy2(str(entry), destination)


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
    source_config_path: Optional[str] = None,
    shared_archive_path: Optional[str] = None,
) -> tuple[str, str]:
    """Create a worker-local runtime directory with cfg/ and aux/ materialized."""
    runtime_shared_dir = _resolve_runtime_transfer_path(shared_dir)
    runtime_job_dir = _resolve_runtime_transfer_path(source_job_dir)
    runtime_shared_archive = ""
    if shared_archive_path:
        runtime_shared_archive = _resolve_runtime_transfer_path(shared_archive_path)

    if not os.path.isdir(runtime_shared_dir) and not (
        runtime_shared_archive and os.path.isfile(runtime_shared_archive)
    ):
        raise RuntimeError(
            "Shared inputs are not available at runtime. Expected either a shared "
            f"directory {runtime_shared_dir!r} or archive {runtime_shared_archive!r}."
        )
    if source_job_dir and not os.path.isdir(runtime_job_dir):
        runtime_job_dir = ""

    sandbox_dir = tempfile.mkdtemp(prefix="skim_runtime_")

    if runtime_shared_archive and os.path.isfile(runtime_shared_archive):
        _extract_tar_archive(runtime_shared_archive, sandbox_dir)

    if os.path.isdir(runtime_shared_dir):
        cfg_bundle = os.path.join(runtime_shared_dir, "cfg_bundle.tar.gz")
        if os.path.isfile(cfg_bundle):
            _extract_tar_archive(cfg_bundle, sandbox_dir)

        aux_bundle = os.path.join(runtime_shared_dir, "aux_bundle.tar.gz")
        if os.path.isfile(aux_bundle):
            _extract_tar_archive(aux_bundle, sandbox_dir)

    runtime_cfg_dir = os.path.join(sandbox_dir, "cfg")
    Path(runtime_cfg_dir).mkdir(parents=True, exist_ok=True)

    submit_config_src = ""
    if source_config_path:
        submit_config_src = _resolve_runtime_transfer_path(source_config_path)
    elif runtime_job_dir:
        submit_config_src = os.path.join(runtime_job_dir, "submit_config.txt")

    if not os.path.isfile(submit_config_src):
        raise RuntimeError(
            f"Per-job submit config not found at runtime: {submit_config_src!r}."
        )
    runtime_submit_config = os.path.join(runtime_cfg_dir, "submit_config.txt")
    shutil.copy2(submit_config_src, runtime_submit_config)

    if runtime_job_dir:
        job_cfg_overlay = os.path.join(runtime_job_dir, "cfg")
        if os.path.isdir(job_cfg_overlay):
            _copy_tree_contents(job_cfg_overlay, runtime_cfg_dir)

    _copy_runtime_loose_cfg_files(
        runtime_cfg_dir,
        exclude_names={os.path.basename(submit_config_src)},
    )

    _materialize_runtime_staged_inputs(runtime_submit_config, sandbox_dir)

    if os.path.isdir(runtime_shared_dir):
        for src_path in _skim_shared_loose_input_files(runtime_shared_dir):
            runtime_src_path = _resolve_runtime_transfer_path(src_path)
            if os.path.isfile(runtime_src_path):
                shutil.copy2(
                    runtime_src_path,
                    os.path.join(sandbox_dir, os.path.basename(src_path)),
                )

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
    source_config_path: Optional[str] = None,
    shared_archive_path: Optional[str] = None,
    stream_output: bool = False,
) -> str:
    """Run a prepared skim job after materializing a worker-local sandbox."""
    runtime_job_dir = None
    try:
        runtime_job_dir, exe_path = _materialize_file_source_runtime_job(
            shared_dir=shared_dir,
            source_job_dir=source_job_dir,
            exe_relpath=exe_relpath,
            source_config_path=source_config_path,
            shared_archive_path=shared_archive_path,
        )
        return _run_analysis_job(
            exe_path=exe_path,
            job_dir=runtime_job_dir,
            root_setup=root_setup,
            container_setup=container_setup,
            config_relpath=os.path.join("cfg", "submit_config.txt"),
            stream_output=stream_output,
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
    archive_path = os.path.join(os.path.dirname(shared_dir), _shared_inputs_archive_name())
    if os.path.isfile(archive_path):
        return [archive_path]
    return _skim_shared_loose_input_files(shared_dir)


def _output_files_exist_batch(output_paths: list[str]) -> dict[str, bool]:
    """Check a mixed list of local and XRootD output paths."""
    result: dict[str, bool] = {}
    eos_paths: list[str] = []

    for path in output_paths:
        if not path:
            result[path] = False
        elif path.startswith("root://") or path.startswith("/eos/") or path.startswith("/store/"):
            eos_paths.append(path)
        else:
            result[path] = os.path.exists(path)

    if eos_paths:
        result.update(_eos_files_exist_batch_util(eos_paths))

    return result


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
        cfg = read_config(os.path.join(job_dir, "submit_config.txt"))
        save = cfg.get("__orig_saveFile", cfg.get("saveFile", ""))
        meta = cfg.get("__orig_metaFile", cfg.get("metaFile", ""))
        info[idx] = {
            "dir": os.path.realpath(job_dir),
            "save": save,
            "meta": meta,
        }
        idx += 1
    return info


def _skim_job_file_list(job_dir: str) -> list[str]:
    """Return the concrete input files configured for a prepared skim job."""
    config_path = os.path.join(job_dir, "submit_config.txt")
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
) -> dict[str, str]:
    """Create an isolated per-file-group directory for a split retry."""
    resub_dir = os.path.join(submission_dir, "resubmissions")
    target_job_dir = os.path.join(
        resub_dir,
        f"job_{job_index}_retry{retry_attempt}_part{part_index}",
    )
    Path(resub_dir).mkdir(parents=True, exist_ok=True)
    if os.path.exists(target_job_dir):
        shutil.rmtree(target_job_dir)
    shutil.copytree(source_job_dir, target_job_dir)

    config_path = os.path.join(target_job_dir, "submit_config.txt")
    config = read_config(config_path)
    normalized_file_list = ",".join(file_list)
    config["fileList"] = normalized_file_list
    if "__orig_fileList" in config:
        config["__orig_fileList"] = normalized_file_list

    for key in ("saveFile", "metaFile", "__orig_saveFile", "__orig_metaFile"):
        if config.get(key):
            config[key] = _append_output_part_suffix(str(config[key]), part_index)

    write_config(config, config_path)
    return {
        "dir": os.path.realpath(target_job_dir),
        "save": str(config.get("__orig_saveFile", config.get("saveFile", ""))),
        "meta": str(config.get("__orig_metaFile", config.get("metaFile", ""))),
    }


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
        elapsed_runtime = float(ad.get("RemoteWallClockTime", 0) or 0)
    except (TypeError, ValueError):
        return False

    return assigned_runtime > 0 and elapsed_runtime > assigned_runtime * grace_factor


def _submit_single_skim_job(
    job_index: int,
    submission_dir: str,
    max_runtime: int,
    request_memory: int,
    extra_transfer_files: Optional[list[str]] = None,
    config_file: str = "submit_config.txt",
    source_job_dir: Optional[str] = None,
    log_label: Optional[str] = None,
) -> Optional[str]:
    """Submit a single prepared skim job for monitor-driven resubmission."""
    job_dir = source_job_dir or os.path.join(submission_dir, f"job_{job_index}")
    resub_dir = os.path.join(submission_dir, "resubmissions")
    Path(resub_dir).mkdir(parents=True, exist_ok=True)
    job_reference = os.path.relpath(job_dir, submission_dir).replace(os.sep, "/").rstrip("/") + "/"
    resub_label = log_label or f"resub_{job_index}"

    submit_template_path = os.path.join(submission_dir, "condor_submit.sub")
    log_base = os.path.join(submission_dir, "condor_logs")
    Path(log_base).mkdir(parents=True, exist_ok=True)

    if os.path.exists(submit_template_path):
        with open(submit_template_path) as fh:
            sub_content = fh.read()

        sub_content = sub_content.replace("job_$(Process)/", job_reference)
        sub_content = sub_content.replace("$(Process)", str(job_index))
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
environment = CONDOR_PROC=$(Process) CONDOR_CLUSTER=$(Cluster)
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


def _read_histogram_config_entries(config_path: str) -> list[dict[str, str]]:
    """Parse a histogramConfig file in either text or YAML format."""
    if not config_path or not os.path.isfile(config_path):
        return []

    if config_path.endswith((".yaml", ".yml")):
        with open(config_path, "r") as fh:
            payload = yaml.safe_load(fh) or []
        if not isinstance(payload, list):
            return []
        entries: list[dict[str, str]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            entries.append({str(key): str(value) for key, value in item.items()})
        return entries

    entries = []
    with open(config_path, "r") as fh:
        for raw_line in fh:
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue
            entry: dict[str, str] = {}
            for token in line.split():
                if "=" not in token:
                    continue
                key, value = token.split("=", 1)
                entry[key.strip()] = value.strip()
            if entry:
                entries.append(entry)
    return entries


def _read_root_histogram_names(meta_file: str) -> list[str]:
    """Return deduplicated histogram names found recursively in *meta_file*."""
    if not meta_file or not os.path.isfile(meta_file):
        return []

    try:
        import uproot  # type: ignore
    except ImportError:
        return []

    names: list[str] = []
    seen: set[str] = set()

    def _visit(directory) -> None:
        for key in directory.keys(cycle=False):
            try:
                obj = directory[key]
            except Exception:
                continue

            if hasattr(obj, "keys") and not hasattr(obj, "values"):
                _visit(obj)
                continue

            if not hasattr(obj, "values"):
                continue

            name = str(key).split(";", 1)[0]
            if name and name not in seen:
                names.append(name)
                seen.add(name)

    with uproot.open(meta_file) as root_file:
        _visit(root_file)
    return names


def _histogram_axis_labels_from_entries(
    entries: list[dict[str, str]],
    histogram_names: list[str],
) -> dict[str, list[str]]:
    """Build axis-label metadata from parsed histogram config entries."""
    axis_labels: dict[str, list[str]] = {}
    if not entries:
        return axis_labels

    first = entries[0]
    for role, key in (
        ("channel", "channelRegions"),
        ("control", "controlRegionRegions"),
        ("sample", "sampleCategoryRegions"),
    ):
        raw = first.get(key, "")
        labels = [label.strip() for label in raw.split(",") if label.strip()]
        if labels:
            axis_labels[role] = labels

    systematic_labels = sorted(
        {
            match.group(1)
            for name in histogram_names
            for match in [re.search(r"_(.+(?:Up|Down))$", name)]
            if match
        }
    )
    axis_labels["systematic"] = ["Nominal"] + systematic_labels
    return axis_labels


def _infer_region_definitions(entries: list[dict[str, str]]) -> list[RegionDefinition]:
    """Synthesize manifest region definitions from histogram axis labels."""
    if not entries:
        return []

    first = entries[0]
    channel_labels = [
        label.strip() for label in first.get("channelRegions", "").split(",") if label.strip()
    ]
    control_labels = [
        label.strip()
        for label in first.get("controlRegionRegions", "").split(",")
        if label.strip()
    ]
    channel_var = first.get("channelVariable", "channel")
    control_var = first.get("controlRegionVariable", "control")

    if not channel_labels and not control_labels:
        return []

    regions: list[RegionDefinition] = []
    if channel_labels and control_labels:
        for channel_name in channel_labels:
            for control_name in control_labels:
                regions.append(
                    RegionDefinition(
                        name=f"{channel_name}/{control_name}",
                        filter_column=f"{channel_var}={channel_name};{control_var}={control_name}",
                        parent=channel_name,
                        description=(
                            f"Histogram region for channel '{channel_name}' and "
                            f"control category '{control_name}'."
                        ),
                    )
                )
        for channel_name in channel_labels:
            regions.append(
                RegionDefinition(
                    name=channel_name,
                    filter_column=f"{channel_var}={channel_name}",
                    description=f"Histogram channel '{channel_name}'.",
                )
            )
        return regions

    labels = control_labels or channel_labels
    variable = control_var if control_labels else channel_var
    return [
        RegionDefinition(
            name=label,
            filter_column=f"{variable}={label}",
            description=f"Histogram region '{label}'.",
        )
        for label in labels
    ]


def _initial_sample_metadata(dataset: DatasetEntry) -> dict[str, dict[str, Any]]:
    """Build default alias metadata from a dataset manifest entry."""
    metadata: dict[str, dict[str, Any]] = {
        dataset.name: {
            "alias_type": "dataset",
            "dataset_name": dataset.name,
            "dtype": dataset.dtype,
        }
    }
    for key in (
        "process",
        "group",
        "stitch_id",
        "sample_type",
        "year",
        "era",
        "campaign",
        "parent",
    ):
        value = getattr(dataset, key)
        if value is not None:
            metadata[dataset.name][key] = value
    return metadata


def _augment_sample_metadata_with_axis_labels(
    sample_metadata: dict[str, dict[str, Any]],
    axis_labels: dict[str, list[str]],
) -> None:
    """Add sample-axis aliases such as STXS template bins into sample metadata."""
    for alias in axis_labels.get("sample", []):
        if not alias:
            continue
        entry = sample_metadata.setdefault(
            alias,
            {
                "alias_type": "histogram_category",
                "source_axis": "sample",
            },
        )
        if "process" not in entry and "_" in alias:
            entry["process"] = alias.split("_", 1)[0]
        if "stxs_bin" not in entry and re.match(r"^[A-Za-z]+_\d+$", alias):
            entry["stxs_bin"] = alias


def _matches_manifest_selector(metadata: dict[str, Any], selector: dict[str, Any], alias: str) -> bool:
    """Return True when *metadata* satisfies a selector mapping."""
    if not selector:
        return True

    for key, expected in selector.items():
        if key == "pattern":
            patterns = expected if isinstance(expected, list) else [expected]
            if not any(isinstance(pattern, str) and fnmatch(alias, pattern) for pattern in patterns):
                return False
            continue

        if key == "tags_any":
            tags = metadata.get("tags", [])
            wanted = expected if isinstance(expected, list) else [expected]
            if not any(tag in tags for tag in wanted):
                return False
            continue

        if key == "tags_all":
            tags = metadata.get("tags", [])
            wanted = expected if isinstance(expected, list) else [expected]
            if any(tag not in tags for tag in wanted):
                return False
            continue

        actual = metadata.get(key)
        if isinstance(expected, list):
            if actual not in expected:
                return False
        else:
            if actual != expected:
                return False

    return True


def _resolve_manifest_samples(
    config_block: dict[str, Any],
    sample_metadata: dict[str, dict[str, Any]],
) -> list[str]:
    """Resolve explicit + selector-based sample aliases to a concrete list."""
    resolved: list[str] = []
    seen: set[str] = set()

    for sample in config_block.get("samples", []):
        if isinstance(sample, str) and sample and sample not in seen:
            resolved.append(sample)
            seen.add(sample)

    selector = config_block.get("select")
    if isinstance(selector, dict):
        for alias, metadata in sample_metadata.items():
            if alias in seen:
                continue
            if _matches_manifest_selector(metadata, selector, alias):
                resolved.append(alias)
                seen.add(alias)

    return resolved


def _resolve_manifest_fit_metadata(
    metadata_config_path: str,
    sample_metadata: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Load optional fit metadata config and resolve selectors to sample lists."""
    if not metadata_config_path or not os.path.isfile(metadata_config_path):
        return sample_metadata, {}, {}

    with open(metadata_config_path, "r") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        return sample_metadata, {}, {}

    merged_sample_metadata: dict[str, dict[str, Any]] = {
        alias: dict(metadata) for alias, metadata in sample_metadata.items()
    }

    for alias, metadata in (payload.get("sample_metadata") or {}).items():
        if not isinstance(alias, str) or not isinstance(metadata, dict):
            continue
        merged_sample_metadata.setdefault(alias, {}).update(metadata)

    sample_combinations: dict[str, dict[str, Any]] = {}
    for combo_name, combo_cfg in (payload.get("sample_combinations") or {}).items():
        if not isinstance(combo_name, str) or not isinstance(combo_cfg, dict):
            continue
        resolved_samples = _resolve_manifest_samples(combo_cfg, merged_sample_metadata)
        combination_cfg = {
            key: value for key, value in combo_cfg.items() if key != "select"
        }
        if resolved_samples:
            combination_cfg["samples"] = resolved_samples
        sample_combinations[combo_name] = combination_cfg

    processes: dict[str, dict[str, Any]] = {}
    for process_name, process_cfg in (payload.get("processes") or {}).items():
        if not isinstance(process_name, str) or not isinstance(process_cfg, dict):
            continue
        resolved_samples = _resolve_manifest_samples(process_cfg, merged_sample_metadata)
        concrete_cfg = {
            key: value for key, value in process_cfg.items() if key != "select"
        }
        if resolved_samples:
            concrete_cfg["samples"] = resolved_samples
        processes[process_name] = concrete_cfg

    return merged_sample_metadata, sample_combinations, processes


def _make_meta_output_name(save_file: str) -> str:
    """Match RootOutputSink's default meta ROOT file naming."""
    if not save_file:
        return ""
    stem, ext = os.path.splitext(save_file)
    if ext:
        return f"{stem}_meta{ext}"
    return save_file + "_meta.root"


def _build_histfill_manifest(
    dataset: DatasetEntry,
    submit_config_path: str,
    dataset_manifest_path: str,
    job_config_path: str,
) -> OutputManifest:
    """Build an OutputManifest describing histogram, metadata, and cutflow outputs."""
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

    job_config = read_config(job_config_path)
    save_file = job_config.get("saveFile", "")
    meta_file = job_config.get("metaFile", "") or _make_meta_output_name(save_file)

    histogram_config_path = _resolve_analysis_config_path(
        submit_config_path,
        job_config.get("histogramConfig", ""),
    )
    histogram_entries = _read_histogram_config_entries(histogram_config_path)
    histogram_names = _read_root_histogram_names(meta_file)
    axis_labels = _histogram_axis_labels_from_entries(histogram_entries, histogram_names)

    sample_metadata = _initial_sample_metadata(dataset)
    _augment_sample_metadata_with_axis_labels(sample_metadata, axis_labels)

    fit_metadata_path = _resolve_analysis_config_path(
        submit_config_path,
        job_config.get("manifestMetadataConfig", job_config.get("fitMetadataConfig", "")),
    )
    sample_metadata, sample_combinations, processes = _resolve_manifest_fit_metadata(
        fit_metadata_path,
        sample_metadata,
    )

    axes: list[HistogramAxisSpec] = []
    if histogram_entries:
        first = histogram_entries[0]
        role_specs = (
            ("channelVariable", "channelBins", "channelLowerBound", "channelUpperBound", "channel"),
            (
                "controlRegionVariable",
                "controlRegionBins",
                "controlRegionLowerBound",
                "controlRegionUpperBound",
                "control",
            ),
            (
                "sampleCategoryVariable",
                "sampleCategoryBins",
                "sampleCategoryLowerBound",
                "sampleCategoryUpperBound",
                "sample",
            ),
        )
        for var_key, bins_key, low_key, high_key, role in role_specs:
            if var_key not in first:
                continue
            try:
                axes.append(
                    HistogramAxisSpec(
                        variable=first[var_key],
                        bins=int(first[bins_key]),
                        lower_bound=float(first[low_key]),
                        upper_bound=float(first[high_key]),
                        label=role,
                    )
                )
            except Exception:
                continue

    manifest = OutputManifest(
        histograms=HistogramSchema(
            output_file=meta_file,
            histogram_names=histogram_names,
            axes=axes,
            axis_labels=axis_labels,
            sample_metadata=sample_metadata,
            sample_combinations=sample_combinations,
            processes=processes,
        ),
        metadata=MetadataSchema(output_file=meta_file),
        cutflow=CutflowSchema(output_file=meta_file),
        regions=_infer_region_definitions(histogram_entries),
        framework_hash=prov.framework_hash,
        config_mtime=prov.config_mtime,
        dataset_manifest_provenance=dataset_manifest_prov,
    )
    return manifest


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
    _store("energy", dataset.energy)
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

    # Forward analysis-specific dataset metadata (for example ptcut) into
    # per-job submit configs. Standard metadata keys above win on collisions.
    if isinstance(dataset.extra, dict):
        for key, value in dataset.extra.items():
            if key in metadata or key is None:
                continue
            _store(str(key), value)

    return metadata


def _collect_skim_normalization(
    dataset_manifest_path: str,
    dataset_job_info: dict[str, dict[int, dict[str, str]]],
    state: dict[str, Any],
) -> dict[str, Any]:
    """Collect per-job generator-weight counters after a skim run completes."""
    manifest = DatasetManifest.load(dataset_manifest_path)
    output: dict[str, Any] = {
        "schema_version": 1,
        "manifest": dataset_manifest_path,
        "lumi": manifest.lumi,
        "datasets": {},
    }

    for dataset_name, job_info in sorted(dataset_job_info.items()):
        dataset = manifest.by_name(dataset_name)
        if dataset is None:
            output["datasets"][dataset_name] = {
                "status": "error",
                "errors": ["dataset is not present in the dataset manifest"],
            }
            continue

        effective_lumi = manifest.lumi_for(dataset.year, dataset.era)
        dataset_output: dict[str, Any] = {
            "manifest_entry": dataset.to_dict(),
            "lumi": effective_lumi,
            "jobs": {},
            "sum_weights": None,
            "sum_sign_weights": None,
            "normalization": None,
            "status": "complete",
            "errors": [],
        }

        raw_total = 0.0
        sign_total = 0.0
        completed_jobs = 0
        for job_index, job_info_entry in sorted(job_info.items()):
            job_state = (
                state.get("datasets", {})
                .get(dataset_name, {})
                .get("jobs", {})
                .get(str(job_index), {})
            )
            job_output: dict[str, Any] = {
                "status": job_state.get("status", "unknown"),
                "meta_file": job_info_entry.get("meta", ""),
                "sum_weights": None,
                "sum_sign_weights": None,
                "error": None,
            }

            if dataset.dtype != "mc":
                if job_output["status"] != "done":
                    job_output["error"] = "job did not finish successfully"
                    dataset_output["errors"].append(
                        f"job {job_index} status is {job_output['status']}"
                    )
                else:
                    completed_jobs += 1
                dataset_output["jobs"][str(job_index)] = job_output
                continue

            if job_output["status"] != "done":
                job_output["error"] = "job did not finish successfully"
                dataset_output["errors"].append(
                    f"job {job_index} status is {job_output['status']}"
                )
                dataset_output["jobs"][str(job_index)] = job_output
                continue

            completed_jobs += 1
            meta_file = str(job_info_entry.get("meta", ""))
            if not meta_file:
                job_output["error"] = "job has no meta output path"
                dataset_output["errors"].append(
                    f"job {job_index} has no meta output path"
                )
                dataset_output["jobs"][str(job_index)] = job_output
                continue

            try:
                raw_sum = _shared_read_weight_sum(meta_file, dataset_name)
                sign_sum = _shared_read_weight_sign_sum(meta_file, dataset_name)
                job_output["sum_weights"] = raw_sum
                job_output["sum_sign_weights"] = sign_sum
                raw_total += raw_sum
                sign_total += sign_sum
            except (OSError, RuntimeError, ValueError) as exc:
                job_output["error"] = str(exc)
                dataset_output["errors"].append(
                    f"job {job_index} counter read failed: {exc}"
                )
            dataset_output["jobs"][str(job_index)] = job_output

        expected_jobs = len(job_info)
        if dataset_output["errors"] or completed_jobs != expected_jobs:
            dataset_output["status"] = "incomplete"
        elif dataset.dtype == "mc":
            dataset_output["sum_weights"] = raw_total
            dataset_output["sum_sign_weights"] = sign_total
            if dataset.xsec is None or raw_total <= 0.0:
                dataset_output["status"] = "error"
                dataset_output["errors"].append(
                    "MC normalization requires a positive collected sum_weights "
                    "and a manifest xsec"
                )
            else:
                dataset_output["normalization"] = (
                    dataset.xsec
                    * dataset.filter_efficiency
                    * dataset.kfac
                    * dataset.extra_scale
                    * effective_lumi
                    * 1000.0
                    / raw_total
                )

        dataset_output["job_count"] = expected_jobs
        dataset_output["completed_jobs"] = completed_jobs
        output["datasets"][dataset_name] = dataset_output

    return output


def _resolve_output_destination_base(
    template_config_path: str,
    config_dict: dict[str, str],
    output_directory: str = "",
) -> str:
    """Resolve the configured final saveDirectory for staged skim outputs."""
    save_dir = str(
        output_directory or config_dict.get("saveDirectory", "") or ""
    ).strip()
    if not save_dir:
        return ""
    if save_dir.startswith("root://") or os.path.isabs(save_dir):
        return save_dir
    template_dir = os.path.dirname(os.path.abspath(template_config_path))
    return os.path.abspath(os.path.join(template_dir, save_dir))


def _rewrite_job_output_destinations(config_path: str, final_output_dir: str) -> None:
    """Rewrite saveDirectory/saveFile/metaFile in a per-job config copy."""
    config_dict = read_config(config_path)
    config_dict["saveDirectory"] = final_output_dir
    job_dir = os.path.basename(os.path.dirname(config_path))
    job_index = job_dir.split("job_", 1)[1] if job_dir.startswith("job_") else job_dir
    config_dict["saveFile"] = os.path.join(final_output_dir, f"output_{job_index}.root")
    config_dict["metaFile"] = os.path.join(final_output_dir, f"meta_{job_index}.root")

    with open(config_path, "w") as fh:
        for key, value in config_dict.items():
            if not key.startswith("__"):
                fh.write(f"{key}={value}\n")


def _stitch_job_overrides(task: object) -> dict[str, str]:
    """Return optional stitching keys to inject into prepared job configs."""
    overrides: dict[str, str] = {}
    stitch_mode = str(getattr(task, "stitch_mode", "") or "").strip()
    stitch_weights = str(getattr(task, "stitch_weights", "") or "").strip()
    if stitch_mode:
        overrides["stitchMode"] = stitch_mode
    if stitch_weights:
        overrides["stitchWeights"] = stitch_weights
    return overrides


# ===========================================================================
# Helper: write a job submit_config.txt from a template + dataset overrides
# ===========================================================================

def _write_job_config(
    job_dir: str,
    template_config_path: str,
    dataset: DatasetEntry,
    output_dir: str,
    manifest_lumi: Optional[float] = None,
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
    redirector = (
        str(template_config.get("xrootdRedirector", "") or "").strip()
        or str(template_config.get("redirector", "") or "").strip()
    )

    normalized_files = []
    for f in file_list:
        if not f:
            continue
        if f.startswith("/") or f.startswith("store/") or f.startswith("root://"):
            if redirector:
                normalized_files.append(ensure_xrootd_redirector(f, redirector))
            else:
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
    if manifest_lumi is not None:
        job_config["lumi"] = str(manifest_lumi)

    # Data samples do not carry MC-only generator weights.
    _remove_mc_generator_config_for_data(job_config)

    # ---- caller-supplied overrides ------------------------------------------
    if extra_overrides:
        job_config.update(extra_overrides)

    # ---- copy referenced auxiliary files ------------------------------------
    if stage_shared_inputs_locally:
        staging_config = dict(template_config)
        if extra_overrides:
            staging_config.update(extra_overrides)
        copied_payloads = _copy_analysis_config_files(
            template_config_path=template_config_path,
            config_dict=staging_config,
            destination_dir=job_dir,
        )
        _copy_analysis_aux_dir(template_config_path, job_dir)
        for key, val in list(job_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt")
                or val.endswith(".yaml")
                or val.endswith(".yml")
                or val.endswith(".json")
                or val.endswith(".json.gz")
                or val.endswith(".onnx")
            ):
                basename = os.path.basename(val)
                if basename in copied_payloads:
                    job_config[key] = basename
    else:
        _ensure_local_analysis_runtime_layout(
            template_config_path,
            job_config,
            job_dir,
            include_aux=False,
        )
        _normalize_shared_config_references(template_config_path, job_config)

    # ---- write the final config file ----------------------------------------
    config_path = os.path.join(job_dir, "submit_config.txt")
    with open(config_path, "w") as fh:
        for key, val in job_config.items():
            if not key.startswith("__"):
                fh.write(f"{key}={val}\n")


def _extract_lfn_for_redirect(url: str) -> str:
    """Extract the bare LFN from an XRootD URL for site_redirectors lookup.

    Handles both regular XRootD URLs (``root://host//store/...``) and
    site-specific test redirector URLs
    (``root://xrootd-cms.infn.it//store/test/xrootd/<site>//store/...``).
    Returns the logical file name starting with ``/store/`` or ``/eos/``,
    or the original *url* if it is not an XRootD URL.
    """
    import re as _re
    if not url.startswith("root://"):
        return url
    # Strip the server component to get the path.
    m = _re.match(r"root://[^/]+/+(.*)", url)
    if not m:
        return url
    path = "/" + m.group(1).lstrip("/")
    # For site-specific test redirectors the path is
    # /store/test/xrootd/<site>//store/... — extract the actual LFN.
    test_m = _re.match(r"/store/test/xrootd/[^/]+/(.*)", path)
    if test_m:
        path = "/" + test_m.group(1).lstrip("/")
    return path


def _select_test_input_url(input_url: str, job_dir: str) -> str:
    """Select a readable Rucio replica URL for the local preflight job."""
    config_path = os.path.join(job_dir, "submit_config.txt")
    if os.path.isfile(config_path):
        config = read_config(config_path)
        redirector = str(
            config.get("xrootdRedirector", "") or config.get("redirector", "")
        ).strip()
        if redirector:
            return ensure_xrootd_redirector(
                _extract_lfn_for_redirect(input_url), redirector
            )

    redirectors_path = os.path.join(job_dir, "site_redirectors.json")
    if not os.path.isfile(redirectors_path):
        return input_url

    try:
        with open(redirectors_path) as fh:
            redirector_map = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return input_url

    lfn = _extract_lfn_for_redirect(input_url)
    candidates = redirector_map.get(lfn, [])
    if not candidates:
        return input_url

    try:
        from xrootd_site_selector import select_best_url  # type: ignore

        selected = select_best_url(lfn, redirectors=list(candidates))
        if selected and selected.startswith("root://") and selected != input_url:
            return selected
    except Exception:
        pass

    # Rucio only supplies replicas that exist at the listed sites. Use the
    # first candidate when probing is unavailable or inconclusive.
    redirector = str(candidates[0]).rstrip("/")
    separator = "/" if "/store/test/xrootd/" in redirector else "//"
    return f"{redirector}{separator}{lfn.lstrip('/')}"


def _write_config_file(config_path: str, config: dict[str, str]) -> None:
    """Write a key/value analysis config while preserving internal-key rules."""
    with open(config_path, "w") as fh:
        for key, value in config.items():
            if not key.startswith("__"):
                fh.write(f"{key}={value}\n")


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
    missing_datasets: list[str] = []
    manifest = DatasetManifest.load(dataset_manifest_path)

    for json_path in sorted(Path(file_list_dir).glob("*.json")):
        if json_path.name.endswith(".perf.json"):
            continue

        with open(json_path) as fh:
            payload = json.load(fh)

        dataset_name = payload.get("sample", json_path.stem)
        all_files = payload.get("files", [])
        groups = payload.get("groups", [])
        # Per-file redirector list from Rucio discovery (may be absent for
        # non-Rucio sources or explicit file lists).
        site_redirectors: dict[str, list[str]] = payload.get("site_redirectors", {})

        # Prefer pre-computed group partitions when available (e.g. from GetRucioFileList).
        # If the file list is empty but groups exist, we still want to create jobs.
        if groups:
            partitions = [
                {"files": g, "first_entry": 0, "last_entry": 0}
                for g in groups
                if g
            ]
        else:
            if not all_files:
                missing_datasets.append(str(dataset_name))
                continue
            partitions = _make_partitions(
                urls=all_files,
                mode=partition,
                files_per_job=files_per_job,
                entries_per_job=entries_per_job,
            )

        dataset_entry = manifest.by_name(str(dataset_name))
        dtype = dataset_entry.dtype if dataset_entry else "mc"
        lumi = (
            manifest.lumi_for(dataset_entry.year, dataset_entry.era)
            if dataset_entry is not None
            else manifest.lumi
        )

        for sub_idx, part in enumerate(partitions):
            # Build per-job site_redirectors: only include LFNs in this partition.
            job_lfns = [
                _extract_lfn_for_redirect(u.strip())
                for u in str(part["files"]).split(",")
                if u.strip()
            ]
            job_site_redirectors = {
                lfn: site_redirectors[lfn]
                for lfn in job_lfns
                if lfn in site_redirectors
            }
            job_plans.append({
                "dataset_name": dataset_name,
                "job_dir": os.path.join(jobs_dir, dataset_name, f"job_{sub_idx}"),
                "out_dir": os.path.join(outputs_dir, dataset_name, f"job_{sub_idx}"),
                "files": part["files"],
                "first_entry": part.get("first_entry", 0),
                "last_entry": part.get("last_entry", 0),
                "dtype": dtype,
                "lumi": lumi,
                "site_redirectors": job_site_redirectors,
            })

    if missing_datasets:
        missing_display = ", ".join(sorted(set(missing_datasets)))
        raise RuntimeError(
            "File-list discovery returned empty payloads for the following dataset(s): "
            f"{missing_display}. Re-run the file-list task and inspect the corresponding "
            "JSON outputs before preparing skim jobs."
        )

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
            "'rucio' (use GetRucioFileList output), "
            "'opendata' (use GetOpenDataFileList output), "
            "'xrdfs' (use GetXRDFSFileList output).  "
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
            "the Rucio query in GetRucioFileList."
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
    output_directory = luigi.Parameter(
        default="",
        description=(
            "Optional final output directory for delegated HTCondor jobs. "
            "When set, it overrides saveDirectory from the submit config and "
            "may be a local EOS path or a root:// URL."
        ),
    )
    container_setup = luigi.Parameter(
        default="",
        description=(
            "Container image path passed to HTCondor via MY.SingularityImage "
            "(for example '/cvmfs/cms.cern.ch/common/cmssw-el9')."
        ),
    )
    want_os = luigi.Parameter(
        default="",
        description=(
            "Optional HTCondor MY.WantOS value to add to the submit file "
            "(for example 'el8')."
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
    stitch_mode = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional stitching mode override written into each job config "
            "(for example 'none' or 'correctionlib')."
        ),
    )
    stitch_weights = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional correctionlib stitching JSON staged in the shared worker "
            "archive and exposed as the 'stitchWeights' config value."
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
    def _shared_archive_path(self) -> str:
        """Tarball containing the worker runtime cfg/ and aux/ layout."""
        return os.path.join(self._run_dir, _shared_inputs_archive_name())

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
            "rucio": "rucioFileList",
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

    def _prepare_skim_jobs_requirement(self):
        return PrepareSkimJobs.req(
            self,
            stitch_mode=self.stitch_mode,
            stitch_weights=self.stitch_weights,
        )


# ===========================================================================
# PrepareSkimJobs – set up shared_inputs/ and per-job directories
# (analogous to older BuildSkimSubmission-style preparation)
# ===========================================================================

class PrepareSkimJobs(AnalysisMixin, SkimMixin, law.Task):
    """Create per-job directories and ``shared_inputs/`` for :class:`SkimTask`.

    This task mirrors the role of older submission-preparation tasks:
    it prepares **all** job directories and the shared executable/library
    staging area **before** any HTCondor or Dask branches are dispatched.

    Workflow
    --------
    When ``--file-source`` is set the task reads the file-list JSON(s) written
    by :class:`~rucio_tasks.GetRucioFileList`,
    :class:`~opendata_tasks.GetOpenDataFileList`, or
    :class:`~xrdfs_tasks.GetXRDFSFileList` from the appropriate
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
        :class:`~xrdfs_tasks.GetXRDFSFileList` is declared automatically because
        both tasks share the same ``dataset_manifest`` parameter.

        For ``--file-source rucio`` and ``--file-source opendata`` the skim
        submit config is reused directly when it contains the corresponding
        ``sampleConfig`` payload, allowing the file-list step to be chained
        automatically from :class:`SkimTask`.
        """
        if self.file_source == "xrdfs":
            from xrdfs_tasks import GetXRDFSFileList  # noqa: PLC0415
            return GetXRDFSFileList.req(
                self,
                name=self._effective_file_source_name,
            )
        if self.file_source == "rucio":
            from rucio_tasks import GetRucioFileList  # noqa: PLC0415
            return GetRucioFileList.req(
                self,
                name=self._effective_file_source_name,
            )
        if self.file_source == "opendata":
            from opendata_tasks import GetOpenDataFileList  # noqa: PLC0415
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
        _build_shared_inputs_archive(shared_dir, self._shared_archive_path)

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

                extra_overrides = _stitch_job_overrides(self)
                if int(plan.get("last_entry", 0)) > 0:
                    extra_overrides["firstEntry"] = str(plan["first_entry"])
                    extra_overrides["lastEntry"] = str(plan["last_entry"])


                _write_job_config(
                    job_dir=job_dir,
                    template_config_path=self.submit_config,
                    dataset=transient,
                    output_dir=out_dir,
                    manifest_lumi=(
                        float(plan["lumi"])
                        if plan.get("lumi") is not None
                        else _manifest_lumi_for_dataset(self.dataset_manifest, transient)
                    ),
                    extra_overrides=extra_overrides,
                    stage_shared_inputs_locally=False,
                )
                _ensure_local_analysis_runtime_layout(
                    self.submit_config,
                    read_config(os.path.join(job_dir, "submit_config.txt")),
                    job_dir,
                )

                # Write the per-job site redirector list so Condor workers
                # can probe all available sites instead of only the generic
                # CMS_REDIRECTORS.
                job_site_redirectors = plan.get("site_redirectors", {})
                with open(os.path.join(job_dir, "site_redirectors.json"), "w") as sr_fh:
                    json.dump(job_site_redirectors, sr_fh)

                job_entries.append({
                    "dataset_name": dataset_name,
                    "job_dir": job_dir,
                    "config_path": os.path.join(job_dir, "submit_config.txt"),
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
                    manifest_lumi=manifest.lumi_for(entry.year, entry.era),
                    extra_overrides=_stitch_job_overrides(self),
                    stage_shared_inputs_locally=False,
                )
                _ensure_local_analysis_runtime_layout(
                    self.submit_config,
                    read_config(os.path.join(job_dir, "submit_config.txt")),
                    job_dir,
                )
                # Write empty site_redirectors.json so Condor transfer works
                # consistently; workers fall back to CMS_REDIRECTORS when empty.
                with open(os.path.join(job_dir, "site_redirectors.json"), "w") as sr_fh:
                    json.dump({}, sr_fh)
                job_entries.append({
                    "dataset_name": entry.name,
                    "job_dir": job_dir,
                    "config_path": os.path.join(job_dir, "submit_config.txt"),
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
        template_config: dict[str, str] = {}
        if os.path.isfile(self.submit_config):
            template_config = read_config(self.submit_config)
            _copy_analysis_config_files(self.submit_config, template_config, test_dir)
            # Keep a cfg/ copy as well as the loose files above. Prepared
            # file-source jobs already refer to staged payloads as cfg/foo;
            # this makes the local preflight layout match the worker archive.
            _ensure_local_analysis_runtime_layout(
                self.submit_config,
                template_config,
                test_dir,
                include_aux=False,
            )
            _copy_analysis_aux_dir(self.submit_config, test_dir)

        # Reuse floats.txt / ints.txt from the first job dir if available
        for aux_name in ("floats.txt", "ints.txt"):
            src = os.path.join(first_job_dir, aux_name)
            if os.path.exists(src):
                dst = os.path.join(test_dir, aux_name)
                if not os.path.exists(dst):
                    shutil.copy2(src, dst)

        # Build a minimal submit config: single file, local outputs, single thread
        #if os.path.isfile(self.submit_config):
        #    test_config = dict(read_config(self.submit_config))
        #else:
        #    test_config = {}
        import copy
        test_config = copy.deepcopy(template_config)
        test_config.update(jcfg)
        _remove_mc_generator_config_for_data(test_config)
        test_config["fileList"] = _select_test_input_url(first_file, first_job_dir)
        test_config["saveFile"] = "test_output.root"
        test_config["metaFile"] = "test_output_meta.root"
        test_config["saveDirectory"] = test_dir
        test_config.setdefault("threads", "1")
        # Remove any stage-out sentinel keys
        for key in ("__orig_saveFile", "__orig_metaFile", "__orig_fileList"):
            test_config.pop(key, None)

        if os.path.isfile(self.submit_config):
            _ensure_local_analysis_runtime_layout(
                self.submit_config,
                test_config,
                test_dir,
            )

        # Resolve config-file references to their basenames (files are already copied)
        for key, val in list(test_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt")
                or val.endswith(".yaml")
                or val.endswith(".yml")
                or val.endswith(".json")
                or val.endswith(".json.gz")
                or val.endswith(".onnx")
            ):
                basename = os.path.basename(val)
                cfg_path = os.path.join(test_dir, "cfg", basename)
                if os.path.exists(cfg_path):
                    test_config[key] = os.path.join("cfg", basename)
                    continue
                if os.path.exists(os.path.join(test_dir, basename)):
                    test_config[key] = basename

        _write_config_file(os.path.join(test_dir, "submit_config.txt"), test_config)

        self.publish_message(
            f"[PrepareSkimJobs] Test job created in {test_dir}/\n"
            f"  Input file: {test_config['fileList']}\n"
            f"  Run manually with:  cd {test_dir} && ./{exe_relpath} submit_config.txt"
        )


# ===========================================================================
# RunSkimTestJob – pre-flight single-file local test
# ===========================================================================

class RunSkimTestJob(AnalysisMixin, SkimMixin, law.Task):
    """Run a single-file local test before dispatching the full :class:`SkimTask`.

    This task mirrors the test job functionality in earlier workflow versions.  It
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
            return self._prepare_skim_jobs_requirement()
        return None

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._test_job_dir, "test_passed.txt")
        )

    def _refresh_test_input_url(self, config_path: str) -> None:
        """Refresh a reused file-source test config from its prepared job."""
        if not self.file_source:
            return

        config = read_config(config_path)
        input_url = str(config.get("fileList", "")).split(",", 1)[0].strip()
        if not input_url:
            return

        prep_path = os.path.join(self._run_dir, "prep_submission.json")
        if not os.path.isfile(prep_path):
            return
        try:
            with open(prep_path) as fh:
                prepared_jobs = json.load(fh)
        except (OSError, json.JSONDecodeError):
            return

        input_lfn = _extract_lfn_for_redirect(input_url)
        for entry in prepared_jobs:
            if not isinstance(entry, dict):
                continue
            source_job_dir = str(entry.get("job_dir", ""))
            if not source_job_dir:
                continue
            source_redirectors = os.path.join(
                source_job_dir, "site_redirectors.json"
            )
            if not os.path.isfile(source_redirectors):
                continue
            try:
                with open(source_redirectors) as fh:
                    redirector_map = json.load(fh)
            except (OSError, json.JSONDecodeError):
                continue
            if input_lfn not in redirector_map:
                continue

            config["fileList"] = _select_test_input_url(input_url, source_job_dir)
            _write_config_file(config_path, config)
            return

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

        test_config = read_config(config_path)
        _remove_mc_generator_config_for_data(test_config)
        _write_config_file(config_path, test_config)
        self._refresh_test_input_url(config_path)

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
                container_setup="",
                stream_output=True,
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
        test_config["lumi"] = str(
            manifest.lumi_for(first_dataset.year, first_dataset.era)
        )
        _remove_mc_generator_config_for_data(test_config)
        test_config.setdefault("threads", "1")
        for key in ("__orig_saveFile", "__orig_metaFile", "__orig_fileList"):
            test_config.pop(key, None)

        if os.path.isfile(self.submit_config):
            _ensure_local_analysis_runtime_layout(
                self.submit_config,
                test_config,
                test_dir,
            )

        # Resolve referenced config files to basenames
        for key, val in list(test_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt")
                or val.endswith(".yaml")
                or val.endswith(".yml")
                or val.endswith(".onnx")
            ):
                basename = os.path.basename(val)
                cfg_path = os.path.join(test_dir, "cfg", basename)
                if os.path.exists(cfg_path):
                    test_config[key] = os.path.join("cfg", basename)
                    continue
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
    """Write concrete Condor submission files for prepared skim jobs.

    Shared executable, library, configuration, auxiliary, and optional
    stitching payloads are transferred as one run-level archive.  Concrete
    per-job directories contain only job-specific metadata and configuration.
    """

    task_namespace = ""

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        return PrepareSkimJobs.req(self)

    def output(self):
        return {
            "manifest": law.LocalFileTarget(os.path.join(self._run_dir, "submission_manifest.json")),
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

        grouped_jobs = _group_prepared_skim_jobs_by_dataset(payload)
        if not grouped_jobs:
            raise RuntimeError("No prepared skim job directories were discovered.")

        template_config = read_config(self.submit_config)
        final_output_base = _resolve_output_destination_base(
            self.submit_config, template_config, self.output_directory
        )
        bookkeeping_outputs_dir = os.path.join(self._run_dir, "outputs")

        Path(self._run_dir).mkdir(parents=True, exist_ok=True)

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

        stitch_weights_path = str(getattr(self, "stitch_weights", "") or "").strip()
        if stitch_weights_path and not os.path.isfile(stitch_weights_path):
            raise RuntimeError(
                f"Stitching weights file not found: {stitch_weights_path!r}"
            )
        if stitch_weights_path:
            # Stitch weights are identical for every job.  Add them to the
            # run-level archive instead of copying one JSON into each job.
            shutil.copy2(
                stitch_weights_path,
                os.path.join(shared_dir, "stitch_weights.json"),
            )
            _build_shared_inputs_archive(shared_dir, self._shared_archive_path)

        shared_transfer_files = _skim_shared_input_files(shared_dir)
        shared_archive_file_names = _shared_archive_file_basenames(
            self._shared_archive_path
        )

        submission_records: list[dict[str, object]] = []
        for dataset_name, dataset_jobs in grouped_jobs.items():
            submission_dir = _dataset_submission_dir(self._run_dir, dataset_name)
            if os.path.exists(submission_dir):
                shutil.rmtree(submission_dir)
            Path(submission_dir).mkdir(parents=True, exist_ok=True)

            jobs_payload: list[dict[str, object]] = []
            for local_index, (global_index, entry) in enumerate(dataset_jobs):
                job_dir_abs = str(entry["job_dir"])
                staged_job_dir = os.path.join(submission_dir, f"job_{local_index}")
                Path(staged_job_dir).mkdir(parents=True, exist_ok=True)
                _copy_job_submission_files(
                    job_dir_abs,
                    staged_job_dir,
                    shared_archive_file_names,
                )
                if stitch_weights_path:
                    staged_config_path = os.path.join(staged_job_dir, "submit_config.txt")
                    staged_config = read_config(staged_config_path)
                    staged_config["stitchWeights"] = "stitch_weights.json"
                    with open(staged_config_path, "w") as staged_config_fh:
                        for key, value in staged_config.items():
                            if not key.startswith("__"):
                                staged_config_fh.write(f"{key}={value}\n")
                if final_output_base and entry.get("out_dir"):
                    final_output_dir = os.path.join(final_output_base, dataset_name)
                    _rewrite_job_output_destinations(
                        os.path.join(staged_job_dir, "submit_config.txt"),
                        final_output_dir,
                    )

                jobs_payload.append({
                    "job_index": local_index,
                    "global_index": global_index,
                    "source_job_dir": job_dir_abs,
                    "job_dir": staged_job_dir,
                    "out_dir": str(entry.get("out_dir", "")),
                })

            submission_transfer_files = list(shared_transfer_files)

            submit_path = write_submit_files(
                submission_dir,
                len(dataset_jobs),
                self._exe_relpath,
                stage_inputs=self.stage_in,
                stage_outputs=True,
                root_setup=self._root_setup_content,
                x509loc=None,
                max_runtime=str(self.max_runtime),
                request_memory=2000,
                request_cpus=1,
                request_disk=20000,
                extra_transfer_files=submission_transfer_files,
                include_aux=False,
                shared_dir_name=None,
                eos_sched=False,
                config_file="submit_config.txt",
                container_setup=self.container_setup,
                want_os=self.want_os,
                python_env_tarball=python_env_staged,
                shared_archive_name=os.path.basename(self._shared_archive_path),
                runtime_config_relpath=os.path.join("cfg", "submit_config.txt"),
            )
            _rewrite_submit_transfer_files(
                submit_path,
                banned_suffixes=["/floats.txt", "/ints.txt"],
                banned_exact_paths=[os.path.join(submission_dir, "shared_inputs")],
            )

            submission_records.append({
                "dataset_name": dataset_name,
                "submission_dir": submission_dir,
                "submit_path": submit_path,
                "runscript_path": os.path.join(submission_dir, "condor_runscript.sh"),
                "job_count": len(dataset_jobs),
                "shared_transfer_files": list(shared_transfer_files),
                "jobs": jobs_payload,
            })

        with self.output()["manifest"].open("w") as fh:
            json.dump({"datasets": submission_records}, fh, indent=2)

        self.publish_message(
            f"Skim condor submission ready.\n"
            f"  {len(payload)} job(s) prepared across {len(submission_records)} dataset submission(s).\n"
            f"  Submission manifest: {self.output()['manifest'].path}"
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
        return law.LocalFileTarget(os.path.join(self._run_dir, "submitted.json"))

    def run(self):
        with PerformanceRecorder("SubmitSkimJobs") as rec:
            build_target = self.input()["build"]
            manifest_path = build_target["manifest"].path
            if not os.path.exists(manifest_path):
                raise RuntimeError(f"submission manifest not found: {manifest_path}")

            with open(manifest_path) as fh:
                submission_manifest = json.load(fh)
            dataset_records = list(submission_manifest.get("datasets", []))
            if not dataset_records:
                raise RuntimeError("No dataset submissions found in submission manifest.")

            submitted_records: list[dict[str, object]] = []
            for record in dataset_records:
                dataset_name = str(record["dataset_name"])
                submit_file = str(record["submit_path"])
                if not os.path.exists(submit_file):
                    raise RuntimeError(
                        f"condor_submit.sub not found for dataset {dataset_name}: {submit_file}"
                    )

                result = subprocess.run(
                    ["condor_submit", submit_file],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                stdout = result.stdout.strip()
                self.publish_message(f"[{dataset_name}] {stdout}")

                cluster_id = ""
                for line in stdout.splitlines():
                    if "cluster" not in line.lower():
                        continue
                    parts = line.split()
                    for idx, part in enumerate(parts):
                        if part.lower().rstrip(".") == "cluster" and idx + 1 < len(parts):
                            cluster_id = parts[idx + 1].rstrip(".")
                            break
                    if cluster_id:
                        break

                submitted_record = dict(record)
                submitted_record["cluster_id"] = cluster_id
                submitted_record["stdout"] = stdout
                submitted_records.append(submitted_record)

                self.publish_message(
                    f"Submitted skim dataset {dataset_name} in cluster {cluster_id} with {record['job_count']} job(s)."
                )

            with self.output().open("w") as fh:
                json.dump({"datasets": submitted_records}, fh, indent=2)

        rec.save(perf_path_for(str(self.output().path)))


class MonitorSkimJobs(AnalysisMixin, SkimMixin, law.Task):
    """Monitor concrete skim Condor jobs with NANO-style resubmission."""

    task_namespace = ""

    max_retries = luigi.IntParameter(
        default=50,
        description="Maximum resubmission attempts per failed skim job (default: 50)",
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
    def _normalization_path(self) -> str:
        return os.path.join(self._run_dir, "normalization.json")

    def normalization_output(self):
        """Return the post-job normalization artifact produced by monitoring."""
        return law.LocalFileTarget(self._normalization_path)

    def complete(self):
        """Require verified outputs and collected normalization metadata."""
        return super().complete() and os.path.isfile(self._normalization_path)

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
        return {"datasets": {}}

    def _save_state(self, state: dict) -> None:
        with open(self._state_file, "w") as fh:
            json.dump(state, fh, indent=2)

    def _discover_jobs(self, submission_dir: str) -> dict[int, dict[str, str]]:
        return _discover_jobs_in_submission_dir(submission_dir)

    def _status_summary(self, state: dict) -> dict[str, int]:
        counts: dict[str, int] = {}
        for dataset_state in state.get("datasets", {}).values():
            for job_state in dataset_state.get("jobs", {}).values():
                status = job_state["status"]
                counts[status] = counts.get(status, 0) + 1
        return counts

    def _reactivate_retryable_failures(self, state: dict) -> int:
        """Make prior permanent failures retryable when the retry budget grew."""
        reactivated = 0
        for dataset_state in state.get("datasets", {}).values():
            for job_state in dataset_state.get("jobs", {}).values():
                if job_state.get("status") != "perm_fail":
                    continue
                retries = int(job_state.get("retries", 0))
                if retries < self.max_retries:
                    job_state["status"] = "submitted"
                    reactivated += 1
        return reactivated

    def run(self):
        with self.input().open("r") as fh:
            submitted_payload = json.load(fh)

        dataset_records = list(submitted_payload.get("datasets", []))
        if not dataset_records:
            raise RuntimeError("No submitted dataset clusters found in submitted.json")

        state = self._load_state()
        dataset_job_info: dict[str, dict[int, dict[str, str]]] = {}
        for record in dataset_records:
            dataset_name = str(record["dataset_name"])
            submission_dir = str(record["submission_dir"])
            initial_cluster = str(record.get("cluster_id", ""))
            if not initial_cluster:
                raise RuntimeError(f"No cluster_id found for dataset {dataset_name}")

            job_info = self._discover_jobs(submission_dir)
            if not job_info:
                raise RuntimeError(
                    f"No job_N directories found in {submission_dir}. "
                    "Run BuildSkimSubmission first."
                )

            dataset_job_info[dataset_name] = job_info
            dataset_state = state.setdefault("datasets", {}).setdefault(dataset_name, {
                "submission_dir": submission_dir,
                "shared_transfer_files": list(record.get("shared_transfer_files", [])),
                "jobs": {},
            })
            dataset_state["submission_dir"] = submission_dir
            dataset_state["shared_transfer_files"] = list(record.get("shared_transfer_files", []))

            for idx in job_info:
                key = str(idx)
                if key not in dataset_state["jobs"]:
                    dataset_state["jobs"][key] = {
                        "status": "submitted",
                        "retries": 0,
                        "cluster": initial_cluster,
                        "proc": idx,
                    }

            for key, persisted_job in dataset_state["jobs"].items():
                if not isinstance(persisted_job, dict):
                    continue
                try:
                    persisted_idx = int(key)
                except (TypeError, ValueError):
                    continue
                if not persisted_job.get("job_dir"):
                    continue
                job_info[persisted_idx] = {
                    "dir": str(persisted_job["job_dir"]),
                    "save": str(persisted_job.get("save", "")),
                    "meta": str(persisted_job.get("meta", "")),
                }

        reactivated = self._reactivate_retryable_failures(state)
        if reactivated:
            self.publish_message(
                f"Reactivated {reactivated} prior permanent failure(s) "
                f"for retry (limit={self.max_retries})."
            )
        self._save_state(state)

        self.publish_message(
            f"Monitoring {sum(len(job_info) for job_info in dataset_job_info.values())} skim job(s) across {len(dataset_job_info)} dataset cluster(s)."
        )

        while True:
            active_clusters = set()
            for dataset_state in state.get("datasets", {}).values():
                for job_state in dataset_state.get("jobs", {}).values():
                    if job_state["status"] in ("submitted", "running", "resubmitting"):
                        active_clusters.add(str(job_state["cluster"]))

            live_status: dict[tuple[str, int], int] = {}
            live_ads: dict[tuple[str, int], dict] = {}
            live_hold_reason: dict[tuple[str, int], str] = {}
            history_exit: dict[tuple[str, int], int] = {}
            for cluster_id in active_clusters:
                for proc, ad in _condor_q_ads(cluster_id).items():
                    if "JobStatus" in ad:
                        live_status[(cluster_id, proc)] = ad["JobStatus"]
                        live_ads[(cluster_id, proc)] = ad
                    if ad.get("HoldReason"):
                        live_hold_reason[(cluster_id, proc)] = str(ad.get("HoldReason"))
                for proc, exit_code in _condor_history_exit(cluster_id).items():
                    history_exit[(cluster_id, proc)] = exit_code

            output_paths_to_check: list[str] = []
            for dataset_name, job_info in dataset_job_info.items():
                dataset_state = state["datasets"][dataset_name]
                for idx in sorted(list(job_info.keys())):
                    job_state = dataset_state["jobs"][str(idx)]
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

            for dataset_name, job_info in dataset_job_info.items():
                dataset_state = state["datasets"][dataset_name]
                submission_dir = str(dataset_state["submission_dir"])
                for idx in sorted(list(job_info.keys())):
                    key = str(idx)
                    job_state = dataset_state["jobs"][key]
                    cluster_id = str(job_state["cluster"])
                    proc = job_state["proc"]
                    status = job_state["status"]
                    if status in ("done", "perm_fail"):
                        continue

                    condor_status = live_status.get((cluster_id, proc))
                    exit_code = history_exit.get((cluster_id, proc))

                    if condor_status == 2:
                        condor_ad = live_ads.get((cluster_id, proc), {})
                        if _condor_job_exceeded_runtime(condor_ad, self.max_runtime):
                            assigned_runtime = condor_ad.get("MaxRuntime", self.max_runtime)
                            elapsed_runtime = condor_ad.get("RemoteWallClockTime", 0)
                            removed = _condor_rm_job(cluster_id, proc)
                            if removed:
                                self.publish_message(
                                    f"[{dataset_name}] Job {idx} exceeded Condor runtime "
                                    f"({elapsed_runtime}s > 1.5 * {assigned_runtime}s); "
                                    "removed from queue."
                                )
                                self._try_resubmit(
                                    dataset_name,
                                    idx,
                                    dataset_state,
                                    job_state,
                                    reason=(
                                        f"runtime {elapsed_runtime}s exceeded "
                                        f"1.5 * assigned runtime {assigned_runtime}s"
                                    ),
                                    job_info=job_info,
                                )
                            else:
                                self.publish_message(
                                    f"[{dataset_name}] Job {idx} exceeded Condor runtime "
                                    "but could not be removed; leaving it running."
                                )
                        else:
                            job_state["status"] = "running"
                        continue

                    if condor_status == 5:
                        hold_reason = live_hold_reason.get((cluster_id, proc), "held without reported reason")
                        removed = _condor_rm_job(cluster_id, proc)
                        if removed:
                            self.publish_message(f"[{dataset_name}] Job {idx} held and removed from queue: {hold_reason}")
                        else:
                            self.publish_message(f"[{dataset_name}] Job {idx} held (could not confirm condor_rm): {hold_reason}")
                        self._try_resubmit(
                            dataset_name,
                            idx,
                            dataset_state,
                            job_state,
                            reason=f"held: {hold_reason}",
                            job_info=job_info,
                        )
                        continue

                    if condor_status is None and exit_code is not None and exit_code != 0:
                        error_tail = _read_failed_job_error(submission_dir, cluster_id, proc, idx)
                        if error_tail:
                            self.publish_message(
                                f"[{dataset_name}] Job {idx} failed with exit code {exit_code}. stderr tail:\n{error_tail}"
                            )
                        self._try_resubmit(
                            dataset_name,
                            idx,
                            dataset_state,
                            job_state,
                            reason=f"exit code {exit_code}",
                            job_info=job_info,
                        )
                        continue

                    if condor_status is None:
                        save_path = job_info[idx]["save"]
                        meta_path = job_info[idx]["meta"]
                        if (
                            (save_path and output_exists.get(save_path, False))
                            or (meta_path and output_exists.get(meta_path, False))
                        ):
                            job_state["status"] = "done"
                            self.publish_message(f"[{dataset_name}] Job {idx}: output verified.")

            self._save_state(state)

            summary = self._status_summary(state)
            done = summary.get("done", 0)
            perm_fail = summary.get("perm_fail", 0)
            total = sum(len(job_info) for job_info in dataset_job_info.values())
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
        normalization = _collect_skim_normalization(
            dataset_manifest_path=self.dataset_manifest,
            dataset_job_info=dataset_job_info,
            state=state,
        )
        normalization_path = self._normalization_path
        with open(normalization_path, "w") as fh:
            json.dump(normalization, fh, indent=2)
        self.publish_message(
            f"Collected per-job generator-weight counters: {normalization_path}"
        )

        with self.output().open("w") as fh:
            fh.write(f"done={done}\n")
            fh.write(f"perm_fail={perm_fail}\n")
            fh.write(f"total={sum(len(job_info) for job_info in dataset_job_info.values())}\n")
            if perm_fail:
                failed = [
                    f"{dataset_name}:{key}"
                    for dataset_name, dataset_state in state.get("datasets", {}).items()
                    for key, job_state in dataset_state.get("jobs", {}).items()
                    if job_state["status"] == "perm_fail"
                ]
                fh.write("perm_failed_jobs=" + ",".join(failed) + "\n")

        if perm_fail:
            self.publish_message(
                f"WARNING: {perm_fail} skim job(s) permanently failed. Details in {self._state_file}"
            )
        else:
            self.publish_message(f"All {done} skim output(s) verified. Monitoring complete.")

    def _try_resubmit(
        self,
        dataset_name: str,
        idx: int,
        dataset_state: dict,
        job_state: dict,
        reason: str,
        job_info: Optional[dict[int, dict[str, str]]] = None,
    ) -> None:
        retries = int(job_state.get("retries", 0))
        if retries >= self.max_retries:
            self.publish_message(
                f"[{dataset_name}] Job {idx}: permanently failed ({reason}, {retries} retries exhausted)."
            )
            job_state["status"] = "perm_fail"
            return

        self.publish_message(
            f"[{dataset_name}] Job {idx} failed ({reason}) - resubmitting "
            f"(attempt {retries + 1}/{self.max_retries})."
        )

        submission_dir = str(dataset_state["submission_dir"])
        tracked_job_info = job_info if job_info is not None else {}
        source_info = dict(tracked_job_info.get(idx, {}))
        source_job_dir = str(
            source_info.get("dir") or os.path.join(submission_dir, f"job_{idx}")
        )
        file_list = _skim_job_file_list(source_job_dir)
        file_groups = _split_skim_file_list(file_list)
        retry_attempt = retries + 1
        submitted_parts: list[tuple[dict[str, str], str]] = []

        for part_index, part_files in enumerate(file_groups):
            part_info = source_info
            log_label = None
            if len(file_groups) > 1:
                try:
                    part_info = _prepare_split_skim_job(
                        source_job_dir=source_job_dir,
                        submission_dir=submission_dir,
                        job_index=idx,
                        retry_attempt=retry_attempt,
                        part_index=part_index,
                        file_list=part_files,
                    )
                except (OSError, ValueError) as exc:
                    self.publish_message(
                        f"[{dataset_name}] Job {idx}: could not prepare split retry "
                        f"part {part_index}: {exc}"
                    )
                    continue
                log_label = f"resub_{idx}_retry{retry_attempt}_part{part_index}"

            new_cluster = _submit_single_skim_job(
                idx,
                submission_dir,
                self.max_runtime,
                2000,
                extra_transfer_files=list(dataset_state.get("shared_transfer_files", [])),
                config_file="submit_config.txt",
                source_job_dir=part_info.get("dir") or source_job_dir,
                log_label=log_label,
            )
            if new_cluster:
                submitted_parts.append((part_info, new_cluster))

        if len(submitted_parts) != len(file_groups):
            for _, cluster_id in submitted_parts:
                _condor_rm_job(cluster_id, 0)
            self.publish_message(
                f"[{dataset_name}] Job {idx}: retry submission did not return "
                "all expected clusters; leaving it retryable."
            )
            job_state["status"] = "submitted"
            return

        def _record_part(
            target_idx: int,
            part_info: dict[str, str],
            cluster_id: str,
        ) -> None:
            tracked_job_info[target_idx] = {
                "dir": str(part_info.get("dir", "")),
                "save": str(part_info.get("save", "")),
                "meta": str(part_info.get("meta", "")),
            }
            target_state = (
                job_state
                if target_idx == idx
                else dataset_state.setdefault("jobs", {}).setdefault(str(target_idx), {})
            )
            target_state.update(
                cluster=cluster_id,
                proc=0,
                retries=retry_attempt,
                status="resubmitting",
                job_dir=str(part_info.get("dir", "")),
                save=str(part_info.get("save", "")),
                meta=str(part_info.get("meta", "")),
            )

        first_part, first_cluster = submitted_parts[0]
        _record_part(idx, first_part, first_cluster)
        next_idx = max(tracked_job_info.keys(), default=idx) + 1
        for part_info, cluster_id in submitted_parts[1:]:
            while str(next_idx) in dataset_state.get("jobs", {}):
                next_idx += 1
            _record_part(next_idx, part_info, cluster_id)
            next_idx += 1


class SkimHTCondorWorkflowProxy(BaseWorkflowProxy):
    """Delegate SkimTask workflow-level htcondor execution to the concrete skim chain."""

    workflow_type = "htcondor"

    def _delegate_task(self):
        return self.task.htcondor_delegate_task()

    def complete(self):
        return self._delegate_task().complete()

    def requires(self):
        return getattr(self.task, "htcondor_workflow_requires")()

    def output(self):
        return self._delegate_task().output()

    def run(self):
        super().run()


class SkimHTCondorWorkflow(BaseWorkflow):
    """Pseudo-htcondor workflow that wraps the concrete skim submission chain."""

    workflow_proxy_cls = SkimHTCondorWorkflowProxy
    exclude_index = True

class SkimTask(AnalysisMixin, SkimMixin, law.LocalWorkflow, SkimHTCondorWorkflow, DaskWorkflow):
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
        | getattr(SkimHTCondorWorkflow, "exclude_params_branch", set())
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
                        "config_path": os.path.join(str(entry["job_dir"]), "submit_config.txt"),
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
                    "config_path": os.path.join(self._jobs_dir, entry.name, "job_0", "submit_config.txt"),
                    "out_dir": os.path.join(self._outputs_dir, entry.name, "job_0"),
                }
                for entry in manifest.datasets
            ]

        manifest = DatasetManifest.load(self.dataset_manifest)
        return [
            {
                "dataset_name": entry.name,
                "job_dir": os.path.join(self._jobs_dir, entry.name),
                "config_path": os.path.join(self._jobs_dir, entry.name, "submit_config.txt"),
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
                        "config_path": str(entry.get("config_path", os.path.join(str(entry["job_dir"]), "submit_config.txt"))),
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

    def _uses_delegated_htcondor_workflow(self) -> bool:
        return self.is_workflow() and str(getattr(self, "effective_workflow", "")) == "htcondor"

    def htcondor_delegate_task(self):
        return MonitorSkimJobs.req(self)

    # ------------------------------------------------------------------
    # Workflow requirements
    # ------------------------------------------------------------------

    def workflow_requires(self):
        """Require prepared jobs before any executor dispatch begins."""
        if self._uses_delegated_htcondor_workflow():
            return DotDict(delegate=self.htcondor_delegate_task())
        reqs = super().workflow_requires()
        reqs["prep"] = self._prepare_skim_jobs_requirement()
        if self.make_test_job:
            reqs["test"] = RunSkimTestJob.req(self)
        return reqs

    def requires(self):
        reqs = [self._prepare_skim_jobs_requirement()]
        if self.make_test_job and not self.is_branch():
            reqs.append(RunSkimTestJob.req(self))
        if len(reqs) == 1:
            return reqs[0]
        return reqs

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def output(self):
        if self._uses_delegated_htcondor_workflow():
            return self.htcondor_delegate_task().output()
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
        if self._uses_delegated_htcondor_workflow():
            return self.htcondor_delegate_task().complete()
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
        if self._uses_delegated_htcondor_workflow():
            return

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
                manifest_lumi=_manifest_lumi_for_dataset(self.dataset_manifest, dataset),
                extra_overrides=_stitch_job_overrides(self),
            )

            task_label = f"SkimTask[{dataset.name}]"
            with PerformanceRecorder(task_label) as rec:
                result = _run_analysis_job(
                    exe_path=exe_path,
                    job_dir=job_dir,
                    stream_output=True,
                )

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
            config_path = str(branch_entry.get("config_path", os.path.join(job_dir, "submit_config.txt")))
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
                    source_config_path=config_path,
                    shared_archive_path=self._shared_archive_path,
                    stream_output=True,
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
    # HTCondor workflow configuration (mirrors SkimTask)
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
        raise RuntimeError(
            "SkimTask does not use LAW remote HTCondor job factories. Use workflow-level "
            "htcondor delegation via BuildSkimSubmission, SubmitSkimJobs, and MonitorSkimJobs."
        )

    def htcondor_group_wrapper_file(self):
        raise RuntimeError(
            "SkimTask does not use LAW remote HTCondor wrappers in delegated htcondor mode."
        )

    def htcondor_wrapper_file(self):
        raise RuntimeError(
            "SkimTask does not use LAW remote HTCondor wrappers in delegated htcondor mode."
        )

    def htcondor_job_file(self):
        raise RuntimeError(
            "SkimTask does not use LAW remote HTCondor job files in delegated htcondor mode."
        )

    def htcondor_log_directory(self):
        """Directory for HTCondor log files."""
        return law.LocalDirectoryTarget(
            os.path.join(self._run_dir, "law_htcondor_logs")
        )

    def htcondor_bootstrap_file(self):
        return None

    def htcondor_stageout_file(self):
        """Disable LAW stageout injection for skim HTCondor jobs."""
        return None

    def htcondor_job_config(self, config, job_num, branches):
        raise RuntimeError(
            "SkimTask branch-level LAW HTCondor execution is disabled. The workflow-level "
            "htcondor mode delegates to the concrete skim submission chain instead."
        )

    def htcondor_workflow_requires(self):
        """Mirror workflow requirements for compatibility with existing tests and callers."""
        return self.workflow_requires()

    def dask_workflow_requires(self):
        """Ensure prepared jobs and optional pre-flight validation exist first."""
        reqs = DotDict(prep=self._prepare_skim_jobs_requirement())
        if self.make_test_job:
            reqs["test"] = RunSkimTestJob.req(self)
        return reqs

    # ------------------------------------------------------------------
    # Dask workflow configuration (mirrors SkimTask)
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
                str(branch_entry.get("config_path", os.path.join(str(branch_entry["job_dir"]), "submit_config.txt"))),
                self._shared_archive_path,
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
            manifest_lumi=_manifest_lumi_for_dataset(self.dataset_manifest, dataset),
            extra_overrides=extra_overrides,
        )

        # ---- run the analysis executable -----------------------------------
        task_label = f"HistFillTask[{dataset.name}]"
        with PerformanceRecorder(task_label) as rec:
            result = _run_analysis_job(
                exe_path=exe_path,
                job_dir=job_dir,
                stream_output=True,
            )

        Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
        rec.save(perf_path_for(self.output().path))

        manifest = _build_histfill_manifest(
            dataset=dataset,
            submit_config_path=self.submit_config,
            dataset_manifest_path=self.dataset_manifest,
            job_config_path=os.path.join(job_dir, "submit_config.txt"),
        )
        manifest_path = os.path.join(out_dir, "output_manifest.yaml")
        manifest.save_yaml(manifest_path)
        self.publish_message(
            f"Histogram manifest written for '{dataset.name}': {manifest_path}"
        )

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
    return _shared_read_intweight_sign_hist(meta_file, sample_name)


def _read_weight_sign_sum(meta_file: str, sample_name: str) -> float:
    return _shared_read_weight_sign_sum(meta_file, sample_name)


def _derive_stitch_weights(
    group_name: str,
    meta_files: dict[str, Union[str, list[str]]],
) -> dict[str, list[float]]:
    return _shared_derive_stitch_weights(
        group_name,
        meta_files,
        _read_intweight_sign_hist,
    )


def _derive_stitch_cross_sections(
    group_name: str,
    meta_files: dict[str, Union[str, list[str]]],
    inclusive_sample: Union[str, list[str]],
    inclusive_cross_section: float,
) -> list[float]:
    return _shared_derive_stitch_cross_sections(
        group_name,
        meta_files,
        inclusive_sample,
        inclusive_cross_section,
        _read_intweight_sign_hist,
    )


def _normalize_stitch_cross_section_era(value: object) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    if normalized in {"run2", "2", "2016", "2017", "2018"}:
        return "run2"
    if normalized in {"24", "2024", "run3_24", "run3_2024"}:
        return "run3_24"
    if normalized in {
        "22_23",
        "2022_2023",
        "run3",
        "run3_22_23",
        "run3_2022_2023",
    }:
        return "run3_22_23"
    raise ValueError(
        f"Unknown stitching cross-section era '{value}'; expected run2, "
        "22/23, or 24"
    )


def _resolve_stitch_cross_section_era(
    group_cfg: dict[str, Any],
    sample_names: list[str],
    source_dataset_manifest: str = "",
    source_skim_name: str = "",
) -> str:
    configured = group_cfg.get("cross_section_era")
    if configured is not None:
        return _normalize_stitch_cross_section_era(configured)

    if source_dataset_manifest and os.path.isfile(source_dataset_manifest):
        manifest = DatasetManifest.load(source_dataset_manifest)
        selected = set(sample_names)
        years = {
            int(entry.year)
            for entry in manifest.datasets
            if entry.name in selected and entry.year is not None
        }
        run2_years = years & {16, 17, 18, 2016, 2017, 2018}
        run3_years = years & {22, 23, 24, 2022, 2023, 2024}
        if run2_years and run3_years:
            raise RuntimeError(
                "Binned cross sections cannot combine Run 2 and Run 3 "
                "samples in one stitching derivation"
            )
        if years and years <= {2016, 2017, 2018, 16, 17, 18}:
            return "run2"
        if years and years & {24, 2024}:
            if years - {24, 2024}:
                raise RuntimeError(
                    "Binned cross sections cannot combine Run 3 2024 samples "
                    "with 2022/23 samples in one stitching derivation"
                )
            return "run3_24"
        if years and years <= {2022, 2023, 22, 23}:
            return "run3_22_23"

    if source_skim_name:
        lowered = source_skim_name.lower()
        if "run2" in lowered:
            return "run2"
        if re.search(r"(?:^|[^0-9])24(?:[^0-9]|$)", lowered):
            return "run3_24"
    return "run3_22_23"


def _resolve_stitch_inclusive_sample(
    group_name: str,
    group_cfg: dict[str, Any],
    sample_names: list[str],
    cross_section_era: str = "",
) -> Optional[Union[str, list[str]]]:
    configured = group_cfg.get("inclusive_samples")
    if configured is None:
        configured = group_cfg.get("inclusive_sample")
    if configured is None:
        return None

    if isinstance(configured, dict):
        era_keys = [cross_section_era, "default"] if cross_section_era else ["default"]
        selected = next(
            (configured[key] for key in era_keys if key in configured), None
        )
        if selected is None:
            raise RuntimeError(
                f"Inclusive samples for group '{group_name}' have no entry for "
                f"cross-section era '{cross_section_era}'"
            )
        configured = selected

    try:
        configured_samples = _shared_normalize_sample_names(
            configured, "inclusive_sample"
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid inclusive samples for group '{group_name}': {exc}"
        ) from exc

    missing_samples = [
        sample_name
        for sample_name in configured_samples
        if sample_name not in sample_names
    ]
    if missing_samples:
        raise RuntimeError(
            f"Inclusive sample(s) {missing_samples} for group '{group_name}' "
            "are not present in the resolved stitch samples"
        )
    return (
        configured_samples[0]
        if len(configured_samples) == 1
        else configured_samples
    )


def _resolve_stitch_inclusive_cross_section(
    group_name: str,
    group_cfg: dict[str, Any],
    config: dict[str, Any],
    era: str,
) -> Optional[float]:
    configured = group_cfg.get(
        "inclusive_cross_section_pb",
        group_cfg.get("inclusive_cross_section"),
    )
    if configured is None:
        configured_by_group = config.get("inclusive_cross_sections", {})
        if isinstance(configured_by_group, dict):
            configured = configured_by_group.get(group_name)
            if isinstance(configured, dict):
                configured = configured.get(era)

    if configured is None:
        return None
    try:
        return float(configured)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Invalid inclusive cross section '{configured}' for group '{group_name}'"
        ) from exc


def _build_correctionlib_cset(
    all_scale_factors: dict[str, dict[str, list[float]]],
    description: str = "Stitching scale factors derived from counter_intWeightSignSum histograms",
) -> "correctionlib.schemav2.CorrectionSet":
    return _shared_build_correctionlib_cset(all_scale_factors, description)


def _stitch_meta_read_path(path: str) -> str:
    """Convert EOS stage-out paths into worker-independent XRootD URLs."""
    if path.startswith("/eos/"):
        return f"root://eosuser.cern.ch/{path}"
    return path


def _stitch_meta_files_for_sample(
    run_name: str,
    sample_name: str,
) -> list[str]:
    """Return local metadata files or staged Condor metadata destinations."""
    output_dir = Path(WORKSPACE) / f"skimRun_{run_name}" / "outputs" / sample_name
    local_paths = sorted(
        str(path)
        for path in output_dir.rglob("meta.root")
        if path.is_file()
    )
    if local_paths:
        return local_paths

    submission_dir = (
        Path(WORKSPACE)
        / f"skimRun_{run_name}"
        / "condor_submissions"
        / _safe_dataset_submission_name(sample_name)
    )
    staged_paths: list[str] = []
    for config_path in sorted(submission_dir.glob("job_*/submit_config.txt")):
        config = read_config(str(config_path))
        meta_path = str(
            config.get("__orig_metaFile", config.get("metaFile", "")) or ""
        ).strip()
        if not meta_path:
            continue
        if (
            os.path.isfile(meta_path)
            or meta_path.startswith("root://")
            or meta_path.startswith(("/eos/", "/store/"))
        ):
            staged_paths.append(_stitch_meta_read_path(meta_path))
    return sorted(dict.fromkeys(staged_paths))


def _resolve_stitch_meta_files(
    group_name: str,
    group_cfg: dict[str, Any],
    stitch_config_path: str,
    source_skim_name: str = "",
    source_dataset_manifest: str = "",
) -> dict[str, Union[str, list[str]]]:
    """Resolve one stitch group to sample names and metadata ROOT files."""
    raw_meta_files = group_cfg.get("meta_files")
    if isinstance(raw_meta_files, dict):
        resolved: dict[str, Union[str, list[str]]] = {}
        for sample_name, raw_paths in raw_meta_files.items():
            paths = raw_paths if isinstance(raw_paths, list) else [raw_paths]
            resolved_paths = [
                _resolve_analysis_config_path(stitch_config_path, str(path))
                for path in paths
            ]
            resolved[str(sample_name)] = (
                resolved_paths[0]
                if len(resolved_paths) == 1
                else resolved_paths
            )
        return resolved

    if not source_skim_name:
        raise RuntimeError(
            f"Group '{group_name}' must contain 'meta_files' unless "
            "source_skim_name is configured."
        )
    if not source_dataset_manifest:
        raise RuntimeError(
            "source_dataset_manifest is required when resolving stitch metadata "
            "from a source skim run."
        )

    manifest = DatasetManifest.load(source_dataset_manifest)
    sample_names = group_cfg.get("samples", [])
    if sample_names and not isinstance(sample_names, list):
        raise RuntimeError(
            f"Group '{group_name}' has a 'samples' value that is not a list."
        )
    if not sample_names:
        selector = group_cfg.get("select")
        if selector is None:
            selector = {"group": group_name}
        if not isinstance(selector, dict):
            raise RuntimeError(
                f"Group '{group_name}' has a 'select' value that is not a mapping."
            )
        try:
            sample_names = [entry.name for entry in manifest.query(**selector)]
        except TypeError as exc:
            raise RuntimeError(
                f"Invalid dataset selector for stitch group '{group_name}': {selector}"
            ) from exc

    if not sample_names:
        raise RuntimeError(
            f"Stitch group '{group_name}' did not resolve to any dataset samples."
        )

    resolved = {}
    for sample_name in sample_names:
        sample = str(sample_name)
        meta_paths = _stitch_meta_files_for_sample(source_skim_name, sample)
        if not meta_paths:
            raise RuntimeError(
                f"No meta.root files found for stitch sample '{sample}' under "
                f"'skimRun_{source_skim_name}/outputs/{sample}' or its Condor "
                "submission metadata."
            )
        resolved[sample] = meta_paths[0] if len(meta_paths) == 1 else meta_paths
    return resolved


class StitchingDerivationTask(law.Task):
    """
    Derive binwise MC stitching scale factors from counter histograms.

    This task reads the ``counter_intWeightSignSum_{sample}`` histograms
    written by the framework's :class:`CounterService` from the merged meta
    ROOT files of a set of stitched MC samples and computes the binwise
    scale factor

        b_n(k) = max(C_n(k), 0) / Σ_m max(C_m(k), 0)

    where ``C_n(k) = P_n(k) − N_n(k)`` is the net signed count (positive
    minus negative weight events) for sample *n* in stitch bin *k*. Negative
    values are treated as zero before normalisation. See the
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
    source_skim_name = luigi.Parameter(
        default="",
        description=(
            "Optional completed SkimTask run used to discover metadata ROOT "
            "files for declarative stitch groups."
        ),
    )
    source_submit_config = luigi.Parameter(
        default="",
        description="Submit config used for the source counter pass.",
    )
    source_exe = luigi.Parameter(
        default="",
        description="Executable used for the source counter pass.",
    )
    source_dataset_manifest = luigi.Parameter(
        default="",
        description="Dataset manifest used for source sample selection.",
    )
    source_file_source = luigi.Parameter(default="")
    source_file_source_name = luigi.Parameter(default="")
    source_files_per_job = luigi.IntParameter(default=50)
    source_partition = luigi.Parameter(default="file_group")
    source_entries_per_job = luigi.IntParameter(default=100_000)
    source_x509 = luigi.Parameter(default="")
    source_stage_in = luigi.BoolParameter(default=False)
    source_root_setup = luigi.Parameter(default="")
    source_container_setup = luigi.Parameter(default="")
    source_want_os = luigi.Parameter(default="")
    source_python_env = luigi.Parameter(default="")
    source_make_test_job = luigi.BoolParameter(default=False)
    source_dask_scheduler = luigi.Parameter(default="", significant=False)
    source_dask_workers = luigi.IntParameter(default=1, significant=False)
    source_max_runtime = luigi.IntParameter(default=3600, significant=False)
    source_workflow = luigi.Parameter(default="", significant=False)
    source_stitch_mode = luigi.Parameter(default="none", significant=False)

    @property
    def _output_dir(self) -> str:
        return os.path.join(WORKSPACE, f"stitchWeights_{self.name}")

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._output_dir, "stitch_weights.json")
        )

    def requires(self):
        if not self.source_skim_name:
            return None
        required = {
            "name": self.source_skim_name,
            "submit_config": self.source_submit_config,
            "exe": self.source_exe,
            "dataset_manifest": self.source_dataset_manifest,
            "file_source": self.source_file_source,
            "file_source_name": self.source_file_source_name,
            "files_per_job": self.source_files_per_job,
            "partition": self.source_partition,
            "entries_per_job": self.source_entries_per_job,
            "x509": self.source_x509,
            "stage_in": self.source_stage_in,
            "root_setup": self.source_root_setup,
            "container_setup": self.source_container_setup,
            "want_os": self.source_want_os,
            "python_env": self.source_python_env,
            "make_test_job": self.source_make_test_job,
            "dask_scheduler": self.source_dask_scheduler,
            "dask_workers": self.source_dask_workers,
            "max_runtime": self.source_max_runtime,
            "workflow": self.source_workflow,
            "stitch_mode": self.source_stitch_mode,
        }
        missing = [key for key in ("submit_config", "exe", "dataset_manifest")
                   if not required[key]]
        if missing:
            raise RuntimeError(
                "Source stitching requires: " + ", ".join(missing)
            )
        return SkimTask.req(self, **required)

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
        all_binned_cross_sections: dict[str, dict[str, Any]] = {}

        with PerformanceRecorder("StitchingDerivationTask") as rec:
            for group_name, group_cfg in config["groups"].items():
                if not isinstance(group_cfg, dict):
                    raise RuntimeError(
                        f"Group '{group_name}' in '{config_path}' must be a mapping."
                    )

                meta_files = _resolve_stitch_meta_files(
                    str(group_name),
                    group_cfg,
                    config_path,
                    source_skim_name=self.source_skim_name,
                    source_dataset_manifest=self.source_dataset_manifest,
                )

                self.publish_message(
                    f"Processing group '{group_name}' "
                    f"({len(meta_files)} sample(s): {list(meta_files)})…"
                )

                scale_factors = _derive_stitch_weights(group_name, meta_files)
                all_scale_factors[group_name] = scale_factors

                cross_section_era = _resolve_stitch_cross_section_era(
                    group_cfg,
                    list(meta_files),
                    source_dataset_manifest=self.source_dataset_manifest,
                    source_skim_name=self.source_skim_name,
                )
                inclusive_sample = _resolve_stitch_inclusive_sample(
                    str(group_name),
                    group_cfg,
                    list(meta_files),
                    cross_section_era,
                )
                inclusive_cross_section = _resolve_stitch_inclusive_cross_section(
                    str(group_name), group_cfg, config, cross_section_era
                )
                if inclusive_sample and inclusive_cross_section is not None:
                    binned_cross_sections = _derive_stitch_cross_sections(
                        str(group_name),
                        meta_files,
                        inclusive_sample,
                        inclusive_cross_section,
                    )
                    all_binned_cross_sections[str(group_name)] = {
                        "era": cross_section_era,
                        "inclusive_sample": inclusive_sample,
                        "inclusive_samples": _shared_normalize_sample_names(
                            inclusive_sample, "inclusive_sample"
                        ),
                        "inclusive_cross_section_pb": inclusive_cross_section,
                        "bin_fractions": [
                            value / inclusive_cross_section
                            for value in binned_cross_sections
                        ],
                        "binned_cross_sections_pb": binned_cross_sections,
                    }
                    self.publish_message(
                        f"  group '{group_name}': binned cross sections computed from "
                        f"inclusive sample '{inclusive_sample}' for {cross_section_era}."
                    )
                elif inclusive_cross_section is not None:
                    self.publish_message(
                        f"  group '{group_name}': no unambiguous inclusive sample; "
                        "binned cross sections were not written for this group."
                    )

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

        cross_sections_path = os.path.join(
            self._output_dir, "stitch_cross_sections.json"
        )
        with open(cross_sections_path, "w") as fh:
            json.dump(
                {
                    "schema_version": 1,
                    "description": (
                        "Cross sections split into stitch bins using the "
                        "nonnegative net signed event fractions of each "
                        "inclusive sample."
                    ),
                    "groups": all_binned_cross_sections,
                },
                fh,
                indent=2,
            )

        rec.save(os.path.join(self._output_dir, "stitch_derivation.perf.json"))
        self.publish_message(
            f"Stitch weights (correctionlib) written to: {json_path}; "
            f"binned cross sections written to: {cross_sections_path}"
        )


def _make_stitching_derivation_task(
    task: SkimTask,
    stitch_config: str,
    stitch_name: str,
    source_skim_name: str,
    source_workflow: str,
) -> StitchingDerivationTask:
    return StitchingDerivationTask(
        stitch_config=stitch_config,
        name=stitch_name or task.name,
        source_skim_name=source_skim_name or f"{task.name}_counts",
        source_submit_config=task.submit_config,
        source_exe=task.exe,
        source_dataset_manifest=task.dataset_manifest,
        source_file_source=task.file_source,
        source_file_source_name=task.file_source_name,
        source_files_per_job=task.files_per_job,
        source_partition=task.partition,
        source_entries_per_job=task.entries_per_job,
        source_x509=task.x509,
        source_stage_in=task.stage_in,
        source_root_setup=task.root_setup,
        source_container_setup=task.container_setup,
        source_want_os=task.want_os,
        source_python_env=task.python_env,
        source_make_test_job=False,
        source_dask_scheduler=task.dask_scheduler,
        source_dask_workers=task.dask_workers,
        source_max_runtime=task.max_runtime,
        source_workflow=source_workflow,
        source_stitch_mode="none",
    )


class StitchingApplySubmitJobs(SubmitSkimJobs):
    """Submit application jobs only after stitching weights are available."""

    task_namespace = ""

    stitch_config = luigi.Parameter()
    stitch_name = luigi.Parameter(default="", significant=False)
    source_skim_name = luigi.Parameter(default="", significant=False)
    source_workflow = luigi.Parameter(default="", significant=False)

    def __init__(self, *args, **kwargs):
        stitch_name = kwargs.get("stitch_name") or kwargs.get("name", "")
        kwargs.setdefault("stitch_mode", "correctionlib")
        kwargs.setdefault(
            "stitch_weights",
            os.path.join(WORKSPACE, f"stitchWeights_{stitch_name}", "stitch_weights.json"),
        )
        super().__init__(*args, **kwargs)

    def _derivation_task(self) -> StitchingDerivationTask:
        return _make_stitching_derivation_task(
            self,
            self.stitch_config,
            self.stitch_name,
            self.source_skim_name,
            self.source_workflow,
        )

    def requires(self):
        requirements = super().requires()
        requirements["stitch_weights"] = self._derivation_task()
        return requirements

    def complete(self):
        if not os.path.isfile(self._derivation_task().output().path):
            return False
        return super().complete()

    def run(self):
        if self.output().exists():
            return
        super().run()


class StitchingApplyMonitorJobs(MonitorSkimJobs):
    """Monitor application jobs after their stitching weights are ready."""

    task_namespace = ""

    stitch_config = luigi.Parameter()
    stitch_name = luigi.Parameter(default="", significant=False)
    source_skim_name = luigi.Parameter(default="", significant=False)
    source_workflow = luigi.Parameter(default="", significant=False)

    def requires(self):
        return StitchingApplySubmitJobs.req(
            self,
            stitch_config=self.stitch_config,
            stitch_name=self.stitch_name,
            source_skim_name=self.source_skim_name,
            source_workflow=self.source_workflow,
        )


# ===========================================================================
# Task 4 – StitchingApplyTask
# ===========================================================================

class StitchingApplyTask(SkimTask):
    """Run the analysis again with LAW-derived stitching weights.

    The task first runs a counter-only :class:`SkimTask` named
    ``<name>_counts`` through :class:`StitchingDerivationTask`, then reuses the
    normal SkimTask executors for the application pass.  The generated
    correctionlib JSON is staged into every application job as
    ``stitchWeights`` and the executable receives ``stitchMode=correctionlib``.
    The weights are included once in the shared worker archive.

    The stitch configuration accepts either explicit ``meta_files`` or
    declarative groups using ``samples`` / ``select``; in the latter case the
    source skim outputs are discovered automatically.
    """

    task_namespace = ""

    stitch_config = luigi.Parameter(
        description="YAML configuration describing the stitched sample groups.",
    )
    stitch_name = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Name of the derived stitching-weight output. Defaults to the "
            "application run name."
        ),
    )
    source_skim_name = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Name of the counter pass. Defaults to '<name>_counts'."
        ),
    )
    max_retries = luigi.IntParameter(
        default=50,
        significant=False,
        description="Maximum resubmission attempts per failed job.",
    )

    def __init__(self, *args, **kwargs):
        apply_name = kwargs.get("name", "")
        stitch_name = kwargs.get("stitch_name") or apply_name
        source_skim_name = kwargs.get("source_skim_name") or f"{apply_name}_counts"
        kwargs.setdefault("stitch_name", stitch_name)
        kwargs.setdefault("source_skim_name", source_skim_name)
        kwargs.setdefault("stitch_mode", "correctionlib")
        kwargs.setdefault(
            "stitch_weights",
            os.path.join(WORKSPACE, f"stitchWeights_{stitch_name}", "stitch_weights.json"),
        )
        super().__init__(*args, **kwargs)

    def htcondor_delegate_task(self):
        return StitchingApplyMonitorJobs.req(
            self,
            stitch_config=self.stitch_config,
            stitch_name=self.stitch_name,
            source_skim_name=self.source_skim_name,
            source_workflow=self.workflow,
            max_retries=self.max_retries,
        )

    def _derivation_task(self) -> StitchingDerivationTask:
        return _make_stitching_derivation_task(
            self,
            self.stitch_config,
            self.stitch_name,
            self.source_skim_name,
            self.workflow,
        )

    def workflow_requires(self):
        requirements = super().workflow_requires()
        requirements["stitch_weights"] = self._derivation_task()
        return requirements

    def requires(self):
        requirements = super().requires()
        derivation = self._derivation_task()
        if requirements is None:
            return derivation
        if isinstance(requirements, list):
            return requirements + [derivation]
        return [requirements, derivation]


# ===========================================================================
# Task 5 – StitchingValidationTask
# ===========================================================================

def _validate_stitch_monitor_summary(run_name: str) -> None:
    """Refuse validation when the application monitor recorded failed jobs."""
    summary_path = Path(WORKSPACE) / f"skimRun_{run_name}" / "all_outputs_verified.txt"
    if not summary_path.is_file():
        return

    summary: dict[str, str] = {}
    with summary_path.open() as fh:
        for raw_line in fh:
            key, separator, value = raw_line.partition("=")
            if separator:
                summary[key.strip()] = value.strip()

    try:
        permanent_failures = int(summary.get("perm_fail", "0"))
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid permanent-failure count in stitching monitor summary "
            f"'{summary_path}'."
        ) from exc

    if permanent_failures <= 0:
        return

    failed_jobs = summary.get("perm_failed_jobs", "")
    details = f" Failed jobs: {failed_jobs}." if failed_jobs else ""
    raise RuntimeError(
        f"Stitching application run '{run_name}' has {permanent_failures} "
        f"permanently failed job(s); refusing to validate partial outputs. "
        f"See '{summary_path}'.{details}"
    )


def _stitch_output_meta_files(
    run_name: str,
    dataset_manifest_path: str,
) -> dict[str, list[str]]:
    """Return every application metadata file grouped by manifest sample."""
    _validate_stitch_monitor_summary(run_name)
    manifest = DatasetManifest.load(dataset_manifest_path)
    meta_files: dict[str, list[str]] = {}
    missing_samples: list[str] = []

    for dataset in manifest.datasets:
        paths = _stitch_meta_files_for_sample(run_name, dataset.name)
        if paths:
            meta_files[dataset.name] = paths
        else:
            missing_samples.append(dataset.name)

    if missing_samples:
        raise RuntimeError(
            f"Stitching application run '{run_name}' has no meta.root output for: "
            + ", ".join(missing_samples)
        )
    return meta_files


def _root_key_basename(key: object) -> str:
    """Return a ROOT object key without its directory or cycle suffix."""
    return str(key).split(";", 1)[0].rsplit("/", 1)[-1]


def _load_stitch_validation_adapter(
    adapter_name: str,
) -> Optional[Any]:
    """Load an optional analysis-owned validation adapter module."""
    if not adapter_name:
        return None
    import importlib

    workspace_path = os.path.abspath(WORKSPACE)
    added_workspace_path = workspace_path not in sys.path
    if added_workspace_path:
        sys.path.insert(0, workspace_path)
    try:
        return importlib.import_module(adapter_name)
    except ImportError as exc:
        raise RuntimeError(
            f"Could not import stitching validation adapter '{adapter_name}'."
        ) from exc
    finally:
        if added_workspace_path:
            sys.path.remove(workspace_path)


def _stitch_validation_boundaries(
    stitch_config: dict[str, Any],
    submit_config: str = "",
    validation_adapter: Optional[Any] = None,
) -> dict[str, list[float]]:
    """Resolve generic histogram boundaries or delegate to an adapter."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "validation_boundaries", None)
        if callable(resolver):
            return resolver(submit_config)

    validation_config = stitch_config.get("validation", {})
    raw_boundaries = (
        validation_config.get("boundaries", {})
        if isinstance(validation_config, dict)
        else {}
    )
    if not isinstance(raw_boundaries, dict):
        raise RuntimeError("Stitch validation 'boundaries' must be a mapping.")
    boundaries: dict[str, list[float]] = {}
    for variable, values in raw_boundaries.items():
        if not isinstance(values, list):
            raise RuntimeError(
                f"Stitch validation boundaries for '{variable}' must be a list."
            )
        try:
            boundaries[str(variable)] = [float(value) for value in values]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Stitch validation boundaries for '{variable}' must be numeric."
            ) from exc
    return boundaries


def _stitch_validation_boundaries_by_process(
    stitch_config: dict[str, Any],
    submit_config: str = "",
    validation_adapter: Optional[Any] = None,
) -> dict[str, list[float]]:
    """Resolve generic group-specific boundaries or delegate to an adapter."""
    if validation_adapter is not None:
        resolver = getattr(
            validation_adapter,
            "validation_boundaries_by_process",
            None,
        )
        if callable(resolver):
            return resolver(submit_config)

    validation_config = stitch_config.get("validation", {})
    raw_boundaries = (
        validation_config.get("boundaries_by_group", {})
        if isinstance(validation_config, dict)
        else {}
    )
    if not isinstance(raw_boundaries, dict):
        raise RuntimeError(
            "Stitch validation 'boundaries_by_group' must be a mapping."
        )
    return {
        str(group): [float(value) for value in values]
        for group, values in raw_boundaries.items()
        if isinstance(values, list)
    }


def _load_stitch_cross_section_report(
    report_path: str,
) -> dict[str, dict[str, Any]]:
    """Load and validate per-bin cross sections from a derivation report."""
    import numpy as np

    if not os.path.isfile(report_path):
        raise RuntimeError(
            f"Stitching cross-section report not found: '{report_path}'"
        )
    with open(report_path) as fh:
        report = json.load(fh)

    groups = report.get("groups") if isinstance(report, dict) else None
    if not isinstance(groups, dict):
        raise RuntimeError(
            f"Stitching cross-section report '{report_path}' has no groups mapping."
        )

    parsed: dict[str, dict[str, Any]] = {}
    for group_name, group_report in groups.items():
        if not isinstance(group_report, dict):
            raise RuntimeError(
                f"Cross-section report group '{group_name}' must be a mapping."
            )
        raw_bins = group_report.get("binned_cross_sections_pb")
        if not isinstance(raw_bins, list) or not raw_bins:
            raise RuntimeError(
                f"Cross-section report group '{group_name}' has no "
                "binned_cross_sections_pb values."
            )
        try:
            bins = [float(value) for value in raw_bins]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Cross-section report group '{group_name}' contains a non-numeric "
                "binned cross section."
            ) from exc
        if any(not np.isfinite(value) or value < 0.0 for value in bins):
            raise RuntimeError(
                f"Cross-section report group '{group_name}' contains invalid "
                "binned cross sections."
            )
        raw_inclusive_sample = group_report.get("inclusive_sample")
        try:
            inclusive_samples = _shared_normalize_sample_names(
                raw_inclusive_sample, "inclusive_sample"
            )
        except ValueError:
            raise RuntimeError(
                f"Cross-section report group '{group_name}' has no valid "
                "inclusive_sample."
            )
        inclusive_sample: Union[str, list[str]] = (
            inclusive_samples[0]
            if len(inclusive_samples) == 1
            else inclusive_samples
        )
        parsed[str(group_name)] = {
            "era": str(group_report.get("era", "")),
            "inclusive_sample": inclusive_sample,
            "binned_cross_sections_pb": bins,
        }
    return parsed


def _stitch_validation_sample_groups(
    sample_names: list[str],
    manifest: DatasetManifest,
    stitch_config: str,
    weights: dict[str, Any],
    report_groups: set[str],
) -> dict[str, str]:
    """Resolve every application sample to its derived stitching group."""
    sample_groups: dict[str, str] = {}

    for correction in weights.get("corrections", []):
        if not isinstance(correction, dict):
            continue
        group_name = str(correction.get("name", ""))
        data = correction.get("data")
        content = data.get("content", []) if isinstance(data, dict) else []
        if data and data.get("nodetype") == "category":
            for item in content:
                if isinstance(item, dict) and "key" in item:
                    sample_groups[str(item["key"])] = group_name

    for entry in manifest.datasets:
        if entry.name in sample_names and entry.group in report_groups:
            sample_groups.setdefault(entry.name, str(entry.group))

    if stitch_config:
        config_path = os.path.abspath(stitch_config)
        if not os.path.isfile(config_path):
            raise RuntimeError(f"Stitch configuration file not found: '{config_path}'")
        with open(config_path) as fh:
            config = yaml.safe_load(fh) or {}
        groups = config.get("groups", {})
        if isinstance(groups, dict):
            for group_name, group_cfg in groups.items():
                if not isinstance(group_cfg, dict):
                    continue
                group = str(group_name)
                configured_samples = group_cfg.get("samples", [])
                if isinstance(configured_samples, list):
                    for sample_name in configured_samples:
                        sample_groups.setdefault(str(sample_name), group)
                meta_files = group_cfg.get("meta_files", {})
                if isinstance(meta_files, dict):
                    for sample_name in meta_files:
                        sample_groups.setdefault(str(sample_name), group)
                selector = group_cfg.get("select")
                if isinstance(selector, dict):
                    try:
                        selected = manifest.query(**selector)
                    except TypeError as exc:
                        raise RuntimeError(
                            f"Invalid dataset selector for stitch group '{group}': "
                            f"{selector}"
                        ) from exc
                    for entry in selected:
                        sample_groups.setdefault(entry.name, group)

    missing = sorted(set(sample_names) - set(sample_groups))
    if missing:
        raise RuntimeError(
            "Could not resolve stitching group for application sample(s): "
            + ", ".join(missing)
        )
    unresolved_groups = sorted(
        {sample_groups[sample] for sample in sample_names} - set(report_groups)
    )
    if unresolved_groups:
        raise RuntimeError(
            "Application samples resolved to groups missing from the cross-section "
            "report: "
            + ", ".join(unresolved_groups)
        )
    return {sample: sample_groups[sample] for sample in sample_names}


def _stitch_validation_process_label(
    group_name: str,
    validation_adapter: Optional[Any] = None,
) -> str:
    """Return an adapter-provided or sanitized group label."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "process_label", None)
        if callable(resolver):
            return str(resolver(group_name))
    label = re.sub(r"[^A-Za-z0-9]+", "_", group_name).strip("_")
    return label or "unknown"


def _stitch_validation_year_set(
    entry: Optional[DatasetEntry],
    report_era: str,
    validation_adapter: Optional[Any] = None,
) -> str:
    """Return an adapter-provided or report-era validation label."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "year_set", None)
        if callable(resolver):
            return str(resolver(entry, report_era))
    if str(report_era).strip():
        return str(report_era).strip()
    year = getattr(entry, "year", None)
    return str(year) if year is not None else "unknown"


def _stitch_validation_sample_scale(
    sample_name: str,
    meta_files: list[str],
    group_report: dict[str, Any],
    excluded_bins: Optional[set[int]] = None,
    stitch_factors: Optional[list[float]] = None,
    factor_threshold: float = 1e-6,
) -> tuple[float, float, float]:
    """Return (plot scale, sample cross section, signed event sum)."""
    import numpy as np

    sign_counts: Optional[np.ndarray] = None
    for meta_file in meta_files:
        values = np.asarray(
            _read_intweight_sign_hist(meta_file, sample_name),
            dtype=float,
        )
        if sign_counts is None:
            sign_counts = values
        elif len(values) != len(sign_counts):
            raise RuntimeError(
                f"Signed counter histogram length mismatch for sample '{sample_name}'."
            )
        else:
            sign_counts += values

    if sign_counts is None or not len(sign_counts):
        raise RuntimeError(f"No signed counter values found for sample '{sample_name}'.")
    if any(not np.isfinite(value) for value in sign_counts):
        raise RuntimeError(
            f"Signed counter histogram for sample '{sample_name}' contains non-finite values."
        )

    cross_section_bins = np.asarray(
        group_report["binned_cross_sections_pb"],
        dtype=float,
    )
    if len(sign_counts) > len(cross_section_bins):
        raise RuntimeError(
            f"Cross-section report has {len(cross_section_bins)} bins for sample "
            f"'{sample_name}', but its signed counter has {len(sign_counts)} bins."
        )
    if stitch_factors is None:
        active_bins = sign_counts > 0.0
    else:
        if factor_threshold < 0.0 or not np.isfinite(factor_threshold):
            raise RuntimeError(
                "Stitch factor threshold must be finite and non-negative."
            )
        factors = np.zeros(len(sign_counts), dtype=float)
        factor_values = np.asarray(stitch_factors, dtype=float)
        if any(not np.isfinite(value) for value in factor_values):
            raise RuntimeError(
                f"Stitch factors for sample '{sample_name}' contain non-finite values."
            )
        factor_count = min(len(factors), len(factor_values))
        factors[:factor_count] = factor_values[:factor_count]
        active_bins = (sign_counts > 0.0) & (factors > factor_threshold)
    if excluded_bins:
        invalid_bins = [
            index for index in excluded_bins
            if index < 0 or index >= len(sign_counts)
        ]
        if invalid_bins:
            raise RuntimeError(
                f"Excluded stitch bin(s) {invalid_bins} are outside the signed "
                f"counter range for sample '{sample_name}'."
            )
        active_bins[list(excluded_bins)] = False

    active_signed_sum = float(np.sum(sign_counts[active_bins]))
    if active_signed_sum == 0.0:
        raise RuntimeError(
            f"Sample '{sample_name}' has zero signed event sum in its active "
            "stitch bins; cannot normalize plots."
        )
    sample_cross_section = float(np.sum(cross_section_bins[: len(sign_counts)][active_bins]))
    return sample_cross_section / active_signed_sum, sample_cross_section, active_signed_sum


def _stitch_validation_sample_factors(
    weights: dict[str, Any],
    group_name: str,
    sample_name: str,
) -> list[float]:
    """Return the derived correction factors for one validation sample."""
    for correction in weights.get("corrections", []):
        if not isinstance(correction, dict) or str(correction.get("name")) != group_name:
            continue
        data = correction.get("data")
        if not isinstance(data, dict) or data.get("nodetype") != "category":
            break
        for item in data.get("content", []):
            if not isinstance(item, dict) or str(item.get("key")) != sample_name:
                continue
            value = item.get("value")
            if not isinstance(value, dict) or value.get("nodetype") != "binning":
                break
            content = value.get("content")
            if not isinstance(content, list):
                break
            try:
                return [float(factor) for factor in content]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitch factors for sample '{sample_name}' are not numeric."
                ) from exc
    raise RuntimeError(
        f"Stitching correction JSON has no factors for sample '{sample_name}' "
        f"in group '{group_name}'."
    )


def _stitch_validation_configured_weight_scale(
    entry: Optional[DatasetEntry],
) -> float:
    """Return the per-event scale already present in application histograms."""
    import math

    if entry is None:
        return 1.0

    configured_scale = (
        (1.0 if entry.xsec is None else float(entry.xsec))
        * float(entry.kfac)
        * float(entry.filter_efficiency)
        * float(entry.extra_scale)
    )
    if not math.isfinite(configured_scale) or configured_scale == 0.0:
        raise RuntimeError(
            f"Application weight scale for sample '{entry.name}' must be finite "
            "and non-zero for validation plots."
        )
    return configured_scale


def _find_stitch_histogram_key(root_file: object, histogram_name: str) -> str:
    keys = getattr(root_file, "keys")(recursive=True)
    for key in keys:
        if _root_key_basename(key) == histogram_name:
            return str(key).split(";", 1)[0]
    raise RuntimeError(
        f"ROOT output is missing validation histogram '{histogram_name}'."
    )


def _stitch_validation_variable_boundaries(
    variable: str,
    process_family: str,
    boundaries: dict[str, list[float]],
    process_boundaries: dict[str, list[float]],
    validation_adapter: Optional[Any] = None,
) -> list[float]:
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "variable_boundaries", None)
        if callable(resolver):
            return list(
                resolver(variable, process_family, boundaries, process_boundaries)
            )
    return list(boundaries.get(variable, process_boundaries.get(process_family, [])))


def _stitch_validation_histogram_specs(
    stitch_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Resolve generic inclusive/stitched histogram plotting specifications."""
    validation_config = stitch_config.get("validation", {})
    if validation_config is None:
        validation_config = {}
    if not isinstance(validation_config, dict):
        raise RuntimeError("Stitch validation configuration must be a mapping.")

    raw_specs = validation_config.get("histograms")
    if raw_specs is None:
        return [
            {
                "name": "LHE_Vpt",
                "stitched_name": "LHE_Vpt_stitched",
                "label": "LHE VpT [GeV]",
                "boundaries": None,
                "xlim": None,
                "log_y": True,
                "ratio_range": [0.8, 1.2],
            },
            {
                "name": "LHE_NpNLO",
                "stitched_name": "LHE_NpNLO_stitched",
                "label": "LHE NpNLO",
                "boundaries": None,
                "xlim": None,
                "log_y": True,
                "ratio_range": [0.8, 1.2],
            },
        ]

    if not isinstance(raw_specs, list) or not raw_specs:
        raise RuntimeError(
            "Stitch validation 'histograms' must be a non-empty list."
        )

    specs: list[dict[str, Any]] = []
    for raw_spec in raw_specs:
        if not isinstance(raw_spec, dict):
            raise RuntimeError("Each stitch validation histogram must be a mapping.")
        name = str(raw_spec.get("name", "")).strip()
        if not name:
            raise RuntimeError(
                "Each stitch validation histogram requires a non-empty 'name'."
            )
        stitched_name = str(
            raw_spec.get("stitched_name", f"{name}_stitched")
        ).strip()
        if not stitched_name:
            raise RuntimeError(
                f"Stitch validation histogram '{name}' has an empty 'stitched_name'."
            )

        boundaries = raw_spec.get("boundaries")
        if boundaries is not None:
            if not isinstance(boundaries, list):
                raise RuntimeError(
                    f"Stitch validation boundaries for '{name}' must be a list."
                )
            try:
                boundaries = [float(value) for value in boundaries]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitch validation boundaries for '{name}' must be numeric."
                ) from exc

        xlim = raw_spec.get("xlim")
        if xlim is not None:
            if not isinstance(xlim, list) or len(xlim) != 2:
                raise RuntimeError(
                    f"Stitch validation xlim for '{name}' must contain two values."
                )
            try:
                xlim = [float(value) for value in xlim]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitch validation xlim for '{name}' must be numeric."
                ) from exc
            if xlim[1] <= xlim[0]:
                raise RuntimeError(
                    f"Stitch validation xlim for '{name}' must be increasing."
                )

        ratio_range = raw_spec.get("ratio_range", [0.8, 1.2])
        if not isinstance(ratio_range, list) or len(ratio_range) != 2:
            raise RuntimeError(
                f"Stitch validation ratio_range for '{name}' must contain two values."
            )
        try:
            ratio_range = [float(value) for value in ratio_range]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Stitch validation ratio_range for '{name}' must be numeric."
            ) from exc
        if ratio_range[1] <= ratio_range[0]:
            raise RuntimeError(
                f"Stitch validation ratio_range for '{name}' must be increasing."
            )

        specs.append(
            {
                "name": name,
                "stitched_name": stitched_name,
                "label": str(raw_spec.get("label", name)),
                "boundaries": boundaries,
                "xlim": xlim,
                "log_y": bool(raw_spec.get("log_y", True)),
                "ratio_range": ratio_range,
            }
        )
    return specs


def _stitch_validation_histogram_data(
    histogram: Any,
) -> tuple[Any, Any, Optional[Any]]:
    import numpy as np

    values, edges = histogram.to_numpy(flow=False)
    values = np.asarray(values, dtype=float)
    edges = np.asarray(edges, dtype=float)
    variances = None
    variance_reader = getattr(histogram, "variances", None)
    if variance_reader is not None:
        raw_variances = variance_reader(flow=False)
        if raw_variances is not None:
            variances = np.asarray(raw_variances, dtype=float)
            if variances.shape != values.shape:
                raise RuntimeError(
                    "Validation histogram variances do not match its values."
                )
    return values, edges, variances


def _stitch_validation_excluded_bins(
    sample_name: str,
    group_name: str,
    group_report: dict[str, Any],
    submit_config: str,
    number_of_bins: int,
    validation_adapter: Optional[Any] = None,
) -> set[int]:
    """Return adapter-provided bins absent from a sample."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "excluded_bins", None)
        if callable(resolver):
            return set(
                resolver(
                    sample_name,
                    group_name,
                    group_report,
                    submit_config,
                    number_of_bins,
                )
            )
    return set()


def _write_stitch_validation_plots(
    meta_files: dict[str, list[str]],
    output_dir: str,
    boundaries: dict[str, list[float]],
    sample_metadata: dict[str, DatasetEntry],
    sample_groups: dict[str, str],
    cross_section_groups: dict[str, dict[str, Any]],
    process_boundaries: dict[str, list[float]],
    weights: dict[str, Any],
    histogram_specs: Optional[list[dict[str, Any]]] = None,
    submit_config: str = "",
    validation_adapter: Optional[Any] = None,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """Write normalized inclusive-versus-stitched validation plots."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import uproot  # type: ignore

    if histogram_specs is None:
        histogram_specs = _stitch_validation_histogram_specs({})
    specs_by_name = {spec["name"]: spec for spec in histogram_specs}

    aggregate: dict[
        tuple[str, str],
        dict[
            str,
            tuple[Any, Any, Optional[Any], Optional[Any], Any],
        ],
    ] = {}
    plot_process_families: dict[tuple[str, str], str] = {}
    normalization_records: dict[str, dict[str, Any]] = {}
    for sample_name, paths in meta_files.items():
        group_name = sample_groups[sample_name]
        group_report = cross_section_groups[group_name]
        raw_inclusive_sample = group_report["inclusive_sample"]
        try:
            inclusive_samples = _shared_normalize_sample_names(
                raw_inclusive_sample, "inclusive_sample"
            )
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid inclusive samples for stitching group '{group_name}': {exc}"
            ) from exc
        if any(sample_name not in meta_files for sample_name in inclusive_samples):
            raise RuntimeError(
                f"Inclusive sample(s) {inclusive_samples} for stitching group "
                f"'{group_name}' have no application metadata output."
            )
        excluded_bins = _stitch_validation_excluded_bins(
            sample_name,
            group_name,
            group_report,
            submit_config,
            len(group_report["binned_cross_sections_pb"]),
            validation_adapter,
        )
        scale, sample_cross_section, signed_sum = _stitch_validation_sample_scale(
            sample_name,
            paths,
            group_report,
            excluded_bins,
            _stitch_validation_sample_factors(weights, group_name, sample_name),
        )
        configured_weight_scale = _stitch_validation_configured_weight_scale(
            sample_metadata.get(sample_name)
        )
        scale /= configured_weight_scale
        process_family = group_name
        if validation_adapter is not None:
            resolver = getattr(validation_adapter, "process_family", None)
            if callable(resolver):
                process_family = str(resolver(group_name) or "all")
        process_label = _stitch_validation_process_label(
            group_name,
            validation_adapter,
        )
        year_set = _stitch_validation_year_set(
            sample_metadata.get(sample_name),
            str(group_report.get("era", "")),
            validation_adapter,
        )
        plot_key = (process_label, year_set)
        plot_process_families.setdefault(plot_key, process_family)
        plot_key_name = f"{process_label}_{year_set}"
        normalization_record = normalization_records.setdefault(
            plot_key_name,
            {
                "process": process_label,
                "year_set": year_set,
                "samples": {},
            },
        )
        normalization_record["samples"][sample_name] = {
            "stitch_group": group_name,
            "cross_section_pb": sample_cross_section,
            "signed_event_sum": signed_sum,
            "scale": scale,
            "configured_weight_scale": configured_weight_scale,
            "is_inclusive_sample": sample_name in inclusive_samples,
            "excluded_stitch_bins": sorted(excluded_bins),
        }
        grouped = aggregate.setdefault(plot_key, {})
        for meta_file in paths:
            with cast(Any, uproot.open(meta_file)) as root_file:
                for histogram_spec in histogram_specs:
                    variable = histogram_spec["name"]
                    inclusive_key = _find_stitch_histogram_key(
                        root_file, variable
                    )
                    stitched_key = _find_stitch_histogram_key(
                        root_file, histogram_spec["stitched_name"]
                    )
                    inclusive, edges, inclusive_variance = (
                        _stitch_validation_histogram_data(root_file[inclusive_key])
                    )
                    stitched, stitched_edges, stitched_variance = (
                        _stitch_validation_histogram_data(root_file[stitched_key])
                    )
                    inclusive *= scale
                    stitched *= scale
                    if inclusive_variance is not None:
                        inclusive_variance *= scale * scale
                    if stitched_variance is not None:
                        stitched_variance *= scale * scale
                    if (
                        inclusive.shape != stitched.shape
                        or not np.array_equal(edges, stitched_edges)
                    ):
                        raise RuntimeError(
                            f"Validation histograms for '{variable}' in "
                            f"'{meta_file}' have incompatible binning."
                        )
                    inclusive_contribution = (
                        inclusive
                        if sample_name in inclusive_samples
                        else np.zeros_like(inclusive)
                    )
                    inclusive_variance_contribution = (
                        inclusive_variance
                        if sample_name in inclusive_samples
                        else (
                            None
                            if inclusive_variance is None
                            else np.zeros_like(inclusive_variance)
                        )
                    )
                    if variable not in grouped:
                        grouped[variable] = (
                            inclusive_contribution,
                            stitched,
                            inclusive_variance_contribution,
                            stitched_variance,
                            edges,
                        )
                    else:
                        (
                            previous_inclusive,
                            previous_stitched,
                            previous_inclusive_variance,
                            previous_stitched_variance,
                            previous_edges,
                        ) = grouped[variable]
                        if not np.array_equal(previous_edges, edges):
                            raise RuntimeError(
                                f"Validation histograms for '{variable}' do not "
                                "share binning across application files."
                            )
                        grouped[variable] = (
                            previous_inclusive + inclusive_contribution,
                            previous_stitched + stitched,
                            (
                                None
                                if previous_inclusive_variance is None
                                or inclusive_variance_contribution is None
                                else previous_inclusive_variance
                                + inclusive_variance_contribution
                            ),
                            (
                                None
                                if previous_stitched_variance is None
                                or stitched_variance is None
                                else previous_stitched_variance + stitched_variance
                            ),
                            previous_edges,
                        )

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    plot_paths: list[str] = []
    for (process_label, year_set), variables in sorted(aggregate.items()):
        for (
            variable,
            (
                inclusive,
                stitched,
                inclusive_variance,
                stitched_variance,
                edges,
            ),
        ) in variables.items():
            histogram_spec = specs_by_name[variable]
            variable_boundaries = histogram_spec["boundaries"]
            if variable_boundaries is None:
                variable_boundaries = _stitch_validation_variable_boundaries(
                    variable,
                    plot_process_families[(process_label, year_set)],
                    boundaries,
                    process_boundaries,
                    validation_adapter,
                )
            xlim = histogram_spec["xlim"]
            x_ticks = None
            x_ticklabels = None
            if variable == "LHE_NpNLO":
                centers = edges[:-1] + 0.5 * np.diff(edges)
                x_ticks = centers
                x_ticklabels = [str(int(round(edge))) for edge in edges[:-1]]
                if xlim is None:
                    xlim = [float(edges[0]), float(edges[-1])]
            elif variable == "LHE_Vpt" and xlim is None:
                xlim = [float(edges[0]), 1000.0]
            safe_process = re.sub(r"[^A-Za-z0-9]+", "_", process_label).strip("_")
            safe_year_set = re.sub(r"[^A-Za-z0-9]+", "_", year_set).strip("_")
            plot_path = os.path.join(
                output_dir,
                f"stitch_{safe_process}_{safe_year_set}_{variable}_comparison.png",
            )
            _plot_ratio_comparison(
                inclusive,
                stitched,
                edges,
                plot_path,
                title=f"Stitching validation: {process_label} {year_set} {variable}",
                xlabel=histogram_spec["label"],
                boundaries=variable_boundaries,
                xlim=xlim,
                log_y=histogram_spec["log_y"],
                ratio_range=histogram_spec["ratio_range"],
                inclusive_variance=inclusive_variance,
                stitched_variance=stitched_variance,
                x_ticks=x_ticks,
                x_ticklabels=x_ticklabels,
            )
            plot_paths.append(plot_path)

    return plot_paths, normalization_records


def _validate_stitch_meta_file(
    meta_file: str,
    sample_name: str,
    require_stitched_histograms: bool = True,
    stitched_histograms: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Validate one application metadata file and return its summary."""
    import math

    try:
        import uproot  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "StitchingValidationTask requires uproot to inspect ROOT outputs."
        ) from exc

    required_names = [
        f"counter_intWeightSignSum_{sample_name}",
        f"counter_weightSignSum_{sample_name}",
    ]
    if require_stitched_histograms:
        required_names.extend(
            stitched_histograms
            or ("LHE_Vpt_stitched", "LHE_NpNLO_stitched")
        )

    with uproot.open(meta_file) as root_file:
        keys = list(root_file.keys(recursive=True))
        matches: dict[str, str] = {}
        for key in keys:
            basename = _root_key_basename(key)
            if basename in required_names and basename not in matches:
                matches[basename] = str(key).split(";", 1)[0]

        missing = [name for name in required_names if name not in matches]
        if missing:
            raise RuntimeError(
                f"Stitching output '{meta_file}' is missing histogram(s): "
                + ", ".join(missing)
            )

        histogram_bins: dict[str, int] = {}
        for name, key in matches.items():
            try:
                values, _ = root_file[key].to_numpy(flow=False)
                if any(not math.isfinite(float(value)) for value in values):
                    raise RuntimeError(
                        f"Stitching output '{meta_file}' contains non-finite values "
                        f"in histogram '{name}'."
                    )
                histogram_bins[name] = len(values)
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(
                    f"Could not read histogram '{name}' from '{meta_file}': {exc}"
                ) from exc

    return {
        "meta_file": meta_file,
        "sample": sample_name,
        "histograms": sorted(matches),
        "histogram_bins": histogram_bins,
    }


class StitchingValidationTask(law.Task):
    """Validate stitched application outputs across every dataset job.

    The task is intentionally separate from :class:`StitchingApplyTask` so it
    can be rerun cheaply after a batch application pass.  It checks all
    ``meta.root`` files below ``skimRun_<name>/outputs/``, including files
    created when a file source split one dataset into multiple jobs.
    """

    task_namespace = ""

    name = luigi.Parameter(
        description="Application run name to validate (the run directory is skimRun_<name>)."
    )
    dataset_manifest = luigi.Parameter(
        description="Dataset manifest used by the application pass."
    )
    stitch_config = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional stitch YAML config. When supplied, every configured group "
            "must be present in the derived correction JSON."
        ),
    )
    stitch_weights = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional correctionlib JSON path. Defaults to "
            "stitchWeights_<name>/stitch_weights.json."
        ),
    )
    stitch_cross_sections = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional stitching cross-section report. Defaults to "
            "stitchWeights_<name>/stitch_cross_sections.json."
        ),
    )
    submit_config = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional submit config passed to the validation adapter."
        ),
    )
    validation_adapter = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Optional Python module providing analysis-owned validation labels, "
            "boundaries, and exclusion policy. Defaults to the adapter named "
            "under validation.adapter in the stitch YAML."
        ),
    )
    require_stitched_histograms = luigi.BoolParameter(
        default=True,
        significant=False,
        description="Require stitched validation histograms in every application metadata file.",
    )

    @property
    def _output_dir(self) -> str:
        return os.path.join(WORKSPACE, f"stitchValidation_{self.name}")

    @property
    def _weights_path(self) -> str:
        raw_path = self.stitch_weights or os.path.join(
            WORKSPACE, f"stitchWeights_{self.name}", "stitch_weights.json"
        )
        return (
            raw_path
            if os.path.isabs(raw_path)
            else os.path.abspath(os.path.join(WORKSPACE, raw_path))
        )

    @property
    def _cross_sections_path(self) -> str:
        raw_path = self.stitch_cross_sections or os.path.join(
            WORKSPACE,
            f"stitchWeights_{self.name}",
            "stitch_cross_sections.json",
        )
        return (
            raw_path
            if os.path.isabs(raw_path)
            else os.path.abspath(os.path.join(WORKSPACE, raw_path))
        )

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._output_dir, "stitch_validation.json")
        )

    def complete(self):
        if not self.output().exists():
            return False
        if not self.require_stitched_histograms:
            return True

        try:
            with open(self.output().path) as fh:
                report = json.load(fh)
            if not isinstance(report, dict):
                return False
            plot_paths = report.get("plots")
            if not isinstance(plot_paths, list) or any(
                not isinstance(path, str) or not os.path.isfile(path)
                for path in plot_paths
            ):
                return False

            stitch_config_data: dict[str, Any] = {}
            if self.stitch_config:
                with open(os.path.abspath(self.stitch_config)) as fh:
                    loaded_config = yaml.safe_load(fh) or {}
                if not isinstance(loaded_config, dict):
                    return False
                stitch_config_data = loaded_config
            histogram_specs = _stitch_validation_histogram_specs(stitch_config_data)
            return all(
                any(
                    path.endswith(f"_{spec['name']}_comparison.png")
                    for path in plot_paths
                )
                for spec in histogram_specs
            )
        except (OSError, TypeError, ValueError, RuntimeError):
            return False

    def run(self):
        meta_files = _stitch_output_meta_files(self.name, self.dataset_manifest)
        manifest = DatasetManifest.load(self.dataset_manifest)
        weights_path = self._weights_path
        if not os.path.isfile(weights_path):
            raise RuntimeError(
                f"Stitching correction JSON not found: '{weights_path}'"
            )

        expected_groups: set[str] = set()
        stitch_config_data: dict[str, Any] = {}
        if self.stitch_config:
            config_path = os.path.abspath(self.stitch_config)
            if not os.path.isfile(config_path):
                raise RuntimeError(f"Stitch configuration file not found: '{config_path}'")
            with open(config_path) as fh:
                stitch_config_data = yaml.safe_load(fh) or {}
            groups = stitch_config_data.get("groups")
            if not isinstance(groups, dict):
                raise RuntimeError(
                    f"Stitch config '{config_path}' must contain a top-level 'groups' mapping."
                )
            expected_groups = {str(group_name) for group_name in groups}

        adapter_name = str(self.validation_adapter).strip()
        if not adapter_name:
            validation_config = stitch_config_data.get("validation", {})
            if isinstance(validation_config, dict):
                adapter_name = str(validation_config.get("adapter", "")).strip()
        validation_adapter = _load_stitch_validation_adapter(adapter_name)

        with open(weights_path) as fh:
            weights = json.load(fh)
        if not isinstance(weights, dict) or not isinstance(
            weights.get("corrections"), list
        ):
            raise RuntimeError(
                f"Stitching correction JSON '{weights_path}' has no corrections list."
            )
        correction_names = {
            str(correction.get("name"))
            for correction in weights.get("corrections", [])
            if isinstance(correction, dict) and correction.get("name")
        }
        missing_groups = sorted(expected_groups - correction_names)
        if missing_groups:
            raise RuntimeError(
                f"Stitching correction JSON '{weights_path}' is missing group(s): "
                + ", ".join(missing_groups)
            )

        validation_histograms = _stitch_validation_histogram_specs(
            stitch_config_data
        )

        cross_section_groups: dict[str, dict[str, Any]] = {}
        sample_groups: dict[str, str] = {}
        sample_metadata = {entry.name: entry for entry in manifest.datasets}
        if self.require_stitched_histograms:
            cross_sections_path = self._cross_sections_path
            cross_section_groups = _load_stitch_cross_section_report(
                cross_sections_path
            )
            sample_groups = _stitch_validation_sample_groups(
                list(meta_files),
                manifest,
                self.stitch_config,
                weights,
                set(cross_section_groups),
            )

        validation_records: list[dict[str, Any]] = []
        for sample_name, paths in sorted(meta_files.items()):
            for meta_file in paths:
                validation_records.append(
                    _validate_stitch_meta_file(
                        meta_file,
                        sample_name,
                        require_stitched_histograms=self.require_stitched_histograms,
                        stitched_histograms=(
                            [
                                spec["stitched_name"]
                                for spec in validation_histograms
                            ]
                            if self.require_stitched_histograms
                            else None
                        ),
                    )
                )

        Path(self._output_dir).mkdir(parents=True, exist_ok=True)
        plot_boundaries = _stitch_validation_boundaries(
            stitch_config_data,
            self.submit_config,
            validation_adapter,
        )
        plot_process_boundaries = _stitch_validation_boundaries_by_process(
            stitch_config_data,
            self.submit_config,
            validation_adapter,
        )
        plot_paths: list[str] = []
        plot_normalization: dict[str, dict[str, Any]] = {}
        if self.require_stitched_histograms:
            plot_paths, plot_normalization = _write_stitch_validation_plots(
                meta_files,
                self._output_dir,
                plot_boundaries,
                sample_metadata,
                sample_groups,
                cross_section_groups,
                plot_process_boundaries,
                weights,
                histogram_specs=validation_histograms,
                submit_config=self.submit_config,
                validation_adapter=validation_adapter,
            )
        with open(self.output().path, "w") as fh:
            json.dump(
                {
                    "application_run": self.name,
                    "weights": weights_path,
                    "samples": len(meta_files),
                    "meta_files": len(validation_records),
                    "groups": sorted(expected_groups or correction_names),
                    "plots": plot_paths,
                    "plot_boundaries": plot_boundaries,
                    "plot_boundaries_by_process": plot_process_boundaries,
                    "plot_normalization": plot_normalization,
                    "files": validation_records,
                },
                fh,
                indent=2,
            )

        self.publish_message(
            f"Validated stitching outputs for {len(meta_files)} sample(s) and "
            f"{len(validation_records)} metadata file(s)."
        )


# ===========================================================================
# Task 6 – CollectHistogramsTask
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
