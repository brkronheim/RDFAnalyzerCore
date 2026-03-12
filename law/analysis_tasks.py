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
from pathlib import Path
from typing import Optional

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
from submission_backend import read_config  # noqa: E402
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


# ===========================================================================
# Helper: write a job submit_config.txt from a template + dataset overrides
# ===========================================================================

def _write_job_config(
    job_dir: str,
    template_config_path: str,
    dataset: DatasetEntry,
    output_dir: str,
    extra_overrides: Optional[dict[str, str]] = None,
) -> None:
    """Create a ``submit_config.txt`` inside *job_dir* for *dataset*.

    Reads the template at *template_config_path*, overrides the dataset-
    specific keys (``fileList``, ``saveFile``, ``metaFile``,
    ``saveDirectory``, ``sample``, ``type``), copies any referenced
    auxiliary config files (``*.txt``, ``*.yaml``) from the template
    directory into *job_dir*, and writes the final config.

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
        job_config["fileList"] = ",".join(dataset.files)
    elif dataset.das:
        job_config["fileList"] = dataset.das

    # ---- output paths -------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    job_config["saveFile"] = os.path.join(output_dir, "skim.root")
    job_config["metaFile"] = os.path.join(output_dir, "meta.root")
    job_config["saveDirectory"] = output_dir

    # ---- sample identity ----------------------------------------------------
    job_config["sample"] = dataset.name
    if dataset.dtype:
        job_config["type"] = dataset.dtype

    # ---- caller-supplied overrides ------------------------------------------
    if extra_overrides:
        job_config.update(extra_overrides)

    # ---- copy referenced auxiliary files ------------------------------------
    for key, val in list(template_config.items()):
        if key.startswith("__"):
            continue
        if isinstance(val, str) and (
            val.endswith(".txt") or val.endswith(".yaml") or val.endswith(".yml")
        ):
            src = os.path.join(template_dir, val)
            if os.path.isfile(src):
                dst = os.path.join(job_dir, os.path.basename(val))
                if not os.path.exists(dst):
                    shutil.copy2(src, dst)
                job_config[key] = os.path.basename(val)

    # ---- write the final config file ----------------------------------------
    config_path = os.path.join(job_dir, "submit_config.txt")
    with open(config_path, "w") as fh:
        for key, val in job_config.items():
            if not key.startswith("__"):
                fh.write(f"{key}={val}\n")


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
    file_source = luigi.Parameter(
        default="",
        description=(
            "Source for input file lists.  One of: '' (use dataset manifest directly), "
            "'nano' (use GetNANOFileList output), 'opendata' (use GetOpenDataFileList output), "
            "'xrdfs' (use GetXRDFSFileList output).  "
            "When set, the task chains from the corresponding file list task and "
            "splits files into per-job chunks using --files-per-job."
        ),
    )
    file_source_name = luigi.Parameter(
        default="",
        description=(
            "Name of the file list task run to chain from (required when --file-source is set)."
        ),
    )
    files_per_job = luigi.IntParameter(
        default=50,
        description=(
            "Maximum number of files per job when --file-source is set (default: 50). "
            "Ignored when --file-source is empty (direct manifest execution)."
        ),
    )
    partition = luigi.Parameter(
        default="file_group",
        description=(
            "Partitioning mode when --file-source is set: "
            "'file_group' (group by count), 'file' (one file per job), "
            "'entry_range' (split by TTree entries, requires uproot). "
            "Ignored when --file-source is empty."
        ),
    )
    entries_per_job = luigi.IntParameter(
        default=100_000,
        description=(
            "Maximum TTree entries per job in 'entry_range' mode (default: 100000). "
            "Requires uproot. Ignored for other partition modes."
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
    max_runtime = luigi.IntParameter(
        default=3600,
        significant=False,
        description="Maximum HTCondor job runtime in seconds (default: 3600).",
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
# Task 1 – SkimTask
# ===========================================================================

class SkimTask(AnalysisMixin, law.LocalWorkflow, HTCondorWorkflow, DaskWorkflow):
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

    # Parameters that vary per-branch must not be forwarded to the workflow
    # task when dispatching via HTCondor or Dask.
    exclude_params_branch = (
        law.LocalWorkflow.exclude_params_branch
        | HTCondorWorkflow.exclude_params_branch
        | DaskWorkflow.exclude_params_branch
        | {"dask_scheduler", "dask_workers", "max_runtime"}
    )

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    # ------------------------------------------------------------------
    # Branch map: two modes
    #   1. file_source == "" → one branch per dataset entry in the manifest
    #   2. file_source set  → read file-list JSONs, create one branch per
    #                         (dataset, partition) combination
    # ------------------------------------------------------------------

    def _file_list_dir(self) -> str:
        """Return the directory containing per-dataset file list JSONs."""
        prefix_map = {
            "nano": "nanoFileList",
            "opendata": "openDataFileList",
            "xrdfs": "xrdfsFileList",
        }
        prefix = prefix_map.get(self.file_source, "")
        if not prefix:
            return ""
        return os.path.join(WORKSPACE, f"{prefix}_{self.file_source_name}")

    def create_branch_map(self):
        if not self.file_source:
            # Original behaviour: one branch per dataset
            manifest = DatasetManifest.load(self.dataset_manifest)
            return {i: entry for i, entry in enumerate(manifest.datasets)}

        # File-source mode: read JSONs, partition files, one branch per chunk
        if not self.file_source_name:
            raise ValueError(
                "--file-source-name must be set when --file-source is provided."
            )
        fl_dir = self._file_list_dir()
        if not os.path.isdir(fl_dir):
            raise RuntimeError(
                f"File list directory not found: {fl_dir!r}. "
                f"Run the corresponding file list task first."
            )

        branches: dict = {}
        branch_idx = 0
        for json_path in sorted(Path(fl_dir).glob("*.json")):
            with open(json_path) as fh:
                payload = json.load(fh)
            sample_name = payload.get("sample", json_path.stem)
            files = payload.get("files", [])
            if not files:
                continue
            partitions = _make_partitions(
                urls=files,
                mode=self.partition,
                files_per_job=self.files_per_job,
                entries_per_job=self.entries_per_job,
            )
            for part in partitions:
                branches[branch_idx] = {
                    "dataset_name": sample_name,
                    "files": part["files"],
                    "first_entry": part["first_entry"],
                    "last_entry": part["last_entry"],
                }
                branch_idx += 1
        return branches

    def output(self):
        branch_data = self.branch_data
        if isinstance(branch_data, DatasetEntry):
            # Standard mode
            return law.LocalFileTarget(
                os.path.join(self._job_outputs_dir, f"{branch_data.name}.done")
            )
        # File-source mode: include branch index to guarantee uniqueness
        dataset_name = branch_data["dataset_name"]
        return law.LocalFileTarget(
            os.path.join(
                self._job_outputs_dir, f"{dataset_name}_b{self.branch}.done"
            )
        )

    def complete(self):
        """Return True only when the `.done` marker and a valid cache sidecar exist.

        Extends the standard LAW output-existence check with a provenance-based
        cache validity check on the produced ``skim.root``.  If the cache
        sidecar is absent, schema-incompatible, or records stale provenance
        the ``.done`` marker is removed so that LAW will re-run this branch and
        regenerate the artifact.

        This override only applies at branch level (``self.is_branch() == True``).
        At the workflow level the standard :meth:`law.LocalWorkflow.complete`
        behaviour is preserved.

        In file-source mode the cache validity check is skipped because no
        single canonical ``skim.root`` path exists per dataset.
        """
        # Delegate non-branch (workflow-level) complete() to LAW.
        if not self.is_branch():
            return super().complete()

        # If the standard output doesn't exist the task is not complete.
        if not super().complete():
            return False

        # Skip cache check in file-source mode
        if self.file_source:
            return True

        # In standard manifest mode branch_data is always a DatasetEntry
        # (file-source mode already returned True above).
        dataset: DatasetEntry = self.branch_data
        skim_file = os.path.join(self._outputs_dir, dataset.name, "skim.root")

        current_prov = _build_task_provenance(
            submit_config_path=self.submit_config,
            dataset_manifest_path=self.dataset_manifest,
        )
        status = check_cache_validity(
            skim_file, current_provenance=current_prov, strict=True
        )

        if status == ArtifactResolutionStatus.COMPATIBLE:
            return True

        # Cache is stale or incompatible – remove the .done marker so LAW
        # will schedule a fresh run of this branch.
        done_path = self.output().path
        if os.path.exists(done_path):
            os.remove(done_path)
        self.publish_message(
            f"[SkimTask] Cache invalidated for '{dataset.name}' "
            f"(status={status.value}): the .done marker has been removed "
            "and the artifact will be regenerated."
        )
        return False

    def run(self):
        branch_data = self.branch_data

        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Analysis executable not found: {exe_path!r}. "
                "Build the analysis before running SkimTask."
            )

        if isinstance(branch_data, DatasetEntry):
            # ---- standard manifest mode ------------------------------------
            dataset = branch_data
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
            rec.save(perf_path_for(self.output().path))

            skim_file = os.path.join(out_dir, "skim.root")
            if os.path.isfile(skim_file):
                manifest = _build_skim_manifest(
                    dataset=dataset,
                    submit_config_path=self.submit_config,
                    dataset_manifest_path=self.dataset_manifest,
                    skim_output_file=skim_file,
                )
                write_cache_sidecar(skim_file, manifest)
                self.publish_message(
                    f"[SkimTask] Cache sidecar written for '{dataset.name}': "
                    f"{skim_file}"
                )

            self.publish_message(f"Skim done for '{dataset.name}': {result}")
            with self.output().open("w") as fh:
                fh.write(f"status=done\ndataset={dataset.name}\n{result}\n")

        else:
            # ---- file-source mode (branch_data is a dict) ------------------
            dataset_name = branch_data["dataset_name"]
            files_str = branch_data["files"]
            first_entry = branch_data.get("first_entry", 0)
            last_entry = branch_data.get("last_entry", 0)

            # Build a transient DatasetEntry from the file-source data
            transient_dataset = DatasetEntry(
                name=dataset_name,
                files=files_str.split(",") if files_str else [],
            )

            extra_overrides: dict = {}
            if first_entry or last_entry:
                extra_overrides["firstEntry"] = str(first_entry)
                extra_overrides["lastEntry"] = str(last_entry)

            # Use branch index in job dir name to avoid collisions
            job_dir = os.path.join(
                self._jobs_dir, dataset_name, f"branch_{self.branch}"
            )
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            out_dir = os.path.join(
                self._outputs_dir, dataset_name, f"branch_{self.branch}"
            )
            _write_job_config(
                job_dir=job_dir,
                template_config_path=self.submit_config,
                dataset=transient_dataset,
                output_dir=out_dir,
                extra_overrides=extra_overrides,
            )

            task_label = f"SkimTask[{dataset_name}/b{self.branch}]"
            with PerformanceRecorder(task_label) as rec:
                result = _run_analysis_job(exe_path=exe_path, job_dir=job_dir)

            Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
            rec.save(perf_path_for(self.output().path))

            self.publish_message(
                f"Skim done for '{dataset_name}' branch {self.branch}: {result}"
            )
            with self.output().open("w") as fh:
                fh.write(
                    f"status=done\ndataset={dataset_name}\n"
                    f"branch={self.branch}\n{result}\n"
                )

    # ------------------------------------------------------------------
    # HTCondor workflow configuration
    # ------------------------------------------------------------------

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(
            os.path.join(self._run_dir, "htcondor_logs")
        )

    def htcondor_job_config(self, config, job_num, branches):
        config.render_variables["analysis_id"] = self.name
        config.custom_content.extend([
            ("+RequestMemory", str(2000)),
            ("+MaxRuntime", str(self.max_runtime)),
            ("request_cpus", "1"),
            ("request_disk", "20000"),
        ])
        return config

    # ------------------------------------------------------------------
    # Dask workflow configuration
    # ------------------------------------------------------------------

    def get_dask_work(self, branch_num: int, branch_data) -> tuple:
        """Return the (callable, args, kwargs) triple for Dask job submission."""
        exe_path = os.path.abspath(self.exe)
        if isinstance(branch_data, DatasetEntry):
            dataset_name = branch_data.name
            job_dir = os.path.join(self._jobs_dir, dataset_name)
        else:
            dataset_name = branch_data["dataset_name"]
            job_dir = os.path.join(
                self._jobs_dir, dataset_name, f"branch_{branch_num}"
            )
        return (
            _run_analysis_job,
            [exe_path, job_dir],
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
