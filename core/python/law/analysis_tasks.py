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
        --submit-config analyses/myAnalysis/cfg/skim_config.yaml \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --name mySkimRun

    # Fill histograms (requires SkimTask outputs when --skim-name is set)
    law run HistFillTask \\
        --exe build/analyses/myAnalysis/myanalysis \\
        --submit-config analyses/myAnalysis/cfg/hist_config.yaml \\
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
matches the ``sample`` / ``type`` key in the analysis submit_config.yaml.
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
import hashlib
import fcntl
import stat
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
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

from rdfproduction.performance import PerformanceRecorder, perf_path_for  # noqa: E402
from rdfproduction.condor_submission import submit_skim_batches  # noqa: E402
from optional_extensions import render_environment_setup  # noqa: E402
from extract_nd_histograms import extract_nd_histograms  # noqa: E402
from dataset_manifest import (  # noqa: E402
    DatasetManifest,
    DatasetEntry,
)
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
    condor_config_return_remap,
    ensure_condor_machine_exclusion,
    ensure_xrootd_redirector,
    get_copy_file_list,
    read_config,
    write_config,
    write_submit_files,
)
import io_policy as _io_policy
from xrd_site_rankings import update_after_run as _update_xrd_site_rankings
from run_bundle import (find_bundle, verify_config_bundle, rewrite_references,
                        file_digest, execution_identity, record_execution)
from workflow_executors import DaskWorkflow, HTCondorWorkflow, _run_analysis_job  # noqa: E402
from rdfproduction.partitioning import _make_partitions  # noqa: E402
from job_sizing import EntryCache, event_budget, load_profile  # noqa: E402
from job_sizing import (profile_matches, select_sizing, sizing_context,
                        update_sizing_after_run as _update_job_sizing)  # noqa: E402
from job_sizing import atomic_json, condor_event_ads, collect_log, fit_pilot, seed_pilot_sizing, pilot_inputs  # noqa: E402
from rdfproduction.partitioning import _query_tree_entries  # noqa: E402
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

# Shared defaults/types for ordinary and source-prefixed sizing parameters.
# Construct a distinct Luigi Parameter each time to retain descriptor ownership.
_JOB_SIZING_PARAMETERS = {
    "sizing_pilots": (luigi.BoolParameter, False),
    "sizing_target_seconds": (luigi.IntParameter, 3600),
    "sizing_pilot_target_seconds": (luigi.IntParameter, 1800),
    "sizing_pilot_parallel": (luigi.IntParameter, 4),
    "sizing_pilot_prefix_entries": (luigi.IntParameter, 10000),
    "sizing_pilot_safety_factor": (luigi.FloatParameter, 1.5),
}


def _job_sizing_parameter(name, **options):
    parameter, default = _JOB_SIZING_PARAMETERS[name]
    return parameter(default=default, **options)


def _job_sizing_kwargs(task, *, input_prefix="", output_prefix=""):
    """Adapt the same six operational fields across the source-task boundary."""
    return {output_prefix + name: getattr(task, input_prefix + name)
            for name in _JOB_SIZING_PARAMETERS}


# Shared production operations also serve the standalone campaign controller.
from native_config_io import job_config_path as prepared_config_path
from production_jobs import (
    AnalysisPaths, SkimPaths, PreparationOperations, PilotOperations, SmokeOperations,
    SubmissionOperations, MonitorOperations, BranchOperations,
    _stream_analysis_output, _remove_mc_generator_config_for_data, _find_dataset_entry,
    _manifest_lumi_for_dataset, _resolve_analysis_config_path, _analysis_config_payload_sources,
    _copy_analysis_config_files, _analysis_aux_dir, _copy_analysis_aux_dir, _bundle_analysis_aux_dir,
    _bundle_analysis_config_files, _symlink_or_copy_path, _staged_copy_is_current,
    _local_shared_libs_with_signature, _ensure_local_analysis_runtime_layout,
    _shared_inputs_archive_name, _validate_staged_skim_executable, _validate_x509_proxy,
    _validate_staged_skim_proxy, _build_shared_inputs_archive, _normalize_shared_config_references,
    _resolve_runtime_transfer_path, _runtime_loose_cfg_files, _copy_runtime_loose_cfg_files,
    _materialize_runtime_staged_inputs, _skim_shared_loose_input_files, _shared_archive_top_level_files,
    _copy_job_submission_files, _copy_tree_contents, _extract_tar_archive, _python_environment_setup,
    _materialize_file_source_runtime_job, _run_prepared_skim_job, _rewrite_submit_transfer_files,
    _skim_shared_input_files, _output_files_exist_batch, _output_file_info_batch, _root_file_tree_error,
    _config_enables_skim, _skim_job_output_errors, _run_skim_job_with_output_contract,
    _safe_dataset_submission_name, _dataset_submission_dir, _group_prepared_skim_jobs_by_dataset,
    _discover_jobs_in_submission_dir, _skim_job_file_list, _split_skim_file_list, _split_skim_partition,
    _append_output_part_suffix, _prepare_split_skim_job, _condor_job_elapsed_runtime,
    _condor_job_exceeded_runtime, _submit_single_skim_job, _rewrite_staged_analysis_config,
    _build_task_provenance, _build_skim_manifest, _read_histogram_config_entries,
    _read_root_histogram_metadata, _read_root_histogram_names, _histogram_axis_labels_from_entries,
    _infer_region_definitions, _initial_sample_metadata, _augment_sample_metadata_with_axis_labels,
    _matches_manifest_selector, _resolve_manifest_samples, _resolve_manifest_fit_metadata,
    _make_meta_output_name, _build_histfill_manifest, _dataset_job_metadata,
    _resolve_output_destination_base, _rewrite_job_output_destinations, _stitch_job_overrides,
    _write_job_config, _extract_lfn_for_redirect, _select_test_input_url, _write_config_file,
    _plan_file_source_jobs, _MC_GENERATOR_CONFIG_KEYS,
)

class AnalysisMixin(AnalysisPaths):
    """Parameters shared by :class:`SkimTask` and :class:`HistFillTask`."""

    submit_config = luigi.Parameter(
        description=(
            "Path to the submit_config.yaml template file for this analysis pass. "
            "Dataset-specific keys (fileList, saveFile, metaFile, sample, type) "
            "are filled in automatically from the dataset manifest; all other "
            "keys are forwarded verbatim to the job."
        ),
    )

    exe = luigi.Parameter(
        description=(
            "Path to the compiled analysis executable. "
            "It is invoked as ``<exe> submit_config.yaml`` inside each job directory."
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

    @property
    def _workspace(self):
        return WORKSPACE


class SkimMixin(SkimPaths):
    """Extra parameters for :class:`PrepareSkimJobs` and :class:`SkimTask`.

    Adds file-discovery chaining, per-job file partitioning, worker-environment
    staging, and multi-executor (local / HTCondor / Dask) control on top of
    the base :class:`AnalysisMixin` parameters.
    """

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

    rucio_group_size_gb = luigi.FloatParameter(
        default=10.0,
        description=(
            "Rucio file-group size threshold in GB (default: 10)."
        ),
    )

    partition = luigi.Parameter(
        default="auto",
        description=(
            "Partitioning mode for splitting file lists into per-job chunks: "
            "'auto' (default: learned event budgets, otherwise file grouping), "
            "'file_group' (group by file count), "
            "'file' (one file per job), "
            "'entry_range' (split every file by entries), or 'events' "
            "(pack whole files by entries, splitting oversized files)."
        ),
    )

    entries_per_job = luigi.IntParameter(
        default=100_000,
        description=(
            "Maximum TTree entries per job in 'entry_range'/'events' modes "
            "(default: 100000); overridden by a matching partition profile."
        ),
    )

    partition_profile = luigi.Parameter(
        default="", description="JSON/YAML sizing profile; empty uses matching learned history, 'none' disables lookup.",
    )

    input_metadata_cache = luigi.Parameter(
        default="", description="Optional entry-cache JSON seed; copied into the new run before planning.",
    )

    entry_range_mode = luigi.ChoiceParameter(
        default="filter", choices=("filter", "source"),
        description="Entry-range execution: filter, or direct source seeking on ROOT 6.40.x or 6.42.00-rc1 (threads=1).",
    )

    sizing_pilots = _job_sizing_parameter(
        'sizing_pilots',
        description='Before main preparation, run one-file Condor pilots for datasets without sizing evidence.',
    )

    sizing_target_seconds = _job_sizing_parameter(
        'sizing_target_seconds',
        description='Estimated work per job from learned production rates, in seconds (default: 3600).',
    )

    sizing_pilot_target_seconds = _job_sizing_parameter(
        'sizing_pilot_target_seconds',
        description='Estimated work per job from provisional pilot rates, in seconds (default: 1800).',
    )

    sizing_pilot_parallel = _job_sizing_parameter(
        'sizing_pilot_parallel',
        description='Maximum simultaneous pilot jobs; submit/verify batches of this size.',
    )

    sizing_pilot_prefix_entries = _job_sizing_parameter(
        'sizing_pilot_prefix_entries',
        description='Short-prefix size paired with a full-file pilot to separate fixed overhead.',
    )

    sizing_pilot_safety_factor = _job_sizing_parameter(
        'sizing_pilot_safety_factor',
        description='Conservative multiplier for provisional pilot rates and fixed overhead (>=1).',
    )

    sizing_pilot_child = luigi.BoolParameter(default=False,
        description="Internal paired-measurement worker mode; disables pilot recursion and regular cost learning.")

    prepare_workers = luigi.IntParameter(
        default=4,
        significant=False,
        description=(
            "Maximum number of threads used to prepare independent per-job "
            "directories and configs (default: 4)."
        ),
    )

    output_validation = luigi.ChoiceParameter(
        default="integrity",
        choices=("exists", "integrity"),
        significant=False,
        description=(
            "Delegated HTCondor output checks: 'exists' uses batched xrdfs listings "
            "without opening ROOT files; 'integrity' also checks ROOT contents and "
            "MC counters (default)."
        ),
    )

    x509 = luigi.Parameter(
        default="",
        description=(
            "Path to the VOMS x509 proxy file.  Copied into "
            "``skimRun_{name}/shared_inputs/x509`` for use by HTCondor workers. "
            "The staged proxy must have at least 48 hours of validity remaining."
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
            "``core/python/law/setup_python_env.sh``.  Staged to every HTCondor worker "
            "node alongside the executable."
        ),
    )

    make_test_job = luigi.BoolParameter(
        default=True,
        description=(
            "If True (default), run one-file local preflight jobs for each available "
            "data and MC sample type before dispatching the full SkimTask workflow. "
            "This catches sample-specific runtime errors before batch submission. "
            "Set --no-make-test-job to skip the preflight and submit immediately."
        ),
    )

    stitch_mode = luigi.Parameter(
        default="",
        description=(
            "Optional stitching mode override written into each job config "
            "(for example 'none' or 'correctionlib')."
        ),
    )

    stitch_weights = luigi.Parameter(
        default="",
        description=(
            "Optional correctionlib stitching JSON staged in the shared worker "
            "archive and exposed as the 'stitchWeights' config value."
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
        default=7200,
        significant=False,
        description="Maximum HTCondor job runtime in seconds (default: 7200); independent of event-sizing targets.",
    )

    request_memory = luigi.IntParameter(
        default=2000,
        significant=False,
        description="HTCondor memory request per job in MB (default: 2000).",
    )

    def _prepare_skim_jobs_requirement(self):
        return PrepareSkimJobs.req(
            self,
            prepare_workers=self.prepare_workers,
            stitch_mode=self.stitch_mode,
            stitch_weights=self.stitch_weights,
        )


class PrepareSkimJobs(AnalysisMixin, SkimMixin, PreparationOperations, law.Task):
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
    writes a native ``submit_config.yaml`` and its per-job constant recipes per
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
        if self.sizing_pilots:
            return RunSkimSizingPilots.req(self)
        return self._discovery_requirement()

    def _discovery_requirement(self):
        """Declare LAW dependency on the file-list task when ``--file-source`` is set.

        For ``--file-source xrdfs`` the dependency on
        :class:`~xrdfs_tasks.GetXRDFSFileList` is declared automatically because
        both tasks share the same ``dataset_manifest`` parameter.

        For ``--file-source rucio``, the dataset manifest is passed directly
        to file discovery. For ``--file-source opendata``, the submit config
        supplies its ``sampleConfig`` payload.
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
                dataset_manifest=self.dataset_manifest,
                size=self.rucio_group_size_gb,
                files_per_job=self.files_per_job,
            )
        if self.file_source == "opendata":
            from opendata_tasks import GetOpenDataFileList  # noqa: PLC0415
            return GetOpenDataFileList.req(
                self,
                name=self._effective_file_source_name,
            )
        return None


    def output(self):
        return law.LocalFileTarget(str(self.preparation_path))

    def complete(self):
        if self.sizing_pilots and not RunSkimSizingPilots.req(self).complete():
            return False
        return self.preparation_complete()

    run = PreparationOperations.prepare


class RunSkimSizingPilots(AnalysisMixin, SkimMixin, PilotOperations, law.Task):
    """Calibrate missing datasets on Condor, in bounded one-file batches."""

    task_namespace = ""

    @property
    def _run_dir(self):
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def requires(self):
        return PrepareSkimJobs._discovery_requirement(self)

    def _pilot_monitor(self, batch):
        return MonitorSkimJobs.req(self, name=batch['name'], dataset_manifest=batch['manifest'],
            file_source='', file_source_name='', partition='file_group',
            partition_profile='none', input_metadata_cache='', make_test_job=False,
            output_directory=batch['output_directory'], sizing_pilots=False, sizing_pilot_child=True,
            max_retries=0, poll_interval=15)


    def output(self):
        return law.LocalFileTarget(str(self.pilot_result_path))

    complete = PilotOperations.pilots_complete

    def run(self):
        plan = self._prepare_plan()
        for batch in plan['batches']:
            monitor = self._pilot_monitor(batch)
            self.publish_message(f"Calibrating {len(batch['datasets'])} dataset(s) in {batch['name']}")
            # Dynamic dependencies release the LAW worker while the batch runs.
            # Yielding one batch at a time bounds both queued and running pilots.
            yield monitor
            if not monitor.complete():
                raise RuntimeError(f"Sizing pilot batch failed: {batch['name']}; bulk preparation stopped")
        self.collect_pilot_results(plan)


class RunSkimTestJob(AnalysisMixin, SkimMixin, SmokeOperations, law.Task):
    """Run one-file data and MC preflights before dispatching :class:`SkimTask`.

    This task runs the analysis executable once per available sample type,
    using one input file each for data and MC when both are present. It validates
    each exit code before expensive batch or multi-dataset dispatch.

    In **file-source mode** (``--file-source`` set) the test job directory is
    prepared by :class:`PrepareSkimJobs`, which is declared as a formal LAW
    prerequisite.

    In **standard manifest mode** (``--file-source`` empty) the task creates the
    test job directory itself using the first file from one data and one MC entry
    when both types are present in the manifest.

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
        return law.LocalFileTarget(str(self.smoke_path))

    complete = SmokeOperations.smoke_complete
    run = SmokeOperations.smoke


class BuildSkimSubmission(AnalysisMixin, SkimMixin, SubmissionOperations, law.Task):
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
        return {"manifest": law.LocalFileTarget(str(self.submission_manifest_path))}

    _run_impl = SubmissionOperations._build_submission_impl
    run = SubmissionOperations.build_submission


class SubmitSkimJobs(AnalysisMixin, SkimMixin, SubmissionOperations, law.Task):
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
        return law.LocalFileTarget(str(self.submitted_path))

    def run(self):
        self.submit(self.input()["build"]["manifest"].path)


class MonitorSkimJobs(AnalysisMixin, SkimMixin, MonitorOperations, law.Task):
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
        return law.LocalFileTarget(str(self.monitor_path))

    complete = MonitorOperations.monitoring_complete
    def run(self):
        with self.input().open("r") as submitted:
            self.monitor(json.load(submitted))


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

class SkimTask(AnalysisMixin, SkimMixin, BranchOperations, law.LocalWorkflow, SkimHTCondorWorkflow, DaskWorkflow):
    """
    Run the analysis executable locally to produce skimmed ROOT files.

    One branch is created per dataset entry in the ``--dataset-manifest``.
    Each branch sets up a dedicated job directory under
    ``skimRun_{name}/jobs/{dataset_name}/``, writes a dataset-specific
    ``submit_config.yaml`` derived from the ``--submit-config`` template, and
    runs the analysis executable.

    The analysis executable is invoked as::

        <exe> submit_config.yaml

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

    exclude_params_branch = (
        getattr(law.LocalWorkflow, "exclude_params_branch", set())
        | getattr(SkimHTCondorWorkflow, "exclude_params_branch", set())
        | getattr(DaskWorkflow, "exclude_params_branch", set())
        | {
            "dask_scheduler",
            "dask_workers",
            "max_runtime",
            "request_memory",
            "make_test_job",
            "prepare_workers",
        }
    )

    cache_branch_map_default = False

    reset_branch_map_before_run = True

    @property
    def _run_dir(self) -> str:
        return os.path.join(WORKSPACE, f"skimRun_{self.name}")

    def create_branch_map(self):
        return {
            i: entry
            for i, entry in enumerate(self._prepared_branch_entries())
        }

    def _uses_delegated_htcondor_workflow(self) -> bool:
        return self.is_workflow() and str(getattr(self, "effective_workflow", "")) == "htcondor"

    def htcondor_delegate_task(self):
        return MonitorSkimJobs.req(self)

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
        reqs = [self.as_workflow()._prepare_skim_jobs_requirement()]
        if self.make_test_job and not self.is_branch():
            reqs.append(RunSkimTestJob.req(self))
        if len(reqs) == 1:
            return reqs[0]
        return reqs

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


    def output(self):
        if self._uses_delegated_htcondor_workflow():
            return self.htcondor_delegate_task().output()
        return law.LocalFileTarget(str(self.branch_output_path()))

    def complete(self):
        if self._uses_delegated_htcondor_workflow():
            return self.htcondor_delegate_task().complete()
        if not self.is_branch():
            if not Path(self._run_dir, "prep_submission.json").is_file():
                return False
            return super().complete()
        return self.branch_complete()

    def run(self):
        if self._uses_delegated_htcondor_workflow():
            return
        return self.run_branch()



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
    stitch_mode = luigi.Parameter(
        default="",
        description="Stitching mode forwarded to the prerequisite SkimTask.",
    )
    stitch_weights = luigi.Parameter(
        default="",
        description="Stitching weights file forwarded to the prerequisite SkimTask.",
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
                stitch_mode=self.stitch_mode,
                stitch_weights=self.stitch_weights,
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
                stitch_mode=self.stitch_mode,
                stitch_weights=self.stitch_weights,
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
                submit_config_path=str(prepared_config_path(os.path.join(self._skim_run_dir, "jobs", dataset.name))) if os.path.isfile(str(prepared_config_path(os.path.join(self._skim_run_dir, "jobs", dataset.name)))) else self.submit_config,
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
                stream_output=_stream_analysis_output(),
                worker_number=self.branch,
            )

        Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
        rec.save(perf_path_for(self.output().path))

        manifest = _build_histfill_manifest(
            dataset=dataset,
            submit_config_path=self.submit_config,
            dataset_manifest_path=self.dataset_manifest,
            job_config_path=str(prepared_config_path(job_dir)),
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

from rdfproduction.stitching_derivation import (
    _read_intweight_sign_hist, _read_weight_sign_sum, _derive_stitch_weights,
    _derive_stitch_cross_sections, _normalize_stitch_cross_section_era,
    _resolve_stitch_cross_section_era, _resolve_stitch_inclusive_sample,
    _resolve_stitch_inclusive_cross_section, _build_correctionlib_cset, _stitch_meta_read_path,
    _stitch_meta_files_for_sample, _resolve_stitch_meta_files, StitchingDerivationOperations,
)
from rdfproduction.stitching_validation import (
    _validate_stitch_monitor_summary, _stitch_output_meta_files, _root_key_basename,
    _load_stitch_validation_adapter, _stitch_validation_boundaries,
    _stitch_validation_boundaries_by_process, _load_stitch_cross_section_report,
    _stitch_validation_sample_groups, _stitch_validation_process_label,
    _stitch_validation_year_set, _stitch_validation_sample_scale,
    _stitch_validation_sample_factors, _stitch_validation_configured_weight_scale,
    _find_stitch_histogram_key, _stitch_validation_variable_boundaries,
    _stitch_validation_histogram_specs, _stitch_validation_histogram_data,
    _stitch_validation_excluded_bins, _write_stitch_validation_plots,
    _validate_stitch_meta_file, StitchingValidationOperations,
)



class StitchingDerivationTask(StitchingDerivationOperations, law.Task):
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
    source_partition = luigi.Parameter(default="auto")
    source_entries_per_job = luigi.IntParameter(default=100_000)
    source_partition_profile = luigi.Parameter(default="")
    source_sizing_pilots = _job_sizing_parameter('sizing_pilots')
    source_sizing_target_seconds = _job_sizing_parameter('sizing_target_seconds')
    source_sizing_pilot_target_seconds = _job_sizing_parameter('sizing_pilot_target_seconds')
    source_sizing_pilot_parallel = _job_sizing_parameter('sizing_pilot_parallel')
    source_sizing_pilot_prefix_entries = _job_sizing_parameter('sizing_pilot_prefix_entries')
    source_sizing_pilot_safety_factor = _job_sizing_parameter('sizing_pilot_safety_factor')
    source_input_metadata_cache = luigi.Parameter(default="")
    source_entry_range_mode = luigi.Parameter(default="filter")
    source_x509 = luigi.Parameter(default="")
    source_stage_in = luigi.BoolParameter(default=False)
    source_root_setup = luigi.Parameter(default="")
    source_container_setup = luigi.Parameter(default="")
    source_want_os = luigi.Parameter(default="")
    source_python_env = luigi.Parameter(default="")
    source_make_test_job = luigi.BoolParameter(default=False)
    source_dask_scheduler = luigi.Parameter(default="", significant=False)
    source_dask_workers = luigi.IntParameter(default=1, significant=False)
    source_max_runtime = luigi.IntParameter(default=7200, significant=False)
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
            "partition_profile": self.source_partition_profile,
            **_job_sizing_kwargs(self, input_prefix="source_"),
            "input_metadata_cache": self.source_input_metadata_cache,
            "entry_range_mode": self.source_entry_range_mode,
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
        self.derive()



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
        source_partition_profile=task.partition_profile,
        **_job_sizing_kwargs(task, output_prefix="source_"),
        source_input_metadata_cache=task.input_metadata_cache,
        source_entry_range_mode=task.entry_range_mode,
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



class StitchingValidationTask(StitchingValidationOperations, law.Task):
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
        self.validate_outputs(_stitch_output_meta_files(self.name, self.dataset_manifest))



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
            "Path to the submit_config.yaml template (forwarded to SkimTask "
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

        # Older workflows may collect histogram-bearing skim files directly.
        # Sparse-first files need the same post-hadd reconstruction as the
        # manifest-driven MergeHistograms task; ordinary skim files are left
        # unchanged.
        extract_nd_histograms(merged_path, merged_path, required=False)

        self.publish_message(
            f"CollectHistogramsTask: merged histogram written to {merged_path}"
        )

        with self.output()["done"].open("w") as fh:
            fh.write(
                f"status=done\nskim_name={self.skim_name}\n"
                f"files_merged={len(skim_files)}\n"
            )
