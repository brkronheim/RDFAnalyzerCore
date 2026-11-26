"""Compatibility imports for the former flat production job module."""

from rdfproduction.jobs.staging import (
    _stream_analysis_output, _MC_GENERATOR_CONFIG_KEYS, _remove_mc_generator_config_for_data,
    _find_dataset_entry, _manifest_lumi_for_dataset, _resolve_analysis_config_path,
    _analysis_config_payload_sources, _copy_analysis_config_files, _analysis_aux_dir,
    _copy_analysis_aux_dir, _bundle_analysis_aux_dir, _bundle_analysis_config_files,
    _symlink_or_copy_path, _staged_copy_is_current, _local_shared_libs_with_signature,
    _ensure_local_analysis_runtime_layout, _shared_inputs_archive_name,
    _validate_staged_skim_executable, _validate_x509_proxy, _validate_staged_skim_proxy,
    _build_shared_inputs_archive, _normalize_shared_config_references,
    _resolve_runtime_transfer_path, _runtime_loose_cfg_files, _copy_runtime_loose_cfg_files,
    _materialize_runtime_staged_inputs, _skim_shared_loose_input_files,
    _shared_archive_top_level_files, _copy_job_submission_files, _copy_tree_contents,
    _extract_tar_archive, _python_environment_setup, _materialize_file_source_runtime_job,
    _run_prepared_skim_job, _rewrite_submit_transfer_files, _skim_shared_input_files,
    _rewrite_staged_analysis_config,
)
from rdfproduction.jobs.output_validation import (
    _output_files_exist_batch, _output_file_info_batch, _root_file_tree_error,
    _config_enables_skim, _skim_job_output_errors, _run_skim_job_with_output_contract,
)
from rdfproduction.jobs.condor_files import (
    _safe_dataset_submission_name, _dataset_submission_dir,
    _group_prepared_skim_jobs_by_dataset, _discover_jobs_in_submission_dir,
    _skim_job_file_list, _split_skim_file_list, _split_skim_partition,
    _append_output_part_suffix, _prepare_split_skim_job, _condor_job_elapsed_runtime,
    _condor_job_exceeded_runtime, _submit_single_skim_job,
)
from rdfproduction.jobs.provenance import (
    _build_task_provenance, _build_skim_manifest, _read_histogram_config_entries,
    _read_root_histogram_metadata, _read_root_histogram_names,
    _histogram_axis_labels_from_entries, _infer_region_definitions, _initial_sample_metadata,
    _augment_sample_metadata_with_axis_labels, _matches_manifest_selector,
    _resolve_manifest_samples, _resolve_manifest_fit_metadata, _make_meta_output_name,
    _build_histfill_manifest,
)
from rdfproduction.jobs.job_config import (
    _dataset_job_metadata, _resolve_output_destination_base, _rewrite_job_output_destinations,
    _stitch_job_overrides, _write_job_config, _extract_lfn_for_redirect,
    _select_test_input_url, _write_config_file, _plan_file_source_jobs,
)
from rdfproduction.jobs.paths import (
    WORKSPACE, AnalysisPaths, SkimPaths,
)
from rdfproduction.jobs.prepare import (
    PreparationOperations,
)
from rdfproduction.jobs.pilots import (
    PilotOperations,
)
from rdfproduction.jobs.smoke import (
    SmokeOperations,
)
from rdfproduction.jobs.submit import (
    SubmissionOperations,
)
from rdfproduction.jobs.monitor import (
    MonitorOperations,
)
from rdfproduction.jobs.branches import (
    BranchOperations,
)
from rdfproduction.jobs.run import (
    ProductionRun,
)
from rdfproduction.transport import collect_local_shared_libs as _collect_skim_shared_libs
from rdfproduction.transport import condor_history_exit as _condor_history_exit
from rdfproduction.transport import condor_event_log_exit as _condor_event_log_exit
from rdfproduction.transport import condor_q_ads as _condor_q_ads
from rdfproduction.transport import condor_rm_job as _condor_rm_job
from rdfproduction.transport import eos_files_exist_batch as _eos_files_exist_batch_util
from rdfproduction.transport import eos_file_info_batch as _eos_file_info_batch_util
from rdfproduction.transport import read_failed_job_error as _read_failed_job_error
from rdfproduction.executors import _run_analysis_job
from rdfproduction.partitioning import _query_tree_entries
from rdfproduction.condor_submission import submit_skim_batches
from derivation_correctionlib import _read_weight_sum as _shared_read_weight_sum
from derivation_correctionlib import _read_weight_sign_sum as _shared_read_weight_sign_sum
from job_sizing import update_sizing_after_run as _update_job_sizing
from xrd_site_rankings import update_after_run as _update_xrd_site_rankings
from submission_backend import read_config
from submission_backend import write_config
from submission_backend import write_submit_files
import time
import subprocess
import yaml
import json
