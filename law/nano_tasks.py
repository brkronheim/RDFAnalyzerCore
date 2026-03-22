"""
Backward-compatibility shim for nano_tasks.

The CMS-NANO-specific code that used to live here has been reorganised into
three focused modules:

* :mod:`rucio_tasks`     – Generic Rucio file-list discovery
                           (:class:`GetRucioFileList`, :class:`GetNANOFileList`)
* :mod:`xrdfs_tasks`     – XRootD (``xrdfs ls``) file-list discovery
                           (:class:`GetXRDFSFileList`)
* :mod:`ingestion_utils` – Shared utility functions (EOS helpers, HTCondor
                           helpers, file-copy utilities, …)

This module re-exports everything that was previously accessible via
``from nano_tasks import …`` so that existing code and tests continue to work
without modification.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Re-export from rucio_tasks
# ---------------------------------------------------------------------------
from rucio_tasks import (  # noqa: F401
    RUCIO_REDIRECTOR,
    GetNANOFileList,
    GetRucioFileList,
    RucioMixin,
    _get_proxy_path,
    _get_rucio_client,
    _get_sample_list,
    _query_rucio,
)

# Legacy constant alias used by some callers
NANO_REDIRECTOR = RUCIO_REDIRECTOR  # noqa: F401

# ---------------------------------------------------------------------------
# Re-export from xrdfs_tasks
# ---------------------------------------------------------------------------
from xrdfs_tasks import (  # noqa: F401
    GetXRDFSFileList,
    XRDFSMixin,
    _xrdfs_list_files,
    xrdfs_list_files,
)

# ---------------------------------------------------------------------------
# Re-export from ingestion_utils
# ---------------------------------------------------------------------------
from ingestion_utils import (  # noqa: F401
    _append_unique_lines,
    _collect_local_shared_libs,
    _condor_history_exit,
    _condor_q_ads,
    _condor_q_cluster,
    _condor_rm_job,
    _copy_dir,
    _copy_file,
    _eos_file_exists,
    _eos_files_exist_batch,
    _normalize_config_paths,
    _read_failed_job_error,
    _rechunk_urls,
    _submit_single_job,
    append_unique_lines,
    collect_local_shared_libs,
    condor_history_exit,
    condor_q_ads,
    condor_q_cluster,
    condor_rm_job,
    copy_dir,
    copy_file,
    eos_file_exists,
    eos_files_exist_batch,
    normalize_config_paths,
    read_failed_job_error,
    rechunk_urls,
    submit_single_job,
)
