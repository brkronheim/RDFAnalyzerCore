"""CERN Open Data file-discovery helpers.

This module contains the pure-Python (non-LAW) helpers for parsing Open Data
sample configs and fetching file metadata from the CERN Open Data portal.  It
mirrors the role of :mod:`rucio_discovery` for Rucio-based workflows and can
be imported without ``luigi`` or ``law``.

The LAW task :class:`law.opendata_tasks.GetOpenDataFileList` imports its
helpers from this module so that unit tests and CLI scripts can use
``_parse_opendata_config`` without pulling in the full LAW/Luigi stack.

Public API
----------
- :func:`load_file_indices` – fetch ``_file_indices`` for a CERN Open Data record
- :func:`process_metadata` – map record files to analysis sample names
- :func:`parse_opendata_config` – parse a sample config (YAML or legacy text)

The ``_``-prefixed aliases (``_load_file_indices``, ``_process_metadata``,
``_parse_opendata_config``) are retained for backward compatibility with
callers that imported these private helpers from ``law/opendata_tasks.py``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Make dataset_manifest importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parents[1]
_LAW_DIR = _REPO_ROOT / "law"

if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from dataset_manifest import DatasetEntry, DatasetManifest  # noqa: E402

# Default XRootD redirector for CERN Open Data EOS files
OPENDATA_REDIRECTOR = "root://eospublic.cern.ch/"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_file_indices(recid):
    """Fetch ``_file_indices`` metadata for a CERN Open Data record.

    Tries ``cernopendata-client`` first, then falls back to the CERN Open Data
    REST API.  Raises :exc:`RuntimeError` if both methods fail.

    Parameters
    ----------
    recid : int or str
        CERN Open Data record ID.

    Returns
    -------
    list
        Raw ``_file_indices`` payload from the record metadata.
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
                api_error = "Response missing metadata._file_indices"
        else:
            api_error = f"CERN Open Data API returned HTTP {response.status_code}"

    raise RuntimeError(
        f"Failed to fetch metadata for recid {recid}. "
        f"Client error: {client_error}. API error: {api_error}."
    )


def process_metadata(recid, sample_names, redirector: str = OPENDATA_REDIRECTOR):
    """Return ``{sample_name: [uri, ...]}`` for the given *recid*.

    Parameters
    ----------
    recid : int or str
        CERN Open Data record ID.
    sample_names : dict[str, str]
        Mapping from Open Data key (as stored in ``_file_indices``) to the
        analysis sample name used in the manifest.
    redirector : str
        XRootD redirector prefix to prepend to bare EOS paths.

    Returns
    -------
    dict[str, list[str]]
        Per-sample list of XRootD URIs.
    """
    from submission_backend import ensure_xrootd_redirector  # lazy import

    result = load_file_indices(recid)
    file_dict: dict[str, list[str]] = {}
    for entry in result:
        for data in entry.get("files", []):
            uri = ensure_xrootd_redirector(data["uri"], redirector)
            key = data["key"].split("_file_index")[0]
            if key not in sample_names:
                continue
            mapped = sample_names[key]
            file_dict.setdefault(mapped, []).append(uri)
    return file_dict


def parse_opendata_config(config_file: str):
    """Parse *config_file* and return ``(samples, recids, lumi)``.

    Accepts only the modern YAML dataset manifest format (files ending in
    ``.yaml`` or ``.yml``). Legacy key=value text config support has been removed.

    For YAML manifests, entries that already have explicit ``files`` are
    included in the returned ``samples`` dict but their ``das`` field is *not*
    added to ``recids`` – those samples already have their files and do not
    need a CERN Open Data API lookup.

    Parameters
    ----------
    config_file : str
        Path to a YAML dataset manifest.

    Returns
    -------
    samples : dict[str, DatasetEntry]
        Typed dataset entries keyed by sample name.
    recids : list[str]
        CERN Open Data record IDs to query for file lists.
    lumi : float
        Integrated luminosity read from the config (defaults to ``1.0``).
    """
    ext = os.path.splitext(config_file)[1].lower()
    if ext not in (".yaml", ".yml"):
        raise ValueError(
            "Open Data sample manifests must be YAML files with a .yaml or .yml extension. "
            "Legacy key=value text format support has been removed."
        )

    manifest = DatasetManifest.load_yaml(config_file)
    samples = {entry.name: entry for entry in manifest.datasets}
    recids: list[str] = []
    for entry in manifest.datasets:
        if entry.files:
            # Explicit file list – no Open Data API lookup needed.
            continue
        if entry.das:
            for r in entry.das.split(","):
                r = r.strip()
                if r and r not in recids:
                    recids.append(r)
    return samples, recids, manifest.lumi


# Backward-compatible private aliases for callers that used the underscore names
# from law/opendata_tasks.py before the helpers were extracted here.
_load_file_indices = load_file_indices
_process_metadata = process_metadata
_parse_opendata_config = parse_opendata_config


__all__ = [
    "OPENDATA_REDIRECTOR",
    "load_file_indices",
    "process_metadata",
    "parse_opendata_config",
    "_load_file_indices",
    "_process_metadata",
    "_parse_opendata_config",
]
