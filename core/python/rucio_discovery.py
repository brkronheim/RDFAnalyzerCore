"""Shared Rucio discovery helpers for LAW tasks and production CLIs.

This module centralizes Rucio client bootstrapping, dataset manifest/sample-config
loading, and replica query grouping so that both ``law/rucio_tasks.py`` and
``core/python/production_submit.py`` can reuse the same public helper surface
without importing private task-module helpers.
"""

from __future__ import annotations

import os
import time
import warnings
from dataclasses import dataclass
from pathlib import Path

from requests.exceptions import ChunkedEncodingError, RequestException
import urllib3

from submission_backend import ensure_xrootd_redirector
from dataset_manifest import DatasetEntry, DatasetManifest

# Default XRootD redirector used when Rucio does not supply a site-specific one.
RUCIO_REDIRECTOR = "root://cms-xrd-global.cern.ch/"

# Rucio needs the default configuration from CMS cvmfs in most environments.
if "RUCIO_HOME" not in os.environ:
    os.environ["RUCIO_HOME"] = "/cvmfs/cms.cern.ch/rucio/current"


@dataclass(frozen=True)
class SampleConfigData:
    """Typed view of a sample-config file used by submission planning code."""

    entries: dict[str, DatasetEntry]
    base_dirs: list[str]
    lumi: float
    whitelist: list[str]
    blacklist: list[str]


def get_proxy_path() -> str:
    """Return the active VOMS proxy path, raising on missing/expired credentials."""
    import subprocess

    try:
        subprocess.run(
            "voms-proxy-info -exists -valid 0:20", shell=True, check=True
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "VOMS proxy expired or missing: "
            "run `voms-proxy-init -voms cms -rfc --valid 168:0`"
        ) from exc

    return subprocess.check_output(
        "voms-proxy-info -path", shell=True, text=True
    ).strip()


def get_rucio_client(proxy: str | None = None):
    """Return an authenticated Rucio client.

    The ``rucio.client.Client`` import stays local so callers in environments
    without Rucio installed can still import this module for unit tests.
    """
    from rucio.client import Client  # type: ignore

    try:
        if not proxy:
            proxy = get_proxy_path()
        return Client()
    except Exception as exc:
        raise RuntimeError(f"Cannot create Rucio client: {exc}") from exc


def query_rucio(
    dataset_path: str,
    file_split_gb: float,
    whitelist: list[str] | None = None,
    blacklist: list[str] | None = None,
    site_override: str = "",
    client=None,
    max_files_per_group: int = 50,
    WL: list[str] | None = None,
    BL: list[str] | None = None,
) -> dict:
    """Query Rucio replicas and return grouped XRootD URL strings with per-file redirectors.

    Returns
    -------
    dict
        ``{"groups": dict[int, str], "site_redirectors": dict[str, list[str]]}``

        * ``"groups"`` maps ``group_index`` to a comma-separated string of XRootD URLs.
        * ``"site_redirectors"`` maps each bare LFN to the ordered list of redirectors
          that should be probed on the worker node.
    """
    if whitelist is None and WL is not None:
        whitelist = WL
    if blacklist is None and BL is not None:
        blacklist = BL
    whitelist = whitelist or []
    blacklist = blacklist or []

    if not dataset_path or not isinstance(dataset_path, str) or not dataset_path.startswith("/"):
        print(f"Warning: invalid Rucio dataset path: {dataset_path!r} - skipping")
        return {"groups": {}, "site_redirectors": {}}

    max_retries = 5
    backoff = 0.5
    replicas = None
    for attempt in range(1, max_retries + 1):
        try:
            replicas = list(
                client.list_replicas([{"scope": "cms", "name": dataset_path}])
            )
            break
        except (ChunkedEncodingError, RequestException, urllib3.exceptions.ProtocolError) as exc:
            if attempt < max_retries:
                print(
                    f"Warning: Rucio attempt {attempt}/{max_retries} failed: {exc}. "
                    f"Retrying in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff *= 2
            else:
                print(
                    f"Error: Rucio failed for {dataset_path!r} after "
                    f"{max_retries} attempts: {exc}"
                )
                return {"groups": {}, "site_redirectors": {}}
        except Exception as exc:
            print(f"Error: unexpected Rucio error for {dataset_path!r}: {exc}")
            return {"groups": {}, "site_redirectors": {}}

    if not replicas:
        print(f"Warning: no replicas found for {dataset_path!r}")
        return {"groups": {}, "site_redirectors": {}}

    import re as _re

    groups: dict[int, str] = {}
    per_file_redirectors: dict[str, list[str]] = {}
    running_size = 0.0
    running_files = 0
    group = 0

    for filedata in replicas:
        size_gb = filedata.get("bytes", 0) * 1e-9

        # Strip any existing redirector from the file name so we always work
        # with a bare LFN.
        file_name = filedata["name"]
        if file_name.startswith("root://"):
            m = _re.match(r"root://[^/]+/+(.*)", file_name)
            if m:
                file_name = "/" + m.group(1)

        # Collect site-specific redirectors from Rucio states for site-aware
        # probing on the worker node.
        states = filedata.get("states", {})
        site_redirectors: list[str] = []
        seen_redirectors: set[str] = set()
        for site_key, state_val in states.items():
            if state_val != "AVAILABLE" or "Tape" in site_key:
                continue
            site = site_key.replace("_Disk", "")
            if blacklist and any(b.lower() in site.lower() for b in blacklist):
                continue
            redir = f"root://xrootd-cms.infn.it//store/test/xrootd/{site}/"
            if redir not in seen_redirectors:
                site_redirectors.append(redir)
                seen_redirectors.add(redir)
        per_file_redirectors[file_name] = site_redirectors

        running_size += size_gb
        running_files += 1
        if running_size > file_split_gb or running_files >= max_files_per_group:
            group += 1
            running_size = size_gb
            running_files = 1

        url = ensure_xrootd_redirector(file_name, RUCIO_REDIRECTOR)
        if group in groups:
            groups[group] += "," + url
        else:
            groups[group] = url

    return {"groups": groups, "site_redirectors": per_file_redirectors}


def load_legacy_sample_config(config_file: str) -> SampleConfigData:
    """Load a legacy key=value sample-config file as typed sample data.

    This compatibility adapter is intentionally explicit. New workflows should
    prefer YAML manifests and `load_sample_config()` on ``.yaml`` / ``.yml``
    files. Legacy ``.txt`` sample configs remain supported only through this
    compatibility helper.
    """
    entries: dict[str, DatasetEntry] = {}
    base_dirs: list[str] = []
    lumi = 1.0
    whitelist: list[str] = []
    blacklist: list[str] = []

    with open(config_file) as fh:
        for raw_line in fh:
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue

            inner: dict[str, str] = {}
            for part in line.split():
                if "=" not in part:
                    continue
                key, value = part.split("=", 1)
                inner[key] = value

            if "name" in inner:
                entry = DatasetEntry.from_legacy_dict(inner)
                entries[entry.name] = entry
            elif "prefix_cern" in inner:
                base_dirs.append(inner["prefix_cern"])
            elif "lumi" in inner:
                lumi = float(inner["lumi"])
            elif "WL" in inner:
                whitelist = [item for item in inner["WL"].split(",") if item]
            elif "BL" in inner:
                blacklist = [item for item in inner["BL"].split(",") if item]

    return SampleConfigData(
        entries=entries,
        base_dirs=base_dirs,
        lumi=lumi,
        whitelist=whitelist,
        blacklist=blacklist,
    )


def load_sample_config(config_file: str) -> SampleConfigData:
    """Return a typed representation of a sample-config file."""
    ext = Path(config_file).suffix.lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        return SampleConfigData(
            entries={entry.name: entry for entry in manifest.datasets},
            base_dirs=[],
            lumi=manifest.lumi,
            whitelist=manifest.whitelist,
            blacklist=manifest.blacklist,
        )

    if ext == ".txt":
        warnings.warn(
            "Legacy text sample configs are deprecated. Convert to YAML manifests "
            "and use load_sample_config() on .yaml/.yml files; this compatibility "
            "helper will continue to parse .txt files for legacy workflows.",
            DeprecationWarning,
            stacklevel=2,
        )
        return load_legacy_sample_config(config_file)

    raise ValueError(
        "Unsupported sample config extension. Use .yaml/.yml for YAML manifests. "
        "Legacy .txt files may be loaded explicitly with load_legacy_sample_config()."
    )


__all__ = [
    "RUCIO_REDIRECTOR",
    "get_proxy_path",
    "get_rucio_client",
    "query_rucio",
    "load_sample_config",
    "load_legacy_sample_config",
    "SampleConfigData",
]
