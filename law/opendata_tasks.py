"""
CERN Open Data file-discovery law tasks.

This module provides file-list discovery for datasets published on the CERN
Open Data portal (https://opendata.cern.ch).  It is an independent input
method on equal footing with Rucio (:mod:`rucio_tasks`) and XRootD
(:mod:`xrdfs_tasks`).

Workflow
--------
  GetOpenDataFileList  (LocalWorkflow, one branch per dataset)
      → For each dataset entry in the sample config, queries the CERN Open
        Data REST API (or ``cernopendata-client`` if available) and writes:

        openDataFileList_{name}/{sample_name}.json
        {"sample": "name", "files": ["root://eospublic.cern.ch//eos/...", ...]}

      → Output is consumed by :class:`~analysis_tasks.SkimTask` via
        ``--file-source opendata --file-source-name <name>``.

Usage
-----
::

    source env.sh

    law run GetOpenDataFileList \\
        --submit-config analyses/myAnalysis/cfg/submit_config.txt \\
        --name myRun --exe build/bin/myanalysis

    law run SkimTask \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --submit-config analyses/myAnalysis/cfg/skim_config.txt \\
        --name mySkimRun \\
        --file-source opendata --file-source-name myRun \\
        --exe build/bin/myanalysis
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore
import requests
from requests.exceptions import RequestException

law.contrib.load("htcondor")

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from submission_backend import (  # noqa: E402
    read_config,
    ensure_xrootd_redirector,
)
from dataset_manifest import DatasetManifest  # noqa: E402
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))

# Default XRootD redirector for CERN Open Data files
OPENDATA_REDIRECTOR = "root://eospublic.cern.ch/"


# ---------------------------------------------------------------------------
# Low-level Open Data helpers
# ---------------------------------------------------------------------------


def _load_file_indices(recid):
    """Fetch ``_file_indices`` metadata for a CERN Open Data record.

    Tries ``cernopendata-client`` first, then falls back to the REST API.
    Raises :exc:`RuntimeError` if both methods fail.
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


def _process_metadata(recid, sample_names):
    """Return ``{sample_name: [uri, ...]}`` for the given *recid*.

    *sample_names* maps Open-Data key → analysis sample name.
    """
    result = _load_file_indices(recid)
    file_dict: dict[str, list[str]] = {}
    for entry in result:
        for data in entry.get("files", []):
            uri = ensure_xrootd_redirector(data["uri"], OPENDATA_REDIRECTOR)
            key = data["key"].split("_file_index")[0]
            if key not in sample_names:
                continue
            mapped = sample_names[key]
            file_dict.setdefault(mapped, []).append(uri)
    return file_dict


def _parse_opendata_config(config_file):
    """Parse *config_file* and return ``(samples, recids, lumi)``.

    Accepts both the legacy ``key=value`` text format **and** the YAML dataset
    manifest format (detected by ``.yaml``/``.yml`` extension).
    """
    ext = os.path.splitext(config_file)[1].lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        samples = manifest.to_legacy_sample_dict()
        recids: list[str] = []
        for entry in manifest.datasets:
            if entry.das:
                for r in entry.das.split(","):
                    r = r.strip()
                    if r and r not in recids:
                        recids.append(r)
        return samples, recids, manifest.lumi

    samples: dict[str, dict] = {}
    recids: list[str] = []
    lumi = 1.0

    with open(config_file) as fh:
        for line in fh:
            line = line.split("#")[0].strip().split()
            inner: dict[str, str] = {}
            for pair in line:
                parts = pair.split("=")
                if len(parts) == 2:
                    inner[parts[0]] = parts[1]
            if "lumi" in inner:
                lumi = float(inner["lumi"])
            if "recids" in inner:
                recids += [r.strip() for r in inner["recids"].split(",") if r.strip()]
            if "name" in inner and "das" in inner:
                samples[inner["name"]] = inner

    return samples, recids, lumi


# ---------------------------------------------------------------------------
# OpenDataMixin – parameters for file-discovery-only tasks
# ---------------------------------------------------------------------------


class OpenDataMixin:
    """Parameters required for CERN Open Data file-list discovery tasks.

    Contains only the parameters needed to locate the sample config and
    query the Open Data API; submission-management parameters live
    exclusively in :class:`~analysis_tasks.SkimMixin`.
    """

    submit_config = luigi.Parameter(
        description=(
            "Path to the submit-config file (key=value or YAML) used to "
            "locate the sample config via the ``sampleConfig`` key."
        ),
    )
    name = luigi.Parameter(
        description=(
            "Run name; file-list JSONs are written to "
            "``openDataFileList_{name}/``."
        ),
    )
    dataset = luigi.Parameter(
        default="",
        description=(
            "Optional: restrict discovery to a single named dataset.  "
            "Leave empty to process all datasets."
        ),
    )

    # ---- derived helpers ---------------------------------------------------

    def _resolve(self, path_value: str) -> str:
        """Resolve *path_value* relative to the submit-config's parent directory."""
        if not path_value:
            return path_value
        if os.path.isabs(path_value):
            return path_value
        config_base = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(self.submit_config)), "..")
        )
        if os.path.exists(path_value):
            return os.path.abspath(path_value)
        return os.path.abspath(os.path.join(config_base, path_value))

    @property
    def _config_dict(self) -> dict:
        return read_config(self.submit_config)

    @property
    def _sample_config(self) -> str:
        return self._resolve(self._config_dict["sampleConfig"])

    @property
    def _file_list_dir(self) -> str:
        return os.path.join(WORKSPACE, f"openDataFileList_{self.name}")


# ---------------------------------------------------------------------------
# GetOpenDataFileList
# ---------------------------------------------------------------------------


class GetOpenDataFileList(OpenDataMixin, law.LocalWorkflow):
    """Produce a per-dataset XRootD file-list JSON from the CERN Open Data portal.

    One branch is created per dataset entry in the sample config.  Each branch
    queries the CERN Open Data REST API (or ``cernopendata-client``) and writes
    a compact JSON file::

        openDataFileList_{name}/{sample_name}.json
        {"sample": "name", "files": ["root://eospublic.cern.ch//eos/...", ...]}

    The output can be consumed by :class:`~analysis_tasks.SkimTask` via::

        --file-source opendata --file-source-name <name>
    """

    task_namespace = ""

    def create_branch_map(self) -> dict[int, str]:
        samples, _, _ = _parse_opendata_config(self._sample_config)
        if self.dataset:
            if self.dataset not in samples:
                raise ValueError(
                    f"Dataset {self.dataset!r} not found in sample config. "
                    f"Available: {sorted(samples.keys())}"
                )
            return {0: self.dataset}
        return {i: key for i, key in enumerate(sorted(samples.keys()))}

    def output(self):
        sample_key = self.branch_data
        return law.LocalFileTarget(
            os.path.join(self._file_list_dir, f"{sample_key}.json")
        )

    def run(self):
        task_label = f"GetOpenDataFileList[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    def _run_impl(self):
        sample_key = self.branch_data
        samples, recids, lumi = _parse_opendata_config(self._sample_config)
        sample = samples[sample_key]
        name = sample["name"]

        sample_names = {s.get("das", s["name"]): s["name"] for s in samples.values()}

        file_list: list[str] = []
        for recid in recids:
            try:
                partial = _process_metadata(recid, sample_names)
            except Exception as e:
                self.publish_message(
                    f"Warning: failed to fetch metadata for recid {recid} "
                    f"(sample '{name}'): {e}"
                )
                continue
            file_list.extend(partial.get(name, []))

        if not file_list:
            self.publish_message(
                f"No files found for sample {name!r}; writing empty list."
            )

        payload = {"sample": name, "files": file_list}
        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(payload, fh, indent=2)
        self.publish_message(
            f"GetOpenDataFileList: {name} → {len(file_list)} file(s)"
        )
