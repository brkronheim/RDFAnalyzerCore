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
import sys
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore

law.contrib.load("htcondor")

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from submission_backend import (  # noqa: E402
    read_config,
    ensure_xrootd_redirector,
)
from dataset_manifest import DatasetEntry, DatasetManifest  # noqa: E402
from opendata_discovery import (  # noqa: E402
    OPENDATA_REDIRECTOR,
    load_file_indices as _load_file_indices,
    process_metadata as _process_metadata,
    parse_opendata_config as _parse_opendata_config,
)
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))


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
        name = sample.name

        sample_names = {(s.das or s.name): s.name for s in samples.values()}

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
