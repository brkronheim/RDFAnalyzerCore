"""
XRootD file-discovery law tasks.

This module provides file-list discovery for datasets stored on any XRootD
server (EOS, dCache, …) using ``xrdfs ls``.  It is an independent input
method on equal footing with Rucio (:mod:`rucio_tasks`) and the CERN Open
Data portal (:mod:`opendata_tasks`).

Workflow
--------
  GetXRDFSFileList  (LocalWorkflow, one branch per dataset)
      → For each dataset entry in the manifest, either uses the explicit
        ``files`` list or runs ``xrdfs ls`` (with parallel BFS) against the
        configured XRootD server and path.
      → Writes:

        xrdfsFileList_{name}/{dataset_name}.json
        {"sample": "dataset_name", "files": ["root://...", ...]}

      → Output is consumed by :class:`~analysis_tasks.SkimTask` via
        ``--file-source xrdfs --file-source-name <name>``.

Usage
-----
::

    source env.sh

    law run GetXRDFSFileList \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --name myRun \\
        --xrdfs-server root://eosuser.cern.ch/

    law run SkimTask \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --submit-config analyses/myAnalysis/cfg/skim_config.yaml \\
        --name mySkimRun \\
        --file-source xrdfs --file-source-name myRun \\
        --exe build/bin/myanalysis
"""

from __future__ import annotations

import json
import os
import subprocess  # re-exported for compatibility with patched callers
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

from submission_backend import ensure_xrootd_redirector  # noqa: E402
from rdfproduction.performance import PerformanceRecorder, perf_path_for  # noqa: E402
from rdfproduction.discovery import _should_ignore_xrdfs_entry, xrdfs_list_files  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))


# Keep the private alias used by older callers.
_xrdfs_list_files = xrdfs_list_files


# ---------------------------------------------------------------------------
# XRDFSMixin
# ---------------------------------------------------------------------------


class XRDFSMixin:
    """Parameters shared by XRootD file-discovery tasks."""

    name = luigi.Parameter(
        description="Run name; file-list JSONs are written to xrdfsFileList_{name}/.",
    )
    dataset_manifest = luigi.Parameter(
        description=(
            "Path to the dataset manifest YAML.  Each dataset entry should "
            "specify either an XRootD server + remote path via the "
            "``xrdfs_server`` and ``xrdfs_path`` fields, or explicit ``files`` URLs."
        ),
    )
    xrdfs_server = luigi.Parameter(
        default="",
        description=(
            "Default XRootD server (e.g. ``root://eosuser.cern.ch/``).  "
            "Can be overridden per-dataset in the manifest."
        ),
    )
    xrdfs_pattern = luigi.Parameter(
        default="*.root",
        description="Glob pattern for file discovery (default: *.root).",
    )
    xrdfs_recursive = luigi.BoolParameter(
        default=True,
        description="Recursively search sub-directories (default: True).",
    )

    @property
    def _file_list_dir(self) -> str:
        return os.path.join(WORKSPACE, f"xrdfsFileList_{self.name}")


# ---------------------------------------------------------------------------
# GetXRDFSFileList
# ---------------------------------------------------------------------------


class GetXRDFSFileList(XRDFSMixin, law.LocalWorkflow):
    """Produce a per-dataset XRootD file-list JSON by running ``xrdfs ls``.

    This task is the canonical input method for files stored on any XRootD
    endpoint (EOS, dCache, …).  It is an independent discovery back-end on
    equal footing with :class:`~rucio_tasks.GetRucioFileList` and
    :class:`~opendata_tasks.GetOpenDataFileList`.

    For each dataset entry in the manifest the task looks for an
    ``xrdfs_path`` field (the remote directory to search) and optionally an
    ``xrdfs_server`` field.  If neither is provided and explicit ``files``
    are listed they are used directly.

    Output per dataset::

        xrdfsFileList_{name}/{dataset_name}.json
        {"sample": "dataset_name", "files": ["root://...", ...]}

    Consumed by :class:`~analysis_tasks.SkimTask` via::

        --file-source xrdfs --file-source-name <name>
    """

    task_namespace = ""

    def create_branch_map(self) -> dict[int, object]:
        from dataset_manifest import DatasetManifest as _DM

        manifest = _DM.load(self.dataset_manifest)
        return {i: entry for i, entry in enumerate(manifest.datasets)}

    def output(self):
        from dataset_manifest import DatasetEntry as _DE

        dataset: _DE = self.branch_data
        return law.LocalFileTarget(
            os.path.join(self._file_list_dir, f"{dataset.name}.json")
        )

    def run(self):
        task_label = f"GetXRDFSFileList[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    def _run_impl(self):
        from dataset_manifest import DatasetEntry as _DE

        dataset: _DE = self.branch_data

        if dataset.files:
            files = [ensure_xrootd_redirector(f) for f in dataset.files]
        else:
            extras = getattr(dataset, "extra", {}) or {}
            server = extras.get("xrdfs_server") or self.xrdfs_server
            remote_path = extras.get("xrdfs_path", "")

            if not remote_path:
                self.publish_message(
                    f"Dataset '{dataset.name}' has no files or xrdfs_path; "
                    "writing empty list."
                )
                files = []
            elif not server:
                raise RuntimeError(
                    f"Dataset '{dataset.name}': xrdfs_path is set but no "
                    "xrdfs_server provided (set --xrdfs-server or add "
                    "'xrdfs_server' to the manifest entry)."
                )
            else:
                files = xrdfs_list_files(
                    server=server,
                    remote_path=remote_path,
                    pattern=self.xrdfs_pattern,
                    recursive=self.xrdfs_recursive,
                )

        payload = {"sample": dataset.name, "files": files}
        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(payload, fh, indent=2)
        self.publish_message(
            f"GetXRDFSFileList: {dataset.name} → {len(files)} file(s)"
        )
