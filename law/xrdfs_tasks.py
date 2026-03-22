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
        --submit-config analyses/myAnalysis/cfg/skim_config.txt \\
        --name mySkimRun \\
        --file-source xrdfs --file-source-name myRun \\
        --exe build/bin/myanalysis
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore

law.contrib.load("htcondor")

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from submission_backend import ensure_xrootd_redirector  # noqa: E402
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))


# ---------------------------------------------------------------------------
# Low-level XRootD helpers
# ---------------------------------------------------------------------------


def xrdfs_list_files(
    server: str,
    remote_path: str,
    pattern: str = "*.root",
    recursive: bool = True,
    timeout: int = 60,
    max_workers: int = 8,
) -> list[str]:
    """Discover files on an XRootD server using ``xrdfs ls``.

    Directory listings are performed in parallel using a
    :class:`~concurrent.futures.ThreadPoolExecutor` BFS traversal, which is
    substantially faster than sequential recursion for deep trees.

    Parameters
    ----------
    server:
        XRootD server URL, e.g. ``root://eosuser.cern.ch/``.
    remote_path:
        Remote directory path to search, e.g. ``/eos/user/a/alice/ntuples/``.
    pattern:
        Glob-style filename pattern matched against the basename only.
    recursive:
        When ``True``, recursively descend into sub-directories.
    timeout:
        Per-``xrdfs ls`` call timeout in seconds.
    max_workers:
        Maximum parallel ``xrdfs ls`` threads per BFS level.

    Returns
    -------
    list[str]
        Sorted list of ``root://{server}/{path}`` URLs for matching files.
    """

    def _ls_single_dir(path: str) -> tuple[list[str], list[str]]:
        """List one directory; return (files, subdirs)."""
        try:
            result = subprocess.run(
                ["xrdfs", server, "ls", "-l", path],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except Exception as exc:
            logging.warning("xrdfs ls failed for %s: %s", path, exc)
            return [], []

        if result.returncode != 0:
            logging.warning(
                "xrdfs ls returned %d for %s: %s",
                result.returncode,
                path,
                result.stderr.strip(),
            )
            return [], []

        files: list[str] = []
        subdirs: list[str] = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if not parts:
                continue
            entry = parts[-1]
            if not entry.startswith("/"):
                continue
            if line.startswith("d") or (len(parts) > 4 and parts[0].startswith("d")):
                subdirs.append(entry)
            elif fnmatch.fnmatch(os.path.basename(entry), pattern):
                files.append(entry)
        return files, subdirs

    all_files: list[str] = []
    queue: list[str] = [remote_path]

    while queue:
        workers = min(len(queue), max_workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_dir = {
                executor.submit(_ls_single_dir, d): d for d in queue
            }
            next_queue: list[str] = []
            for fut in as_completed(future_to_dir):
                try:
                    files, subdirs = fut.result()
                except Exception as exc:
                    logging.warning(
                        "xrdfs listing failed for %s: %s",
                        future_to_dir[fut],
                        exc,
                    )
                    files, subdirs = [], []
                all_files.extend(files)
                if recursive:
                    next_queue.extend(subdirs)
        queue = next_queue

    return [
        f"{server.rstrip('/')}/{p.lstrip('/')}"
        for p in sorted(all_files)
    ]


# Keep the private-name alias used by older code
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
