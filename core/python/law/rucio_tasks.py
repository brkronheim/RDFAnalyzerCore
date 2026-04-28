"""
Generic Rucio file-discovery law tasks.

This module provides :class:`GetRucioFileList`, a generic Rucio file-discovery
task that can query any Rucio instance for file replicas given a dataset
manifest or legacy sample-config file.

Workflow
--------
  GetRucioFileList  (LocalWorkflow, one branch per dataset)
      → Queries Rucio for each dataset's DAS/Rucio path (or uses explicit
        ``files`` from the manifest), groups the resulting URLs by size and
        file count, then writes:

        rucioFileList_{name}/{sample_name}.json
        {"sample": "name", "files": ["root://...", ...], "groups": [...]}

      → The ``groups`` field encodes Rucio-computed file groupings and is
        consumed by :class:`~analysis_tasks.PrepareSkimJobs` to avoid
        re-partitioning large datasets.

The output directory prefix is ``rucioFileList_{name}``.

Usage
-----
::

    source env.sh

    # Discover files via Rucio and write per-dataset JSON lists
    law run GetRucioFileList \\
        --submit-config analyses/myAnalysis/cfg/submit_config.txt \\
        --name myRun --x509 /tmp/x509 --exe build/bin/myanalysis

    # Chain into SkimTask using the 'rucio' source
    law run SkimTask \\
        --submit-config analyses/myAnalysis/cfg/submit_config.txt \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --name mySkimRun \\
        --file-source rucio --file-source-name myRun \\
        --exe build/bin/myanalysis
"""

from __future__ import annotations

import json
import os
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
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from submission_backend import (  # noqa: E402
    read_config,
    ensure_xrootd_redirector,
)
from rucio_discovery import (  # noqa: E402
    get_proxy_path,
    get_rucio_client,
    load_sample_config,
    query_rucio,
)
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

# Backward-compatible private aliases retained for compatibility imports/tests.
_get_proxy_path = get_proxy_path
_get_rucio_client = get_rucio_client
_query_rucio = query_rucio


# ---------------------------------------------------------------------------
# RucioMixin – parameters for file-discovery-only tasks
# ---------------------------------------------------------------------------


class RucioMixin:
    """Parameters required for Rucio-based file-list discovery tasks.

    This is a stripped-down replacement for the old ``NANOMixin``.  It contains
    only the parameters needed to locate the sample config and query Rucio;
    all submission-management parameters have been removed and now live
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
            "``rucioFileList_{name}/``."
        ),
    )
    dataset = luigi.Parameter(
        default="",
        description=(
            "Optional: restrict discovery to a single named dataset from the "
            "sampleConfig.  Leave empty to process all datasets."
        ),
    )
    size = luigi.FloatParameter(
        default=10.0,
        description="Maximum GB of data per Rucio file group (default: 10).",
    )
    files_per_job = luigi.IntParameter(
        default=50,
        description=(
            "Maximum number of files per Rucio file group (default: 50).  "
            "Groups are consumed by PrepareSkimJobs for job-level splitting."
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
        """Directory where per-dataset file-list JSONs are written."""
        return os.path.join(WORKSPACE, f"rucioFileList_{self.name}")


# ---------------------------------------------------------------------------
# GetRucioFileList – generic Rucio file-discovery task
# ---------------------------------------------------------------------------


class GetRucioFileList(RucioMixin, law.LocalWorkflow):
    """Produce a per-dataset XRootD file-list JSON by querying Rucio.

    One branch is created per dataset entry in the sample config.  Each branch
    queries the Rucio replica catalogue (or uses an explicit ``files`` field
    from the manifest) and writes a compact JSON file::

        rucioFileList_{name}/{sample_name}.json
        {
          "sample": "name",
          "files":  ["root://...", ...],
          "groups": ["url1,url2", "url3,url4", ...]   // optional, from Rucio
        }

    The optional ``groups`` field encodes Rucio-computed size+count groupings
    so that :class:`~analysis_tasks.PrepareSkimJobs` can respect them without
    re-partitioning.

    The output can be consumed by :class:`~analysis_tasks.SkimTask` via::

        --file-source rucio --file-source-name <name>
    """

    task_namespace = ""

    def create_branch_map(self) -> dict[int, str]:
        sample_config = load_sample_config(self._sample_config)
        entries = sample_config.entries
        if self.dataset:
            if self.dataset not in entries:
                raise ValueError(
                    f"Dataset {self.dataset!r} not found in sample config. "
                    f"Available: {sorted(entries.keys())}"
                )
            return {0: self.dataset}
        return {i: key for i, key in enumerate(sorted(entries.keys()))}

    def output(self):
        sample_key = self.branch_data
        return law.LocalFileTarget(
            os.path.join(self._file_list_dir, f"{sample_key}.json")
        )

    def run(self):
        task_label = f"GetRucioFileList[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    def _run_impl(self):  # noqa: C901 – intentionally thorough
        sample_key = self.branch_data
        sample_config = load_sample_config(self._sample_config)
        entry = sample_config.entries[sample_key]
        whitelist = sample_config.whitelist
        blacklist = sample_config.blacklist

        name = entry.name
        das_path = entry.das or ""
        site_override = entry.site or ""

        all_urls: list[str] = []
        seen_urls: set[str] = set()
        rucio_groups: list[str] = []

        # Explicit file list overrides Rucio discovery
        explicit_files = entry.files
        if explicit_files:
            for url in explicit_files:
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(ensure_xrootd_redirector(url))
        elif das_path:
            try:
                client = get_rucio_client()
            except Exception as e:
                raise RuntimeError(f"Cannot open Rucio client: {e}") from e

            das_entries = [d.strip() for d in das_path.split(",") if d.strip()]

            if len(das_entries) == 1:
                partial_results = {
                    das_entries[0]: query_rucio(
                        das_entries[0],
                        self.size,
                        whitelist,
                        blacklist,
                        site_override,
                        client,
                        max_files_per_group=self.files_per_job,
                    )
                }
            else:
                partial_results: dict[str, dict] = {}
                workers = min(len(das_entries), 8)
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_to_entry = {
                        executor.submit(
                            query_rucio,
                            entry,
                            self.size,
                            whitelist,
                            blacklist,
                            site_override,
                            client,
                            self.files_per_job,
                        ): entry
                        for entry in das_entries
                    }
                    for fut in as_completed(future_to_entry):
                        entry = future_to_entry[fut]
                        try:
                            partial_results[entry] = fut.result()
                        except Exception as exc:
                            raise RuntimeError(
                                f"Rucio query failed for entry {entry!r}: {exc}"
                            ) from exc

            for das_entry in das_entries:
                partial = partial_results.get(das_entry, {})
                for gkey in sorted(partial.keys()):
                    group_str = partial[gkey]
                    group_urls = [
                        u.strip() for u in group_str.split(",") if u.strip()
                    ]
                    new_urls = [u for u in group_urls if u not in seen_urls]
                    if new_urls:
                        seen_urls.update(new_urls)
                        all_urls.extend(new_urls)
                        rucio_groups.append(",".join(new_urls))
        else:
            self.publish_message(
                f"No Rucio path or explicit files for sample {name!r}; "
                "writing empty list."
            )

        # Normalise /store → //store in XRootD URLs
        fixed_urls: list[str] = []
        for url in all_urls:
            if "/store" in url and "//store" not in url:
                url = url.replace("/store", "//store")
            fixed_urls.append(url)

        payload: dict = {"sample": name, "files": fixed_urls}
        if rucio_groups:
            payload["groups"] = rucio_groups

        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(payload, fh, indent=2)
        self.publish_message(
            f"GetRucioFileList: {name} → {len(fixed_urls)} file(s), "
            f"{len(rucio_groups)} Rucio group(s)"
        )


