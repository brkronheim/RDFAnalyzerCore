"""
Generic Rucio file-discovery law tasks.

This module replaces the CMS-NANO-specific ``GetNANOFileList`` with the
generic :class:`GetRucioFileList`, which can query any Rucio instance for
file replicas given a dataset manifest or legacy sample-config file.

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

The output directory prefix is ``rucioFileList_{name}/`` (canonical) or
``nanoFileList_{name}/`` when the ``--file-source nano`` backward-compatible
alias is used in :class:`~analysis_tasks.SkimTask`.

Usage
-----
::

    source env.sh

    # Discover files via Rucio and write per-dataset JSON lists
    law run GetRucioFileList \\
        --submit-config analyses/myAnalysis/cfg/submit_config.txt \\
        --name myRun --x509 /tmp/x509 --exe build/bin/myanalysis

    # Chain into SkimTask using the canonical 'rucio' source
    law run SkimTask \\
        --submit-config analyses/myAnalysis/cfg/skim_config.txt \\
        --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
        --name mySkimRun \\
        --file-source rucio --file-source-name myRun \\
        --exe build/bin/myanalysis

    # Backward-compatible 'nano' source alias also works
    law run SkimTask ... --file-source nano --file-source-name myRun
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore
from requests.exceptions import ChunkedEncodingError, RequestException
import urllib3

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

# Default XRootD redirector used when Rucio does not supply a site-specific one
RUCIO_REDIRECTOR = "root://cms-xrd-global.cern.ch/"

# Rucio needs the default configuration → taken from CMS cvmfs defaults
if "RUCIO_HOME" not in os.environ:
    os.environ["RUCIO_HOME"] = "/cvmfs/cms.cern.ch/rucio/current"


# ---------------------------------------------------------------------------
# Low-level Rucio helpers
# ---------------------------------------------------------------------------


def _get_proxy_path() -> str:
    """Return the path to the active VOMS proxy, or raise if it is missing/expired."""
    import subprocess
    try:
        subprocess.run(
            "voms-proxy-info -exists -valid 0:20", shell=True, check=True
        )
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "VOMS proxy expired or missing: "
            "run `voms-proxy-init -voms cms -rfc --valid 168:0`"
        )
    import subprocess as _sp
    return _sp.check_output(
        "voms-proxy-info -path", shell=True, text=True
    ).strip()


def _get_rucio_client(proxy: str | None = None):
    """Return an authenticated Rucio client.

    Lazily imports ``rucio.client.Client`` so environments without Rucio
    installed can still load this module for testing.
    """
    from rucio.client import Client  # type: ignore

    try:
        if not proxy:
            proxy = _get_proxy_path()
        return Client()
    except Exception as e:
        raise RuntimeError(f"Cannot create Rucio client: {e}") from e


# Generic CMS XRootD redirectors used as fallbacks in the per-file redirector list.
_CMS_FALLBACK_REDIRECTORS: list[str] = [
    #"root://cmsxrootd.fnal.gov/",
    #"root://xrootd-cms.infn.it/",
    #"root://cms-xrd-global.cern.ch/",
]


def _query_rucio(
    dataset_path: str,
    file_split_gb: float,
    whitelist: list[str],
    blacklist: list[str],
    site_override: str,
    client,
    max_files_per_group: int = 50,
) -> dict:
    """Query Rucio for replicas of *dataset_path* and return grouped URL strings
    together with a complete per-file redirector list for site-aware probing on
    the worker node.

    Files are grouped so that each group contains at most *max_files_per_group*
    files and at most *file_split_gb* GB of data.  The URL stored in each group
    uses the generic global redirector so that it is always reachable as a
    fallback.  The per-file redirector list (``"site_redirectors"`` key) contains
    ALL available site-specific redirectors for every file, plus the three generic
    CMS redirectors as fallbacks.  Workers use this list to probe each candidate
    site in parallel and select the fastest one.

    Parameters
    ----------
    dataset_path:
        Rucio dataset identifier, e.g. ``/DYJetsToLL_M-50/Run2-MiniAOD/MINIAODSIM``.
    file_split_gb:
        Maximum GB of data per job group.
    whitelist:
        Preferred site names.  No longer used for URL selection (all available
        sites are now collected), but kept for API compatibility.
    blacklist:
        Site names to skip when collecting site-specific redirectors.
    site_override:
        If non-empty, force this specific site for all replicas.
    client:
        An authenticated ``rucio.client.Client`` instance.
    max_files_per_group:
        Hard cap on the number of files in a single group.

    Returns
    -------
    dict
        ``{"groups": dict[int, str], "site_redirectors": dict[str, list[str]]}``

        * ``"groups"`` maps ``group_index`` to a comma-separated string of
          XRootD URLs (using the global redirector as a reliable fallback).
        * ``"site_redirectors"`` maps each bare LFN to the ordered list of
          redirectors that should be probed on the worker node.  Site-specific
          redirectors (from Rucio ``states``) come first, followed by the
          generic CMS fallbacks so that the worker always has a usable option.
    """
    if not dataset_path or not isinstance(dataset_path, str) or not dataset_path.startswith("/"):
        print(f"Warning: invalid Rucio dataset path: {dataset_path!r} – skipping")
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
        except (ChunkedEncodingError, RequestException, urllib3.exceptions.ProtocolError) as e:
            if attempt < max_retries:
                print(
                    f"Warning: Rucio attempt {attempt}/{max_retries} failed: {e}. "
                    f"Retrying in {backoff:.1f}s…"
                )
                time.sleep(backoff)
                backoff *= 2
            else:
                print(
                    f"Error: Rucio failed for {dataset_path!r} after "
                    f"{max_retries} attempts: {e}"
                )
                return {"groups": {}, "site_redirectors": {}}
        except Exception as e:
            print(f"Error: unexpected Rucio error for {dataset_path!r}: {e}")
            return {"groups": {}, "site_redirectors": {}}

    if not replicas:
        print(f"Warning: no replicas found for {dataset_path!r}")
        return {"groups": {}, "site_redirectors": {}}

    groups: dict[int, str] = {}
    group_sizes: dict[int, float] = {}
    group_counts: dict[int, int] = {}
    per_file_redirectors: dict[str, list[str]] = {}
    running_size = 0.0
    running_files = 0
    group = 0

    for filedata in replicas:
        size_gb = filedata.get("bytes", 0) * 1e-9

        # Strip any existing redirector from the file name so we always work
        # with a bare LFN.  Match one or more slashes after the host to handle
        # both "root://host/store/..." and "root://host//store/..." forms.
        file_name = filedata["name"]
        if file_name.startswith("root://"):
            m = re.match(r"root://[^/]+/+(.*)", file_name)
            if m:
                file_name = "/" + m.group(1)

        # Collect ALL available site-specific redirectors for this file so the
        # worker can probe each one and pick the fastest.  Every AVAILABLE
        # non-Tape site (not on the blacklist) contributes a site-specific
        # test redirector of the form:
        #   root://xrootd-cms.infn.it//store/test/xrootd/<site>/
        # The generic CMS redirectors are appended as fallbacks so the worker
        # can always reach the file even if all site-specific probes fail.
        states = filedata.get("states", {})
        site_redirectors: list[str] = []
        seen_redirectors: set[str] = set()
        for site_key, state_val in states.items():
            if state_val != "AVAILABLE" or "Tape" in site_key:
                continue
            site = site_key.replace("_Disk", "")
            # Skip blacklisted sites.
            if blacklist and any(b.lower() in site.lower() for b in blacklist):
                continue
            redir = f"root://xrootd-cms.infn.it//store/test/xrootd/{site}/"
            if redir not in seen_redirectors:
                site_redirectors.append(redir)
                seen_redirectors.add(redir)
        # Append the generic CMS redirectors as fallbacks (deduplicated).
        for fb in _CMS_FALLBACK_REDIRECTORS:
            if fb not in seen_redirectors:
                site_redirectors.append(fb)
                seen_redirectors.add(fb)

        per_file_redirectors[file_name] = site_redirectors

        running_size += size_gb
        running_files += 1
        if running_size > file_split_gb or running_files >= max_files_per_group:
            group += 1
            running_size = size_gb
            running_files = 1

        # The URL stored in groups uses the global redirector as a reliable
        # fallback.  The worker will replace it with the fastest available URL
        # after probing the per-file redirector list.
        url = ensure_xrootd_redirector(file_name, RUCIO_REDIRECTOR)
        if group in groups:
            groups[group] += "," + url
            group_counts[group] += 1
            group_sizes[group] += round(size_gb, 1)
        else:
            groups[group] = url
            group_counts[group] = 1
            group_sizes[group] = round(size_gb, 1)

    return {"groups": groups, "site_redirectors": per_file_redirectors}


def _get_sample_list(config_file: str):
    """Parse *config_file* and return ``(sampleDict, baseDirs, lumi, WL, BL)``.

    Accepts both the legacy ``key=value`` text format **and** the YAML dataset
    manifest format (detected by ``.yaml``/``.yml`` extension).
    """
    ext = os.path.splitext(config_file)[1].lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        return (
            manifest.to_legacy_sample_dict(),
            [],
            manifest.lumi,
            manifest.whitelist,
            manifest.blacklist,
        )

    config_dict: dict[str, dict] = {}
    base_dirs: list[str] = []
    lumi = 1.0
    whitelist: list[str] = []
    blacklist: list[str] = []
    with open(config_file) as fh:
        for line in fh:
            line = line.split("#")[0].strip().split()
            inner: dict[str, str] = {}
            for pair in line:
                parts = pair.split("=")
                if len(parts) == 2:
                    inner[parts[0]] = parts[1]
            if "name" in inner:
                config_dict[inner["name"]] = inner
            elif "prefix_cern" in inner:
                base_dirs.append(inner["prefix_cern"])
            elif "lumi" in inner:
                lumi = float(inner["lumi"])
            elif "WL" in inner:
                whitelist = inner["WL"].split(",")
            elif "BL" in inner:
                blacklist = inner["BL"].split(",")
    return config_dict, base_dirs, lumi, whitelist, blacklist


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

    The legacy ``--file-source nano`` alias also resolves to this task's
    output directory (``nanoFileList_{name}/``) for backward compatibility;
    see :class:`GetNANOFileList`.
    """

    task_namespace = ""

    def create_branch_map(self) -> dict[int, str]:
        sample_list, _, _, _, _ = _get_sample_list(self._sample_config)
        if self.dataset:
            if self.dataset not in sample_list:
                raise ValueError(
                    f"Dataset {self.dataset!r} not found in sample config. "
                    f"Available: {sorted(sample_list.keys())}"
                )
            return {0: self.dataset}
        return {i: key for i, key in enumerate(sorted(sample_list.keys()))}

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
        sample_list, _, lumi, whitelist, blacklist = _get_sample_list(
            self._sample_config
        )
        sample = sample_list[sample_key]

        name = sample["name"]
        das_path = sample.get("das", "")
        site_override = sample.get("site", "")

        all_urls: list[str] = []
        seen_urls: set[str] = set()
        rucio_groups: list[str] = []
        all_site_redirectors: dict[str, list[str]] = {}

        # Explicit file list overrides Rucio discovery
        explicit_files = [
            f.strip()
            for f in sample.get("fileList", "").split(",")
            if f.strip()
        ]
        if explicit_files:
            for url in explicit_files:
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(ensure_xrootd_redirector(url))
        elif das_path:
            try:
                client = _get_rucio_client()
            except Exception as e:
                raise RuntimeError(f"Cannot open Rucio client: {e}") from e

            das_entries = [d.strip() for d in das_path.split(",") if d.strip()]

            if len(das_entries) == 1:
                partial_results = {
                    das_entries[0]: _query_rucio(
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
                            _query_rucio,
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

            all_site_redirectors: dict[str, list[str]] = {}
            for das_entry in das_entries:
                result = partial_results.get(das_entry, {"groups": {}, "site_redirectors": {}})
                partial_groups = result.get("groups", {})
                partial_site_redir = result.get("site_redirectors", {})
                for gkey in sorted(partial_groups.keys()):
                    group_str = partial_groups[gkey]
                    group_urls = [
                        u.strip() for u in group_str.split(",") if u.strip()
                    ]
                    new_urls = [u for u in group_urls if u not in seen_urls]
                    if new_urls:
                        seen_urls.update(new_urls)
                        all_urls.extend(new_urls)
                        rucio_groups.append(",".join(new_urls))
                all_site_redirectors.update(partial_site_redir)
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
        if all_site_redirectors:
            payload["site_redirectors"] = all_site_redirectors

        Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output().path, "w") as fh:
            json.dump(payload, fh, indent=2)
        self.publish_message(
            f"GetRucioFileList: {name} → {len(fixed_urls)} file(s), "
            f"{len(rucio_groups)} Rucio group(s)"
        )


# ---------------------------------------------------------------------------
# GetNANOFileList – backward-compatible alias that writes to nanoFileList_*
# ---------------------------------------------------------------------------


class _NanoCompatMixin(RucioMixin):
    """Overrides ``_file_list_dir`` to use the legacy ``nanoFileList_`` prefix.

    This keeps the output directory layout identical to the old
    ``GetNANOFileList`` task so that existing ``SkimTask --file-source nano``
    workflows continue to work without modification.
    """

    @property
    def _file_list_dir(self) -> str:  # type: ignore[override]
        return os.path.join(WORKSPACE, f"nanoFileList_{self.name}")


class GetNANOFileList(_NanoCompatMixin, law.LocalWorkflow):
    """Backward-compatible alias for :class:`GetRucioFileList`.

    Writes to ``nanoFileList_{name}/`` so that existing workflows using
    ``--file-source nano`` continue to work unchanged.

    New workflows should prefer ``GetRucioFileList`` with
    ``--file-source rucio``.
    """

    task_namespace = ""

    # Delegate all logic to GetRucioFileList's implementation
    create_branch_map = GetRucioFileList.create_branch_map
    output = GetRucioFileList.output

    def run(self):
        task_label = f"GetNANOFileList[branch={self.branch}]"
        with PerformanceRecorder(task_label) as rec:
            self._run_impl()
        rec.save(perf_path_for(self.output().path))

    _run_impl = GetRucioFileList._run_impl
