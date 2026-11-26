"""Resolve partition input lists and write the per-job configuration."""

from __future__ import annotations

from native_config_io import job_config_path
from pathlib import Path
from typing import Optional
import json
import os

from dataset_manifest import DatasetEntry
from dataset_manifest import DatasetManifest
from job_sizing import EntryCache
from job_sizing import event_budget
from job_sizing import load_profile
from job_sizing import profile_matches
from ..partitioning import _make_partitions
from submission_backend import ensure_xrootd_redirector
from submission_backend import write_config
from submission_backend import read_config
from run_bundle import find_bundle
import io_policy as _io_policy
from . import staging as _job_staging
import rdfproduction.partitioning as _partitioning


def _dataset_job_metadata(dataset: DatasetEntry) -> dict[str, str]:
    """Return per-dataset manifest metadata to inject into a job config.

    The skim workflow now materializes manifest metadata directly into every
    per-job submit config so analysis code can query sample attributes without
    reparsing the manifest. ``dtype`` carries the framework-level ``mc`` /
    ``data`` classification, while ``type`` is reserved for the legacy numeric
    sample code and is derived from ``sample_type`` or ``stitch_id`` when
    available.
    """
    metadata: dict[str, str] = {
        "sample": dataset.name,
    }

    def _store(key: str, value: object) -> None:
        if value is None:
            return
        if isinstance(value, list):
            if not value:
                return
            metadata[key] = ",".join(str(item) for item in value)
            return
        metadata[key] = str(value)

    _store("name", dataset.name)
    _store("year", dataset.year)
    _store("era", dataset.era)
    _store("campaign", dataset.campaign)
    _store("process", dataset.process)
    _store("group", dataset.group)
    _store("stitch_id", dataset.stitch_id)
    _store("sample_type", dataset.sample_type)
    _store("xsec", dataset.xsec)
    _store("filter_efficiency", dataset.filter_efficiency)
    _store("kfac", dataset.kfac)
    _store("energy", dataset.energy)
    _store("extra_scale", dataset.extra_scale)
    _store("sum_weights", dataset.sum_weights)
    _store("genWeightMode", dataset.generator_weight_mode)
    _store("dtype", dataset.dtype)
    _store("parent", dataset.parent)
    _store("das", dataset.das)

    if dataset.filter_efficiency is not None:
        metadata["filterEfficiency"] = str(dataset.filter_efficiency)
    if dataset.extra_scale is not None:
        metadata["extraScale"] = str(dataset.extra_scale)
    if dataset.sum_weights is not None:
        metadata["norm"] = str(dataset.sum_weights)

    legacy_type = None
    if dataset.sample_type is not None:
        legacy_type = dataset.sample_type
    elif dataset.stitch_id is not None:
        legacy_type = dataset.stitch_id
    elif dataset.dtype:
        legacy_type = dataset.dtype
    if legacy_type is not None:
        metadata["type"] = str(legacy_type)

    # Forward analysis-specific dataset metadata (for example ptcut) into
    # per-job submit configs. Standard metadata keys above win on collisions.
    if isinstance(dataset.extra, dict):
        for key, value in dataset.extra.items():
            if key in metadata or key is None:
                continue
            _store(str(key), value)

    return metadata


def _resolve_output_destination_base(
    template_config_path: str,
    config_dict: dict[str, str],
    output_directory: str = "",
) -> str:
    """Resolve an EOS/XRootD destination for staged HTCondor skim outputs."""
    save_dir = str(
        output_directory or config_dict.get("saveDirectory", "") or ""
    ).strip()
    if not save_dir:
        return ""
    if (
        save_dir.startswith("root://")
        or save_dir.startswith("/eos/")
    ):
        return save_dir
    if os.path.isabs(save_dir):
        resolved_save_dir = save_dir
    else:
        template_dir = os.path.dirname(os.path.abspath(template_config_path))
        resolved_save_dir = os.path.abspath(os.path.join(template_dir, save_dir))
    raise RuntimeError(
        "HTCondor stage-out requires an EOS destination (/eos/...) or an explicit "
        f"root:// URL; saveDirectory {save_dir!r} resolves to local path "
        f"{resolved_save_dir!r}. Pass --output-directory with a remote destination."
    )


def _rewrite_job_output_destinations(config_path: str, final_output_dir: str) -> Path:
    """Rewrite saveDirectory/saveFile/metaFile in a per-job config copy."""
    config_dict = read_config(config_path)
    config_dict["saveDirectory"] = final_output_dir
    job_dir = os.path.basename(os.path.dirname(config_path))
    job_index = job_dir.split("job_", 1)[1] if job_dir.startswith("job_") else job_dir
    config_dict["saveFile"] = os.path.join(final_output_dir, f"output_{job_index}.root")
    config_dict["metaFile"] = os.path.join(final_output_dir, f"meta_{job_index}.root")

    from native_config_io import runtime_yaml_path
    target = runtime_yaml_path(config_path)
    _write_config_file(target, config_dict)
    return target


def _stitch_job_overrides(task: object) -> dict[str, str]:
    """Return optional stitching keys to inject into prepared job configs."""
    overrides: dict[str, str] = {}
    stitch_mode = str(getattr(task, "stitch_mode", "") or "").strip()
    stitch_weights = str(getattr(task, "stitch_weights", "") or "").strip()
    if stitch_mode:
        overrides["stitchMode"] = stitch_mode
    if stitch_weights:
        overrides["stitchWeights"] = stitch_weights
    return overrides


def _write_job_config(
    job_dir: str,
    template_config_path: str,
    dataset: DatasetEntry,
    output_dir: str,
    manifest_lumi: Optional[float] = None,
    extra_overrides: Optional[dict[str, str]] = None,
    stage_shared_inputs_locally: bool = True,
    template_config: Optional[dict[str, str]] = None,
    runtime_payload_sources: Optional[list[str]] = None,
    prepared_payloads: Optional[dict[str, bytes]] = None,
) -> None:
    """Create a ``submit_config.yaml`` inside *job_dir* for *dataset*.

    Reads the template at *template_config_path*, overrides the dataset-
    specific keys (``fileList``, ``saveFile``, ``metaFile``,
    ``saveDirectory``) and injects manifest metadata (including ``dtype``,
    ``stitch_id``, ``sample_type``, ``process``, ``group``, etc.) into the
    main per-job config. ``type`` is reserved for the legacy numeric sample
    code and is derived from ``sample_type`` or ``stitch_id`` when present.
    Referenced auxiliary config files (``*.txt``, ``*.yaml``) are copied from
    the template directory into *job_dir* and the final config is written.

    Parameters
    ----------
    job_dir:
        Directory to write ``submit_config.yaml`` into (created if absent).
    template_config_path:
        Path to the user-provided submit config template.
    dataset:
        :class:`~dataset_manifest.DatasetEntry` whose files and metadata
        are used to fill in the dataset-specific fields.
    output_dir:
        Directory where the analysis executable should write its output
        ROOT files for this dataset.
    extra_overrides:
        Additional key-value overrides applied after the standard ones.
    """
    if template_config is None:
        template_config = read_config(template_config_path)
    template_dir = os.path.dirname(os.path.abspath(template_config_path))

    job_config = dict(template_config)
    bundle = find_bundle(template_config_path)
    if bundle:
        job_config["runBundleId"] = json.loads((bundle / "bundle_manifest.json").read_text())["bundle_id"]

    # ---- file list ----------------------------------------------------------
    if dataset.files:
        file_list = [f.strip() for f in dataset.files if f and f.strip()]
    elif dataset.das:
        file_list = [dataset.das.strip()]
    else:
        file_list = []

    # Ensure each entry is accessible via XRootD redirector when appropriate.
    # Do not modify local filenames (input_*.root) or other non-EOS paths.
    redirector = (
        str(template_config.get("xrootdRedirector", "") or "").strip()
        or str(template_config.get("redirector", "") or "").strip()
    )

    normalized_files = []
    for f in file_list:
        if not f:
            continue
        if f.startswith("/") or f.startswith("store/") or f.startswith("root://"):
            if redirector:
                normalized_files.append(ensure_xrootd_redirector(f, redirector))
            else:
                normalized_files.append(ensure_xrootd_redirector(f))
        else:
            normalized_files.append(f)

    if normalized_files:
        job_config["fileList"] = ",".join(normalized_files)

    # ---- output paths -------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    job_config["saveFile"] = os.path.join(output_dir, "skim.root")
    job_config["metaFile"] = os.path.join(output_dir, "meta.root")
    job_config["saveDirectory"] = output_dir

    # The caller may provide a new job directory; create it before staging
    # auxiliary inputs and writing submit_config.yaml.
    os.makedirs(job_dir, exist_ok=True)

    # ---- sample identity / manifest metadata --------------------------------
    job_config.update(_dataset_job_metadata(dataset))
    if manifest_lumi is not None:
        job_config["lumi"] = str(manifest_lumi)

    # Data samples do not carry MC-only generator weights.
    _job_staging._remove_mc_generator_config_for_data(job_config)

    # ---- caller-supplied overrides ------------------------------------------
    if extra_overrides:
        job_config.update(extra_overrides)

    # ---- copy referenced auxiliary files ------------------------------------
    if stage_shared_inputs_locally:
        staging_config = dict(template_config)
        if extra_overrides:
            staging_config.update(extra_overrides)
        copied_payloads = _job_staging._copy_analysis_config_files(
            template_config_path=template_config_path,
            config_dict=staging_config,
            destination_dir=job_dir,
        )
        _job_staging._copy_analysis_aux_dir(template_config_path, job_dir)
        for key, val in list(job_config.items()):
            if key.startswith("__"):
                continue
            if isinstance(val, str) and (
                val.endswith(".txt")
                or val.endswith(".yaml")
                or val.endswith(".yml")
                or val.endswith(".json")
                or val.endswith(".json.gz")
                or val.endswith(".onnx")
            ):
                basename = _job_staging._asset_name(val, staging_config)
                if basename in copied_payloads:
                    job_config[key] = basename
    else:
        _job_staging._ensure_local_analysis_runtime_layout(
            template_config_path,
            job_config,
            job_dir,
            payload_sources=runtime_payload_sources,
            prepared_payloads=prepared_payloads,
        )
        _job_staging._normalize_shared_config_references(
            template_config_path,
            job_config,
            staged_payload_names=(
                {_job_staging._asset_name(src, job_config) for src in runtime_payload_sources}
                if runtime_payload_sources is not None
                else None
            ),
        )

    # ---- write the final config file ----------------------------------------
    config_path = os.path.join(job_dir, "submit_config.yaml")
    _write_config_file(config_path, job_config)


def _extract_lfn_for_redirect(url: str) -> str:
    """Extract the bare LFN from an XRootD URL for site_redirectors lookup.

    Handles both regular XRootD URLs (``root://host//store/...``) and
    site-specific test redirector URLs
    (``root://xrootd-cms.infn.it//store/test/xrootd/<site>//store/...``).
    Returns the logical file name starting with ``/store/`` or ``/eos/``,
    or the original *url* if it is not an XRootD URL.
    """
    import re as _re
    if not url.startswith("root://"):
        return url
    # Strip the server component to get the path.
    m = _re.match(r"root://[^/]+/+(.*)", url)
    if not m:
        return url
    path = "/" + m.group(1).lstrip("/")
    # For site-specific test redirectors the path is
    # /store/test/xrootd/<site>//store/... — extract the actual LFN.
    test_m = _re.match(_io_policy.SITE_PATH_PATTERN, path)
    if test_m:
        path = "/" + test_m.group(1).lstrip("/")
    return path


def _select_test_input_url(input_url: str, job_dir: str) -> str:
    """Select a readable Rucio replica URL for the local preflight job."""
    config_path = str(job_config_path(job_dir))
    if os.path.isfile(config_path):
        config = read_config(config_path)
        redirector = str(
            config.get("xrootdRedirector", "") or config.get("redirector", "")
        ).strip()
        if redirector:
            return ensure_xrootd_redirector(
                _extract_lfn_for_redirect(input_url), redirector
            )

    redirectors_path = os.path.join(job_dir, "site_redirectors.json")
    if not os.path.isfile(redirectors_path):
        return input_url

    try:
        with open(redirectors_path) as fh:
            redirector_map = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return input_url

    lfn = _extract_lfn_for_redirect(input_url)
    candidates = redirector_map.get(lfn, [])
    if not candidates:
        return input_url

    try:
        from xrootd_site_selector import select_best_url  # type: ignore

        selected = select_best_url(lfn, redirectors=list(candidates))
        if selected and selected.startswith("root://") and selected != input_url:
            return selected
    except Exception:
        pass

    # Rucio only supplies replicas that exist at the listed sites. Use the
    # first candidate when probing is unavailable or inconclusive.
    redirector = str(candidates[0]).rstrip("/")
    separator = "/" if _io_policy.SITE_REDIRECTOR_MARKER in redirector else "//"
    return f"{redirector}{separator}{lfn.lstrip('/')}"


def _write_config_file(config_path: str, config: dict[str, str]) -> None:
    """Write a key/value analysis config while preserving internal-key rules."""
    write_config({key: value for key, value in config.items() if not key.startswith("__")}, config_path)


def _plan_file_source_jobs(
    *,
    dataset_manifest_path: str,
    file_list_dir: str,
    partition: str,
    files_per_job: int,
    entries_per_job: int,
    jobs_dir: str,
    outputs_dir: str,
    manifest: Optional[DatasetManifest] = None,
    partition_profile: str = "",
    input_metadata_cache: str = "",
    tree_name: str = "Events",
) -> list[dict[str, object]]:
    """Plan only manifest datasets, allowing reuse of a larger discovery cache."""
    job_plans: list[dict[str, object]] = []
    missing_datasets: list[str] = []
    if manifest is None:
        manifest = DatasetManifest.load(dataset_manifest_path)
    manifest_entries = {entry.name: entry for entry in manifest.datasets}
    discovered_datasets: set[str] = set()
    profile = load_profile(partition_profile if partition_profile != "none" else "")
    if profile and partition not in {"auto", "events", "entry_range"}:
        raise ValueError("--partition-profile requires --partition auto, events or entry_range")
    entry_cache = EntryCache(input_metadata_cache or None)

    for json_path in sorted(Path(file_list_dir).glob("*.json")):
        if json_path.name.endswith(".perf.json"):
            continue

        with open(json_path) as fh:
            payload = json.load(fh)

        dataset_name = payload.get("sample", json_path.stem)
        dataset_entry = manifest_entries.get(str(dataset_name))
        if dataset_entry is None:
            # A reused cache can contain other eras or analyses. Such entries
            # must never become jobs with fallback MC identity/calibrations.
            continue
        if dataset_entry.name in discovered_datasets:
            raise RuntimeError(
                f"Duplicate file-list discovery payload for dataset {dataset_entry.name!r} "
                f"in {file_list_dir!r}; remove the ambiguous cache entry before preparation."
            )
        discovered_datasets.add(dataset_entry.name)
        all_files = payload.get("files", [])
        groups = payload.get("groups", [])
        # Per-file redirector list from Rucio discovery (may be absent for
        # non-Rucio sources or explicit file lists).
        site_redirectors: dict[str, list[str]] = payload.get("site_redirectors", {})

        # Discovery groups retain their byte-size boundaries, but must not
        # override explicit file/event policies or a tighter file-count cap.
        group_files = [[u.strip() for u in str(g).split(",") if u.strip()]
                       for g in groups if g]
        flat_files = sorted({u.strip() for u in (all_files or [u for g in group_files for u in g])
                             if u.strip()})
        if not flat_files:
            missing_datasets.append(str(dataset_name))
            continue
        if group_files and set(flat_files) != {u for g in group_files for u in g}:
            raise ValueError(f"Discovery files/groups disagree for {dataset_name}")
        budget = entries_per_job
        mode = ("events" if profile_matches(profile, dataset_entry) else "file_group") if partition == "auto" else partition
        if mode in {"events", "entry_range"}:
            budget = event_budget(profile, dataset_entry, entries_per_job)
            entry_cache.populate(flat_files, _partitioning._query_tree_entries, tree_name)
        if groups and mode in {"file_group", "events"}:
            partitions = []
            seen = set()
            for urls in group_files:
                if seen.intersection(urls) or len(set(urls)) != len(urls):
                    raise ValueError(f"Duplicate files in discovery groups for {dataset_name}")
                seen.update(urls)
                partitions.extend(_make_partitions(
                    urls=urls, mode=mode, files_per_job=files_per_job,
                    entries_per_job=budget, entry_cache=entry_cache, tree_name=tree_name))
        else:
            partitions = _make_partitions(
                urls=flat_files,
                mode=mode,
                files_per_job=files_per_job,
                entries_per_job=budget,
                entry_cache=entry_cache,
                tree_name=tree_name,
            )

        dtype = dataset_entry.dtype
        lumi = manifest.lumi_for(dataset_entry.year, dataset_entry.era)

        for sub_idx, part in enumerate(partitions):
            # Build per-job site_redirectors: only include LFNs in this partition.
            job_lfns = [
                _extract_lfn_for_redirect(u.strip())
                for u in str(part["files"]).split(",")
                if u.strip()
            ]
            job_site_redirectors = {
                lfn: site_redirectors[lfn]
                for lfn in job_lfns
                if lfn in site_redirectors
            }
            job_plans.append({
                "dataset_name": dataset_name,
                "job_dir": os.path.join(jobs_dir, dataset_name, f"job_{sub_idx}"),
                "out_dir": os.path.join(outputs_dir, dataset_name, f"job_{sub_idx}"),
                "files": part["files"],
                "first_entry": part.get("first_entry", 0),
                "last_entry": part.get("last_entry", 0),
                "planned_entries": part.get("planned_entries"),
                "dtype": dtype,
                "lumi": lumi,
                "site_redirectors": job_site_redirectors,
            })

    undiscovered = sorted(set(manifest_entries) - discovered_datasets)
    if undiscovered:
        raise RuntimeError(
            "File-list discovery is missing payloads for manifest dataset(s): "
            f"{', '.join(undiscovered)}. Complete discovery before preparing skim jobs."
        )
    if missing_datasets:
        missing_display = ", ".join(sorted(set(missing_datasets)))
        raise RuntimeError(
            "File-list discovery returned empty payloads for the following dataset(s): "
            f"{missing_display}. Re-run the file-list task and inspect the corresponding "
            "JSON outputs before preparing skim jobs."
        )

    return job_plans
