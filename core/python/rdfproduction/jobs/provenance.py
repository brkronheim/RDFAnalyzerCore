"""Build skim and histogram manifests from analysis and job provenance."""

from __future__ import annotations

from fnmatch import fnmatch
from typing import Any
from typing import Optional
import datetime
import hashlib
import json
import os
import re
import subprocess

from dataset_manifest import DatasetEntry
from dataset_manifest import DatasetManifest
from output_schema import CutflowSchema
from output_schema import DatasetManifestProvenance
from output_schema import HistogramAxisSpec
from output_schema import HistogramSchema
from output_schema import IntermediateArtifactSchema
from output_schema import MetadataSchema
from output_schema import OutputManifest
from output_schema import ProvenanceRecord
from output_schema import RegionDefinition
from submission_backend import ensure_xrootd_redirector
from submission_backend import read_config
import yaml
from . import staging as _job_staging

from .. import PYTHON_ROOT


_HERE = str(PYTHON_ROOT)


def _build_task_provenance(
    submit_config_path: Optional[str] = None,
    dataset_manifest_path: Optional[str] = None,
    extra_input_paths: Optional[list[str]] = None,
) -> ProvenanceRecord:
    """Collect a :class:`~output_schema.ProvenanceRecord` for the current task.

    Gathers whichever provenance fields are available in the current
    environment:

    * **framework_hash** – HEAD commit hash of the repository containing
      this file, obtained via ``git rev-parse HEAD`` (silently omitted when
      git is unavailable or the working tree is not inside a git repository).
    * **config_mtime** – UTC modification timestamp of *submit_config_path*
      in ISO 8601 format (omitted when the file does not exist).
    * **dataset_manifest_hash** – SHA-256 hex digest of *dataset_manifest_path*
      via :meth:`~dataset_manifest.DatasetManifest.file_hash` (omitted when
      the file does not exist or the hash cannot be computed).
    * **task_config_hash** – SHA-256 digest of the effective job config and
      referenced stitching weights, when those inputs are available.

    Parameters
    ----------
    submit_config_path:
        Path to the submit_config.yaml template for the current task.
    dataset_manifest_path:
        Path to the dataset manifest YAML file for the current task.
    extra_input_paths:
        Optional file inputs whose content should participate in cache identity.

    Returns
    -------
    ProvenanceRecord
        Best-effort provenance snapshot; fields that cannot be determined
        are left as ``None``.
    """
    framework_hash: Optional[str] = None
    try:
        result = subprocess.run(
            ["git", "-C", _HERE, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            framework_hash = result.stdout.strip() or None
    except Exception:
        pass

    config_mtime: Optional[str] = None
    if submit_config_path and os.path.isfile(submit_config_path):
        mtime = os.path.getmtime(submit_config_path)
        config_mtime = datetime.datetime.fromtimestamp(
            mtime, tz=datetime.timezone.utc
        ).isoformat()

    dataset_manifest_hash: Optional[str] = None
    if dataset_manifest_path and os.path.isfile(dataset_manifest_path):
        try:
            dataset_manifest_hash = DatasetManifest.file_hash(dataset_manifest_path)
        except Exception:
            pass

    task_config_hash: Optional[str] = None
    if (submit_config_path and os.path.isfile(submit_config_path)) or extra_input_paths:
        digest = hashlib.sha256()
        input_paths = list(extra_input_paths or [])
        if submit_config_path and os.path.isfile(submit_config_path):
            config_path = os.path.abspath(submit_config_path)
            digest.update(b"config-path\0" + config_path.encode("utf-8") + b"\0")
            with open(config_path, "rb") as config_fh:
                for chunk in iter(lambda: config_fh.read(1024 * 1024), b""):
                    digest.update(chunk)
            configured = read_config(config_path)
            stitch_weights = str(configured.get("stitchWeights", "")).strip()
            if stitch_weights:
                input_paths.append(stitch_weights)
        seen_paths: set[str] = set()
        for input_path in input_paths:
            if not input_path:
                continue
            resolved_path = (
                _job_staging._resolve_analysis_config_path(submit_config_path, input_path)
                if submit_config_path
                else input_path
            )
            normalized_path = os.path.abspath(resolved_path) if not str(resolved_path).startswith("root://") else str(resolved_path)
            if normalized_path in seen_paths:
                continue
            seen_paths.add(normalized_path)
            digest.update(b"input-path\0" + normalized_path.encode("utf-8") + b"\0")
            if os.path.isfile(normalized_path):
                with open(normalized_path, "rb") as input_fh:
                    for chunk in iter(lambda: input_fh.read(1024 * 1024), b""):
                        digest.update(chunk)
            else:
                # Remote URLs and missing optional files still affect the
                # identity by path; task setup separately validates required
                # inputs before execution.
                digest.update(b"unreadable-or-remote\0")
        task_config_hash = digest.hexdigest()

    return ProvenanceRecord(
        framework_hash=framework_hash,
        config_mtime=config_mtime,
        dataset_manifest_hash=dataset_manifest_hash,
        task_config_hash=task_config_hash,
    )


def _build_skim_manifest(
    dataset: DatasetEntry,
    submit_config_path: str,
    dataset_manifest_path: str,
    skim_output_file: str,
    extra_input_paths: Optional[list[str]] = None,
) -> OutputManifest:
    """Build an :class:`~output_schema.OutputManifest` for a skim artifact.

    Constructs the manifest with an :class:`~output_schema.IntermediateArtifactSchema`
    recording the artifact as a ``"preselection"`` kind, combined with
    :class:`~output_schema.DatasetManifestProvenance` capturing the dataset
    selection context and the provenance fields collected by
    :func:`_build_task_provenance`.

    Parameters
    ----------
    dataset:
        The :class:`~dataset_manifest.DatasetEntry` processed in this branch.
    submit_config_path:
        Path to the submit_config.yaml template file.
    dataset_manifest_path:
        Path to the dataset manifest YAML file.
    skim_output_file:
        Absolute path to the produced skim ROOT file.

    Returns
    -------
    OutputManifest
        Manifest suitable for passing to :func:`~output_schema.write_cache_sidecar`.
    """
    prov = _build_task_provenance(
        submit_config_path=submit_config_path,
        dataset_manifest_path=dataset_manifest_path,
        extra_input_paths=extra_input_paths,
    )

    manifest_hash: Optional[str] = None
    if os.path.isfile(dataset_manifest_path):
        try:
            manifest_hash = DatasetManifest.file_hash(dataset_manifest_path)
        except Exception:
            pass

    dataset_manifest_prov = DatasetManifestProvenance(
        manifest_path=dataset_manifest_path,
        manifest_hash=manifest_hash,
        resolved_entry_names=[dataset.name],
    )

    artifact = IntermediateArtifactSchema(
        artifact_kind="preselection",
        output_file=skim_output_file,
    )

    return OutputManifest(
        intermediate_artifacts=[artifact],
        framework_hash=prov.framework_hash,
        config_mtime=prov.config_mtime,
        task_config_hash=prov.task_config_hash,
        dataset_manifest_provenance=dataset_manifest_prov,
    )


def _read_histogram_config_entries(config_path: str) -> list[dict[str, str]]:
    """Parse a histogramConfig file in either text or YAML format."""
    if not config_path or not os.path.isfile(config_path):
        return []

    from native_config_io import read_config_records
    return read_config_records(config_path)


def _read_root_histogram_metadata(
    meta_file: str,
) -> tuple[list[str], Optional[dict[str, Any]]]:
    """Read histogram keys and any embedded sparse-first schema."""
    if not meta_file:
        return [], None
    is_remote_path = meta_file.startswith(
        ("root://", "xroot://", "/eos/", "/store/")
    )
    if not is_remote_path and not os.path.isfile(meta_file):
        return [], None

    try:
        import uproot  # type: ignore
    except ImportError:
        return [], None

    names: list[str] = []
    seen: set[str] = set()

    def _visit(directory, prefix: str = "") -> None:
        for key in directory.keys(cycle=False):
            try:
                obj = directory[key]
            except Exception:
                continue

            name = str(key).split(";", 1)[0]
            if hasattr(obj, "keys") and not (
                hasattr(obj, "axes") and hasattr(obj, "values")
            ):
                _visit(obj, f"{prefix}{name}/")
                continue

            if not hasattr(obj, "values"):
                continue

            full_name = f"{prefix}{name}"
            if full_name and full_name not in seen:
                names.append(full_name)
                seen.add(full_name)

    open_path = meta_file
    if meta_file.startswith(("root://", "/eos/", "/store/")):
        remote_open_path = ensure_xrootd_redirector(
            meta_file, "root://eosuser.cern.ch/"
        )
        if meta_file.startswith("root://") or not os.path.isfile(meta_file):
            open_path = remote_open_path
    with uproot.open(open_path) as root_file:
        try:
            embedded = json.loads(str(root_file["__rdf_histogram_schema_v1"]))
        except KeyError:
            embedded = None
        if isinstance(embedded, dict) and isinstance(
            embedded.get("histograms"), list
        ):
            directory = embedded.get("directory", "nd_histograms_v1")
            names = [
                f"{directory}/{entry['key']}"
                for entry in embedded["histograms"]
                if isinstance(entry, dict) and isinstance(entry.get("key"), str)
            ]
            return names, embedded
        _visit(root_file)
    return names, None


def _read_root_histogram_names(meta_file: str) -> list[str]:
    """Return deduplicated histogram names found recursively in *meta_file*."""
    return _read_root_histogram_metadata(meta_file)[0]


def _histogram_axis_labels_from_entries(
    entries: list[dict[str, str]],
    histogram_names: list[str],
    embedded_schema: Optional[dict[str, Any]] = None,
) -> dict[str, list[str]]:
    """Build axis-label metadata from parsed histogram config entries."""
    axis_labels: dict[str, list[str]] = {}
    if embedded_schema and isinstance(embedded_schema.get("histograms"), list):
        for histogram in embedded_schema["histograms"]:
            if not isinstance(histogram, dict):
                continue
            for axis in histogram.get("axes", []):
                if not isinstance(axis, dict):
                    continue
                role = axis.get("role")
                labels = axis.get("labels")
                if (
                    isinstance(role, str)
                    and isinstance(labels, list)
                    and labels
                    and role not in axis_labels
                ):
                    axis_labels[role] = labels

    if entries:
        first = entries[0]
        for role, key in (
            ("channel", "channelRegions"),
            ("control", "controlRegionRegions"),
            ("sample", "sampleCategoryRegions"),
        ):
            if role in axis_labels:
                continue
            raw = first.get(key, "")
            labels = [label.strip() for label in raw.split(",") if label.strip()]
            if labels:
                axis_labels[role] = labels

    if "systematic" not in axis_labels:
        systematic_labels = sorted(
            {
                match.group(1)
                for name in histogram_names
                for match in [re.search(r"_(.+(?:Up|Down))$", name)]
                if match
            }
        )
        axis_labels["systematic"] = ["Nominal"] + systematic_labels
    return axis_labels


def _infer_region_definitions(entries: list[dict[str, str]]) -> list[RegionDefinition]:
    """Synthesize manifest region definitions from histogram axis labels."""
    if not entries:
        return []

    first = entries[0]
    channel_labels = [
        label.strip() for label in first.get("channelRegions", "").split(",") if label.strip()
    ]
    control_labels = [
        label.strip()
        for label in first.get("controlRegionRegions", "").split(",")
        if label.strip()
    ]
    channel_var = first.get("channelVariable", "channel")
    control_var = first.get("controlRegionVariable", "control")

    if not channel_labels and not control_labels:
        return []

    regions: list[RegionDefinition] = []
    if channel_labels and control_labels:
        for channel_name in channel_labels:
            for control_name in control_labels:
                regions.append(
                    RegionDefinition(
                        name=f"{channel_name}/{control_name}",
                        filter_column=f"{channel_var}={channel_name};{control_var}={control_name}",
                        parent=channel_name,
                        description=(
                            f"Histogram region for channel '{channel_name}' and "
                            f"control category '{control_name}'."
                        ),
                    )
                )
        for channel_name in channel_labels:
            regions.append(
                RegionDefinition(
                    name=channel_name,
                    filter_column=f"{channel_var}={channel_name}",
                    description=f"Histogram channel '{channel_name}'.",
                )
            )
        return regions

    labels = control_labels or channel_labels
    variable = control_var if control_labels else channel_var
    return [
        RegionDefinition(
            name=label,
            filter_column=f"{variable}={label}",
            description=f"Histogram region '{label}'.",
        )
        for label in labels
    ]


def _initial_sample_metadata(dataset: DatasetEntry) -> dict[str, dict[str, Any]]:
    """Build default alias metadata from a dataset manifest entry."""
    metadata: dict[str, dict[str, Any]] = {
        dataset.name: {
            "alias_type": "dataset",
            "dataset_name": dataset.name,
            "dtype": dataset.dtype,
        }
    }
    for key in (
        "process",
        "group",
        "stitch_id",
        "sample_type",
        "year",
        "era",
        "campaign",
        "parent",
    ):
        value = getattr(dataset, key)
        if value is not None:
            metadata[dataset.name][key] = value
    return metadata


def _augment_sample_metadata_with_axis_labels(
    sample_metadata: dict[str, dict[str, Any]],
    axis_labels: dict[str, list[str]],
) -> None:
    """Add sample-axis aliases such as STXS template bins into sample metadata."""
    for alias in axis_labels.get("sample", []):
        if not alias:
            continue
        entry = sample_metadata.setdefault(
            alias,
            {
                "alias_type": "histogram_category",
                "source_axis": "sample",
            },
        )
        if "process" not in entry and "_" in alias:
            entry["process"] = alias.split("_", 1)[0]
        if "stxs_bin" not in entry and re.match(r"^[A-Za-z]+_\d+$", alias):
            entry["stxs_bin"] = alias


def _matches_manifest_selector(metadata: dict[str, Any], selector: dict[str, Any], alias: str) -> bool:
    """Return True when *metadata* satisfies a selector mapping."""
    if not selector:
        return True

    for key, expected in selector.items():
        if key == "pattern":
            patterns = expected if isinstance(expected, list) else [expected]
            if not any(isinstance(pattern, str) and fnmatch(alias, pattern) for pattern in patterns):
                return False
            continue

        if key == "tags_any":
            tags = metadata.get("tags", [])
            wanted = expected if isinstance(expected, list) else [expected]
            if not any(tag in tags for tag in wanted):
                return False
            continue

        if key == "tags_all":
            tags = metadata.get("tags", [])
            wanted = expected if isinstance(expected, list) else [expected]
            if any(tag not in tags for tag in wanted):
                return False
            continue

        actual = metadata.get(key)
        if isinstance(expected, list):
            if actual not in expected:
                return False
        else:
            if actual != expected:
                return False

    return True


def _resolve_manifest_samples(
    config_block: dict[str, Any],
    sample_metadata: dict[str, dict[str, Any]],
) -> list[str]:
    """Resolve explicit + selector-based sample aliases to a concrete list."""
    resolved: list[str] = []
    seen: set[str] = set()

    for sample in config_block.get("samples", []):
        if isinstance(sample, str) and sample and sample not in seen:
            resolved.append(sample)
            seen.add(sample)

    selector = config_block.get("select")
    if isinstance(selector, dict):
        for alias, metadata in sample_metadata.items():
            if alias in seen:
                continue
            if _matches_manifest_selector(metadata, selector, alias):
                resolved.append(alias)
                seen.add(alias)

    return resolved


def _resolve_manifest_fit_metadata(
    metadata_config_path: str,
    sample_metadata: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Load optional fit metadata config and resolve selectors to sample lists."""
    if not metadata_config_path or not os.path.isfile(metadata_config_path):
        return sample_metadata, {}, {}

    with open(metadata_config_path, "r") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        return sample_metadata, {}, {}

    merged_sample_metadata: dict[str, dict[str, Any]] = {
        alias: dict(metadata) for alias, metadata in sample_metadata.items()
    }

    for alias, metadata in (payload.get("sample_metadata") or {}).items():
        if not isinstance(alias, str) or not isinstance(metadata, dict):
            continue
        merged_sample_metadata.setdefault(alias, {}).update(metadata)

    sample_combinations: dict[str, dict[str, Any]] = {}
    for combo_name, combo_cfg in (payload.get("sample_combinations") or {}).items():
        if not isinstance(combo_name, str) or not isinstance(combo_cfg, dict):
            continue
        resolved_samples = _resolve_manifest_samples(combo_cfg, merged_sample_metadata)
        combination_cfg = {
            key: value for key, value in combo_cfg.items() if key != "select"
        }
        if resolved_samples:
            combination_cfg["samples"] = resolved_samples
        sample_combinations[combo_name] = combination_cfg

    processes: dict[str, dict[str, Any]] = {}
    for process_name, process_cfg in (payload.get("processes") or {}).items():
        if not isinstance(process_name, str) or not isinstance(process_cfg, dict):
            continue
        resolved_samples = _resolve_manifest_samples(process_cfg, merged_sample_metadata)
        concrete_cfg = {
            key: value for key, value in process_cfg.items() if key != "select"
        }
        if resolved_samples:
            concrete_cfg["samples"] = resolved_samples
        processes[process_name] = concrete_cfg

    return merged_sample_metadata, sample_combinations, processes


def _make_meta_output_name(save_file: str) -> str:
    """Match RootOutputSink's default meta ROOT file naming."""
    if not save_file:
        return ""
    stem, ext = os.path.splitext(save_file)
    if ext:
        return f"{stem}_meta{ext}"
    return save_file + "_meta.root"


def _build_histfill_manifest(
    dataset: DatasetEntry,
    submit_config_path: str,
    dataset_manifest_path: str,
    job_config_path: str,
    *,
    meta_file: Optional[str] = None,
) -> OutputManifest:
    """Build an OutputManifest describing histogram, metadata, and cutflow outputs."""
    prov = _build_task_provenance(
        submit_config_path=submit_config_path,
        dataset_manifest_path=dataset_manifest_path,
    )

    manifest_hash: Optional[str] = None
    if os.path.isfile(dataset_manifest_path):
        try:
            manifest_hash = DatasetManifest.file_hash(dataset_manifest_path)
        except Exception:
            pass

    dataset_manifest_prov = DatasetManifestProvenance(
        manifest_path=dataset_manifest_path,
        manifest_hash=manifest_hash,
        resolved_entry_names=[dataset.name],
    )

    job_config = read_config(job_config_path)
    save_file = job_config.get("saveFile", "")
    meta_file = meta_file or job_config.get("metaFile", "") or _make_meta_output_name(save_file)

    histogram_ref = job_config.get("histogramConfig", "")
    histogram_config_path = _job_staging._resolve_analysis_config_path(job_config_path, histogram_ref)
    if not os.path.isfile(histogram_config_path):
        histogram_config_path = _job_staging._resolve_analysis_config_path(submit_config_path, histogram_ref)
    histogram_entries = _read_histogram_config_entries(histogram_config_path)
    histogram_names, embedded_histogram_schema = _read_root_histogram_metadata(
        meta_file
    )
    axis_labels = _histogram_axis_labels_from_entries(
        histogram_entries, histogram_names, embedded_histogram_schema
    )

    sample_metadata = _initial_sample_metadata(dataset)
    _augment_sample_metadata_with_axis_labels(sample_metadata, axis_labels)

    fit_metadata_path = _job_staging._resolve_analysis_config_path(
        submit_config_path,
        job_config.get("manifestMetadataConfig", job_config.get("fitMetadataConfig", "")),
    )
    sample_metadata, sample_combinations, processes = _resolve_manifest_fit_metadata(
        fit_metadata_path,
        sample_metadata,
    )

    axes: list[HistogramAxisSpec] = []
    if histogram_entries:
        first = histogram_entries[0]
        role_specs = (
            ("channelVariable", "channelBins", "channelLowerBound", "channelUpperBound", "channel"),
            (
                "controlRegionVariable",
                "controlRegionBins",
                "controlRegionLowerBound",
                "controlRegionUpperBound",
                "control",
            ),
            (
                "sampleCategoryVariable",
                "sampleCategoryBins",
                "sampleCategoryLowerBound",
                "sampleCategoryUpperBound",
                "sample",
            ),
        )
        for var_key, bins_key, low_key, high_key, role in role_specs:
            if var_key not in first:
                continue
            try:
                axes.append(
                    HistogramAxisSpec(
                        variable=first[var_key],
                        bins=int(first[bins_key]),
                        lower_bound=float(first[low_key]),
                        upper_bound=float(first[high_key]),
                        label=role,
                    )
                )
            except Exception:
                continue

    manifest = OutputManifest(
        histograms=HistogramSchema(
            output_file=meta_file,
            histogram_names=histogram_names,
            axes=axes,
            axis_labels=axis_labels,
            sample_metadata=sample_metadata,
            sample_combinations=sample_combinations,
            processes=processes,
        ),
        metadata=MetadataSchema(output_file=meta_file),
        cutflow=CutflowSchema(output_file=meta_file),
        regions=_infer_region_definitions(histogram_entries),
        framework_hash=prov.framework_hash,
        config_mtime=prov.config_mtime,
        dataset_manifest_provenance=dataset_manifest_prov,
    )
    return manifest
