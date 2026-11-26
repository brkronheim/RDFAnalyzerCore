"""Check stitched metadata and render comparisons for configured histograms."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union
from typing import cast
import yaml
from dataset_manifest import DatasetManifest
from dataset_manifest import DatasetEntry
from derivation_correctionlib import _normalize_sample_names as _shared_normalize_sample_names
from stitching_plotting import plot_ratio_comparison as _plot_ratio_comparison
from . import stitching_derivation


def _validate_stitch_monitor_summary(run_name: str) -> None:
    """Refuse validation when the application monitor recorded failed jobs."""
    summary_path = Path(stitching_derivation.WORKSPACE) / f"skimRun_{run_name}" / "all_outputs_verified.txt"
    if not summary_path.is_file():
        return

    summary: dict[str, str] = {}
    with summary_path.open() as fh:
        for raw_line in fh:
            key, separator, value = raw_line.partition("=")
            if separator:
                summary[key.strip()] = value.strip()

    try:
        permanent_failures = int(summary.get("perm_fail", "0"))
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid permanent-failure count in stitching monitor summary "
            f"'{summary_path}'."
        ) from exc

    if permanent_failures <= 0:
        return

    failed_jobs = summary.get("perm_failed_jobs", "")
    details = f" Failed jobs: {failed_jobs}." if failed_jobs else ""
    raise RuntimeError(
        f"Stitching application run '{run_name}' has {permanent_failures} "
        f"permanently failed job(s); refusing to validate partial outputs. "
        f"See '{summary_path}'.{details}"
    )


def _stitch_output_meta_files(
    run_name: str,
    dataset_manifest_path: str,
) -> dict[str, list[str]]:
    """Return every application metadata file grouped by manifest sample."""
    _validate_stitch_monitor_summary(run_name)
    manifest = DatasetManifest.load(dataset_manifest_path)
    meta_files: dict[str, list[str]] = {}
    missing_samples: list[str] = []

    for dataset in manifest.datasets:
        paths = stitching_derivation._stitch_meta_files_for_sample(run_name, dataset.name)
        if paths:
            meta_files[dataset.name] = paths
        else:
            missing_samples.append(dataset.name)

    if missing_samples:
        raise RuntimeError(
            f"Stitching application run '{run_name}' has no meta.root output for: "
            + ", ".join(missing_samples)
        )
    return meta_files


def _root_key_basename(key: object) -> str:
    """Return a ROOT object key without its directory or cycle suffix."""
    return str(key).split(";", 1)[0].rsplit("/", 1)[-1]


def _load_stitch_validation_adapter(
    adapter_name: str,
) -> Optional[Any]:
    """Load an optional analysis-owned validation adapter module."""
    if not adapter_name:
        return None
    import importlib

    workspace_path = os.path.abspath(stitching_derivation.WORKSPACE)
    added_workspace_path = workspace_path not in sys.path
    if added_workspace_path:
        sys.path.insert(0, workspace_path)
    try:
        return importlib.import_module(adapter_name)
    except ImportError as exc:
        raise RuntimeError(
            f"Could not import stitching validation adapter '{adapter_name}'."
        ) from exc
    finally:
        if added_workspace_path:
            sys.path.remove(workspace_path)


def _stitch_validation_boundaries(
    stitch_config: dict[str, Any],
    submit_config: str = "",
    validation_adapter: Optional[Any] = None,
) -> dict[str, list[float]]:
    """Resolve generic histogram boundaries or delegate to an adapter."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "validation_boundaries", None)
        if callable(resolver):
            return resolver(submit_config)

    validation_config = stitch_config.get("validation", {})
    raw_boundaries = (
        validation_config.get("boundaries", {})
        if isinstance(validation_config, dict)
        else {}
    )
    if not isinstance(raw_boundaries, dict):
        raise RuntimeError("Stitch validation 'boundaries' must be a mapping.")
    boundaries: dict[str, list[float]] = {}
    for variable, values in raw_boundaries.items():
        if not isinstance(values, list):
            raise RuntimeError(
                f"Stitch validation boundaries for '{variable}' must be a list."
            )
        try:
            boundaries[str(variable)] = [float(value) for value in values]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Stitch validation boundaries for '{variable}' must be numeric."
            ) from exc
    return boundaries


def _stitch_validation_boundaries_by_process(
    stitch_config: dict[str, Any],
    submit_config: str = "",
    validation_adapter: Optional[Any] = None,
) -> dict[str, list[float]]:
    """Resolve generic group-specific boundaries or delegate to an adapter."""
    if validation_adapter is not None:
        resolver = getattr(
            validation_adapter,
            "validation_boundaries_by_process",
            None,
        )
        if callable(resolver):
            return resolver(submit_config)

    validation_config = stitch_config.get("validation", {})
    raw_boundaries = (
        validation_config.get("boundaries_by_group", {})
        if isinstance(validation_config, dict)
        else {}
    )
    if not isinstance(raw_boundaries, dict):
        raise RuntimeError(
            "Stitch validation 'boundaries_by_group' must be a mapping."
        )
    return {
        str(group): [float(value) for value in values]
        for group, values in raw_boundaries.items()
        if isinstance(values, list)
    }


def _load_stitch_cross_section_report(
    report_path: str,
) -> dict[str, dict[str, Any]]:
    """Load and validate per-bin cross sections from a derivation report."""
    import numpy as np

    if not os.path.isfile(report_path):
        raise RuntimeError(
            f"Stitching cross-section report not found: '{report_path}'"
        )
    with open(report_path) as fh:
        report = json.load(fh)

    groups = report.get("groups") if isinstance(report, dict) else None
    if not isinstance(groups, dict):
        raise RuntimeError(
            f"Stitching cross-section report '{report_path}' has no groups mapping."
        )

    parsed: dict[str, dict[str, Any]] = {}
    for group_name, group_report in groups.items():
        if not isinstance(group_report, dict):
            raise RuntimeError(
                f"Cross-section report group '{group_name}' must be a mapping."
            )
        raw_bins = group_report.get("binned_cross_sections_pb")
        if not isinstance(raw_bins, list) or not raw_bins:
            raise RuntimeError(
                f"Cross-section report group '{group_name}' has no "
                "binned_cross_sections_pb values."
            )
        try:
            bins = [float(value) for value in raw_bins]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Cross-section report group '{group_name}' contains a non-numeric "
                "binned cross section."
            ) from exc
        if any(not np.isfinite(value) or value < 0.0 for value in bins):
            raise RuntimeError(
                f"Cross-section report group '{group_name}' contains invalid "
                "binned cross sections."
            )
        raw_inclusive_sample = group_report.get("inclusive_sample")
        try:
            inclusive_samples = _shared_normalize_sample_names(
                raw_inclusive_sample, "inclusive_sample"
            )
        except ValueError:
            raise RuntimeError(
                f"Cross-section report group '{group_name}' has no valid "
                "inclusive_sample."
            )
        inclusive_sample: Union[str, list[str]] = (
            inclusive_samples[0]
            if len(inclusive_samples) == 1
            else inclusive_samples
        )
        parsed[str(group_name)] = {
            "era": str(group_report.get("era", "")),
            "inclusive_sample": inclusive_sample,
            "binned_cross_sections_pb": bins,
        }
    return parsed


def _stitch_validation_sample_groups(
    sample_names: list[str],
    manifest: DatasetManifest,
    stitch_config: str,
    weights: dict[str, Any],
    report_groups: set[str],
) -> dict[str, str]:
    """Resolve every application sample to its derived stitching group."""
    sample_groups: dict[str, str] = {}

    for correction in weights.get("corrections", []):
        if not isinstance(correction, dict):
            continue
        group_name = str(correction.get("name", ""))
        data = correction.get("data")
        content = data.get("content", []) if isinstance(data, dict) else []
        if data and data.get("nodetype") == "category":
            for item in content:
                if isinstance(item, dict) and "key" in item:
                    sample_groups[str(item["key"])] = group_name

    for entry in manifest.datasets:
        if entry.name in sample_names and entry.group in report_groups:
            sample_groups.setdefault(entry.name, str(entry.group))

    if stitch_config:
        config_path = os.path.abspath(stitch_config)
        if not os.path.isfile(config_path):
            raise RuntimeError(f"Stitch configuration file not found: '{config_path}'")
        with open(config_path) as fh:
            config = yaml.safe_load(fh) or {}
        groups = config.get("groups", {})
        if isinstance(groups, dict):
            for group_name, group_cfg in groups.items():
                if not isinstance(group_cfg, dict):
                    continue
                group = str(group_name)
                configured_samples = group_cfg.get("samples", [])
                if isinstance(configured_samples, list):
                    for sample_name in configured_samples:
                        sample_groups.setdefault(str(sample_name), group)
                meta_files = group_cfg.get("meta_files", {})
                if isinstance(meta_files, dict):
                    for sample_name in meta_files:
                        sample_groups.setdefault(str(sample_name), group)
                selector = group_cfg.get("select")
                if isinstance(selector, dict):
                    try:
                        selected = manifest.query(**selector)
                    except TypeError as exc:
                        raise RuntimeError(
                            f"Invalid dataset selector for stitch group '{group}': "
                            f"{selector}"
                        ) from exc
                    for entry in selected:
                        sample_groups.setdefault(entry.name, group)

    missing = sorted(set(sample_names) - set(sample_groups))
    if missing:
        raise RuntimeError(
            "Could not resolve stitching group for application sample(s): "
            + ", ".join(missing)
        )
    unresolved_groups = sorted(
        {sample_groups[sample] for sample in sample_names} - set(report_groups)
    )
    if unresolved_groups:
        raise RuntimeError(
            "Application samples resolved to groups missing from the cross-section "
            "report: "
            + ", ".join(unresolved_groups)
        )
    return {sample: sample_groups[sample] for sample in sample_names}


def _stitch_validation_process_label(
    group_name: str,
    validation_adapter: Optional[Any] = None,
) -> str:
    """Return an adapter-provided or sanitized group label."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "process_label", None)
        if callable(resolver):
            return str(resolver(group_name))
    label = re.sub(r"[^A-Za-z0-9]+", "_", group_name).strip("_")
    return label or "unknown"


def _stitch_validation_year_set(
    entry: Optional[DatasetEntry],
    report_era: str,
    validation_adapter: Optional[Any] = None,
) -> str:
    """Return an adapter-provided or report-era validation label."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "year_set", None)
        if callable(resolver):
            return str(resolver(entry, report_era))
    if str(report_era).strip():
        return str(report_era).strip()
    year = getattr(entry, "year", None)
    return str(year) if year is not None else "unknown"


def _stitch_validation_sample_scale(
    sample_name: str,
    meta_files: list[str],
    group_report: dict[str, Any],
    excluded_bins: Optional[set[int]] = None,
    stitch_factors: Optional[list[float]] = None,
    factor_threshold: float = 1e-6,
) -> tuple[float, float, float]:
    """Return (plot scale, sample cross section, signed event sum)."""
    import numpy as np

    sign_counts: Optional[np.ndarray] = None
    for meta_file in meta_files:
        values = np.asarray(
            stitching_derivation._read_intweight_sign_hist(meta_file, sample_name),
            dtype=float,
        )
        if sign_counts is None:
            sign_counts = values
        elif len(values) != len(sign_counts):
            raise RuntimeError(
                f"Signed counter histogram length mismatch for sample '{sample_name}'."
            )
        else:
            sign_counts += values

    if sign_counts is None or not len(sign_counts):
        raise RuntimeError(f"No signed counter values found for sample '{sample_name}'.")
    if any(not np.isfinite(value) for value in sign_counts):
        raise RuntimeError(
            f"Signed counter histogram for sample '{sample_name}' contains non-finite values."
        )

    cross_section_bins = np.asarray(
        group_report["binned_cross_sections_pb"],
        dtype=float,
    )
    if len(sign_counts) > len(cross_section_bins):
        raise RuntimeError(
            f"Cross-section report has {len(cross_section_bins)} bins for sample "
            f"'{sample_name}', but its signed counter has {len(sign_counts)} bins."
        )
    if stitch_factors is None:
        active_bins = sign_counts > 0.0
    else:
        if factor_threshold < 0.0 or not np.isfinite(factor_threshold):
            raise RuntimeError(
                "Stitch factor threshold must be finite and non-negative."
            )
        factors = np.zeros(len(sign_counts), dtype=float)
        factor_values = np.asarray(stitch_factors, dtype=float)
        if any(not np.isfinite(value) for value in factor_values):
            raise RuntimeError(
                f"Stitch factors for sample '{sample_name}' contain non-finite values."
            )
        factor_count = min(len(factors), len(factor_values))
        factors[:factor_count] = factor_values[:factor_count]
        active_bins = (sign_counts > 0.0) & (factors > factor_threshold)
    if excluded_bins:
        invalid_bins = [
            index for index in excluded_bins
            if index < 0 or index >= len(sign_counts)
        ]
        if invalid_bins:
            raise RuntimeError(
                f"Excluded stitch bin(s) {invalid_bins} are outside the signed "
                f"counter range for sample '{sample_name}'."
            )
        active_bins[list(excluded_bins)] = False

    active_signed_sum = float(np.sum(sign_counts[active_bins]))
    if active_signed_sum == 0.0:
        raise RuntimeError(
            f"Sample '{sample_name}' has zero signed event sum in its active "
            "stitch bins; cannot normalize plots."
        )
    sample_cross_section = float(np.sum(cross_section_bins[: len(sign_counts)][active_bins]))
    return sample_cross_section / active_signed_sum, sample_cross_section, active_signed_sum


def _stitch_validation_sample_factors(
    weights: dict[str, Any],
    group_name: str,
    sample_name: str,
) -> list[float]:
    """Return the derived correction factors for one validation sample."""
    for correction in weights.get("corrections", []):
        if not isinstance(correction, dict) or str(correction.get("name")) != group_name:
            continue
        data = correction.get("data")
        if not isinstance(data, dict) or data.get("nodetype") != "category":
            break
        for item in data.get("content", []):
            if not isinstance(item, dict) or str(item.get("key")) != sample_name:
                continue
            value = item.get("value")
            if not isinstance(value, dict) or value.get("nodetype") != "binning":
                break
            content = value.get("content")
            if not isinstance(content, list):
                break
            try:
                return [float(factor) for factor in content]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitch factors for sample '{sample_name}' are not numeric."
                ) from exc
    raise RuntimeError(
        f"Stitching correction JSON has no factors for sample '{sample_name}' "
        f"in group '{group_name}'."
    )


def _stitch_validation_configured_weight_scale(
    entry: Optional[DatasetEntry],
) -> float:
    """Return the per-event scale already present in application histograms."""
    import math

    if entry is None:
        return 1.0

    configured_scale = (
        (1.0 if entry.xsec is None else float(entry.xsec))
        * float(entry.kfac)
        * float(entry.filter_efficiency)
        * float(entry.extra_scale)
    )
    if not math.isfinite(configured_scale) or configured_scale == 0.0:
        raise RuntimeError(
            f"Application weight scale for sample '{entry.name}' must be finite "
            "and non-zero for validation plots."
        )
    return configured_scale


def _find_stitch_histogram_key(root_file: object, histogram_name: str) -> str:
    keys = getattr(root_file, "keys")(recursive=True)
    for key in keys:
        if _root_key_basename(key) == histogram_name:
            return str(key).split(";", 1)[0]
    raise RuntimeError(
        f"ROOT output is missing validation histogram '{histogram_name}'."
    )


def _stitch_validation_variable_boundaries(
    variable: str,
    process_family: str,
    boundaries: dict[str, list[float]],
    process_boundaries: dict[str, list[float]],
    validation_adapter: Optional[Any] = None,
) -> list[float]:
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "variable_boundaries", None)
        if callable(resolver):
            return list(
                resolver(variable, process_family, boundaries, process_boundaries)
            )
    return list(boundaries.get(variable, process_boundaries.get(process_family, [])))


def _stitch_validation_histogram_specs(
    stitch_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Resolve generic inclusive/stitched histogram plotting specifications."""
    validation_config = stitch_config.get("validation", {})
    if validation_config is None:
        validation_config = {}
    if not isinstance(validation_config, dict):
        raise RuntimeError("Stitch validation configuration must be a mapping.")

    raw_specs = validation_config.get("histograms")
    if raw_specs is None:
        return [
            {
                "name": "LHE_Vpt",
                "stitched_name": "LHE_Vpt_stitched",
                "label": "LHE VpT [GeV]",
                "boundaries": None,
                "xlim": None,
                "log_y": True,
                "ratio_range": [0.8, 1.2],
            },
            {
                "name": "LHE_NpNLO",
                "stitched_name": "LHE_NpNLO_stitched",
                "label": "LHE NpNLO",
                "boundaries": None,
                "xlim": None,
                "log_y": True,
                "ratio_range": [0.8, 1.2],
            },
        ]

    if not isinstance(raw_specs, list) or not raw_specs:
        raise RuntimeError(
            "Stitch validation 'histograms' must be a non-empty list."
        )

    specs: list[dict[str, Any]] = []
    for raw_spec in raw_specs:
        if not isinstance(raw_spec, dict):
            raise RuntimeError("Each stitch validation histogram must be a mapping.")
        name = str(raw_spec.get("name", "")).strip()
        if not name:
            raise RuntimeError(
                "Each stitch validation histogram requires a non-empty 'name'."
            )
        stitched_name = str(
            raw_spec.get("stitched_name", f"{name}_stitched")
        ).strip()
        if not stitched_name:
            raise RuntimeError(
                f"Stitch validation histogram '{name}' has an empty 'stitched_name'."
            )

        boundaries = raw_spec.get("boundaries")
        if boundaries is not None:
            if not isinstance(boundaries, list):
                raise RuntimeError(
                    f"Stitch validation boundaries for '{name}' must be a list."
                )
            try:
                boundaries = [float(value) for value in boundaries]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitch validation boundaries for '{name}' must be numeric."
                ) from exc

        xlim = raw_spec.get("xlim")
        if xlim is not None:
            if not isinstance(xlim, list) or len(xlim) != 2:
                raise RuntimeError(
                    f"Stitch validation xlim for '{name}' must contain two values."
                )
            try:
                xlim = [float(value) for value in xlim]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitch validation xlim for '{name}' must be numeric."
                ) from exc
            if xlim[1] <= xlim[0]:
                raise RuntimeError(
                    f"Stitch validation xlim for '{name}' must be increasing."
                )

        ratio_range = raw_spec.get("ratio_range", [0.8, 1.2])
        if not isinstance(ratio_range, list) or len(ratio_range) != 2:
            raise RuntimeError(
                f"Stitch validation ratio_range for '{name}' must contain two values."
            )
        try:
            ratio_range = [float(value) for value in ratio_range]
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Stitch validation ratio_range for '{name}' must be numeric."
            ) from exc
        if ratio_range[1] <= ratio_range[0]:
            raise RuntimeError(
                f"Stitch validation ratio_range for '{name}' must be increasing."
            )

        specs.append(
            {
                "name": name,
                "stitched_name": stitched_name,
                "label": str(raw_spec.get("label", name)),
                "boundaries": boundaries,
                "xlim": xlim,
                "log_y": bool(raw_spec.get("log_y", True)),
                "ratio_range": ratio_range,
            }
        )
    return specs


def _stitch_validation_histogram_data(
    histogram: Any,
) -> tuple[Any, Any, Optional[Any]]:
    import numpy as np

    values, edges = histogram.to_numpy(flow=False)
    values = np.asarray(values, dtype=float)
    edges = np.asarray(edges, dtype=float)
    variances = None
    variance_reader = getattr(histogram, "variances", None)
    if variance_reader is not None:
        raw_variances = variance_reader(flow=False)
        if raw_variances is not None:
            variances = np.asarray(raw_variances, dtype=float)
            if variances.shape != values.shape:
                raise RuntimeError(
                    "Validation histogram variances do not match its values."
                )
    return values, edges, variances


def _stitch_validation_excluded_bins(
    sample_name: str,
    group_name: str,
    group_report: dict[str, Any],
    submit_config: str,
    number_of_bins: int,
    validation_adapter: Optional[Any] = None,
) -> set[int]:
    """Return adapter-provided bins absent from a sample."""
    if validation_adapter is not None:
        resolver = getattr(validation_adapter, "excluded_bins", None)
        if callable(resolver):
            return set(
                resolver(
                    sample_name,
                    group_name,
                    group_report,
                    submit_config,
                    number_of_bins,
                )
            )
    return set()


def _write_stitch_validation_plots(
    meta_files: dict[str, list[str]],
    output_dir: str,
    boundaries: dict[str, list[float]],
    sample_metadata: dict[str, DatasetEntry],
    sample_groups: dict[str, str],
    cross_section_groups: dict[str, dict[str, Any]],
    process_boundaries: dict[str, list[float]],
    weights: dict[str, Any],
    histogram_specs: Optional[list[dict[str, Any]]] = None,
    submit_config: str = "",
    validation_adapter: Optional[Any] = None,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """Write normalized inclusive-versus-stitched validation plots."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import uproot  # type: ignore

    if histogram_specs is None:
        histogram_specs = _stitch_validation_histogram_specs({})
    specs_by_name = {spec["name"]: spec for spec in histogram_specs}

    aggregate: dict[
        tuple[str, str],
        dict[
            str,
            tuple[Any, Any, Optional[Any], Optional[Any], Any],
        ],
    ] = {}
    plot_process_families: dict[tuple[str, str], str] = {}
    normalization_records: dict[str, dict[str, Any]] = {}
    for sample_name, paths in meta_files.items():
        group_name = sample_groups[sample_name]
        group_report = cross_section_groups[group_name]
        raw_inclusive_sample = group_report["inclusive_sample"]
        try:
            inclusive_samples = _shared_normalize_sample_names(
                raw_inclusive_sample, "inclusive_sample"
            )
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid inclusive samples for stitching group '{group_name}': {exc}"
            ) from exc
        if any(sample_name not in meta_files for sample_name in inclusive_samples):
            raise RuntimeError(
                f"Inclusive sample(s) {inclusive_samples} for stitching group "
                f"'{group_name}' have no application metadata output."
            )
        excluded_bins = _stitch_validation_excluded_bins(
            sample_name,
            group_name,
            group_report,
            submit_config,
            len(group_report["binned_cross_sections_pb"]),
            validation_adapter,
        )
        scale, sample_cross_section, signed_sum = _stitch_validation_sample_scale(
            sample_name,
            paths,
            group_report,
            excluded_bins,
            _stitch_validation_sample_factors(weights, group_name, sample_name),
        )
        configured_weight_scale = _stitch_validation_configured_weight_scale(
            sample_metadata.get(sample_name)
        )
        scale /= configured_weight_scale
        process_family = group_name
        if validation_adapter is not None:
            resolver = getattr(validation_adapter, "process_family", None)
            if callable(resolver):
                process_family = str(resolver(group_name) or "all")
        process_label = _stitch_validation_process_label(
            group_name,
            validation_adapter,
        )
        year_set = _stitch_validation_year_set(
            sample_metadata.get(sample_name),
            str(group_report.get("era", "")),
            validation_adapter,
        )
        plot_key = (process_label, year_set)
        plot_process_families.setdefault(plot_key, process_family)
        plot_key_name = f"{process_label}_{year_set}"
        normalization_record = normalization_records.setdefault(
            plot_key_name,
            {
                "process": process_label,
                "year_set": year_set,
                "samples": {},
            },
        )
        normalization_record["samples"][sample_name] = {
            "stitch_group": group_name,
            "cross_section_pb": sample_cross_section,
            "signed_event_sum": signed_sum,
            "scale": scale,
            "configured_weight_scale": configured_weight_scale,
            "is_inclusive_sample": sample_name in inclusive_samples,
            "excluded_stitch_bins": sorted(excluded_bins),
        }
        grouped = aggregate.setdefault(plot_key, {})
        for meta_file in paths:
            with cast(Any, uproot.open(meta_file)) as root_file:
                for histogram_spec in histogram_specs:
                    variable = histogram_spec["name"]
                    inclusive_key = _find_stitch_histogram_key(
                        root_file, variable
                    )
                    stitched_key = _find_stitch_histogram_key(
                        root_file, histogram_spec["stitched_name"]
                    )
                    inclusive, edges, inclusive_variance = (
                        _stitch_validation_histogram_data(root_file[inclusive_key])
                    )
                    stitched, stitched_edges, stitched_variance = (
                        _stitch_validation_histogram_data(root_file[stitched_key])
                    )
                    inclusive *= scale
                    stitched *= scale
                    if inclusive_variance is not None:
                        inclusive_variance *= scale * scale
                    if stitched_variance is not None:
                        stitched_variance *= scale * scale
                    if (
                        inclusive.shape != stitched.shape
                        or not np.array_equal(edges, stitched_edges)
                    ):
                        raise RuntimeError(
                            f"Validation histograms for '{variable}' in "
                            f"'{meta_file}' have incompatible binning."
                        )
                    inclusive_contribution = (
                        inclusive
                        if sample_name in inclusive_samples
                        else np.zeros_like(inclusive)
                    )
                    inclusive_variance_contribution = (
                        inclusive_variance
                        if sample_name in inclusive_samples
                        else (
                            None
                            if inclusive_variance is None
                            else np.zeros_like(inclusive_variance)
                        )
                    )
                    if variable not in grouped:
                        grouped[variable] = (
                            inclusive_contribution,
                            stitched,
                            inclusive_variance_contribution,
                            stitched_variance,
                            edges,
                        )
                    else:
                        (
                            previous_inclusive,
                            previous_stitched,
                            previous_inclusive_variance,
                            previous_stitched_variance,
                            previous_edges,
                        ) = grouped[variable]
                        if not np.array_equal(previous_edges, edges):
                            raise RuntimeError(
                                f"Validation histograms for '{variable}' do not "
                                "share binning across application files."
                            )
                        grouped[variable] = (
                            previous_inclusive + inclusive_contribution,
                            previous_stitched + stitched,
                            (
                                None
                                if previous_inclusive_variance is None
                                or inclusive_variance_contribution is None
                                else previous_inclusive_variance
                                + inclusive_variance_contribution
                            ),
                            (
                                None
                                if previous_stitched_variance is None
                                or stitched_variance is None
                                else previous_stitched_variance + stitched_variance
                            ),
                            previous_edges,
                        )

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    plot_paths: list[str] = []
    for (process_label, year_set), variables in sorted(aggregate.items()):
        for (
            variable,
            (
                inclusive,
                stitched,
                inclusive_variance,
                stitched_variance,
                edges,
            ),
        ) in variables.items():
            histogram_spec = specs_by_name[variable]
            variable_boundaries = histogram_spec["boundaries"]
            if variable_boundaries is None:
                variable_boundaries = _stitch_validation_variable_boundaries(
                    variable,
                    plot_process_families[(process_label, year_set)],
                    boundaries,
                    process_boundaries,
                    validation_adapter,
                )
            xlim = histogram_spec["xlim"]
            x_ticks = None
            x_ticklabels = None
            if variable == "LHE_NpNLO":
                centers = edges[:-1] + 0.5 * np.diff(edges)
                x_ticks = centers
                x_ticklabels = [str(int(round(edge))) for edge in edges[:-1]]
                if xlim is None:
                    xlim = [float(edges[0]), float(edges[-1])]
            elif variable == "LHE_Vpt" and xlim is None:
                xlim = [float(edges[0]), 1000.0]
            safe_process = re.sub(r"[^A-Za-z0-9]+", "_", process_label).strip("_")
            safe_year_set = re.sub(r"[^A-Za-z0-9]+", "_", year_set).strip("_")
            plot_path = os.path.join(
                output_dir,
                f"stitch_{safe_process}_{safe_year_set}_{variable}_comparison.png",
            )
            _plot_ratio_comparison(
                inclusive,
                stitched,
                edges,
                plot_path,
                title=f"Stitching validation: {process_label} {year_set} {variable}",
                xlabel=histogram_spec["label"],
                boundaries=variable_boundaries,
                xlim=xlim,
                log_y=histogram_spec["log_y"],
                ratio_range=histogram_spec["ratio_range"],
                inclusive_variance=inclusive_variance,
                stitched_variance=stitched_variance,
                x_ticks=x_ticks,
                x_ticklabels=x_ticklabels,
            )
            plot_paths.append(plot_path)

    return plot_paths, normalization_records


def _validate_stitch_meta_file(
    meta_file: str,
    sample_name: str,
    require_stitched_histograms: bool = True,
    stitched_histograms: Optional[list[str]] = None,
    application_check: Optional[Callable[[Any], dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Validate one application metadata file and return its summary."""
    import math

    try:
        import uproot  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "StitchingValidationTask requires uproot to inspect ROOT outputs."
        ) from exc

    required_names = [
        f"counter_intWeightSignSum_{sample_name}",
        f"counter_weightSignSum_{sample_name}",
    ]
    if require_stitched_histograms:
        required_names.extend(
            stitched_histograms
            or ("LHE_Vpt_stitched", "LHE_NpNLO_stitched")
        )

    application_record = {}
    with uproot.open(meta_file) as root_file:
        keys = list(root_file.keys(recursive=True))
        matches: dict[str, str] = {}
        for key in keys:
            basename = _root_key_basename(key)
            if basename in required_names and basename not in matches:
                matches[basename] = str(key).split(";", 1)[0]

        missing = [name for name in required_names if name not in matches]
        if missing:
            raise RuntimeError(
                f"Stitching output '{meta_file}' is missing histogram(s): "
                + ", ".join(missing)
            )

        histogram_bins: dict[str, int] = {}
        for name, key in matches.items():
            try:
                values, _ = root_file[key].to_numpy(flow=False)
                if any(not math.isfinite(float(value)) for value in values):
                    raise RuntimeError(
                        f"Stitching output '{meta_file}' contains non-finite values "
                        f"in histogram '{name}'."
                    )
                histogram_bins[name] = len(values)
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(
                    f"Could not read histogram '{name}' from '{meta_file}': {exc}"
                ) from exc

        if application_check is not None:
            try:
                application_record = {"application_closure": application_check(root_file)}
            except (KeyError, RuntimeError, ValueError) as exc:
                raise RuntimeError(
                    f"Stitching closure failed for '{sample_name}' in '{meta_file}': {exc}"
                ) from exc

    return {
        "meta_file": meta_file,
        "sample": sample_name,
        "histograms": sorted(matches),
        "histogram_bins": histogram_bins,
        **application_record,
    }


class StitchingValidationOperations(object):
    @property
    def _output_dir(self) -> str:
        return os.path.join(stitching_derivation.WORKSPACE, f"stitchValidation_{self.name}")

    @property
    def _weights_path(self) -> str:
        raw_path = self.stitch_weights or os.path.join(
            stitching_derivation.WORKSPACE, f"stitchWeights_{self.name}", "stitch_weights.json"
        )
        return (
            raw_path
            if os.path.isabs(raw_path)
            else os.path.abspath(os.path.join(stitching_derivation.WORKSPACE, raw_path))
        )

    @property
    def _cross_sections_path(self) -> str:
        raw_path = self.stitch_cross_sections or os.path.join(
            stitching_derivation.WORKSPACE,
            f"stitchWeights_{self.name}",
            "stitch_cross_sections.json",
        )
        return (
            raw_path
            if os.path.isabs(raw_path)
            else os.path.abspath(os.path.join(stitching_derivation.WORKSPACE, raw_path))
        )

    def validate_outputs(self, meta_files, *, output_dir=None):
        publish = self.publish_message if output_dir is None else print
        output_dir = Path(output_dir or self._output_dir)
        manifest = DatasetManifest.load(self.dataset_manifest)
        weights_path = self._weights_path
        if not os.path.isfile(weights_path):
            raise RuntimeError(
                f"Stitching correction JSON not found: '{weights_path}'"
            )

        expected_groups: set[str] = set()
        stitch_config_data: dict[str, Any] = {}
        if self.stitch_config:
            config_path = os.path.abspath(self.stitch_config)
            if not os.path.isfile(config_path):
                raise RuntimeError(f"Stitch configuration file not found: '{config_path}'")
            with open(config_path) as fh:
                stitch_config_data = yaml.safe_load(fh) or {}
            groups = stitch_config_data.get("groups")
            if not isinstance(groups, dict):
                raise RuntimeError(
                    f"Stitch config '{config_path}' must contain a top-level 'groups' mapping."
                )
            expected_groups = {str(group_name) for group_name in groups}

        validation_config = stitch_config_data.get("validation") or {}
        adapter_name = str(self.validation_adapter).strip()
        if not adapter_name:
            if isinstance(validation_config, dict):
                adapter_name = str(validation_config.get("adapter", "")).strip()
        validation_adapter = _load_stitch_validation_adapter(adapter_name)

        with open(weights_path) as fh:
            weights = json.load(fh)
        if not isinstance(weights, dict) or not isinstance(
            weights.get("corrections"), list
        ):
            raise RuntimeError(
                f"Stitching correction JSON '{weights_path}' has no corrections list."
            )
        correction_names = {
            str(correction.get("name"))
            for correction in weights.get("corrections", [])
            if isinstance(correction, dict) and correction.get("name")
        }
        missing_groups = sorted(expected_groups - correction_names)
        if missing_groups:
            raise RuntimeError(
                f"Stitching correction JSON '{weights_path}' is missing group(s): "
                + ", ".join(missing_groups)
            )

        validation_histograms = _stitch_validation_histogram_specs(
            stitch_config_data
        )
        application_check = None
        if self.require_stitched_histograms and validation_config.get("application_closure", False):
            application_check = getattr(validation_adapter, "validate_application_file", None)
            if not callable(application_check):
                raise RuntimeError(
                    "Stitch application closure requires an adapter with validate_application_file."
                )

        cross_section_groups: dict[str, dict[str, Any]] = {}
        sample_groups: dict[str, str] = {}
        sample_metadata = {entry.name: entry for entry in manifest.datasets}
        if self.require_stitched_histograms:
            cross_sections_path = self._cross_sections_path
            cross_section_groups = _load_stitch_cross_section_report(
                cross_sections_path
            )
            sample_groups = _stitch_validation_sample_groups(
                list(meta_files),
                manifest,
                self.stitch_config,
                weights,
                set(cross_section_groups),
            )

        validation_records: list[dict[str, Any]] = []
        for sample_name, paths in sorted(meta_files.items()):
            sample_check = None
            if application_check is not None:
                sample_check = partial(
                    application_check,
                    sample_name=sample_name,
                    factors=_stitch_validation_sample_factors(
                        weights, sample_groups[sample_name], sample_name
                    ),
                    norm_scale=_stitch_validation_configured_weight_scale(
                        sample_metadata.get(sample_name)
                    ),
                )
            for meta_file in paths:
                validation_records.append(
                    _validate_stitch_meta_file(
                        meta_file,
                        sample_name,
                        require_stitched_histograms=self.require_stitched_histograms,
                        stitched_histograms=(
                            [
                                spec["stitched_name"]
                                for spec in validation_histograms
                            ]
                            if self.require_stitched_histograms
                            else None
                        ),
                        application_check=sample_check,
                    )
                )

        output_dir.mkdir(parents=True, exist_ok=True)
        plot_boundaries = _stitch_validation_boundaries(
            stitch_config_data,
            self.submit_config,
            validation_adapter,
        )
        plot_process_boundaries = _stitch_validation_boundaries_by_process(
            stitch_config_data,
            self.submit_config,
            validation_adapter,
        )
        plot_paths: list[str] = []
        plot_normalization: dict[str, dict[str, Any]] = {}
        if self.require_stitched_histograms:
            plot_paths, plot_normalization = _write_stitch_validation_plots(
                meta_files,
                str(output_dir),
                plot_boundaries,
                sample_metadata,
                sample_groups,
                cross_section_groups,
                plot_process_boundaries,
                weights,
                histogram_specs=validation_histograms,
                submit_config=self.submit_config,
                validation_adapter=validation_adapter,
            )
        with (output_dir / "stitch_validation.json").open("w") as fh:
            json.dump(
                {
                    "application_run": self.name,
                    "weights": weights_path,
                    "samples": len(meta_files),
                    "meta_files": len(validation_records),
                    "groups": sorted(expected_groups or correction_names),
                    "plots": plot_paths,
                    "plot_boundaries": plot_boundaries,
                    "plot_boundaries_by_process": plot_process_boundaries,
                    "plot_normalization": plot_normalization,
                    "files": validation_records,
                },
                fh,
                indent=2,
            )

        publish(
            f"Validated stitching outputs for {len(meta_files)} sample(s) and "
            f"{len(validation_records)} metadata file(s)."
        )


@dataclass
class StitchingValidation(StitchingValidationOperations):
    name: str
    dataset_manifest: str
    stitch_config: str = ""
    stitch_weights: str = ""
    stitch_cross_sections: str = ""
    submit_config: str = ""
    validation_adapter: str = ""
    require_stitched_histograms: bool = True

    publish_message = staticmethod(print)
