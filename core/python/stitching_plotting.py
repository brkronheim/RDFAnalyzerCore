"""Reusable plotting helpers for inclusive-versus-stitched histograms."""

from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Optional, Sequence

import numpy as np


def ratio_with_uncertainty(
    numerator: Sequence[float],
    denominator: Sequence[float],
    numerator_variance: Optional[Sequence[float]] = None,
    denominator_variance: Optional[Sequence[float]] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Return a bin-by-bin ratio and independent statistical uncertainty."""
    numerator_array = np.asarray(numerator, dtype=float)
    denominator_array = np.asarray(denominator, dtype=float)
    if numerator_array.shape != denominator_array.shape:
        raise ValueError("Ratio inputs must have identical shapes")

    ratio = np.full(numerator_array.shape, np.nan, dtype=float)
    np.divide(
        numerator_array,
        denominator_array,
        out=ratio,
        where=denominator_array != 0.0,
    )
    uncertainty = np.full(numerator_array.shape, np.nan, dtype=float)
    if numerator_variance is None or denominator_variance is None:
        return ratio, uncertainty

    numerator_variance_array = np.asarray(numerator_variance, dtype=float)
    denominator_variance_array = np.asarray(denominator_variance, dtype=float)
    if (
        numerator_variance_array.shape != numerator_array.shape
        or denominator_variance_array.shape != denominator_array.shape
    ):
        raise ValueError("Ratio variances must match the histogram shape")

    valid = (
        (denominator_array != 0.0)
        & np.isfinite(numerator_array)
        & np.isfinite(denominator_array)
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        denominator_squared = denominator_array[valid] * denominator_array[valid]
        uncertainty[valid] = np.sqrt(
            np.abs(numerator_variance_array[valid]) / denominator_squared
            + numerator_array[valid]
            * numerator_array[valid]
            * np.abs(denominator_variance_array[valid])
            / (denominator_squared * denominator_squared)
        )
    return ratio, uncertainty


def pull_with_uncertainty(
    stitched: Sequence[float],
    inclusive: Sequence[float],
    stitched_variance: Sequence[float],
    inclusive_variance: Sequence[float],
) -> np.ndarray:
    """Return the difference in units of combined independent MC uncertainty."""
    stitched_array = np.asarray(stitched, dtype=float)
    inclusive_array = np.asarray(inclusive, dtype=float)
    stitched_variance_array = np.asarray(stitched_variance, dtype=float)
    inclusive_variance_array = np.asarray(inclusive_variance, dtype=float)
    if (
        stitched_array.shape != inclusive_array.shape
        or stitched_variance_array.shape != stitched_array.shape
        or inclusive_variance_array.shape != inclusive_array.shape
    ):
        raise ValueError("Pull inputs and variances must have identical shapes")

    combined_variance = (
        np.abs(stitched_variance_array) + np.abs(inclusive_variance_array)
    )
    pull = np.full(stitched_array.shape, np.nan, dtype=float)
    valid = (
        (combined_variance > 0.0)
        & np.isfinite(stitched_array)
        & np.isfinite(inclusive_array)
        & np.isfinite(combined_variance)
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        np.divide(
            stitched_array - inclusive_array,
            np.sqrt(combined_variance),
            out=pull,
            where=valid,
        )
    return pull


def ratio_plot_range(
    ratio: Sequence[float],
    ratio_uncertainty: Optional[Sequence[float]] = None,
    ratio_range: Sequence[float] = (0.8, 1.2),
) -> tuple[float, float]:
    """Return configured S/I bounds, tightened around fully visible data."""
    ratio_array = np.asarray(ratio, dtype=float)
    configured_range = np.asarray(ratio_range, dtype=float)
    if configured_range.shape != (2,) or not np.all(np.isfinite(configured_range)):
        raise ValueError("Ratio range must contain two finite values")
    if configured_range[1] <= configured_range[0]:
        raise ValueError("Ratio range must be increasing")

    valid_ratio = np.isfinite(ratio_array)
    data_ratio = ratio_array[valid_ratio]
    data_lower = data_ratio.copy()
    data_upper = data_ratio.copy()
    if ratio_uncertainty is not None:
        uncertainty_array = np.asarray(ratio_uncertainty, dtype=float)
        if uncertainty_array.shape != ratio_array.shape:
            raise ValueError("Ratio uncertainty must match the ratio shape")
        valid_uncertainty = (
            valid_ratio
            & np.isfinite(uncertainty_array)
            & (uncertainty_array >= 0.0)
        )
        uncertainty_available = valid_uncertainty[valid_ratio]
        data_uncertainty = uncertainty_array[valid_ratio]
        data_lower = np.where(
            uncertainty_available,
            data_ratio - data_uncertainty,
            data_ratio,
        )
        data_upper = np.where(
            uncertainty_available,
            data_ratio + data_uncertainty,
            data_ratio,
        )

    if not np.any(valid_ratio):
        return float(configured_range[0]), float(configured_range[1])

    observed_lower = float(np.min(data_lower))
    observed_upper = float(np.max(data_upper))
    if (
        observed_lower <= configured_range[0]
        or observed_upper >= configured_range[1]
    ):
        return float(configured_range[0]), float(configured_range[1])

    observed_span = observed_upper - observed_lower
    padding = max(
        0.1 * observed_span,
        0.05 * (configured_range[1] - configured_range[0]),
    )
    return (
        max(float(configured_range[0]), observed_lower - padding),
        min(float(configured_range[1]), observed_upper + padding),
    )


def plot_ratio_comparison(
    inclusive: Sequence[float],
    stitched: Sequence[float],
    edges: Sequence[float],
    output_path: str | PathLike[str],
    *,
    title: str,
    xlabel: str,
    boundaries: Sequence[float] = (),
    xlim: Optional[Sequence[float]] = None,
    log_y: bool = True,
    ratio_range: Sequence[float] = (0.8, 1.2),
    pull_range: Sequence[float] = (-5.0, 5.0),
    inclusive_variance: Optional[Sequence[float]] = None,
    stitched_variance: Optional[Sequence[float]] = None,
    inclusive_label: str = "Inclusive",
    stitched_label: str = "Stitched",
    ylabel: str = "Cross-section weighted signed events",
    ratio_ylabel: str = "S / I",
    x_ticks: Optional[Sequence[float]] = None,
    x_ticklabels: Optional[Sequence[str]] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Write an inclusive-versus-stitched comparison with ratio and pull panels."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    inclusive_array = np.asarray(inclusive, dtype=float)
    stitched_array = np.asarray(stitched, dtype=float)
    edge_array = np.asarray(edges, dtype=float)
    if (
        inclusive_array.ndim != 1
        or stitched_array.shape != inclusive_array.shape
        or edge_array.shape != (inclusive_array.size + 1,)
    ):
        raise ValueError("Histogram values and edges have incompatible shapes")

    ratio, ratio_uncertainty = ratio_with_uncertainty(
        stitched_array,
        inclusive_array,
        stitched_variance,
        inclusive_variance,
    )
    effective_ratio_range = ratio_plot_range(
        ratio,
        ratio_uncertainty,
        ratio_range,
    )
    centers = edge_array[:-1] + 0.5 * np.diff(edge_array)

    figure, (axis, ratio_axis, pull_axis) = plt.subplots(
        3,
        1,
        figsize=(10, 11),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1, 1], "hspace": 0.05},
        constrained_layout=True,
    )

    if log_y:
        plotted_inclusive = np.where(inclusive_array > 0.0, inclusive_array, np.nan)
        plotted_stitched = np.where(stitched_array > 0.0, stitched_array, np.nan)
    else:
        plotted_inclusive = inclusive_array
        plotted_stitched = stitched_array
    axis.stairs(
        plotted_inclusive,
        edge_array,
        linewidth=1.5,
        label=inclusive_label,
        color="tab:orange",
    )
    axis.stairs(
        plotted_stitched,
        edge_array,
        linewidth=1.5,
        label=stitched_label,
        color="tab:blue",
    )

    if inclusive_variance is not None:
        inclusive_error = np.sqrt(np.abs(np.asarray(inclusive_variance, dtype=float)))
        valid = np.isfinite(inclusive_array)
        if log_y:
            valid &= inclusive_array > 0.0
        axis.errorbar(
            centers[valid],
            inclusive_array[valid],
            yerr=inclusive_error[valid],
            fmt="none",
            color="tab:orange",
            linewidth=0.8,
            alpha=0.8,
        )
    if stitched_variance is not None:
        stitched_error = np.sqrt(np.abs(np.asarray(stitched_variance, dtype=float)))
        valid = np.isfinite(stitched_array)
        if log_y:
            valid &= stitched_array > 0.0
        axis.errorbar(
            centers[valid],
            stitched_array[valid],
            yerr=stitched_error[valid],
            fmt="none",
            color="tab:blue",
            linewidth=0.8,
            alpha=0.8,
        )

    if log_y:
        axis.set_yscale("log")
    for boundary_index, boundary in enumerate(boundaries):
        axis.axvline(
            boundary,
            color="black",
            linestyle="--",
            linewidth=0.8,
            alpha=0.65,
            label="Stitch boundary" if boundary_index == 0 else None,
        )
        ratio_axis.axvline(
            boundary,
            color="black",
            linestyle="--",
            linewidth=0.8,
            alpha=0.65,
        )
        pull_axis.axvline(
            boundary,
            color="black",
            linestyle="--",
            linewidth=0.8,
            alpha=0.65,
        )

    ratio_axis.stairs(ratio, edge_array, linewidth=1.2, color="tab:green")
    valid_ratio = np.isfinite(ratio) & np.isfinite(ratio_uncertainty)
    if np.any(valid_ratio):
        ratio_axis.errorbar(
            centers[valid_ratio],
            ratio[valid_ratio],
            yerr=ratio_uncertainty[valid_ratio],
            fmt="none",
            color="tab:green",
            linewidth=0.8,
            alpha=0.8,
        )
    ratio_axis.axhline(1.0, color="black", linewidth=0.8, alpha=0.65)
    ratio_axis.set_ylabel(ratio_ylabel)
    ratio_axis.set_ylim(*effective_ratio_range)
    pull_axis.axhline(0.0, color="black", linewidth=0.8, alpha=0.65)
    if inclusive_variance is not None and stitched_variance is not None:
        pull = pull_with_uncertainty(
            stitched_array,
            inclusive_array,
            stitched_variance,
            inclusive_variance,
        )
        valid_pull = np.isfinite(pull)
        pull_axis.bar(
            centers[valid_pull],
            pull[valid_pull],
            width=np.diff(edge_array)[valid_pull],
            align="center",
            fill=False,
            linewidth=0.8,
            edgecolor="tab:purple",
            hatch="///",
        )
    pull_axis.set_ylabel("Pull")
    pull_axis.set_ylim(*pull_range)
    pull_axis.set_xlabel(xlabel)
    if x_ticks is not None:
        ratio_axis.set_xticks(x_ticks)
    if x_ticklabels is not None:
        ratio_axis.set_xticklabels(x_ticklabels)
    if xlim is not None:
        axis.set_xlim(*xlim)

    axis.tick_params(labelbottom=False)
    ratio_axis.tick_params(labelbottom=False)
    axis.set_ylabel(ylabel)
    axis.set_title(title)
    axis.legend()

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(destination, dpi=150)
    plt.close(figure)
    return ratio, ratio_uncertainty