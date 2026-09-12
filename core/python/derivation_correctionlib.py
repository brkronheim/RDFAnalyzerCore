"""Reusable correctionlib builders for stage-1 derivations.

This module currently hosts the stitching derivation helpers extracted from the
LAW task implementation. The functions are kept deliberately small and
framework-facing so later tagger and STXS builders can live beside them.
"""

from __future__ import annotations

import os
from typing import Callable, Optional, Union


def _normalize_sample_names(sample_spec: object, field_name: str) -> list[str]:
    """Flatten a sample name or nested sample-name collection."""
    if isinstance(sample_spec, str):
        if sample_spec:
            return [sample_spec]
        raise ValueError(f"{field_name} must not contain an empty sample name")

    if isinstance(sample_spec, (list, tuple)):
        sample_names: list[str] = []
        for item in sample_spec:
            sample_names.extend(_normalize_sample_names(item, field_name))
        if sample_names:
            return sample_names

    raise ValueError(
        f"{field_name} must be a sample name or a non-empty collection "
        "of sample names"
    )


def _meta_file_exists(meta_file: str) -> bool:
    if meta_file.startswith(("root://", "xroot://")):
        return True
    return os.path.isfile(meta_file)


def _read_intweight_sign_hist(meta_file: str, sample_name: str) -> list[float]:
    """Read the signed-count histogram for one stitched sample."""
    hist_name = f"counter_intWeightSignSum_{sample_name}"

    try:
        import uproot  # type: ignore

        with uproot.open(meta_file) as f:
            keys = [k.split(";")[0] for k in f.keys()]
            if hist_name not in keys:
                raise RuntimeError(
                    f"Histogram '{hist_name}' not found in '{meta_file}'."
                )
            hist = f[hist_name]
            return list(hist.values())

    except ImportError:
        pass

    try:
        import ROOT  # type: ignore

        tfile = ROOT.TFile.Open(meta_file, "READ")
        if not tfile or tfile.IsZombie():
            raise RuntimeError(f"Cannot open ROOT file: '{meta_file}'")
        hist = tfile.Get(hist_name)
        if not hist:
            tfile.Close()
            raise RuntimeError(
                f"Histogram '{hist_name}' not found in '{meta_file}'."
            )
        values = [hist.GetBinContent(b) for b in range(1, hist.GetNbinsX() + 1)]
        tfile.Close()
        return values

    except ImportError as exc:
        raise RuntimeError(
            "Neither uproot nor ROOT (PyROOT) is importable. "
            "Install uproot (``pip install uproot``) to read counter histograms "
            "without a full ROOT installation."
        ) from exc


def _read_weight_sign_sum(meta_file: str, sample_name: str) -> float:
    """Read the signed event total for one sample."""
    hist_name = f"counter_weightSignSum_{sample_name}"

    try:
        import uproot  # type: ignore

        with uproot.open(meta_file) as f:
            keys = [k.split(";", 1)[0] for k in f.keys(recursive=True)]
            if hist_name not in keys:
                raise RuntimeError(
                    f"Histogram '{hist_name}' not found in '{meta_file}'."
                )
            values = f[hist_name].values()
            if len(values) != 1:
                raise RuntimeError(
                    f"Histogram '{hist_name}' in '{meta_file}' must have one bin."
                )
            return float(values[0])

    except ImportError:
        pass

    try:
        import ROOT  # type: ignore

        tfile = ROOT.TFile.Open(meta_file, "READ")
        if not tfile or tfile.IsZombie():
            raise RuntimeError(f"Cannot open ROOT file: '{meta_file}'")
        hist = tfile.Get(hist_name)
        if not hist:
            tfile.Close()
            raise RuntimeError(
                f"Histogram '{hist_name}' not found in '{meta_file}'."
            )
        if hist.GetNbinsX() != 1:
            tfile.Close()
            raise RuntimeError(
                f"Histogram '{hist_name}' in '{meta_file}' must have one bin."
            )
        value = float(hist.GetBinContent(1))
        tfile.Close()
        return value

    except ImportError as exc:
        raise RuntimeError(
            "Neither uproot nor ROOT (PyROOT) is importable. "
            "Install uproot (``pip install uproot``) to read counter histograms "
            "without a full ROOT installation."
        ) from exc


def _read_weight_sum(meta_file: str, sample_name: str) -> float:
    """Read the raw generator-weight total for one sample."""
    hist_name = f"counter_weightSum_{sample_name}"

    try:
        import uproot  # type: ignore

        with uproot.open(meta_file) as f:
            keys = [k.split(";", 1)[0] for k in f.keys(recursive=True)]
            if hist_name not in keys:
                raise RuntimeError(
                    f"Histogram '{hist_name}' not found in '{meta_file}'."
                )
            values = f[hist_name].values()
            if len(values) != 1:
                raise RuntimeError(
                    f"Histogram '{hist_name}' in '{meta_file}' must have one bin."
                )
            return float(values[0])

    except ImportError:
        pass

    try:
        import ROOT  # type: ignore

        tfile = ROOT.TFile.Open(meta_file, "READ")
        if not tfile or tfile.IsZombie():
            raise RuntimeError(f"Cannot open ROOT file: '{meta_file}'")
        hist = tfile.Get(hist_name)
        if not hist:
            tfile.Close()
            raise RuntimeError(
                f"Histogram '{hist_name}' not found in '{meta_file}'."
            )
        if hist.GetNbinsX() != 1:
            tfile.Close()
            raise RuntimeError(
                f"Histogram '{hist_name}' in '{meta_file}' must have one bin."
            )
        value = float(hist.GetBinContent(1))
        tfile.Close()
        return value

    except ImportError as exc:
        raise RuntimeError(
            "Neither uproot nor ROOT (PyROOT) is importable. "
            "Install uproot (``pip install uproot``) to read counter histograms "
            "without a full ROOT installation."
        ) from exc


def _derive_stitch_weights(
    group_name: str,
    meta_files: dict[str, Union[str, list[str]]],
    read_intweight_sign_hist: Callable[[str, str], list[float]] | None = None,
) -> dict[str, list[float]]:
    """Compute per-bin factors from nonnegative net signed counts."""
    import numpy as np  # type: ignore

    if read_intweight_sign_hist is None:
        read_intweight_sign_hist = _read_intweight_sign_hist

    sample_names = list(meta_files.keys())

    sign_counts: dict[str, "np.ndarray"] = {}
    ref_len: Optional[int] = None

    for sample_name, meta_file_spec in meta_files.items():
        meta_paths = (
            [meta_file_spec]
            if isinstance(meta_file_spec, str)
            else list(meta_file_spec)
        )
        if not meta_paths:
            raise RuntimeError(
                f"[StitchingDerivationTask] No meta files supplied for sample "
                f"'{sample_name}' in group '{group_name}'"
            )

        arr: Optional["np.ndarray"] = None
        for meta_file in meta_paths:
            if not _meta_file_exists(meta_file):
                raise RuntimeError(
                    f"[StitchingDerivationTask] Meta file not found for sample "
                    f"'{sample_name}' in group '{group_name}': '{meta_file}'"
                )
            values = read_intweight_sign_hist(meta_file, sample_name)
            current = np.array(values, dtype=np.float64)
            if arr is None:
                arr = current
            elif len(current) != len(arr):
                raise RuntimeError(
                    f"[StitchingDerivationTask] Histogram length mismatch in "
                    f"sample '{sample_name}' of group '{group_name}': "
                    f"job histogram has {len(current)} bins but previous files "
                    f"had {len(arr)} bins."
                )
            else:
                arr += current

        assert arr is not None

        if ref_len is None:
            ref_len = len(arr)
        elif len(arr) != ref_len:
            raise RuntimeError(
                f"[StitchingDerivationTask] Histogram length mismatch in "
                f"group '{group_name}': sample '{sample_name}' has "
                f"{len(arr)} bins but previous samples had {ref_len} bins. "
                "All samples in a group must use the same stitch variable "
                "with the same binning."
            )
        sign_counts[sample_name] = arr

    assert ref_len is not None

    nonnegative_counts = {
        sample_name: np.maximum(arr, 0.0)
        for sample_name, arr in sign_counts.items()
    }
    total = np.zeros(ref_len, dtype=np.float64)
    for arr in nonnegative_counts.values():
        total += arr

    scale_factors: dict[str, "np.ndarray"] = {}
    with np.errstate(invalid="ignore", divide="ignore"):
        for sample_name in sample_names:
            c_n = nonnegative_counts[sample_name]
            scale_factors[sample_name] = np.where(total != 0.0, c_n / total, 0.0)

    non_zero_mask = total != 0.0
    if np.any(non_zero_mask):
        last_active = int(np.where(non_zero_mask)[0][-1])
        n_used = last_active + 1
    else:
        n_used = 1

    return {name: arr[:n_used].tolist() for name, arr in scale_factors.items()}


def _derive_stitch_cross_sections(
    group_name: str,
    meta_files: dict[str, Union[str, list[str]]],
    inclusive_samples: object,
    inclusive_cross_section: float,
    read_intweight_sign_hist: Callable[[str, str], list[float]] | None = None,
) -> list[float]:
    """Split an inclusive cross section into stitch-bin cross sections.

    The fraction in each bin is computed from the inclusive sample's signed
    counter with negative net bins treated as zero, so the result uses the same
    event definition as the stitching weights and excludes bins removed by the
    stitch configuration. ``inclusive_samples`` may be nested to represent
    independent sample partitions along multiple axes; all counters are summed
    before calculating the fractions.
    """
    import numpy as np  # type: ignore

    if not np.isfinite(inclusive_cross_section) or inclusive_cross_section <= 0.0:
        raise ValueError(
            f"Inclusive cross section for group '{group_name}' must be finite "
            "and positive"
        )

    if read_intweight_sign_hist is None:
        read_intweight_sign_hist = _read_intweight_sign_hist

    try:
        sample_names = _normalize_sample_names(
            inclusive_samples, "inclusive_sample"
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid inclusive samples for stitch group '{group_name}': {exc}"
        ) from exc

    inclusive_counts = None
    for inclusive_sample in sample_names:
        if inclusive_sample not in meta_files:
            raise RuntimeError(
                f"Inclusive sample '{inclusive_sample}' is not configured in "
                f"stitch group '{group_name}'"
            )

        meta_file_spec = meta_files[inclusive_sample]
        meta_paths = (
            [meta_file_spec]
            if isinstance(meta_file_spec, str)
            else list(meta_file_spec)
        )
        if not meta_paths:
            raise RuntimeError(
                f"No meta files supplied for inclusive sample '{inclusive_sample}' "
                f"in group '{group_name}'"
            )

        for meta_file in meta_paths:
            if not _meta_file_exists(meta_file):
                raise RuntimeError(
                    f"[StitchingDerivationTask] Meta file not found for sample "
                    f"'{inclusive_sample}' in group '{group_name}': '{meta_file}'"
                )
            values = np.array(
                read_intweight_sign_hist(meta_file, inclusive_sample),
                dtype=np.float64,
            )
            if inclusive_counts is None:
                inclusive_counts = values
            elif len(values) != len(inclusive_counts):
                raise RuntimeError(
                    f"[StitchingDerivationTask] Histogram length mismatch in "
                    f"inclusive sample '{inclusive_sample}' of group '{group_name}'"
                )
            else:
                inclusive_counts += values

    assert inclusive_counts is not None
    nonnegative_counts = np.maximum(inclusive_counts, 0.0)
    total = float(np.sum(nonnegative_counts))
    if total == 0.0:
        raise RuntimeError(
            f"Inclusive samples {sample_names} in group '{group_name}' "
            "has zero nonnegative signed events; cannot calculate binned cross sections"
        )

    return (
        inclusive_cross_section * nonnegative_counts / total
    ).tolist()


def _build_correctionlib_cset(
    all_scale_factors: dict[str, dict[str, list[float]]],
    description: str = "Stitching scale factors derived from counter_intWeightSignSum histograms",
) -> "correctionlib.schemav2.CorrectionSet":
    """Build a correctionlib CorrectionSet from derived scale factors."""
    try:
        import correctionlib.schemav2 as cs  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "correctionlib is required to write stitching scale factors as a "
            "correction JSON. Install it with: pip install correctionlib"
        ) from exc

    corrections = []

    for group_name, sample_factors in all_scale_factors.items():
        n_bins = max(len(v) for v in sample_factors.values())
        edges = [float(k) for k in range(n_bins + 1)]

        cat_items = []
        for sample_name, factors in sample_factors.items():
            padded = factors + [0.0] * (n_bins - len(factors))
            binning = cs.Binning(
                nodetype="binning",
                input="stitch_id",
                edges=edges,
                content=padded,
                flow="clamp",
            )
            cat_items.append(cs.CategoryItem(key=sample_name, value=binning))

        correction = cs.Correction(
            name=group_name,
            description=(
                f"Binwise stitching scale factors for group '{group_name}'. "
                f"Evaluate: cset[\"{group_name}\"].evaluate(sample_name, stitch_id). "
                "b_n(k) = max(C_n(k), 0) / sum_m max(C_m(k), 0), where C_n(k) "
                "is the net signed event count (positive minus negative weight "
                "events) for sample n in stitch bin k."
            ),
            version=1,
            inputs=[
                cs.Variable(
                    name="sample_name",
                    type="string",
                    description="Sample identifier (matches meta_files key in stitch config)",
                ),
                cs.Variable(
                    name="stitch_id",
                    type="int",
                    description=(
                        "Integer stitching bin ID stored per event by "
                        "counterIntWeightBranch"
                    ),
                ),
            ],
            output=cs.Variable(
                name="weight",
                type="real",
                description=(
                    "Stitching scale factor b_n(k) = max(C_n(k), 0) / "
                    "sum_m max(C_m(k), 0)"
                ),
            ),
            data=cs.Category(
                nodetype="category",
                input="sample_name",
                content=cat_items,
            ),
        )
        corrections.append(correction)

    return cs.CorrectionSet(
        schema_version=2,
        description=description,
        corrections=corrections,
    )
