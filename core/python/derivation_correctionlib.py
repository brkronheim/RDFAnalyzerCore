"""Reusable correctionlib builders for stage-1 derivations.

This module currently hosts the stitching derivation helpers extracted from the
LAW task implementation. The functions are kept deliberately small and
framework-facing so later tagger and STXS builders can live beside them.
"""

from __future__ import annotations

import os
from typing import Callable, Optional


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


def _derive_stitch_weights(
    group_name: str,
    meta_files: dict[str, str],
    read_intweight_sign_hist: Callable[[str, str], list[float]] | None = None,
) -> dict[str, list[float]]:
    """Compute per-bin stitching scale factors for one group."""
    import numpy as np  # type: ignore

    if read_intweight_sign_hist is None:
        read_intweight_sign_hist = _read_intweight_sign_hist

    sample_names = list(meta_files.keys())

    sign_counts: dict[str, "np.ndarray"] = {}
    ref_len: Optional[int] = None

    for sample_name, meta_file in meta_files.items():
        if not os.path.isfile(meta_file):
            raise RuntimeError(
                f"[StitchingDerivationTask] Meta file not found for sample "
                f"'{sample_name}' in group '{group_name}': '{meta_file}'"
            )
        values = read_intweight_sign_hist(meta_file, sample_name)
        arr = np.array(values, dtype=np.float64)

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

    total = np.zeros(ref_len, dtype=np.float64)
    for arr in sign_counts.values():
        total += arr

    scale_factors: dict[str, "np.ndarray"] = {}
    with np.errstate(invalid="ignore", divide="ignore"):
        for sample_name in sample_names:
            c_n = sign_counts[sample_name]
            scale_factors[sample_name] = np.where(total != 0.0, c_n / total, 0.0)

    non_zero_mask = total != 0.0
    if np.any(non_zero_mask):
        last_active = int(np.where(non_zero_mask)[0][-1])
        n_used = last_active + 1
    else:
        n_used = 1

    return {name: arr[:n_used].tolist() for name, arr in scale_factors.items()}


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
                "b_n(k) = C_n(k) / sum_m C_m(k) where C_n(k) is the net signed "
                "event count (positive minus negative weight events) for sample n "
                "in stitch bin k."
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
                description="Stitching scale factor b_n(k) = C_n(k) / sum_m C_m(k)",
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
