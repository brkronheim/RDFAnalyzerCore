"""Shared histogram input selection and auditable MC normalization."""
import math
from pathlib import Path
import re

from dataset_manifest import normalize_generator_weight_mode


def safe_dataset_name(name):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name)).strip("._") or "dataset"


def dataset_files(directory, name):
    """Choose a final result, then a snapshot, then disjoint job outputs."""
    directory = Path(directory)
    names = list(dict.fromkeys((str(name), safe_dataset_name(name))))
    for suffix in (".root", "_partial.root"):
        for stem in names:
            path = directory / (stem + suffix)
            if path.is_file():
                return [path.resolve()]
    matches = set()
    for stem in names:
        matches.update(path.resolve() for path in directory.glob(stem + "_*.root")
                       if path.is_file() and not path.name.endswith("_partial.root"))
        if (directory / stem).is_dir():
            matches.update(path.resolve() for path in (directory / stem).rglob("*.root")
                           if path.is_file())
    return sorted(matches)


def dataset_luminosity(manifest, dataset):
    # Keep the same calendar-year precedence as DatasetManifest.lumi_for.
    year = dataset.get("year")
    by_year = manifest.get("lumi_by_year", {})
    value = by_year.get(year, by_year.get(str(year))) if isinstance(by_year, dict) else None
    value = manifest.get("lumi", 0) if value is None else value
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"invalid luminosity for {dataset.get('name')!r}: {value}")
    return value


def normalization_record(dataset, denominator, lumi_fb, *, inputs=(), allow_negative=False):
    """Compute all factors once and retain the selected denominator's meaning."""
    mode = normalize_generator_weight_mode(dataset.get("generator_weight_mode", "full"))
    denominator, lumi_fb = float(denominator), float(lumi_fb)
    if not math.isfinite(denominator) or denominator == 0 or (denominator < 0 and not allow_negative):
        raise ValueError(f"invalid {mode} generator-weight denominator for {dataset.get('name')!r}: {denominator}")
    if not math.isfinite(lumi_fb) or lumi_fb <= 0:
        raise ValueError("luminosity must be finite and positive")
    factors = {key: float(dataset[key]) if dataset.get(key) is not None else default
               for key, default in (("xsec", math.nan), ("filter_efficiency", 1.),
                                    ("kfac", 1.), ("extra_scale", 1.))}
    if not all(math.isfinite(value) for value in factors.values()):
        raise ValueError(f"invalid normalization factors for {dataset.get('name')!r}: {factors}")
    scale = math.prod(factors.values()) * lumi_fb * 1000. / denominator
    if not math.isfinite(scale):
        raise ValueError("non-finite normalization scale")
    return dict(scale=scale, denominator=denominator, generator_weight_mode=mode,
                denominator_name="sum_sign_weights" if mode == "sign" else "sum_weights",
                lumi_fb=lumi_fb, factors=factors, inputs=list(map(str, inputs)))
