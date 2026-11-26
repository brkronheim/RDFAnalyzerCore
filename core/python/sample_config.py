"""Experiment-independent YAML and legacy sample-config loading."""
from __future__ import annotations
import warnings
from dataclasses import dataclass, field
from pathlib import Path
if __package__:
    from .dataset_manifest import DatasetEntry, DatasetManifest
else:
    from dataset_manifest import DatasetEntry, DatasetManifest

@dataclass(frozen=True)
class SampleConfigData:
    """Typed view of a sample-config file used by submission planning code."""

    entries: dict[str, DatasetEntry]
    base_dirs: list[str]
    lumi: float
    whitelist: list[str]
    blacklist: list[str]
    record_ids: list[str] = field(default_factory=list)


def legacy_sample_records(config_file: str, *, strict=False):
    with open(config_file, encoding="utf-8") as stream:
        for lineno, raw in enumerate(stream, 1):
            line = raw.partition("#")[0].strip()
            entry = {}
            for token in line.split():
                key, eq, value = token.partition("=")
                if not key or not eq:
                    if strict:
                        raise ValueError(f"{config_file}:{lineno}: expected key=value, got {token!r}")
                    continue
                if key in entry:
                    raise ValueError(f"{config_file}:{lineno}: duplicate key {key!r}")
                entry[key] = value
            if entry:
                yield lineno, entry


def load_legacy_sample_config(config_file: str, *, strict=False) -> SampleConfigData:
    """Load a legacy key=value sample-config file as typed sample data.

    This compatibility adapter is intentionally explicit. New workflows should
    prefer YAML manifests and `load_sample_config()` on ``.yaml`` / ``.yml``
    files. Legacy ``.txt`` sample configs remain supported only through this
    compatibility helper.
    """
    entries: dict[str, DatasetEntry] = {}
    base_dirs: list[str] = []
    lumi = 1.0
    whitelist: list[str] = []
    blacklist: list[str] = []

    record_ids = []
    for lineno, inner in legacy_sample_records(config_file, strict=strict):
        if "name" in inner:
            if strict:
                for key, parser in (("year", int), ("stitch_id", int), ("sample_type", int),
                                    ("xsec", float), ("norm", float), ("filterEfficiency", float),
                                    ("kfac", float), ("extraScale", float), ("energy", float)):
                    if key in inner:
                        try:
                            parser(inner[key])
                        except ValueError:
                            raise ValueError(f"{config_file}:{lineno}: invalid {key} value {inner[key]!r}") from None
            entry = DatasetEntry.from_legacy_dict(inner)
            if entry.name in entries:
                raise ValueError(f"{config_file}:{lineno}: duplicate sample name {entry.name!r}")
            entries[entry.name] = entry
            continue
        if "prefix_cern" in inner:
            base_dirs.append(inner["prefix_cern"])
        if "lumi" in inner:
            lumi = float(inner["lumi"])
        if "WL" in inner:
            whitelist = [item for item in inner["WL"].split(",") if item]
        if "BL" in inner:
            blacklist = [item for item in inner["BL"].split(",") if item]
        if "recids" in inner:
            ids = [item for item in inner["recids"].split(",") if item]
            if strict and not all(item.isdigit() for item in ids):
                raise ValueError(f"{config_file}:{lineno}: recids must be comma-separated record numbers")
            record_ids.extend(ids)
        unknown = set(inner) - {"prefix_cern", "lumi", "WL", "BL", "recids"}
        if strict and unknown:
            raise ValueError(f"{config_file}:{lineno}: unrecognized sample option(s) {sorted(unknown)}")

    return SampleConfigData(
        entries=entries,
        base_dirs=base_dirs,
        lumi=lumi,
        whitelist=whitelist,
        blacklist=blacklist,
        record_ids=record_ids,
    )


def convert_legacy_manifest(path):
    data = load_legacy_sample_config(str(path), strict=True)
    manifest = DatasetManifest(datasets=list(data.entries.values()), lumi=data.lumi,
                               whitelist=data.whitelist, blacklist=data.blacklist,
                               base_dirs=data.base_dirs, record_ids=data.record_ids)
    errors = [msg for msg in manifest.validate() if not msg.startswith("Warning:")]
    if errors:
        raise ValueError(f"{path}: " + "; ".join(errors))
    return manifest


def load_sample_config(config_file: str) -> SampleConfigData:
    """Return a typed representation of a sample-config file."""
    ext = Path(config_file).suffix.lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        return SampleConfigData(
            entries={entry.name: entry for entry in manifest.datasets},
            base_dirs=manifest.base_dirs,
            lumi=manifest.lumi,
            whitelist=manifest.whitelist,
            blacklist=manifest.blacklist,
            record_ids=manifest.record_ids,
        )

    if ext == ".txt":
        warnings.warn(
            "Legacy text sample configs are deprecated. Convert to YAML manifests "
            "and use load_sample_config() on .yaml/.yml files; this compatibility "
            "helper will continue to parse .txt files for legacy workflows.",
            DeprecationWarning,
            stacklevel=2,
        )
        return load_legacy_sample_config(config_file)

    raise ValueError(
        "Unsupported sample config extension. Use .yaml/.yml for YAML manifests. "
        "Legacy .txt files may be loaded explicitly with load_legacy_sample_config()."
    )

