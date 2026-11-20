"""Experiment-independent YAML and legacy sample-config loading."""
from __future__ import annotations
import warnings
from dataclasses import dataclass
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


def load_legacy_sample_config(config_file: str) -> SampleConfigData:
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

    with open(config_file) as fh:
        for raw_line in fh:
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue

            inner: dict[str, str] = {}
            for part in line.split():
                if "=" not in part:
                    continue
                key, value = part.split("=", 1)
                inner[key] = value

            if "name" in inner:
                entry = DatasetEntry.from_legacy_dict(inner)
                entries[entry.name] = entry
            elif "prefix_cern" in inner:
                base_dirs.append(inner["prefix_cern"])
            elif "lumi" in inner:
                lumi = float(inner["lumi"])
            elif "WL" in inner:
                whitelist = [item for item in inner["WL"].split(",") if item]
            elif "BL" in inner:
                blacklist = [item for item in inner["BL"].split(",") if item]

    return SampleConfigData(
        entries=entries,
        base_dirs=base_dirs,
        lumi=lumi,
        whitelist=whitelist,
        blacklist=blacklist,
    )


def load_sample_config(config_file: str) -> SampleConfigData:
    """Return a typed representation of a sample-config file."""
    ext = Path(config_file).suffix.lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        return SampleConfigData(
            entries={entry.name: entry for entry in manifest.datasets},
            base_dirs=[],
            lumi=manifest.lumi,
            whitelist=manifest.whitelist,
            blacklist=manifest.blacklist,
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

