#!/usr/bin/env python3
"""Build a dataset manifest by injecting file lists from JSON payloads."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dataset_manifest import DatasetManifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-manifest", required=True, help="Base manifest YAML path")
    parser.add_argument("--filelist-dir", required=True, help="Directory containing per-sample JSON files")
    parser.add_argument("--output", required=True, help="Output manifest YAML path")
    return parser.parse_args()


def _collect_filelists(filelist_dir: Path) -> dict[str, list[str]]:
    filelists: dict[str, list[str]] = {}
    for json_path in sorted(filelist_dir.glob("*.json")):
        if json_path.name.endswith(".perf.json"):
            continue
        with json_path.open() as handle:
            payload = json.load(handle)
        sample = str(payload.get("sample", "")).strip()
        files = [str(value).strip() for value in payload.get("files", []) if str(value).strip()]
        if sample and files:
            filelists[sample] = files
    return filelists


def main() -> int:
    args = parse_args()
    manifest = DatasetManifest.load_yaml(args.base_manifest)
    filelists = _collect_filelists(Path(args.filelist_dir))

    for entry in manifest.datasets:
        files = filelists.get(entry.name)
        if files:
            entry.files = files
            entry.das = None

    manifest.save_yaml(args.output)
    DatasetManifest.load_yaml(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
