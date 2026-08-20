#!/usr/bin/env python3
"""Convert VHbb-style samples.json into a DatasetManifest YAML file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dataset_manifest import DatasetEntry, DatasetManifest


def _infer_process(sample_name: str) -> str:
    lower = sample_name.lower()
    if lower.startswith("zh"):
        return "zh"
    if lower.startswith("ggzh"):
        return "ggzh"
    if lower.startswith("wplush") or "whplus" in lower or "whplus" in lower:
        return "wh_plus"
    if lower.startswith("wminush") or "whminus" in lower:
        return "wh_minus"
    if "data" in lower:
        return "data_obs"
    if "tt" in lower:
        return "ttbar"
    if "wjets" in lower or "dy" in lower or "zjets" in lower:
        return "vjets"
    if lower.startswith("wz") or lower.startswith("zz") or lower.startswith("ww"):
        return "diboson"
    return lower


def _infer_group(process: str, is_mc: bool) -> str:
    if not is_mc:
        return "data"
    if process in {"zh", "ggzh", "wh_plus", "wh_minus"}:
        return "signal"
    if process == "ttbar":
        return "ttbar"
    if process == "vjets":
        return "vjets"
    if process == "diboson":
        return "diboson"
    return "background"


def convert(samples_path: Path, output_path: Path, lumi_by_year: dict[int, float]) -> None:
    with samples_path.open() as handle:
        raw: dict[str, Any] = json.load(handle)

    datasets: list[DatasetEntry] = []
    for sample_key, payload in raw.items():
        files = payload.get("files", [])
        for file_block in files:
            metadata = file_block.get("metadata", {})
            das_names = file_block.get("das_names", [])
            if not das_names:
                continue

            year_label = str(metadata.get("year", "")).strip()
            if not year_label:
                continue

            year_number = int(year_label[:4])
            is_mc = bool(metadata.get("isMC", True))
            sample_name = str(payload.get("sample", sample_key)).strip()
            process = _infer_process(sample_name)
            group = _infer_group(process, is_mc)
            era = None
            if not is_mc:
                era = year_label

            entry_name = f"{sample_name}_{year_label}"
            das = ",".join(str(value).strip() for value in das_names if str(value).strip())
            xsec = metadata.get("xsec")
            entry = DatasetEntry(
                name=entry_name,
                year=year_number,
                era=era,
                campaign=year_label,
                process=process,
                group=group,
                dtype="mc" if is_mc else "data",
                xsec=float(xsec) if xsec is not None else None,
                das=das,
            )
            datasets.append(entry)

    manifest = DatasetManifest(datasets=datasets, lumi=1.0, lumi_by_year=lumi_by_year)
    manifest.save_yaml(str(output_path))
    # Validate by re-loading through the production parser.
    DatasetManifest.load_yaml(str(output_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples-json", required=True, help="Path to VHbb samples.json")
    parser.add_argument("--output", required=True, help="Output dataset manifest YAML path")
    parser.add_argument(
        "--lumi-by-year",
        default="2022:8.077,2022EE:26.671,2023:17.794,2023BPix:9.451",
        help=(
            "Comma-separated year:lumi mapping for manifest metadata. "
            "Non-integer year tags are ignored."
        ),
    )
    return parser.parse_args()


def _parse_lumi_map(spec: str) -> dict[int, float]:
    out: dict[int, float] = {}
    for token in spec.split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        key, value = token.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key[:4].isdigit():
            continue
        out[int(key[:4])] = float(value)
    return out


def main() -> int:
    args = parse_args()
    convert(Path(args.samples_json), Path(args.output), _parse_lumi_map(args.lumi_by_year))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
