#!/usr/bin/env python3
"""Convert block-structured samples JSON into a DatasetManifest YAML file."""

from __future__ import annotations

import argparse
import json
import stat
import tempfile
from pathlib import Path
from typing import Any, Callable

from dataset_manifest import DatasetEntry, DatasetManifest


def convert(
    samples_path: Path, output_path: Path, lumi_by_year: dict[int, float],
    *, process_classifier: Callable[[str], str] = str.lower,
    group_classifier: Callable[[str, bool], str] = lambda process, is_mc: "background" if is_mc else "data",
) -> None:
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
            flag = metadata.get("isMC", True)
            token = str(flag).strip().lower()
            if token not in {"true", "false", "1", "0"}:
                raise ValueError(f"{sample_key}: invalid isMC flag {flag!r}")
            is_mc = token in {"true", "1"}
            sample_name = str(payload.get("sample", sample_key)).strip()
            process = process_classifier(sample_name)
            group = group_classifier(process, is_mc)

            entry_name = f"{sample_name}_{year_label}"
            das = ",".join(str(value).strip() for value in das_names if str(value).strip())
            xsec = metadata.get("xsec")
            entry = DatasetEntry(
                name=entry_name,
                year=year_number,
                era=None if is_mc else year_label,
                campaign=year_label,
                process=process,
                group=group,
                dtype="mc" if is_mc else "data",
                xsec=float(xsec) if xsec is not None else None,
                das=das,
            )
            datasets.append(entry)

    manifest = DatasetManifest(datasets=datasets, lumi=1.0, lumi_by_year=lumi_by_year)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    target = output_path.resolve()
    with tempfile.TemporaryDirectory(dir=target.parent) as directory:
        candidate = Path(directory) / "manifest.yaml"
        manifest.save_yaml(str(candidate))
        DatasetManifest.load_yaml(str(candidate))
        try:
            mode = target.stat().st_mode
        except FileNotFoundError:
            pass  # save_yaml created the candidate with normal file permissions.
        else:
            if not stat.S_ISREG(mode):
                raise ValueError(f"Expected a regular manifest file at {target}")
            candidate.chmod(stat.S_IMODE(mode))
        candidate.replace(target)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples-json", required=True, help="Path to block-structured samples JSON")
    parser.add_argument("--output", required=True, help="Output dataset manifest YAML path")
    parser.add_argument(
        "--lumi-by-year",
        default="",
        help=(
            "Comma-separated year:lumi mapping for manifest metadata. "
            "Use an annual value for each year. Era tags cannot supply an annual luminosity."
        ),
    )
    return parser.parse_args()


def _parse_lumi_map(spec: str) -> dict[int, float]:
    out: dict[int, float] = {}
    era_years: set[int] = set()
    for token in spec.split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        key, value = token.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key.isdecimal():
            if len(key) > 4 and key[:4].isdecimal():
                era_years.add(int(key[:4]))
            continue
        out[int(key)] = float(value)
    missing = sorted(era_years - out.keys())
    if missing:
        raise ValueError(f"Provide annual year:lumi values for {missing}; era luminosities are ambiguous")
    return out


def main() -> int:
    args = parse_args()
    convert(Path(args.samples_json), Path(args.output), _parse_lumi_map(args.lumi_by_year))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
