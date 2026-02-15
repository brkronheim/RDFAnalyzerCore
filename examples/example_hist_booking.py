#!/usr/bin/env python3
"""
Example: ND histogram booking structs from RDFAnalyzerCore Python bindings.

Demonstrates:
- C++-style method names (Define/Filter)
- HistInfo and SelectionInfo construction in Python
- NDHistogramManager plugin setup and booking helper usage
"""

from __future__ import annotations

import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


sys.path.insert(0, str(_repo_root() / "build" / "python"))

import rdfanalyzer  # type: ignore  # noqa: E402


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python examples/example_hist_booking.py <config_file>")
        return 1

    analyzer = rdfanalyzer.Analyzer(sys.argv[1])

    analyzer.Define("pt_gev", "pt / 1000.0", ["pt"])
    analyzer.Filter("baseline", "pt_gev > 20.0", ["pt_gev"])

    analyzer.AddPlugin("hist", "NDHistogramManager")

    hist_info = rdfanalyzer.HistInfo(
        "h_pt_gev",
        "pt_gev",
        "p_{T} [GeV]",
        "pt_gev",
        40,
        0.0,
        200.0,
    )

    selection = rdfanalyzer.SelectionInfo(
        "baseline",
        2,
        0.0,
        2.0,
        ["inclusive"],
    )

    region_names = analyzer.bookNDHistograms(
        "hist",
        [hist_info],
        [selection],
        "nominal",
    )
    print(f"Booked ND histogram regions: {region_names}")

    analyzer.saveNDHistograms("hist", [[hist_info]], region_names, "nominal")
    analyzer.clearNDHistograms("hist")

    analyzer.save()
    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
