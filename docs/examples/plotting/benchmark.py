#!/usr/bin/env python3
"""Measure warmed sessions, one-shot wrappers, and an optional matplotlib reference."""
import argparse
import json
from pathlib import Path
import resource
import sys
import time

from generate_gallery import requests, matplotlib_reference


def rss_kib():
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module-dir", type=Path, default=Path("build/python"))
    parser.add_argument("--output", type=Path, default=Path("build/plot-benchmark"))
    parser.add_argument("--plots", type=int, default=1000)
    parser.add_argument("--single-shots", type=int, default=20)
    parser.add_argument("--matplotlib", type=int, default=0)
    args = parser.parse_args()
    sys.path.insert(0, str(args.module_dir.resolve()))
    import rdfanalyzer as rdf
    args.output.mkdir(parents=True, exist_ok=True)
    spec = requests(args.output)[0]
    spec["additionalOutputs"] = []  # This workload measures PNG export only.
    request = rdf.PlottingUtility.parseRequests(json.dumps([spec]))[0]
    session = rdf.PlotSession()
    # Warm ROOT's painters, image encoders, fonts, drawable arrays, and filesystem.
    for _ in range(5):
        result = session.render(request)
        if not result.success:
            raise RuntimeError(result.message)
    warm = session.stats()
    rss_before = rss_kib()
    start = time.perf_counter()
    for _ in range(args.plots):
        result = session.render(request)
        if not result.success:
            raise RuntimeError(result.message)
    elapsed = time.perf_counter() - start
    after = session.stats()
    report = {"persistent": {
        "plots": args.plots, "seconds": elapsed, "plotsPerSecond": args.plots / elapsed,
        "newDrawableObjects": after.drawableObjects - warm.drawableObjects,
        "bufferGrowths": after.bufferGrowths - warm.bufferGrowths,
        "rssBeforeKiB": rss_before, "rssAfterKiB": rss_kib(),
        "peakRssKiB": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "preparationSeconds": after.preparationSeconds - warm.preparationSeconds,
        "layoutSeconds": after.renderingSeconds - warm.renderingSeconds,
        "paintAndExportSeconds": after.exportSeconds - warm.exportSeconds,
    }}
    if args.single_shots:
        utility = rdf.PlottingUtility()
        start = time.perf_counter()
        for _ in range(args.single_shots):
            result = utility.makeStackPlot(request)
            if not result.success:
                raise RuntimeError(result.message)
        report["oneShot"] = {"plots": args.single_shots, "seconds": time.perf_counter() - start}
    if args.matplotlib:
        matplotlib_reference(spec, args.output / "matplotlib.png")
        start = time.perf_counter()
        for _ in range(args.matplotlib):
            matplotlib_reference(spec, args.output / "matplotlib.png")
        report["matplotlibReference"] = {
            "plots": args.matplotlib, "seconds": time.perf_counter() - start,
            "note": "Illustrative comparison: similar 1000px two-panel figure; styling/content are not identical.",
        }
    (args.output / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
