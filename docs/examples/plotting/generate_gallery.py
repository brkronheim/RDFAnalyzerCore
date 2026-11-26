#!/usr/bin/env python3
"""Generate reproducible plotting examples and optionally render them.

python docs/examples/plotting/generate_gallery.py --output build/plot-gallery \
    --renderer build/core/rdf-plot --matplotlib
"""
from __future__ import annotations

import argparse
import copy
import json
import math
from pathlib import Path
import random
import subprocess


def requests(output: Path):
    edges = [50 + 5 * i for i in range(31)]
    centers = [(a + b) / 2 for a, b in zip(edges[:-1], edges[1:])]
    light = [220 * math.exp(-(x - 50) / 55) for x in centers]
    top = [42 * math.exp(-(x - 50) / 90) for x in centers]
    diboson = [18 * math.exp(-(x - 50) / 75) for x in centers]
    signal = [10 * math.exp(-0.5 * ((x - 125) / 12) ** 2) for x in centers]
    total = [sum(v) for v in zip(light, top, diboson, signal)]
    rng = random.Random(1729)
    data = [max(0, round(rng.gauss(y, math.sqrt(y)))) for y in total]
    series = []
    for label, color, values in zip(
        ["V+light jets", "Top", "Diboson", "VH signal"],
        ["#4477AA", "#CC6677", "#66AA88", "#CCAA44"],
        [light, top, diboson, signal],
    ):
        series.append({"legendLabel": label, "color": color, "values": values,
                       "variances": [0.7 * value for value in values]})
    series[-1].update(isSignal=True, displayScale=5)
    series.append({"legendLabel": "Data", "isData": True, "values": data, "variances": data})
    base = {"outputFile": str(output / "stack.png"), "title": "Z #rightarrow #mu#mu, at least two jets",
            "experiment": "cms", "experimentExtra": "Simulation", "luminosityLabel": "100 fb^{-1} (13.6 TeV)",
            "xAxisTitle": "Dijet mass [GeV]", "yAxisTitle": "Events / 5 GeV",
            "binEdges": edges, "panels": ["ratio"], "processes": series,
            "systematicLow": [0.06 * y for y in total], "systematicHigh": [0.08 * y for y in total]}
    result = [base]
    four = copy.deepcopy(base)
    four.update(outputFile=str(output / "signal_panels.png"),
                panels=["significance", "signal_background", "ratio"],
                blindBins=[110 <= x <= 140 for x in centers],
                additionalOutputs=[str(output / "signal_panels.pdf"), str(output / "signal_panels.svg")])
    result.append(four)
    log = copy.deepcopy(base)
    log.update(outputFile=str(output / "stack_log.png"), logY=True)
    result.append(log)
    categorical = copy.deepcopy(base)
    categorical.update(outputFile=str(output / "categories.png"), title="Categorical fit input",
                       binLabels=[f"{int(a)}-{int(b)}" for a, b in zip(edges[:-1], edges[1:])])
    result.append(categorical)
    overlay = copy.deepcopy(base)
    overlay.update(outputFile=str(output / "overlay.png"), title="Generator comparison", kind="overlay",
                   processes=series[:2], drawRatio=True, panels=["ratio"], signalOverlay=False,
                   experiment="atlas", experimentExtra="Simulation")
    result.append(overlay)
    matrix = []
    rows, columns = 16, 12
    for row in range(rows):
        for column in range(columns):
            matrix.append(200 * math.exp(-0.5 * ((row - column) / 1.3) ** 2)
                          - 12 * math.exp(-0.5 * ((row - column - 3) / 1.5) ** 2))
    result.append({"kind": "heatmap", "outputFile": str(output / "response.png"),
                   "title": "Signed detector response", "experimentExtra": "Simulation", "luminosityLabel": "100 fb^{-1} (13.6 TeV)",
                   "xAxisTitle": "Reconstructed p_{T} bin", "yAxisTitle": "Generator category",
                   "zAxisTitle": "Weighted events", "binEdges": list(range(columns + 1)),
                   "yBinEdges": list(range(rows + 1)), "matrix": matrix,
                   "yBinLabels": [f"Z+jets, n_{{jet}} = {row // 4}, p_{{T}} bin {row % 4}" for row in range(rows)],
                   "horizontalSeparators": [4, 8, 12], "style": {"height": 900}})
    for spec in result:
        spec.setdefault("additionalOutputs", []).append(str(Path(spec["outputFile"]).with_suffix(".C")))
    return result


def matplotlib_reference(spec, destination):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    edges = np.array(spec["binEdges"])
    centers = (edges[:-1] + edges[1:]) / 2
    fig, (ax, ratio) = plt.subplots(2, 1, figsize=(10, 8), sharex=True,
                                   gridspec_kw={"height_ratios": [3, 1], "hspace": 0.05})
    total = np.zeros(len(centers))
    variance = np.zeros_like(total)
    for series in spec["processes"][:-1]:
        values = np.array(series["values"])
        ax.stairs(total + values, edges, baseline=total, fill=True,
                  color=series["color"], label=series["legendLabel"])
        total += values
        variance += series["variances"]
    data = np.array(spec["processes"][-1]["values"])
    ax.stairs(total + np.sqrt(variance), edges, baseline=total - np.sqrt(variance),
              fill=True, color="black", alpha=0.15)
    ax.errorbar(centers, data, yerr=np.sqrt(data), fmt="o", color="black", markersize=4)
    ratio.errorbar(centers, data / total, yerr=np.sqrt(data) / total,
                   fmt="o", color="black", markersize=4)
    ratio.stairs(1 + np.sqrt(variance) / total, edges,
                  baseline=1 - np.sqrt(variance) / total, fill=True, color="black", alpha=0.15)
    ratio.axhline(1, color="gray", linestyle="--", linewidth=1)
    ratio.set_ylim(0.5, 1.5)
    ratio.set_ylabel("Data / MC")
    ratio.set_xlabel("Dijet mass [GeV]")
    ax.set_ylabel("Events / 5 GeV")
    ax.set_title("Z → μμ, at least two jets", loc="left")
    ax.legend(frameon=False, ncol=2)
    fig.subplots_adjust(left=0.14, right=0.955, top=0.92, bottom=0.10)
    fig.savefig(destination, dpi=100)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("build/plot-gallery"))
    parser.add_argument("--renderer", type=Path)
    parser.add_argument("--matplotlib", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    specs = requests(args.output)
    config = args.output / "plots.json"
    config.write_text(json.dumps({"schema_version": 1, "plots": specs}, indent=2) + "\n")
    if args.renderer:
        subprocess.run([str(args.renderer), "--config", str(config), "--report", str(args.output / "report.yaml")], check=True)
    if args.matplotlib:
        matplotlib_reference(specs[0], args.output / "matplotlib_reference.png")


if __name__ == "__main__":
    main()
