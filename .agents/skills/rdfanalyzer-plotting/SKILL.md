---
name: rdfanalyzer-plotting
description: Use or develop RDFAnalyzerCore PlottingUtility, persistent plot sessions, YAML/JSON requests, rdf-plot, uncertainty bands, or plot input projection performance.
---

# Plotting and rendering

For `MakePlot`, `MakePlots`, or `ManifestPlotTask`, read [LAW plotting](references/law-plots.md), including branch/session grouping and completion stubs.

## Identify the layer

Separate histogram reading/projection, normalization/aggregation/uncertainty calculations, and ROOT drawing/export. Start with the requested analysis wrapper before selecting the shared renderer. Read [PLOTTING](../../../docs/PLOTTING.md) and [PlottingUtility.h](../../../core/interface/PlottingUtility.h); inspect implementation files actually included by [src/CMakeLists.txt](../../../core/src/CMakeLists.txt). This checkout includes ongoing plotting work, so verify exports against the built module/executable.

[PlottingInput.cc](../../../core/src/PlottingInput.cc) prepares inputs; [PlottingConfig.cc](../../../core/src/PlottingConfig.cc) parses shared requests; [PlottingSession.cc](../../../core/src/PlottingSession.cc) manages persistent rendering; [PlottingUtility.cc](../../../core/src/PlottingUtility.cc) contains supporting behavior. The CLI is [core/tools/plot.cc](../../../core/tools/plot.cc). Python request parsing is exposed through the bindings and [plot_config.py](../../../core/python/plot_config.py).

## Preserve the physics contract

- Trace `scale` and normalization-counter division with units. Physical scale and signal `displayScale` affect different things. Verify grouped processes, data sums, and overlays before interpreting ratios.
- Preserve sumw2 for signed weights, nuisance names/correlations when grouping processes, bin edges, and flow policy. Read the implemented uncertainty reduction; do not infer it from a band label.
- Respect configured blind bins in plots, reports, and `.C` export. Verify that the presentation change does not leak blinded values into another output.
- Keep ROOT drawable objects alive through paint/export. Use one `PlottingUtility::Session` for a campaign when reuse is wanted. Input-preparation workers and independent renderer processes are separate concurrency controls; follow the implementation before using threads around ROOT drawing.

For schema decoding/projection use [histograms](../rdfanalyzer-histograms/SKILL.md). For a requested performance improvement, measure the actual wrapper on bounded representative inputs, identify the dominant stage, implement the change, and compare wall time and numerical results. A rendering microbenchmark does not establish end-to-end projection/systematic speed.

## Verify

Use [testPlottingUtility](../../../core/tests/cpp/testPlottingUtility.cc), [testPlottingSession](../../../core/tests/cpp/testPlottingSession.cc), [binding tests](../../../core/tests/cpp/test_plotting_bindings.py), and the relevant [LAW plot tests](../../../core/tests/law/test_plot_tasks.py). Validate successful files plus the numerical input/result contract; visually inspect a representative plot if layout changed. See [project map](../rdfanalyzer-analysis-projects/SKILL.md) for analysis-specific wrappers.
