# LAW plotting tasks

Read [plot_tasks.py](../../../../core/python/law/plot_tasks.py), [manifest_plot_tasks.py](../../../../core/python/law/manifest_plot_tasks.py), [plot_config.py](../../../../core/python/plot_config.py), and [renderer guidance](../SKILL.md). Verify module discovery using the [LAW environment reference](../../rdfanalyzer-production/references/law-environment.md).

`MakePlot` consumes a meta file, default histogram name, output file and JSON-encoded process list. It constructs a native request, applies plot options, calls PlottingUtility and verifies the returned result. Follow each process's histogram override, directory, scale and normalization counter. The framework plot-view decorator is a no-op.

`MakePlots` loads a JSON array and constructs workflow branches. `plots_per_branch` must be positive and controls how many requests share a batch/session within a branch. This wrapper's JSON array contract differs from every schema accepted by the native YAML/JSON parser; use its actual `_load_plot_configs` and branch implementation. For performance, measure projection, request preparation and rendering separately and verify output/yield agreement when changing grouping.

`ManifestPlotTask` loads an `OutputManifest` and plot configs, can require `MergeAll` through `merge_input_dir`, and derives its histogram file and region contexts from the manifest. Read how paths are resolved in this task; do not assume every consumer uses the merge task's manifest-relative resolution. Output region directories do not themselves select histogram bins. Trace `{region}` expansion into actual histogram/process selections and sparse options.

## Distinguish real plots from completion stubs

If `rdfanalyzer` cannot import or the configured histogram file is missing, the current `ManifestPlotTask.run()` writes **`.stub.json`** files and records them among `plots_created`. Inspect `provenance.json`, `errors`, warnings, actual file extensions and native result success before reporting a successful physics plot. Existing targets or task completion can reflect stub output. When inputs are available, failed request/result processing raises and should be traced to the earliest concrete reader/renderer error.

Tests: [plot tasks](../../../../core/tests/law/test_plot_tasks.py) and [manifest plots](../../../../core/tests/law/test_manifest_plot_tasks.py). Verify at least one real render and the relevant missing-input/stub behavior when changing the wrapper. Inspect a representative image for layout changes; task-marker existence alone does not validate presentation.
