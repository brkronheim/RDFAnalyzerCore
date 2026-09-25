---
name: rdfanalyzer-analysis
description: Understand, run, or extend an analysis built with RDFAnalyzerCore by tracing its entry point, configuration, RDataFrame graph, managers, weights, and outputs. Use for analysis-specific work; for reusable framework behavior, also use rdfanalyzer-development.
---

# Understand and use an RDFAnalyzer analysis

Use this skill when a task asks what an analysis does, where a selection or weight comes from, how to run it, or where an analysis-level change belongs. An analysis owns its physics decisions; RDFAnalyzerCore supplies the `Analyzer`, the RDataFrame-backed graph, manager/plugin infrastructure, and output lifecycle.

## Trace an analysis

1. **Locate the executable and entry point.** Read the analysis `CMakeLists.txt`, identify its target and source, and inspect `main()`. Record how `Analyzer` is constructed, which configuration it receives, which managers are created or called, and where execution starts.
2. **Resolve the configuration graph.** Follow the main config into plugin configs, input lists or manifests, output/tree settings, histogram definitions, and correction/model payloads. Record data/MC mode, years, luminosity, normalization, and path-resolution behavior. Check the framework implementation when relative-path behavior matters.
3. **Follow the event graph.** Match input branches to compatibility/correction columns and analysis-specific `Define`s. Then list `Filter`s in order. Distinguish event-rejecting filters from columns used only for histogram categories or regions.
4. **Trace weights and systematics.** Find the final event-weight column and trace each factor to its source. Check data/MC branches, generator-weight normalization, optional corrections, and how varied inputs or derived columns are connected. Registration or configuration alone does not prove a variation is applied.
5. **Trace booked actions and outputs.** Match histogram variables and axes to defined columns and booking calls. Inspect `Analyzer::run()` and the output managers to determine whether the configured run writes a skim tree, histograms, counters, provenance, or a combination. Use manifests and task configs only when the analysis actually has a production workflow.
6. **Report evidence.** Explain the analysis as a concise pipeline and cite the source/config locations for thresholds, weights, regions, and outputs. When prose disagrees with executable code, report the mismatch and establish intended behavior before changing physics logic.

## Configure numeric selections

Analysis-level numeric cut values, working-point thresholds, and region
boundaries should live in clearly commented `floatConfig` and `intConfig`
files. RDFAnalyzerCore's `DataManager` registers those values as per-sample
dataframe columns before analysis `Define`s run, so selection functions should
consume the named columns rather than embedding numeric literals. Keep category
codes and axis labels in the same configuration graph, and ensure the files are
referenced by every local and staged run configuration. This makes selection
changes reviewable and allows iteration without recompiling the analysis.

For framework behavior, start with [Architecture](../../../docs/ARCHITECTURE.md) and [API Reference](../../../docs/API_REFERENCE.md). Treat `core/interface/` as the public contract, then use `core/src/` and the relevant `core/plugins/` implementation to resolve behavior. The [analysis walkthrough](../../../docs/ANALYSIS_WALKTHROUGH.md) demonstrates this tracing method with `analyses/ExampleAnalysis/`; its physics and configuration choices are illustrative, not framework defaults.

## Run or change an analysis

Before running, check the executable target, input files, data/MC mode, payloads, normalization, path resolution, and output destinations. Build the relevant target and use a small local sample when one is available. Do not assume `run()` writes a skim: verify `enableSkim` and the registered output actions. If behavior changes, update matching analysis docs/configs and add a meaningful regression test when the project has a test harness.

For a framework capability missing from the analysis API, also read `rdfanalyzer-development` and keep the reusable change in the framework. For batch workflows, follow that analysis's actual task graph and the generic [LAW Tasks](../../../docs/LAW_TASKS.md) and [Batch Submission](../../../docs/BATCH_SUBMISSION.md) references. Do not start discovery, remote jobs, retries, or shared-storage writes unless the user requested those actions.
