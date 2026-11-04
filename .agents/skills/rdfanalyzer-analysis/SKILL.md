---
name: rdfanalyzer-analysis
description: Understand, run, or extend an analysis built with RDFAnalyzerCore by tracing its entry point, configuration, RDataFrame graph, managers, weights, and outputs. Use for analysis-specific work; for reusable framework behavior, also use rdfanalyzer-development.
---

# Understand and use an RDFAnalyzer analysis

Use this skill when a task asks what an analysis does, where a selection or weight comes from, how to run it, or where an analysis-level change belongs. An analysis owns its physics decisions; RDFAnalyzerCore supplies the `Analyzer`, the RDataFrame-backed graph, manager/plugin infrastructure, and output lifecycle.

## Load only the relevant task skill

Use the [project map](../rdfanalyzer-analysis-projects/SKILL.md) for checkout-specific entry points, then choose the domain that answers the task. These skills supplement the tracing workflow below; do not read the whole catalog for one request.

| Task | Skill |
|---|---|
| Config files, constants, branches, friends, dataset metadata | [Config and data](../rdfanalyzer-config-data/SKILL.md) |
| Era/payload resolution, compatibility, MET and event cleaning | [CMS](../rdfanalyzer-cms/SKILL.md) |
| Corrected jets/leptons, collection indices, correctionlib | [Object corrections](../rdfanalyzer-object-corrections/SKILL.md) |
| Trigger acceptance, regions, cutflow | [Selection and regions](../rdfanalyzer-selection-regions/SKILL.md) |
| Event product, generator variations, normalization/stitching | [Weights](../rdfanalyzer-weights/SKILL.md) |
| Discriminants, working points, SFs and efficiency maps | [Tagging](../rdfanalyzer-tagging/SKILL.md) |
| Variation propagation and acceptance migrations | [Systematics](../rdfanalyzer-systematics/SKILL.md) |
| Config booking, sparse axes, schemas and projections | [Histograms](../rdfanalyzer-histograms/SKILL.md) |
| QA, response plots, renderer and plotting performance | [Plotting](../rdfanalyzer-plotting/SKILL.md) |
| ONNX, BDT or SOFIE model use | [Inference](../rdfanalyzer-inference/SKILL.md) |
| Particle constraints and CPU/GPU reconstruction fits | [Kinematic fits](../rdfanalyzer-kinematic-fit/SKILL.md) |
| STXS classification and acceptance/migration derivation | [STXS](../rdfanalyzer-stxs/SKILL.md) |
| Skim/meta output, reports, provenance and artifact manifests | [Outputs](../rdfanalyzer-outputs/SKILL.md) |
| Wrappers, LAW, Condor/Dask, staging and recovery | [Production](../rdfanalyzer-production/SKILL.md) |
| Remote file opening, caches and read throughput | [Input I/O](../rdfanalyzer-input-io/SKILL.md) |
| Datacards, nuisance policy and saved fit audits | [Statistics](../rdfanalyzer-statistics/SKILL.md) |
| Python analysis authoring and native wrappers | [Python](../rdfanalyzer-python/SKILL.md) |
| Build wrappers, CI/containers, dependencies and verification | [Build and tests](../rdfanalyzer-build/SKILL.md) |

For reusable manager/service behavior use [development](../rdfanalyzer-development/SKILL.md) and [plugin lifecycle](../rdfanalyzer-plugin-lifecycle/SKILL.md). The [component inventory](../rdfanalyzer-development/references/components.yaml) maps every plugin directory to an owner skill when a class name is the only clue.

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

Before editing, check framework status and the analysis directory's Git root, HEAD, and status independently; `analyses/*` is ignored at the framework root. Before running, check the executable target, input files, data/MC mode, payloads, normalization, path resolution, and output destinations. Build the relevant target and use a small local sample when one is available. Do not assume `run()` writes a skim: verify `enableSkim` and the registered output actions. If behavior changes, update matching analysis docs/configs and add a meaningful regression test when the project has a test harness.

For a framework capability missing from the analysis API, also read `rdfanalyzer-development` and keep the reusable change in the framework. For batch workflows, follow that analysis's actual task graph and the generic [LAW Tasks](../../../docs/LAW_TASKS.md) and [Batch Submission](../../../docs/BATCH_SUBMISSION.md) references. Keep discovery, remote jobs, retries, and shared-storage writes within the user's requested execution scope; a diagnosis alone does not authorize recovery actions.
