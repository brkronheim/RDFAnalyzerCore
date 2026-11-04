---
name: rdfanalyzer-selection-regions
description: Trace triggers, dataset overlap, filters, region hierarchies, and cutflow accounting in RDFAnalyzerCore analyses and selection plugins.
---

# Triggers, regions, and cutflows

## Find where events are accepted

Trace `Analyzer::Filter` calls, boolean/category definitions, region declarations, and histogram selection axes separately. A category column does not reject events until a consumer filters or indexes it. Use [systematics](../rdfanalyzer-systematics/SKILL.md) before changing acceptance; keep analysis thresholds in its configured constants.

[TriggerManager](../../../core/plugins/TriggerManager/TriggerManager.cc) reads `triggerConfig` records with `name`, `sample`, and `triggers`. Follow its selection methods, available HLT columns, `primaryDataset`, and legacy `type` handling, then the analysis call site. Verify data-stream overlap policy from the executable path. A trigger decision and a trigger scale factor are separate operations; see [weights](../rdfanalyzer-weights/SKILL.md).

[RegionManager](../../../core/plugins/RegionManager/RegionManager.h) captures a base dataframe at the first declaration, builds independent root/child filtered nodes, and leaves the main analysis node unchanged. Define all region boolean inputs before that capture; parents must exist before children. Overlapping root regions are allowed by the model and can cause multiple histogram fills. Check parent chains and exclusivity only where the analysis requires it.

[CutflowManager](../../../core/plugins/CutflowManager/CutflowManager.cc) books count/weighted actions in `execute()` and writes their summaries in `finalize()`. Follow its declared steps and graph nodes; do not label an arbitrary sum as a sequential cut efficiency. [CounterService](../../../core/src/CounterService.cc) provides different prefilter accounting. Inspect the actual weight used in each counter.

For `GoldenJsonManager`, `JetVetoMapManager`, and CMS MET/HEM cleaning, read [CMS cleaning](../rdfanalyzer-cms/references/cleaning.md). For region-axis packing and schema labels use [histograms](../rdfanalyzer-histograms/SKILL.md).

## Verify

Use [trigger tests](../../../core/tests/cpp/testTriggerManager.cc), [Analyzer trigger logic](../../../core/tests/cpp/testAnalyzer_TriggerLogic.cc), [region tests](../../../core/tests/cpp/testRegionManager.cc), and [cutflow tests](../../../core/tests/cpp/testCutflowManager.cc). Verify known events at boundaries and parent/child counts, with weighted and unweighted totals distinguished.
