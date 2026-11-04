---
name: rdfanalyzer-systematics
description: Trace or change systematic column propagation, explicit variation labels, variation-dependent selection masks, packed bundles, and downstream nuisance coverage in RDFAnalyzerCore.
---

# Systematics and acceptance

## Trace an end-to-end variation

Follow one varied input through registration, derived columns, filters, event weights, histogram axes, and the consuming shape/card. Use [ISystematicManager](../../../core/interface/api/ISystematicManager.h), [SystematicManager.cc](../../../core/src/SystematicManager.cc), the template methods in [IDataFrameProvider](../../../core/interface/api/IDataFrameProvider.h), and `Define`, `DefineNominal`, `Redefine`, and `Filter` in [analyzer.h](../../../core/interface/analyzer.h). Registration alone does not define or use a variation.

Distinguish paired Up/Down registrations from explicit named columns such as generator-weight replicas. Ask the manager for label and column mappings rather than inventing a suffix convention. `DefineNominal` intentionally bypasses expansion; audit uses when a value should follow varied inputs. Raw RDF graph changes can bypass expansion and tracking.

`Analyzer::Filter` constructs cumulative masks for the variation labels and retains an event when **any** relevant mask passes. It stores the individual masks so histogram/skim consumers can recover per-variation acceptance. A nominal prefilter added directly to an RNode can irreversibly remove migrations. Read the current implementation before changing filter semantics; verify nominal-fail/variation-pass and nominal-pass/variation-fail events.

For compact transport, inspect [SystematicBundle](../../../core/interface/SystematicBundle.h), [SystematicBundle.cc](../../../core/src/SystematicBundle.cc), [RDFColumnPacker](../../../core/src/RDFColumnPacker.cc), and [ColumnProjection](../../../core/interface/ColumnProjection.h). Preserve the common ordering of values, weights, labels, and selection masks. Configuration switches at the framework, histogram-manager, per-histogram `nominalOnly`, and skim-output levels have different scopes; trace each consumer.

Python [variation_orchestrator.py](../../../core/python/variation_orchestrator.py) plans available/missing variations; [nuisance_groups.py](../../../core/python/nuisance_groups.py) describes downstream grouping. Neither retroactively produces missing event variations.

## Verify

Start with [testSystematicManager](../../../core/tests/cpp/testSystematicManager.cc), [testSystematicPropagation](../../../core/tests/cpp/testSystematicPropagation.cc), [testSystematicBundle](../../../core/tests/cpp/testSystematicBundle.cc), and [test_variation_orchestrator](../../../core/tests/python/test_variation_orchestrator.py). Check yields and labels in a booked histogram when the propagation path changes. Use [histograms](../rdfanalyzer-histograms/SKILL.md), [weights](../rdfanalyzer-weights/SKILL.md), or [statistics](../rdfanalyzer-statistics/SKILL.md) for the affected consumer.
