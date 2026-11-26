---
name: rdfanalyzer-systematics
description: Trace or change systematic column propagation, explicit variation labels, variation-dependent selection masks, packed bundles, and downstream nuisance coverage in RDFAnalyzerCore.
---

# Systematics and acceptance

## Prevent collisions and redundant dependencies

Read [safe systematic authoring](../../../docs/SYSTEMATIC_AUTHORING.md) when
adding explicit calibration uncertainties, changing dependency propagation or
moving expensive work behind selections. It gives checked C++/Python patterns
and the distinction between framework safeguards and legacy caller obligations.

Legacy C++ `DefineNominal` keeps an existing column silently. Use distinct
physical names for explicit calibrated shifts, check for duplicates, and install
their mappings before downstream definitions. Registration does not rewrite
existing descendants. Do not fix a collision by weakening output comparisons.

The shared `DefineVariations` preflight rejects conflicting new output requests
inside one expansion, before lowering. It deliberately does not certify existing
producers or cross-call remapping. `DataManager::DefineVector` is a separate
lowering and does not inherit this check. Never describe this as global strict
definition ownership or an installed/deployed guarantee without build evidence.

Keep numerical inputs separate from selection context. Early rejection must
retain the relevant variation union and the populations of independently booked
actions. ROOT already evaluates demanded definitions lazily; prove unnecessary
event work using call counts/profiles, not graph size alone.

## Trace an end-to-end variation

Follow one varied input through registration, derived columns, filters, event weights, histogram axes, and the consuming shape/card. Use [ISystematicManager](../../../core/interface/api/ISystematicManager.h), [SystematicManager.cc](../../../core/src/SystematicManager.cc), the template methods in [IDataFrameProvider](../../../core/interface/api/IDataFrameProvider.h), and `Define`, `DefineNominal`, `Redefine`, and `Filter` in [analyzer.h](../../../core/interface/analyzer.h). Registration alone does not define or use a variation.

Distinguish paired Up/Down registrations from explicit named columns such as generator-weight replicas. Ask the manager for label and column mappings rather than inventing a suffix convention. `DefineNominal` intentionally bypasses expansion; audit uses when a value should follow varied inputs. Raw RDF graph changes can bypass expansion and tracking.

An exact variation label shared by paired and explicit registrations occupies
one systematic-axis slot. `makeSystList` derives counter indices from that
unique list. When reading older outputs with repeated labels, verify the
copies agree, including sumw2, and select one copy instead of summing them.

Typed C++ definitions and Python expression definitions share
[DefineVariations](../../../core/interface/detail/DefineVariations.h), including
derived explicit replicas. The C++ public signatures and existing duplicate-name
behavior are unchanged; Python definitions reject duplicates. Inspect both
`PythonAuthoringTest` and `SystematicPropagationTest` after changing this planner.

`DataManager::DefineVector` propagates paired and explicit labels into packed
inputs. Check a downstream model score as well as column registration:
`BDTManagerTest` covers feature order, gating and explicit/paired input shifts.

`Analyzer::Filter` constructs cumulative masks for the variation labels and retains an event when **any** relevant mask passes. It stores the individual masks so histogram/skim consumers can recover per-variation acceptance. A nominal prefilter added directly to an RNode can irreversibly remove migrations. Read the current implementation before changing filter semantics; verify nominal-fail/variation-pass and nominal-pass/variation-fail events.

The internal [selection union](../../../core/interface/detail/SelectionFilters.h)
uses compiled bool callables for one through four unique masks and an expression
for larger inventories. This avoids Cling startup for simple native graphs,
without changing acceptance or the public Filter signature.

For compact transport, inspect [SystematicBundle](../../../core/interface/SystematicBundle.h), [SystematicBundle.cc](../../../core/src/SystematicBundle.cc), [RDFColumnPacker](../../../core/src/RDFColumnPacker.cc), and [ColumnProjection](../../../core/interface/ColumnProjection.h). Preserve the common ordering of values, weights, labels, and selection masks. Configuration switches at the framework, histogram-manager, per-histogram `nominalOnly`, and skim-output levels have different scopes; trace each consumer.

Python [variation_orchestrator.py](../../../core/python/variation_orchestrator.py) plans available/missing variations; [nuisance_groups.py](../../../core/python/nuisance_groups.py) describes downstream grouping. Neither retroactively produces missing event variations.

## Verify

Start with [testSystematicManager](../../../core/tests/cpp/testSystematicManager.cc), [testSystematicPropagation](../../../core/tests/cpp/testSystematicPropagation.cc), [testSystematicBundle](../../../core/tests/cpp/testSystematicBundle.cc), and [test_variation_orchestrator](../../../core/tests/python/test_variation_orchestrator.py). Check yields and labels in a booked histogram when the propagation path changes. Use [histograms](../rdfanalyzer-histograms/SKILL.md), [weights](../rdfanalyzer-weights/SKILL.md), or [statistics](../rdfanalyzer-statistics/SKILL.md) for the affected consumer.
