---
name: rdfanalyzer-statistics
description: Generate or audit RDFAnalyzerCore Combine datacards, shape inputs, nuisance groups, manifest-aware fits, and analysis-specific prepared statistical bundles.
---

# Datacards and fits

[diagnostics.py](../../../core/python/statistical_inference/diagnostics.py)
provides logged subprocess execution and content identities for saved fit
diagnostics. [covariance.py](../../../core/python/statistical_inference/covariance.py)
validates covariance eigenmodes without assigning a physics acceptance policy.
Use [derived artifact verification](../../../core/python/run_bundle.py) before
loading newly sealed prepared cards; legacy cards retain their previous checks.

For selectable Combine/Rabbit fits, native GPU setup, supported card semantics,
and vjet adapters, read [Rabbit integration](../../../docs/RABBIT_INTEGRATION.md).
The shared implementation is [statistical_inference](../../../core/python/statistical_inference/)
and [run_fit.py](../../../core/python/run_fit.py). Keep TensorFlow imports in the
dedicated worker. A GPU request must fail if devices are unavailable; preserve
the reported interpolation and MC-stat policy when comparing backends.

For the concrete datacard/fit task families and their backend/completion behavior, read [LAW fits](references/law-fits.md).

## Find the statistical implementation

Identify whether the caller uses generic [create_datacards.py](../../../core/python/create_datacards.py), LAW [combine_tasks.py](../../../core/python/law/combine_tasks.py), or analysis-owned response/polynomial tooling. Read [DATACARD_GENERATOR](../../../docs/DATACARD_GENERATOR.md), [COMBINE_INTEGRATION](../../../docs/COMBINE_INTEGRATION.md), and [NUISANCE_GROUPS](../../../docs/NUISANCE_GROUPS.md) only for the selected path. The [vjet fit reference](../rdfanalyzer-analysis-projects/references/vjet-fits.md) covers its separate implementation.

Trace input files/schema, process combinations, luminosity/normalization, regions, observable/bin edges, systematic templates, and nuisance correlations into the **final card and shapes**. Explain explicit bin/channel counts and boundaries for a saved result. Current default configs do not describe an already-prepared bundle; read its stored metadata and card first.

[variation_orchestrator.py](../../../core/python/variation_orchestrator.py), [nuisance_groups.py](../../../core/python/nuisance_groups.py), and [validate_datacard_generator.py](../../../core/python/validate_datacard_generator.py) support coverage/config validation. Verify which validation the actual task invokes. Missing-variation policies, rate versus shape effects, correlation labels, and grouped templates must remain aligned. Preserve requested process separation and signed-weight/statistical uncertainty treatment.

For fits, inspect the exact command, workspace/POI map, ranges, observed/Asimov choice, fit status, covariance/rank diagnostics, and output metadata. A successful exit or forced positive-definite covariance does not establish parameter identifiability. State which diagnostics support a claim and which runtime artifacts were actually examined. Event definitions require reprocessing to change shapes; card/merge policy is determined at preparation.

## Verify

Tests: [datacard generator](../../../core/tests/python/test_datacard_generator.py), [uproot datacards](../../../core/tests/python/test_uproot_datacard.py), [nuisance groups](../../../core/tests/python/test_nuisance_groups.py), and [Combine tasks](../../../core/tests/law/test_combine_tasks.py). Verify a small card's rates, binning, names, and shifted shapes; run a fit only when that execution is within the request and runtime is available. Use [build](../rdfanalyzer-build/SKILL.md) for optional Combine dependencies and [outputs](../rdfanalyzer-outputs/SKILL.md) for manifest compatibility.
