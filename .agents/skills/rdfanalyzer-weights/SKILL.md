---
name: rdfanalyzer-weights
description: Trace or change nominal event-weight products, normalization, generator scale/PDF/PS variations, CMS scale factors, and weight auditing in RDFAnalyzerCore.
---

# Event weights and normalization

## Establish the actual product

Find the histogram/skim weight column and trace every factor back to its producer. Record sample classification, luminosity and cross-section units, filtering/stitching factors, generator sign/full mode, and denominator source. Do not infer that the analysis uses WeightManager or `setupCmsStandardWeights` merely because they exist.

Read [WeightManager.h](../../../core/plugins/WeightManager/WeightManager.h) and [implementation](../../../core/plugins/WeightManager/WeightManager.cc). Scale factors are named components; normalizations are scalar factors. A nuisance substitutes a component in the nominal product. The explicit component-name overload matters when many nuisances replace the same b-tag factor. Check immediate versus deferred definitions and when audit actions are booked; audit entries become meaningful after finalization.

## Generator and CMS paths

[GenWeightManager](../../../core/plugins/GenWeightManager/GenWeightManager.cc) handles generator nominal and LHE/PS/PDF columns, configurable branch names, replica offsets, and explicit variation registration. Current `genWeightMode` accepts `sign` and `full`; keep this consistent with denominator selection in [dataset_manifest.py](../../../core/python/dataset_manifest.py) and the staged job config. Inspect `lheScaleValue` for eight/nine-entry indexing and `vectorValueOrOne` for missing/nonfinite entries. Do not assume every generator has the same nominal normalization, PDF IDs, or replica layout.

CMS standard scale-factor recipes and channel-mask policies are owned by the
[CMS weights skill](../../../extensions/cms/.agents/skills/rdfanalyzer-cms-weights/SKILL.md).
Use the [CMS router](../rdfanalyzer-cms/SKILL.md) to locate an external checkout.

For sample stitching, read [STITCHING](../../../docs/STITCHING.md), the analysis's definitions, and `_derive_stitch_weights` in [analysis_tasks.py](../../../core/python/law/analysis_tasks.py). A selection acceptance counter, signed-generator sum, and raw event count are different denominators. [CounterService](../../../core/src/CounterService.cc) can capture the graph before the first filter; verify what the specific run records.

## Verify

Use [testWeightManager](../../../core/tests/cpp/testWeightManager.cc), [testGenWeightManager](../../../core/tests/cpp/testGenWeightManager.cc), and [test_dataset_manifest](../../../core/tests/python/test_dataset_manifest.py); production normalization changes also touch [test_analysis_tasks](../../../core/tests/law/test_analysis_tasks.py). Check signed and zero weights, substitution without double multiplication, denominator consistency, and at least one downstream varied yield for the changed path.
