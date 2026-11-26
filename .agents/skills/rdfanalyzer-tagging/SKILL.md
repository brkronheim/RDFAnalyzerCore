---
name: rdfanalyzer-tagging
description: Configure or debug TaggerWorkingPointManager, discriminator recipes, b/c-tag fixed-working-point versus shape weights, and tagger efficiency-map use.
---

# Taggers, working points, and efficiencies

[EfficiencyMeasurement.h](../../../core/interface/EfficiencyMeasurement.h)
books selected-object pass/fail pT/eta histograms at the caller's graph stage.
[efficiency.py](../../../core/python/efficiency.py) combines weighted sufficient
statistics, rebins only at saved boundaries, and exports correctionlib inputs.
Supply the population, weights, groups, WPs and axes explicitly. The prefilter
TaggerEfficiencyService has a different lifecycle; do not substitute it for
an analysis-selected measurement without checking column availability.

## Resolve score, collection, and method

Start with the analysis's selected object collection and discriminator expression. Read [TaggerWorkingPointManager.h](../../../core/plugins/TaggerWorkingPointManager/TaggerWorkingPointManager.h), [implementation](../../../core/plugins/TaggerWorkingPointManager/TaggerWorkingPointManager.cc), [WorkingPointRecipe](../../../core/interface/WorkingPointRecipe.h), and [TAGGER_WORKING_POINTS](../../../docs/TAGGER_WORKING_POINTS.md). Track one-score cuts versus combined CvL/CvB cuts, WP order, and configured tagger branch names. A score cut changes selection; a scale factor changes the event weight.

For configured actions, `taggerConfig` records use a `type` discriminator. Inspect `setupFromConfigFile()` for the required fields of that action rather than borrowing the schema of another action. Registration, collection building, per-object SF evaluation, fixed-WP event weighting, and connection to WeightManager are distinct steps.

## Weight and efficiency contract

Follow `defineFixedWorkingPointWeight` and `registerWeightsWithWeightManager`, then the consuming final event-weight product. Fixed-WP event weights need efficiencies for the selected **reconstructed** jet population, including the untagged contribution; a matched-gen-jet diagnostic is not automatically such an efficiency. Match flavor, kinematics, tagger/WP, era, and jet selection to the payload axes. Continuous shape corrections use a different method and input contract.

Inspect [TaggerEfficiencyService](../../../core/interface/TaggerEfficiencyService.h) and [implementation](../../../core/src/TaggerEfficiencyService.cc) when deriving maps. Establish numerator/denominator definitions, flavor mapping, unmatched treatment, bin edges/flow policy, and weighting. A payload file existing does not show that it was evaluated, connected, or valid for an unsupported tagger path. Report unsupported configurations explicitly and honor the requested enabled/disabled behavior.

Use [weights](../rdfanalyzer-weights/SKILL.md) for nuisance component substitution, [object corrections](../rdfanalyzer-object-corrections/SKILL.md) for corrected jets, and the [project map](../rdfanalyzer-analysis-projects/SKILL.md) for analysis-specific efficiency builders and score formulas.

## Verify

Closest tests: [TaggerWorkingPointManager](../../../core/tests/cpp/testTaggerWorkingPointManager.cc), [WorkingPointRecipe](../../../core/tests/cpp/testWorkingPointRecipe.cc), and [PhysicsObjectCollection](../../../core/tests/cpp/testPhysicsObjectCollection.cc). Verify tagged/untagged and flavor cases with known efficiencies, then a small event smoke test before asserting event-level fixed-WP readiness.

CMS era/payload and weight-recipe integration belongs to the
[extension CMS weights skill](../../../extensions/cms/.agents/skills/rdfanalyzer-cms-weights/SKILL.md).
