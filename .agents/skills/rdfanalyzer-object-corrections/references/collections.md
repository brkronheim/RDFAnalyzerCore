# Physics-object and corrected collections

Read [PhysicsObjectCollection.h](../../../../core/interface/PhysicsObjectCollection.h) and [PHYSICS_OBJECTS](../../../../docs/PHYSICS_OBJECTS.md). Collections retain original branch indices while holding selected/corrected four-vectors. `index`, `indices`, feature extraction, overlap removal, filtering, pairs/triplets, typed payloads, and cached features must all use the intended index space.

When adding a selection/correction, verify vector lengths, empty collections, ordering, feature-cache behavior, and whether an operation returns a new collection or changes the existing one. Track original indices through sorting, overlap removal, and corrected kinematics; do not index the original flavor/tagger array by the selected collection position. For variations inspect `PhysicsObjectVariationMap` and preserve consistent named variations without assuming identical membership across shifts.

Concrete corrected CMS wrappers are documented in the
[extension collection reference](../../../../extensions/cms/.agents/skills/rdfanalyzer-cms-object-corrections/references/collections.md).

Keep analysis object masks and overlap criteria in the project; use wrapper/framework changes only for reusable behavior. When an analysis supplies regressed raw pt/mass overrides, trace them before applying JEC to avoid mixing regression and correction conventions.

Tests: [PhysicsObjectCollection](../../../../core/tests/cpp/testPhysicsObjectCollection.cc). Include an original-index mapping case and a variation that changes membership when that behavior changes. For tagger features and weights use [tagging](../../rdfanalyzer-tagging/SKILL.md).
