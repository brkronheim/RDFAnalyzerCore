---
name: rdfanalyzer-kinematic-fit
description: Configure or develop KinematicFitManager particle inputs, constraints, resolutions, convergence, and CPU/CUDA execution in RDFAnalyzerCore.
---

# Kinematic fits

Read [KinematicFitManager.h](../../../core/plugins/KinematicFitManager/KinematicFitManager.h), [manager implementation](../../../core/plugins/KinematicFitManager/KinematicFitManager.cc), [CPU fitter](../../../core/plugins/KinematicFitManager/KinematicFit.h), and [GPU fitter](../../../core/plugins/KinematicFitManager/KinematicFitGPU.h) for the requested execution path.

`kinematicFitConfig` records require `name`, `particles`, and `constraints`. Follow parser methods and `applyFit`/`applyAllFits`; registering the config alone is insufficient. Resolve actual selected scalar columns, collection-indexed particles, recoil vectors, and special MET eta/numeric mass values. Missing collection elements can become zero inputs; verify `runVar` or other analysis guards rather than treating a fit output as evidence of a physical candidate.

Distinguish hard versus soft constraints and their units. MASS uses `massSigma`; PT and PX/PY have separate widths; DELTAR uses its own input columns. Inspect the implemented resolution model, covariance assumptions, MET measurement flag, degrees of freedom, and convergence/status outputs. A selected neutrino solution, fitted mass, and measured MET are different quantities.

Trace output names, sentinel/failure handling, analysis cuts, and systematic propagation. Keep the analysis's particle assignment and physics selection in its own project. Use [CMS](../rdfanalyzer-cms/SKILL.md) for corrected inputs and [systematics](../rdfanalyzer-systematics/SKILL.md) for variations.

Tests: [CPU](../../../core/tests/cpp/testKinematicFitManager.cc) and [GPU](../../../core/tests/cpp/testKinematicFitManagerGpu.cc). `USE_CUDA` is a build option; inspect the current cache and runtime before claiming GPU validation. Check a solvable constraint, a failure/degenerate case, and relevant CPU/GPU numerical agreement when changing the solver.
