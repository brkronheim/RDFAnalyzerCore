# Reweighting, training, comparison, and stitching

These are distinct projects; resolve which produced or consumes the requested artifact before changing code.

## nlo-reweight

Optional independent checkout: [nlo-reweight](../../../../analyses/nlo-reweight). [CMakeLists](../../../../analyses/nlo-reweight/CMakeLists.txt) builds `nloReweightSkim` from [nloReweightSkim.cc](../../../../analyses/nlo-reweight/nloReweightSkim.cc), gen helpers and execution code. It also builds a Pythia8 subset from its vendored tree. Inspect [interface](../../../../analyses/nlo-reweight/interface), [src](../../../../analyses/nlo-reweight/src), and [cfg](../../../../analyses/nlo-reweight/cfg) for extraction, event-modulo selection, weights, and saved features.

Training lives under [python/nlo_reweight](../../../../analyses/nlo-reweight/python/nlo_reweight): `cli.py`, `workflow.py`/`workflow_config.py`, `dataset_builder.py`, `model.py`/`networks`, `train.py`, `calibration.py`, `post_training.py`, and `export.py`. Trace the chosen config and entry point rather than treating all modules as one executable path. Data preparation, negative-weight conventions, head order, calibration, and exported graph metadata must agree. Check [python/tests](../../../../analyses/nlo-reweight/python/tests) and preserve its independent worktree modifications.

## testReweight

[compare_reweighting.py](../../../../analyses/testReweight/compare_reweighting.py) compares locally compiled [extractParticles.cc](../../../../analyses/testReweight/extractParticles.cc) with a supplied Python-ABI-specific extractor extension. Follow [build_source_extractor.py](../../../../analyses/testReweight/build_source_extractor.py), [coffea_nlo_reweight_function.py](../../../../analyses/testReweight/coffea_nlo_reweight_function.py), [comparison_observables.py](../../../../analyses/testReweight/comparison_observables.py), and [pca_systematics.py](../../../../analyses/testReweight/pca_systematics.py).

Check requested event limit per file, selected input branch, model graph/sidecar, provider, batching/padding, and output head. Source-versus-binary extractors can have different features and must each feed the model independently when comparison requires it. Distinguish raw probabilities, calibrated probabilities, signed model weights, and generator weights from current code. Compare PS clustering/counts, GenJet fiducial counts, lepton-cleaned jets, and dressed/post-shower boson definitions independently. Inspect [test_comparison.py](../../../../analyses/testReweight/test_comparison.py) for runnable coverage and the [README](../../../../analyses/testReweight/README.md) for examples, checking them against the implementation.

## rdfAnalyzerStitch

Optional independent checkout: [CMakeLists](../../../../analyses/rdfAnalyzerStitch/CMakeLists.txt) builds `vjetStitch` from [vjetStitch.cc](../../../../analyses/rdfAnalyzerStitch/vjetStitch.cc). Trace [cfg](../../../../analyses/rdfAnalyzerStitch/cfg), generator-weight mode, LHE-variable selection, sign counters, booked histograms, and downstream `StitchingDerivationTask` consumers in [analysis_tasks.py](../../../../core/python/law/analysis_tasks.py). Inclusive/binned overlap removal and derived normalization weights are different parts of stitching; establish both. Use [LAW stitching](../../rdfanalyzer-production/references/stitching.md) for the two-pass task workflow.

Use [weights](../../rdfanalyzer-weights/SKILL.md), [inference](../../rdfanalyzer-inference/SKILL.md), and [build](../../rdfanalyzer-build/SKILL.md) for shared layers. Do not replace an existing reference binary/model as part of an inspection or comparison request.
