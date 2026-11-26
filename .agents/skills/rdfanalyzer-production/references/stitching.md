# LAW stitching derivation, application, and validation

Read [analysis_tasks.py](../../../../core/python/law/analysis_tasks.py), [STITCHING](../../../../docs/STITCHING.md), and the analysis's configured counter/weight definitions. Search for the exact classes `StitchingDerivationTask`, `StitchingApplySubmitJobs`, `StitchingApplyMonitorJobs`, `StitchingApplyTask`, and `StitchingValidationTask`; the shorter name `StitchDerivationTask` is not a task class.

The [Snakemake composition](../../../../examples/snakemake/README.md#two-pass-stitching)
uses [stitching.py](../../../../core/python/rdfproduction/stitching.py)
to pass the verified ROOT files to the same derivation and validation logic.
It binds the generated correction to the application spec after derivation
and tracks the passes under separate run names. Inspect the two indexes for
exact terminal partition paths after a split.

## Resolve both event passes

`StitchingDerivationTask` reads `--stitch-config` groups with explicit `meta_files` or declarative sample selections. With `source_skim_name` it requires a source `SkimTask`; inspect the forwarded submit config, executable, manifest, file-source, partition and runtime options. `--name` namespaces `stitchWeights_<name>` beneath the module's resolved workspace.

Read `_read_intweight_sign_hist`, `_derive_stitch_weights`, and cross-section helpers. Binwise scale factors use nonnegative net signed counts normalized across samples. Check actual counter keys, sample identifiers, stitch-ID encoding, supported era/inclusive-sample resolution, and empty/negative-bin behavior. A generic sum of event weights is not automatically the required signed count. Derivation writes `stitch_weights.json`, `stitch_cross_sections.json` and performance information; the primary output target alone does not establish the validity of all companions.

`StitchingApplyTask` arranges the source counter pass, derivation, and application pass. Its default source name is `<name>_counts`; `_make_stitching_derivation_task` sets source stitching mode to `none`. Application receives `stitchMode=correctionlib` and the derived `stitchWeights` path. `StitchingApplySubmitJobs` and `StitchingApplyMonitorJobs` enforce that dependency for delegated Condor submission/monitoring. Inspect overrides and generated configs to confirm the correction is applied once and present in both loose/shared archives.

A derivation or validation command can have upstream production requirements. Inspect `requires()` before running it against live storage. Preserve independent source and application run states when the request is to resume or diagnose one stage.

## Validate the application

`StitchingValidationTask` discovers application metadata across dataset jobs, inspects configured correction groups, and checks required stitched histograms. Its `complete()` can inspect saved report and expected plot paths; `require_stitched_histograms` changes that contract. Follow the selected validation adapter for analysis-owned histogram specifications, boundaries, exclusions and normalization. The framework must not invent physics boundaries from another analysis.

Inspect the saved validation JSON: sample/file counts, groups, paths, per-file records, plots, process-specific boundaries, and plot normalization. Verify unstitched/stitched distributions and total normalization against the requested convention. A correction JSON that loads successfully does not establish correct overlap removal or application yields.

Use [weights](../../rdfanalyzer-weights/SKILL.md) for sign/full denominator consistency, [reports](../../rdfanalyzer-outputs/references/reports.md) for evidence limits, and [analysis projects](../../rdfanalyzer-analysis-projects/references/reweighting.md) for project entry points. Closest coverage is [test_analysis_tasks.py](../../../../core/tests/law/test_analysis_tasks.py), [test_stitching_plotting.py](../../../../core/tests/python/test_stitching_plotting.py), and [dataset-manifest tests](../../../../core/tests/python/test_dataset_manifest.py).
