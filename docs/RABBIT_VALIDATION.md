# Rabbit integration validation, 2026-10-29

The integration was exercised on four NVIDIA L40 GPUs (46,068 MiB each), with
NVIDIA driver 615.71.10. The dedicated environment uses the pinned Rabbit commit
and CUDA dependencies in [the lock file](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/requirements/rabbit-gpu.lock).

## Completed checks

* Central statistics, datacard, nuisance-group, LAW and vjet-adapter suite:
  **187 passed, 8 skipped**. Optional native tests were exercised separately.
* Native suite: **3 passed in 136.34 seconds**. This checks an analytic Poisson
  model on CPU, one GPU and four GPUs; normalized V+jets polynomial and ttbar
  coefficients; and weighted MC statistics with asymmetric shape nuisances.
* The observed-data toy recovered `mu = 1.1`, `beta = 0.9`; errors were
  approximately `0.0512156751` and `0.0502681960`. Values, covariance and postfit
  predictions matched the analytic likelihood at the regression tolerances.
* With weighted MC statistics and asymmetric nuisances, the largest absolute
  CPU/four-GPU full covariance difference was `8.88e-16`; both EDM values were zero.
* The vjet polynomial entry point completed with both engines on the same
  synthetic saved card. Both returned coefficients of one. The largest absolute
  covariance difference divided by the largest Combine covariance entry was
  **0.003372**, about **0.34%**, comparing numerical robustHesse with Rabbit's
  analytic derivatives. This does not establish equivalence of different shape
  interpolation or MC-stat conventions.
* An existing vjet card was converted read-only: **22 channels, 38 model
  parameters, 63,905 channel/process/nuisance effects**, in **321.24 seconds**.
  Refits can reuse a prepared-model cache after source-hash validation.
  This was an input-conversion check. Subsequent full production benchmarks
  are recorded below.
* Python compilation, the skill-tree validator, dependency consistency and
  whitespace checks passed. Final focused contract checks passed: **12 passed**.

The comparison values are recorded in
[rabbit_validation_20261029.json](examples/statistics/rabbit_validation_20261029.json).
Commands and statistical policies are in [Rabbit integration](RABBIT_INTEGRATION.md).

## Joint 2022 preEE/postEE production benchmark

The full Asimov comparison uses the requested
`mergeRun_vjet_run_cfg_taggersall_20261028_103655_2022_preEE` and
`mergeRun_vjet_run_cfg_taggersall_20261028_103655_2022_postEE` inputs in a joint
model with **22 channels, 406 bins, 31 free model parameters and 100 explicit
nuisances**, with theory nuisances profiled and MC statistics enabled.

Full Rabbit fits completed on CPU, one L40, and four L40 GPUs. All three returned
successful minimizer statuses and positive-definite covariance matrices. Their
131-parameter covariance matrices agree to about `1e-12` in relative errors.
Rabbit CPU was fastest for this nominal Asimov workload. The four-GPU fit uses
sequential Hessian-vector products (`--rabbit-hvp-batch 1`) to compute the full
covariance. Zero-valued shifted templates use the explicit, audited native
Rabbit floor; this does not assert equivalence to Combine's shape interpolation.

The standalone CMS Combine baseline completed directly on byte-identical
original card/shape/model artifacts, independently of Rabbit. Its numerical
`robustHesse` retained all 537 parameters and returned a positive-definite
covariance. The initial Minuit covariance was forced positive definite
(status 1); that warning is retained separately from the robust covariance.

| Backend | Native driver (s) | Fresh input to result (s) |
|---|---:|---:|
| CMS Combine with robustHesse | 4370.14 | 4868.70 |
| Rabbit CPU | 245.59 | 1151.71 |
| Rabbit one L40 | 308.00 | 1233.53 |
| Rabbit four L40, HVP batch 1 | 800.75 | 1712.69 |

The fresh Rabbit totals include 781.98 seconds of shared portable-input
conversion. These are single-run Asimov measurements, including native output
production; they do not establish observed-data minimization performance.
All model values are nominal. Rabbit's normalization errors are 5.3 to 6.4%
smaller than Combine's, and the largest explicit-nuisance error difference is
61.5% (`JES_FlavorQCD_2022_postEE`). These engines' shape interpolation and
MC-stat policies are not asserted equivalent. The generated datasets differ
by at most 0.0235 events per physical bin.

The compact [production validation record](examples/statistics/production_backend_benchmark_20261029.json)
keeps the numerical summary with the framework documentation.

## Combine-compatible mode

The opt-in `--rabbit-likelihood combine` patch reproduces Combine v11 additive
shape interpolation, separate AsymPow rates, TH1F template handling and Gaussian
autoMCStats. It preserves zero-valued shifts and bins gaining support. Compatible
results have their own output directories; native Rabbit behavior remains the
default. See the [compatibility contract](RABBIT_INTEGRATION.md#combine-compatible-likelihood).

Native workspace regression checks cover ordinary templates, zero-bin and
asymmetric-rate cases, disabled MC statistics, and two channels with different
lengths. They compare predictions, MC variances, profiled NLL, gradients and
Hessians. A separate toy covariance check compares Rabbit's finite-stencil
estimator with actual standalone robustHesse; uncertainties agree within 0.1%.

An independent production-workspace check at 229 displaced parameter points
agrees in bin means to a maximum scaled difference of 6.41e-15, MC variances
to 2.44e-15, and profiled means to 4.49e-15. The largest absolute profiled-NLL
difference is 4.55e-9. Shared observations isolate likelihood conventions from
Asimov-generation rounding. Three additional points saturating a Gaussian
MC-statistical bound are retained but excluded from this interior profiling
check; boundary profiles have not been validated.

The requested joint production model was rerun on CPU, one GPU and four GPUs,
with all 131 explicit parameters floating. Every fit completed with a positive
definite covariance and no parameter removals. The completed pure Combine
baseline above is reused on byte-identical inputs.

| Compatible Rabbit configuration | Native driver (s) | Cached input to result (s) |
|---|---:|---:|
| CPU | 369.80 | 421.37 |
| One L40 | 267.78 | 316.44 |
| Four L40 | 533.48 | 581.81 |

Normalization uncertainties differ from native robustHesse by 0.28–0.32%; the
largest error difference among the 31 physical model parameters is 1.42%.
CPU/GPU parameter errors agree within 11 parts per million. Default uncertainty
estimates are **not identical**: the largest explicit-nuisance error difference
is 14.14% for `PS_FSR_qcd_2022_postEE`. With the same finite-difference nodes,
independent Rabbit likelihood evaluations reduce the largest error difference
across all 131 explicit parameters to 0.223%. Only node locations are shared;
Combine covariance values never enter Rabbit's calculation.

The default compatible estimator targets delta NLL 0.1 in each unprofiled
direction, then eliminates the diagonal Gaussian MC-statistical block through a
Schur complement. Native robustHesse accepts 0.095–0.105 and starts its search
from Minuit errors. The clipped, strongly correlated production model is sensitive
to these different stencils. Analytic Hessians are available for diagnostics;
they give a particularly large JES uncertainty difference at nominal floor
bins. Saved Asimov counts also retain a generation-rounding difference of at
most 0.031 events per bin (relative difference below 6e-8).

A compact [compatibility validation record](examples/statistics/rabbit_combine_compatibility_20261029.json)
preserves the numerical evidence with the framework documentation.
Smaller native Rabbit errors are not evidence of a better result;
they came from a different statistical model. Compatible Rabbit on one GPU is
the fastest tested way to evaluate the intended model. Neither Hessian method
has established coverage for the sensitive clipped nuisances; profile intervals
are the appropriate next accuracy check.

## Existing analysis-test failures

The broader selected vjet suite reported **26 passed, 4 skipped, 5 failed**,
plus 33 passing subtests. Failures are in selection/binning functions untouched
by this integration:

* `test_compact_source_mapping_matches_legacy_pooled_truth`
* `test_merge_policy_merges_reco_bins_without_coarsening_truth_bins`
* `test_negative_nominal_and_varied_components_use_shared_policy`
* `test_scope_uses_metadata_for_extra_source_slots`
* `test_source_mapping_preserves_all_bins_and_failure_slots`

They concern source-bin dictionary keys, legacy truth-bin expectations and
observed cells with zero prediction. Backend execution, correction export
reading, and relevant CLI checks pass. The analysis checkout has no usable Git
HEAD and is ignored by the framework repository; review its adapter edits
independently of the framework worktree.

## Runtime findings

The sparse single-GPU path failed the numerical smoke test with non-finite
likelihoods. GPU execution uses dense tensors, also required by Rabbit's native
multi-device fitter. CPU sparse and dense GPU results agree in the supported
tests. Input reading uses owned arrays rather than mutating eager tensor buffers.
Native logs confirmed four-device sharding and CUDA placement. GPU requests fail
when devices are unavailable.

The shared Combine runtime keeps `text2workspace.py` on the invoking Python
environment, preventing shebang selection of an interpreter without NumPy.
