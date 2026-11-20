# LAW datacards, fits, and analytic workspaces

Read [combine_tasks.py](../../../../core/python/law/combine_tasks.py) and the [statistics workflow](../SKILL.md) before selecting a task. Inspect task discovery, `requires()`, output target type and completion behavior; directory existence alone is not evidence of a successful fit.

| Task | Inputs and behavior |
|---|---|
| `CreateDatacard` | Runs the generic YAML-driven datacard generator; inspect resolved input files and generated card/shape paths. |
| `RunCombine` | Requires `CreateDatacard` when `datacard_path` is empty; an explicit card path bypasses that dependency. Discovers Combine and runs selected cards in the results directory with method/options and per-card logs. |
| `ManifestDatacardTask` | Loads merged output schema, region/sample metadata and nuisance coverage; `merge_input_dir` can add a `MergeAll` dependency. |
| `ManifestFitTask` | Requires manifest datacards unless an explicit `datacard_dir` is supplied; supports `combine`, native CPU/GPU `rabbit`, and a lightweight `analysis` histogram chi-square backend. Rabbit jointly fits unique-channel cards and verifies completion against input/model hashes. |
| `AnalyticWorkspaceFitTask` | Reads workspace YAML plus histogram ROOT file, builds per-channel and combined RooFit workspaces/cards, then optionally invokes Combine. |

`ManifestDatacardTask`'s `strict_coverage` changes warnings into failures for incomplete nuisance groups. Follow `_prepare_manifest_datacard_config`, available variation detection and actual template reads before claiming coverage. Verify a top-level DAG option reaches the chosen task; current `FullAnalysisDAG.requires()` passes different arguments along fit and direct-datacard branches.

For Rabbit, `rabbit_zero_shape_policy` is forwarded by the DAG and is part of
the task identity. The default `error` rejects zero-valued shifted yields;
explicit `native` enables the audited relative `1e-3` floor described in the
[Rabbit policies](../../../../docs/RABBIT_INTEGRATION.md#statistical-and-device-policies).
`rabbit_hvp_batch` is also forwarded and included in task identity. Its default
`0` leaves native batch selection unchanged; `1` computes sequential
Hessian-vector products while retaining the full covariance.
`rabbit_likelihood` selects `native` or `combine`, is forwarded by the DAG,
and is significant for task completion. The compatible mode preserves zero
templates and uses Gaussian autoMCStats; it rejects native zero flooring and
native lite/full overrides.
`rabbit_covariance` parameter is also forwarded and significant; `auto` selects
finite stencils in compatible mode and automatic derivatives in native mode.
Read the
[compatibility contract](../../../../docs/RABBIT_INTEGRATION.md#combine-compatible-likelihood)
before comparing saved likelihoods or interpreting finite-step covariance differences.

For `RunCombine` and `ManifestFitTask`, inspect `_find_combine`, selected backend/method, `shlex`-parsed options, cwd, chosen cards, logs, provenance and resulting ROOT/JSON artifacts. The `analysis` backend is a separate statistical calculation; do not label it a Combine likelihood fit. A process returning zero is only one fit diagnostic.

`AnalyticWorkspaceFitTask` requires compatible PyROOT/RooFit. `--skip-fit` explicitly builds inputs without Combine. Its current run also warns and returns when the Combine executable is unavailable, leaving cards/workspaces available. Inspect executed commands/logs before reporting fit completion. Missing channel histograms can be skipped with warnings; compare active channels with the requested workspace YAML. The task's output is a directory, so inspect partial results after a failed attempt before relying on `complete()`.

Tests: [test_combine_tasks.py](../../../../core/tests/law/test_combine_tasks.py), [datacard generation](../../../../core/tests/python/test_datacard_generator.py), and [uproot shapes](../../../../core/tests/python/test_uproot_datacard.py). For optional native builds read [build dependencies](../../rdfanalyzer-build/references/wrappers-dependencies.md). For vjet's separate card/model pipeline use [vjet fits](../../rdfanalyzer-analysis-projects/references/vjet-fits.md).
