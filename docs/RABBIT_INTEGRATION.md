# Combine and Rabbit statistical backends

The optional `core/python/statistical_inference/` package centralizes standalone
Combine setup, validated MultiDimFit execution, strict binned-template loading,
native Rabbit input generation, and result access. Importing the package does
not import TensorFlow. Analysis selections and era/correlation policy remain in
their analysis projects.

## Install the CUDA runtime

Use Python 3.12 on this checkout. The dedicated environment leaves the analysis
environment unchanged:

```bash
RDF_RABBIT_BASE_PYTHON="$PWD/.venv/bin/python" bash scripts/setup_rabbit.sh gpu
```

This pins Rabbit commit `a0e8323a1f6489abc5d25b6c1c90933ccee55deb`, TensorFlow
2.20.0 with the `and-cuda` extra, TensorFlow Probability 0.25.0, and the
`tf-keras` compatibility package at version 2.20.1.
`requirements/rabbit-gpu.lock` records the complete tested environment, including
CUDA libraries and test dependencies. To reproduce it exactly, create the venv
and install that lock file with pip. For a fresh CPU-only environment, run
`setup_rabbit.sh cpu`. To keep CPU and GPU installations side by side, set
`RDF_RABBIT_VENV` to a distinct path for each and select the environment with
`RDF_RABBIT_PYTHON`.

The GPU worker exposes pip-installed NVIDIA libraries before importing
TensorFlow and enables memory growth. It requires real GPU visibility; a request
for GPUs fails if TensorFlow cannot see enough devices. Device indices refer to
the visible devices after any inherited `CUDA_VISIBLE_DEVICES` restriction.
`RDF_RABBIT_PYTHON` or `--rabbit-python` selects another dedicated interpreter.
The worker verifies the upstream commit, rather than accepting an untested
package solely because it has the same version number.

## Run generic cards

Combine requires the [ROOT/Combine environment](BUILD_WITH_COMBINE.md#usage)
and pandas in the Python environment used to convert datacards.
Rabbit runs in its separate environment and needs no PyROOT.

```bash
.venv/bin/python core/python/run_fit.py \
  --datacard /path/to/datacard.txt --backend combine \
  --dataset asimov --output-dir /path/to/comparison

.venv/bin/python core/python/run_fit.py \
  --datacard /path/to/datacard.txt --backend rabbit \
  --dataset asimov --output-dir /path/to/comparison \
  --rabbit-device gpu --rabbit-n-devices 4 --rabbit-devices 0,1,2,3
```

For a CPU fit use `--rabbit-device cpu`. A single-GPU fit defaults to one
available GPU, or use `--rabbit-devices 1`. Rabbit can jointly fit multiple
unique-channel cards passed to `--datacard`. Combine accepts one simultaneous
card in this command. Never pass both a combined card and its constituent cards.

The public `read_model()` loader preserves observations, bin edges, process
identities, weighted variances, nuisance applicability/correlations, and groups.
It supports local TH1 shapes, counting cards, `lnN`, `shape`, numeric bounded
`rateParam`, and global `autoMCStats 0 1 [1]`. Arbitrary RooWorkspace objects,
formula rateParams without a structured adapter, other nuisance distributions,
and unknown directives raise errors. There is no automatic fallback backend.

The central card generator now writes `data_obs` separately from the fitted
processes. Missing histograms for a configured data sample are errors. Its
historical no-data configuration writes nominal pseudo-data with a message;
it must not be interpreted as an observed-data measurement.

## vjet-njetpt

Prepare inputs once with the existing analysis options. Both engines consume
the final saved channel/bin definitions, process aliases, nuisance names, and
polynomial metadata:

```bash
python analyses/vjet-njetpt/aux/fit_polynomial.py \
  --stage fit --card-dir CARDS --backend combine --asimov \
  --output CARDS/combine_result.json

python analyses/vjet-njetpt/aux/fit_polynomial.py \
  --stage fit --card-dir CARDS --backend rabbit --asimov \
  --rabbit-device gpu --rabbit-n-devices 4 --rabbit-devices 0,1,2,3 \
  --output CARDS/rabbit_result.json
```

`fit_response.py` also supports `--backend rabbit`, including its binned model,
single-era preparation and multi-era preparation. Theory nuisances retain the
existing opt-in policy. `--enable-theory-nuisances` profiles them in either
engine. Free ttbar tensor-polynomial coefficients remain model nuisances.

The Rabbit adapter evaluates the saved V+jets source-bin Bernstein weights,
normalization denominators, failure categories, ttbar weights, and aliases.
It checks that ttbar formula rateParams match the structured metadata.
Parameters with finite bounds use a logistic coordinate; one-sided positive
parameters use an exponential coordinate. Reported values and the complete
covariance are transformed back with the Jacobian. Parameters near a bound
are flagged; Hessian errors there are not profile intervals.

The shared `fit_section()` reader accepts new backend-neutral summaries and
historical Combine summaries. Correction plots, parameter summaries, correctionlib
export and pre/postfit-bin plots use it. Rabbit pre/postfit predictions and
covariance bands are exported as ordinary JSON so plotting needs no TensorFlow.
Combine-specific impacts/GoF scripts remain Combine-specific. Rabbit's optional
native scans/impacts are exposed by `run_fit.py`; native HDF5 parameter names
refer to internal coordinates, mapped by the saved `model.json`.

## Statistical and device policies

### Combine-compatible likelihood

Select `--rabbit-likelihood combine` (API: `likelihood="combine"`, LAW:
`rabbit_likelihood=combine`) to use the prepared card's Combine v11 template
semantics in the Rabbit CPU/GPU worker. The default `native` mode preserves
the earlier Rabbit log-normal/gamma calculation and its saved benchmarks.

```bash
python analyses/vjet-njetpt/aux/fit_polynomial.py \
  --stage fit --card-dir CARDS --backend rabbit --asimov \
  --enable-theory-nuisances --rabbit-likelihood combine \
  --rabbit-device gpu --rabbit-n-devices 4 --rabbit-devices 0,1,2,3 \
  --rabbit-hvp-batch 1 --output CARDS/rabbit_compatible_result.json
```

The compatible kernels implement normalized additive `shape` interpolation,
Combine's C2 transition and extrapolation, separate AsymPow rate modifiers,
TH1F rounding, the shape-rate cutoff/constant precision, and the original
positivity handling. Zero shifts and zero nominal bins gaining support are
preserved. Physics coefficients multiply the full morphed prediction.
Gaussian `autoMCStats 0 1 1` terms use the original template variances scaled
by the current process coefficients, and are analytically profiled. Gaussian
nuisances use a smooth coordinate spanning the native [-7, 7] range; the full
covariance is transformed back to physical coordinates.
Compatible results use `rabbit/asimov_combine/` or `rabbit/observed_combine/`
so switching modes preserves native-mode outputs in the same fit directory.

This mode requires template-derived rates (`rate -1`), the supported `shape`
and `lnN` card subset, and `--rabbit-mc-stat card` or `none`. It uses dense
tensors. `--rabbit-zero-shape-policy native` and the native lite/full overrides
are rejected when combined with this mode. Unsupported card directives remain
errors. It does not import RooFit into TensorFlow or change the Combine inputs.

Likelihood matching and covariance estimation are separate checks. In compatible
mode, `--rabbit-covariance auto` selects `combine`: finite stencils of the
unprofiled likelihood in physical coordinates, followed by elimination of the
diagonal Gaussian MC-statistical block through a Schur complement. Stencil
offsets target a change of 0.1 in NLL; `combine_covariance.json` records them.
This uses Rabbit/TensorFlow predictions throughout. It requires a positive
definite Hessian and does not regularize eigenvalues or remove parameters.
The native Combine robustHesse step search can choose slightly different
offsets, so numerical agreement must still be checked on the actual model.

`--rabbit-covariance analytic` retains Rabbit's automatic Hessian for diagnostics.
Clipping at nominal floor bins can give substantially different automatic and
finite-step curvature even when likelihood evaluations agree. Native likelihood
mode keeps the automatic Hessian by default. Boundary solutions need profile
intervals with either covariance method.

`statistical_inference.compat_probe` evaluates a compatible fit's bin means,
MC variances, profiled likelihood and derivatives in the Rabbit interpreter.
`statistical_inference.compat_reference` independently evaluates a workspace
built by native `text2workspace.py`, in a separate PyROOT process. Native
regression tests are enabled with `RDF_TEST_RABBIT_COMBINE=1` in
`core/tests/python/test_rabbit_combine_compat.py`.

For a covariance diagnostic with identical finite-difference nodes, run
`python -m statistical_inference.compat_stencils MODEL_JSON COMBINE_LOG STENCILS_JSON`
on a native verbose robustHesse log. Then run `compat_probe` in the Rabbit
interpreter with `--covariance-stencils STENCILS_JSON` and a nominal point file
containing `[{}]`. It writes a separate `.covariance.json`, using Rabbit's own
likelihood at those nodes. Model identity and node coverage are checked; logs
without both bisection traces are rejected. This shares node locations at the
log's printed precision, never Combine covariance values, and does not change
the default fit or its benchmark timing.

### Native Rabbit likelihood

* `--rabbit-minimizer-method` selects an explicit numerical method for controlled
  retries (`trust-krylov`, `trust-exact`, `trust-ncg`, `BFGS`, `L-BFGS-B`).
  Omitting it preserves Rabbit's default. The API uses `minimizer_method`;
  the job and native argument records preserve the choice. Compare retries
  on the identical saved model and retain rejected attempts.
  `--rabbit-minimizer-gtol` (API `minimizer_gtol`) sets a finite positive
  raw-coordinate gradient tolerance; the default remains 1e-7. A tolerance
  study must also compare EDM, fitted values and covariance, especially at
  boundaries or when the NLL has numerical precision limitations.
* Asimov jobs explicitly run Rabbit's minimizer before computing covariance.
  Native Rabbit `-t -1` alone evaluates curvature without minimizing; the worker
  overrides that dispatch and requires a recorded minimizer status.
  The worker sets an explicit gradient-norm tolerance of `1e-7`; EDM and
  positive-definite model covariance are checked separately.
* `--rabbit-hvp-batch N` controls directions per Hessian-vector batch without
  reducing covariance coverage. `1` uses sequential products, limiting tracing
  and device memory for structured multi-GPU models. The native multi-GPU
  default batches up to 256 directions; compilation can dominate small fits.
* Both up/down templates are passed explicitly with `symmetrize=None`.
  Rabbit's asymmetric log-normal interpolation is not identical to Combine's
  `shape` morphing. Agreement at template points does not prove agreement of
  the full likelihood.
* `--rabbit-mc-stat card` enables Rabbit lite/gamma MC statistics when the card
  requests the supported `autoMCStats` policy. `none`, `lite`, and `full` are
  explicit alternatives. These are recorded approximations/policies, not a
  claim of exact equivalence to Combine's automatic statistical nuisances.
* Negative input bins are rejected in both modes. Variations creating support
  from zero nominal are rejected in native mode.
  Apply the analysis's common support/merging policy before comparing engines.
* Zero-valued shifted bins with positive nominal yield are rejected by default.
  `--rabbit-zero-shape-policy native` explicitly enables Rabbit's native
  relative `1e-3` floor for those shifted yields. The input tensor metadata and
  fit summary retain counts by nuisance and the summed affected nominal yield.
  That sum runs over effect/direction/bin entries and can count the same nominal
  bin more than once.
  This changes interpolation relative to Combine; compare uncertainties with
  that policy visible. Exactly zero nominal bins gaining support remain rejected.
* GPU fits use dense tensors: Rabbit's sharded path requires them, and the
  sparse CUDA path failed the numerical smoke test on this environment.
  CPU fits can use sparse tensors. Dense tensors may require substantially more
  memory; benchmark representative inputs before a full production fit.
* Tensor loading uses owned NumPy arrays and TensorFlow's public conversion API
  inside the isolated worker, avoiding upstream's writable-eager-buffer assumption.
* Multi-GPU fits shard bins across devices. Native impacts currently require one
  device. Four GPUs can be slower for small models because compilation and
  communication dominate.

## Outputs and completion

Rabbit writes `rabbit/{observed,asimov}/` below the requested output directory:
the portable model, input tensor, native HDF5 results, native arguments, log,
physical parameter/covariance summary, histogram JSON, and completion record.
The prepared-model cache verifies all source hashes and its own identity before
reuse, avoiding repeated loading of large collections of ROOT systematic shapes.
Uncached TH1 conversion defaults to ROOT C++ deserialization with owned NumPy
copies. This requires PyROOT in the preparing analysis interpreter; the isolated
TensorFlow worker remains ROOT-free. Select `--rabbit-template-reader uproot`
or `template_reader="uproot"` in `read_model`, `cached_model`, or `run_rabbit`
to use Uproot explicitly. Both readers produce the
same portable-model schema/identity and use the same source-hash validation.
The vjet study runner also defaults to ROOT and exposes `--template-reader uproot`; its
`--reuse-model` option shares a validated cache between study directories.

On large models, optional histogram-error Jacobians can allocate arrays
quadratic in the number of fit bins. `--rabbit-no-histogram-errors` skips that
saved-bin-error calculation and histogram chi2 while retaining minimization and the full
parameter covariance, including all profiled nuisance correlations. The
result records `histogram_errors_computed=false`. Default histogram readers
reject such results when uncertainty bands are required; values-only reading
is available through `read_fit_histograms(..., require_uncertainties=False)`.
Correction-function bands can still use the complete parameter covariance.
`--rabbit-hvp-batch 1` independently bounds the number of Hessian-vector
directions evaluated together without reducing covariance coverage.

Completion is published only after checking EDM, finite values, and positive
definiteness of the retained model covariance. The raw minimizer status is
retained even when its success flag disagrees with the EDM test. Failed
postfit covariance calculations can retain `minimized_point.json` for
diagnosis without publishing completion or uncertainty results. A minimized
point alone is not an accepted fit.

`ManifestFitTask --fitting-backend rabbit` runs its cards jointly. Parameters
`--rabbit-python`, `--rabbit-device`, `--rabbit-devices`, `--rabbit-n-devices`,
`--rabbit-mc-stat`, and `--fit-dataset` also propagate through `FullAnalysisDAG`.
The LAW default dataset remains observed. Rabbit task completion verifies task
identity, current model/input hashes, and the native output. The historical
`analysis` backend remains the separate lightweight chi-square calculation.
Manifest Combine execution uses the shared standalone runner; its `--method`
and `--combine-options` remain Combine-specific.

## Validation

```bash
PYTHONPATH=core/python .venv-rabbit/bin/python -m pytest \
  core/tests/python/test_statistical_inference.py \
  core/tests/law/test_combine_tasks.py core/tests/law/test_dag_tasks.py

PYTHONPATH=core/python RDF_TEST_RABBIT=1 RDF_TEST_RABBIT_GPUS=1,4 \
  .venv-rabbit/bin/python -m pytest core/tests/python/test_rabbit_runtime.py
```

The opt-in native test compares physical best-fit parameters, covariance and
saved predictions with an analytic two-parameter Poisson model, on CPU and
the requested GPU counts. It requires actual device access outside restricted
sandboxes. Full-analysis equivalence and production performance require their
own representative model validation.

Recorded node results and remaining analysis-test failures are in
[the validation report](RABBIT_VALIDATION.md).

Upstream sources: [TensorWriter](https://github.com/WMass/rabbit/blob/a0e8323a1f6489abc5d25b6c1c90933ccee55deb/rabbit/tensorwriter.py),
[parameter model](https://github.com/WMass/rabbit/blob/a0e8323a1f6489abc5d25b6c1c90933ccee55deb/rabbit/param_models/param_model.py),
[multi-device fitter](https://github.com/WMass/rabbit/blob/a0e8323a1f6489abc5d25b6c1c90933ccee55deb/rabbit/sharding.py).
