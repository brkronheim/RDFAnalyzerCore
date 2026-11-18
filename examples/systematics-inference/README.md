# Systematics and native BDT inference in a Python analysis

This deliberately synthetic tutorial uses an actual `BDTManager` model inside
the native event loop. Python/Numba constructs its feature; the existing
SystematicManager propagates a shape variation through selection, feature
packing and inference. A separate weight variation changes histogram weights.
It is not a CMS calibration, trained physics classifier, AGC benchmark or new
inference implementation. The one-split model in this directory is an authored
test asset: raw margins -1/+1, base margin0, split ml_feature<0.6. BDTManager
applies its sigmoid, giving approximately0.269/0.731, not raw margins.

Use the full Python installation and its matching ROOT/Numba environment. Default
invocation imports only the standard-library frontend and describes settings,
the exact variations and expected outputs without opening input/model files or
constructing a native analysis:

```bash
python /path/to/prefix/share/rdfanalyzer/examples/systematics-inference/analysis.py
```

The explicit input-creation and native execution commands are:

```bash
python /path/to/prefix/share/rdfanalyzer/examples/systematics-inference/analysis.py \
  --mode make-input --input /tmp/new-inference.root
python /path/to/prefix/share/rdfanalyzer/examples/systematics-inference/analysis.py \
  --mode run --input /tmp/new-inference.root --output /tmp/new-inference-output.root
```

Data/model configuration is staged only for explicit run. A temporary native
model record preserves feature order and the `run_model` scalar-bool gate;
`AddPlugin`/`applyAllBDTs` load/apply the supplied core manager. Its source model
remains available through execution. Native model configuration currently needs
paths without whitespace, so the asset is copied to the run's temporary directory.
Use a TMPDIR without whitespace. No inference is substituted into a Python event
callback or synthetic linear score. Input and metadata paths must be fresh.

## Selection, variation and signed-weight oracle

Three events have x=[0.45,0.55,0.85], genWeight=[1,-2,3]. The graph counts input,
defines explicit up/down x columns, registers a shape pair, filters x>0.5, then
defines the float feature and applies BDTManager. Shape changes x by1.2/0.8.
The weight nuisance substitutes genWeight by2/0.5 times itself. It does not
change features or inference. Labels are independent slices, not a Cartesian
product. These prescriptions are deterministic tutorial inputs, not uncertainties
derived from a calibration.

| Slice | Accepted events | Weighted low/high score bins | Variance low/high |
|---|---|---|---|
| Nominal | 2 | -2,3 | 4,9 |
| shapeUp | 3 | 1,1 | 1,13 |
| shapeDown | 1 | 0,3 | 0,9 |
| weightUp | 2 | -4,6 | 16,36 |
| weightDown | 2 | -1,1.5 | 1,2.25 |

Each score histogram has2 bins over0–1, with empty underflow/overflow. Matching
unweighted `acceptance` histograms expose the selected counts independently of
signed-weight cancellation. The first event fails nominal selection but passes
shapeUp; the second changes BDT leaf under shapeUp. `selected_union` is3 because
the native filter retains the union of varied acceptance. It is not the nominal
selected count of2. Histograms use their booked variation's mask.

The graph is freely interleaved. Shape and weight registration precede the filter
and consumers; the Numba feature remains downstream of selection. Its varied
inputs are declared to SystematicManager for feature/BDT reevaluation, while
weight-only slices use nominal kinematic inputs. Native/plugin
internals can remain opaque in Python descriptions; this guide does not claim
measured evaluator call counts from a configuration or recorded builder.

Lightweight acceptance checks configuration/default imports, ordinary Python
shift kernels, the mathematical oracle and recorded native-plugin/graph setup
order. Installed native acceptance also passes: Numba/BDT outputs, varied
acceptance masks, persisted flows/variances and exactly one event loop.
The in-memory variance comparison allows 1e-14 relative roundoff from squaring
ROOT's reported bin error; persisted Sumw2 checks remain exact. This validates
the synthetic tutorial, without establishing calibrated physics or throughput.
