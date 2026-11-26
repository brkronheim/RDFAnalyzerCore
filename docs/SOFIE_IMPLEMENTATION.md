# SOFIE Manager Implementation Guide

Generate a C++ inference header with the standalone [`sofie` package on
PyPI](https://pypi.org/project/sofie/), then compile it into your analysis.
`SofieManager` calls a C++ wrapper around the generated session from the
RDataFrame graph. Generation runs in Python before the analysis build.
The executable runs the generated code and reads any separate weights file
when constructing a session.

## Generate SOFIE C++ from ONNX

Install the generator in a Python environment and use a model with one
float32 input tensor of shape `[1, 4]` and one float output tensor for this
example. The base package reads ONNX directly and the generation step works
without PyROOT or ONNX Runtime. The package also offers optional
`sofie[keras]` and `sofie[torch]` extras for native model formats. See the
[PyPI usage examples](https://pypi.org/project/sofie/#usage) for those parsers.

```bash
python -m pip install sofie
```

```python
import sofie

if not sofie.onnx.check_model("classifier.onnx"):
    raise SystemExit("The model uses operators unsupported by SOFIE")

model = sofie.onnx.parse("classifier.onnx")
model.generate()
model.output_generated("classifier.hxx")
```

Keep the generated header and any emitted weights file together. The default
separate weights for this example are in `classifier.dat` when the model has
weights to write. Read the generated header for the namespace, `Session`
constructor and `infer` signature before writing your wrapper. An output
header's filename alone does not determine the namespace. See the
[standalone SOFIE CPU example](https://github.com/ML4EP/SOFIE#cpu-inference)
for the generated C++ interface.

## Register and apply the compiled model

In this example, `analyzer` is an existing C++ `Analyzer`. Its graph already
has scalar float columns `jet_pt`, `jet_eta`, `jet_phi` and `jet_mass`, and a
scalar bool column `has_jet`. Use the same feature order and preprocessing as
training. The sample wrapper assumes the generated header declares
`SOFIE_classifier::Session`, that `infer` takes a float pointer, and that the
session uses a separate weights file.

```cpp
#include <SofieManager.h>
#include <analyzer.h>
#include <memory>
#include <stdexcept>
#include <vector>
#include "classifier.hxx"

void addClassifier(Analyzer& analyzer) {
    auto manager = SofieManager::create(analyzer);
    auto infer = std::make_shared<SofieInferenceFunction>(
        [](const std::vector<float>& features) {
            if (features.size() != 4) {
                throw std::invalid_argument("classifier expects four features");
            }
            static thread_local SOFIE_classifier::Session session("classifier.dat");
            auto input = features;
            return session.infer(input.data());
        });
    manager->registerModel("classifier", infer,
                           {"jet_pt", "jet_eta", "jet_phi", "jet_mass"}, "has_jet");
    manager->applyModel("classifier");
}
```

This session is reused per thread so workers using ROOT ImplicitMT have
independent scratch buffers. The fixed weights path is for a single model in
this example. Resolve it for your actual execution directory and stage the
file with the executable when submitting workers. For a different signature
or weight mode, adapt the wrapper to the generated header.

`applyModel` defines `input_classifier` and the scalar `Float_t` output
`classifier`. A false `runVar` produces `-1.0f` without calling the wrapper.
For enabled events the manager takes element zero of the returned vector
and throws if it is empty. To select a different element, return that element
as the first value from your wrapper. To expose multiple values, use
separate graph columns or a custom vector-valued definition.

For a multi-input model, split the packed feature vector at the tensor
boundaries given by the export and pass the pointers to `infer` in the order
declared in the generated header. Multi-output sessions can return a vector
of tensors. Select the output tensor and element your scalar column represents.
Validate downstream calibrated scores and weights on representative events
as well as the raw network scores. For example, with `sofie==0.1.0` a tested
multi-head NLO calibration passed the operator check, but the emitted code
reused the first head's Boolean condition across the other heads. Include
events where different heads take different branches in the comparison.

## Systematic variations

`SofieManager::applyModel` uses `DefineVector` to assemble the features for
one evaluation. If a feature has registered Up/Down shifts or explicit
variation labels, the framework builds the corresponding feature vectors.
The score definition depends on both that vector and the boolean `runVar`,
so a shifted gate also propagates. Register the variations before applying
the model. RDataFrame evaluates score columns when an action consumes them.
For each requested score whose gate is true the wrapper runs once for that
event and column. A false gate yields `-1.0f` and skips the inference call.

A one-event scalar test model booked the nominal, JES-up and JES-down scores in the same
RDataFrame loop. The nominal and up gates passed and returned scores of 2
and 5. The down gate failed and returned -1. The wrapper ran exactly twice. Unaffected features
retain their nominal values in the shifted input vectors.

ONNX also has an optional `systematicBundle` execution path. For a supported
model, it packs active variation rows from the same event into one inference
call, then maps the results back to the corresponding variations. It can
combine the run gate with a selection mask. SOFIE has no such configuration
or batched callable contract. Its feature packing groups inputs **within**
one variation. OpenBLAS accelerates the generated numerical kernels inside
each enabled call.

The NLO ensemble's raw output shape `[1, 20, 18]` contains bootstrap replicas
and model heads. Those 360 values belong to one evaluation. The ONNX bundle
path requires one scalar per row of each output tensor, so that raw tensor
also falls outside its supported layout. Even a scalar-output export needs
a batch dimension large enough for the active variation rows. A fixed batch
of one can accommodate only one active row. A SOFIE wrapper can select a head
or compute a replica summary for the scalar score column. See
[systematic authoring](SYSTEMATIC_AUTHORING.md) for variation registration.

## Optional model metadata

Manual registration is sufficient. To declare model metadata in a separate
file, set `sofieConfig: cfg/sofie_models.yaml` in the main configuration and
include a record such as the following. An explicitly configured file must exist and each entry must provide a model name, feature list and gate.

```yaml
- name: classifier
  inputVariables: jet_pt,jet_eta,jet_phi,jet_mass
  runVar: has_jet
```

Keep the record and registration consistent. Earlier entries for a model name
are retained when the callable is registered. The record supplies the ordered
features and the gate. The C++ analysis supplies the callable. Register every
configured model before calling `applyAllModels()`.

## Build, deploy and validate

An in-tree analysis executable linked to `coreAll` automatically uses an
available LP64 OpenBLAS for the generated CPU kernels.

```cmake
add_executable(myanalysis analysis.cc)
target_include_directories(myanalysis PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/sofie_models")
target_link_libraries(myanalysis PRIVATE coreAll)
```

An installed Analyzer SDK consumer gets the same default from
`RDFAnalyzer::Core`. CMake discovers the library on the consumer's machine.
On GNU-compatible Linux links, executables that use no symbols from OpenBLAS
can omit it from their runtime dependencies. A framework build without
OpenBLAS can still configure. If your target requires the generated BLAS
kernels, `rdf_target_link_sofie(myanalysis)` checks for a provider at
configure time.

To select a different **LP64** BLAS for the build, configure with
`-DRDF_SOFIE_BLAS_LIBRARY=/absolute/path/to/libblas.so`.
For a single target with an existing provider, pass its link target or
library path as the second argument to `rdf_target_link_sofie`.
The provider must export the Fortran symbols used by the generated header,
such as `sgemm_`, and be available in the worker runtime environment.
CMake selects the library; the provider controls its own thread pool.
For one BLAS thread per inference, use `OMP_NUM_THREADS=1` with an OpenMP
OpenBLAS build or `OPENBLAS_NUM_THREADS=1` with a pthreads build.

Re-export the model, regenerate the header and weights as a set, and rebuild
the analysis when the model changes. Record the generator version and source
model identity. Compare known predictions with the originating model using
identical features and preprocessing, then test the gate and an ImplicitMT
run. Check that workers can read the weights file and load the linked BLAS.
Measure setup and event-loop execution separately if comparing performance
with [ONNX Runtime](ONNX_IMPLEMENTATION.md).

See the [configuration reference](CONFIG_REFERENCE.md#sofie-manager-configuration)
and [C++ API reference](API_REFERENCE.md#sofiemanager) for framework details.

An omitted `sofieConfig` permits programmatic registration alone. An explicit recipe must be readable and each model record must provide nonempty `name`, `inputVariables` and `runVar` fields.
