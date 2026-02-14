## Dev Branch
This branch is for active development and may be unstable. The repository provides the core, analysis‑agnostic framework for constructing and running an Analyzer pipeline using ROOT RDataFrame. Analysis-specific code lives in separate repos under analyses/.

## Requirements
- ROOT 6.30/02 (progress bar support was added around 6.28).

## Repository Organization
- core: framework code (Analyzer, managers, interfaces, plugins, utilities).
- analyses: analysis-specific code (kept in separate repos and cloned here).
- runners: concrete applications that build and execute analyses.
- core/python: helper scripts for job submission and orchestration.

## Plugins Overview

The framework includes several plugins for common analysis tasks:

### BDTManager
Manages Boosted Decision Trees (BDTs) using the FastForest library. Load BDTs from text files and apply them to data with sigmoid activation.

### OnnxManager
Manages ONNX machine learning models. Load and evaluate ONNX models on input features. ONNX Runtime is automatically downloaded during the build process, so no manual installation is required.

### SofieManager
Manages SOFIE (System for Optimized Fast Inference code Emit) models from ROOT TMVA. SOFIE generates optimized C++ code from ONNX models at build time for fast inference. Unlike ONNX models which are loaded at runtime, SOFIE models are compiled directly into the framework.

### CorrectionManager
Applies scale factors and corrections using the correctionlib library.

### TriggerManager
Handles trigger logic and trigger menu configuration.

### NDHistogramManager
Books and fills N-dimensional histograms with support for systematics, regions, and categories. Supports both manual histogram booking and config-driven histogram definitions.

**Config-Driven Histograms**: Define histograms in a configuration file for dynamic runtime booking. See `docs/CONFIG_HISTOGRAMS.md` for detailed documentation.

Quick example:
```cpp
// Enable histogram manager
auto histManager = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());
analyzer.addPlugin("histogramManager", std::move(histManager));

// Define variables and apply filters
analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"});
analyzer.Filter("quality", isGood, {"jet_quality"});

// Book histograms from config file (after all defines/filters)
analyzer.bookConfigHistograms();

// Save results
analyzer.save();
```

Config file format (`histograms.txt`):
```
name=pt_hist variable=jet_pt weight=event_weight bins=50 lowerBound=0.0 upperBound=500.0
```

## Installing
Clone the repository:
```
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
```

## Building
On lxplus:
```
source env.sh
```

Build via the wrapper script:
```
source build.sh
```

## Adding Analyses (Philosophy)
Analyses are developed in separate repositories and cloned into analyses/. The build automatically discovers and adds any subdirectory in analyses/ that contains a CMakeLists.txt, so users only need to clone their analysis repo into that folder and re-run the build.

Example:
```
cd analyses
git clone <your-analysis-repo> MyAnalysis
```
Then rebuild with:
```
source build.sh
```

---

# How the Framework Works

## Core Concepts
The framework is centered around an Analyzer that orchestrates:
- A configuration provider (ConfigurationManager).
- A dataframe provider (DataManager, wrapping ROOT::RDataFrame).
- A systematic manager (SystematicManager).
- Plugins (BDTManager, CorrectionManager, TriggerManager, NDHistogramManager, etc.).
- Output sinks (skim output and metadata/hist output).

All managers are wired in C++ (compile‑time wiring). The Analyzer is agnostic to concrete plugin types and interacts through interfaces.

## Data Flow
1. Configuration is loaded and parsed by ConfigurationManager.
2. DataManager builds a TChain and exposes a ROOT::RDataFrame.
3. Variables are defined, aliases and optional branches are registered.
4. Plugins attach logic (corrections, triggers, BDTs, histograms).
5. The event loop executes via RDataFrame actions.
6. Outputs are written via output sinks and histogram save paths.

---

# Defining Variables and Cuts

## Defining Variables
Variables are defined on the DataManager, which updates the current RDataFrame node. All user-defined variables become columns in the dataframe.

### Scalar variable
```
dataManager->Define("myVar", []() { return 42.0; }, {}, *systematicManager);
```

### Derived variable with dependencies
```
dataManager->Define("ptOverMass",
										[](double pt, double mass) { return pt / mass; },
										{"pt", "mass"},
										*systematicManager);
```

### Per-sample constants
```
dataManager->DefinePerSample("sampleWeight",
														 [](unsigned int, const ROOT::RDF::RSampleInfo&) { return 1.0f; });
```

### Vectors
To create a vector column from scalars or concatenate RVecs, use DefineVector:
```
dataManager->DefineVector("lepPts", {"lep1_pt", "lep2_pt"}, "Float_t", *systematicManager);
```

DefineVector enforces that all input columns are either scalar or RVec. Mixed inputs are not supported.

## Cuts / Filters
Filters are applied to the current dataframe node and can be chained:
```
dataManager->Filter([](double pt) { return pt > 25.0; }, {"pt"});
```

Filters can be used by plugins or analysis code to restrict event selection prior to histogramming or output.

---

# Systematics
Systematics are tracked by SystematicManager. When variables are defined via Define with a SystematicManager, systematic variations are registered and can be propagated through the pipeline. SystematicManager can generate per-sample systematic indices and exposes lists of active variations.

---

# Plugin System

## Overview
The core uses a plugin-based architecture for all manager components:
- Analyzer is agnostic to specific types (BDT, Correction, Trigger, NDHistogram, etc.).
- Any manager implementing IPluggableManager can be used as a plugin.
- New plugins can be added without modifying Analyzer.

## Instantiating Plugins (C++ Wiring)
Plugins are created directly in C++ and passed to Analyzer:
```cpp
auto bdt = std::make_unique<BDTManager>(*configProvider);
auto corr = std::make_unique<CorrectionManager>(*configProvider);
auto trig = std::make_unique<TriggerManager>(*configProvider);
```

## Using Plugins with Analyzer
Analyzer is constructed with a map of plugin role names to plugin instances:
```cpp
std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
plugins["bdt"] = std::move(bdt);
plugins["correction"] = std::move(corr);
plugins["trigger"] = std::move(trig);
Analyzer analyzer(configFile, std::move(plugins));
```

Access plugins via:
```cpp
IBDTManager* bdt = analyzer.getPlugin<IBDTManager>("bdt");
```

## Extending with New Plugins
- Implement a manager that inherits IPluggableManager.
- Register it with REGISTER_MANAGER_TYPE (if used by factory creation).
- Add it to the plugin map when constructing Analyzer.
- Access it via getPlugin<YourInterface>("your_role").

---

# Using OnnxManager for Machine Learning Models

OnnxManager enables you to load and evaluate ONNX (Open Neural Network Exchange) machine learning models on your data. It works similarly to BDTManager but supports the broader ONNX format, which can represent neural networks and other ML models.

## Setup

ONNX Runtime is automatically downloaded and configured during the CMake build, so no manual installation is needed. The framework downloads pre-built binaries from the official ONNX Runtime releases.

## Configuration

Create a configuration file (e.g., `cfg/onnx_models.txt`) with your ONNX models:

```
file=path/to/model1.onnx name=dnn_score inputVariables=pt,eta,phi,mass runVar=has_jet
file=path/to/model2.onnx name=classifier inputVariables=lep_pt,lep_eta,met runVar=pass_presel
```

Each line specifies:
- **file**: Path to the ONNX model file
- **name**: Name for the model output column in the DataFrame
- **inputVariables**: Comma-separated list of input feature names (must match DataFrame columns)
- **runVar**: Boolean column name that controls when the model is evaluated

Add this to your main configuration file:
```
onnxConfig=cfg/onnx_models.txt
```

## Using in C++

### Instantiate OnnxManager
```cpp
auto onnxManager = std::make_unique<OnnxManager>(*configProvider);
ManagerContext ctx{*configProvider, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
onnxManager->setContext(ctx);
```

**Important**: Models are loaded during construction but NOT automatically applied to the DataFrame. You must explicitly call `applyModel()` or `applyAllModels()` after defining all input features.

### Apply ONNX Models
```cpp
// First, define your input features
dataManager->Define("pt", [](float x) { return x; }, {"jet_pt"}, *systematicManager);
dataManager->Define("eta", [](float x) { return x; }, {"jet_eta"}, *systematicManager);
// ... define all required features ...

// Then apply the models
onnxManager->applyModel("dnn_score");

// Or apply all configured models
onnxManager->applyAllModels();
```

### Access Model Information
```cpp
// Get the list of all model names
auto modelNames = onnxManager->getAllModelNames();

// Get input features for a specific model
const auto& features = onnxManager->getModelFeatures("dnn_score");

// Get the run variable for a model
const auto& runVar = onnxManager->getRunVar("dnn_score");

// Get ONNX input/output names from the model
const auto& inputNames = onnxManager->getModelInputNames("dnn_score");
const auto& outputNames = onnxManager->getModelOutputNames("dnn_score");
```

## Multiple Outputs

Models with multiple outputs are fully supported (e.g., ParticleTransformer with bootstrapped models):

```cpp
// Apply a multi-output model
onnxManager->applyModel("particle_transformer");

// Access the individual outputs
auto df = dataManager->getDataFrame();
auto output0 = df.Take<float>("particle_transformer_output0");
auto output1 = df.Take<float>("particle_transformer_output1");
auto output2 = df.Take<float>("particle_transformer_output2");
```

For models with multiple outputs:
- Each output tensor creates a separate column
- Columns are named `{modelName}_output0`, `{modelName}_output1`, etc.
- An intermediate column `{modelName}_outputs` contains all outputs as a vector

## Behavior

- When `runVar` evaluates to `true`, the model inference runs and returns the model output(s)
- When `runVar` evaluates to `false`, the output(s) are set to `-1.0` (skipping computation)
- The manager creates an intermediate column `input_<modelName>` containing the input feature vector
- Models are loaded once at construction time and reused for all events
- **Models are NOT applied automatically** - you must explicitly call `applyModel()` after defining inputs

## Creating ONNX Models

You can create ONNX models from various ML frameworks:

### From scikit-learn
```python
from sklearn.ensemble import RandomForestClassifier
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# Train your model
model = RandomForestClassifier()
model.fit(X_train, y_train)

# Convert to ONNX
initial_type = [('float_input', FloatTensorType([None, X_train.shape[1]]))]
onnx_model = convert_sklearn(model, initial_types=initial_type)

# Save
with open("model.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())
```

### From PyTorch
```python
import torch

# Define and train your model
model = MyNeuralNetwork()
# ... train model ...

# Export to ONNX
dummy_input = torch.randn(1, num_features)
torch.onnx.export(model, dummy_input, "model.onnx")
```

### From TensorFlow/Keras
```python
import tf2onnx
import tensorflow.keras as keras

# Load or train your Keras model
model = keras.models.load_model("model.h5")

# Convert to ONNX
onnx_model, _ = tf2onnx.convert.from_keras(model)

# Save
with open("model.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())
```

---

# Using SofieManager for Machine Learning Models

SofieManager enables you to use SOFIE (System for Optimized Fast Inference code Emit) models from ROOT TMVA. SOFIE generates optimized C++ inference code from ONNX models at build time, providing faster inference than runtime ONNX evaluation.

## Key Differences from OnnxManager

- **Build-time compilation**: SOFIE models are converted from ONNX to C++ code and compiled with the framework
- **No runtime file loading**: Models are embedded as compiled code, not loaded from files at runtime
- **Manual registration**: You must manually register SOFIE inference functions in your code
- **Better performance**: Generated C++ code is optimized and avoids runtime overhead

## Generating SOFIE Code from ONNX Models

SOFIE requires you to generate C++ inference code from your ONNX models. This is typically done using ROOT's Python interface:

```python
import ROOT
from ROOT import TMVA

# Load your ONNX model
model = TMVA.Experimental.SOFIE.RModelParser_ONNX("model.onnx")

# Generate C++ code
model.Generate()
model.OutputGenerated("MyModel.hxx")
```

This generates a header file containing the inference code that you can include in your project.

## Configuration

Create a configuration file (e.g., `cfg/sofie_models.txt`) with your SOFIE models:

```
name=dnn_score inputVariables=pt,eta,phi,mass runVar=has_jet
name=classifier inputVariables=lep_pt,lep_eta,met runVar=pass_presel
```

Each line specifies:
- **name**: Name for the model output column in the DataFrame
- **inputVariables**: Comma-separated list of input feature names (must match DataFrame columns)
- **runVar**: Boolean column name that controls when the model is evaluated

Add this to your main configuration file:
```
sofieConfig=cfg/sofie_models.txt
```

Note: Unlike ONNX/BDT managers, the configuration does NOT include file paths since SOFIE models are compiled code.

## Using in C++

### Include Generated Headers

First, include the SOFIE-generated headers for your models:
```cpp
#include "MyDNNModel.hxx"  // Generated SOFIE code
```

### Create Wrapper Functions

Create wrapper functions that match the SofieInferenceFunction signature:
```cpp
std::vector<float> dnnScoreInference(const std::vector<float>& input) {
    // Assuming the generated model has a Session class
    TMVA_SOFIE_MyDNNModel::Session session;
    std::vector<float> output = session.infer(input.data());
    return output;
}
```

### Instantiate and Register Models

```cpp
auto sofieManager = std::make_unique<SofieManager>(*configProvider);
ManagerContext ctx{*configProvider, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
sofieManager->setContext(ctx);

// Register your SOFIE models manually
auto dnnFunc = std::make_shared<SofieInferenceFunction>(dnnScoreInference);
std::vector<std::string> dnnFeatures = {"pt", "eta", "phi", "mass"};
sofieManager->registerModel("dnn_score", dnnFunc, dnnFeatures, "has_jet");
```

### Apply SOFIE Models

```cpp
// First, define your input features
dataManager->Define("pt", [](float x) { return x; }, {"jet_pt"}, *systematicManager);
dataManager->Define("eta", [](float x) { return x; }, {"jet_eta"}, *systematicManager);
// ... define all required features ...

// Then apply the models
sofieManager->applyModel("dnn_score");

// Or apply all configured models
sofieManager->applyAllModels();
```

### Access Model Information

```cpp
// Get the list of all model names
auto modelNames = sofieManager->getAllModelNames();

// Get input features for a specific model
const auto& features = sofieManager->getModelFeatures("dnn_score");

// Get the run variable for a model
const auto& runVar = sofieManager->getRunVar("dnn_score");
```

## Behavior

- When `runVar` evaluates to `true`, the model inference runs and returns the model output
- When `runVar` evaluates to `false`, the output is set to `-1.0` (skipping computation)
- The manager creates an intermediate column `input_<modelName>` containing the input feature vector
- Models are registered once at setup time and reused for all events
- **Models are NOT applied automatically** - you must explicitly call `applyModel()` after defining inputs

## Complete Example

```cpp
#include <SofieManager.h>
#include "MyGeneratedModel.hxx"

// Wrapper function for SOFIE model
std::vector<float> myModelInference(const std::vector<float>& input) {
    TMVA_SOFIE_MyModel::Session session;
    std::vector<float> output = session.infer(input.data());
    return output;
}

// In your analysis code
auto sofieManager = std::make_unique<SofieManager>(*configProvider);
ManagerContext ctx{*configProvider, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
sofieManager->setContext(ctx);

// Register the model
auto inferenceFunc = std::make_shared<SofieInferenceFunction>(myModelInference);
std::vector<std::string> features = {"var1", "var2", "var3"};
sofieManager->registerModel("my_model", inferenceFunc, features, "run_model");

// Define input variables
dataManager->Define("var1", [](float x) { return x; }, {"input1"}, *systematicManager);
dataManager->Define("var2", [](float x) { return x; }, {"input2"}, *systematicManager);
dataManager->Define("var3", [](float x) { return x; }, {"input3"}, *systematicManager);
dataManager->Define("run_model", []() { return true; }, {}, *systematicManager);

// Apply the model
sofieManager->applyModel("my_model");

// Use the output
auto df = dataManager->getDataFrame();
auto result = df.Take<float>("my_model");
```

---

# Histogram Output

## NDHistogramManager
NDHistogramManager books and saves N-dimensional histograms using a custom THnMulti action. It supports:
- Scalar or RVec inputs for base variables, weights, and region/category axes.
- Systematic axes via SystematicManager.
- Multi-fill handling with scalar expansion to match vector sizes.

### Defining Histograms
Define histogram metadata using histInfo and selectionInfo:
```cpp
std::vector<histInfo> infos = {
	histInfo("jet_pt", "jet_pt", "Jet pT", "weight", 50, 0.0, 500.0)
};

std::vector<selectionInfo> selections = {
	selectionInfo("channel", 3, 0, 3),
	selectionInfo("region",  2, 0, 2),
	selectionInfo("sample",  5, 0, 5)
};

std::vector<std::vector<std::string>> regionNames = {
	{"chanA", "chanB", "chanC"},
	{"SR", "CR"},
	{"sample1", "sample2", "sample3", "sample4", "sample5"}
};

histogramManager->bookND(infos, selections, "", regionNames);
```

The systematic axis is appended automatically if not already present in regionNames.

### Saving Histograms
Histograms are saved with:
```
histogramManager->saveHists(fullHistList, allRegionNames);
```

The file is chosen using metaFile in the config, with saveFile as a fallback.

---

# End-to-End Example: Cuts + Histograms + Outputs

Below is a minimal example that:
1) Defines variables and cuts
2) Books histograms
3) Writes both skim and metadata/hist outputs

```cpp
#include <Analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <SystematicManager.h>
#include <DefaultLogger.h>
#include <RootOutputSink.h>
#include <NDHistogramManager.h>
#include <api/ManagerContext.h>
#include <plots.h>

// Configure and wire core services
auto config = std::make_unique<ConfigurationManager>("cfg/analysis.txt");
auto dataManager = std::make_unique<DataManager>(*config);
auto systematicManager = std::make_unique<SystematicManager>();
auto logger = std::make_unique<DefaultLogger>();

// Outputs (skim + metadata/hist)
auto skimSink = std::make_unique<RootOutputSink>(config->get("saveFile"));
auto metaSink = std::make_unique<RootOutputSink>(config->get("metaFile"));

// Histogram manager
auto histManager = std::make_unique<NDHistogramManager>(*config);
ManagerContext ctx{*config, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
histManager->setContext(ctx);

// Define variables
dataManager->Define("weight", []() { return 1.0; }, {}, *systematicManager);
dataManager->Define("pt", []() { return 42.0; }, {}, *systematicManager);
dataManager->Define("eta", []() { return 0.1; }, {}, *systematicManager);

// Define cuts (filters)
dataManager->Filter([](double pt) { return pt > 25.0; }, {"pt"});
dataManager->Filter([](double eta) { return std::abs(eta) < 2.4; }, {"eta"});

// Book histograms
std::vector<histInfo> infos = {
	histInfo("h_pt", "pt", "pT", "weight", 50, 0.0, 500.0),
	histInfo("h_eta", "eta", "eta", "weight", 48, -2.4, 2.4)
};

std::vector<selectionInfo> selections = {
	selectionInfo("zero__", 1, 0.0, 1.0) // default channel axis
};

std::vector<std::vector<std::string>> regionNames = {
	{"all"}
};

histManager->bookND(infos, selections, "", regionNames);

// Run the event loop by triggering histogram computation
auto &histos = histManager->GetHistos();
for (auto &h : histos) {
	h.GetPtr();
}

// Save histograms (metadata/hist output)
std::vector<std::vector<histInfo>> fullHistList = {infos};
histManager->saveHists(fullHistList, regionNames);

// Save skim output (example: write via sink as needed for your runner)
// skimSink->write(...);
```

---

# Output Sinks
Analyzer uses output sinks to separate skim output (event-level or tree output) from metadata/hist output. The default sink for skims is RootOutputSink; metadata/hist output is routed through meta sinks. This allows clean separation between event outputs and histogram/metadata files.

---

# Configuration
ConfigurationManager reads configuration files and exposes a uniform API for values and config parsing. It supports:
- Key/value retrieval.
- Pair-based configs (e.g., constants).
- Multi-key configs (e.g., aliases, optional branches).

Typical configuration keys include:
- saveFile: default output file for skims.
- metaFile: output file for metadata/histograms.
- Input file and tree name settings (analysis-specific).

## Counters and Generator-Weight Sums
The built-in CounterService can log event counts and optional weight sums per sample. Enable it and configure the branches in your config file:

Required:
- enableCounters: set to 1/true to activate the CounterService.

Optional (weight sums):
- counterWeightBranch: branch/column name holding per-event generator weights (float/double).
- counterIntWeightBranch: integer-valued branch/column name to sum (useful for stitching codes or integer-based weights).

Example config:
```
enableCounters=true
counterWeightBranch=genWeight
counterIntWeightBranch=stitchWeight
```

Example analysis code to define an integer stitching weight before save():
```
analyzer.Define("stitchWeight",
				[](int nGenJets, int ptBin) { return 100 * nGenJets + ptBin; },
				{"nGenJets", "ptBin"});
```

The CounterService runs during Analyzer::save() and logs:
- total entries
- sum of counterWeightBranch (if set)
- sum of counterIntWeightBranch (if set)

When counterIntWeightBranch is configured, the service also writes a TH1D to the
metadata ROOT output (metaFile, falling back to saveFile if metaFile is unset):
- histogram name: counter_intWeightSum_<sample>
- x-axis: integer branch values (one bin per integer value)
- bin content: sum of counterWeightBranch for events in that bin (or unit weights if counterWeightBranch is unset)

---

# Custom ROOT Dictionaries (Serialization)
To process custom object types (for example, MiniAOD-style classes), you can provide ROOT dictionary definitions that are built and linked into the framework. The build system exposes cache variables to supply headers, a LinkDef file, include paths, and optional source files.

## What you provide
- Headers that declare the classes to serialize.
- A LinkDef.h file with ROOT dictionary directives.
- Optional source files that implement the classes.

## Configure the build
Pass the following CMake cache variables when configuring:
- RDF_CUSTOM_DICT_HEADERS: semicolon-separated list of headers
- RDF_CUSTOM_DICT_LINKDEF: path to LinkDef.h
- RDF_CUSTOM_DICT_INCLUDE_DIRS: semicolon-separated include dirs
- RDF_CUSTOM_DICT_SOURCES: semicolon-separated source files (optional)
- RDF_CUSTOM_DICT_TARGET: target name for the dictionary library (optional)

Example:
```
cmake -S . -B build \
	-DRDF_CUSTOM_DICT_HEADERS="/path/to/include/MyEvent.h;/path/to/include/MyObject.h" \
	-DRDF_CUSTOM_DICT_LINKDEF="/path/to/include/MyLinkDef.h" \
	-DRDF_CUSTOM_DICT_INCLUDE_DIRS="/path/to/include;/path/to/other/includes" \
	-DRDF_CUSTOM_DICT_SOURCES="/path/to/src/MyEvent.cc;/path/to/src/MyObject.cc" \
	-DRDF_CUSTOM_DICT_TARGET="MyCustomDict"
```

The dictionary library is built via ROOT_GENERATE_DICTIONARY and linked into the core library so RDataFrame can read and stream the custom types.

---

# Tests
Tests live under core/test and cover managers, configuration, and histogramming. Run the test suite with:
```
source test.sh
```

---

# Condor Submission Helpers
The submission scripts in core/python share a common backend (core/python/submission_backend.py) that generates Condor run scripts and submit files. There are two frontends:
- core/python/generateSubmissionFilesNANO.py (Rucio-based discovery)
- core/python/generateSubmissionFilesOpenData.py (CERN Open Data discovery)

These scripts are the recommended way to submit analyses to HTCondor from this framework.

Important: when using `--eos-sched` to create submissions under EOS, you must activate the EOS Condor submission environment before submitting. Run:

```
module load lxbatch/eossubmit
```

This ensures `condor_submit` targets the EOS-aware batch system used at your site.

## Common options
- --exe PATH: path to the C++ executable to run
- --root-setup "CMD": command to source ROOT (optional). If omitted, the job uses only whatever is available on the worker.
- --stage-inputs: xrdcp input ROOT files to the worker before running
  - When `--stage-inputs` is enabled the submitter will xrdcp URLs to local files before running the job. If a URL contains a site-specific test redirector (e.g. `.../store/test/xrootd/<SITE>//store/mc/...`) the staging step will automatically normalize it to the generic path (`root://xrootd-cms.infn.it/store/mc/...`) for the copy. NOTE: this normalization is applied only for the xrdcp staging step — in-job `xrootd` access paths are left unchanged.
- --stage-outputs: write outputs locally, then xrdcp to final destination
- --spool: prepare for condor_submit -spool by copying shared inputs (aux + executable) once per submission
- --threads N: number of worker threads used for remote-metadata queries and per-sample splitting (default: 4). Applies to both `generateSubmissionFilesNANO.py` and `generateSubmissionFilesOpenData.py` — set to 1 for serial (default-compatible) execution.

## Shared inputs (aux + executable)
The submitters now stage the executable and aux directory once per submission under:
```
condorSub_<name>/shared/
```
Each job transfers those shared inputs and the runscript links them into the working directory at runtime. This reduces the size of staging directories and works with EOS spooling (condor_submit -spool). The executable path is resolved to an absolute path to avoid dangling symlinks in test jobs.

## Output staging details
When --stage-outputs is enabled, the runscript:
1) Rewrites saveFile/metaFile in cfg/submit_config.txt to local filenames
2) Runs the executable
3) xrdcp’s the local files to the original destinations

This avoids writing outputs directly over xrootd during processing and reduces network load.

# Notes
- All core managers are plugins. Analyzer does not depend on concrete types.
- Wiring is compile-time C++ construction.
- NDHistogramManager supports multi-fill inputs by expanding scalars to the reference vector length when needed.

