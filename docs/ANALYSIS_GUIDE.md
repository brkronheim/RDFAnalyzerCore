# Analysis Guide

This comprehensive guide walks you through creating a complete analysis with RDFAnalyzerCore using the config-based approach.

## Table of Contents

- [Overview](#overview)
- [Analysis Structure](#analysis-structure)
- [Step-by-Step Tutorial](#step-by-step-tutorial)
- [Config-Based Analysis Pattern](#config-based-analysis-pattern)
- [Working with Data](#working-with-data)
- [Event Selection](#event-selection)
- [Machine Learning Integration](#machine-learning-integration)
- [Corrections and Scale Factors](#corrections-and-scale-factors)
- [Histogramming](#histogramming)
- [Systematics](#systematics)
  - [SystematicManager Integration](#systematicmanager-integration)
  - [Systematics on PhysicsObjectCollections](#systematics-on-physicsobjectcollections)
- [Managing Event Weights](#managing-event-weights)
- [Analysis Regions](#analysis-regions)
- [Cutflow and Event Selection Monitoring](#cutflow-and-event-selection-monitoring)
- [Complete Example](#complete-example)
- [Advanced Topics](#advanced-topics)
- [Friend Trees](#friend-trees)

## Overview

RDFAnalyzerCore uses a **config-driven architecture** where:
- Core behavior is controlled by text configuration files
- Plugin managers handle common tasks (BDTs, corrections, triggers)
- Analysis-specific logic is written in C++
- The framework orchestrates everything through the Analyzer class

This approach separates configuration from code, making analyses more maintainable and reproducible.

## Analysis Structure

A typical analysis repository has this structure:

```
MyAnalysis/
├── CMakeLists.txt           # Build configuration
├── analysis.cc              # Main analysis code
├── cfg/                     # Configuration directory
│   ├── main_config.txt     # Main configuration
│   ├── output.txt          # Branches to save
│   ├── floats.txt          # Constant columns used by weights
│   ├── histograms.txt      # Histogram definitions
│   ├── bdts.txt           # BDT models (optional)
│   ├── onnx_models.txt    # Neural networks (optional)
│   ├── corrections.txt     # Scale factors (optional)
│   └── triggers.txt        # Trigger configuration (optional)
├── aux/                     # Auxiliary files
│   ├── models/             # ML models
│   └── corrections/        # Correction files
└── scripts/                 # Helper scripts
    └── submit_jobs.py
```

## Step-by-Step Tutorial

### Step 1: Create Analysis Directory

```bash
cd analyses
mkdir MyAnalysis
cd MyAnalysis
```

### Step 2: Create CMakeLists.txt

```cmake
# Tell CMake this is an analysis
add_executable(myanalysis analysis.cc)

# Link against RDFAnalyzerCore and ROOT
target_link_libraries(myanalysis
    PRIVATE
    coreAll
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    ROOT::Hist
    ROOT::MathCore
    ROOT::RIO
)

# Set output directory
set_target_properties(myanalysis PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/analyses/MyAnalysis"
)
```

### Step 3: Create Main Configuration File

Create `cfg/main_config.txt`:

```
# ============== Input/Output ==============
fileList=/path/to/data.root
saveFile=output/myanalysis.root
metaFile=output/histograms.root
saveTree=Events
enableSkim=true
saveConfig=cfg/output.txt
floatConfig=cfg/floats.txt
histogramConfig=cfg/histograms.txt

# ============== Performance ==============
threads=-1
batch=false
# Optional ROOT input settings for remote TTree files:
# treeCacheSizeMB=64
# treeCacheLearnEntries=10
# treeCachePrefill=0
# treeCacheAsyncPrefetch=1
# treeCacheAsyncOpenNext=1

# ============== Plugin configuration paths ==============
# Referencing a plugin config does not construct the plugin. Create and
# register each plugin in analysis.cc when the analysis uses it.
# onnxConfig=cfg/onnx_models.txt
# correctionConfig=cfg/corrections.txt
# triggerConfig=cfg/triggers.txt
```

For remote reads, `treeCacheSizeMB` sets a fixed cache size in MiB per ROOT
tree cache (0 disables the cache; maximum 512). Multithreaded reads may use
multiple caches, and ROOT may clamp the actual size.
Alternatively, `rootTTreeCacheSize` sets the `ROOT_TTREECACHE_SIZE` environment
variable to a multiplier of ROOT's estimated cache size, such as `2.0`.
The config key `ROOT_TTREECACHE_SIZE` is accepted as an alias.
It is **not a byte count**. Set only one of these two size options.

`treeCacheLearnEntries` sets how many initial entries ROOT uses to discover
branches for the cache (at least 1). `treeCachePrefill` accepts `0` or `1` and
controls whether ROOT preloads baskets from all branches during learning.
`treeCacheAsyncPrefetch` accepts `0` or `1` and controls ROOT's
`TFile.AsyncReading` option. These controls are optional; absent keys leave
ROOT's defaults or the existing process environment in effect. The cache and
prefetch settings can increase memory use or unnecessary reads, so compare
them with an otherwise identical job.
`treeCacheAsyncOpenNext=1` overlaps opening the next XRootD file with processing
the current one. It is single-threaded and opt-in; it does not preload the next
file's event data.

LAW's `PrepareSkimJobs` copies these keys from the submit config template into
each job's `submit_config.txt`, including its test job. Its worker setup copies
that per-job config into the runtime sandbox, where the executable applies the
settings before opening input files.

### Step 4: Create Output Branch Configuration

Create `cfg/output.txt` listing columns defined by this analysis that should be kept in the skim:

```
ZBosonMass
LeadingMuonPt
SubleadingMuonPt
```

Create `cfg/floats.txt` with a constant weight column:

```text
normScale=1.0
```

Create `cfg/histograms.txt` with one histogram definition per line:

```text
name=ZBosonMass variable=ZBosonMass weight=normScale bins=60 lowerBound=70 upperBound=110 label=Z Boson Mass [GeV]
name=LeadingMuonPt variable=LeadingMuonPt weight=normScale bins=50 lowerBound=20 upperBound=150 label=Leading Muon pT [GeV]
name=SubleadingMuonPt variable=SubleadingMuonPt weight=normScale bins=50 lowerBound=20 upperBound=150 label=Subleading Muon pT [GeV]
```

### Step 5: Write Analysis Code

Create `analysis.cc`:

```cpp
#include <analyzer.h>
#include <NDHistogramManager.h>

#include <iostream>

int main(int argc, char **argv) {
    // Validate arguments
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    // Create analyzer with configuration file
    Analyzer analyzer(argv[1]);

    // The factory creates and registers this manager as "histogramManager".
    auto histManager = NDHistogramManager::create(analyzer);

    // Define your analysis variables and cuts here
    // They must include the columns referenced by cfg/histograms.txt and
    // cfg/output.txt; see the sections below for examples.

    analyzer.bookConfigHistograms();

    // Save results
    analyzer.run();

    return 0;
}
```

### Step 6: Build and Run

```bash
# The steps above leave the shell in analyses/MyAnalysis. Build from the repo root.
cd ../..
source build.sh
cd analyses/MyAnalysis
../../build/analyses/MyAnalysis/myanalysis cfg/main_config.txt
```

Run from the analysis source directory so relative input/output paths and constant-file existence checks use the expected working directory.

## Config-Based Analysis Pattern

Configuration holds paths and values consumed by the framework and registered plugins. It does not construct plugins on its own.

### 1. Configuration Setup

Configuration files hold I/O, output, and manager settings; analysis code defines the physics behavior and constructs the managers it uses.

```cpp
// Configuration is loaded by the Analyzer constructor
Analyzer analyzer("cfg/main_config.txt");
```

The Analyzer constructor:
- Loads the main configuration
- Sets up the data pipeline
- Configures output sinks

The analysis then creates and registers the managers it needs. Each registered manager receives its context and reads its own config during setup.

### 2. Analysis Logic in C++

Your C++ code defines the analysis-specific logic:

```cpp
// Define variables
analyzer.Define("my_variable", myFunction, {"input1", "input2"});

// Apply cuts
analyzer.Filter("my_cut", cutFunction, {"variable"});

// Book histograms (if using NDHistogramManager)
// Apply ML models (if using OnnxManager/BDTManager)
```

### 3. Plugin Integration

Plugins must be constructed and registered in C++. Their settings can be read from paths in the main config. The `create()` factories register managers under their documented default roles; `getPlugin<T>(role)` must use that same role if retrieving one later.

```cpp
#include <OnnxManager.h>

// Creates and registers the manager as "onnxManager".
auto onnx = OnnxManager::create(analyzer);

// Use plugin
onnx->applyAllModels();
```

### 4. Execution

```cpp
// Trigger execution and save outputs
analyzer.run();
```

This separation means:
- **Config changes** don't require recompilation
- **Code changes** don't affect configuration
- **Reproducibility** is ensured by versioning configs
- **Batch jobs** can use different configs with same binary

## Working with Data

### Defining Variables

The Analyzer provides methods to define new DataFrame columns:

#### Simple Scalar Variable

```cpp
analyzer.Define("weight", []() { 
    return 1.0; 
}, {});
```

#### Derived Variable

```cpp
analyzer.Define("jet_pt_gev", 
    [](float pt_mev) { return pt_mev / 1000.0; },
    {"jet_pt"}
);
```

#### Variable with Multiple Inputs

```cpp
analyzer.Define("delta_phi",
    [](float phi1, float phi2) {
        float dphi = phi1 - phi2;
        while (dphi > M_PI) dphi -= 2*M_PI;
        while (dphi < -M_PI) dphi += 2*M_PI;
        return dphi;
    },
    {"lepton_phi", "jet_phi"}
);
```

#### Vector Variables

```cpp
analyzer.Define("good_jets",
    [](const ROOT::VecOps::RVec<float>& pt, 
       const ROOT::VecOps::RVec<float>& eta) {
        ROOT::VecOps::RVec<int> indices;
        for (size_t i = 0; i < pt.size(); i++) {
            if (pt[i] > 25.0 && std::abs(eta[i]) < 2.5) {
                indices.push_back(i);
            }
        }
        return indices;
    },
    {"jet_pt", "jet_eta"}
);
```

#### Extracting Vector Elements

```cpp
// Get leading jet pT
analyzer.Define("leading_jet_pt",
    [](const ROOT::VecOps::RVec<float>& jet_pt) {
        return jet_pt.size() > 0 ? jet_pt[0] : -1.0f;
    },
    {"jet_pt"}
);
```

### Per-Sample Variables

Define variables that depend on the input sample:

```cpp
analyzer.DefinePerSample("sample_weight",
    [](unsigned int slot, const ROOT::RDF::RSampleInfo& info) {
        std::string sampleName = info.AsString();
        if (sampleName.find("ttbar") != std::string::npos) {
            return 1.2f;  // Scale factor for ttbar
        }
        return 1.0f;
    }
);
```

## Event Selection

### Applying Filters

Filters reduce the dataset to events passing selection criteria:

```cpp
// Single cut
analyzer.Filter("pt_cut",
    [](float pt) { return pt > 25.0; },
    {"jet_pt"}
);

// Multiple conditions
analyzer.Filter("lepton_selection",
    [](int n_leptons, float leading_pt) {
        return n_leptons >= 2 && leading_pt > 30.0;
    },
    {"n_leptons", "leading_lepton_pt"}
);
```

### Filter with Named Cut

Named filters appear in cut flow reports:

```cpp
analyzer.Filter("trigger_passed", 
    [](bool trigger) { return trigger; },
    {"HLT_Mu24"}
);

analyzer.Filter("jet_selection",
    [](int n_jets) { return n_jets >= 4; },
    {"n_jets"}
);
```

### Vector-Based Selection

```cpp
// Select events with at least 2 good muons
analyzer.Define("good_muons",
    [](const ROOT::VecOps::RVec<float>& pt,
       const ROOT::VecOps::RVec<float>& eta,
       const ROOT::VecOps::RVec<int>& id) {
        return ROOT::VecOps::Where(
            pt > 25.0 && abs(eta) < 2.4 && id == 1
        );
    },
    {"muon_pt", "muon_eta", "muon_id"}
);

analyzer.Filter("two_muons",
    [](const ROOT::VecOps::RVec<int>& good_muons) {
        return good_muons.size() == 2;
    },
    {"good_muons"}
);
```

## Machine Learning Integration

### Using ONNX Models (Recommended)

ONNX models provide a standard format for ML models from any framework.

#### 1. Configure ONNX Models

Create `cfg/onnx_models.txt`:
```
file=aux/models/classifier.onnx name=ml_score inputVariables=jet_pt,jet_eta,jet_btag runVar=has_jets
```

Add to main config:
```
onnxConfig=cfg/onnx_models.txt
```

#### 2. Define Input Features

```cpp
// Define all required input features
analyzer.Define("jet_pt", /*...*/);
analyzer.Define("jet_eta", /*...*/);
analyzer.Define("jet_btag", /*...*/);
analyzer.Define("has_jets", 
    [](int n_jets) { return n_jets > 0; },
    {"n_jets"}
);
```

#### 3. Apply Model

```cpp
#include <OnnxManager.h>

// The factory also registers the manager as "onnxManager".
auto onnxMgr = OnnxManager::create(analyzer);

// Apply all configured models
onnxMgr->applyAllModels();

// Now "ml_score" is available as a DataFrame column
```

#### 4. Use Model Output

```cpp
// Apply ML-based selection
analyzer.Filter("ml_selection",
    [](float score) { return score > 0.8; },
    {"ml_score"}
);

// Or use in variables
analyzer.Define("final_weight",
    [](float weight, float ml_score) {
        return weight * ml_score;
    },
    {"event_weight", "ml_score"}
);
```

### Multi-Output Models

For models with multiple outputs (e.g., ParticleTransformer):

```cpp
// Model "transformer" creates: transformer_output0, transformer_output1, ...
onnxMgr->applyModel("transformer");

// Use individual outputs
analyzer.Define("is_top_jet",
    [](float score0) { return score0 > 0.7; },
    {"transformer_output0"}
);

analyzer.Define("is_qcd_jet",
    [](float score1) { return score1 > 0.5; },
    {"transformer_output1"}
);
```

### Using BDTs (FastForest)

Similar to ONNX but for FastForest BDT files:

Create `cfg/bdts.txt`:
```
file=aux/bdts/signal_bdt.txt name=bdt_score inputVariables=var1,var2,var3 runVar=pass_presel
```

```cpp
#include <BDTManager.h>

// The factory also registers the manager as "bdtManager".
auto bdtMgr = BDTManager::create(analyzer);
bdtMgr->applyAllBDTs();
```

### Using SOFIE Models (Maximum Performance)

SOFIE (System for Optimized Fast Inference code Emit) generates compiled C++ code from ONNX models for maximum performance.

**Key Difference**: SOFIE models are compiled at build time, not loaded at runtime.

#### 1. Generate SOFIE C++ Code

```python
import ROOT
from ROOT import TMVA

# Convert ONNX to SOFIE C++ code
model = TMVA.Experimental.SOFIE.RModelParser_ONNX("classifier.onnx")
model.Generate()
model.OutputGenerated("ClassifierModel.hxx")
```

#### 2. Create Wrapper Function

```cpp
#include "ClassifierModel.hxx"  // SOFIE-generated

std::vector<float> classifierInference(const std::vector<float>& input) {
    static TMVA_SOFIE_ClassifierModel::Session session;
    return session.infer(input.data());
}
```

#### 3. Register and Use Model

```cpp
#include <SofieManager.h>

// The factory also registers the manager as "sofieManager".
auto sofieMgr = SofieManager::create(analyzer);

// Register model manually (not auto-loaded like ONNX/BDT)
auto inferenceFunc = std::make_shared<SofieInferenceFunction>(classifierInference);
sofieMgr->registerModel("classifier", inferenceFunc, 
                       {"jet_pt", "jet_eta", "jet_phi"}, "has_jet");

// Apply model
sofieMgr->applyModel("classifier");
```

**When to use SOFIE**:
- Maximum speed is critical
- Model is finalized (won't change frequently)
- Can rebuild between updates

**When to use ONNX**:
- Model still being developed
- Need runtime flexibility
- Want to swap models without recompiling

**See**: [SOFIE Implementation Guide](SOFIE_IMPLEMENTATION.md) for complete details.

### Creating ONNX Models for Analysis

From a trained scikit-learn model:

```python
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# Convert trained model
initial_type = [('float_input', FloatTensorType([None, n_features]))]
onnx_model = convert_sklearn(clf, initial_types=initial_type)

# Save
with open("classifier.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())
```

From PyTorch:

```python
import torch

# Export trained model
dummy_input = torch.randn(1, n_features)
torch.onnx.export(model, dummy_input, "classifier.onnx",
                  input_names=['input'],
                  output_names=['output'])
```

## Corrections and Scale Factors

### Using CorrectionManager

CorrectionManager applies corrections using the correctionlib format.

#### 1. Configure Corrections

Create `cfg/corrections.txt`:
```
file=aux/corrections/muon_sf.json correctionName=NUM_TightID_DEN_TrackerMuons name=muon_id_sf inputVariables=muon_pt,muon_eta
file=aux/corrections/muon_sf.json correctionName=NUM_TightRelIso_DEN_TightIDandIPCut name=muon_iso_sf inputVariables=muon_pt,muon_eta
```

Add to main config:
```
correctionConfig=cfg/corrections.txt
```

#### 2. Apply Corrections

The CorrectionManager automatically applies all configured corrections, creating output columns.

```cpp
// Corrections are now available as DataFrame columns
analyzer.Define("total_muon_sf",
    [](float id_sf, float iso_sf) {
        return id_sf * iso_sf;
    },
    {"muon_id_sf", "muon_iso_sf"}
);

analyzer.Define("event_weight",
    [](float gen_weight, float total_sf) {
        return gen_weight * total_sf;
    },
    {"genWeight", "total_muon_sf"}
);
```

### Creating Correction Files

Use the correctionlib Python library:

```python
import correctionlib
import correctionlib.schemav2 as schema

# Define a correction
corr = schema.Correction(
    name="muon_id_sf",
    version=1,
    inputs=[
        schema.Variable(name="pt", type="real"),
        schema.Variable(name="eta", type="real"),
    ],
    output=schema.Variable(name="weight", type="real"),
    data=...  # Your correction data
)

# Create correction set
cset = schema.CorrectionSet(
    schema_version=2,
    corrections=[corr]
)

# Save to file
with open("corrections.json", "w") as f:
    f.write(cset.json())
```

### Jet Energy Corrections (JES/JER) with JetEnergyScaleManager

For CMS-style Jet Energy Scale (JES) and Jet Energy Resolution (JER)
corrections, use the dedicated `JetEnergyScaleManager` plugin.  It
integrates with `CorrectionManager` for correctionlib evaluations and with
`PhysicsObjectCollection` for clean jet-collection management.

> **Full reference**: [Jet Energy Corrections Guide](JET_ENERGY_CORRECTIONS.md)

#### Typical CMS NanoAOD JEC workflow

```cpp
#include <JetEnergyScaleManager.h>
#include <CorrectionManager.h>
#include <PhysicsObjectCollection.h>

// Factories register the default roles "jetEnergyScaleManager" and
// "correctionManager".
auto jes = JetEnergyScaleManager::create(analyzer);
auto cm = CorrectionManager::create(analyzer);

// 1. Declare jet/MET column names.
jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
jes->setMETColumns("MET_pt", "MET_phi");

// 2. Build a selected jet collection (any user selection).
analyzer.Define("goodJets",
    [](const RVec<float>& pt, const RVec<float>& eta,
       const RVec<float>& phi, const RVec<float>& mass) {
        return PhysicsObjectCollection(pt, eta, phi, mass,
                                       (pt > 25.f) && (abs(eta) < 2.4f));
    },
    {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
);

// 3. Strip NanoAOD JEC → raw pT.
jes->removeExistingCorrections("Jet_rawFactor");

// 4. Evaluate the nominal compound JEC and apply it.
cm->registerCorrection(
    "jec_nominal",
    "jet_jerc.json.gz",
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
cm->applyCorrectionVec("jec_nominal", {}, {}, "Jet_jec_sf_nominal");
jes->applyCorrection("Jet_pt_raw", "Jet_jec_sf_nominal", "Jet_pt_jec");

// 5. Register and apply CMS JES systematic set (reduced: "Total" only).
jes->registerSystematicSources("reduced", {"Total"});
jes->applySystematicSet(*cm, "jes_unc", "reduced",
                        "Jet_pt_jec", "Jet_pt_jes");

// 6. Propagate JEC to MET (Type-1).
jes->propagateMET("MET_pt", "MET_phi",
                  "Jet_pt_raw", "Jet_pt_jec",
                  "MET_pt_jec", "MET_phi_jec");

// 7. Produce corrected PhysicsObjectCollection columns.
jes->setInputJetCollection("goodJets");
jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
jes->defineVariationCollections("goodJets_jec", "goodJets",
                                "goodJets_variations");
// After execute():
//   "goodJets_jec"        PhysicsObjectCollection (nominal JEC)
//   "goodJets_TotalUp"    PhysicsObjectCollection (JES up)
//   "goodJets_TotalDown"  PhysicsObjectCollection (JES down)
//   "goodJets_variations" PhysicsObjectVariationMap (all of the above)

```

The JEC manager creates its corrected collection and variation-map columns in
its `execute()` hook. Those are not input columns available when the analysis
body calls `Analyzer::Define`, so this example does not add a generic downstream
definition that consumes them. See the Systematics section for the supported
three-argument `Define` pattern when the varied input columns already exist.

#### Applying the full CMS JES source set

```cpp
jes->registerSystematicSources("full", {
    "AbsoluteCal", "AbsoluteScale", "AbsoluteMPFBias",
    "FlavorQCD", "Fragmentation", "PileUpDataMC",
    "PileUpPtRef", "RelativeFSR", "RelativeJEREC1",
    "RelativeJEREC2", "RelativeJERHF",
    "RelativePtBB", "RelativePtEC1", "RelativePtEC2",
    "RelativePtHF", "RelativeBal", "RelativeSample"
});
jes->applySystematicSet(*cm, "jes_unc", "full", "Jet_pt_jec", "Jet_pt_jes");
// Creates 34 variation columns and registers 17 systematic families.
```

## Histogramming

### Using NDHistogramManager

NDHistogramManager provides sophisticated N-dimensional histogramming with support for systematics and regions.

#### Basic Histogram

```cpp
#include <plots.h>  // For histInfo, selectionInfo
#include <NDHistogramManager.h>

// Creates and registers the manager under "histogramManager".
auto histMgr = NDHistogramManager::create(analyzer);

// Define histogram metadata
std::vector<histInfo> hists = {
    histInfo("h_jet_pt", "jet_pt", "Jet p_{T}", "event_weight", 50, 0.0, 500.0),
    histInfo("h_jet_eta", "jet_eta", "Jet #eta", "event_weight", 50, -2.5, 2.5),
};

// Define selection axes (regions, channels, etc.)
std::vector<selectionInfo> selections = {
    selectionInfo("channel", 2, 0, 2),  // 2 channels
    selectionInfo("region", 3, 0, 3),   // 3 regions
};

// Names for selection bins
std::vector<std::vector<std::string>> regionNames = {
    {"ee", "mumu"},              // Channel names
    {"SR", "CR_top", "CR_wjets"} // Region names
};

// Book histograms
histMgr->bookND(hists, selections, "", regionNames);
```

#### Defining Selection Variables

```cpp
// Define channel variable (0 = ee, 1 = mumu)
analyzer.Define("channel",
    [](int leading_pdg, int subleading_pdg) {
        if (abs(leading_pdg) == 11 && abs(subleading_pdg) == 11) return 0;
        if (abs(leading_pdg) == 13 && abs(subleading_pdg) == 13) return 1;
        return -1;
    },
    {"leading_lepton_pdg", "subleading_lepton_pdg"}
);

// Define region variable (0 = SR, 1 = CR_top, 2 = CR_wjets)
analyzer.Define("region",
    [](float met, int n_bjets, int n_jets) {
        if (met > 50 && n_bjets >= 1) return 0;  // SR
        if (met < 50 && n_bjets >= 2) return 1;  // CR_top
        if (met < 50 && n_bjets == 0 && n_jets >= 2) return 2;  // CR_wjets
        return -1;
    },
    {"met", "n_bjets", "n_jets"}
);
```

#### Saving Histograms

```cpp
// Analyzer::run() executes booked actions and saves histograms from the
// registered NDHistogramManager to the metadata output file.
analyzer.run();
```

### Vector-Based Histograms

Fill one histogram entry per vector element:

```cpp
// This fills one entry per jet
histInfo("h_all_jets_pt", "jet_pt", "All Jets p_{T}", "event_weight", 50, 0, 500)
```

Make sure `jet_pt` is a vector column. The histogram manager automatically expands scalars to match vector sizes.

## Systematics

### SystematicManager Integration

SystematicManager tracks systematic variations and propagates them through the analysis.

#### Defining Variables with Systematics

```cpp
// Analyzer owns this manager and returns it by reference.
auto& sysMgr = analyzer.getSystematicManager();

// Assume these input columns already exist in the dataframe. Map the nominal
// input to its actual JES up/down columns before defining a derived variable.
sysMgr.registerVariationColumns(
    "jet_pt", "jes", "jet_pt_jes_up", "jet_pt_jes_down");

// Analyzer::Define uses the registered mapping to define nominal and varied
// output columns. Its callable receives input columns, not a variation label.
analyzer.Define("jet_pt_corrected",
    [](const ROOT::VecOps::RVec<float>& pt,
       const ROOT::VecOps::RVec<float>& scale) {
        return pt * scale;
    },
    {"jet_pt", "jet_scale"});
```

#### Registering Systematics

```cpp
// For conventional columns jet_pt_jesUp / jet_pt_jesDown, register the
// family against the varied input variable before calling Analyzer::Define.
auto& sysMgr = analyzer.getSystematicManager();
sysMgr.registerSystematic("jes", {"jet_pt"});

// If the source columns use other names, add an explicit mapping; this call
// also registers "jes" as affecting "jet_pt".
sysMgr.registerVariationColumns(
    "jet_pt", "jes", "jet_pt_jes_up", "jet_pt_jes_down");
```

#### Filters with Systematics

When a systematic affects a filter input, `Analyzer::Filter` evaluates the
predicate for nominal and shifted inputs. For a filter named `jet_pt`, this
produces `pass_jet_pt`, `pass_jet_pt_jesUp`, and `pass_jet_pt_jesDown`. Each
variation has a cumulative boolean mask across all filters. The dataframe keeps
an event when at least one variation passes every filter; events that fail all
variations are removed. NDHistogramManager uses the same masks to skip fills
for variations that fail, so a retained migration event contributes only to
the systematic bins whose full selection it passes. This also applies to
explicitly registered variation labels.

Register systematic variations before the filters that should use them. A
variation introduced after earlier filters cannot restore events those filters
already removed.

#### Histograms with Systematics

When booking histograms with NDHistogramManager, an automatic systematic axis is added:

```cpp
// Histograms are automatically filled for all systematic variations
// Access via: hist[channel][region][systematic]
```

### Systematics on PhysicsObjectCollections

The preferred way to apply systematic variations in CMS analyses is through
`PhysicsObjectCollection` and the object-energy manager plugins.  This approach
lets the manager produce nominal and varied object collections from one
selection. Generic `Analyzer::Define` expansion applies only to variations
registered before the definition is made; the JEC manager's deferred outputs
have the lifecycle limitation described below.

#### Step 1 — Define your object collection on the nominal kinematics

```cpp
analyzer.Define("goodJets",
    [](const RVec<float>& pt, const RVec<float>& eta,
       const RVec<float>& phi, const RVec<float>& mass) {
        return PhysicsObjectCollection(pt, eta, phi, mass,
                                       (pt > 25.f) && (abs(eta) < 2.4f));
    },
    {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
);
```

#### Step 2 — Apply CMS corrections and register systematic sources

`JetEnergyScaleManager` handles the entire JEC/JES/JER pipeline, including
corrected `PhysicsObjectCollection` outputs:

```cpp
// Include <JetEnergyScaleManager.h> and <CorrectionManager.h> in the source.
// Factories register the default roles "jetEnergyScaleManager" and
// "correctionManager".
auto jes = JetEnergyScaleManager::create(analyzer);
auto cm = CorrectionManager::create(analyzer);

jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
jes->setMETColumns("MET_pt", "MET_phi");

// Strip NanoAOD embedded JEC → raw pT
jes->removeExistingCorrections("Jet_rawFactor");

// Evaluate the nominal compound JEC from a correctionlib payload.
// Mixed per-jet RVec inputs and scalar rho are handled automatically.
cm->registerCorrection(
    "jec_nominal",
    "jet_jerc.json.gz",
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
cm->applyCorrectionVec("jec_nominal", {}, {}, "Jet_jec_sf_nominal");
jes->applyCorrection("Jet_pt_raw", "Jet_jec_sf_nominal", "Jet_pt_jec");

// Register all CMS JES uncertainty sources and apply them in one call.
jes->registerSystematicSources("reduced", {"Total"});
jes->applySystematicSet(*cm, "jes_unc", "reduced",
                        "Jet_pt_jec", "Jet_pt_jes");

// Type-1 MET propagation for JEC and JES
jes->propagateMET("MET_pt", "MET_phi",
                  "Jet_pt_raw", "Jet_pt_jec",
                  "MET_pt_jec", "MET_phi_jec");
```

#### Step 3 — Produce corrected collections for nominal + all variations

```cpp
jes->setInputJetCollection("goodJets");
// Define a corrected nominal collection
jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
// Define per-variation collections and assemble a variation map
jes->defineVariationCollections("goodJets_jec", "goodJets",
                                "goodJets_variations");
```

After `analyzer.run()`:

| Column | Description |
|--------|-------------|
| `goodJets_jec` | Nominal JEC-corrected `PhysicsObjectCollection` |
| `goodJets_TotalUp` | JES Total up variation `PhysicsObjectCollection` |
| `goodJets_TotalDown` | JES Total down variation `PhysicsObjectCollection` |
| `goodJets_variations` | `PhysicsObjectVariationMap` keyed by `"nominal"`, `"TotalUp"`, `"TotalDown"` |

#### Step 4 — Use the corrected outputs

`Analyzer::Define` accepts exactly three arguments: the output name, a
callable, and its input columns. It expands definitions only for systematic
variations already registered when the call is made. The JEC manager registers
its corrected-collection variations during its `execute()` hook in
`Analyzer::run()`, after analysis-body `Define` calls have been made. Therefore
this API cannot use a pre-run `Define` to consume the JEC manager's later-created
`goodJets_jec` or `goodJets_TotalUp/Down` columns. The JEC manager produces
those outputs itself; this example stops at the manager outputs rather than
claiming generic downstream `Define` calls can consume them in the same run.

#### Using the full CMS JES source set

```cpp
jes->registerSystematicSources("full", {
    "AbsoluteCal", "AbsoluteScale", "AbsoluteMPFBias",
    "FlavorQCD", "Fragmentation", "PileUpDataMC",
    "PileUpPtRef", "RelativeFSR", "RelativeJEREC1",
    "RelativeJEREC2", "RelativeJERHF",
    "RelativePtBB", "RelativePtEC1", "RelativePtEC2",
    "RelativePtHF", "RelativeBal", "RelativeSample"
});
jes->applySystematicSet(*cm, "jes_unc", "full", "Jet_pt_jec", "Jet_pt_jes");
// Creates 34 variation columns and registers 17 systematic families in one call.
```

#### Manual corrected-kinematics approach (non-JES use cases)

For object types without a dedicated manager (or custom corrections), call
`withCorrectedPt` or `withCorrectedKinematics` directly:

```cpp
// Electron energy scale correction
analyzer.Define("goodElectrons_scaled",
    [](const PhysicsObjectCollection& electrons,
       const RVec<float>& pt_scaled) {
        return electrons.withCorrectedPt(pt_scaled);
    },
    {"goodElectrons", "Electron_pt_scale"}
);

// Full 4-vector replacement (e.g. tau energy scale with mass rescaling)
analyzer.Define("goodTaus_corrected",
    [](const PhysicsObjectCollection& taus,
       const RVec<float>& pt_corr, const RVec<float>& eta_corr,
       const RVec<float>& phi_corr, const RVec<float>& mass_corr) {
        return taus.withCorrectedKinematics(pt_corr, eta_corr,
                                            phi_corr, mass_corr);
    },
    {"goodTaus", "Tau_pt_tes", "Tau_eta_tes",
     "Tau_phi_tes", "Tau_mass_tes"}
);
```

> **Full reference**: [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md) and
> [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)

## Managing Event Weights

WeightManager is a plugin that assembles the nominal event weight and any
systematic weight variations from individually registered components.  It
replaces ad-hoc per-event weight expressions scattered across the analysis
with a single, audited weight definition.

### Base Weight: Generator Weight and Weight Sum Tracking

In Monte Carlo analyses the first thing to include is the **per-event generator
weight** (e.g. `genWeight` in NanoAOD).  This must be registered both in the
config file and in your analysis code:

1. **Config file** — tell `CounterService` which branch holds the generator weight
   so that it can accumulate the sum-of-weights for normalisation:

   ```
   enableCounters = true
   counterWeightBranch = genWeight
   ```

   `enableCounters` defaults to `false`; set it to `true` or CounterService
   will not be registered and these histograms will not be written.

   CounterService then writes two histograms to the meta ROOT file:
   - `counter_weightSum_<sample>` — sum of `genWeight` over all events (used as
     `sumW` when computing the luminosity normalisation)
   - `counter_weightSignSum_<sample>` — sum of `sign(genWeight)`, which is used
     by `StitchingDerivationTask` to compute per-bin stitching scale factors

2. **Analysis code** — add `genWeight` as the first scale factor so that the final
   weight column includes it:

   ```cpp
   wm->addScaleFactor("genWeight", "genWeight");
   ```

For samples with negative-weight events (common in NLO generators) use
`sign(genWeight)` instead of `genWeight` itself to avoid sign-weighted distortions
while still properly normalising the sample. Keep `enableCounters = true` and
`counterWeightBranch = genWeight` in the config so `CounterService` tracks the
true sum, then add the sign column as the scale factor:

```cpp
// Define sign-of-weight column
analyzer.Define("genWeightSign", [](float w){ return w >= 0.f ? 1.f : -1.f; }, {"genWeight"});
wm->addScaleFactor("genWeight", "genWeightSign");
```

### Interaction with Stitching Weights

For inclusive+exclusive sample sets (e.g. DY + DY high-pT), per-event stitching
scale factors are derived by `StitchingDerivationTask` from the
`counter_intWeightSignSum_*` histograms written by CounterService.  To enable
this:

1. In the config file set `counterIntWeightBranch` to the integer stitching-bin
   column (produced by your analysis or CorrectionManager):

   ```
   enableCounters = true
   counterWeightBranch = genWeight
   counterIntWeightBranch = stitchBin
   ```

2. After stitching weights are derived you load the resulting correctionlib JSON
   and apply it as a per-event scale factor:

   ```cpp
   // Apply the derived stitching weight
   wm->addScaleFactor("stitchSF", "stitch_weight_col");
   ```

See [LAW_TASKS.md — StitchingDerivationTask](LAW_TASKS.md#5-stitchingderivationtask) for
details on deriving these weights from LAW.

### Registering WeightManager

Create and register a `WeightManager` plugin with its factory:

```cpp
#include <WeightManager.h>

// Creates and registers the manager under "weightManager".
auto wm = WeightManager::create(analyzer);
```

### Adding Weight Components

Register the generator weight first, then all additional per-event scale
factor columns and scalar normalization factors before calling
`defineNominalWeight()`:

```cpp
// Base generator weight — must come first.
wm->addScaleFactor("genWeight", "genWeight");

// Additional per-event scale factor columns (must already exist on the dataframe).
wm->addScaleFactor("leptonSF",  "lepton_sf_col");
wm->addScaleFactor("pileupSF",  "pu_weight_col");

// Scalar normalization applied uniformly to every event (lumi × xsec / sumW).
// sumW should come from the counterWeightSum histogram in a previous skim.
double normFactor = lumi * xsec / sumW;
wm->addNormalization("lumi_xsec", normFactor);
```

### Registering Weight Variations

For each systematic that changes a scale factor, register the up/down
column pair:

```cpp
wm->addWeightVariation("bTagSF", "bTagSF_up_col", "bTagSF_dn_col");
wm->addWeightVariation("pileupSF", "pu_weight_up", "pu_weight_dn");
```

### Defining Weight Columns

Schedule the weight columns to be defined on the dataframe.  The columns
are created just before the event loop starts (`execute()`):

```cpp
// Nominal weight: product of all SFs and normalizations.
wm->defineNominalWeight("weight_nominal");

// Varied weights: replace the named SF component with its up/down variant.
wm->defineVariedWeight("bTagSF",  "up",   "weight_bTagSF_up");
wm->defineVariedWeight("bTagSF",  "down", "weight_bTagSF_dn");
wm->defineVariedWeight("pileupSF","up",   "weight_pileupSF_up");
wm->defineVariedWeight("pileupSF","down", "weight_pileupSF_dn");
```

Retrieve the defined column names for use when booking histograms:

```cpp
std::string nomCol = wm->getNominalWeightColumn();         // "weight_nominal"
std::string upCol  = wm->getWeightColumn("bTagSF", "up"); // "weight_bTagSF_up"
```

### Audit Information

After `analyzer.run()` completes, WeightManager writes per-component
statistics (sum, mean, min, max, negative-event count) to the meta ROOT
output file and to the analysis logger.  You can also inspect the entries
programmatically:

```cpp
for (const auto& entry : wm->getAuditEntries()) {
    std::cout << entry.name
              << "  sum=" << entry.sumWeights
              << "  mean=" << entry.meanWeight
              << "  negEvents=" << entry.negativeCount << "\n";
}
```

### Complete WeightManager Example

```cpp
#include <analyzer.h>
#include <WeightManager.h>

void myAnalysis() {
    Analyzer analyzer("config.txt");

    // Creates and registers the manager under "weightManager".
    auto wm = WeightManager::create(analyzer);

    // Define SF columns on the dataframe first.
    analyzer.Define("lepton_sf_col", computeLeptonSF, {"lep_pt", "lep_eta"});
    analyzer.Define("bTagSF_col",    computeBTagSF,   {"jet_pt", "jet_eta"});
    analyzer.Define("bTagSF_up_col", computeBTagSFUp, {"jet_pt", "jet_eta"});
    analyzer.Define("bTagSF_dn_col", computeBTagSFDn, {"jet_pt", "jet_eta"});

    // Register components.
    wm->addScaleFactor("leptonSF", "lepton_sf_col");
    wm->addScaleFactor("bTagSF",   "bTagSF_col");
    wm->addNormalization("lumi_xsec", 59.7 * 831.76 / 1e8);

    // Register systematic variations.
    wm->addWeightVariation("bTagSF", "bTagSF_up_col", "bTagSF_dn_col");

    // Schedule column definitions.
    wm->defineNominalWeight("weight_nominal");
    wm->defineVariedWeight("bTagSF", "up",   "weight_bTagSF_up");
    wm->defineVariedWeight("bTagSF", "down", "weight_bTagSF_dn");

    // Use the nominal weight column when booking histograms.
    std::string nomCol = wm->getNominalWeightColumn();

    analyzer.run();
}
```

## Analysis Regions

RegionManager declares named analysis regions with an optional hierarchy,
providing filtered RDataFrame nodes that other plugins (histogram and cutflow
managers) can consume.  The main analysis dataframe is not modified; filtered
branches are built independently inside the plugin.

### Registering RegionManager

```cpp
#include <RegionManager.h>

// Creates and registers the manager under "regionManager".
auto rm = RegionManager::create(analyzer);
```

### Defining Boolean Filter Columns

Define all boolean filter columns on the dataframe **before** calling
`declareRegion()` for the first time, because the base node is captured at
that point:

```cpp
analyzer.Define("isPresel",       passPreselection,     {"lep_pt", "met"});
analyzer.Define("isSignalRegion", passSignalSelection,  {"mva_score"});
analyzer.Define("isControlRegion",passControlSelection, {"mva_score"});
analyzer.Define("isTightSignal",  passTightSignal,      {"mva_score", "bdt_score"});
```

### Declaring Regions

Declare root regions (no parent) first, then child regions:

```cpp
// Root region — applied to the base dataframe.
rm->declareRegion("presel", "isPresel");

// Child regions — filtered on top of their parent.
rm->declareRegion("signal",  "isSignalRegion",  "presel");
rm->declareRegion("control", "isControlRegion", "presel");

// Grandchild region.
rm->declareRegion("tight_signal", "isTightSignal", "signal");
```

### Retrieving Region DataFrames

```cpp
ROOT::RDF::RNode signalDf  = rm->getRegionDataFrame("signal");
ROOT::RDF::RNode controlDf = rm->getRegionDataFrame("control");
```

### Validating the Hierarchy

`validate()` checks for cycles, missing parent references, and duplicate
names and returns a list of error strings (empty means valid):

```cpp
auto errors = rm->validate();
if (!errors.empty()) {
    for (const auto& e : errors) std::cerr << e << "\n";
}
```

At analysis startup `initialize()` runs the same check and throws
`std::runtime_error` on failure.  `finalize()` writes a region-summary
entry to the meta ROOT output file.

### Using Regions with NDHistogramManager

```cpp
ndh->bindToRegionManager(rm.get());
// NDHistogramManager now fills histograms separately for each declared region.
```

### Using Regions with CutflowManager

```cpp
cfm->bindToRegionManager(rm.get());
// CutflowManager books per-region count actions in a single event-loop pass.
```

### Complete RegionManager Example

The typical pattern is to declare regions and then let the bound plugins
(NDHistogramManager, CutflowManager) handle the per-region filling automatically.
You do not need to retrieve raw per-region DataFrames; plugins that are bound
to the RegionManager iterate over all regions internally.

```cpp
#include <analyzer.h>
#include <RegionManager.h>
#include <NDHistogramManager.h>
#include <CutflowManager.h>

int main(int argc, char* argv[]) {
    Analyzer analyzer(argv[1]);

    // Factories register the default roles "regionManager",
    // "histogramManager", and "cutflowManager".
    auto rm = RegionManager::create(analyzer);
    auto ndh = NDHistogramManager::create(analyzer);
    auto cfm = CutflowManager::create(analyzer);

    // Step 1 — define boolean selection columns.
    analyzer.Define("isPresel",        passPreselection,    {"lep_pt", "met"});
    analyzer.Define("isSignalRegion",  passSignalSelection, {"mva_score"});
    analyzer.Define("isControlRegion", passControlSelection,{"mva_score"});

    // Step 2 — declare regions.
    rm->declareRegion("presel",   "isPresel");
    rm->declareRegion("signal",   "isSignalRegion",  "presel");
    rm->declareRegion("control",  "isControlRegion", "presel");

    // Step 3 — bind plugins so they operate per-region automatically.
    ndh->bindToRegionManager(rm.get());  // each histogram gains a region axis
    cfm->bindToRegionManager(rm.get());  // cutflow tables produced per region

    // Step 4 — register cuts (applies to the full dataframe; cfm tracks
    //           per-region counts separately via the bound RegionManager).
    cfm->addCut("presel",  "isPresel");
    cfm->addCut("signal",  "isSignalRegion");

    // Step 5 — book config-driven histograms (fills all declared regions).
    ndh->bookConfigHistograms();

    // Step 6 — execute.
    analyzer.run();
    return 0;
}
```

## Cutflow and Event Selection Monitoring

CutflowManager computes a **sequential cutflow** (events remaining after
each successive cut) and an **N-1 table** (events passing all cuts except
one) in a single event-loop pass.

### Registering CutflowManager

```cpp
#include <CutflowManager.h>

// Creates and registers the manager under "cutflowManager".
auto cfm = CutflowManager::create(analyzer);
```

### Adding Cuts

Define all boolean cut columns on the dataframe **before** the first
`addCut()` call, since the base node (needed for N-1) is captured then.
`addCut()` also applies each filter to the main analysis dataframe:

```cpp
// Define boolean columns first.
analyzer.Define("leptonPtCut",  passLeptonPt,  {"lep_pt"});
analyzer.Define("metCut",       passMET,       {"met"});
analyzer.Define("bTagCut",      passBTag,      {"jet_btag"});

// Register cuts in selection order.
cfm->addCut("lepton_pT", "leptonPtCut");
cfm->addCut("MET",       "metCut");
cfm->addCut("b-tagging", "bTagCut");
```

All `addCut()` calls must be made **before** `analyzer.run()` (i.e. before
`execute()` is triggered by the framework).

### Binding to a RegionManager (Optional)

When a RegionManager is bound, `execute()` books per-region count actions
alongside the global ones, so global and per-region cutflows are computed in
a single pass:

```cpp
cfm->bindToRegionManager(rm.get());
```

### Output Written Automatically

After `analyzer.run()`:

- **`cutflow`** — TH1D with sequential event counts after each cut.
- **`cutflow_nminus1`** — TH1D with N-1 counts.
- **`cutflow_regions`** — TH2D of regions × cuts (only when bound to a
  RegionManager).

All three histograms are written to the meta ROOT output file and a summary
table is printed via the analysis logger.

### Accessing Results Programmatically

```cpp
// After analyzer.run().

// Global cutflow.
for (const auto& [name, count] : cfm->getCutflowCounts()) {
    std::cout << name << ": " << count << " events\n";
}

// N-1 table.
for (const auto& [name, count] : cfm->getNMinusOneCounts()) {
    std::cout << name << " (N-1): " << count << " events\n";
}

// Per-region cutflow (requires bindToRegionManager()).
for (const auto& [name, count] : cfm->getRegionCutflowCounts("signal")) {
    std::cout << "signal / " << name << ": " << count << " events\n";
}
```

### Complete CutflowManager Example

```cpp
#include <analyzer.h>
#include <RegionManager.h>
#include <CutflowManager.h>

void myAnalysis() {
    Analyzer analyzer("config.txt");

    // Factories register the default roles "regionManager" and
    // "cutflowManager".
    auto rm = RegionManager::create(analyzer);
    auto cfm = CutflowManager::create(analyzer);

    // Define all boolean columns before the first addCut().
    analyzer.Define("leptonPtCut", passLeptonPt, {"lep_pt"});
    analyzer.Define("metCut",      passMET,      {"met"});
    analyzer.Define("bTagCut",     passBTag,     {"jet_btag"});

    // Register cuts (also applies filters to the main dataframe).
    cfm->addCut("lepton_pT", "leptonPtCut");
    cfm->addCut("MET",       "metCut");
    cfm->addCut("b-tagging", "bTagCut");

    // Declare regions after all cuts are registered.
    analyzer.Define("isSignalRegion", passSignal, {"bdt_score"});
    rm->declareRegion("signal", "isSignalRegion");

    // Bind for per-region cutflows.
    cfm->bindToRegionManager(rm.get());

    analyzer.run();
    // Results are written to the meta ROOT file automatically.
}
```

## Complete Example

Here's a complete analysis putting it all together:

```cpp
#include <analyzer.h>
#include <NDHistogramManager.h>
#include <OnnxManager.h>
#include <plots.h>
#include <iostream>
#include <cmath>

// ========== Helper Functions ==========

ROOT::VecOps::RVec<int> selectGoodJets(
    const ROOT::VecOps::RVec<float>& pt,
    const ROOT::VecOps::RVec<float>& eta,
    const ROOT::VecOps::RVec<float>& btag) {
    
    ROOT::VecOps::RVec<int> indices;
    for (size_t i = 0; i < pt.size(); i++) {
        if (pt[i] > 25.0 && std::abs(eta[i]) < 2.5 && btag[i] > 0.5) {
            indices.push_back(i);
        }
    }
    return indices;
}

float calculateMT(float pt, float phi, float met, float met_phi) {
    float dphi = phi - met_phi;
    while (dphi > M_PI) dphi -= 2*M_PI;
    while (dphi < -M_PI) dphi += 2*M_PI;
    return std::sqrt(2 * pt * met * (1 - std::cos(dphi)));
}

// ========== Main Analysis ==========

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    // Initialize analyzer
    Analyzer analyzer(argv[1]);

    // Factories register the default roles "onnxManager" and
    // "histogramManager".
    auto onnxMgr = OnnxManager::create(analyzer);
    auto histMgr = NDHistogramManager::create(analyzer);

    // ===== Define Variables =====
    
    // Good jets selection
    analyzer.Define("good_jet_indices", selectGoodJets,
        {"jet_pt", "jet_eta", "jet_btag"});
    
    analyzer.Define("n_good_jets",
        [](const ROOT::VecOps::RVec<int>& indices) { 
            return static_cast<int>(indices.size()); 
        },
        {"good_jet_indices"});
    
    // Leading jet properties
    analyzer.Define("leading_jet_pt",
        [](const ROOT::VecOps::RVec<float>& pt, 
           const ROOT::VecOps::RVec<int>& indices) {
            return indices.size() > 0 ? pt[indices[0]] : -1.0f;
        },
        {"jet_pt", "good_jet_indices"});
    
    // Transverse mass
    analyzer.Define("mt_w", calculateMT,
        {"lepton_pt", "lepton_phi", "met", "met_phi"});

    // ===== Event Selection =====
    
    analyzer.Filter("trigger",
        [](bool trig) { return trig; },
        {"HLT_IsoMu24"});
    
    analyzer.Filter("one_lepton",
        [](int n_lep) { return n_lep == 1; },
        {"n_leptons"});
    
    analyzer.Filter("lepton_pt_cut",
        [](float pt) { return pt > 30.0; },
        {"lepton_pt"});
    
    analyzer.Filter("at_least_4_jets",
        [](int n_jets) { return n_jets >= 4; },
        {"n_good_jets"});

    // ===== Apply ML Models =====
    
    if (!onnxMgr->getAllModelNames().empty()) {
        onnxMgr->applyAllModels();
        
        // Use ML score
        analyzer.Define("pass_ml_cut",
            [](float score) { return score > 0.7; },
            {"ml_score"});
        
        analyzer.Filter("ml_selection",
            [](bool pass) { return pass; },
            {"pass_ml_cut"});
    }

    // ===== Apply Corrections =====
    
    analyzer.Define("event_weight",
        [](float gen_weight, float lep_sf, float btag_sf) {
            return gen_weight * lep_sf * btag_sf;
        },
        {"genWeight", "lepton_sf", "btag_sf"});

    // ===== Define Regions =====
    
    analyzer.Define("region",
        [](float mt, int n_bjets) {
            if (mt > 50 && n_bjets >= 2) return 0;  // Signal region
            if (mt < 50 && n_bjets >= 2) return 1;  // Control region
            return -1;
        },
        {"mt_w", "n_bjets"});

    // ===== Book Histograms =====
    
    std::vector<histInfo> hists = {
        histInfo("h_njets", "n_good_jets", "N_{jets}", "event_weight", 10, 0, 10),
        histInfo("h_leading_jet_pt", "leading_jet_pt", "Leading Jet p_{T}",
                "event_weight", 50, 0, 500),
        histInfo("h_mt", "mt_w", "M_{T}(W)", "event_weight", 40, 0, 200),
    };

    std::vector<selectionInfo> selections = {
        selectionInfo("region", 2, 0, 2),
    };

    std::vector<std::vector<std::string>> regionNames = {
        {"SR", "CR"},
    };

    histMgr->bookND(hists, selections, "", regionNames);

    // ===== Execute and save configured outputs =====
    analyzer.run();

    std::cout << "Analysis complete!" << std::endl;
    return 0;
}
```

## Advanced Topics

### Conditional ML Evaluation

Save computation by only running ML models when needed:

```cpp
// Define condition
analyzer.Define("run_expensive_model",
    [](int n_jets, float met) {
        return n_jets >= 4 && met > 50.0;
    },
    {"n_good_jets", "met"});
```

In config:
```
file=model.onnx name=ml_score inputVariables=features runVar=run_expensive_model
```

### Chaining Analyses

Use output of one analysis as input to another:

```cpp
// First analysis: skim and save
analyzer1.run();   // Writes a skim only if enableSkim is true in the config

// Second analysis: load skim
// In config: fileList=skim_output.root
Analyzer analyzer2("second_analysis_config.txt");
analyzer2.Define(...);  // Further processing
analyzer2.run();
```

### Per-Sample Processing

Handle different samples with different logic:

```cpp
analyzer.DefinePerSample("cross_section",
    [](unsigned int slot, const ROOT::RDF::RSampleInfo& info) {
        std::string sample = info.AsString();
        if (sample.find("ttbar") != std::string::npos) return 831.76;
        if (sample.find("wjets") != std::string::npos) return 61526.7;
        return 1.0;
    });

analyzer.Define("event_weight",
    [](float gen_weight, float xsec, float n_events) {
        return gen_weight * xsec / n_events;
    },
    {"genWeight", "cross_section", "total_events"});
```

### Snapshot Intermediate Results

Save intermediate processing steps:

```cpp
// After preselection
analyzer.Filter("preselection", ...);

// Save snapshot (getDataFrameUnsafe() returns a raw RNode — bypasses systematic
// expansion and provenance tracking. Only use this for advanced
// operations not supported by the standard Analyzer API.)
auto df = analyzer.getDataFrameUnsafe();
df.Snapshot("Events", "preselection_output.root", {"useful_branches"});

// Continue processing
analyzer.Define(...);
```

### Using Helper Functions

Create reusable helper functions:

```cpp
// In a header file
namespace MyAnalysis {
    ROOT::VecOps::RVec<int> selectObjects(...) { /*...*/ }
    float computeVariable(...) { /*...*/ }
    bool passSelection(...) { /*...*/ }
}

// In analysis
analyzer.Define("selected", MyAnalysis::selectObjects, {...});
```

## Friend Trees

Friend trees let you attach extra branches stored in separate ROOT files to
the main event tree without merging the files.  The framework adds friend
TChains to the main chain before the RDataFrame is created, so all friend
branches are immediately accessible by name.

### Configuration via submit_config.txt

Add one `friendTreeConfig` line per friend tree:

```
friendTreeConfig: calib,Events,/data/calib_files/*.root
friendTreeConfig: pileup,Events,/data/pileup/pu_weights.root
```

The format is `alias,treeName,fileGlob`.  The alias is used to access friend
branches (e.g. `calib.pt`).

### Configuration via dataset_manifest.yaml

Friend trees can also be specified per dataset under the `friend_trees` key.
This is useful when different samples require different sidecar files:

```yaml
datasets:
  - name: ttbar_2022
    process: ttbar
    files:
      - /data/ttbar/ttbar_2022.root
    friend_trees:
      - alias: calib
        tree_name: Events
        files:
          - /data/calib/calib_2022.root
          - root://eosserver.cern.ch//eos/calib/calib_remote.root
        index_branches: [run, luminosityBlock]

      - alias: bTagWeights
        tree_name: Events
        directory: /data/btag_weights/2022/
        globs: [".root"]
        antiglobs: ["FAIL"]
```

### FriendTreeConfig Fields

| Field | Type | Description |
|---|---|---|
| `alias` | string | Name used to access friend branches (e.g. `alias.branch`). |
| `tree_name` | string | TTree name inside the friend file(s). Default: `"Events"`. |
| `files` | list | Explicit ROOT file paths or XRootD URLs. Takes precedence over `directory`. |
| `directory` | string | Directory scanned for ROOT files (alternative to `files`). |
| `globs` | list | Filename substrings to include when scanning `directory`. Default: `[".root"]`. |
| `antiglobs` | list | Filename substrings to exclude when scanning `directory`. Default: `[]`. |
| `index_branches` | list | Branch names for index-based event matching (see below). |

### Index-Based Event Matching

By default ROOT matches friend tree entries by sequential position.  If file
ordering differs between the main tree and the friend tree, set
`index_branches` to enable `TChain::BuildIndex`-based matching:

```yaml
index_branches: [run, luminosityBlock]   # match on run + lumi block
# or
index_branches: [run, event]             # match on run + event number
```

The first two entries are used as the major and minor index keys respectively.
Leave `index_branches` empty for position-based (sequential) matching.

## Best Practices

1. **Configuration Management**
   - Version control all config files with Git
   - Use descriptive names for configuration files
   - Document non-obvious configuration choices

2. **Code Organization**
   - Put helper functions in separate headers
   - Use namespaces to avoid name collisions
   - Add comments explaining physics reasoning

3. **Performance**
   - Apply filters early to reduce data volume
   - Use conditional ML evaluation
   - Leverage ROOT's multithreading (`threads=-1`)

4. **Debugging**
   - Start with small input files during development
   - Use `batch=false` to see progress bars
   - Print intermediate results with `Display()` or `Report()`

5. **Reproducibility**
   - Pin ML model versions
   - Document correction file sources
   - Track configuration changes in Git

6. **Testing**
   - Test with different input samples
   - Verify histogram contents make physical sense
   - Check cutflow reports

## Troubleshooting

### Model Not Applied

**Problem**: ONNX/BDT model configured but output column doesn't exist.

**Solution**: Explicitly call `applyModel()` or `applyAllModels()` after defining inputs.

### Missing Input Features

**Problem**: Model application fails with "column not found".

**Solution**: Ensure all `inputVariables` are defined before applying model.

### Histogram Empty

**Problem**: Histograms are booked but contain no entries.

**Solution**: Check that:
- Filters aren't rejecting all events
- Weight column exists and isn't zero
- Region/channel variables are defined correctly
- You called `GetPtr()` on histograms to trigger computation

### Performance Issues

**Problem**: Analysis runs slowly.

**Solution**:
- Enable multithreading (`threads=-1`)
- Apply filters early
- Use `batch=true` for non-interactive jobs
- Profile with ROOT's profiling tools

## Next Steps

- **Understand internals**: See [ARCHITECTURE.md](ARCHITECTURE.md)
- **Add custom features**: See [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md)
- **Deploy to batch**: Use HTCondor submission scripts in `core/python/`
- **Configuration reference**: See [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)

Happy analyzing!
