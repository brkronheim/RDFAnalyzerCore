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
- [Complete Example](#complete-example)
- [Advanced Topics](#advanced-topics)

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

# Link against the core framework
target_link_libraries(myanalysis
    PRIVATE
    RDFCore
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
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

# ============== Performance ==============
threads=-1
batch=false

# ============== Output Branches ==============
saveConfig=cfg/output.txt

# ============== Plugins ==============
# Add as needed for your analysis
# bdtConfig=cfg/bdts.txt
# onnxConfig=cfg/onnx_models.txt
# correctionConfig=cfg/corrections.txt
# triggerConfig=cfg/triggers.txt
```

### Step 4: Create Output Branch Configuration

Create `cfg/output.txt` listing branches to keep:

```
selected_muon_pt
selected_muon_eta
selected_jet_pt
selected_jet_mass
event_weight
```

### Step 5: Write Analysis Code

Create `analysis.cc`:

```cpp
#include <analyzer.h>
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>

#include <iostream>

int main(int argc, char **argv) {
    // Validate arguments
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    // Create analyzer with configuration file
    Analyzer analyzer(argv[1]);

    // Define your analysis variables and cuts here
    // (see next sections for examples)

    // Save results
    analyzer.save();

    return 0;
}
```

### Step 6: Build and Run

```bash
cd /path/to/RDFAnalyzerCore
source build.sh
cd build/analyses/MyAnalysis
./myanalysis cfg/main_config.txt
```

## Config-Based Analysis Pattern

The config-based approach follows this pattern:

### 1. Configuration Setup

All behavioral configuration is in text files:

```cpp
// Configuration is loaded by the Analyzer constructor
Analyzer analyzer("cfg/main_config.txt");
```

The Analyzer automatically:
- Loads the main configuration
- Initializes plugin managers based on config
- Sets up the data pipeline
- Configures output sinks

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

Plugins are configured via config files but accessed in C++:

```cpp
// Get plugin from analyzer
auto* onnxManager = analyzer.getPlugin<IOnnxManager>("onnx");

// Use plugin
onnxManager->applyAllModels();
```

### 4. Execution

```cpp
// Trigger execution and save outputs
analyzer.save();
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
// Get ONNX manager
auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");

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
auto* bdtMgr = analyzer.getPlugin<IBDTManager>("bdt");
bdtMgr->applyAllModels();
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
auto* sofieMgr = analyzer.getPlugin<ISofieManager>("sofie");

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

## Histogramming

### Using NDHistogramManager

NDHistogramManager provides sophisticated N-dimensional histogramming with support for systematics and regions.

#### Basic Histogram

```cpp
#include <plots.h>  // For histInfo, selectionInfo

// Get histogram manager
auto* histMgr = analyzer.getPlugin<INDHistogramManager>("histogram");

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
// Trigger execution by accessing histogram pointers
auto& histos = histMgr->GetHistos();
for (auto& h : histos) {
    h.GetPtr();  // Force computation
}

// Save histograms to file
std::vector<std::vector<histInfo>> fullHistList = {hists};
histMgr->saveHists(fullHistList, regionNames);
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
// Get systematic manager
auto* sysMgr = analyzer.getSystematicManager();

// Define variable with systematic variations
dataManager->Define("jet_pt_corrected",
    [](const ROOT::VecOps::RVec<float>& pt,
       const ROOT::VecOps::RVec<float>& jes_up,
       const ROOT::VecOps::RVec<float>& jes_down,
       const std::string& systematic) {
        if (systematic == "jes_up") return pt * jes_up;
        if (systematic == "jes_down") return pt * jes_down;
        return pt;  // Nominal
    },
    {"jet_pt", "jet_jes_up", "jet_jes_down"},
    *sysMgr  // Pass systematic manager
);
```

#### Registering Systematics

```cpp
// Register systematic variations
sysMgr->registerSystematic("jes_up");
sysMgr->registerSystematic("jes_down");
sysMgr->registerSystematic("jer_up");
sysMgr->registerSystematic("jer_down");
```

#### Histograms with Systematics

When booking histograms with NDHistogramManager, an automatic systematic axis is added:

```cpp
// Histograms are automatically filled for all systematic variations
// Access via: hist[channel][region][systematic]
```

## Complete Example

Here's a complete analysis putting it all together:

```cpp
#include <analyzer.h>
#include <ManagerFactory.h>
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
    
    auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");
    if (onnxMgr) {
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
    
    auto* histMgr = analyzer.getPlugin<INDHistogramManager>("histogram");
    if (histMgr) {
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
        
        // Trigger computation
        auto& histos = histMgr->GetHistos();
        for (auto& h : histos) {
            h.GetPtr();
        }
        
        // Save
        std::vector<std::vector<histInfo>> fullHistList = {hists};
        histMgr->saveHists(fullHistList, regionNames);
    }

    // ===== Save Output =====
    
    analyzer.save();

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
analyzer1.save();  // Creates skim_output.root

// Second analysis: load skim
// In config: fileList=skim_output.root
Analyzer analyzer2("second_analysis_config.txt");
analyzer2.Define(...);  // Further processing
analyzer2.save();
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

// Save snapshot
auto df = analyzer.getDataFrame();
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
