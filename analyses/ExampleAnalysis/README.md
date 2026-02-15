# ATLAS Dimuon Analysis Example

This example demonstrates a complete Z→μμ analysis using the RDFAnalyzerCore framework with ATLAS Open Data. It showcases the most recent framework functionality.

## Overview

This analysis selects events with exactly two good muons and reconstructs the Z boson mass from the dimuon system. It demonstrates several key features of the RDFAnalyzerCore framework:

## Features Demonstrated

### 1. **Config-Driven Histogram Booking (NDHistogramManager)**
- Histograms are defined in `histograms.txt` configuration file
- No manual histogram booking code required
- Automatically supports systematic variations
- Histograms defined:
  - Z boson mass (70-110 GeV)
  - Leading muon pT (20-150 GeV)
  - Subleading muon pT (20-150 GeV)

### 2. **Systematic Variation Infrastructure**
- Demonstrates the systematic variation registration API
- Shows how to declare systematics that affect specific variables
- Framework infrastructure for propagating systematics through the analysis
- Example systematics registered (infrastructure only):
  - `muonScale_up`: Would represent +1% momentum scale variation
  - `muonScale_down`: Would represent -1% momentum scale variation
- To fully implement: modify variable definitions to apply actual variations
- See main README for complete systematic variation examples

### 3. **Modern Analyzer API**
- Single `Analyzer` object manages the entire workflow
- Plugin system for modular functionality (NDHistogramManager)
- Clean separation of configuration and code
- Method chaining for readable analysis flow

### 4. **Physics Analysis Flow**
1. Select muons passing ID cuts and preselection
2. Require isolation from jets (ΔR > 0.4)
3. Apply pT threshold (≥ 20 GeV)
4. Select events with exactly two good muons
5. Reconstruct Z boson from leading and subleading muons
6. Apply Z mass window selection (≤ 150 GeV)
7. Fill histograms for output variables

## Configuration Files

### `cfg.txt` - Main Configuration
- Input data files from ATLAS Open Data (PHYSLITE format)
- Output file path
- Thread count (-1 = auto-detect)
- References to additional config files:
  - `histogramConfig`: Histogram definitions
  - `floatConfig`: Float parameters (normalization scale)
  - `intConfig`: Integer parameters (sample type)
  - `saveConfig`: Output branch selection

### `histograms.txt` - Histogram Definitions
Defines histograms using simple key=value format:
```
name=ZBosonMass variable=ZBosonMass weight=normScale bins=60 lowerBound=70.0 upperBound=110.0
```

Each histogram specification includes:
- `name`: Histogram identifier
- `variable`: Branch/variable to histogram
- `weight`: Event weight variable
- `bins`, `lowerBound`, `upperBound`: Binning specification
- `label`: (optional) Axis label

### `floats.txt` - Float Parameters
- `normScale`: Overall normalization factor for MC
- `sampleNorm`: Sample cross-section × luminosity

### `ints.txt` - Integer Parameters
- `type`: Sample type identifier

### `output.txt` - Output Branches
Lists branches to save in output ROOT file:
- `ZBosonMass`: Reconstructed Z boson mass in GeV

## Building and Running

### Build
```bash
# From RDFAnalyzerCore root directory
source build.sh

# Or with CMake directly
cmake -S . -B build
cmake --build build -j$(nproc)
```

### Run
```bash
cd build/analyses/ExampleAnalysis
./analysis ../../analyses/ExampleAnalysis/cfg.txt
```

**Note**: This analysis uses ATLAS Open Data files hosted on EOS via XRootD. Access requires either:
- Running on lxplus at CERN
- Having XRootD access to eospublic.cern.ch
- Modifying `cfg.txt` to use local ROOT files

## Output

### Skimmed ROOT File
Location specified by `saveFile` in `cfg.txt`
- Contains selected events with branches listed in `output.txt`
- Includes `ZBosonMass` variable in GeV

### Histograms
Saved in the same ROOT file in `histograms/` directory:
- `ZBosonMass`: Z boson invariant mass
- `LeadingMuonPt`: Leading muon transverse momentum
- `SubleadingMuonPt`: Subleading muon transverse momentum

For each histogram, if systematic variations are implemented, they would be stored as separate histograms:
- `<name>_nominal`: Central value
- `<name>_muonScale_up`: +1% muon momentum scale (when implemented)
- `<name>_muonScale_down`: -1% muon momentum scale (when implemented)

## Analysis Code Structure

### Helper Functions

#### `getGoodMuons()`
Selects muons passing:
- ID cuts (`DFCommonMuonPassIDCuts`)
- Preselection (`DFCommonMuonPassPreselection`)
- Jet separation (ΔR > 0.4)
- pT threshold (≥ 20 GeV)

Returns indices sorted by pT (highest first).

#### `getMuon<N>()`
Template function extracting the Nth muon's 4-momentum.

#### `twoMuons()`
Filter requiring exactly 2 good muons.

#### `massFilter()`
Selects Z candidates with mass ≤ 150 GeV.

#### `getLeadingMuonPt()` / `getSubleadingMuonPt()`
Extract individual muon pT values in GeV for histogramming.

### Analysis Flow in `main()`

1. **Setup**: Create `Analyzer` from config file
2. **Add Plugins**: Register `NDHistogramManager` for histogram booking
3. **Register Systematics**: Define muon momentum scale variations
4. **Define Variables**: Chain Define/Filter calls to build analysis pipeline
5. **Book Histograms**: Call `bookConfigHistograms()` to load histogram config
6. **Execute**: Call `save()` to run the dataframe and write output

## Key Framework Features Used

- **Analyzer Class**: High-level API for building analyses
- **NDHistogramManager Plugin**: Config-driven histogram booking
- **Systematic Propagation**: Automatic handling of uncertainty variations
- **Method Chaining**: Fluent API for readable code
- **Lazy Evaluation**: RDataFrame optimizes execution automatically
- **Config-Driven Design**: Separate physics logic from configuration

## Extending This Example

### Adding More Histograms
Edit `histograms.txt` to add new histogram definitions:
```
name=muon_eta variable=LeadingMuonEta weight=normScale bins=50 lowerBound=-2.5 upperBound=2.5
```

### Adding More Systematics
Register additional systematic variations in the code:
```cpp
an.getSystematicManager().registerSystematic("muonResolution_up", {"AnalysisMuonsAuxDyn.pt"});
```

### Adding ML Models
Add ONNX or BDT plugins for machine learning inference:
```cpp
auto onnxMgr = std::make_unique<OnnxManager>(an.getConfigurationProvider());
an.addPlugin("onnx", std::move(onnxMgr));
```

### Adding Corrections
Add CorrectionManager plugin for scale factors:
```cpp
auto corrMgr = std::make_unique<CorrectionManager>(an.getConfigurationProvider());
an.addPlugin("correction", std::move(corrMgr));
```

### Statistical Analysis Workflow

After running the analysis and producing histograms, you can perform statistical analysis using CMS Combine:

1. **Generate Datacards**: Use the Python datacard generator script:
```bash
python core/python/create_datacards.py example_datacard_config.yaml
```

2. **Build with Combine Support** (optional):
```bash
cmake -S . -B build -DBUILD_COMBINE=ON
cmake --build build -j$(nproc)
```

3. **Run Statistical Analysis**:
```bash
# Set limits
combine -M AsymptoticLimits datacard.txt

# Fit signal strength
combine -M FitDiagnostics datacard.txt
```

See the complete workflow in:
- [Complete Analysis Workflow Script](../../examples/complete_analysis_workflow.sh)
- [Datacard Generator Guide](../../docs/DATACARD_GENERATOR.md)
- [Combine Integration Guide](../../docs/COMBINE_INTEGRATION.md)

## References

- [RDFAnalyzerCore Documentation](https://brkronheim.github.io/RDFAnalyzerCore/)
- [Config-Driven Histograms Guide](../../docs/CONFIG_HISTOGRAMS.md)
- [Analysis Guide](../../docs/ANALYSIS_GUIDE.md)
- [ATLAS Open Data](http://opendata.cern.ch/docs/about-atlas)
