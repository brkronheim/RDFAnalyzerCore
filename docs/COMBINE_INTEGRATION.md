# CMS Combine Integration

This guide explains how to use RDFAnalyzerCore with CMS Combine for statistical analysis, limit setting, and fits.

## Overview

RDFAnalyzerCore provides a complete workflow from analysis to statistical inference:

1. **Analysis**: Run RDFAnalyzerCore to process data and create histograms
2. **Datacards**: Use `create_datacards.py` to generate CMS Combine datacards
3. **Statistics**: Use Combine to perform fits, set limits, and extract results
4. **Advanced Tools**: Use CombineHarvester for sophisticated statistical models

## Building with Combine Support

### Prerequisites

- ROOT 6.30+ with Python bindings
- CMake 3.19+
- Python 3.6+ with PyYAML
- Git

### Build Options

The framework provides three build options for optional components:

```bash
# Core framework only (default)
cmake -S . -B build

# With tests disabled
cmake -S . -B build -DBUILD_TESTS=OFF

# With CMS Combine
cmake -S . -B build -DBUILD_COMBINE=ON

# With CMS Combine and CombineHarvester
cmake -S . -B build \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON

# Full build with all features
cmake -S . -B build \
    -DBUILD_TESTS=ON \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON
```

### Building

```bash
# Clone the repository
git clone https://github.com/brkronheim/RDFAnalyzerCore.git
cd RDFAnalyzerCore

# Setup ROOT environment (on lxplus)
source env.sh

# Configure with Combine support
cmake -S . -B build \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON

# Build (this will take several minutes)
cmake --build build -j$(nproc)
```

After building, Combine and CombineHarvester will be available at:
- **Combine**: `build/external/HiggsAnalysis/CombinedLimit/`
- **CombineHarvester**: `build/external/CombineHarvester/`

### Installation Locations

```bash
# Combine executable
build/external/HiggsAnalysis/CombinedLimit/bin/combine

# CombineHarvester tools
build/external/CombineHarvester/CombineTools/bin/
```

## Complete Analysis Workflow

This section demonstrates a complete analysis from data processing to limit extraction.

### Step 1: Run RDFAnalyzer Analysis

First, run your analysis to produce ROOT files with histograms:

```cpp
// myanalysis.cpp
#include <analyzer.h>

int main(int argc, char **argv) {
    Analyzer analyzer(argv[1]);
    
    // Setup histogram manager
    auto histMgr = std::make_unique<NDHistogramManager>(
        analyzer.getConfigurationProvider());
    analyzer.addPlugin("histogramManager", std::move(histMgr));
    
    // Define variables
    analyzer.Define("mT", computeTransverseMass, {"met", "lepton_pt", "lepton_phi"});
    analyzer.Define("nJets", countJets, {"jet_pt", "jet_eta"});
    
    // Apply selection
    analyzer.Filter("signal_selection", 
        [](float mT, int nJets) { return mT > 100 && nJets >= 4; },
        {"mT", "nJets"});
    
    // Book histograms from config
    analyzer.bookConfigHistograms();
    
    // Save outputs
    analyzer.save();
    
    return 0;
}
```

**Configuration** (`config.txt`):
```
fileList=data.root
saveFile=output_data.root
histogramConfig=cfg/histograms.txt
```

**Histogram Configuration** (`cfg/histograms.txt`):
```
name=mT variable=mT weight=event_weight bins=20 lowerBound=0.0 upperBound=500.0
name=nJets variable=nJets weight=event_weight bins=10 lowerBound=0 upperBound=10
```

**Run the analysis**:
```bash
# Process data
./build/analyses/MyAnalysis/myanalysis config_data.txt

# Process signal
./build/analyses/MyAnalysis/myanalysis config_signal.txt

# Process backgrounds
./build/analyses/MyAnalysis/myanalysis config_ttbar.txt
./build/analyses/MyAnalysis/myanalysis config_wjets.txt
```

This produces ROOT files:
- `output_data.root`
- `output_signal.root`
- `output_ttbar.root`
- `output_wjets.root`

### Step 2: Create Datacards

Create a YAML configuration for the datacard generator:

```yaml
# datacard_config.yaml
output_dir: "datacards"

input_files:
  data_obs:
    path: "output_data.root"
    type: "data"
  
  signal:
    path: "output_signal.root"
    type: "signal"
  
  ttbar:
    path: "output_ttbar.root"
    type: "background"
  
  wjets:
    path: "output_wjets.root"
    type: "background"

processes:
  signal:
    samples: [signal]
    description: "Signal process"
  
  ttbar:
    samples: [ttbar]
    description: "ttbar background"
  
  wjets:
    samples: [wjets]
    description: "W+jets background"

control_regions:
  signal_region:
    observable: "mT"
    processes: [signal, ttbar, wjets]
    signal_processes: [signal]
    data_process: "data_obs"
    rebin: 2
    description: "Signal region"

systematics:
  lumi:
    type: "rate"
    distribution: "lnN"
    value: 1.025
    applies_to:
      signal: true
      ttbar: true
      wjets: true
    description: "Luminosity uncertainty"
  
  JES:
    type: "shape"
    distribution: "shape"
    variation: 0.03
    applies_to:
      signal: true
      ttbar: true
      wjets: true
    correlated: true
    description: "Jet energy scale"
```

**Generate datacards**:
```bash
python core/python/create_datacards.py datacard_config.yaml
```

This creates:
- `datacards/datacard_signal_region.txt`
- `datacards/shapes_signal_region.root`

### Step 3: Run Combine

Now use CMS Combine to perform statistical analysis:

#### Asymptotic Limits

Calculate expected and observed limits using the asymptotic CLs method:

```bash
cd datacards

# Set path to combine executable
COMBINE=../build/external/HiggsAnalysis/CombinedLimit/bin/combine

# Run asymptotic limit calculation
$COMBINE -M AsymptoticLimits datacard_signal_region.txt \
    -n .SignalRegion \
    --run blind  # Remove --run blind to unblind

# Output: higgsCombine.SignalRegion.AsymptoticLimits.mH120.root
```

**Results**:
```
Observed Limit: r < 1.23
Expected 2.5%: r < 0.42
Expected 16.0%: r < 0.58
Expected 50.0%: r < 0.85
Expected 84.0%: r < 1.25
Expected 97.5%: r < 1.78
```

#### Toy-Based Limits

For more accurate limits with toys:

```bash
# Generate toys and calculate CLs limits
$COMBINE -M HybridNew datacard_signal_region.txt \
    --LHCmode LHC-limits \
    -T 5000 -s -1 \
    -n .SignalRegion_Toys

# Extract limit from toys
$COMBINE -M HybridNew datacard_signal_region.txt \
    --LHCmode LHC-limits \
    --readHybridResults \
    --grid=higgsCombine.SignalRegion_Toys.HybridNew.mH120.*.root
```

#### Maximum Likelihood Fit

Perform a maximum likelihood fit to extract best-fit signal strength:

```bash
# Fit signal strength
$COMBINE -M FitDiagnostics datacard_signal_region.txt \
    -n .SignalRegion \
    --saveShapes \
    --saveWithUncertainties

# Output: fitDiagnostics.SignalRegion.root
```

**Extract results**:
```python
import ROOT
f = ROOT.TFile.Open("fitDiagnostics.SignalRegion.root")
tree = f.Get("tree_fit_sb")
tree.GetEntry(0)
print(f"Best-fit signal strength: {tree.r:.3f} +/- {tree.rErr:.3f}")
```

#### Impacts

Calculate impact of each systematic uncertainty:

```bash
# Initial fit
$COMBINE -M MultiDimFit datacard_signal_region.txt \
    -n .nominal \
    --algo singles \
    --redefineSignalPOIs r

# Fit with each nuisance parameter
$COMBINE -M MultiDimFit datacard_signal_region.txt \
    -n .impacts \
    --algo impact \
    --redefineSignalPOIs r \
    -P lumi --floatOtherPOIs=1

# Repeat for each systematic...
```

#### Likelihood Scans

Scan the likelihood as a function of the signal strength:

```bash
# 1D likelihood scan
$COMBINE -M MultiDimFit datacard_signal_region.txt \
    -n .scan \
    --algo grid \
    --points 100 \
    --setParameterRanges r=0,3 \
    --redefineSignalPOIs r

# Output: higgsCombine.scan.MultiDimFit.mH120.root
```

**Plot the scan**:
```python
import ROOT
f = ROOT.TFile.Open("higgsCombine.scan.MultiDimFit.mH120.root")
tree = f.Get("limit")
canvas = ROOT.TCanvas("c", "c", 800, 600)
tree.Draw("2*deltaNLL:r", "deltaNLL < 10")
canvas.SaveAs("likelihood_scan.png")
```

### Step 4: Advanced Analysis with CombineHarvester

CombineHarvester provides advanced tools for complex statistical models:

#### Creating Datacards Programmatically

```cpp
// combine_harvester_example.cpp
#include "CombineHarvester/CombineTools/interface/CombineHarvester.h"

int main() {
    ch::CombineHarvester cb;
    
    // Add observations
    cb.AddObservations({"*"}, {"analysis"}, {"13TeV"}, {"signal_region"}, {});
    
    // Add signal
    cb.AddProcesses({"*"}, {"analysis"}, {"13TeV"}, {"signal_region"}, 
                    {"signal"}, {}, true);
    
    // Add backgrounds
    cb.AddProcesses({"*"}, {"analysis"}, {"13TeV"}, {"signal_region"}, 
                    {"ttbar", "wjets"}, {}, false);
    
    // Add systematics
    cb.cp().process({"signal", "ttbar", "wjets"})
      .AddSyst(cb, "lumi", "lnN", ch::syst::SystMap<>::init(1.025));
    
    // Extract shapes from ROOT file
    cb.cp().backgrounds().ExtractShapes(
        "shapes_signal_region.root",
        "$BIN/$PROCESS",
        "$BIN/$PROCESS_$SYSTEMATIC");
    
    // Write datacard
    cb.WriteDatacard("datacard_from_harvester.txt", 
                     "shapes_from_harvester.root");
    
    return 0;
}
```

#### Morphing and Interpolation

CombineHarvester can interpolate between signal mass points:

```bash
# Use CH tools for morphing
CH_TOOLS=../build/external/CombineHarvester/CombineTools/bin

$CH_TOOLS/MorphingTH1 \
    --input_folder datacards/ \
    --output morphed_datacards/ \
    --masses 400,500,600,700 \
    --process signal
```

## Complete Example Script

Here's a shell script that runs the complete workflow:

```bash
#!/bin/bash
# run_complete_analysis.sh

set -e  # Exit on error

echo "=== RDFAnalyzerCore Complete Analysis Workflow ==="

# Step 1: Run analysis
echo "Step 1: Running analysis..."
./build/analyses/MyAnalysis/myanalysis config_data.txt
./build/analyses/MyAnalysis/myanalysis config_signal.txt
./build/analyses/MyAnalysis/myanalysis config_ttbar.txt
./build/analyses/MyAnalysis/myanalysis config_wjets.txt

# Step 2: Create datacards
echo "Step 2: Creating datacards..."
python core/python/create_datacards.py datacard_config.yaml

# Step 3: Run Combine
echo "Step 3: Running statistical analysis..."
cd datacards
COMBINE=../build/external/HiggsAnalysis/CombinedLimit/bin/combine

# Asymptotic limits
echo "  -> Calculating asymptotic limits..."
$COMBINE -M AsymptoticLimits datacard_signal_region.txt -n .Result

# Maximum likelihood fit
echo "  -> Performing ML fit..."
$COMBINE -M FitDiagnostics datacard_signal_region.txt -n .Result

# Likelihood scan
echo "  -> Running likelihood scan..."
$COMBINE -M MultiDimFit datacard_signal_region.txt \
    -n .scan \
    --algo grid \
    --points 50 \
    --setParameterRanges r=0,2

cd ..

echo "=== Analysis complete! ==="
echo "Results in datacards/ directory:"
echo "  - Limit: higgsCombine.Result.AsymptoticLimits.mH120.root"
echo "  - Fit: fitDiagnostics.Result.root"
echo "  - Scan: higgsCombine.scan.MultiDimFit.mH120.root"
```

## Interpreting Results

### Limit Results

The limit on the signal strength `r` (ratio to nominal cross section):
- **r < 1**: Excludes the signal hypothesis at current cross section
- **r > 1**: Signal not excluded; could be present at higher rate
- **Observed = Expected**: Good agreement with background-only hypothesis

### Fit Results

From `FitDiagnostics`:
- **r**: Best-fit signal strength
- **Pulls**: How much each nuisance parameter shifted
- **Impacts**: Effect of each systematic on r

### Significance

Calculate the significance of an excess:

```bash
$COMBINE -M Significance datacard_signal_region.txt
```

## Tips and Best Practices

### 1. Validate Datacards

Always validate datacards before running fits:

```bash
# Check datacard syntax
$COMBINE -M ValidateDatacards datacard_signal_region.txt

# Print datacard contents
$COMBINE -M PrintDatacard datacard_signal_region.txt
```

### 2. Use Workspaces

For complex models, create a workspace:

```bash
# Create workspace
text2workspace.py datacard_signal_region.txt -o workspace.root

# Use workspace in fits
$COMBINE -M AsymptoticLimits workspace.root
```

### 3. Debugging

Check for issues:

```bash
# Verbose output
$COMBINE -M FitDiagnostics datacard_signal_region.txt -v 3

# Save full fit information
$COMBINE -M FitDiagnostics datacard_signal_region.txt \
    --saveShapes \
    --saveWithUncertainties \
    --saveNormalizations
```

### 4. Parallel Processing

Speed up toy generation:

```bash
# Run toys in parallel
for i in {1..10}; do
    $COMBINE -M HybridNew datacard_signal_region.txt \
        -T 500 -s $i -n .job$i &
done
wait

# Merge results
hadd -f toys_merged.root higgsCombine.job*.root
```

## Troubleshooting

### Combine Not Found

If combine executable is not found:

```bash
# Check build
ls -la build/external/HiggsAnalysis/CombinedLimit/bin/combine

# Rebuild if necessary
cmake --build build --target combine_build
```

### Missing Systematics

If systematic variations are missing:

1. Check that your analysis outputs include systematic variations
2. Use placeholder variations (datacard generator creates them automatically)
3. Verify systematic names match between analysis and datacard config

### Fit Failures

If fits fail:

1. Check for negative bins in histograms
2. Verify sufficient statistics in all bins
3. Check for extreme systematic variations
4. Use `--cminDefaultMinimizerStrategy 0` for faster (less robust) fits

## References

- **CMS Combine**: https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/
- **CombineHarvester**: https://cms-analysis.github.io/CombineHarvester/
- **Datacard Generator**: See `docs/DATACARD_GENERATOR.md`
- **Statistics Tutorial**: https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideHiggsAnalysisCombinedLimit

## Support

For issues:
- RDFAnalyzerCore: Open issue on GitHub
- Combine: See Combine documentation and GitHub issues
- CombineHarvester: See CombineHarvester documentation

---

**Complete Analysis Capability**: With this integration, RDFAnalyzerCore now provides a complete end-to-end analysis framework from data processing to statistical inference and limit extraction.
