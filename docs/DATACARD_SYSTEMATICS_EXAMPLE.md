# Example: Creating Histograms with Systematic Variations for Datacards

This example demonstrates how to book histograms with systematic variations in RDFAnalyzerCore
for use with the datacard generator.

## Overview

When creating datacards, shape systematics require Up and Down histogram variations. The datacard 
generator looks for histograms with naming convention: `{observable}_{systematic}Up` and 
`{observable}_{systematic}Down`.

## Example Analysis Code

```cpp
#include "analyzer.h"
#include <NDHistogramManager.h>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config.txt>" << std::endl;
        return 1;
    }

    // Create analyzer
    auto analyzer = Analyzer(argv[1]);
    
    // Add histogram manager
    auto histManager = std::make_unique<NDHistogramManager>(
        analyzer.getConfigurationProvider());
    analyzer.addPlugin("histogramManager", std::move(histManager));
    
    // Define observable (nominal)
    analyzer.Define("jet_pt", computeJetPt, {"jet_px", "jet_py"});
    analyzer.Define("mT", computeTransverseMass, {"lepton_pt", "met"});
    analyzer.Define("event_weight", computeWeight, {"mc_weight", "pu_weight"});
    
    // Define systematic variations
    // JES Up variation
    analyzer.Define("jet_pt_JESUp", 
        [](const ROOT::VecOps::RVec<float>& pt) {
            return pt * 1.03;  // 3% JES uncertainty
        }, 
        {"jet_pt"});
    
    analyzer.Define("mT_JESUp", computeTransverseMass, 
        {"lepton_pt", "met_JESUp"});
    
    // JES Down variation
    analyzer.Define("jet_pt_JESDown", 
        [](const ROOT::VecOps::RVec<float>& pt) {
            return pt * 0.97;  // 3% JES uncertainty
        }, 
        {"jet_pt"});
    
    analyzer.Define("mT_JESDown", computeTransverseMass, 
        {"lepton_pt", "met_JESDown"});
    
    // b-tagging Up variation
    analyzer.Define("event_weight_btagUp", computeWeightBtagUp, 
        {"mc_weight", "pu_weight", "btag_sf_up"});
    
    // b-tagging Down variation
    analyzer.Define("event_weight_btagDown", computeWeightBtagDown, 
        {"mc_weight", "pu_weight", "btag_sf_down"});
    
    // Apply selection
    analyzer.Filter("signal_selection", passSelection, 
        {"lepton_pt", "jet_pt", "met"});
    
    // Book nominal histogram
    histManager->BookSingleHistogram(
        analyzer.getCurrentNode(),
        "mT",              // histogram name
        "mT",              // variable
        "event_weight",    // weight
        50, 0.0, 500.0     // bins, min, max
    );
    
    // Book JES systematic histograms
    histManager->BookSingleHistogram(
        analyzer.getCurrentNode(),
        "mT_JESUp",        // histogram name must match systematic
        "mT_JESUp",        // variable with JES up
        "event_weight",    // nominal weight
        50, 0.0, 500.0
    );
    
    histManager->BookSingleHistogram(
        analyzer.getCurrentNode(),
        "mT_JESDown",
        "mT_JESDown",
        "event_weight",
        50, 0.0, 500.0
    );
    
    // Book b-tagging systematic histograms (weight variation)
    histManager->BookSingleHistogram(
        analyzer.getCurrentNode(),
        "mT_btagUp",
        "mT",              // nominal observable
        "event_weight_btagUp",  // varied weight
        50, 0.0, 500.0
    );
    
    histManager->BookSingleHistogram(
        analyzer.getCurrentNode(),
        "mT_btagDown",
        "mT",
        "event_weight_btagDown",
        50, 0.0, 500.0
    );
    
    // Execute and save
    analyzer.save();
    
    return 0;
}
```

## Using Systematic Manager

For more sophisticated systematic handling, use the SystematicManager:

{% raw %}
```cpp
#include "analyzer.h"
#include <NDHistogramManager.h>

int main(int argc, char** argv) {
    auto analyzer = Analyzer(argv[1]);
    auto histManager = std::make_unique<NDHistogramManager>(
        analyzer.getConfigurationProvider());
    analyzer.addPlugin("histogramManager", std::move(histManager));
    
    auto* sysMgr = analyzer.getSystematicManager();
    
    // Register systematics
    sysMgr->registerSystematic("JES_up");
    sysMgr->registerSystematic("JES_down");
    sysMgr->registerSystematic("btag_up");
    sysMgr->registerSystematic("btag_down");
    
    // Define variables with systematic awareness
    analyzer.Define("jet_pt",
        [](const ROOT::VecOps::RVec<float>& pt, const std::string& sys) {
            if (sys == "JES_up") return pt * 1.03;
            if (sys == "JES_down") return pt * 0.97;
            return pt;
        },
        {"jet_pt_raw"},
        sysMgr
    );
    
    analyzer.Define("mT", computeTransverseMass, 
        {"lepton_pt", "met"});
    
    analyzer.Define("event_weight",
        [](float mc_weight, float pu_weight, const std::string& sys) {
            float weight = mc_weight * pu_weight;
            if (sys == "btag_up") weight *= 1.05;  // Example scale factor
            if (sys == "btag_down") weight *= 0.95;
            return weight;
        },
        {"mc_weight", "pu_weight"},
        sysMgr
    );
    
    analyzer.Filter("selection", passSelection, {"lepton_pt", "jet_pt"});
    
    // Book histogram - automatically books for all systematics
    histManager->bookND(
        analyzer.getCurrentNode(),
        "mT",
        {"mT"},
        {"event_weight"},
        {{50, 0.0, 500.0}},
        sysMgr  // Pass systematic manager
    );
    
    analyzer.save();
    return 0;
}
```
{% endraw %}

## Histogram Naming Convention

The datacard generator expects:
- **Nominal**: `{observable}` (e.g., `mT`)
- **Up variation**: `{observable}_{systematic}Up` (e.g., `mT_JESUp`)
- **Down variation**: `{observable}_{systematic}Down` (e.g., `mT_JESDown`)

Where `{systematic}` matches the name in your datacard YAML config:

```yaml
systematics:
  JES:  # This name must match
    type: "shape"
    applies_to:
      signal: true
      ttbar: true
```

## Common Systematic Types

### 1. Shape Systematics (Observable Variations)

Changes the shape of the distribution:

```cpp
// Jet Energy Scale
analyzer.Define("mT_JESUp", computeMT_JESUp, {"jet_pt_up", "met_up"});
analyzer.Define("mT_JESDown", computeMT_JESDown, {"jet_pt_down", "met_down"});

// Jet Energy Resolution
analyzer.Define("mT_JERUp", computeMT_JERUp, {"jet_pt_smeared_up"});
analyzer.Define("mT_JERDown", computeMT_JERDown, {"jet_pt_smeared_down"});
```

### 2. Weight Systematics (Event Weight Variations)

Observable stays the same, weight changes:

```cpp
// b-tagging scale factors
analyzer.Define("weight_btagUp", computeWeight_btagUp, {"btag_sf_up"});
analyzer.Define("weight_btagDown", computeWeight_btagDown, {"btag_sf_down"});

histManager->BookSingleHistogram(
    analyzer.getCurrentNode(),
    "mT_btagUp",       // histogram name
    "mT",              // same observable
    "weight_btagUp",   // different weight
    50, 0.0, 500.0
);
```

### 3. Rate Systematics (Normalization Only)

Defined entirely in the YAML config, no additional histograms needed:

```yaml
systematics:
  lumi:
    type: "rate"
    value: 1.025  # 2.5% uncertainty
```

## Complete Workflow

### 1. Analysis Code
Create histograms with systematic variations as shown above.

### 2. Run Analysis
```bash
./myanalysis config.txt
# Output: signal.root, ttbar.root, wjets.root, etc.
```

### 3. Create Datacard Configuration

`datacard_config.yaml`:
```yaml
input_files:
  signal:
    path: "signal.root"
  ttbar:
    path: "ttbar.root"

systematics:
  JES:
    type: "shape"
    applies_to:
      signal: true
      ttbar: true
  btag:
    type: "shape"
    applies_to:
      signal: true
      ttbar: true
  lumi:
    type: "rate"
    value: 1.025
    applies_to:
      signal: true
      ttbar: true
```

### 4. Generate Datacards
```bash
python create_datacards.py datacard_config.yaml
# Output: datacards/datacard_signal_region.txt
#         datacards/shapes_signal_region.root
```

### 5. Run Combine
```bash
cd datacards
combine -M AsymptoticLimits datacard_signal_region.txt
```

## Tips

1. **Consistent Naming**: Ensure systematic names match between:
   - C++ code: `mT_JESUp`
   - YAML config: `JES` (automatically appends Up/Down)

2. **Same Binning**: All systematic variations must have identical binning to the nominal.

3. **Fallback**: If systematic histograms are missing, the generator creates placeholder variations.

4. **Validation**: Check your ROOT files before generating datacards:
   ```bash
   rootls -l signal.root
   # Should show: mT, mT_JESUp, mT_JESDown, etc.
   ```

5. **CombineHarvester**: For complex analyses with many systematics, consider using CombineHarvester
   for datacard management and manipulation.
