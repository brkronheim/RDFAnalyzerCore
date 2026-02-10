# Config-Driven Histogram Feature

## Overview

This feature allows histograms to be defined in a configuration file and automatically booked at runtime. Histograms respect filters and defines applied to the dataframe and work with systematic variations just like manually-defined histograms.

## Configuration File Format

Create a histogram configuration file with one histogram definition per line. Each line contains key=value pairs.

### Required Fields

- `name`: Histogram name
- `variable`: Variable to histogram
- `weight`: Weight variable
- `bins`: Number of bins
- `lowerBound`: Lower bound of histogram
- `upperBound`: Upper bound of histogram

### Optional Fields

- `label`: Axis label (defaults to variable name)
- `suffix`: Suffix to append to histogram name
- `channelVariable`: Variable for channel axis
- `channelBins`: Number of channel bins
- `channelLowerBound`: Lower bound for channel
- `channelUpperBound`: Upper bound for channel
- `channelRegions`: Comma-separated list of channel region names
- `controlRegionVariable`: Variable for control region axis
- `controlRegionBins`: Number of control region bins
- `controlRegionLowerBound`: Lower bound for control region
- `controlRegionUpperBound`: Upper bound for control region
- `controlRegionRegions`: Comma-separated list of control region names
- `sampleCategoryVariable`: Variable for sample category axis
- `sampleCategoryBins`: Number of sample category bins
- `sampleCategoryLowerBound`: Lower bound for sample category
- `sampleCategoryUpperBound`: Upper bound for sample category
- `sampleCategoryRegions`: Comma-separated list of sample category names

## Example Configuration File

```txt
# Simple 1D histogram
name=pt_hist variable=jet_pt weight=event_weight bins=50 lowerBound=0.0 upperBound=500.0 label=Jet pT [GeV] suffix=jets

# Histogram with channel separation
name=eta_hist variable=jet_eta weight=event_weight bins=40 lowerBound=-5.0 upperBound=5.0 channelVariable=channel channelBins=2 channelLowerBound=0.0 channelUpperBound=2.0 channelRegions=ee,mumu suffix=dilep

# Full N-dimensional histogram with all separations
name=mass_hist variable=dimuon_mass weight=event_weight bins=60 lowerBound=70.0 upperBound=110.0 channelVariable=channel channelBins=2 channelLowerBound=0.0 channelUpperBound=2.0 channelRegions=signal,control controlRegionVariable=region controlRegionBins=3 controlRegionLowerBound=0.0 controlRegionUpperBound=3.0 controlRegionRegions=SR,CR1,CR2 sampleCategoryVariable=sample sampleCategoryBins=2 sampleCategoryLowerBound=0.0 sampleCategoryUpperBound=2.0 sampleCategoryRegions=data,MC suffix=analysis
```

## Usage in Code

### Basic Setup

```cpp
#include <analyzer.h>
#include <NDHistogramManager.h>

// Create analyzer with config file
auto analyzer = Analyzer("config.txt");

// Add NDHistogramManager plugin
auto histManager = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());
analyzer.addPlugin("histogramManager", std::move(histManager));
```

### Configuration File Reference

In your main config file (e.g., `config.txt`), add:

```txt
histogramConfig=histograms.txt
```

### Define Variables and Apply Filters

```cpp
// Define variables
analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"})
        .Define("jet_eta", computeEta, {"jet_px", "jet_py", "jet_pz"})
        .Define("event_weight", computeWeight, {"mc_weight", "pu_weight"})
        .Define("channel", getChannel, {"lepton_type"});

// Apply filters
analyzer.Filter("good_events", isGoodEvent, {"event_flags"})
        .Filter("jet_quality", hasGoodJets, {"jet_pt", "jet_eta"});
```

### Book Config Histograms

After all defines and filters are applied, book the histograms from config:

```cpp
// Book histograms defined in config file
analyzer.bookConfigHistograms();
```

### Save Results

```cpp
// Trigger dataframe execution and save histograms
analyzer.save();
```

## Complete Example

```cpp
#include <analyzer.h>
#include <NDHistogramManager.h>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config.txt>" << std::endl;
        return 1;
    }

    // Create analyzer
    auto analyzer = Analyzer(argv[1]);

    // Add histogram manager plugin
    auto histManager = std::make_unique<NDHistogramManager>(
        analyzer.getConfigurationProvider());
    analyzer.addPlugin("histogramManager", std::move(histManager));

    // Define variables
    analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"})
            .Define("jet_eta", computeEta, {"jet_px", "jet_py", "jet_pz"})
            .Define("event_weight", computeWeight, {"mc_weight", "pu_weight"})
            .Define("channel", getChannel, {"lepton_type"});

    // Apply event selection
    analyzer.Filter("trigger", passTrigger, {"trigger_bits"})
            .Filter("lepton_selection", hasGoodLeptons, {"lepton_pt", "lepton_eta"})
            .Filter("jet_selection", hasGoodJets, {"jet_pt", "jet_eta"});

    // Book histograms from config file
    analyzer.bookConfigHistograms();

    // Manually book additional histograms if needed
    // (config histograms and manual histograms work together)
    
    // Execute dataframe and save results
    analyzer.save();

    return 0;
}
```

## Features

### Flexible Dimensionality

- **1D histograms**: Only specify required fields (name, variable, weight, bins, bounds)
- **Multi-dimensional**: Add channel, control region, and/or sample category dimensions as needed
- **Mixed usage**: Combine config-defined and manually-defined histograms in the same analysis

### Systematic Variations

Config-defined histograms automatically handle systematic variations. If you register systematics that affect variables used in histograms, the histograms will be filled for each systematic variation.

```cpp
// Register systematic
analyzer.getSystematicManager().registerSystematic("JES_up", {"jet_pt", "jet_eta"});

// Define systematic variations
analyzer.Define("jet_pt_JES_up", applyJESUp, {"jet_pt"})
        .Define("jet_eta_JES_up", applyJESUpEta, {"jet_eta"});

// Histograms using jet_pt and jet_eta will automatically 
// include JES_up variation bins
analyzer.bookConfigHistograms();
```

### Deferred Booking

Histograms are booked after all defines and filters have been applied, ensuring they:
- Respect all event selections
- See all defined variables
- Work with the final dataframe state

### Backward Compatibility

The existing methods for booking histograms remain unchanged:
- `NDHistogramManager::BookSingleHistogram()`
- `NDHistogramManager::bookND()`

You can use config-driven histograms alongside manually-defined histograms in the same analysis.

## Notes

- Histogram variables and weights must be defined before calling `bookConfigHistograms()`
- If no `histogramConfig` is specified in the main config, no config histograms are loaded
- Missing required fields in the histogram config will throw an exception
- Invalid config files will throw an exception with details
- Histograms are saved to the file specified by `metaFile` or `saveFile` in the main config
