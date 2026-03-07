# New Features Guide

This document describes recent features added to RDFAnalyzerCore. For complete documentation, see the main guides linked below.

## Table of Contents

- [KinematicFitManager Plugin](#kinematicfitmanager-plugin)
- [GoldenJsonManager Plugin](#goldenjsonmanager-plugin)
- [ONNX Input Padding](#onnx-input-padding)
- [Glob Pattern Support for Output Selection](#glob-pattern-support-for-output-selection)
- [CorrectionManager Vector Support](#correctionmanager-vector-support)
- [Boost Histogram Backend](#boost-histogram-backend)
- [PlottingUtility Python Bindings](#plottingutility-python-bindings)
- [Law Workflow Integration](#law-workflow-integration)

---

## KinematicFitManager Plugin

Performs kinematic fitting for reconstructed decay topologies using mass, momentum, and recoil constraints.

### Features

- Flexible constraint configuration (mass, pT, recoil)
- Support for MET/recoil particles
- Collection-indexed particle selection
- Three-body mass constraints
- Resonance width (soft mass constraints) via `massSigma`
- Per-event fit skipping with `runVar`

### Configuration

Create a kinematic fit configuration file:

```
# kfit.txt
name=VH_fit outputPrefix=kfit_
particles=lep1,lep2,jet1,jet2,MET
lep1.type=collection lep1.collection=Muon lep1.index=0
lep2.type=collection lep2.collection=Muon lep2.index=1
jet1.type=collection jet1.collection=Jet jet1.index=0
jet2.type=collection jet2.collection=Jet jet2.index=1
MET.type=recoil MET.collection=MET

# Mass constraints
constraint1.type=mass constraint1.particles=lep1,lep2
constraint1.targetMass=91188.0 constraint1.massSigma=2495.0
constraint2.type=mass constraint2.particles=jet1,jet2
constraint2.targetMass=125090.0 constraint2.massSigma=3000.0

# Run control
runVar=do_kfit
```

### Usage

```cpp
#include <KinematicFitManager.h>

// Add plugin
auto kfitMgr = std::make_unique<KinematicFitManager>(
    analyzer.getConfigurationProvider()
);
analyzer.addPlugin("kinematicFit", std::move(kfitMgr));

// Apply fits (defines output columns)
analyzer.getPlugin<IKinematicFitManager>("kinematicFit")->applyAllFits();

// Fitted four-momentum columns are now available:
// kfit_lep1_pt, kfit_lep1_eta, kfit_lep1_phi, kfit_lep1_mass, etc.
// Also: kfit_chi2, kfit_status
```

### Output Columns

For each fit, the following columns are defined:
- `{outputPrefix}{particle}_pt/eta/phi/mass` - Fitted four-momentum
- `{outputPrefix}chi2` - Fit χ²
- `{outputPrefix}status` - Fit status (0 = success)

---

## GoldenJsonManager Plugin

Filters data events based on CMS golden JSON certification files for run/lumi-section validity.

### Features

- Data-only filtering (automatic MC skip)
- Multi-file support (per-era JSONs)
- Merges all JSON files into unified validity map
- Embedded JSON parser (no external dependencies)

### Configuration

Create a file listing JSON paths:

```
# golden_json_files.txt
cfg/Cert_2022.json
cfg/Cert_2023.json
```

Main config:

```
# config.txt
type=data
goldenJsonConfig=cfg/golden_json_files.txt
```

### Usage

```cpp
#include <GoldenJsonManager.h>

// Add plugin
auto goldenJson = std::make_unique<GoldenJsonManager>(
    analyzer.getConfigurationProvider()
);
analyzer.addPlugin("goldenJson", std::move(goldenJson));

// Apply filter (automatic - reads run/luminosityBlock branches)
analyzer.getPlugin<IGoldenJsonManager>("goldenJson")->applyGoldenJson();
```

The plugin automatically filters events where `(run, luminosityBlock)` is not certified. For MC (when `type != "data"`), the plugin does nothing.

---

## ONNX Input Padding

ONNX models with fixed-size inputs (e.g., transformer attention mechanisms) now support automatic zero-padding.

### Configuration

Add `paddingSize` to your ONNX model configuration:

```
# onnx_models.txt
file=transformer.onnx name=ParT inputVariables=pt,eta,phi paddingSize=128
```

### Behavior

- Input vectors shorter than `paddingSize` are zero-padded
- Input vectors longer than `paddingSize` are truncated
- Omitting `paddingSize` preserves existing behavior (no padding)

### Usage Example

```cpp
// With paddingSize=128:
// Input: RVec<float>{1.0, 2.0, 3.0}  (size 3)
// Padded to: RVec<float>{1.0, 2.0, 3.0, 0.0, 0.0, ..., 0.0}  (size 128)
```

---

## Glob Pattern Support for Output Selection

The `saveConfig` file now supports glob patterns for column selection.

### Configuration

```
# saveConfig.txt
# Exact column names
event
run
luminosityBlock

# Glob patterns
Muon_*           # All Muon columns
Electron_*       # All Electron columns
*_phi            # All phi columns
Jet_pt
Jet_eta
```

### Behavior

- Patterns with `*` or `?` are expanded against available columns
- Exact column names continue to work unchanged
- Overlapping patterns are deduplicated
- Non-matching patterns are silently skipped

---

## CorrectionManager Vector Support

Apply corrections element-wise over object collections (e.g., per-jet scale factors).

### Configuration

Same as scalar corrections:

```
# corrections.txt
correction=jet_sf file=corrections.json evaluator=jet_energy_scale inputVariables=pt,eta
```

### Usage

```cpp
// For RVec<float> columns jet_pt, jet_eta
correctionMgr->applyCorrectionVec("jet_sf", {"nominal"});

// Defines RVec<float> column "jet_sf" with one value per jet
// Use downstream:
analyzer.Define("corrected_pt",
    [](const RVec<float>& pt, const RVec<float>& sf) {
        return pt * sf;
    },
    {"jet_pt", "jet_sf"}
);
```

See [API Reference](API_REFERENCE.md#correctionmanager) for details.

---

## Boost Histogram Backend

NDHistogramManager now supports Boost.Histogram as an alternative backend with automatic dense/sparse storage selection.

### Features

- Automatic memory-based selection: dense (fast) vs sparse (memory-safe)
- Threshold: 64 MiB per thread
- Both ROOT (`THnMulti`) and Boost (`BHnMulti`) backends supported
- Transparent to users (same booking interface)

### Configuration

```
# histograms.txt
name=jet_pt_eta variable=jet_pt,jet_eta bins=50,20 ...
```

The framework automatically selects:
- **Dense storage**: Small histograms (< 64 MiB) → O(1) array access
- **Sparse storage**: Large histograms → hash-based storage

### Performance

Typical 5D histogram (4×4×5×50×40 = 160K bins):
- Dense memory: ~26 MB → selects dense (fast)
- Fill performance: ~10-50x faster than always-sparse

Large histogram (10×10×10×200×100 = 20M bins):
- Dense memory: ~14 GB → falls back to sparse (safe)

---

## PlottingUtility Python Bindings

Create ROOT stack plots from Python with the full C++ plotting functionality.

### Python API

```python
import rdfanalyzer

# Configure process
proc = rdfanalyzer.PlotProcessConfig()
proc.directory = "signal"
proc.histogramName = "pt"
proc.legendLabel = "Signal MC"
proc.color = 2
proc.normalizationHistogram = "counter_weightSum_signal"

# Create plot request
req = rdfanalyzer.PlotRequest()
req.metaFile = "meta.root"
req.outputFile = "pt.pdf"
req.xAxisTitle = "p_{T} [GeV]"
req.yAxisTitle = "Events"
req.logY = False
req.drawRatio = True
req.processes = [proc]

# Generate plot
result = rdfanalyzer.PlottingUtility().makeStackPlot(req)
assert result.success, result.message
```

### Features

- Stack plots with data/MC ratio panels
- Log-y scale support
- Automatic normalization
- PCA-based systematic envelopes
- Parallel batch plotting

See [Python Bindings](PYTHON_BINDINGS.md) for complete API.

---

## Law Workflow Integration

Law task framework for managing analysis workflows on batch systems.

### Available Tasks

#### NANO Analysis
- `SubmitNANOJobs` - Submit CMS NanoAOD analysis to HTCondor/DASK
- `MonitorNANOJobs` - Monitor job progress
- `ValidateNANOOutputs` - Validate ROOT outputs

#### Plotting
- `MakePlot` - Single stack plot
- `MakePlots` - Batch plotting from config

#### Combine Datacards
- `CreateDatacard` - Generate CMS combine datacards
- `RunCombine` - Run statistical fits

### Example Workflow

```bash
# Submit analysis
law run SubmitNANOJobs \
    --config cfg/analysis.txt \
    --dataset /DoubleMuon/Run2022*/NANOAOD \
    --backend htcondor

# Create datacards
law run CreateDatacard \
    --datacard-config cfg/datacard.yaml \
    --name myRun

# Run fits
law run RunCombine \
    --name myRun \
    --method AsymptoticLimits
```

See [Production Manager Guide](PRODUCTION_MANAGER.md) and [Combine Integration](COMBINE_INTEGRATION.md) for details.

---

## Summary

These new features enhance RDFAnalyzerCore with:

1. **Advanced physics**: Kinematic fitting for decay reconstruction
2. **Data quality**: Automated golden JSON filtering
3. **ML flexibility**: Transformer model support via input padding
4. **Convenience**: Glob patterns, vector corrections, Python plotting
5. **Performance**: Optimized histogram storage
6. **Workflows**: Law integration for production analyses

For complete documentation of each feature, see the main documentation pages linked from the [documentation index](index.md).

## Related Documentation

- [Getting Started](GETTING_STARTED.md)
- [Configuration Reference](CONFIG_REFERENCE.md)
- [API Reference](API_REFERENCE.md)
- [Plugin Development](PLUGIN_DEVELOPMENT.md)
- [Production Manager](PRODUCTION_MANAGER.md)
- [Combine Integration](COMBINE_INTEGRATION.md)
