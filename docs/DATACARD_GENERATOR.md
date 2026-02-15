# CMS Combine Datacard Generator

This tool generates datacards and ROOT files for CMS combine fits from RDFAnalyzerCore analysis output files.

## Overview

The datacard generator reads histogram outputs from analysis runs and creates:
1. **Datacards** (`.txt` files) in CMS combine format
2. **ROOT files** with properly formatted histograms for combine

It supports:
- Multiple control regions with different observables
- Sample combination (binned and stitched samples)
- Process grouping and configuration
- Observable rebinning
- Systematic uncertainties (rate and shape)
- Systematic correlations across regions and processes

## Installation

The script requires:
- Python 3.6+
- PyYAML
- ROOT with Python bindings

Install dependencies:
```bash
pip install pyyaml
```

Make sure ROOT is available in your Python environment.

## Usage

### Basic Usage

```bash
python core/python/create_datacards.py config.yaml
```

### Command Line Options

```
python core/python/create_datacards.py [-h] [-v] config

Positional arguments:
  config         YAML configuration file

Optional arguments:
  -h, --help     Show help message and exit
  -v, --verbose  Verbose output with full error traces
```

## Configuration File

The tool is controlled by a YAML configuration file. See `example_datacard_config.yaml` for a complete example.

### Configuration Structure

#### 1. Output Directory

```yaml
output_dir: "datacards"
```

Specifies where datacards and ROOT files will be created.

#### 2. Input Files

```yaml
input_files:
  data_obs:
    path: "output/data.root"
    type: "data"
  
  signal:
    path: "output/signal.root"
    type: "signal"
  
  ttbar:
    path: "output/ttbar.root"
    type: "background"
```

Maps sample names to their ROOT file paths. Each sample should have a unique name.

#### 3. Sample Combinations

```yaml
sample_combinations:
  wjets:
    method: "sum"  # or "weighted"
    samples:
      - wjets_ht100to200
      - wjets_ht200to400
      - wjets_ht400to600
    weights:  # Optional, for weighted combination
      wjets_ht100to200: 1.0
      wjets_ht200to400: 1.0
      wjets_ht400to600: 1.0
```

Defines how to combine multiple samples (e.g., HT-binned or mass-binned samples):
- **method**: `"sum"` for simple addition, `"weighted"` for weighted sum
- **samples**: List of sample names to combine
- **weights**: Optional weights for each sample (used with weighted method)

#### 4. Processes

```yaml
processes:
  signal:
    samples:
      - signal_m500
    description: "Signal process"
  
  ttbar:
    samples:
      - ttbar
    description: "Top pair production"
  
  wjets:
    samples:
      - wjets  # Can reference a combined sample
    description: "W+jets production"
```

Groups samples into physics processes:
- **samples**: List of sample names (can reference combined samples)
- **description**: Human-readable description

#### 5. Control Regions

```yaml
control_regions:
  signal_region:
    observable: "mT"  # Histogram name
    processes:
      - signal
      - ttbar
      - wjets
    signal_processes:
      - signal
    data_process: "data_obs"
    rebin: 2  # Integer for uniform rebinning
    # OR
    # rebin: [0, 50, 100, 200, 500]  # Variable bin edges
    description: "Signal region"
```

Defines control regions:
- **observable**: Name of the histogram to use (must exist in ROOT files)
- **processes**: List of processes to include
- **signal_processes**: Which processes are signals (get negative process IDs)
- **data_process**: Name of the data sample
- **rebin**: Optional rebinning (integer factor or list of bin edges)
- **description**: Human-readable description

#### 6. Systematics

```yaml
systematics:
  # Rate uncertainty
  lumi:
    type: "rate"
    distribution: "lnN"
    value: 1.025  # 2.5% uncertainty
    applies_to:
      signal: true
      ttbar: true
      wjets: false
    regions:
      - signal_region
    description: "Luminosity"
  
  # Shape uncertainty
  JES:
    type: "shape"
    distribution: "shape"
    variation: 0.03  # For placeholder variations
    applies_to:
      signal: true
      ttbar: true
    regions:
      - signal_region
    correlated: true
    description: "Jet energy scale"
```

Defines systematic uncertainties:
- **type**: `"rate"` for normalization, `"shape"` for shape variation
- **distribution**: `"lnN"` for log-normal, `"shape"` for shape uncertainties
- **value**: Uncertainty value (for rate uncertainties)
- **variation**: Variation size (for shape uncertainties, placeholder)
- **applies_to**: Dictionary of process names and whether systematic applies
- **regions**: List of regions where systematic applies (all if not specified)
- **correlated**: Whether systematic is correlated across regions
- **description**: Human-readable description

#### 7. Correlations

```yaml
correlations:
  JES:
    type: "full"
    regions: "all"
    processes: "all"
  
  qcd_norm:
    type: "uncorrelated"
    regions: "all"
```

Specifies correlation structure (optional, for documentation):
- **type**: `"full"` or `"uncorrelated"`
- **regions**: Which regions are correlated
- **processes**: Which processes are correlated

## Input File Format

The script reads ROOT files produced by RDFAnalyzerCore analyses. These files should contain:

- **TH1 histograms**: Standard 1D histograms
- **THnSparse histograms**: Multi-dimensional sparse histograms (automatically projected)

### Histogram Naming

Histograms should be named according to the observable specified in the control region configuration. The script tries multiple naming conventions:

- `{observable}_{region}`
- `{region}_{observable}`
- `{observable}`
- `{observable}_{sample}`

### Systematic Variations

For shape systematics, the input files should contain:
- Nominal histogram: `{observable}`
- Up variation: `{observable}_{systematic}Up`
- Down variation: `{observable}_{systematic}Down`

If systematic variations are not found in the input files, placeholder variations will be created.

## Output Files

### Datacards

Datacards are text files in CMS combine format:
- `datacard_{region}.txt`

Each datacard includes:
- Bin definitions
- Process definitions
- Observed event counts
- Expected rates
- Systematic uncertainties

### ROOT Files

ROOT files contain histograms for combine:
- `shapes_{region}.root`

Each file includes:
- Nominal histograms for each process
- Systematic variation histograms (Up/Down)

## Example Workflow

1. **Run your RDFAnalyzer analysis** to produce ROOT files with histograms:
```bash
./myanalysis config.txt
```

2. **Create a datacard configuration YAML file** (see `example_datacard_config.yaml`)

3. **Generate datacards**:
```bash
python core/python/create_datacards.py my_config.yaml
```

4. **Run combine**:
```bash
combine -M AsymptoticLimits datacards/datacard_signal_region.txt
```

## Advanced Features

### Variable Binning

Use variable bin edges for rebinning:
```yaml
control_regions:
  my_region:
    rebin: [0, 50, 100, 150, 200, 300, 500, 1000]
```

### Uncorrelated Systematics

Create separate systematics for different regions:
```yaml
systematics:
  qcd_norm_SR:
    type: "rate"
    value: 1.30
    applies_to:
      qcd: true
    regions:
      - signal_region
  
  qcd_norm_CR:
    type: "rate"
    value: 1.30
    applies_to:
      qcd: true
    regions:
      - control_region
```

### Process Selection per Region

Different regions can have different processes:
```yaml
control_regions:
  signal_region:
    processes:
      - signal
      - ttbar
      - wjets
  
  control_region:
    processes:
      - ttbar
      - wjets
      # No signal in control region
```

## Troubleshooting

### File Not Found
- Check that input file paths are correct in the configuration
- Use absolute paths or paths relative to where you run the script

### Histogram Not Found
- Verify histogram names in your ROOT files match the observable names
- Check histogram naming conventions
- Use `rootls` or `rootbrowse` to inspect your ROOT files

### Missing Systematic Variations
- If shape systematics are missing from input files, placeholder variations will be created
- For production use, ensure your analysis outputs include systematic variations

### Empty Histograms
- Check that your analysis produced events that passed selections
- Verify that sample combinations are configured correctly

## Integration with RDFAnalyzerCore

This tool is designed to work with RDFAnalyzerCore analysis outputs. To prepare your analysis:

1. **Book histograms** using NDHistogramManager:
```cpp
auto histManager = std::make_unique<NDHistogramManager>(
    analyzer.getConfigurationProvider());
analyzer.addPlugin("histogramManager", std::move(histManager));
```

2. **Define observables** and apply selections:
```cpp
analyzer.Define("mT", computeMT, {"met", "lepton_pt"});
analyzer.Filter("signal_selection", passSelection, {"mT", "nJets"});
```

3. **Book histograms** from config:
```cpp
analyzer.bookConfigHistograms();
```

4. **Run and save**:
```cpp
analyzer.save();
```

The output ROOT file can then be used as input to the datacard generator.

## References

- [CMS Combine Documentation](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/)
- [RDFAnalyzerCore Documentation](https://github.com/brkronheim/RDFAnalyzerCore)

## Support

For issues or questions:
- Check the example configuration file
- Review the RDFAnalyzerCore documentation
- Open an issue on GitHub
