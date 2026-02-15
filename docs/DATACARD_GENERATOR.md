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
- Observable rebinning (uniform and variable bin widths)
- Systematic uncertainties (rate and shape)
- Systematic correlations across regions and processes
- Automatic reading of systematic variations from input files

## Installation

The script requires:
- Python 3.6+
- PyYAML
- uproot (for reading ROOT files without PyROOT dependency)
- numpy

Install dependencies:
```bash
pip install pyyaml uproot awkward numpy
```

**Note**: This script uses `uproot` for reading ROOT files, which does not require ROOT to be installed.
This makes it portable and easier to use in various environments.

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

The script reads ROOT files produced by RDFAnalyzerCore analyses using `uproot`. These files should contain:

- **TH1 histograms**: Standard 1D histograms (TH1F, TH1D, etc.)

**Note**: Only 1D histograms are supported. THnSparse and multi-dimensional histograms are not supported.
Ensure your analysis outputs 1D histograms for the observables specified in your datacard configuration.

### Histogram Naming

Histograms should be named according to the observable specified in the control region configuration. The script tries multiple naming conventions:

- `{observable}_{region}`
- `{region}_{observable}`
- `{observable}`
- `{observable}_{sample}`

**Example**: If your observable is `mT` and region is `signal_region`, the script will look for histograms named:
- `mT_signal_region`
- `signal_region_mT`
- `mT`

### Systematic Variations

For shape systematics, the input files should contain histogram variations with the naming convention:

- **Nominal histogram**: `{observable}` (e.g., `mT`)
- **Up variation**: `{observable}_{systematic}Up` (e.g., `mT_JESUp`)
- **Down variation**: `{observable}_{systematic}Down` (e.g., `mT_JESDown`)

Where `{systematic}` matches the name in your YAML configuration (e.g., `JES`, `btag`, etc.).

**Important**: The systematic name in your YAML config (e.g., `JES`) should match the suffix in your histogram names (e.g., `mT_JESUp`, `mT_JESDown`).

#### Example

If your YAML config has:
```yaml
systematics:
  JES:
    type: "shape"
```

Your ROOT files should contain:
- `mT` (nominal)
- `mT_JESUp` (up variation)
- `mT_JESDown` (down variation)

#### Fallback Behavior

If systematic variation histograms are not found in the input files, the script will:
1. Print a warning
2. Create placeholder variations by scaling the nominal histogram by ±`variation` parameter
3. Use these placeholders in the datacard (not recommended for final results)

For production analyses, always provide proper systematic variations in your input ROOT files.

See [DATACARD_SYSTEMATICS_EXAMPLE.md](DATACARD_SYSTEMATICS_EXAMPLE.md) for detailed examples of creating 
histograms with systematic variations in your RDFAnalyzer code.

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

The ROOT files are written using `uproot` and are fully compatible with CMS combine.

## Example Workflow

1. **Run your RDFAnalyzer analysis** to produce ROOT files with histograms:
```bash
./myanalysis config.txt
# Outputs: signal.root, ttbar.root, wjets.root, data.root
```

Each output file should contain:
- Nominal histograms (e.g., `mT`, `nJets`)
- Systematic variations (e.g., `mT_JESUp`, `mT_JESDown`, `mT_btagUp`, `mT_btagDown`)

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

3. **Book histograms** (including systematic variations):
```cpp
// Nominal
histManager->BookSingleHistogram(analyzer.getCurrentNode(),
    "mT", "mT", "event_weight", 50, 0.0, 500.0);

// Systematic variations
histManager->BookSingleHistogram(analyzer.getCurrentNode(),
    "mT_JESUp", "mT_JESUp", "event_weight", 50, 0.0, 500.0);
histManager->BookSingleHistogram(analyzer.getCurrentNode(),
    "mT_JESDown", "mT_JESDown", "event_weight", 50, 0.0, 500.0);
```

See [DATACARD_SYSTEMATICS_EXAMPLE.md](DATACARD_SYSTEMATICS_EXAMPLE.md) for comprehensive examples.

4. **Run and save**:
```cpp
analyzer.save();
```

The output ROOT file can then be used as input to the datacard generator.

## Integration with CMS Combine and CombineHarvester

### Building with Combine Support

RDFAnalyzerCore can optionally build CMS Combine and CombineHarvester alongside the framework:

```bash
# Build with Combine support
cmake -S . -B build -DBUILD_COMBINE=ON

# Build with Combine and CombineHarvester
cmake -S . -B build \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON

cmake --build build -j$(nproc)
```

This builds Combine in `build/external/HiggsAnalysis/CombinedLimit/` and makes it available for statistical analysis.

See [COMBINE_INTEGRATION.md](COMBINE_INTEGRATION.md) for complete documentation.

### Running CMS Combine

After generating datacards, you can run CMS Combine for statistical analysis:

**Quick Example:**

```bash
# Generate datacards
python core/python/create_datacards.py my_config.yaml

# Navigate to output directory
cd datacards

# Run Combine (if built with framework)
../build/external/HiggsAnalysis/CombinedLimit/exe/combine \
    -M AsymptoticLimits datacard_signal_region.txt

# Or use system Combine installation
combine -M AsymptoticLimits datacard_signal_region.txt
```

### Common Combine Commands

**Asymptotic Limits:**
```bash
combine -M AsymptoticLimits datacard_signal_region.txt -n MyAnalysis
```

**Maximum Likelihood Fit:**
```bash
combine -M MaxLikelihoodFit datacard_signal_region.txt --saveShapes --saveWithUncertainties
```

**Significance:**
```bash
combine -M Significance datacard_signal_region.txt
```

**Impact Plots:**
```bash
combineTool.py -M Impacts -d datacard_signal_region.txt --doInitialFit --robustFit 1
combineTool.py -M Impacts -d datacard_signal_region.txt --doFits --robustFit 1
combineTool.py -M Impacts -d datacard_signal_region.txt -o impacts.json
plotImpacts.py -i impacts.json -o impacts
```

### Using CombineHarvester

For complex analyses with multiple channels, CombineHarvester provides powerful tools:

```python
import CombineHarvester.CombineTools.ch as ch

# Create CombineHarvester instance
cb = ch.CombineHarvester()

# Add observations and processes
cb.AddObservations(['*'], ['myanalysis'], ['13TeV'], ['signal_region'], [(0, 0)])
cb.AddProcesses(['*'], ['myanalysis'], ['13TeV'], ['signal_region'], ['signal'], [(0, 0)], True)
cb.AddProcesses(['*'], ['myanalysis'], ['13TeV'], ['signal_region'], 
                ['ttbar', 'wjets', 'zjets'], [(0, 0)], False)

# Extract shapes from ROOT file
cb.cp().ExtractShapes(
    'shapes_signal_region.root',
    '$BIN/$PROCESS',
    '$BIN/$PROCESS_$SYSTEMATIC')

# Write datacards
cb.WriteDatacard('combined_datacard.txt', 'combined_shapes.root')
```

See the [Combine Integration Guide](COMBINE_INTEGRATION.md) for:
- Complete workflow examples
- CombineHarvester tutorials
- Advanced statistical methods
- Result extraction and plotting

## Complete Workflow Example

Here's a complete example workflow from analysis to results:

```bash
# 1. Build framework with Combine support
cd RDFAnalyzerCore
source env.sh
cmake -S . -B build -DBUILD_COMBINE=ON -DBUILD_COMBINE_HARVESTER=ON
cmake --build build -j$(nproc)

# 2. Run analysis (produces ROOT files with histograms)
cd build/analyses/MyAnalysis
./myanalysis config.txt
# Outputs: signal.root, ttbar.root, wjets.root, data.root

# 3. Generate datacards
cd ../../..
python core/python/create_datacards.py analysis_datacards.yaml
# Outputs: datacards/datacard_signal_region.txt, datacards/shapes_signal_region.root

# 4. Run Combine
cd datacards
COMBINE=../build/external/HiggsAnalysis/CombinedLimit/exe/combine

# Set limits
$COMBINE -M AsymptoticLimits datacard_signal_region.txt -n _MyAnalysis

# Run fit
$COMBINE -M MaxLikelihoodFit datacard_signal_region.txt \
    --saveShapes --saveWithUncertainties -n _MyAnalysis

# Calculate significance
$COMBINE -M Significance datacard_signal_region.txt -n _MyAnalysis

# 5. View results
root -l higgsCombine_MyAnalysis.AsymptoticLimits.mH120.root
```

## Troubleshooting

### uproot ImportError
- Install uproot: `pip install uproot awkward numpy`
- The script no longer requires PyROOT

### File Not Found
- Check that input file paths are correct in the configuration
- Use absolute paths or paths relative to where you run the script

### Histogram Not Found
- Verify histogram names in your ROOT files match the observable names
- Check histogram naming conventions
- Use `uproot` to inspect your ROOT files:
  ```python
  import uproot
  f = uproot.open("signal.root")
  print(f.keys())  # List all histograms
  ```

### Missing Systematic Variations
- If shape systematics are missing from input files, placeholder variations will be created with a warning
- For production use, ensure your analysis outputs include systematic variations
- See [DATACARD_SYSTEMATICS_EXAMPLE.md](DATACARD_SYSTEMATICS_EXAMPLE.md) for examples

### Empty Histograms
- Check that your analysis produced events that passed selections
- Verify that sample combinations are configured correctly

### Binning Mismatches
- All systematic variations must have the same binning as the nominal histogram
- Check that your analysis produces consistent binning

## References

- [CMS Combine Documentation](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/)
- [Combine Integration Guide](COMBINE_INTEGRATION.md) - Complete workflow with RDFAnalyzerCore
- [Datacard Systematics Example](DATACARD_SYSTEMATICS_EXAMPLE.md) - Creating histograms with systematic variations
- [RDFAnalyzerCore Documentation](https://github.com/brkronheim/RDFAnalyzerCore)

## Support

For issues or questions:
- Check the example configuration file (`example_datacard_config.yaml`)
- Review the systematics example (`DATACARD_SYSTEMATICS_EXAMPLE.md`)
- Check the Combine Integration Guide (`COMBINE_INTEGRATION.md`)
- Review the RDFAnalyzerCore documentation
- Open an issue on GitHub
