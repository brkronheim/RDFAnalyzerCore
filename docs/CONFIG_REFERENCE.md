# Configuration Reference

This document provides a complete reference for all configuration options in RDFAnalyzerCore.

## Table of Contents

- [Main Configuration File](#main-configuration-file)
- [Plugin Configurations](#plugin-configurations)
- [Advanced Configuration](#advanced-configuration)
- [Configuration File Format](#configuration-file-format)
- [Example Configurations](#example-configurations)

## Main Configuration File

The main configuration file controls the overall behavior of the framework. It's typically passed as a command-line argument to your analysis executable.

### Input/Output Configuration

#### Required Options

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `saveFile` | Path | Output ROOT file for event-level data (skim) | `output/data.root` |
| `fileList` | Comma-separated URLs/paths | Input ROOT files to process | `root://xrootd/file1.root,/path/file2.root` |

#### Optional I/O Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `saveDirectory` | Path | (empty) | Local directory where intermediate outputs are saved |
| `saveTree` | String | `Events` | Name of the output tree |
| `treeList` | Comma-separated | `CollectionTree,POOLCollectionTree` | Tree names to search for in input files (tried in order) |
| `metaFile` | Path | Same as `saveFile` | Separate file for histograms and metadata |
| `antiglobs` | Comma-separated | (empty) | Reject input files containing these strings |
| `globs` | Comma-separated | (empty) | Accept only input files containing these strings |

### Performance Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `threads` | Integer | `-1` | Number of ROOT ImplicitMT threads. `-1` = auto (all cores) |

### Batch Processing

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch` | Boolean | `false` | Enable batch mode (disables interactive progress bars) |

### Output Branch Configuration

| Option | Type | Description |
|--------|------|-------------|
| `saveConfig` | Path | Configuration file listing branches to save in output tree |

The `saveConfig` file contains one branch name per line. It also supports **glob patterns** for flexible column selection:

```
# Exact column names
branch1
branch2
branch3

# Glob patterns
Muon_*           # All Muon columns
Electron_*       # All Electron columns
*_phi            # All phi columns
Jet_pt           # Specific column
Jet_eta          # Specific column
```

**Glob Pattern Behavior**:
- Patterns with `*` or `?` are expanded against available DataFrame columns
- Exact column names continue to work unchanged
- Overlapping patterns are automatically deduplicated
- Non-matching patterns are silently skipped (no error)
- Mix exact names and glob patterns in the same file

### Data Loading Helpers

| Option | Type | Description |
|--------|------|-------------|
| `floatConfig` | Path | Configuration file with float constants to define |
| `intConfig` | Path | Configuration file with integer constants to define |
| `aliasConfig` | Path | Configuration file with branch aliases |
| `optionalBranchesConfig` | Path | Configuration file listing branches that may not exist in all inputs |

#### Float/Int Config Format

These files define constants that are loaded as DataFrame columns:
```
constantName=value
weight=1.0
luminosity=139000.0
```

#### Alias Config Format

Define short names for long branch names:
```
pt=leptons_pt
eta=leptons_eta
```

#### Optional Branches Config Format

List branches that might not exist in all input files (prevents errors):
```
branch_that_might_be_missing
another_optional_branch
```

## Plugin Configurations

Plugins are configured via separate files referenced from the main config.

### BDT Manager Configuration

**Main config option**: `bdtConfig=path/to/bdts.txt`

**BDT config format** (one model per line):
```
file=path/to/model.txt name=output_column inputVariables=var1,var2,var3 runVar=condition_column
```

**Parameters**:
- `file`: Path to FastForest BDT text file
- `name`: Name of the output column in the DataFrame
- `inputVariables`: Comma-separated list of input feature column names
- `runVar`: Boolean column name; model runs only when this is true (outputs -1.0 when false)

**Example**:
```
file=aux/bdt_signal.txt name=bdt_score inputVariables=jet_pt,jet_eta,jet_phi,jet_mass runVar=has_jet
file=aux/bdt_background.txt name=bdt_bkg_score inputVariables=lep_pt,met runVar=pass_presel
```

### ONNX Manager Configuration

**Main config option**: `onnxConfig=path/to/onnx_models.txt`

**ONNX config format** (one model per line):
```
file=path/to/model.onnx name=output_column inputVariables=var1,var2,var3 runVar=condition_column
```

**Parameters**: Same as BDT Manager

**Multi-Output Support**: Models with multiple outputs automatically create columns named `{name}_output0`, `{name}_output1`, etc.

**Example**:
```
file=models/dnn_classifier.onnx name=dnn_score inputVariables=pt,eta,phi,mass,btag runVar=has_jets
file=models/particle_transformer.onnx name=transformer inputVariables=jet_features runVar=pass_cuts
```

**Note**: ONNX models are loaded at construction but NOT automatically applied. You must call `applyModel()` or `applyAllModels()` after defining all input features.

### SOFIE Manager Configuration

**Main config option**: `sofieConfig=path/to/sofie_models.txt`

**SOFIE config format** (one model per line):
```
name=output_column inputVariables=var1,var2,var3 runVar=condition_column
```

**Parameters**: Same as ONNX/BDT but WITHOUT `file` (SOFIE models are compiled code, not runtime files)

**Example**:
```
name=sofie_dnn inputVariables=pt,eta,phi,mass runVar=has_jet
name=sofie_classifier inputVariables=lep_pt,met runVar=pass_selection
```

**Key Differences from ONNX/BDT**:
- No `file` parameter (models compiled at build time)
- Must manually register models via `registerModel()` in C++ code
- Potentially faster inference (compiled code eliminates runtime overhead) but less flexible (rebuild required for updates)

**See**: [SOFIE Implementation Guide](SOFIE_IMPLEMENTATION.md) for complete usage including:
- Generating SOFIE C++ code from ONNX
- Registering models in analysis code
- When to use SOFIE vs ONNX

### Correction Manager Configuration

**Main config option**: `correctionConfig=path/to/corrections.txt`

**Correction config format** (one correction per line):
```
file=path/to/corrections.json correctionName=correction_key name=output_column inputVariables=var1,var2
```

**Parameters**:
- `file`: Path to correctionlib JSON file
- `correctionName`: Key of the correction within the JSON file
- `name`: Name of the output column in the DataFrame
- `inputVariables`: Comma-separated list of input column names (order matters!)

**Example**:
```
file=aux/scale_factors.json correctionName=muon_id_sf name=muon_sf inputVariables=muon_pt,muon_eta
file=aux/scale_factors.json correctionName=electron_iso_sf name=electron_sf inputVariables=electron_pt,electron_eta
```

#### Applying corrections to a single object per event (`applyCorrection`)

Call `applyCorrection(name, stringArguments)` in your analysis code. The
`inputVariables` columns must be **scalar** (one value per event). String
inputs declared in the correctionlib JSON are supplied via `stringArguments`
in the order they appear in the JSON file.

```cpp
// Scalar correction – one scale factor per event
correctionManager.applyCorrection("muon_sf", {"nominal"});
```

The resulting `muon_sf` column contains a single `Float_t` per event.

#### Applying corrections over a collection of objects per event (`applyCorrectionVec`)

Use `applyCorrectionVec(name, stringArguments)` when you need to evaluate the
same correction once **per object** in a collection (e.g. all jets in an
event). The `inputVariables` columns must be **RVec columns** (one vector per
event, one entry per object).

String inputs are still supplied as plain strings via `stringArguments`; the
same string value is used for every object in the collection.

```cpp
// Vector correction – one scale factor per jet, per event
// Requires jet_pt and jet_eta to be RVec<float> (or similar) columns
correctionManager.applyCorrectionVec("jet_sf", {"nominal"});
```

The resulting `jet_sf` column contains a `ROOT::VecOps::RVec<Float_t>` per
event, with one scale factor for each object.

**Full example** (config + analysis code):

*corrections.txt*:
```
file=aux/jet_sf.json correctionName=jet_energy_scale name=jet_sf inputVariables=jet_pt,jet_eta
```

*Analysis code*:
```cpp
// jet_pt and jet_eta are RVec<float> columns (one entry per jet per event)
correctionManager.applyCorrectionVec("jet_sf", {"nominal"});

// Access the per-jet scale factors downstream
dataManager.Define("corrected_jet_pt",
    [](const ROOT::VecOps::RVec<float>& pt,
       const ROOT::VecOps::RVec<Float_t>& sf) {
        return pt * sf;
    },
    {"jet_pt", "jet_sf"}
);
```

### Trigger Manager Configuration

**Main config option**: `triggerConfig=path/to/triggers.txt`

**Trigger config format** (one group per line):
```
name=group_name sample=sample_name triggers=trig1,trig2,trig3 triggerVetos=veto1,veto2
```

**Parameters**:
- `name`: Name for this trigger group
- `sample`: Sample identifier this group applies to
- `triggers`: Comma-separated list of trigger names (OR logic)
- `triggerVetos`: Comma-separated list of veto triggers (optional)

**Example**:
```
name=single_muon sample=data triggers=HLT_Mu24,HLT_Mu27 triggerVetos=
name=double_muon sample=mc triggers=HLT_Mu17_Mu8,HLT_Mu17_TkMu8 triggerVetos=HLT_Mu24,HLT_Mu27
```

The trigger logic: Event passes if ANY trigger fires AND NO veto trigger fires.

### WeightManager Configuration

WeightManager is configured **programmatically** in your analysis C++ code. The plugin must be registered in the main config, but all weight components are declared at runtime via the API.

**Main config entry**:
```
# WeightManager
# Note: WeightManager is configured programmatically in your analysis code,
# not via the config file. However, it must be registered as a plugin.
WeightManager = WeightManager
```

**Registered programmatically in C++**:
```cpp
auto* wm = analyzer->getPlugin<WeightManager>("weights");

// Scale factors: named per-event multiplicative corrections (dataframe columns)
wm->addScaleFactor("pileup_sf",  "pu_weight");
wm->addScaleFactor("btag_sf",    "btag_weight");

// Normalization weights: scalar factors applied uniformly to all events
wm->addNormalization("lumi_xsec", 0.0412);  // e.g. xsec * lumi / sumWeights

// Systematic weight variations: named up/down shifts
wm->addWeightVariation("pileup", "pu_weight_up", "pu_weight_down");

// Define the nominal combined weight column on the dataframe
wm->defineNominalWeight("weight_nominal");

// Define varied weight columns (for systematic histograms)
wm->defineVariedWeight("pileup", "up",   "weight_pileup_up");
wm->defineVariedWeight("pileup", "down", "weight_pileup_down");
```

After `analyzer->run()`, per-component audit statistics (sum, mean, min, max, negative-event count) are written to the meta ROOT file and logged.

### RegionManager Configuration

RegionManager is configured **programmatically** in your analysis C++ code. Regions are declared via `declareRegion()` and do not use a config file.

**Main config entry**:
```
# RegionManager
# Note: RegionManager is configured programmatically in your analysis code.
# Regions are declared using declareRegion() in your analysis.
RegionManager = RegionManager
```

**Declared programmatically in C++**:
```cpp
// 1. Define boolean filter columns upfront
analyzer->Define("pass_presel",  [](float pt){ return pt > 20.f; }, {"pt"});
analyzer->Define("pass_signal",  [](float mva){ return mva > 0.8f; }, {"mva"});
analyzer->Define("pass_control", [](float mva){ return mva < 0.4f; }, {"mva"});

// 2. Declare regions (parent before child)
auto* rm = analyzer->getPlugin<RegionManager>("regions");
rm->declareRegion("presel",  "pass_presel");
rm->declareRegion("signal",  "pass_signal",  "presel");  // child of presel
rm->declareRegion("control", "pass_control", "presel");  // child of presel

// 3. Retrieve per-region DataFrames for histogramming
ROOT::RDF::RNode signalDf = rm->getRegionDataFrame("signal");
```

Child regions are strict subsets of their parent. `initialize()` validates the hierarchy (no cycles, no missing parents, no duplicate names). `finalize()` writes a region-summary `TNamed` to the meta ROOT file.

### GoldenJsonManager Configuration

Applies CMS data quality certification filters against one or more golden JSON files. The filter is **automatically skipped for MC samples** — it only activates when the config key `type=data`.

**Main config options**:
```
# GoldenJsonManager - CMS data quality certification
GoldenJsonManager = GoldenJsonManager
goldenJsonConfig = /path/to/golden_json_list.txt
```

**goldenJsonConfig**: Path to a text file listing golden JSON file paths, **one per line**. Lines beginning with `#` are treated as comments.

**Example golden JSON list file** (`golden_json_list.txt`):
```
# CMS golden JSON files for Run2 UL
/data/cert/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt
/data/cert/Cert_294927-306462_13TeV_UL2017_Collisions17_JSON.txt
```

**Golden JSON file format** (standard CMS format):
```json
{"355100": [[1, 100], [150, 200]], "355101": [[1, 50]]}
```
Each key is a run number; the value is a list of `[lumi_min, lumi_max]` certified luminosity section ranges.

**Apply the filter in C++**:
```cpp
auto* gjm = analyzer->getPlugin<GoldenJsonManager>("goldenJson");
gjm->applyGoldenJson();
```

| Config Key | Type | Description |
|------------|------|-------------|
| `goldenJsonConfig` | Path | Text file listing golden JSON paths, one per line |

### CutflowManager Configuration

CutflowManager cuts are registered **programmatically** in your analysis C++ code. Results are written automatically to the meta ROOT file after `analyzer->run()`.

**Main config entry**:
```
# CutflowManager
# Note: CutflowManager cuts are registered programmatically.
# Results are written to the meta ROOT file automatically.
CutflowManager = CutflowManager
```

**Registered programmatically in C++**:
```cpp
// 1. Define boolean cut columns upfront (all columns needed by ANY cut
//    must exist before the first addCut() call)
analyzer->Define("pass_ptCut",  [](float pt){ return pt > 30.f; },          {"pt"});
analyzer->Define("pass_etaCut", [](float eta){ return std::abs(eta) < 2.4f; }, {"eta"});

// 2. Register cuts (also applies each filter to the dataframe)
auto* cfm = analyzer->getPlugin<CutflowManager>("cutflow");
cfm->addCut("ptCut",  "pass_ptCut");
cfm->addCut("etaCut", "pass_etaCut");

// 3. (Optional) bind to a RegionManager for per-region cutflows
cfm->bindToRegionManager(analyzer->getPlugin<RegionManager>("regions"));
```

**Outputs written to the meta ROOT file**:
- `cutflow` — `TH1D` with sequential event counts after each cut
- `cutflow_nminus1` — `TH1D` with N-1 counts (all cuts except one applied)
- `cutflow_regions` — `TH2D` (regions × cuts) when a RegionManager is bound

## Analysis YAML Configuration

In addition to the text-based config file, RDFAnalyzerCore supports an **analysis YAML config** as a newer, structured alternative for specifying regions, nuisance groups, histogram configuration, friend trees, and plugin settings.

**Main config option** (text config points to the YAML file):
```
analysisConfig=cfg/analysis.yaml
```

**YAML config capabilities** (partial list):
```yaml
plugins:
  WeightManager:
    nominal_weight: weight_nominal
    scale_factors:
      - column: pu_weight
        label: "Pileup SF"

regions:
  - name: signal
    filter: pass_signal
  - name: control
    filter: pass_control
    parent: signal

nuisance_groups:
  - name: pileup
    type: weight
    up: pu_weight_up
    down: pu_weight_down

friend_trees:
  - file: friends.root
    tree: Friends
```

The YAML config is validated at startup against a JSON Schema. See **[CONFIGURATION_VALIDATION.md](CONFIGURATION_VALIDATION.md)** for the full schema reference and validation error messages.

## Advanced Configuration

### Counter Service

Track event counts and weight sums per sample.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enableCounters` | Boolean | `false` | Enable the CounterService |
| `counterWeightBranch` | String | (empty) | Column name for per-event weights (e.g., `genWeight`) |
| `counterIntWeightBranch` | String | (empty) | Column name for integer-valued weights (e.g., stitching codes) |

**When enabled**, the CounterService logs:
- Total entries per sample
- Sum of `counterWeightBranch` (if set)
- Sum of `counterIntWeightBranch` (if set)

**For integer weights**, a histogram is written to the metadata file:
- Name: `counter_intWeightSum_<sample>`
- X-axis: Integer values from `counterIntWeightBranch`
- Bin content: Sum of `counterWeightBranch` for events with that integer value

**Example**:
```
enableCounters=true
counterWeightBranch=genWeight
counterIntWeightBranch=stitchWeight
```

### Custom ROOT Dictionaries

For processing custom C++ objects (e.g., MiniAOD-style classes), you can build ROOT dictionaries.

Configure via CMake cache variables:
```bash
cmake -S . -B build \
  -DRDF_CUSTOM_DICT_HEADERS="/path/to/MyEvent.h;/path/to/MyObject.h" \
  -DRDF_CUSTOM_DICT_LINKDEF="/path/to/MyLinkDef.h" \
  -DRDF_CUSTOM_DICT_INCLUDE_DIRS="/path/to/include" \
  -DRDF_CUSTOM_DICT_SOURCES="/path/to/src/MyEvent.cc" \
  -DRDF_CUSTOM_DICT_TARGET="MyCustomDict"
```

The dictionary is built and linked into the core library automatically.

## Configuration File Format

### General Format

Configuration files use a simple `key=value` format:
```
key1=value1
key2=value2
```

### Comments

Lines starting with `#` are treated as comments:
```
# This is a comment
saveFile=output.root  # This is also a comment
```

### Multi-Value Options

Some options accept comma-separated values:
```
fileList=file1.root,file2.root,file3.root
antiglobs=temp,test,debug
```

### Boolean Options

Boolean values can be specified as:
- `true`, `True`, `1`, `yes`, `Yes` → true
- `false`, `False`, `0`, `no`, `No` → false

### Path Handling

- Relative paths are resolved relative to the working directory
- XRootD URLs are supported for remote file access: `root://server//path/file.root`
- EOS paths: `root://eospublic.cern.ch//eos/path/file.root`

## Example Configurations

### Minimal Configuration

```
# Input
fileList=/data/input.root

# Output
saveFile=output.root

# Basic settings
threads=4
```

### Full Analysis Configuration

```
# ============== I/O Configuration ==============
fileList=root://xrootd.server//path/data1.root,root://xrootd.server//path/data2.root
saveFile=output/analysis_output.root
metaFile=output/histograms.root
saveDirectory=temp/
saveTree=Events
treeList=CollectionTree,Events

# ============== Performance ==============
threads=8
batch=false

# ============== Output Selection ==============
saveConfig=cfg/output_branches.txt

# ============== Helpers ==============
floatConfig=cfg/constants.txt
intConfig=cfg/int_constants.txt
aliasConfig=cfg/aliases.txt
optionalBranchesConfig=cfg/optional.txt

# ============== Plugins ==============
bdtConfig=cfg/bdts.txt
onnxConfig=cfg/onnx_models.txt
correctionConfig=cfg/corrections.txt
triggerConfig=cfg/triggers.txt

# ============== Counters ==============
enableCounters=true
counterWeightBranch=genWeight
counterIntWeightBranch=stitchCode

# ============== File Filtering ==============
antiglobs=test,debug,temp
globs=DAOD,PHYSLITE
```

### HTCondor Batch Configuration

```
# Input from DAS/Rucio (one file per job)
fileList=root://xrootd//path/input_%(jobnumber)s.root

# Output to EOS
saveFile=root://eosuser.cern.ch//eos/user/u/username/output_%(jobnumber)s.root
metaFile=root://eosuser.cern.ch//eos/user/u/username/hists_%(jobnumber)s.root

# Batch mode (no progress bars)
batch=true
threads=-1

# Analysis config
saveConfig=cfg/branches.txt
bdtConfig=cfg/bdts.txt
correctionConfig=cfg/corrections.txt
```

## Configuration Best Practices

1. **Organize by sections**: Group related options together with comments
2. **Use descriptive names**: Make config files self-documenting
3. **Separate plugin configs**: Keep plugin configurations in separate files
4. **Version control**: Track configurations in Git alongside your analysis code
5. **Document special values**: Add comments explaining non-obvious settings
6. **Test incrementally**: Start with minimal configs and add complexity gradually
7. **Use relative paths**: Makes configs portable across systems
8. **Template configs**: Create templates for common analysis patterns

## Debugging Configuration Issues

### Enable verbose output

Add to your C++ code:
```cpp
configProvider->dump();  // Print all loaded configuration
```

### Check loaded values

```cpp
std::cout << "saveFile: " << configProvider->get("saveFile") << std::endl;
```

### Validate plugin configs

Ensure plugin config files exist and are readable:
```bash
ls -l cfg/*.txt
```

### Common mistakes

- Missing required options (`saveFile`, `fileList`)
- Incorrect file paths (check working directory)
- Malformed plugin config lines (missing parameters)
- Type mismatches (string where boolean expected)
- Comma in paths (interpreted as multi-value separator)

## See Also

- [GETTING_STARTED.md](GETTING_STARTED.md) - Quick start guide
- [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md) - Building analyses
- [API_REFERENCE.md](API_REFERENCE.md) - C++ API documentation
- [CONFIGURATION_VALIDATION.md](CONFIGURATION_VALIDATION.md) - YAML config schema and validation
- [NUISANCE_GROUPS.md](NUISANCE_GROUPS.md) - Systematic nuisance group configuration
- [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md) - Physics object collection reference
