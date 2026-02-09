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

The `saveConfig` file contains one branch name per line:
```
branch1
branch2
branch3
```

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

**Note**: SOFIE models must be manually registered via `registerModel()` in C++ code before calling `applyModel()`.

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
