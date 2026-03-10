# Configuration Validation

RDFAnalyzerCore ships a configuration validation tool that catches mis-spelled keys, missing files, bad numeric values and structural inconsistencies **before** a long analysis run starts.  Fixing problems early avoids silent data-quality issues and wasted CPU time on the grid.

The tool lives in `core/python/validate_config.py` and supports two independent validation modes:

| Mode | Input | Use when |
|------|-------|----------|
| **Submit config** | `key=value` text file | validating a snapshot/analysis submission config before running |
| **Analysis config** | YAML file | validating regions, nuisances, histograms, friend trees and plugins |

---

## CLI Usage

```
python -m validate_config --config path/to/submit_config.txt [--mode {auto,nano,opendata}]
python -m validate_config --analysis-config path/to/analysis.yaml
```

The two modes are mutually exclusive; exactly one of `--config` or `--analysis-config` must be supplied.

### Options

| Flag | Description |
|------|-------------|
| `--config PATH` | Validate a submit config text file (see [Submit Config Validation](#submit-config-validation)). |
| `--analysis-config PATH` | Validate a full YAML analysis config (see [Analysis Config Validation](#analysis-config-validation)). |
| `--mode {auto,nano,opendata}` | Applies only to `--config`.  Controls how the embedded sample config is interpreted.  `auto` (default) detects the mode from the content of the sample config file. |

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | Validation passed (warnings may still be printed). |
| `1` | One or more errors were found, or the config file was not found. |

### Example output

```
Config validation warnings:
- Missing 'norm' in sample config (line 4): 'norm' will be calculated on the fly and is deprecated
Config validation OK
```

```
Config validation failed:
- Missing required key 'sampleConfig' in submit config
- File not found for 'floatConfig': /path/to/float_config.txt
```

---

## Submit Config Validation

### `validate_submit_config(config_path, mode='auto')`

Validates a `key=value` text file used to configure the snapshot / analysis submission step.

```python
from validate_config import validate_submit_config
errors, warnings = validate_submit_config("configs/submit_config.txt", mode="nano")
```

**Returns** `(errors: list[str], warnings: list[str])`.  
**Raises** `ValidationError` if `config_path` does not exist.

### What is checked

#### Required keys

The following keys must be present and non-empty in every submit config:

| Key | Purpose |
|-----|---------|
| `sampleConfig` | Path to the sample/dataset config file (text or YAML). |
| `saveDirectory` | Output directory for the snapshot/analysis results. |
| `saveTree` | Name of the ROOT tree to save. |

#### Optional file-existence checks

If the following keys are present and non-empty, the referenced files must exist on the filesystem:

| Key | Required to exist |
|-----|------------------|
| `sampleConfig` | Yes – error if absent |
| `floatConfig` | Yes – error if absent |
| `intConfig` | Yes – error if absent |
| `saveConfig` | No – warning if absent |

Paths are resolved relative to the parent directory of the submit config file.

#### `threads` value

When `threads` is present its value must be an integer, the string `auto`, or the string `max`; anything else is an error.

### Sample config validation

The sample config (pointed to by `sampleConfig`) is validated according to its format:

- **YAML (`.yaml` / `.yml`)** – loaded via `DatasetManifest.load_yaml()` and then mode-specific field checks are applied (see below).
- **Text (any other extension)** – parsed as `key=value` entries and mode-specific checks are applied.

#### Mode detection

When `--mode auto` is used (the default) the mode is inferred from the sample config content:

- **OpenData** – any entry that has a purely numeric `das` field (a CERN Open Data record ID) triggers OpenData mode.
- **NANO** – everything else.

#### NANO mode sample checks

Each sample entry is expected to contain:

| Field | Type | Requirement |
|-------|------|-------------|
| `das` | string | Required (DAS path to CMS dataset) |
| `xsec` | float | Required; must be a valid float |
| `type` | int | Required; must be a valid integer |
| `norm` | float | Deprecated; warning if absent, error if present but not a float |

#### OpenData mode sample checks

| Check | Severity |
|-------|----------|
| A `recids` entry must be present | Error |
| Each sample entry must have a `das` field (record ID) | Error |
| Numeric fields (`xsec`, `norm`, `kfac`, `extraScale`, `lumi`) must be valid floats | Error |
| `type` field must be a valid integer | Error |

---

## Analysis Config Validation

### `validate_analysis_config(config_path)`

Validates a comprehensive YAML analysis configuration file.  All validation is structural – the function does not execute any C++ code or load ROOT files.

```python
from validate_config import validate_analysis_config
errors, warnings = validate_analysis_config("configs/analysis.yaml")
```

**Returns** `(errors: list[str], warnings: list[str])`.  
**Raises** `ValidationError` if `config_path` does not exist.

The recognised top-level keys are:

| Key | Validator called |
|-----|-----------------|
| `regions` | `validate_regions_config()` |
| `nuisance_groups` | `validate_nuisance_groups_config()` |
| `histogram_config` | `validate_histogram_config_file()` |
| `friend_trees` | `validate_friend_config()` |
| `plugins` | `validate_plugin_config()` (checked against `_KNOWN_PLUGINS`) |

Unknown top-level keys generate a warning.  If both `friend_trees` and `friends` are present simultaneously, a warning is emitted and only `friend_trees` is validated.

### `validate_regions_config(regions_data, source_context='')`

Validates the `regions` list.  Each entry must be a dict with at minimum:

| Key | Requirement |
|-----|-------------|
| `name` | Required |
| `filter_column` | Required |
| `parent` | Optional (string name of a parent region) |
| `description` | Optional |

After individual entry validation the region hierarchy is checked for cycles, unknown parent references and duplicate names via `validate_region_hierarchy()`.

### `validate_nuisance_groups_config(nuisance_data, source_context='')`

Validates the `nuisance_groups` list.  Each entry must be a dict with at minimum a `name` key.  Additional checks:

- Parsed via `NuisanceGroupDefinition.from_dict()` and its own `validate()` method.
- Duplicate `name` values are errors.
- A group with no `systematics` declared generates a warning.
- Each `output_usage` value must be one of the recognised usages (e.g. `HISTOGRAM`, `DATACARD`).

### `validate_histogram_config_file(path)`

Validates a whitespace-separated histogram configuration text file.  Each non-comment, non-blank line is treated as a histogram entry.

**Required keys per entry:**

| Key | Type | Constraint |
|-----|------|------------|
| `name` | string | Unique within the file |
| `variable` | string | — |
| `bins` | int | Must be positive |
| `lowerBound` | float | Must be less than `upperBound` |
| `upperBound` | float | Must be greater than `lowerBound` |

The validator also checks the optional channel, control-region and sample-category axis keys (`channelBins`, `controlRegionBins`, `sampleCategoryBins`, etc.) for correct numeric types and bound ordering.  Unknown keys generate a warning.  Duplicate histogram names are errors.

### `validate_friend_config(friends_data, source_context='')`

Validates the `friend_trees` list.  Each entry must be a dict with:

| Key | Requirement |
|-----|-------------|
| `alias` | Required; must be unique |
| `tree_name` / `treeName` | Optional string |
| `directory` | Optional string |
| `files` / `fileList` | Optional list |
| `globs` / `antiglobs` | Optional lists |
| `index_branches` / `indexBranches` | Optional lists |

A warning is issued when an entry has neither a `files`/`fileList` source nor a `directory`.  Unknown keys generate a warning.

---

## Plugin Config Validation

The `plugins` section of an analysis config is a **mapping** (dict) from plugin name to plugin-specific configuration.  Each plugin config value may be a dict of options, `null`, or `true` (flag-style activation with no options).

### `validate_plugin_config(plugins_data, source_context='')`

Checks that each plugin name is in the known registry and that all declared config keys satisfy type constraints.  Unknown plugin names generate a warning (not an error) so that third-party or future plugins are not blocked.

### `_KNOWN_PLUGINS` registry

| Plugin | Optional keys | Key types |
|--------|--------------|-----------|
| `RegionManager` | `enabled` | `bool` |
| `WeightManager` | `nominal_weight`, `scale_factors`, `normalizations`, `weight_variations` | `str`, `list`, `list`, `list` |
| `NDHistogramManager` | `histogram_config`, `output_file`, `region_aware` | `str`, `str`, `bool` |
| `CutflowManager` | `enabled` | `bool` |
| `CorrectionManager` | `corrections` | `list` |
| `BDTManager` | `models` | `list` |
| `OnnxManager` | `models` | `list` |
| `SofieManager` | `models` | `list` |
| `TriggerManager` | `triggers` | `list` |
| `GoldenJsonManager` | `json_files` | `list` |
| `KinematicFitManager` | `fits` | `list` |
| `NamedObjectManager` | `objects` | `list` |

All plugins have an empty `required_keys` list – every key is optional.  A plugin entry can therefore be as minimal as `PluginName: true`.

---

## Programmatic Usage

Both validators are importable as a Python library:

```python
from validate_config import validate_analysis_config, validate_submit_config, ValidationError

# --- Analysis YAML config ---
try:
    errors, warnings = validate_analysis_config("configs/analysis.yaml")
except ValidationError as exc:
    print(f"File not found: {exc}")
    raise SystemExit(1)

for w in warnings:
    print(f"WARNING: {w}")
if errors:
    for e in errors:
        print(f"ERROR: {e}")
    raise SystemExit(1)
print("Analysis config is valid.")

# --- Submit config ---
try:
    errors, warnings = validate_submit_config("configs/submit_config.txt", mode="nano")
except ValidationError as exc:
    print(f"File not found: {exc}")
    raise SystemExit(1)

if errors:
    print("Submit config has errors:", errors)
```

The functions never raise on validation errors – they always return `(errors, warnings)` lists.  `ValidationError` is only raised when the input file itself cannot be found.

---

## Analysis YAML Config Format

Below is a fully annotated example covering all recognised top-level sections:

```yaml
# ------------------------------------------------------------------
# regions – list of selection regions applied by RegionManager
# ------------------------------------------------------------------
regions:
  - name: preselection          # required; unique within the file
    filter_column: isPresel     # required; boolean branch in the RDataFrame

  - name: signal_region
    filter_column: isSignalRegion
    parent: preselection        # optional; must name another region in this list
    description: "Main SR"      # optional human-readable label

  - name: control_region_ttbar
    filter_column: isCRttbar
    parent: preselection

# ------------------------------------------------------------------
# nuisance_groups – systematic uncertainty groups for downstream tools
# ------------------------------------------------------------------
nuisance_groups:
  - name: JES                   # required; unique within the file
    group_type: SHAPE           # e.g. SHAPE or NORM
    systematics:
      - JES_up
      - JES_down
    processes: [ttbar, WJets]   # optional process filter
    regions: [signal_region]    # optional region filter
    output_usage:               # what outputs consume this group
      - HISTOGRAM
      - DATACARD

# ------------------------------------------------------------------
# histogram_config – path to a whitespace-separated histogram spec
# (resolved relative to the directory containing this YAML file)
# ------------------------------------------------------------------
histogram_config: histograms.txt

# ------------------------------------------------------------------
# friend_trees – sidecar ROOT trees merged at runtime
# ------------------------------------------------------------------
friend_trees:
  - alias: scores               # required; unique identifier
    tree_name: Events           # optional; defaults to the main tree name
    directory: /path/to/scores/ # optional; scanned for matching files
    globs:                      # optional; shell globs applied inside directory
      - "*.root"
    index_branches:             # optional; branches used to align the friend tree
      - run
      - luminosityBlock
      - event

# ------------------------------------------------------------------
# plugins – map of plugin name → config (all keys optional)
# ------------------------------------------------------------------
plugins:
  RegionManager:                # activates RegionManager with default settings
    enabled: true

  WeightManager:
    nominal_weight: "genWeight * puWeight"
    scale_factors: []
    normalizations: []
    weight_variations: []

  NDHistogramManager:
    histogram_config: histograms.txt
    output_file: histograms.root
    region_aware: true

  CutflowManager:
    enabled: true

  CorrectionManager:
    corrections:
      - { name: pileup, file: corrections/pileup.json, set: Collisions18_UltraLegacy }

  BDTManager:
    models:
      - { path: models/bdt_signal.pkl, output_branch: bdt_score }

  OnnxManager:
    models:
      - { path: models/nn_classifier.onnx, output_branch: nn_score }

  TriggerManager:
    triggers:
      - HLT_IsoMu27
      - HLT_Ele32_WPTight_Gsf

  GoldenJsonManager:
    json_files:
      - data/Cert_Run2018_14TeV_UL.json

  KinematicFitManager:
    fits:
      - { name: top_mass_fit, type: ttbar }

  NamedObjectManager:
    objects:
      - { name: jet_selector, type: JetSelector }
```
