# Nuisance Groups and Variation Orchestration

This document describes the nuisance group system and its variation orchestration
layer in RDFAnalyzerCore.  These two modules—`nuisance_groups.py` and
`variation_orchestrator.py`—provide a structured, queryable registry of systematic
uncertainty groups and a unified API for determining which variation columns apply
for any given `(process, region, output_usage)` context.

---

## Table of Contents

1. [Overview](#1-overview)
2. [NuisanceGroupType Enum](#2-nuisancegrouptype-enum)
3. [NuisanceGroupOutputUsage Enum](#3-nuisancegroupoutputusage-enum)
4. [NuisanceGroup Dataclass](#4-nuisancegroup-dataclass)
5. [NuisanceGroupRegistry](#5-nuisancegroupregistry)
6. [Coverage Validation](#6-coverage-validation)
7. [VariationOrchestrator](#7-variationorchestrator)
8. [Integration with Analysis Config YAML](#8-integration-with-analysis-config-yaml)
9. [Usage Examples](#9-usage-examples)

---

## 1. Overview

In HEP analyses, *systematic uncertainties* are grouped into *nuisance parameters*
that are propagated through the statistical model (e.g. a CMS-Combine datacard).
Each nuisance parameter is associated with a set of *shape* or *rate* variations:
an up-shifted and a down-shifted copy of the affected histogram or normalisation
factor.

The nuisance group system provides:

- **NuisanceGroup** – a named collection of related systematic variations with
  metadata: type, applicable processes, applicable regions, and which downstream
  outputs consume it.
- **NuisanceGroupRegistry** – a container that stores all declared nuisance groups
  and supports efficient filtering queries (by process, region, output type, or
  group type) and coverage validation.
- **VariationOrchestrator** – a higher-level object that combines the nuisance
  registry with weight-variation specifications to produce a complete
  `VariationPlan` for any `(process, region, output_usage)` context, enabling
  downstream tools (histogram filling, datacard generation, plotting) to book all
  required variations in a single RDF pass without re-deriving applicability.

The framework follows the **"systematics are columns"** convention: each systematic
variation is represented as a dedicated dataframe column (e.g. `JESUp`, `JESDown`).
The nuisance group system maps logical group names to the expected column names but
does not modify the dataframe itself.

---

## 2. NuisanceGroupType Enum

```python
class NuisanceGroupType(str, enum.Enum):
    SHAPE         = "shape"
    RATE          = "rate"
    NORMALIZATION = "normalization"
    OTHER         = "other"
```

| Value           | Description |
|-----------------|-------------|
| `SHAPE`         | The group contributes a shape uncertainty: the histogram morphing differs between the up and down shifts. |
| `RATE`          | The group contributes only a rate (normalisation) uncertainty; the histogram shape is unchanged. |
| `NORMALIZATION` | Alias for `RATE`, kept for compatibility with existing YAML configs that use the `"normalization"` label. |
| `OTHER`         | Catch-all for custom or non-standard variation types. |

Because `NuisanceGroupType` inherits from `str`, its values can be compared
directly to plain strings (e.g. `group.group_type == "shape"`).

---

## 3. NuisanceGroupOutputUsage Enum

```python
class NuisanceGroupOutputUsage(str, enum.Enum):
    HISTOGRAM = "histogram"
    DATACARD  = "datacard"
    PLOT      = "plot"
```

| Value       | Description |
|-------------|-------------|
| `HISTOGRAM` | Variations from this group are used when filling analysis histograms. |
| `DATACARD`  | Variations from this group appear in CMS-Combine datacards. |
| `PLOT`      | Variations from this group are rendered on analysis plots. |

An empty `output_usage` list on a `NuisanceGroup` means the group is consumed by
**all** outputs.

---

## 4. NuisanceGroup Dataclass

```python
from dataclasses import dataclass, field
from typing import List

@dataclass
class NuisanceGroup:
    name:              str
    group_type:        str       = "shape"  # one of VALID_GROUP_TYPES
    systematics:       List[str] = field(default_factory=list)
    processes:         List[str] = field(default_factory=list)
    regions:           List[str] = field(default_factory=list)
    output_usage:      List[str] = field(default_factory=list)
    description:       str       = ""
    correlation_group: str       = ""
```

### Fields

| Field               | Type         | Default    | Description |
|---------------------|--------------|------------|-------------|
| `name`              | `str`        | —          | Unique identifier for the group (e.g. `"jet_energy_scale"`). |
| `group_type`        | `str`        | `"shape"`  | Classification; must be one of `VALID_GROUP_TYPES` (`"shape"`, `"rate"`, `"normalization"`, `"other"`). |
| `systematics`       | `List[str]`  | `[]`       | Base names of the systematic variations in this group (e.g. `["JES", "JER"]`). The framework expects `<name>Up` and `<name>Down` column names. |
| `processes`         | `List[str]`  | `[]`       | Physics processes / samples this group applies to. An **empty list** means the group applies to **all** processes. |
| `regions`           | `List[str]`  | `[]`       | Analysis regions this group applies to. An **empty list** means the group applies to **all** regions. |
| `output_usage`      | `List[str]`  | `[]`       | Downstream tools that should consume this group. Each entry must be one of `VALID_OUTPUT_USAGES`. An **empty list** means **all** outputs. |
| `description`       | `str`        | `""`       | Optional human-readable description of the group. |
| `correlation_group` | `str`        | `""`       | Optional label for grouping correlated systematics across multiple `NuisanceGroup` objects (e.g. `"lumi_correlated"`). Empty string means no explicit correlation labelling. |

> **Note**: `group_type` and each `output_usage` entry are stored as plain strings
> and validated against `VALID_GROUP_TYPES` / `VALID_OUTPUT_USAGES` by
> `NuisanceGroup.validate()`.  String values from `NuisanceGroupType` and
> `NuisanceGroupOutputUsage` (e.g. `NuisanceGroupType.SHAPE.value`) may be used
> interchangeably.

### Methods

#### `applies_to_process(process: str) → bool`

Returns `True` when this group applies to *process*.  A group with an empty
`processes` list applies to all processes.

```python
group.applies_to_process("signal")   # True if "signal" in group.processes or processes is empty
```

#### `applies_to_region(region: str) → bool`

Returns `True` when this group applies to *region*.  A group with an empty
`regions` list applies to all regions.

#### `used_for_output(usage: str) → bool`

Returns `True` when this group is consumed by the downstream tool identified by
*usage* (e.g. `"datacard"`).  A group with an empty `output_usage` list is
consumed by all outputs.

#### `validate() → List[str]`

Returns a list of validation error strings (empty list = valid).  Checks that:

- `name` is not empty.
- `group_type` is one of `VALID_GROUP_TYPES`.
- Every entry in `output_usage` is one of `VALID_OUTPUT_USAGES`.

#### `to_dict() → Dict[str, Any]`

Serialises the group to a plain Python dict via `dataclasses.asdict`.

#### `from_dict(data: Dict[str, Any]) → NuisanceGroup`  _(classmethod)_

Constructs a `NuisanceGroup` from a dict, silently ignoring unknown keys.

---

## 5. NuisanceGroupRegistry

`NuisanceGroupRegistry` stores and queries a collection of `NuisanceGroup` objects.

```python
class NuisanceGroupRegistry:
    def __init__(self, groups: Optional[List[NuisanceGroup]] = None) -> None: ...
```

### Adding Groups

#### `add_group(group: NuisanceGroup) → None`

Appends *group* to the registry.  Raises `ValueError` if a group with the same
`name` already exists.

### Properties

#### `groups → List[NuisanceGroup]`

A read-only copy of all registered groups.

### Query Methods

All query methods return lists of `NuisanceGroup` objects.  The empty-list
semantics of `processes`, `regions`, and `output_usage` (meaning "all") are
respected automatically.

| Method | Description |
|--------|-------------|
| `get_groups_for_process(process: str)` | All groups that apply to *process*. |
| `get_groups_for_region(region: str)` | All groups that apply to *region*. |
| `get_groups_for_output(usage: str)` | All groups consumed by *usage* (e.g. `"datacard"`). |
| `get_groups_by_type(group_type: str)` | All groups whose `group_type` matches *group_type*. |

#### `get_systematics_for_process_and_region(process, region, output_usage=None) → Dict[str, NuisanceGroup]`

Returns a mapping of `systematic_name → NuisanceGroup` for all systematics that
apply in the given `(process, region)` combination, optionally filtered to
*output_usage*.  When the same systematic appears in multiple groups, only the
first match is returned.

```python
syst_map = registry.get_systematics_for_process_and_region(
    process="signal",
    region="signal_region",
    output_usage="histogram",
)
# syst_map == {"JES": <NuisanceGroup 'jet_energy'>, "JER": <NuisanceGroup 'jet_energy'>, ...}
```

### Validation Methods

#### `validate() → List[str]`

Validates all `NuisanceGroup` definitions in the registry.  Returns a list of
error strings (empty = all valid).  Checks for duplicate group names and runs
`NuisanceGroup.validate()` on each group.

#### `validate_coverage(available_variations: Dict[str, List[str]]) → List[CoverageIssue]`

Checks that all declared systematics have both up and down shifts present in the
analysis output.

**Parameters:**

- `available_variations`: a mapping of base systematic name (e.g. `"JES"`) to
  the list of actual variation column or histogram names found in the output
  (e.g. `["JESUp", "JESDown"]`).  Column names are compared case-insensitively
  on the direction suffix (`Up` / `Down`).

**Returns:** a list of `CoverageIssue` objects.  An empty list means complete
up+down coverage for all declared systematics.  Possible error conditions:

- The systematic is **not present at all** in `available_variations`.
- The systematic has **neither Up nor Down** variation despite being present.
- The systematic is **missing the Up** variation only.
- The systematic is **missing the Down** variation only.
- A nuisance group has **no systematics declared** (reported as a `WARNING`).

### Serialisation and I/O

#### `to_dict() → Dict[str, Any]`

Serialises the registry to `{"groups": [...]}`.

#### `from_dict(data) → NuisanceGroupRegistry`  _(classmethod)_

Constructs from a dict (as produced by `to_dict()`).

#### `from_config(config: Dict[str, Any]) → NuisanceGroupRegistry`  _(classmethod)_

Constructs from a user-supplied configuration dict.  Two formats are recognised:

1. **Groups list format** — the dict has a top-level `"groups"` key containing a
   list of group dicts (same format as `from_dict`).  This takes precedence.

2. **Flat systematics dict format** — the dict has a top-level `"systematics"` key
   using the flat dictionary format also accepted by `DatacardGenerator`.  Each
   key is treated as both the group name and its single systematic; `applies_to`
   sub-key maps to `processes`.

#### `save_yaml(path: str) → None`

Writes the registry to a YAML file at *path*.

#### `load_yaml(path: str) → NuisanceGroupRegistry`  _(classmethod)_

Loads a registry from a YAML file.

### YAML File Format

The canonical YAML format (written by `save_yaml` / read by `load_yaml`):

```yaml
groups:
  - name: jet_energy_scale
    group_type: shape
    systematics:
      - JES
      - JER
    processes:
      - signal
      - ttbar
    regions:
      - signal_region
      - control_ttbar
    output_usage:
      - histogram
      - datacard
    description: "Jet energy scale and resolution uncertainties"
    correlation_group: ""

  - name: luminosity
    group_type: rate
    systematics:
      - lumi
    processes: []          # applies to all processes
    regions: []            # applies to all regions
    output_usage:
      - datacard
    description: "Integrated luminosity uncertainty"
    correlation_group: lumi_correlated

  - name: pileup_reweight
    group_type: shape
    systematics:
      - PU
    processes: []
    regions: []
    output_usage: []       # consumed by all outputs
    description: "Pileup reweighting uncertainty"
    correlation_group: ""
```

---

## 6. Coverage Validation

### CoverageSeverity Enum

```python
class CoverageSeverity(str, enum.Enum):
    WARNING = "warning"
    ERROR   = "error"
```

- `ERROR`: the issue prevents the output from being used (e.g. a missing
  systematic variation column).
- `WARNING`: the issue should be reviewed but is not immediately fatal (e.g. a
  nuisance group that declares no systematics).

### CoverageIssue Dataclass

```python
@dataclass
class CoverageIssue:
    severity:        CoverageSeverity
    group_name:      str
    systematic_name: str   # empty when the issue applies to the whole group
    message:         str
```

| Field             | Description |
|-------------------|-------------|
| `severity`        | `ERROR` or `WARNING`. |
| `group_name`      | Name of the `NuisanceGroup` that has the issue. |
| `systematic_name` | Name of the individual systematic within the group, or empty string when the issue applies to the whole group (e.g. empty systematics list). |
| `message`         | Human-readable description of the issue. |

`CoverageIssue` provides `to_dict()` and `from_dict()` for serialisation.

---

## 7. VariationOrchestrator

The `VariationOrchestrator` (in `variation_orchestrator.py`) is the higher-level
API that combines nuisance groups with weight variations and answers the question:
*"For this process, region, and output type, which variation columns are required?"*

### Supporting Dataclasses

#### `WeightVariationSpec`

Specification for a single named weight variation (as opposed to a shape-varying
systematic column).

```python
@dataclass
class WeightVariationSpec:
    name:           str
    nominal_column: str
    up_column:      str
    down_column:    str
    processes:      List[str] = []    # empty = all processes
    regions:        List[str] = []    # empty = all regions
    output_usage:   List[str] = []    # empty = all outputs
```

| Field            | Description |
|------------------|-------------|
| `name`           | Logical name for this weight variation (e.g. `"btag"`). |
| `nominal_column` | Name of the nominal weight column used with this variation (e.g. `"weight_nominal"`). |
| `up_column`      | Name of the up-shifted weight column (e.g. `"weight_btagUp"`). |
| `down_column`    | Name of the down-shifted weight column (e.g. `"weight_btagDown"`). |
| `processes`      | Applicable processes; empty list means all. |
| `regions`        | Applicable regions; empty list means all. |
| `output_usage`   | Applicable outputs; empty list means all. |

Helper methods: `applies_to_process(process)`, `applies_to_region(region)`,
`used_for_output(usage)`, `required_columns() → List[str]` (returns
`[up_column, down_column]`, omitting empties).

#### `SystematicVariationSpec`

Specification for a single systematic variation derived from a nuisance group.

```python
@dataclass
class SystematicVariationSpec:
    base_name:   str
    up_column:   str          # default: f"{base_name}Up"
    down_column: str          # default: f"{base_name}Down"
    group_name:  str
    group_type:  str = "shape"
```

| Field         | Description |
|---------------|-------------|
| `base_name`   | Base systematic name (e.g. `"JES"`). |
| `up_column`   | Expected dataframe column for the up shift (e.g. `"JESUp"`). |
| `down_column` | Expected dataframe column for the down shift (e.g. `"JESDown"`). |
| `group_name`  | Name of the owning `NuisanceGroup`. |
| `group_type`  | Type of the owning group (`"shape"`, `"rate"`, etc.). |

Helper method: `required_columns() → List[str]` (returns `[up_column, down_column]`).

#### `VariationPlan`

The complete set of variations for a `(process, region, output_usage)` context,
produced by `VariationOrchestrator.get_variation_plan()`.

```python
@dataclass
class VariationPlan:
    process:               str
    region:                str
    output_usage:          str
    nominal_weight:        str
    systematic_variations: List[SystematicVariationSpec] = []
    weight_variations:     List[WeightVariationSpec]     = []
```

| Field                   | Description |
|-------------------------|-------------|
| `process`               | Physics process / sample name. |
| `region`                | Analysis region name. |
| `output_usage`          | Downstream tool for which this plan was created. |
| `nominal_weight`        | Name of the nominal weight column (empty string = unweighted). |
| `systematic_variations` | Ordered list of shape/rate systematic variation specs. |
| `weight_variations`     | Ordered list of weight variation specs applicable in this context. |

Helper methods:

- `all_required_columns() → List[str]` — deduplicated list of all dataframe
  columns needed (nominal weight + all systematic + all weight variation columns).
  Suitable for a pre-flight column check before booking histograms.
- `systematic_variation_names() → List[str]` — base names of all systematic
  variations.
- `weight_variation_names() → List[str]` — names of all weight variations.
- `to_dict() → Dict[str, Any]` — serialises the plan.

#### `MissingVariationReport`

A coverage finding for a single systematic or weight variation, produced by
`VariationOrchestrator.validate_coverage()`.

```python
@dataclass
class MissingVariationReport:
    process:      str
    region:       str
    group_name:   str      # "(weight) <name>" for weight variations
    variable:     str
    missing_up:   bool = False
    missing_down: bool = False
    not_found:    bool = False
    severity:     str  = "error"    # "error" or "warn"
```

#### `MissingSeverity` Enum

```python
class MissingSeverity(str, enum.Enum):
    WARN  = "warn"
    ERROR = "error"
```

Controls how missing-variation findings are classified in the orchestrator.

### VariationOrchestrator Class

```python
class VariationOrchestrator:
    def __init__(
        self,
        nuisance_registry: NuisanceGroupRegistry,
        weight_variations: Optional[List[WeightVariationSpec]] = None,
        nominal_weight:    str = "",
        missing_severity:  MissingSeverity | str = MissingSeverity.ERROR,
    ) -> None: ...
```

**Parameters:**

| Parameter          | Description |
|--------------------|-------------|
| `nuisance_registry` | Registry of all declared nuisance groups. |
| `weight_variations` | Optional list of weight-variation specs. |
| `nominal_weight`    | Name of the nominal event-weight column (empty string = unweighted). |
| `missing_severity`  | Default severity for missing-variation findings (`"error"` or `"warn"`). |

**Properties:**

| Property            | Description |
|---------------------|-------------|
| `nuisance_registry` | The backing `NuisanceGroupRegistry`. |
| `weight_variations` | Read-only copy of registered `WeightVariationSpec` objects. |
| `nominal_weight`    | Name of the global nominal weight column. |
| `missing_severity`  | Default severity for missing-variation findings. |

#### `add_weight_variation(spec: WeightVariationSpec) → None`

Registers an additional `WeightVariationSpec` after construction.

#### `get_variation_plan(process, region, output_usage) → VariationPlan`

Builds the complete `VariationPlan` for the given `(process, region, output_usage)`
context.

- Queries the nuisance registry with
  `get_systematics_for_process_and_region(process, region, output_usage)` to build
  the list of `SystematicVariationSpec` objects.  Up/down column names are derived
  as `f"{base_name}Up"` / `f"{base_name}Down"`.
- Filters `weight_variations` to those applicable to the context.
- Returns a `VariationPlan` ready for consumption by histogram-filling or
  datacard-generation code.

#### `validate_coverage(available_columns, processes=None, regions=None, output_usage=None, severity=None) → List[MissingVariationReport]`

Validates that all required variation columns are present in *available_columns*
for every `(process, region)` pair in the Cartesian product of *processes* × *regions*.

**Parameters:**

- `available_columns`: sequence of column names present in the dataframe or output.
- `processes`: processes to check.  `None` infers from the registry.
- `regions`: regions to check.  `None` infers from the registry.
- `output_usage`: restrict the check to groups that apply to this output.
- `severity`: override the default `missing_severity` for this call.

Returns one `MissingVariationReport` per missing or incomplete variation.

#### `build_validation_report(report, available_columns, processes=None, regions=None, output_usage=None, severity=None) → None`

Populates a `ValidationReport` (from `validation_report.py`) with
`NuisanceGroupCoverageEntry` records—one per nuisance group per
`(process, region)` pair—and appends free-form error or warning messages for
each missing variation.  Integrates directly with the validation reporting
pipeline.

---

## 8. Integration with Analysis Config YAML

Nuisance groups are typically declared in the analysis configuration YAML under a
`nuisance_groups` key.  The `NuisanceGroupRegistry.from_config()` classmethod
accepts two formats.

### Groups List Format (recommended)

```yaml
nuisance_groups:
  groups:
    - name: jet_energy_scale
      group_type: shape
      systematics: [JES, JER]
      processes: [signal, ttbar, wjets]
      regions: []                       # all regions
      output_usage: [histogram, datacard]
      description: "Jet energy scale and resolution"
      correlation_group: ""

    - name: b_tagging
      group_type: shape
      systematics: [btagSF_bc, btagSF_light]
      processes: []                     # all processes
      regions: [signal_region, control_ttbar]
      output_usage: [histogram, datacard, plot]
      description: "b-tagging scale factor uncertainties"
      correlation_group: ""

    - name: luminosity
      group_type: rate
      systematics: [lumi]
      processes: []
      regions: []
      output_usage: [datacard]
      description: "Integrated luminosity uncertainty (2.5%)"
      correlation_group: lumi_run3_corr
```

### Flat Systematics Format (datacard-compatible)

```yaml
nuisance_groups:
  systematics:
    JES:
      type: shape
      applies_to:
        signal: true
        ttbar: true
      regions: [signal_region]
      output_usage: [datacard, histogram]
      description: "Jet energy scale"

    lumi:
      type: rate
      applies_to: {}           # empty = all processes
      output_usage: [datacard]
      description: "Luminosity"
```

In Python:

```python
from nuisance_groups import NuisanceGroupRegistry

with open("analysis_config.yaml") as f:
    config = yaml.safe_load(f)

registry = NuisanceGroupRegistry.from_config(config["nuisance_groups"])
```

---

## 9. Usage Examples

### Creating a Registry and Querying It

```python
from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry

# Build a registry programmatically
registry = NuisanceGroupRegistry()

registry.add_group(NuisanceGroup(
    name="jet_energy_scale",
    group_type="shape",
    systematics=["JES", "JER"],
    processes=["signal", "ttbar"],
    regions=["signal_region", "control_ttbar"],
    output_usage=["histogram", "datacard"],
    description="Jet energy scale and resolution uncertainties",
))

registry.add_group(NuisanceGroup(
    name="luminosity",
    group_type="rate",
    systematics=["lumi"],
    processes=[],     # all processes
    regions=[],       # all regions
    output_usage=["datacard"],
    description="Integrated luminosity uncertainty",
    correlation_group="lumi_run3",
))

# Query
datacard_groups = registry.get_groups_for_output("datacard")
signal_groups   = registry.get_groups_for_process("signal")
shape_groups    = registry.get_groups_by_type("shape")

print(f"Datacard groups: {[g.name for g in datacard_groups]}")
# Datacard groups: ['jet_energy_scale', 'luminosity']
```

### Loading from and Saving to YAML

```python
# Load
registry = NuisanceGroupRegistry.load_yaml("nuisance_groups.yaml")

# Inspect
for group in registry.groups:
    print(f"{group.name}: {group.systematics}")

# Save
registry.save_yaml("nuisance_groups_updated.yaml")
```

### Validating Coverage

```python
# available_variations maps base name → list of actual column/histogram names
available = {
    "JES":  ["JESUp", "JESDown"],
    "JER":  ["JERUp"],            # missing Down!
    "lumi": ["lumiUp", "lumiDown"],
}

issues = registry.validate_coverage(available)
for issue in issues:
    print(f"[{issue.severity}] {issue.group_name}/{issue.systematic_name}: {issue.message}")

# [error] jet_energy_scale/JER: Systematic 'JER' in group 'jet_energy_scale'
#         is missing the Down variation.
```

### Creating a VariationOrchestrator and Getting a VariationPlan

```python
from nuisance_groups import NuisanceGroupRegistry
from variation_orchestrator import VariationOrchestrator, WeightVariationSpec

registry = NuisanceGroupRegistry.load_yaml("nuisance_groups.yaml")

weight_vars = [
    WeightVariationSpec(
        name="btag",
        nominal_column="weight_nominal",
        up_column="weight_btagUp",
        down_column="weight_btagDown",
        processes=["signal", "ttbar"],
        regions=[],          # all regions
        output_usage=["histogram"],
    ),
    WeightVariationSpec(
        name="pileup",
        nominal_column="weight_nominal",
        up_column="weight_puUp",
        down_column="weight_puDown",
    ),
]

orch = VariationOrchestrator(
    nuisance_registry=registry,
    weight_variations=weight_vars,
    nominal_weight="weight_nominal",
)

# Get the plan for a specific context
plan = orch.get_variation_plan(
    process="signal",
    region="signal_region",
    output_usage="histogram",
)

print(f"Nominal weight : {plan.nominal_weight}")
print(f"Systematic variations ({len(plan.systematic_variations)}):")
for sv in plan.systematic_variations:
    print(f"  {sv.base_name:20s}  up={sv.up_column}  down={sv.down_column}  [{sv.group_type}]")

print(f"Weight variations ({len(plan.weight_variations)}):")
for wv in plan.weight_variations:
    print(f"  {wv.name:20s}  up={wv.up_column}  down={wv.down_column}")

# Pre-flight column check
required_cols = plan.all_required_columns()
print(f"All required columns ({len(required_cols)}): {required_cols}")
```

### Validating Coverage with the Orchestrator

```python
# df_columns: the list of columns in the ROOT DataFrame or output histogram file
df_columns = ["JESUp", "JESDown", "lumiUp", "lumiDown",
              "weight_nominal", "weight_btagUp", "weight_btagDown",
              "weight_puUp"]  # weight_puDown missing!

findings = orch.validate_coverage(
    available_columns=df_columns,
    processes=["signal", "ttbar"],
    regions=["signal_region", "control_ttbar"],
    output_usage="histogram",
)

for f in findings:
    print(f"[{f.severity}] {f.process}/{f.region} - {f.group_name}/{f.variable}: "
          f"missing_up={f.missing_up} missing_down={f.missing_down}")
```

### Populating a ValidationReport

```python
from validation_report import ValidationReport
from variation_orchestrator import VariationOrchestrator

report = ValidationReport(stage="histogram")

orch.build_validation_report(
    report=report,
    available_columns=df_columns,
    processes=["signal", "ttbar"],
    regions=["signal_region", "control_ttbar"],
    output_usage="histogram",
)

print(report.to_text())
if report.has_errors:
    report.save_yaml("reports/histogram_validation.yaml")
```
