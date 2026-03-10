# Validation and Reproducibility Reports

This document describes the two structured report systems in RDFAnalyzerCore:
`ValidationReport` (post-production validation diagnostics) and
`ReproducibilityReport` (provenance-driven audit trail).  Both are implemented in
the Python modules `core/python/validation_report.py` and
`core/python/reproducibility_report.py` respectively.

---

## Table of Contents

1. [Overview](#1-overview)

**Part A: ValidationReport**

2. [ValidationReport Overview](#2-validationreport-overview)
3. [Report Entry Types](#3-report-entry-types)
4. [ValidationReport Class](#4-validationreport-class)
5. [generate_report_from_manifest()](#5-generate_report_from_manifest)
6. [CLI Usage — validation_report.py](#6-cli-usage--validation_reportpy)

**Part B: ReproducibilityReport**

7. [ReproducibilityReport Overview](#7-reproducibilityreport-overview)
8. [Provenance Sections](#8-provenance-sections)
9. [ReproducibilityReport Class](#9-reproducibilityreport-class)
10. [CLI Usage — reproducibility_report.py](#10-cli-usage--reproducibility_reportpy)
11. [Integration with ProvenanceService](#11-integration-with-provenanceservice)
12. [Usage Examples](#12-usage-examples)

---

## 1. Overview

Reproducibility and correctness are critical in HEP analyses, where complex
multi-stage processing pipelines, many systematic uncertainties, and large datasets
make silent failures easy to miss and hard to diagnose.  RDFAnalyzerCore provides
two complementary report systems:

- **`ValidationReport`** — a structured post-production audit that captures event
  counts, cutflows, missing branches, configuration mismatches, systematic
  coverage, weight statistics, output file integrity, region definitions, and
  nuisance-group coverage.  It is designed to be generated and checked after each
  production stage.

- **`ReproducibilityReport`** — a provenance-driven record that captures all
  build-time, runtime, and configuration information needed to reproduce or
  cross-check an analysis run: git hashes, compiler versions, ROOT version,
  container tags, configuration hashes, input file hashes, dataset manifest
  information, and per-plugin entries.

Both reports support serialisation to machine-readable (JSON and YAML) and
human-readable (plain text) formats, and both have CLI entry points for
command-line use.

---

## Part A: ValidationReport

## 2. ValidationReport Overview

`ValidationReport` (in `core/python/validation_report.py`) is the structured
validation and audit report for a single production stage.  It aggregates
diagnostics produced during validation and exposes a summary `has_errors` /
`has_warnings` boolean pair for CI/CD gates.

**When to use it:**

- After any production stage (skim, histogram filling, datacard generation) to
  confirm expected event counts, check that all systematic variation columns are
  present, and verify output file integrity.
- Automatically via `generate_report_from_manifest()` to bootstrap a report from
  an `OutputManifest`.
- Programmatically by calling individual `add_*` methods to record diagnostics
  from custom validation code.
- Via `VariationOrchestrator.build_validation_report()` to fill in nuisance-group
  coverage details.

---

## 3. Report Entry Types

### `EventCountEntry`

Event count record for a single sample at a single production stage.

```python
@dataclass
class EventCountEntry:
    sample:          str
    stage:           str
    total_events:    int
    selected_events: Optional[int]   = None
    efficiency:      Optional[float] = None   # auto-computed if None
```

| Field             | Description |
|-------------------|-------------|
| `sample`          | Dataset / sample identifier (e.g. `"ttbar"`). |
| `stage`           | Production stage name (e.g. `"preselection"`, `"skim"`). |
| `total_events`    | Total number of input events before any selection. |
| `selected_events` | Events surviving the stage selection.  `None` if not recorded. |
| `efficiency`      | `selected_events / total_events`.  Computed automatically in `__post_init__` when both counts are available and this field is `None`. |

---

### `CutflowEntry`

A single step in a cutflow table.

```python
@dataclass
class CutflowEntry:
    cut_name:             str
    events_passed:        int
    events_cumulative:    Optional[int]   = None
    relative_efficiency:  Optional[float] = None
    cumulative_efficiency: Optional[float] = None
```

| Field                  | Description |
|------------------------|-------------|
| `cut_name`             | Human-readable name of the selection cut. |
| `events_passed`        | Absolute number of events that passed this cut. |
| `events_cumulative`    | Cumulative events remaining after all cuts up to this one.  `None` if not recorded. |
| `relative_efficiency`  | Fraction passing this cut relative to the previous cut.  `None` if not recorded. |
| `cumulative_efficiency` | Fraction passing all cuts up to this cut relative to the first cut.  `None` if not recorded. |

---

### `MissingBranchEntry`

Record of branches expected in a schema but absent from the output.

```python
@dataclass
class MissingBranchEntry:
    artifact_role:    str
    expected_branches: List[str]
    missing_branches:  List[str]
```

| Field              | Description |
|--------------------|-------------|
| `artifact_role`    | The schema role (e.g. `"skim"`, `"intermediate_artifacts[0]"`). |
| `expected_branches` | Complete list of branches declared in the schema. |
| `missing_branches` | Subset of `expected_branches` not found in the output. |

**Property:** `is_complete → bool` — `True` when `missing_branches` is empty.

---

### `ConfigMismatchEntry`

A single configuration key whose value differs from what was expected.

```python
@dataclass
class ConfigMismatchEntry:
    key:      str
    expected: Any
    actual:   Any
    severity: ReportSeverity = ReportSeverity.ERROR
```

| Field      | Description |
|------------|-------------|
| `key`      | Configuration parameter name. |
| `expected` | Value declared in the schema or reference configuration. |
| `actual`   | Value found in the running job or output artifact. |
| `severity` | Severity of this mismatch (`INFO`, `WARNING`, or `ERROR`). |

Provides `to_dict()` and `from_dict()`.

---

### `SystematicEntry`

Coverage record for a single systematic variation.

```python
from dataclasses import dataclass, field
from typing import List

@dataclass
class SystematicEntry:
    systematic_name:  str
    has_up:           bool       = False
    has_down:         bool       = False
    extra_variations: List[str]  = field(default_factory=list)
```

| Field              | Description |
|--------------------|-------------|
| `systematic_name`  | Name of the systematic variation (e.g. `"JES"`, `"PU"`). |
| `has_up`           | Whether the up-shift variation is present in the output. |
| `has_down`         | Whether the down-shift variation is present in the output. |
| `extra_variations` | Any additional variation tags beyond `"up"` and `"down"`. |

**Property:** `is_complete → bool` — `True` when both `has_up` and `has_down` are `True`.

---

### `WeightSummaryEntry`

Weight statistics for a single sample.

```python
@dataclass
class WeightSummaryEntry:
    sample:              str
    sum_weights:         float
    sum_weights_squared: Optional[float] = None
    n_events:            Optional[int]   = None
    n_negative_weights:  Optional[int]   = None
    min_weight:          Optional[float] = None
    max_weight:          Optional[float] = None
```

| Field                | Description |
|----------------------|-------------|
| `sample`             | Dataset / sample identifier. |
| `sum_weights`        | Sum of event weights. |
| `sum_weights_squared` | Sum of squared event weights (for uncertainty estimates). |
| `n_events`           | Total events used to compute the weight sum. |
| `n_negative_weights` | Number of events with a negative weight. |
| `min_weight`         | Minimum event weight observed. |
| `max_weight`         | Maximum event weight observed. |

---

### `OutputIntegrityEntry`

Integrity record for a single declared output artifact.

```python
@dataclass
class OutputIntegrityEntry:
    artifact_role: str
    path:          str
    exists:        Optional[bool]  = None
    size_bytes:    Optional[int]   = None
    issues:        List[str]       = []
```

| Field           | Description |
|-----------------|-------------|
| `artifact_role` | Schema role (e.g. `"skim"`, `"cutflow"`). |
| `path`          | Expected filesystem path. |
| `exists`        | `True` if found on disk, `False` if absent, `None` if not checked. |
| `size_bytes`    | File size in bytes.  `None` when not available. |
| `issues`        | Structural or format issues (e.g. `"File is empty (0 bytes)"`). |

**Property:** `is_ok → bool` — `True` when the file exists (or was not checked)
and `issues` is empty.

---

### `RegionEntry`

Validation record for a single declared analysis region.

```python
@dataclass
class RegionEntry:
    region_name:   str
    filter_column: str
    parent:        str       = ""
    is_valid:      bool      = True
    issues:        List[str] = []
    covered_by:    List[str] = []
```

| Field           | Description |
|-----------------|-------------|
| `region_name`   | Unique name of the region (e.g. `"signal"`, `"control_wjets"`). |
| `filter_column` | Name of the boolean dataframe column that selects events in this region. |
| `parent`        | Name of the parent region, or empty string for a root region. |
| `is_valid`      | `True` when the region passed all validation checks. |
| `issues`        | Validation error or warning strings. |
| `covered_by`    | Artifact roles that reference this region (e.g. `["histograms", "cutflow"]`).  Empty list means no output covers this region. |

---

### `NuisanceGroupCoverageEntry`

Coverage record for a single nuisance group.

```python
@dataclass
class NuisanceGroupCoverageEntry:
    group_name:   str
    group_type:   str        = "shape"
    systematics:  List[str]  = []
    processes:    List[str]  = []
    regions:      List[str]  = []
    output_usage: List[str]  = []
    missing_up:   List[str]  = []
    missing_down: List[str]  = []
    not_found:    List[str]  = []
    severity:     str        = "error"   # "error" or "warn"
```

| Field          | Description |
|----------------|-------------|
| `group_name`   | Name of the nuisance group (e.g. `"jet_energy_scale"`). |
| `group_type`   | Type: `"shape"`, `"rate"`, etc. |
| `systematics`  | Base names of the systematics in this group. |
| `processes`    | Applicable processes; empty = all. |
| `regions`      | Applicable regions; empty = all. |
| `output_usage` | Applicable outputs; empty = all. |
| `missing_up`   | Systematics missing their Up variation column. |
| `missing_down` | Systematics missing their Down variation column. |
| `not_found`    | Systematics not present in the output at all. |
| `severity`     | `"error"` (default) causes `has_errors` to be `True`; `"warn"` records as warning only. |

**Property:** `is_complete → bool` — `True` when `missing_up`, `missing_down`, and
`not_found` are all empty.

---

### `RegionReferenceEntry`

Validation record for a region name referenced in a histogram or cutflow configuration.

```python
@dataclass
class RegionReferenceEntry:
    config_type:       str          # "histogram" or "cutflow"
    config_name:       str
    referenced_region: str
    is_known:          bool = True
```

| Field              | Description |
|--------------------|-------------|
| `config_type`      | Kind of config containing the reference: `"histogram"` or `"cutflow"`. |
| `config_name`      | Name of the histogram or cutflow entry that references the region. |
| `referenced_region` | The region name that was referenced. |
| `is_known`         | `True` when the region was found in the set of declared regions. |

**Property:** `is_valid → bool` — alias for `is_known`.

---

### `ReportSeverity` Enum

```python
class ReportSeverity(str, enum.Enum):
    INFO    = "info"
    WARNING = "warning"
    ERROR   = "error"
```

Used in `ConfigMismatchEntry`.

---

## 4. ValidationReport Class

```python
class ValidationReport:
    def __init__(self, stage: str, timestamp: Optional[str] = None) -> None: ...
```

**Parameters:**

| Parameter   | Description |
|-------------|-------------|
| `stage`     | Name of the production stage this report covers (e.g. `"preselection"`, `"histogram"`). |
| `timestamp` | UTC timestamp (ISO 8601).  Defaults to the current time when `None`. |

### Attributes

The following list attributes are populated by the `add_*` methods:

| Attribute                | Type                              | Description |
|--------------------------|-----------------------------------|-------------|
| `stage`                  | `str`                             | Production stage name. |
| `timestamp`              | `str`                             | ISO 8601 creation timestamp. |
| `report_version`         | `int`                             | Schema version (currently `1`). |
| `event_counts`           | `List[EventCountEntry]`           | Event count records. |
| `cutflow`                | `List[CutflowEntry]`              | Cutflow steps. |
| `missing_branches`       | `List[MissingBranchEntry]`        | Missing-branch records. |
| `config_mismatches`      | `List[ConfigMismatchEntry]`       | Configuration mismatch records. |
| `systematics`            | `List[SystematicEntry]`           | Systematic coverage records. |
| `weight_summaries`       | `List[WeightSummaryEntry]`        | Weight statistic records. |
| `output_integrity`       | `List[OutputIntegrityEntry]`      | Output artifact integrity records. |
| `regions`                | `List[RegionEntry]`               | Analysis region validation records. |
| `nuisance_group_coverage`| `List[NuisanceGroupCoverageEntry]`| Nuisance group coverage records. |
| `region_references`      | `List[RegionReferenceEntry]`      | Region-reference validation records. |
| `errors`                 | `List[str]`                       | Free-form error messages. |
| `warnings`               | `List[str]`                       | Free-form warning messages. |

### Summary Properties

#### `has_errors → bool`

Returns `True` when the report contains any error-level finding:

- Any free-form `errors` message.
- Any `ConfigMismatchEntry` with `severity == ERROR`.
- Any `OutputIntegrityEntry` where `is_ok` is `False`.
- Any `MissingBranchEntry` where `missing_branches` is non-empty.
- Any `RegionEntry` where `is_valid` is `False`.
- Any `NuisanceGroupCoverageEntry` that is incomplete and has `severity == "error"`.
- Any `RegionReferenceEntry` where `is_known` is `False`.

#### `has_warnings → bool`

Returns `True` when the report contains any warning-level finding:

- Any free-form `warnings` message.
- Any `ConfigMismatchEntry` with `severity == WARNING`.
- Any `SystematicEntry` that is not complete.
- Any `NuisanceGroupCoverageEntry` that is incomplete and has `severity != "error"`.

### Add Methods

| Method | Entry Type |
|--------|------------|
| `add_event_count(entry)` | `EventCountEntry` |
| `add_cutflow_step(entry)` | `CutflowEntry` |
| `add_missing_branches(entry)` | `MissingBranchEntry` |
| `add_config_mismatch(entry)` | `ConfigMismatchEntry` |
| `add_systematic(entry)` | `SystematicEntry` |
| `add_weight_summary(entry)` | `WeightSummaryEntry` |
| `add_output_integrity(entry)` | `OutputIntegrityEntry` |
| `add_region(entry)` | `RegionEntry` |
| `add_nuisance_group_coverage(entry)` | `NuisanceGroupCoverageEntry` |
| `add_region_reference(entry)` | `RegionReferenceEntry` |
| `add_error(message: str)` | Free-form error string |
| `add_warning(message: str)` | Free-form warning string |

### Output Methods

| Method | Description |
|--------|-------------|
| `to_dict() → Dict[str, Any]` | Full report as a JSON/YAML-serialisable dict, including a `summary` block with counts. |
| `to_json(indent=2) → str` | JSON string. |
| `to_yaml() → str` | YAML string. |
| `to_text() → str` | Human-readable ASCII-formatted string with section headers and tabular layouts. |
| `save_json(path: str)` | Writes JSON to *path* (creates parent directories as needed). |
| `save_yaml(path: str)` | Writes YAML to *path*. |
| `save_text(path: str)` | Writes human-readable text to *path*. |

### Deserialisation Methods

| Method | Description |
|--------|-------------|
| `from_dict(data) → ValidationReport` | Constructs from a plain dict (classmethod). |
| `load_yaml(path) → ValidationReport` | Loads from a YAML file (classmethod). |
| `load_json(path) → ValidationReport` | Loads from a JSON file (classmethod). |

---

## 5. `generate_report_from_manifest()`

```python
def generate_report_from_manifest(
    manifest:    OutputManifest,
    stage:       str  = "unknown",
    check_files: bool = False,
) -> ValidationReport:
```

Generates a `ValidationReport` by inspecting an `OutputManifest`.  The function
populates the following sections automatically:

| Section | What is populated |
|---------|-------------------|
| **Errors** | Any errors returned by `manifest.validate()`. |
| **Output integrity** | One `OutputIntegrityEntry` per declared output file (`skim`, `histograms`, `metadata`, `cutflow`, `law_artifacts`, `intermediate_artifacts`).  When `check_files=True`, file existence and size are checked on disk. |
| **Missing branches** | One `MissingBranchEntry` stub per schema that declares a branch/column list (`skim.branches`, `intermediate_artifacts[i].columns`).  The `missing_branches` list is empty at generation time; callers fill it after comparing against the actual file. |
| **Config mismatches** | One `ConfigMismatchEntry` per schema version that does not match the expected constant (e.g. `SKIM_SCHEMA_VERSION`). |
| **Cutflow** | One `CutflowEntry` stub per counter key in `manifest.cutflow.counter_keys` (event counts are not available without reading the ROOT file). |
| **Regions** | One `RegionEntry` per declared region with validation status, hierarchy issues, and `covered_by` (which artifact roles reference the region by name). |
| **Nuisance group coverage** | One `NuisanceGroupCoverageEntry` stub per declared nuisance group.  The `missing_up`, `missing_down`, and `not_found` fields are empty; callers with column information should call `VariationOrchestrator.build_validation_report()` to fill them in. |

**Parameters:**

| Parameter     | Description |
|---------------|-------------|
| `manifest`    | The `OutputManifest` to inspect. |
| `stage`       | Production stage name embedded in the report. |
| `check_files` | When `True`, test each declared output file for existence and record its size. |

### Helper: `validate_region_references()`

```python
def validate_region_references(
    report:        ValidationReport,
    known_regions: List[str],
    referenced:    List[Dict[str, str]],
) -> None:
```

Populates *report* with `RegionReferenceEntry` records.  For each item in
*referenced* (a dict with keys `"config_type"`, `"config_name"`, `"region"`),
checks whether `"region"` is in `known_regions` and appends the result.

---

## 6. CLI Usage — `validation_report.py`

The module provides a CLI entry point that generates a validation report from an
`OutputManifest` YAML file.

```
usage: validation_report.py [-h] [--stage STAGE] [--check-files]
                             [--out-yaml PATH] [--out-json PATH]
                             [--out-text PATH] [--print]
                             manifest
```

**Positional argument:**

| Argument   | Description |
|------------|-------------|
| `manifest` | Path to the `output_manifest.yaml` file to inspect. |

**Optional arguments:**

| Flag              | Description |
|-------------------|-------------|
| `--stage STAGE`   | Production stage name to embed in the report (default: `unknown`). |
| `--check-files`   | Check whether declared output files exist on disk. |
| `--out-yaml PATH` | Write machine-readable YAML report to PATH. |
| `--out-json PATH` | Write machine-readable JSON report to PATH. |
| `--out-text PATH` | Write human-readable text report to PATH. |
| `--print`         | Print the human-readable report to stdout. |

When none of `--out-yaml`, `--out-json`, or `--out-text` are provided (and `--print`
is not explicitly passed), the report is printed to stdout by default.

The exit code is `0` if the report has no errors, `1` otherwise.

**Examples:**

```bash
# Print a text report to stdout
python validation_report.py job_42/output_manifest.yaml --stage histogram

# Generate all formats and check files on disk
python validation_report.py job_42/output_manifest.yaml \
    --stage histogram \
    --check-files \
    --out-yaml reports/histogram.yaml \
    --out-json reports/histogram.json \
    --out-text reports/histogram.txt
```

---

## Part B: ReproducibilityReport

## 7. ReproducibilityReport Overview

`ReproducibilityReport` (in `core/python/reproducibility_report.py`) is a
structured, provenance-driven record of all information needed to reproduce or
cross-check an analysis run.

Provenance data is collected at C++ analysis time by the `ProvenanceService` and
written as `TNamed` objects into a `provenance` TDirectory inside the meta ROOT
output file.  The Python module reads this data via **uproot** and organises the
flat `{key: value}` map into logical sections.

**What is captured:**

- **Framework build info** — git hash, dirty flag, build timestamp, compiler.
- **ROOT version** — the version of ROOT used at build time.
- **Analysis repository** — git hash of the user's analysis code.
- **Environment** — container tag, number of processing threads.
- **Configuration** — deterministic hash of the full configuration map and of the
  file-list file.
- **Input file hashes** — MD5 digests of auxiliary files referenced by configuration.
- **Dataset manifest** — identity of the dataset manifest (file hash, query
  parameters, resolved dataset entries).
- **Plugin provenance** — per-plugin entries contributed by each plugin's
  `collectProvenanceEntries()` hook, including an auto-computed `config_hash` per
  plugin role.
- **Task metadata** — arbitrary key–value pairs injected via
  `Analyzer::setTaskMetadata()`.

---

## 8. Provenance Sections

The report organises provenance entries by namespace prefix.

### `framework` property

Keys under `framework.*` and `root.*` namespaces.

Typical keys:

| Provenance key             | Example value        | Description |
|----------------------------|----------------------|-------------|
| `framework.git_hash`       | `"abc123def"`        | Git commit hash of RDFAnalyzerCore. |
| `framework.git_dirty`      | `"false"`            | `"true"` if the working tree had uncommitted changes. |
| `framework.build_timestamp`| `"2024-11-01T12:00"` | Build timestamp. |
| `framework.compiler`       | `"g++ 13.2.0"`       | Compiler used to build the framework. |
| `root.version`             | `"6.32.02"`          | ROOT version at build time. |

### `analysis` property

Keys under the `analysis.*` namespace.

| Provenance key          | Description |
|-------------------------|-------------|
| `analysis.git_hash`     | Git commit hash of the analysis repository. |
| `analysis.git_dirty`    | `"true"` if the analysis working tree was dirty. |

### `environment` property

Keys under `env.*` and `executor.*` namespaces.

| Provenance key           | Example value   | Description |
|--------------------------|-----------------|-------------|
| `env.container_tag`      | `"cms:2024_v3"` | Container image tag used to run the analysis. |
| `executor.num_threads`   | `"8"`           | Number of threads used by the RDF executor. |

### `configuration` property

Keys under `config.*` (stored as-is) and `filelist.*` namespaces (prefixed with
`filelist_` to avoid collisions).

| Provenance key      | Appears as            | Description |
|---------------------|-----------------------|-------------|
| `config.hash`       | `hash`                | Deterministic hash of the full configuration map. |
| `filelist.hash`     | `filelist_hash`       | Hash of the file-list file. |
| `filelist.path`     | `filelist_path`       | Path to the file-list file. |

### `file_hashes` property

Keys under the `file.hash.*` namespace.  Each entry maps an auxiliary input
filename to its MD5 digest.

```
file.hash.weights.root  →  "d41d8cd98f00b204e9800998ecf8427e"
```

### `dataset_manifest` property

Keys under the `dataset_manifest.*` namespace.

| Provenance key                   | Description |
|----------------------------------|-------------|
| `dataset_manifest.hash`          | Hash of the dataset manifest file. |
| `dataset_manifest.query_params`  | Query parameters used to resolve datasets. |
| `dataset_manifest.n_entries`     | Number of resolved dataset entries. |

### `plugins` property

Per-role plugin provenance, keyed by role name.  Returns a dict of the form
`{role: {sub_key: value}}`, parsed from `plugin.<role>.<sub_key>` entries.

```python
{
    "histManager": {
        "version":     "2",
        "config_hash": "c0ffee42",
    },
    "btagSF": {
        "config_hash": "deadbeef",
    },
}
```

When a plugin role is recorded but has no sub-entries, the inner dict is empty.

### `task_metadata` property

Keys under the `task.*` namespace — arbitrary key–value pairs injected via
`Analyzer::setTaskMetadata()`.

### `other` property

All entries that do not match any of the known namespace prefixes above.

---

## 9. ReproducibilityReport Class

```python
class ReproducibilityReport:
    def __init__(
        self,
        provenance: Optional[Dict[str, str]] = None,
        timestamp:  Optional[str] = None,
    ) -> None: ...
```

**Parameters:**

| Parameter    | Description |
|--------------|-------------|
| `provenance` | Flat `{key: value}` provenance map.  Defaults to empty. |
| `timestamp`  | UTC creation timestamp (ISO 8601).  Defaults to the current time. |

**Attributes:**

| Attribute          | Description |
|--------------------|-------------|
| `provenance`       | Flat `{key: value}` provenance map (all entries). |
| `timestamp`        | ISO 8601 creation timestamp. |
| `report_version`   | Schema version (currently `1`). |

### Output Methods

| Method | Description |
|--------|-------------|
| `to_dict() → Dict[str, Any]` | Full report as a JSON/YAML-serialisable dict.  Includes both the flat `provenance` map and all structured sections derived from it, plus a `summary` block with entry counts per section. |
| `to_json(indent=2) → str` | JSON string. |
| `to_yaml() → str` | YAML string. |
| `to_text() → str` | Human-readable ASCII-formatted string with section headers and two-column key/value tables.  Intended for terminal display or log files. |
| `save_json(path: str)` | Writes JSON to *path* (creates parent directories as needed). |
| `save_yaml(path: str)` | Writes YAML to *path*. |
| `save_text(path: str)` | Writes human-readable text to *path*. |

### Deserialisation Methods

| Method | Description |
|--------|-------------|
| `from_dict(data) → ReproducibilityReport` | Constructs from a plain dict.  Accepts both full report dicts (with `"provenance"` sub-key) and bare flat provenance maps (classmethod). |
| `load_yaml(path) → ReproducibilityReport` | Loads from a YAML file (classmethod). |
| `load_json(path) → ReproducibilityReport` | Loads from a JSON file (classmethod). |

### Module-Level Functions

#### `load_provenance_from_root(meta_root_path: str) → Dict[str, str]`

Reads the `provenance` TDirectory from a ROOT meta file using **uproot**.  Each
`TNamed` object in the directory contributes one entry: the TNamed name is the key,
the TNamed title is the value.  Raises `FileNotFoundError` if the file does not
exist and `ImportError` if uproot is not installed.  Returns an empty dict if the
`"provenance"` directory is absent.

#### `build_report_from_provenance(provenance, timestamp=None) → ReproducibilityReport`

Thin wrapper around the `ReproducibilityReport` constructor, provided for symmetry
with other `build_report_from_*` helpers in the framework.

---

## 10. CLI Usage — `reproducibility_report.py`

```
usage: reproducibility_report.py [-h] [--load-yaml FILE] [--load-json FILE]
                                  [--yaml FILE] [--json FILE] [--text FILE]
                                  [--quiet]
                                  [META_ROOT_FILE]
```

**Input (one of the following):**

| Argument / Flag          | Description |
|--------------------------|-------------|
| `META_ROOT_FILE`         | Path to the ROOT meta output file containing the `provenance` TDirectory (read via uproot). |
| `--load-yaml FILE`       | Load an existing report from a previously saved YAML file. |
| `--load-json FILE`       | Load an existing report from a previously saved JSON file. |

**Output flags (all optional; if none given, prints to stdout):**

| Flag             | Description |
|------------------|-------------|
| `--yaml FILE`    | Write machine-readable YAML report to FILE. |
| `--json FILE`    | Write machine-readable JSON report to FILE. |
| `--text FILE`    | Write human-readable text report to FILE. |
| `--quiet`        | Do not print the report to stdout. |

Exit code is `0` on success, `1` on error.

**Examples:**

```bash
# Read from ROOT meta file and print to stdout
python reproducibility_report.py output_meta.root

# Read and save all formats
python reproducibility_report.py output_meta.root \
    --yaml  reports/repro.yaml \
    --json  reports/repro.json \
    --text  reports/repro.txt \
    --quiet

# Re-load a saved YAML report
python reproducibility_report.py --load-yaml reports/repro.yaml
```

---

## 11. Integration with ProvenanceService

The C++ `ProvenanceService` (in the framework core) collects provenance entries
at analysis runtime and writes them into the meta ROOT output file as `TNamed`
objects inside a `provenance` TDirectory.

Entries are written with the flat namespaced key convention:

```
framework.git_hash        → commit hash of RDFAnalyzerCore
framework.git_dirty       → "true" / "false"
root.version              → ROOT version string
analysis.git_hash         → analysis repo commit hash
env.container_tag         → container image tag
executor.num_threads      → number of RDF threads
config.hash               → hash of the configuration map
filelist.hash             → hash of the file-list file
file.hash.<filename>      → MD5 of auxiliary input file
dataset_manifest.hash     → hash of the dataset manifest
plugin.<role>.version     → per-plugin version string
plugin.<role>.config_hash → hash of that plugin's configuration
task.<key>                → user-injected key–value metadata
```

On the Python side, `load_provenance_from_root()` reads all entries from the
`provenance` TDirectory and returns them as a flat `{str: str}` dict, which is
then passed to `build_report_from_provenance()` (or directly to
`ReproducibilityReport(provenance=...)`).

---

## 12. Usage Examples

### Building a ValidationReport Programmatically

```python
from validation_report import (
    ValidationReport, EventCountEntry, CutflowEntry,
    SystematicEntry, WeightSummaryEntry, OutputIntegrityEntry,
)

report = ValidationReport(stage="preselection")

# Event counts
report.add_event_count(EventCountEntry(
    sample="ttbar", stage="preselection",
    total_events=5_000_000, selected_events=312_450,
))
report.add_event_count(EventCountEntry(
    sample="signal_m500", stage="preselection",
    total_events=100_000, selected_events=62_300,
))

# Cutflow
report.add_cutflow_step(CutflowEntry(
    cut_name="trigger", events_passed=4_800_000,
    events_cumulative=4_800_000,
    relative_efficiency=0.96, cumulative_efficiency=0.96,
))
report.add_cutflow_step(CutflowEntry(
    cut_name="lepton_veto", events_passed=3_200_000,
    events_cumulative=3_200_000,
    relative_efficiency=0.667, cumulative_efficiency=0.640,
))

# Systematic coverage
report.add_systematic(SystematicEntry("JES", has_up=True,  has_down=True))
report.add_systematic(SystematicEntry("JER", has_up=True,  has_down=False))  # incomplete

# Weight summary
report.add_weight_summary(WeightSummaryEntry(
    sample="ttbar", sum_weights=4_987_321.5,
    sum_weights_squared=4_891_234.0,
    n_events=5_000_000, n_negative_weights=0,
    min_weight=0.42, max_weight=3.71,
))

# Free-form warning
report.add_warning("Scale factor file is from a previous run epoch; review recommended.")

# Check status
print(f"has_errors:   {report.has_errors}")    # False (JER warning is not an error)
print(f"has_warnings: {report.has_warnings}")  # True

# Save all formats
report.save_yaml("reports/preselection.yaml")
report.save_json("reports/preselection.json")
report.save_text("reports/preselection.txt")

# Print human-readable summary
print(report.to_text())
```

### Generating a Report from an OutputManifest

```python
from output_schema import OutputManifest
from validation_report import generate_report_from_manifest

manifest = OutputManifest.load_yaml("job_42/output_manifest.yaml")
report = generate_report_from_manifest(
    manifest, stage="histogram", check_files=True
)

# Add nuisance group column-level coverage via the orchestrator
from variation_orchestrator import VariationOrchestrator
# (orch was built earlier with the registry and weight variations)
orch.build_validation_report(
    report,
    available_columns=df_columns,
    processes=["signal", "ttbar"],
    regions=["signal_region", "control_ttbar"],
    output_usage="histogram",
)

report.save_yaml("job_42/validation_report.yaml")
if report.has_errors:
    raise RuntimeError("Validation failed — check job_42/validation_report.yaml")
```

### Loading a ReproducibilityReport from a ROOT Meta File

```python
from reproducibility_report import (
    load_provenance_from_root,
    build_report_from_provenance,
    ReproducibilityReport,
)

# Read provenance from the ROOT meta file (requires uproot)
prov = load_provenance_from_root("output_meta.root")
report = build_report_from_provenance(prov)

# Print a human-readable summary
print(report.to_text())

# Save
report.save_yaml("reports/reproducibility.yaml")
report.save_json("reports/reproducibility.json")
```

### Building a Report Directly from a Flat Dict

```python
from reproducibility_report import ReproducibilityReport

report = ReproducibilityReport(provenance={
    "framework.git_hash":    "abc123",
    "framework.git_dirty":   "false",
    "root.version":          "6.32.02",
    "env.container_tag":     "cms:2024_v3",
    "config.hash":           "deadbeef",
    "plugin.histManager.version": "2",
})

# Inspect structured sections
print(report.framework)
# {"git_hash": "abc123", "git_dirty": "false", "version": "6.32.02"}

print(report.plugins)
# {"histManager": {"version": "2"}}
```

### Comparing Two ReproducibilityReports

```python
from reproducibility_report import ReproducibilityReport

report_a = ReproducibilityReport.load_yaml("run_a/repro.yaml")
report_b = ReproducibilityReport.load_yaml("run_b/repro.yaml")

# Compare key provenance fields
fields_to_compare = [
    "framework.git_hash",
    "config.hash",
    "filelist.hash",
]

print("Provenance comparison:")
for key in fields_to_compare:
    val_a = report_a.provenance.get(key, "(missing)")
    val_b = report_b.provenance.get(key, "(missing)")
    match = "OK" if val_a == val_b else "MISMATCH"
    print(f"  [{match}] {key}: {val_a!r}  vs  {val_b!r}")

# Detect any keys that differ
all_keys = set(report_a.provenance) | set(report_b.provenance)
diffs = [
    k for k in sorted(all_keys)
    if report_a.provenance.get(k) != report_b.provenance.get(k)
]
if diffs:
    print(f"\nDiffering keys ({len(diffs)}):")
    for k in diffs:
        print(f"  {k}: {report_a.provenance.get(k)!r}  →  {report_b.provenance.get(k)!r}")
else:
    print("\nAll provenance entries match.")
```

### Round-Trip Through YAML

```python
import yaml
from reproducibility_report import ReproducibilityReport

original = ReproducibilityReport(
    provenance={"framework.git_hash": "abc123", "config.hash": "42"}
)
yaml_str = original.to_yaml()

# Deserialise from the YAML dict
restored = ReproducibilityReport.from_dict(yaml.safe_load(yaml_str))
assert restored.provenance == original.provenance
```
