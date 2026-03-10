# Output Schema System

## Overview

The output schema system provides explicit, versioned metadata definitions for every type of file produced by the RDFAnalyzerCore framework. Rather than relying on implicit conventions or runtime discovery, each output type is described by a dedicated schema class whose format contract is captured in a single `OutputManifest` YAML file written alongside the job outputs.

**Why it exists:**  downstream tools — datacards, plotting scripts, statistical frameworks, batch-merge steps — need to know the exact structure of the outputs they consume. By persisting a manifest, any consumer can:

- Verify output files have the expected format without reading the ROOT files.
- Detect when outputs were produced by an older (incompatible) schema version.
- Detect when outputs are *stale* (schema version is current but the environment has changed, e.g. a newer framework git commit).
- Validate inputs before merging multiple job outputs.

The framework covers six output categories:

| Category | Produced by | Schema class |
|---|---|---|
| **Skims** | `RootOutputSink` | `SkimSchema` |
| **Histograms** | `NDHistogramManager` | `HistogramSchema` |
| **Metadata / provenance** | `ProvenanceService` | `MetadataSchema` |
| **Cutflows** | `CounterService` | `CutflowSchema` |
| **LAW artifacts** | `law/nano_tasks.py`, `law/opendata_tasks.py` | `LawArtifactSchema` |
| **Intermediate artifacts** | Pipeline caching layer | `IntermediateArtifactSchema` |

---

## OutputManifest

`OutputManifest` is the top-level container that holds references to all schema objects produced in a single analysis job.

### Fields

| Field | Type | Description |
|---|---|---|
| `manifest_version` | `int` | Container format version (must equal `CURRENT_VERSION = 1`). |
| `skim` | `SkimSchema \| None` | Schema for the skim ROOT file, if produced. |
| `histograms` | `HistogramSchema \| None` | Schema for the histogram ROOT file, if applicable. |
| `metadata` | `MetadataSchema \| None` | Schema for the provenance metadata output, if applicable. |
| `cutflow` | `CutflowSchema \| None` | Schema for the cutflow output, if applicable. |
| `law_artifacts` | `list[LawArtifactSchema]` | Schemas for LAW task artifacts. Empty list if none. |
| `intermediate_artifacts` | `list[IntermediateArtifactSchema]` | Schemas for cached intermediate artifacts. |
| `regions` | `list[RegionDefinition]` | Named analysis regions declared by `RegionManager`. |
| `nuisance_groups` | `list[NuisanceGroupDefinition]` | Nuisance / systematic group definitions. |
| `framework_hash` | `str \| None` | Git commit hash of RDFAnalyzerCore at job-submission time. |
| `user_repo_hash` | `str \| None` | Git commit hash of the user analysis repository. |
| `config_mtime` | `str \| None` | UTC ISO 8601 modification time of the job configuration file. |
| `dataset_manifest_provenance` | `DatasetManifestProvenance \| None` | Identity and query record for the dataset manifest used. |

At least one of `skim`, `histograms`, `metadata`, `cutflow`, `law_artifacts`, or `intermediate_artifacts` must be populated; `validate()` returns an error if all are absent.

### Methods

| Method | Signature | Description |
|---|---|---|
| `save_yaml` | `(path: str) -> None` | Serialise the manifest to a YAML file. Parent directories are created automatically. |
| `load_yaml` | `(path: str) -> OutputManifest` | Classmethod. Load and deserialise a manifest from a YAML file. |
| `validate` | `() -> list[str]` | Run structural validation on all contained schemas. Returns a (possibly empty) list of error strings. |
| `check_version_compatibility` | `(manifest: OutputManifest) -> None` | Static method. Raise `SchemaVersionError` if any stored schema version differs from the current code version. |
| `provenance` | `() -> ProvenanceRecord` | Build a `ProvenanceRecord` from the manifest's recorded hashes and timestamps. |
| `resolve` | `(current_provenance=None) -> dict[str, ArtifactResolutionStatus]` | Convenience wrapper for `resolve_manifest()` that uses this manifest's stored provenance as the baseline. |

### YAML Example

```yaml
manifest_version: 1
framework_hash: a1b2c3d4e5f6
user_repo_hash: 9f8e7d6c5b4a
config_mtime: "2024-03-15T12:00:00+00:00"

skim:
  schema_version: 1
  output_file: output/sample_0.root
  tree_name: Events
  branches:
    - Muon_pt
    - Muon_eta
    - MET_pt

histograms:
  schema_version: 1
  output_file: output/sample_0_meta.root
  histogram_names:
    - muon_pt_vs_eta
  axes:
    - variable: Muon_pt
      bins: 50
      lower_bound: 0.0
      upper_bound: 200.0
      label: "Muon p_{T} [GeV]"
    - variable: Muon_eta
      bins: 30
      lower_bound: -3.0
      upper_bound: 3.0
      label: "#eta"

metadata:
  schema_version: 1
  output_file: output/sample_0_meta.root
  provenance_dir: provenance
  required_keys:
    - framework.git_hash
    - config.hash
  optional_keys:
    - dataset_manifest.file_hash

cutflow:
  schema_version: 1
  output_file: output/sample_0_meta.root
  counter_keys:
    - sample_0.total
    - sample_0.weighted

law_artifacts: []
intermediate_artifacts: []
regions: []
nuisance_groups: []
dataset_manifest_provenance: null
```

---

## Schema Types

### SkimSchema

Describes a ROOT `TTree` skim file written by `RootOutputSink`.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `output_file` | `str` | `""` | Path or pattern of the output ROOT file. Must not be empty. |
| `tree_name` | `str` | `"Events"` | Name of the `TTree` inside the ROOT file. Must not be empty. |
| `branches` | `list[str]` | `[]` | Expected branch names. Empty list means all branches are accepted. |

```python
from output_schema import SkimSchema

skim = SkimSchema(
    output_file="output/sample_0.root",
    tree_name="Events",
    branches=["Muon_pt", "Muon_eta", "MET_pt"],
)
errors = skim.validate()  # [] if valid
```

---

### HistogramAxisSpec

Describes a single axis dimension of a `THnSparseF` histogram.

| Field | Type | Default | Description |
|---|---|---|---|
| `variable` | `str` | `""` | Branch/column name used to fill this axis. Must not be empty. |
| `bins` | `int` | `0` | Number of bins. Must be `> 0`. |
| `lower_bound` | `float` | `0.0` | Lower edge of the axis range. Must be less than `upper_bound`. |
| `upper_bound` | `float` | `1.0` | Upper edge of the axis range. |
| `label` | `str` | `""` | Human-readable axis label (supports ROOT LaTeX notation). |

---

### HistogramSchema

Describes `THnSparseF` histogram objects saved by `NDHistogramManager` into the meta-output ROOT file.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `output_file` | `str` | `""` | Path or pattern of the meta-output ROOT file. Must not be empty. |
| `histogram_names` | `list[str]` | `[]` | Names of the `THnSparseF` objects expected in the file. |
| `axes` | `list[HistogramAxisSpec]` | `[]` | Axis specifications shared across all histograms in this schema. |

```python
from output_schema import HistogramSchema, HistogramAxisSpec

histograms = HistogramSchema(
    output_file="output/sample_0_meta.root",
    histogram_names=["muon_pt_vs_eta"],
    axes=[
        HistogramAxisSpec(variable="Muon_pt", bins=50, lower_bound=0.0, upper_bound=200.0, label="Muon p_{T} [GeV]"),
        HistogramAxisSpec(variable="Muon_eta", bins=30, lower_bound=-3.0, upper_bound=3.0, label="#eta"),
    ],
)
```

---

### MetadataSchema

Describes the provenance metadata output written by `ProvenanceService` as `TNamed` objects inside a `TDirectory` in the meta-output ROOT file.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `output_file` | `str` | `""` | Path or pattern of the meta-output ROOT file. Must not be empty. |
| `provenance_dir` | `str` | `"provenance"` | Name of the `TDirectory` holding the provenance objects. Must not be empty. |
| `required_keys` | `list[str]` | `PROVENANCE_REQUIRED_KEYS` | Keys that must be present. Defaults to the framework-defined required set. |
| `optional_keys` | `list[str]` | `PROVENANCE_OPTIONAL_KEYS` | Keys that may optionally be present. |

**Default required provenance keys** (`PROVENANCE_REQUIRED_KEYS`):

```
framework.git_hash
framework.git_dirty
framework.build_timestamp
framework.compiler
root.version
config.hash
executor.num_threads
```

**Default optional provenance keys** (`PROVENANCE_OPTIONAL_KEYS`):

```
analysis.git_hash
analysis.git_dirty
env.container_tag
filelist.hash
dataset_manifest.file_hash
dataset_manifest.query_params
dataset_manifest.resolved_entries
```

---

### CutflowSchema

Describes event-count histograms written by `CounterService`.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `output_file` | `str` | `""` | Path or pattern of the ROOT file containing counter objects. Typically the same as the meta-output file. Must not be empty. |
| `counter_keys` | `list[str]` | `[]` | Ordered counter key names expected in the output. Empty list disables key-level validation. |

---

### LawArtifactSchema

Describes a single output file produced by a LAW workflow task (`PrepareNANOSample`, `BuildNANOSubmission`, `SubmitNANOJobs`, `MonitorNANOJobs`, `RunNANOAnalysisJob`, …).

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `artifact_type` | `str` | `""` | Logical type of the artifact. Must be one of the recognised types (see below). |
| `path_pattern` | `str` | `""` | Glob-compatible path pattern for the expected output file(s). |
| `format` | `str` | `"text"` | Serialisation format: `"json"`, `"text"`, `"shell"`, or `"root"`. |

**Recognised `artifact_type` values** (`LAW_ARTIFACT_TYPES`):

| Value | LAW task |
|---|---|
| `prepare_sample` | `PrepareNANOSample` |
| `build_submission` | `BuildNANOSubmission` |
| `submit_jobs` | `SubmitNANOJobs` |
| `monitor_jobs` | `MonitorNANOJobs` |
| `run_job` | `RunNANOAnalysisJob` |
| `monitor_state` | State-monitoring tasks |

---

### IntermediateArtifactSchema

Describes a mid-pipeline cached result that can be reused across workflow stages.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `artifact_kind` | `str` | `""` | Logical kind of the intermediate artifact. Must be one of the recognised kinds (see below). |
| `output_file` | `str` | `""` | Path or pattern of the output ROOT file. Must not be empty. |
| `tree_name` | `str` | `"Events"` | Name of the `TTree` inside the ROOT file. Must not be empty. |
| `columns` | `list[str]` | `[]` | Branch/column names materialised in this snapshot. Empty list means all columns are included. |

**Recognised `artifact_kind` values** (`INTERMEDIATE_ARTIFACT_KINDS`):

| Value | Description |
|---|---|
| `preselection` | Output after an initial preselection filter is applied. |
| `reduced_skim` | Skim produced after an event-level reduction step. |
| `column_snapshot` | Snapshot caching derived column/branch values. |
| `enriched_skim` | Skim enriched with pre-computed physics objects. |

---

### RegionDefinition

Describes a named analysis region declared by `RegionManager`. Regions select a sub-sample of events via a boolean filter column, and can form a parent–child hierarchy.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `name` | `str` | `""` | Unique region name (e.g. `"signal"`, `"control_wjets"`). Must not be empty. |
| `filter_column` | `str` | `""` | Boolean dataframe column that selects events in this region. Must not be empty. |
| `parent` | `str` | `""` | Name of the parent region, or empty string for a root region. |
| `description` | `str` | `""` | Optional human-readable description. |

The `validate_region_hierarchy()` helper validates a list of `RegionDefinition` objects, checking for duplicate names, unknown parent references, and cycles in the parent–child graph.

---

### NuisanceGroupDefinition

Collects related systematic variations and records which processes, regions, and downstream tools they apply to.

| Field | Type | Default | Description |
|---|---|---|---|
| `schema_version` | `int` | `1` | Schema format version. |
| `name` | `str` | `""` | Unique group name (e.g. `"jet_energy_scale"`). Must not be empty. |
| `group_type` | `str` | `"shape"` | One of `"shape"`, `"rate"`, `"normalization"`, `"other"`. |
| `systematics` | `list[str]` | `[]` | Base names of systematic variations (e.g. `["JES", "JER"]`). Each is expected to have `Up` and `Down` shifts. |
| `processes` | `list[str]` | `[]` | Processes this group applies to. Empty list means all processes. |
| `regions` | `list[str]` | `[]` | Analysis regions this group applies to. Empty list means all regions. |
| `output_usage` | `list[str]` | `[]` | Downstream tools: `"histogram"`, `"datacard"`, `"plot"`. Empty list means all outputs. |
| `description` | `str` | `""` | Optional human-readable description. |
| `correlation_group` | `str` | `""` | Optional label grouping correlated systematics across definitions. |

The `validate_nuisance_coverage()` helper checks that all declared systematics have corresponding `Up` and `Down` variations present in the output.

---

## Artifact Resolution and Caching

### ProvenanceRecord

A snapshot of the versioning context at the time an artifact was produced. Used to determine whether a previously produced artifact is still up-to-date.

| Field | Type | Description |
|---|---|---|
| `framework_hash` | `str \| None` | Git commit hash of the RDFAnalyzerCore framework. |
| `user_repo_hash` | `str \| None` | Git commit hash of the user analysis repository. |
| `config_mtime` | `str \| None` | UTC ISO 8601 modification time of the job configuration file. |
| `dataset_manifest_hash` | `str \| None` | SHA-256 hash of the dataset manifest file. |

Any field may be `None` when the information is unavailable. Two records are considered *matching* when every field that is non-`None` in **both** records has the same value. Fields that are `None` in either record are treated as "unknown" and do not cause a mismatch.

```python
from output_schema import ProvenanceRecord

recorded = manifest.provenance()
current = ProvenanceRecord(framework_hash="new_fw_hash")
if not recorded.matches(current):
    print("Artifact is stale – consider regenerating")
```

---

### ArtifactResolutionStatus

An `enum.Enum` with three values:

| Value | Meaning |
|---|---|
| `COMPATIBLE` | Schema version matches `CURRENT_VERSION` **and** provenance matches (or no comparison was requested). No regeneration needed. |
| `STALE` | Schema version is current but recorded provenance differs from the current environment. The artifact *can* still be used but should be regenerated when convenient. |
| `MUST_REGENERATE` | Schema version does **not** match `CURRENT_VERSION`. The artifact is incompatible and **must** be regenerated before use. |

---

### resolve_artifact()

```python
def resolve_artifact(
    artifact,
    recorded_provenance: ProvenanceRecord | None = None,
    current_provenance: ProvenanceRecord | None = None,
) -> ArtifactResolutionStatus
```

Determines whether a single schema object is compatible, stale, or must be regenerated by applying the three-step resolution rules:

1. **MUST_REGENERATE** – `artifact.schema_version != artifact.CURRENT_VERSION`.
2. **STALE** – both provenance records are provided and `recorded_provenance.matches(current_provenance)` is `False`.
3. **COMPATIBLE** – version is current and provenances match (or comparison was not requested).

---

### resolve_manifest()

```python
def resolve_manifest(
    manifest: OutputManifest,
    current_provenance: ProvenanceRecord | None = None,
) -> dict[str, ArtifactResolutionStatus]
```

Resolves all schemas in a manifest. Uses `manifest.provenance()` as the recorded provenance baseline. Returns a dict mapping role names to statuses:

```
{
    "skim": ArtifactResolutionStatus.COMPATIBLE,
    "histograms": ArtifactResolutionStatus.STALE,
    "law_artifacts[0]": ArtifactResolutionStatus.COMPATIBLE,
    ...
}
```

---

### CachedArtifact

Represents an intermediate artifact written to disk with an attached schema and provenance sidecar.

| Field | Type | Description |
|---|---|---|
| `artifact_path` | `str` | Path to the cached artifact file. |
| `manifest` | `OutputManifest` | Full `OutputManifest` describing the schemas and provenance. |
| `cached_at` | `str` | ISO 8601 UTC timestamp when the artifact was cached. |

---

### Cache Sidecar Files

The sidecar file is written at `{artifact_path}.cache.yaml` (the suffix constant `CACHE_SIDECAR_SUFFIX = ".cache.yaml"`). For example, `output/presel.root` gets a sidecar at `output/presel.root.cache.yaml`.

**`write_cache_sidecar(artifact_path, manifest, cached_at=None) -> str`**

Writes a `CachedArtifact` as YAML to `{artifact_path}.cache.yaml`. Returns the absolute path of the written sidecar.

**`read_cache_sidecar(artifact_path) -> CachedArtifact`**

Reads and deserialises the sidecar for a given artifact. Raises `FileNotFoundError` if no sidecar exists, `ValueError` if the content is not a valid YAML mapping.

**`check_cache_validity(artifact_path, current_provenance=None, strict=False) -> ArtifactResolutionStatus`**

Reads the sidecar and applies `resolve_manifest()` to determine whether the cached artifact can be reused. Resolution rules (in order):

1. **MUST_REGENERATE** – the artifact file or sidecar does not exist.
2. **MUST_REGENERATE** – any schema version in the sidecar does not match `CURRENT_VERSION`.
3. **STALE** – all schema versions are current but recorded provenance does not match `current_provenance`.
4. **COMPATIBLE** – all versions are current and provenance matches (or no `current_provenance` was supplied).

When `strict=True`, `STALE` is promoted to `MUST_REGENERATE`.

```python
from output_schema import (
    write_cache_sidecar, check_cache_validity,
    ArtifactResolutionStatus, ProvenanceRecord,
)

# After producing presel.root:
write_cache_sidecar("output/presel.root", manifest)

# Later, before reusing the cache:
current = ProvenanceRecord(framework_hash="abc123", user_repo_hash="xyz789")
status = check_cache_validity("output/presel.root", current_provenance=current)

if status == ArtifactResolutionStatus.COMPATIBLE:
    pass  # safe to reuse
elif status == ArtifactResolutionStatus.STALE:
    pass  # usable but outdated – regenerate when convenient
elif status == ArtifactResolutionStatus.MUST_REGENERATE:
    pass  # must regenerate before use
```

---

## Manifest Merging

After batch jobs produce individual per-sample manifests, the outputs must be merged (e.g. via `hadd`). The schema API provides two functions for this workflow.

### validate_merge_inputs()

```python
def validate_merge_inputs(
    manifests: list[OutputManifest],
    required_roles: list[str] | None = None,
) -> list[str]
```

Validates a collection of manifests for merge compatibility. This is the **canonical pre-merge validation API**: call it before any `hadd` or histogram-addition step. Returns a (possibly empty) list of human-readable error strings — an empty list means all inputs are compatible and the merge may proceed.

**Checks performed (in order):**

1. At least one manifest is provided.
2. Every manifest passes its own `validate()` check.
3. Every manifest passes `check_version_compatibility()` (no outdated schema versions).
4. All manifests expose the **same set** of scalar artifact roles (`skim`, `histograms`, `metadata`, `cutflow`).
5. For each shared scalar role, all manifests carry the **same** `schema_version`.
6. All manifests contain the **same number** of `law_artifacts`.
7. Corresponding `law_artifacts` entries share the same `schema_version`.
8. Same count and version checks for `intermediate_artifacts`.
9. If `required_roles` is supplied, every listed role must be present in every manifest.

Valid values for `required_roles`: `"skim"`, `"histograms"`, `"metadata"`, `"cutflow"`, `"law_artifacts"`, `"intermediate_artifacts"`.

### merge_manifests()

```python
def merge_manifests(
    manifests: list[OutputManifest],
    framework_hash: str | None = None,
    user_repo_hash: str | None = None,
    required_roles: list[str] | None = None,
) -> OutputManifest
```

Builds a merged `OutputManifest` from validated inputs. Calls `validate_merge_inputs()` internally and raises `MergeInputValidationError` if any checks fail. Schema definitions (including `output_file` path patterns) are copied from the **first** manifest. After the actual file merge (e.g. `hadd`), callers should update the `output_file` fields on the returned manifest's schemas to point to the merged output path before saving.

### MergeInputValidationError

`RuntimeError` subclass raised when merge inputs fail validation. Contains all diagnostics from all invalid inputs so callers can report every problem in a single message.

```python
from output_schema import (
    OutputManifest, merge_manifests, MergeInputValidationError,
)

manifests = [OutputManifest.load_yaml(p) for p in manifest_paths]
try:
    merged = merge_manifests(
        manifests,
        framework_hash=current_fw_hash,
        required_roles=["histograms"],
    )
except MergeInputValidationError as exc:
    print(f"Cannot merge: {exc}")
    raise

# Run hadd / histogram addition here...
merged.histograms.output_file = "merged_meta.root"
merged.save_yaml("merged/output_manifest.yaml")
```

---

## Schema Versioning

### Version Constants

Each schema class has a module-level version constant and a `CURRENT_VERSION` class attribute. Bump the module-level constant (and add a note to the version table in `output_schema.py`) whenever you make a breaking change to that output format.

| Constant | Value | Schema |
|---|---|---|
| `SKIM_SCHEMA_VERSION` | `1` | `SkimSchema` |
| `HISTOGRAM_SCHEMA_VERSION` | `1` | `HistogramSchema` |
| `METADATA_SCHEMA_VERSION` | `1` | `MetadataSchema` |
| `CUTFLOW_SCHEMA_VERSION` | `1` | `CutflowSchema` |
| `LAW_ARTIFACT_SCHEMA_VERSION` | `1` | `LawArtifactSchema` |
| `INTERMEDIATE_ARTIFACT_SCHEMA_VERSION` | `1` | `IntermediateArtifactSchema` |
| `REGION_DEFINITION_VERSION` | `1` | `RegionDefinition` |
| `NUISANCE_GROUP_DEFINITION_VERSION` | `1` | `NuisanceGroupDefinition` |
| `OUTPUT_MANIFEST_VERSION` | `1` | `OutputManifest` |

### SCHEMA_REGISTRY

A `dict[str, int]` mapping schema names to their current versions. Useful for programmatic queries.

```python
from output_schema import SCHEMA_REGISTRY

print(SCHEMA_REGISTRY)
# {
#   "skim": 1,
#   "histogram": 1,
#   "metadata": 1,
#   "cutflow": 1,
#   "law_artifact": 1,
#   "intermediate_artifact": 1,
#   "region_definition": 1,
#   "nuisance_group_definition": 1,
#   "output_manifest": 1,
# }
```

### SchemaVersionError

`RuntimeError` subclass raised by `check_version_compatibility()` when a loaded schema version does not match `CURRENT_VERSION`. Catch this separately from other `RuntimeError` exceptions to apply migration logic or emit clear user-facing messages.

---

## Usage Examples

### Creating an OutputManifest

```python
from output_schema import (
    OutputManifest,
    SkimSchema,
    HistogramSchema,
    HistogramAxisSpec,
    MetadataSchema,
    CutflowSchema,
    RegionDefinition,
    NuisanceGroupDefinition,
)

manifest = OutputManifest(
    skim=SkimSchema(
        output_file="output/sample_0.root",
        tree_name="Events",
        branches=["Muon_pt", "Muon_eta", "MET_pt"],
    ),
    histograms=HistogramSchema(
        output_file="output/sample_0_meta.root",
        histogram_names=["muon_pt"],
        axes=[
            HistogramAxisSpec(
                variable="Muon_pt",
                bins=50,
                lower_bound=0.0,
                upper_bound=200.0,
                label="Muon p_{T} [GeV]",
            )
        ],
    ),
    metadata=MetadataSchema(output_file="output/sample_0_meta.root"),
    cutflow=CutflowSchema(output_file="output/sample_0_meta.root"),
    regions=[
        RegionDefinition(name="signal", filter_column="pass_signal_sel"),
        RegionDefinition(
            name="signal_tight",
            filter_column="pass_tight_sel",
            parent="signal",
        ),
    ],
    nuisance_groups=[
        NuisanceGroupDefinition(
            name="jet_energy_scale",
            group_type="shape",
            systematics=["JES", "JER"],
            output_usage=["histogram", "datacard"],
        )
    ],
    framework_hash="a1b2c3d4",
    user_repo_hash="9f8e7d6c",
)

errors = manifest.validate()
if errors:
    raise ValueError("Schema validation failed:\n" + "\n".join(errors))

manifest.save_yaml("output/output_manifest.yaml")
```

### Loading and Validating a Manifest

```python
from output_schema import OutputManifest, SchemaVersionError

# Load the manifest
manifest = OutputManifest.load_yaml("output/output_manifest.yaml")

# Validate structural correctness
errors = manifest.validate()
if errors:
    for e in errors:
        print(f"  ERROR: {e}")

# Check that all schema versions are current (raises if not)
try:
    OutputManifest.check_version_compatibility(manifest)
except SchemaVersionError as exc:
    print(f"Schema version mismatch: {exc}")
    # apply migration logic or abort
```

### Checking Artifact Status

```python
from output_schema import (
    OutputManifest,
    ProvenanceRecord,
    resolve_manifest,
    ArtifactResolutionStatus,
)

manifest = OutputManifest.load_yaml("output/output_manifest.yaml")

current = ProvenanceRecord(
    framework_hash="new_fw_hash_abc",
    user_repo_hash="new_user_hash_xyz",
)

statuses = resolve_manifest(manifest, current_provenance=current)

for role, status in statuses.items():
    if status == ArtifactResolutionStatus.MUST_REGENERATE:
        print(f"  {role}: MUST REGENERATE (schema version mismatch)")
    elif status == ArtifactResolutionStatus.STALE:
        print(f"  {role}: stale (environment changed, regenerate when convenient)")
    else:
        print(f"  {role}: compatible")
```

### Merging Manifests

```python
from output_schema import (
    OutputManifest,
    merge_manifests,
    MergeInputValidationError,
)
import glob

manifest_paths = glob.glob("jobs/*/output_manifest.yaml")
manifests = [OutputManifest.load_yaml(p) for p in manifest_paths]

try:
    merged = merge_manifests(
        manifests,
        framework_hash="a1b2c3d4",
        required_roles=["histograms"],
    )
except MergeInputValidationError as exc:
    print(f"Cannot merge: {exc}")
    raise

# Run the actual file merge (e.g. hadd) here, then update output paths:
merged.histograms.output_file = "merged/merged_meta.root"
merged.save_yaml("merged/output_manifest.yaml")
```
