# Dataset Manifest

## Overview

The dataset manifest is the primary metadata registry for HEP dataset samples in RDFAnalyzerCore. It provides a rich, structured model that replaces flat sample-config text files with a fully-typed, queryable YAML format.

**What it stores:**

- Integrated luminosity (global and per-year)
- Per-sample physics normalisation: cross-section, filter efficiency, k-factors, sum of weights
- Dataset identity: year, era, campaign, physics process, sample group, stitching IDs
- File discovery: DAS paths for CMS NanoAOD, explicit file lists, XRootD URLs
- Dataset type: `"mc"` or `"data"`
- Derived dataset lineage via `parent` references
- Friend tree / sidecar attachments for extra branches from separate ROOT files

Analyses and LAW tasks query datasets through this metadata model rather than relying solely on flat file lists, enabling reproducible and auditable dataset selection.

---

## YAML Format

The top-level structure of a dataset manifest YAML file:

```yaml
lumi: 59740.0          # Global / fallback integrated luminosity in pb^-1
lumi_by_year:          # Optional per-year luminosities (take precedence over lumi)
  "2018": 59740.0
  "2022": 38010.0
  "2023": 27010.0
whitelist: []          # Rucio site whitelist (empty = all sites allowed)
blacklist: []          # Rucio site blacklist

datasets:
  - name: DYJets_2018
    year: 2018
    era: null           # null for MC
    campaign: Run2 UL
    process: DYJets
    group: DY
    dtype: mc
    xsec: 6077.22
    filter_efficiency: 1.0
    kfac: 1.0
    extra_scale: 1.0
    sum_weights: 1234567.0
    stitch_id: 0
    das: /DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM
    files: []
    parent: null
    friend_trees: []

  - name: SingleMuon_2018C
    year: 2018
    era: Run2018C
    campaign: null
    process: data
    group: data
    dtype: data
    xsec: null
    filter_efficiency: 1.0
    kfac: 1.0
    extra_scale: 1.0
    sum_weights: null
    stitch_id: null
    das: /SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD
    files: []
    parent: null
    friend_trees: []

  - name: DYJets_2018_skim
    year: 2018
    era: null
    campaign: Run2 UL
    process: DYJets
    group: DY
    dtype: mc
    xsec: 6077.22
    filter_efficiency: 1.0
    kfac: 1.0
    extra_scale: 1.0
    sum_weights: 1234567.0
    stitch_id: null
    das: null
    files:
      - /data/skims/DYJets_2018_skim.root
      - root://eosserver.cern.ch//eos/cms/DYJets_2018_skim.root
    parent: DYJets_2018
    friend_trees:
      - alias: calib
        tree_name: Events
        files:
          - /data/calib/DYJets_2018_calib.root
        directory: null
        globs:
          - .root
        antiglobs: []
        index_branches:
          - run
          - luminosityBlock
```

---

## DatasetEntry

A `DatasetEntry` is a Python dataclass (`@dataclass`) that models a single HEP dataset sample.

### Identity Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | *(required)* | Unique sample identifier. Used by submission scripts and LAW tasks. Must be unique within a manifest. |
| `year` | `int \| None` | `None` | Data-taking or MC production year (e.g. `2022`, `2023`). |
| `era` | `str \| None` | `None` | Run era within the year (e.g. `"Run2018C"`). Set to `None` for MC. |
| `campaign` | `str \| None` | `None` | MC production campaign tag (e.g. `"Run3Summer22NanoAODv12"`). |
| `process` | `str \| None` | `None` | Physics process label (e.g. `"ttbar"`, `"wjets"`, `"data"`). |
| `group` | `str \| None` | `None` | Sample grouping label for collecting stitched or HT-binned samples (e.g. `"wjets_ht"`). All samples sharing a group can be queried together with `DatasetManifest.query(group=...)`. |
| `stitch_id` | `int \| None` | `None` | Integer stitching code written into the event stream and read back by the `CounterService` `counterIntWeightBranch` mechanism. |

### Normalisation Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `xsec` | `float \| None` | `None` | Cross-section in pb. Leave `None` for real data. |
| `filter_efficiency` | `float` | `1.0` | Generator-level filter efficiency (fraction surviving the generator filter). |
| `kfac` | `float` | `1.0` | QCD k-factor to scale the LO cross-section to (N)NLO. |
| `extra_scale` | `float` | `1.0` | Additional arbitrary scale factor. |
| `sum_weights` | `float \| None` | `None` | Sum of generator weights. Computed at run-time when `None`. |

### Type Field

| Field | Type | Default | Description |
|---|---|---|---|
| `dtype` | `str` | `"mc"` | Dataset type. Either `"mc"` (simulation) or `"data"` (real data). |

### File Discovery Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `das` | `str \| None` | `None` | Comma-separated DAS path(s) for CMS NanoAOD Rucio discovery (e.g. `/TTto2L2Nu.../NANOAODSIM`). |
| `files` | `list[str]` | `[]` | Explicit list of ROOT file paths or XRootD URLs. Used instead of `das` for open-data or local file inputs. |

### Provenance Field

| Field | Type | Default | Description |
|---|---|---|---|
| `parent` | `str \| None` | `None` | Name of the parent `DatasetEntry` from which this dataset was derived. Enables lineage tracking for skimmed or filtered datasets. |

### Sidecar Field

| Field | Type | Default | Description |
|---|---|---|---|
| `friend_trees` | `list[FriendTreeConfig]` | `[]` | Friend tree / sidecar configurations to attach to this dataset. See [FriendTreeConfig](#friendtreeconfig). |

### Methods

| Method | Description |
|---|---|
| `validate()` | Not present as a standalone method on `DatasetEntry`; validation is performed at the manifest level via `DatasetManifest.validate()`. |
| `to_dict() -> dict` | Return the full entry as a plain Python dict suitable for YAML serialisation. `None` values are preserved. |
| `from_dict(data: dict) -> DatasetEntry` | Classmethod. Construct a `DatasetEntry` from a plain dict. Unknown keys are silently ignored for forward-compatibility. Nested `friend_trees` dicts are deserialised automatically. |
| `to_legacy_dict() -> dict[str, str]` | Return a `{key: str}` dict compatible with the legacy `getSampleList` / `_get_sample_list` format. Only non-`None` values are included; all values are stringified. |

---

## FriendTreeConfig

A `FriendTreeConfig` describes a sidecar or friend tree input attached to a dataset. The framework attaches the friend `TChain` to the main chain *before* the `RDataFrame` is created, making all friend branches immediately accessible.

Both local file paths and XRootD remote URLs (`root://...`) are supported in the `files` list.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `alias` | `str` | *(required)* | Alias used to access friend tree branches in ROOT. Branches from a friend registered as `"calib"` are accessible as `calib.pt`, `calib.eta`, etc. |
| `tree_name` | `str` | `"Events"` | Name of the `TTree` inside the friend ROOT file(s). |
| `files` | `list[str]` | `[]` | Explicit list of ROOT file paths or XRootD URLs. Takes precedence over `directory` when both are set. |
| `directory` | `str \| None` | `None` | Path to a local directory scanned recursively for ROOT files. Used when no explicit `files` list is provided. |
| `globs` | `list[str]` | `[".root"]` | Filename patterns to *include* when scanning `directory`. |
| `antiglobs` | `list[str]` | `[]` | Filename patterns to *exclude* when scanning `directory`. |
| `index_branches` | `list[str]` | `[]` | Branch names used as event identifiers for index-based matching. When non-empty, the framework calls `TChain::BuildIndex` using the first two entries as the major and minor index keys. |

### index_branches Common Configurations

| Value | Use case |
|---|---|
| `["run", "luminosityBlock"]` | Match on run and luminosity block |
| `["run", "event"]` | Match on run and event number |
| `[]` (empty) | Position-based (sequential-order) matching — the default ROOT friend tree behaviour |

### Use Case

Attach pre-computed calibration corrections or tagger outputs from separate ROOT files:

```yaml
friend_trees:
  - alias: calib
    tree_name: Events
    files:
      - /data/calib_2022.root
      - root://eosserver.cern.ch//eos/data/calib_remote.root
    index_branches:
      - run
      - luminosityBlock
```

### Methods

| Method | Description |
|---|---|
| `to_dict() -> dict` | Return the config as a plain Python dict suitable for YAML serialisation. |
| `from_dict(data: dict) -> FriendTreeConfig` | Classmethod. Construct from a plain dict. Unknown keys are silently ignored. |

---

## DatasetManifest API

`DatasetManifest` is a container for a collection of `DatasetEntry` objects with rich querying capabilities.

### Constructor

```python
DatasetManifest(
    datasets: list[DatasetEntry] | None = None,
    lumi: float = 1.0,
    whitelist: list[str] | None = None,
    blacklist: list[str] | None = None,
    lumi_by_year: dict[int, float] | None = None,
)
```

### Properties

| Attribute | Type | Description |
|---|---|---|
| `datasets` | `list[DatasetEntry]` | All registered dataset entries. |
| `lumi` | `float` | Global integrated luminosity in pb⁻¹. Used as the fallback when no per-year entry is found. |
| `lumi_by_year` | `dict[int, float]` | Optional per-year luminosity mapping (year → pb⁻¹). Takes precedence over the global `lumi` in `lumi_for()`. |
| `whitelist` | `list[str]` | Rucio site whitelist (empty = all sites allowed). |
| `blacklist` | `list[str]` | Rucio site blacklist. |

### Query Methods

#### `by_name(name: str) -> DatasetEntry | None`

Return the `DatasetEntry` with the given `name`, or `None` if not found.

```python
entry = manifest.by_name("ttbar_powheg_2022")
```

#### `query(**kwargs) -> list[DatasetEntry]`

Filter datasets by metadata criteria. All supplied keyword arguments are combined with logical AND. Each argument may be a single value or a list of values (list matches any entry whose field is contained in that list — logical OR within the field).

| Parameter | Type | Description |
|---|---|---|
| `year` | `int \| list[int]` | Filter by data-taking year. |
| `era` | `str \| list[str]` | Filter by run era. |
| `campaign` | `str \| list[str]` | Filter by MC production campaign. |
| `process` | `str \| list[str]` | Filter by physics process label. |
| `group` | `str \| list[str]` | Filter by sample group label. |
| `dtype` | `str \| list[str]` | Filter by dataset type (`"mc"` or `"data"`). |
| `parent` | `str \| list[str]` | Filter by parent dataset name. |

```python
# All 2022 MC ttbar samples
ttbar_2022 = manifest.query(year=2022, dtype="mc", process="ttbar")

# Samples across multiple years
multi = manifest.query(year=[2022, 2023], dtype="mc", process="ttbar")

# All samples in a stitched group
wjets = manifest.query(group="wjets_ht")
```

#### `lumi_for(year=None, era=None) -> float`

Return the luminosity in pb⁻¹ for the given year. Look-up order:

1. If `year` is given and present in `lumi_by_year`, return that value.
2. Otherwise fall back to the global `lumi`.

> **Note:** The `era` parameter is reserved for future per-era look-up and is currently ignored.

```python
lumi_2022 = manifest.lumi_for(year=2022)   # from lumi_by_year
lumi_all  = manifest.lumi_for()             # global lumi
```

#### Discovery helpers

| Method | Description |
|---|---|
| `get_groups() -> list[str]` | Sorted list of unique group labels across all datasets. |
| `get_processes() -> list[str]` | Sorted list of unique process labels. |
| `get_years() -> list[int]` | Sorted list of unique years. |
| `get_eras() -> list[str]` | Sorted list of unique era labels across all datasets. |
| `get_eras_for_year(year: int) -> list[str]` | Sorted list of unique era labels for a specific year. |

### I/O Methods

#### `DatasetManifest.load(path: str) -> DatasetManifest`

Classmethod. Auto-detect format and load a manifest:

- Files ending in `.yaml` / `.yml` are loaded as YAML manifests.
- All other files are interpreted as legacy key=value text configs.

#### `DatasetManifest.load_yaml(path: str) -> DatasetManifest`

Classmethod. Load a YAML manifest file.

#### `DatasetManifest.save_yaml(path: str) -> None`

Serialise the manifest to a YAML file. Parent directories are created automatically.

#### `DatasetManifest.file_hash(path: str) -> str`

Static method. Return the SHA-256 hex digest of the manifest file at `path` (64-character lowercase hex string, or `"<not found>"` if the file cannot be opened). Useful for recording the exact manifest revision used in a workflow task, enabling reproducible dataset selection.

```python
h = DatasetManifest.file_hash("datasets.yaml")
# "3a5f2b..." – stable identifier for this manifest revision
```

### Validation

#### `validate() -> list[str]`

Check the manifest for consistency and return a list of error/warning messages. An empty list means the manifest is consistent.

**Checks performed:**

- **Duplicate names** — every `DatasetEntry.name` must be unique.
- **Missing parent references** — if `entry.parent` is set, a `DatasetEntry` with that name must exist in the manifest.
- **Unknown `lumi_by_year` keys** — years in `lumi_by_year` that do not appear in any dataset are reported as `"Warning:"` messages.

```python
errors = manifest.validate()
if errors:
    for msg in errors:
        print(msg)
```

### Container Protocol

`DatasetManifest` supports `len()` and iteration:

```python
print(f"Manifest contains {len(manifest)} datasets")
for entry in manifest:
    print(entry.name)
```

---

## Python Examples

### Creating a Manifest Programmatically

```python
from dataset_manifest import DatasetManifest, DatasetEntry, FriendTreeConfig

entry_mc = DatasetEntry(
    name="ttbar_powheg_2022",
    year=2022,
    campaign="Run3Summer22NanoAODv12",
    process="ttbar",
    group="ttbar",
    dtype="mc",
    xsec=98.34,
    kfac=1.0,
    sum_weights=8_765_432.0,
    das="/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22NanoAODv12-130X_mcRun3_2022_realistic_v5-v2/NANOAODSIM",
)

entry_data = DatasetEntry(
    name="Muon_Run2022C",
    year=2022,
    era="Run2022C",
    process="data",
    group="data",
    dtype="data",
    das="/Muon/Run2022C-22Sep2023-v1/NANOAOD",
)

manifest = DatasetManifest(
    datasets=[entry_mc, entry_data],
    lumi=38010.0,
    lumi_by_year={2022: 38010.0, 2023: 27010.0},
)

errors = manifest.validate()
if errors:
    raise ValueError("Manifest invalid:\n" + "\n".join(errors))

manifest.save_yaml("datasets.yaml")
```

### Loading from YAML

```python
from dataset_manifest import DatasetManifest

manifest = DatasetManifest.load("datasets.yaml")

# Check the manifest revision
file_hash = DatasetManifest.file_hash("datasets.yaml")
print(f"Manifest revision: {file_hash}")

# Validate
errors = manifest.validate()
if errors:
    for msg in errors:
        print(f"  {msg}")
```

### Querying Datasets

```python
from dataset_manifest import DatasetManifest

manifest = DatasetManifest.load("datasets.yaml")

# Single dataset by name
ttbar = manifest.by_name("ttbar_powheg_2022")
if ttbar:
    print(f"ttbar xsec: {ttbar.xsec} pb")

# All MC samples for 2022 ttbar
samples = manifest.query(year=2022, dtype="mc", process="ttbar")
print(f"Found {len(samples)} ttbar MC samples")

# Samples from multiple years at once
run3 = manifest.query(year=[2022, 2023], dtype="mc")
print(f"Run 3 MC samples: {len(run3)}")

# All samples in a stitched W+jets group
wjets = manifest.query(group="wjets_ht")

# Per-year luminosity
lumi_2022 = manifest.lumi_for(year=2022)
lumi_2023 = manifest.lumi_for(year=2023)
lumi_total = lumi_2022 + lumi_2023
print(f"Total Run 3 lumi: {lumi_total:.0f} pb^-1")
```

### Iterating Over MC and Data Datasets Separately

```python
from dataset_manifest import DatasetManifest

manifest = DatasetManifest.load("datasets.yaml")

mc_samples = manifest.query(dtype="mc")
data_samples = manifest.query(dtype="data")

print("MC samples:")
for sample in mc_samples:
    lumi = manifest.lumi_for(year=sample.year)
    effective_xsec = (
        sample.xsec * sample.filter_efficiency * sample.kfac * sample.extra_scale
        if sample.xsec is not None
        else None
    )
    print(f"  {sample.name}: xsec_eff = {effective_xsec:.4f} pb" if effective_xsec else f"  {sample.name}")

print("\nData samples:")
for sample in data_samples:
    print(f"  {sample.name} (era: {sample.era}, year: {sample.year})")

# Discover all unique years and eras
print(f"\nYears in manifest: {manifest.get_years()}")
for year in manifest.get_years():
    print(f"  {year}: eras = {manifest.get_eras_for_year(year)}")
```

### Working with Friend Trees

```python
from dataset_manifest import DatasetManifest, DatasetEntry, FriendTreeConfig

entry = DatasetEntry(
    name="DYJets_2022",
    year=2022,
    process="DYJets",
    dtype="mc",
    xsec=6077.22,
    das="/DYJetsToLL_M-50_TuneCP5_13p6TeV-amcatnloFXFX-pythia8/Run3Summer22NanoAODv12-130X_mcRun3_2022_realistic_v5-v2/NANOAODSIM",
    friend_trees=[
        FriendTreeConfig(
            alias="calib",
            tree_name="Events",
            files=[
                "/data/calib/DYJets_2022_jerc.root",
                "root://eosserver.cern.ch//eos/cms/DYJets_2022_jerc_remote.root",
            ],
            index_branches=["run", "luminosityBlock"],
        ),
        FriendTreeConfig(
            alias="tagger",
            tree_name="Events",
            directory="/data/taggers/2022/",
            globs=[".root"],
            antiglobs=["_failed"],
        ),
    ],
)

# Serialise
d = entry.to_dict()
# Load back
from dataset_manifest import DatasetEntry
reloaded = DatasetEntry.from_dict(d)
assert reloaded.friend_trees[0].alias == "calib"
```

---

## Integration with LAW Tasks

LAW workflow tasks (`PrepareNANOSample`, `BuildNANOSubmission`, `RunNANOAnalysisJob`, …) accept the dataset manifest via the `--dataset-manifest` command-line parameter:

```bash
law run RunNANOAnalysisJob \
    --dataset-manifest datasets.yaml \
    --config config/analysis.yaml \
    --sample DYJets_2022 \
    --branch 0
```

Within a LAW task, the manifest is typically loaded, queried, and the resolved dataset selection is recorded in `DatasetManifestProvenance` for full reproducibility:

```python
from dataset_manifest import DatasetManifest
from output_schema import DatasetManifestProvenance, OutputManifest

# Load the manifest
manifest = DatasetManifest.load(self.dataset_manifest)
manifest_hash = DatasetManifest.file_hash(self.dataset_manifest)

# Query the relevant subset
query = {"year": self.year, "dtype": "mc", "process": self.process}
entries = manifest.query(**query)

# Record provenance for the output manifest
prov = DatasetManifestProvenance(
    manifest_path=self.dataset_manifest,
    manifest_hash=manifest_hash,
    query_params=query,
    resolved_entry_names=[e.name for e in entries],
)

output_manifest = OutputManifest(
    skim=...,
    dataset_manifest_provenance=prov,
    framework_hash=current_fw_hash,
)
output_manifest.save_yaml(self.output_manifest_path)
```

The `DatasetManifestProvenance` records:

| Field | Description |
|---|---|
| `manifest_path` | Path to the manifest file. |
| `manifest_hash` | SHA-256 hex digest of the manifest file (from `DatasetManifest.file_hash()`). |
| `query_params` | Dict of keyword arguments passed to `DatasetManifest.query()`. |
| `resolved_entry_names` | Ordered list of `DatasetEntry.name` values selected by the query. |

This makes every dataset selection fully auditable and reproducible: given the manifest hash and query parameters, the resolved entry list can be independently verified.

---

## Legacy Format Compatibility

For backward compatibility, `DatasetManifest.load()` also accepts the legacy key=value text format used by older submission scripts:

```text
lumi=59740.0
WL=T2_DE_RWTH,T2_CH_CERN
BL=T2_US_Vanderbilt

name=DYJets_2018 xsec=6077.22 das=/DYJetsToLL/... type=mc kfac=1.0 norm=1234567.0 year=2018
name=SingleMuon_2018C das=/SingleMuon/... type=data year=2018 era=Run2018C
```

Global directives:
- `lumi=<float>` — integrated luminosity
- `WL=site1,site2` — site whitelist
- `BL=site1,site2` — site blacklist

Per-sample recognised fields: `name`, `xsec`, `das`, `type` (`"mc"`/`"data"`), `kfac`, `extraScale`, `filterEfficiency`, `norm` (sum of weights), `year`, `era`, `campaign`, `process`, `group`, `stitch_id`, `parent`, `fileList` (comma-separated file paths).

The YAML format is preferred for new analyses; the legacy format is maintained solely for compatibility with existing submission scripts.
