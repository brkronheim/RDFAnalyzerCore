# LAW Task System Reference

> **Quick start**: See [`law/README.md`](../law/README.md) for environment setup
> and an introduction to the broader batch-submission workflows.  This document
> is a detailed reference for the analysis-specific tasks defined in
> [`law/analysis_tasks.py`](../law/analysis_tasks.py) and their supporting modules.

---

## Table of Contents

1. [Overview](#1-overview)
2. [AnalysisMixin Parameters](#2-analysismixin-parameters)
3. [SkimTask](#3-skimtask)
4. [HistFillTask](#4-histfilltask)
5. [StitchingDerivationTask](#5-stitchingderivationtask)
6. [BranchMapPolicy](#6-branchmappolicy)
7. [Dask Executor](#7-dask-executor)
8. [Failure Handling](#8-failure-handling)
9. [Performance Recording](#9-performance-recording)
10. [Complete Workflow Example](#10-complete-workflow-example)

---

## 1. Overview

The LAW (Luigi Analysis Workflow) task system provides three high-level
pipeline tasks that orchestrate the C++ analysis executable over a set of
datasets defined in a YAML manifest:

| Task | Class | Purpose |
|------|-------|---------|
| Skim pass | `SkimTask` | Run exe per dataset to produce skimmed ROOT files |
| Histogram fill | `HistFillTask` | Fill histograms, optionally reading skim outputs |
| Stitching weights | `StitchingDerivationTask` | Derive MC stitching scale factors |

All three tasks live in `law/analysis_tasks.py` and are invoked with the
standard LAW command:

```bash
source law/env.sh
law run <TaskName> [parameters]
```

Supporting modules:

| Module | Purpose |
|--------|---------|
| `law/branch_map_policy.py` | Multi-dimensional branch map generation |
| `law/workflow_executors.py` | Dask distributed execution back-end |
| `law/failure_handler.py` | Smart retry logic with failure classification |
| `law/performance_recorder.py` | Wall-time, RSS and throughput metrics |

---

## 2. AnalysisMixin Parameters

`AnalysisMixin` is a Python mixin class that injects a common set of Luigi
parameters into `SkimTask` and `HistFillTask`.  Every parameter listed here is
available on both tasks.

### `--submit-config` *(str, required)*

Path to the `submit_config.txt` template file.  The task reads this file,
overrides the dataset-specific keys automatically, copies referenced auxiliary
files (`.txt`, `.yaml`) into the per-job directory, and writes a finalised
`submit_config.txt` for the executable.

Keys overridden automatically per dataset:

| Key | Value |
|-----|-------|
| `fileList` | Comma-separated list of input ROOT files (or DAS path) |
| `saveFile` | `<outputs_dir>/<dataset>/skim.root` |
| `metaFile` | `<outputs_dir>/<dataset>/meta.root` |
| `saveDirectory` | `<outputs_dir>/<dataset>/` |
| `sample` | Dataset name from the manifest |
| `type` | Dataset `dtype` field from the manifest (if set) |

All other keys in the template are forwarded verbatim.

### `--exe` *(str, required)*

Path to the compiled analysis executable.  The executable is invoked as:

```bash
<exe> submit_config.txt
```

from inside the per-job directory.  The path is resolved to an absolute path
before use.

### `--name` *(str, required)*

Run name used to namespace output directories.

- `SkimTask` writes to `skimRun_<name>/`
- `HistFillTask` writes to `histRun_<name>/`

### `--dataset-manifest` *(str, required)*

Path to the YAML dataset manifest file.  One workflow branch is created per
`DatasetEntry` in the manifest.

### `--num-workers` *(str, default: `"auto"`)*

Thread pool size for parallel branch execution under `--workflow local`.  The
special value `"auto"` lets LAW choose based on the number of CPU cores.

### `--log-level` *(str, default: `"info"`)*

Logging verbosity.  Valid values: `"debug"`, `"info"`, `"warning"`, `"error"`.

---

## 3. SkimTask

```python
class SkimTask(AnalysisMixin, law.LocalWorkflow)
```

### Purpose

Runs the analysis executable once per dataset to produce skimmed ROOT output
files.  Each branch corresponds to one `DatasetEntry` from the dataset manifest.

### Parameters

Inherits all [AnalysisMixin parameters](#2-analysismixin-parameters).  No
additional parameters.

### Branch Map

One branch per dataset entry (index 0 … N-1, sorted by dataset name).  The
`branch_data` object is a `DatasetEntry` with fields `name`, `files`, `das`,
and `dtype`.

For multi-dimensional branching (regions, systematics) use
[BranchMapPolicy](#6-branchmappolicy).

### Output Structure

```
skimRun_<name>/
├── jobs/
│   └── <dataset_name>/
│       └── submit_config.txt      ← per-dataset config written by the task
├── outputs/
│   └── <dataset_name>/
│       ├── skim.root              ← skimmed event tree
│       ├── meta.root              ← counter / meta histograms
│       └── .cache.yaml            ← provenance sidecar (written by task)
└── job_outputs/
    ├── <dataset_name>.done        ← completion marker (LAW output target)
    └── <dataset_name>.perf.json   ← performance metrics
```

### Caching and `complete()`

`SkimTask` overrides `complete()` to perform a provenance-based cache
validity check:

1. If `<dataset_name>.done` does not exist → **not complete**.
2. Reads `.cache.yaml` sidecar co-located with `skim.root`.
3. Compares stored provenance (framework git hash, config mtime, manifest
   hash) against current values.
4. If the cache is `COMPATIBLE` → **complete**.
5. If the cache is stale or absent, the `.done` marker is **deleted** and the
   task is rescheduled, guaranteeing that stale artifacts are regenerated.

This check only applies at branch level; at the workflow level the standard
LAW behaviour is preserved.

### Example Command

```bash
law run SkimTask \
    --submit-config analyses/myAnalysis/cfg/skim_config.txt \
    --exe build/analyses/myAnalysis/myanalysis \
    --name mySkimRun \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml
```

---

## 4. HistFillTask

```python
class HistFillTask(AnalysisMixin, law.LocalWorkflow)
```

### Purpose

Runs the analysis executable once per dataset to fill analysis histograms.
Optionally chains after a completed `SkimTask` by automatically wiring the
skim output as the input file list.

### Parameters

Inherits all [AnalysisMixin parameters](#2-analysismixin-parameters), plus:

#### `--skim-name` *(str, default: `""`)*

Name of a completed `SkimTask` run whose outputs should be used as input
for this histogram fill pass.

- **When set**: `HistFillTask` declares `SkimTask --name <skim-name>` as a
  dependency.  Before running each branch the task checks the skim cache
  validity and overrides `fileList` with
  `skimRun_<skim_name>/outputs/<dataset>/skim.root`.
- **When empty**: The `fileList` from the submit config template is used.  The
  dataset must have `files` or `das` defined in the manifest.

### Chain Mode

When `--skim-name` is set, `HistFillTask` validates the skim cache before
consuming it:

| Cache status | Behaviour |
|---|---|
| `COMPATIBLE` | Proceeds normally; logs confirmation |
| `STALE` | Issues a warning but proceeds; suggests re-running SkimTask |
| `MUST_REGENERATE` | Issues a warning and proceeds; provenance unverifiable |

### Output Structure

```
histRun_<name>/
├── jobs/
│   └── <dataset_name>/
│       └── submit_config.txt
├── outputs/
│   └── <dataset_name>/
│       └── (histogram ROOT files written by the executable)
└── job_outputs/
    ├── <dataset_name>.done
    └── <dataset_name>.perf.json
```

### Example Commands

**Standalone (reads files from manifest or submit config):**

```bash
law run HistFillTask \
    --submit-config analyses/myAnalysis/cfg/hist_config.txt \
    --exe build/analyses/myAnalysis/myanalysis \
    --name myHistRun \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml
```

**Chained after SkimTask:**

```bash
law run HistFillTask \
    --submit-config analyses/myAnalysis/cfg/hist_config.txt \
    --exe build/analyses/myAnalysis/myanalysis \
    --name myHistRun \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
    --skim-name mySkimRun
```

LAW will automatically run `SkimTask --name mySkimRun` if it has not yet
completed.

---

## 5. StitchingDerivationTask

```python
class StitchingDerivationTask(law.Task)
```

### Purpose

Derives binwise MC stitching scale factors from counter histograms written by
the framework's `CounterService`.  Outputs a
[correctionlib](https://github.com/cms-nanoAOD/correctionlib) `CorrectionSet`
JSON that can be consumed directly by `CorrectionManager`.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--stitch-config` | str | Yes | Path to YAML stitching configuration file |
| `--name` | str | Yes | Run name; output goes to `stitchWeights_<name>/` |

### Algorithm

For each MC sample group the task reads the
`counter_intWeightSignSum_<sample_name>` histogram from the merged meta ROOT
file.  For each bin *k* of the histogram the net signed count is:

```
C_n(k) = P_n(k) − N_n(k)
```

where *P_n* and *N_n* are the positive-weight and negative-weight event counts.

The per-sample, per-bin stitching scale factor is then:

```
b_n(k) = C_n(k) / Σ_m C_m(k)
```

Bins where all samples contribute zero events are assigned a scale factor of
0.  Trailing all-zero bins are trimmed to keep the output JSON compact.

**ROOT file reader**: `uproot` is the preferred reader (no ROOT installation
required).  PyROOT is used as a fallback in environments such as CMSSW where
`uproot` is unavailable.

### YAML Configuration Format

```yaml
groups:
  wjets_ht:                              # Group name → one Correction in the output
    meta_files:
      wjets_ht_0: /path/to/wjets_ht_0_meta.root   # sample_name: path
      wjets_ht_1: /path/to/wjets_ht_1_meta.root
      wjets_ht_2: /path/to/wjets_ht_2_meta.root
  dy_jets:
    meta_files:
      dy_jets_lo:  /path/to/dy_jets_lo_meta.root
      dy_jets_nlo: /path/to/dy_jets_nlo_meta.root
```

Each key under `meta_files` is the `sampleName` embedded in the histogram
name by `CounterService`.  This must exactly match the `sample` / `type` key
used in the `submit_config.txt` during the analysis run that produced the meta
ROOT file.

### Output: `stitchWeights_<name>/stitch_weights.json`

A correctionlib `CorrectionSet` (schema version 2) with one `Correction` per
group.  Each correction takes two inputs:

| Input | Type | Description |
|-------|------|-------------|
| `sample_name` | string | Sample identifier (key under `meta_files`) |
| `stitch_id` | int | Integer stitch bin from `counterIntWeightBranch` |

and returns the stitching scale factor `b_n(stitch_id)` as a `real`.

### Using the Output

**Python (correctionlib):**

```python
import correctionlib
cset = correctionlib.CorrectionSet.from_file("stitchWeights_myRun/stitch_weights.json")
sf = cset["wjets_ht"].evaluate("wjets_ht_0", int(stitch_id))
```

**C++ (CorrectionManager):**

In `submit_config.txt`, point `correctionlibConfig` at the JSON file and
call `CorrectionManager::evaluate("wjets_ht", sample_name, stitch_id)`.

### Example Command

```bash
law run StitchingDerivationTask \
    --stitch-config analyses/myAnalysis/cfg/stitch_config.yaml \
    --name myStitch
```

---

## 6. BranchMapPolicy

Source: `law/branch_map_policy.py`

The branch map system allows analysis tasks to branch over multiple dimensions
of the analysis parameter space.  By default tasks create one branch per
dataset, but regions and systematic scopes can be added as additional axes.

### `BranchingDimension` Enum

```python
class BranchingDimension(str, enum.Enum):
    DATASET          = "dataset"
    REGION           = "region"
    SYSTEMATIC_SCOPE = "systematic_scope"
```

| Value | Description |
|-------|-------------|
| `DATASET` | One branch per `DatasetEntry` in the dataset manifest.  Always active. |
| `REGION` | One branch per named region declared in `OutputManifest.regions`. |
| `SYSTEMATIC_SCOPE` | One branch per nuisance-group scope from `OutputManifest.nuisance_groups`. |

### `BranchMapEntry` Dataclass

```python
@dataclass(frozen=True)
class BranchMapEntry:
    dataset_name: str
    region: Optional[str] = None
    systematic_scope: Optional[str] = None
```

A `BranchMapEntry` is the `branch_data` object available inside a branch
task's `run()` method.  When a dimension is not active, the corresponding
field is `None`.

### `BranchingPolicy` Dataclass

```python
@dataclass
class BranchingPolicy:
    dimensions: List[BranchingDimension]          # default: [DATASET]
    max_branches: Optional[int] = None            # hard cap; None = no limit
    systematic_output_usage: Optional[str] = None # filter by output_usage string
    systematic_group_names: List[str] = []        # allow-list of group names
```

**`max_branches`**: When set, `generate_branch_map()` raises
`BranchMapGenerationError` if the computed number of branches would exceed this
value.  Always set this when deploying to a cluster with limited job slots.

**`systematic_output_usage`**: Only systematic groups whose `output_usage` list
contains this string are expanded.  Typical values: `"datacard"`,
`"histogram"`, `"plot"`.

**`systematic_group_names`**: When non-empty, only groups whose `name` appears
in this list are expanded.  Useful for targeted re-runs.

### Factory Methods

```python
# One branch per dataset (default behaviour)
policy = BranchingPolicy.dataset_only()

# One branch per dataset × region
policy = BranchingPolicy.dataset_and_regions(max_branches=200)

# One branch per dataset × region × systematic scope
policy = BranchingPolicy.dataset_regions_and_systematics(
    systematic_output_usage="datacard",
    systematic_group_names=["jet_energy", "b_tagging"],
    max_branches=1000,
)
```

### `from_config_str()` Compact Syntax

Policies can be passed as a compact string, e.g. via a Luigi parameter:

```python
policy = BranchingPolicy.from_config_str("dims=dataset:region,max_branches=100")
```

Supported keys:

| Key | Example | Description |
|-----|---------|-------------|
| `dims` | `dims=dataset:region` | Colon-separated dimension names |
| `max_branches` | `max_branches=200` | Integer cap, or `none` |
| `systematic_usage` | `systematic_usage=datacard` | `systematic_output_usage` value |
| `systematic_groups` | `systematic_groups=jet_energy:b_tagging` | Colon-separated allow-list |

Passing an empty string or `"default"` returns `BranchingPolicy.dataset_only()`.

### `generate_branch_map()` Function

```python
def generate_branch_map(
    policy: BranchingPolicy,
    dataset_manifest: DatasetManifest,
    output_manifest: Optional[OutputManifest] = None,
) -> Dict[int, BranchMapEntry]:
```

Returns a reproducible `{int: BranchMapEntry}` mapping.  The branch index
ordering is deterministic: datasets are sorted alphabetically as the outer
loop, then regions, then systematic scopes.

**Usage inside a LAW task:**

```python
def create_branch_map(self):
    policy = BranchingPolicy.from_config_str(self.branching_policy)
    ds_manifest = DatasetManifest.load(self.dataset_manifest)
    out_manifest = (
        OutputManifest.load_yaml(self.output_manifest)
        if self.output_manifest else None
    )
    return generate_branch_map(policy, ds_manifest, out_manifest)
```

### Scaling Guidance

The three dimensions multiply.  Use `max_branches` as a safety cap.

| Datasets (D) | Regions (R) | Groups (S) | Total branches | Recommended policy |
|:---:|:---:|:---:|:---:|---|
| 10 | 1 | 1 | **10** | `DATASET` only |
| 10 | 5 | 1 | **50** | `DATASET + REGION` |
| 10 | 5 | 8 | **400** | split systematics |
| 50 | 10 | 20 | **10 000** | ⚠ not recommended |

---

## 7. Dask Executor

Source: `law/workflow_executors.py`

### Overview

`DaskWorkflow` is a LAW workflow base class that dispatches branch tasks to a
[Dask distributed](https://distributed.dask.org/) cluster when
`--workflow dask` is passed on the command line.

```python
class RunMyJobs(MyMixin, law.LocalWorkflow, law.HTCondorWorkflow, DaskWorkflow):
    ...
```

Inheriting from `DaskWorkflow` alongside `law.LocalWorkflow` allows the same
task to support three execution back-ends selectable at runtime:

```bash
law run RunMyJobs --workflow local       # run branches in local thread pool
law run RunMyJobs --workflow htcondor   # submit via HTCondor
law run RunMyJobs --workflow dask \
    --dask-scheduler tcp://scheduler:8786   # dispatch to Dask cluster
```

### `DaskWorkflow` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--dask-scheduler` | str | `""` | Dask scheduler address, e.g. `tcp://host:8786`.  Empty = start a local cluster (testing only). |
| `--dask-workers` | int | `1` | Workers in the local cluster when `--dask-scheduler` is empty. |

These parameters are marked `significant=False` and are not forwarded to
branch tasks.

### `get_dask_work()` — Required Override

```python
def get_dask_work(self, branch_num: int, branch_data) -> tuple:
    """Return (callable, args, kwargs) for this branch."""
    raise NotImplementedError
```

The callable must be **picklable** (i.e. a module-level function, not a
lambda or closure).  `_run_analysis_job` from `workflow_executors.py` is the
standard picklable worker function used by `SkimTask` and `HistFillTask`.

**Example implementation:**

```python
from workflow_executors import DaskWorkflow, _run_analysis_job

class MySkimTask(MyMixin, law.LocalWorkflow, DaskWorkflow):

    def get_dask_work(self, branch_num, branch_data):
        exe = os.path.abspath(self.exe)
        job_dir = os.path.join(self._jobs_dir, branch_data.dataset_name)
        return _run_analysis_job, [exe, job_dir, "", ""], {}
```

### `DaskWorkflowProxy`

The proxy class `DaskWorkflowProxy` manages the Dask client lifecycle:

1. Creates or connects to the Dask scheduler.
2. Calls `task.get_dask_work(branch_num, branch_data)` for each incomplete
   branch and submits the returned callable as a Dask future.
3. As futures complete, writes the branch output `.done` file so LAW marks
   the branch complete.
4. Copies `job.perf.json` (written on the Dask worker by `_run_analysis_job`)
   to alongside the `.done` file for consistent performance data location.
5. On failure, invokes `run_with_retries()` to re-submit to Dask with
   per-category retry policies.
6. Emits the `DiagnosticSummary` report at the end of the run.

### `dask_retry_policies` Attribute

Override this class attribute to customise per-category retry policies:

```python
from failure_handler import DEFAULT_RETRY_POLICIES, FailureCategory, RetryPolicy

class MyTask(MyMixin, DaskWorkflow):
    dask_retry_policies = {
        **DEFAULT_RETRY_POLICIES,
        FailureCategory.TRANSIENT_IO: RetryPolicy(max_retries=10, base_delay=5.0),
    }
```

---

## 8. Failure Handling

Source: `law/failure_handler.py`

The failure handler module provides smart retry logic that classifies each
exception into a category and applies a per-category retry policy with
exponential backoff.

### `FailureCategory` Enum

```python
class FailureCategory(enum.Enum):
    TRANSIENT_IO    = "transient_io"
    EXECUTOR        = "executor"
    CONFIGURATION   = "configuration"
    CORRUPTED_INPUT = "corrupted_input"
    ANALYSIS_CRASH  = "analysis_crash"
    UNKNOWN         = "unknown"
```

| Category | Description | Typical causes |
|----------|-------------|----------------|
| `TRANSIENT_IO` | Temporary network or storage errors | XRootD timeout, EOS I/O error, CVMFS failure |
| `EXECUTOR` | Execution infrastructure failures | Dask worker crash, OOM kill, HTCondor hold |
| `CONFIGURATION` | Bad configuration | Missing executable, invalid YAML, permission denied |
| `CORRUPTED_INPUT` | Corrupt or truncated input data | Bad ROOT file, checksum mismatch |
| `ANALYSIS_CRASH` | C++ executable crash | Segfault, assertion failure, `std::exception` |
| `UNKNOWN` | Unclassified failure | Anything not matching above patterns |

### `DEFAULT_RETRY_POLICIES`

| Category | `max_retries` | `base_delay` (s) | `backoff_factor` |
|----------|:---:|:---:|:---:|
| `TRANSIENT_IO` | 5 | 10.0 | 2.0 |
| `EXECUTOR` | 3 | 30.0 | 2.0 |
| `CONFIGURATION` | 1 | 5.0 | 1.0 |
| `CORRUPTED_INPUT` | 1 | 5.0 | 1.0 |
| `ANALYSIS_CRASH` | 1 | 5.0 | 1.0 |
| `UNKNOWN` | 2 | 15.0 | 2.0 |

All policies have `jitter=True` by default (adds up to 25% random noise to
each delay to avoid thundering-herd effects when many branches retry
simultaneously).

### `classify_failure()`

```python
def classify_failure(exc: BaseException, stderr: str = "") -> FailureCategory:
```

Classifies an exception by:

1. Checking the Python exception type (e.g. `FileNotFoundError` → `CONFIGURATION`).
2. Searching the combined string `str(exc) + "\n" + stderr` for known
   error-message patterns (case-insensitive regex matching), tested in priority
   order: `ANALYSIS_CRASH` → `CORRUPTED_INPUT` → `TRANSIENT_IO` → `EXECUTOR`
   → `CONFIGURATION`.
3. Returning `UNKNOWN` if no pattern matched.

### `run_with_retries()`

```python
def run_with_retries(
    fn: Callable,
    args: list,
    kwargs: dict | None = None,
    branch_num: int = -1,
    summary: DiagnosticSummary | None = None,
    policies: dict[FailureCategory, RetryPolicy] | None = None,
) -> Any:
```

Executes `fn(*args, **kwargs)` with smart retry logic.  On each failure the
exception is classified and the policy for its category is consulted.  If
retries are exhausted the final exception is re-raised.

**Basic usage:**

```python
from failure_handler import run_with_retries, DiagnosticSummary, DEFAULT_RETRY_POLICIES

summary = DiagnosticSummary()
result = run_with_retries(
    fn=my_job_callable,
    args=[arg1, arg2],
    branch_num=42,
    summary=summary,
    policies=DEFAULT_RETRY_POLICIES,
)
print(summary.report())
```

### `DiagnosticSummary`

Accumulates `FailureRecord` objects and produces a structured report.

```python
summary = DiagnosticSummary()

# Add failure records manually or via run_with_retries
summary.add(FailureRecord(
    branch_num=3,
    attempt=1,
    category=FailureCategory.TRANSIENT_IO,
    message="XRootD timeout",
))

print(summary.report())
# DiagnosticSummary: 1 failure event(s) across 1 branch(es).
# Failures by category:
#   transient_io         :    1 event(s)
# Individual failure events:
#   [2024-01-15T12:00:00Z] branch=3 attempt=1 category=transient_io
#     XRootD timeout
```

Key methods:

| Method | Return type | Description |
|--------|-------------|-------------|
| `add(record)` | `None` | Append a `FailureRecord` |
| `records` | `list[FailureRecord]` | All recorded failure events |
| `by_category()` | `dict[FailureCategory, list[FailureRecord]]` | Grouped by category |
| `failed_branches()` | `set[int]` | Branch numbers with ≥1 failure |
| `total_failures()` | `int` | Total count of failure events |
| `report()` | `str` | Human-readable multi-line summary |

---

## 9. Performance Recording

Source: `law/performance_recorder.py`

### `PerformanceRecorder` Context Manager

```python
class PerformanceRecorder:
    task_name: str
    start_time: Optional[str]   # ISO 8601 UTC
    end_time: Optional[str]     # ISO 8601 UTC
    wall_time_s: Optional[float]
    peak_rss_mb: Optional[float]
    throughput_mbs: Optional[float]
```

Captures execution performance metrics for a named task unit.

**Basic timing:**

```python
from performance_recorder import PerformanceRecorder

with PerformanceRecorder("BuildSubmission") as rec:
    _do_build_work()
rec.save("/path/to/output.perf.json")
```

**Subprocess with memory monitoring:**

```python
with PerformanceRecorder("analysis_job:dataset_name") as rec:
    proc = subprocess.Popen(cmd, shell=True)
    rec.monitor_process(proc.pid)   # starts background RSS polling thread
    proc.communicate()
    rec.set_throughput(input_bytes=estimate_job_input_bytes(job_dir))
rec.save(os.path.join(job_dir, "job.perf.json"))
```

### Methods

#### `monitor_process(pid, poll_interval=0.25)`

Starts a background daemon thread that polls `/proc/<pid>/status` (and direct
children) every `poll_interval` seconds.  **Linux only**; no-op on other
platforms.  Must be called inside the `with` block after the subprocess has
been launched.

#### `set_throughput(input_bytes)`

Computes and stores `throughput_mbs = input_bytes / wall_time_s / 1024²`.
Call before or after context exit (uses elapsed time if called after).

#### `save(path)`

Writes a JSON file to `path` (parent directories created automatically):

```json
{
  "task_name": "SkimTask[ttbar]",
  "start_time": "2024-01-15T12:00:00.123456+00:00",
  "end_time":   "2024-01-15T12:05:23.456789+00:00",
  "wall_time_s": 323.333,
  "peak_rss_mb": 1842.5,
  "throughput_mbs": 12.847
}
```

### Helper Functions

#### `estimate_job_input_bytes(job_dir) → int`

Reads `submit_config.txt` inside `job_dir` and sums the sizes of all locally
accessible files listed under `fileList`.  Remote XRootD paths are skipped.
Returns `0` if the config is absent or all files are remote.

#### `perf_path_for(output_path) → str`

Derives the performance JSON path from a task output path by replacing the
`.done`, `.json`, or `.txt` suffix with `.perf.json`:

```python
perf_path_for("/jobs/job_3.done")     # → "/jobs/job_3.perf.json"
perf_path_for("/jobs/sample_0.json")  # → "/jobs/sample_0.perf.json"
```

---

## 10. Complete Workflow Example

This example shows a full end-to-end LAW workflow: skim pass → histogram fill
→ stitching weight derivation.

### Directory layout assumed

```
analyses/myAnalysis/
├── cfg/
│   ├── skim_config.txt        ← submit_config.txt template for skim pass
│   ├── hist_config.txt        ← submit_config.txt template for hist fill
│   ├── datasets.yaml          ← dataset manifest
│   └── stitch_config.yaml     ← stitching derivation config
build/
└── analyses/myAnalysis/
    └── myanalysis             ← compiled executable
```

### Step 1 — Environment setup

```bash
cd /path/to/RDFAnalyzerCore
source law/env.sh
```

### Step 2 — Run skim pass

```bash
law run SkimTask \
    --submit-config analyses/myAnalysis/cfg/skim_config.txt \
    --exe build/analyses/myAnalysis/myanalysis \
    --name v1 \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
    --workers 4
```

Outputs land in `skimRun_v1/`.  Caching ensures that unchanged datasets are
not re-processed on subsequent runs.

### Step 3 — Fill histograms (chained after skim)

```bash
law run HistFillTask \
    --submit-config analyses/myAnalysis/cfg/hist_config.txt \
    --exe build/analyses/myAnalysis/myanalysis \
    --name v1 \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
    --skim-name v1 \
    --workers 4
```

LAW checks that `SkimTask --name v1` is complete before starting.  Each
dataset's skim ROOT file is automatically wired as the input `fileList`.

### Step 4 — Derive MC stitching weights

```bash
law run StitchingDerivationTask \
    --stitch-config analyses/myAnalysis/cfg/stitch_config.yaml \
    --name v1
```

Output: `stitchWeights_v1/stitch_weights.json`

### Step 5 — Dispatch to Dask cluster (optional)

Replace `--workflow local` with `--workflow dask` for any task:

```bash
law run SkimTask \
    --submit-config analyses/myAnalysis/cfg/skim_config.txt \
    --exe build/analyses/myAnalysis/myanalysis \
    --name v1 \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
    --workflow dask \
    --dask-scheduler tcp://dask-scheduler.cluster.local:8786
```

The Dask proxy will submit one future per incomplete branch, apply smart
retry logic on failures, and print a `DiagnosticSummary` if any retries
occurred.

### Inspecting performance metrics

```bash
# Wall time and throughput for each dataset in the skim pass
for f in skimRun_v1/job_outputs/*.perf.json; do
    echo "--- $f ---"
    python -c "import json,sys; d=json.load(open('$f')); \
               print(f'  wall={d[\"wall_time_s\"]}s  rss={d[\"peak_rss_mb\"]}MB  \
throughput={d[\"throughput_mbs\"]}MB/s')"
done
```
