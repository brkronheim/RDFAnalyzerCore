# LAW Workflows for Batch Submission

This folder contains three LAW-based workflows:

- **[nano_tasks.py](nano_tasks.py)** – CMS NanoAOD data via Rucio
- **[opendata_tasks.py](opendata_tasks.py)** – CERN Open Data Portal
- **[combine_tasks.py](combine_tasks.py)** – CMS Combine datacards and fits

Both the NANO and Open Data workflows share the same four-task structure and
identical parameter conventions.  The Combine workflow provides two tasks that
integrate with their outputs.

---

## Table of Contents

- [Overview](#overview)
- [Local Workflow Tasks](#local-workflow-tasks)
  - [SkimTask](#skimtask)
  - [HistFillTask](#histfilltask)
  - [StitchingDerivationTask](#stitchingderivationtask)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [NANO / Rucio Workflow](#nano--rucio-workflow)
- [CERN Open Data Workflow](#cern-open-data-workflow)
- [Combine Workflow](#combine-workflow)
- [End-to-End Pipeline: From Data to Statistical Results](#end-to-end-pipeline-from-data-to-statistical-results)
- [Common Parameters](#common-parameters)
- [Important Behavior Notes](#important-behavior-notes)
- [Monitoring and Resubmission](#monitoring-and-resubmission)
- [Advanced Topics](#advanced-topics)
  - [Branch Map Policy](#branch-map-policy)
  - [Dask Executor](#dask-executor)
  - [Performance Records](#performance-records)

---

## Local Workflow Tasks

`analysis_tasks.py` provides the following LAW tasks for running the analysis
framework **locally** (or dispatching to HTCondor/Dask).  They are simpler
than the NANO/OpenData batch workflows and are the recommended starting point
for new analyses.  See [docs/LAW_TASKS.md](../docs/LAW_TASKS.md) for the
complete parameter reference.

```bash
source law/env.sh
law index
```

### SkimTask

Runs the analysis executable once per dataset entry defined in a dataset
manifest YAML.  Each branch executes one dataset job and writes a `.done`
marker file when finished.

A **pre-flight local test job** (``RunSkimTestJob``) is run automatically
before the full workflow is dispatched.  It executes the analysis on a single
input file, catching runtime errors before potentially hundreds of jobs are
submitted.  The test can be skipped with ``--no-make-test-job``.

**Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--submit-config` | *(required)* | Path to `submit_config.txt` template |
| `--exe` | *(required)* | Path to the compiled analysis executable |
| `--name` | *(required)* | Run name; outputs go to `skimRun_<name>/` |
| `--dataset-manifest` | *(required)* | Path to the YAML dataset manifest |
| `--make-test-job` | `True` | Run a single-file test before full dispatch |
| `--file-source` | `""` | File-list source: `""`, `nano`, `opendata`, or `xrdfs` |
| `--file-source-name` | `""` | Name matching a completed file-list task run |
| `--num-workers` | auto | Thread-pool size for the local runner |

**Output structure**

```
skimRun_<name>/
  test_job/            ← Pre-flight test outputs (single file)
  outputs/<dataset>/   ← ROOT skim and meta files
  job_outputs/         ← <dataset_name>.done marker files
                         <dataset_name>.perf.json performance records
```

**Example**

```bash
# Standard mode: one branch per manifest entry, with pre-flight test
law run SkimTask \
  --submit-config cfg/submit_config.txt \
  --exe ./build/MyAnalysis \
  --name myRun \
  --dataset-manifest datasets.yaml

# Skip the pre-flight test
law run SkimTask \
  --submit-config cfg/submit_config.txt \
  --exe ./build/MyAnalysis \
  --name myRun \
  --dataset-manifest datasets.yaml \
  --no-make-test-job

# File-source mode: SkimTask can trigger GetNANOFileList automatically
law run SkimTask \
  --submit-config cfg/submit_config.txt \
  --exe ./build/MyAnalysis \
  --name myRun \
  --dataset-manifest datasets.yaml \
  --file-source nano

# XRDFS mode can chain automatically from GetXRDFSFileList
law run SkimTask \
  --submit-config cfg/submit_config.txt \
  --exe ./build/MyAnalysis \
  --name myRun \
  --dataset-manifest datasets.yaml \
  --file-source xrdfs \
  --file-source-name myFiles
```

### RunSkimTestJob

Runs the analysis executable with a **single input file** to validate that the
analysis compiles and runs without runtime errors before submitting the full
workflow.  This task is required automatically by ``SkimTask`` when
``--make-test-job`` is set (the default).

In **file-source mode** it uses the ``PrepareSkimJobs`` output directory.  The
``xrdfs``, ``nano``, and ``opendata`` backends can all be chained automatically.
When ``--file-source-name`` is omitted, the task falls back to ``--name`` for
the upstream file-list run.  In **standard manifest mode** it creates the test
directory itself from the first dataset's first file.

**Output**: ``skimRun_<name>/test_job/test_passed.txt``

### PrepareSkimJobs

Creates per-job configuration directories and the shared ``shared_inputs/``
directory (executable, libraries, x509 proxy) for use by ``SkimTask`` in
file-source mode.

When ``--file-source xrdfs`` is set, ``PrepareSkimJobs`` automatically
declares a LAW dependency on :class:`GetXRDFSFileList` so that the full chain
``GetXRDFSFileList → PrepareSkimJobs → RunSkimTestJob → SkimTask``
runs automatically.

### HistFillTask

Runs the analysis executable to fill analysis histograms, one branch per
dataset.  When `--skim-name` is supplied the task declares a LAW dependency
on the matching `SkimTask` and automatically uses its output directory as the
input file list for each dataset job.

**Parameters** – identical to `SkimTask`, plus:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--skim-name` | `""` | Name of a completed `SkimTask` run to chain from |

**Examples**

```bash
# Standalone (no SkimTask dependency)
law run HistFillTask \
  --submit-config law/submit_config.txt \
  --exe ./build/MyHistFill \
  --name myHist \
  --dataset-manifest datasets.yaml

# Chained after SkimTask – inputs are taken from skimRun_myRun/outputs/
law run HistFillTask \
  --submit-config law/submit_config.txt \
  --exe ./build/MyHistFill \
  --name myHist \
  --dataset-manifest datasets.yaml \
  --skim-name myRun
```

Output goes to `histRun_<name>/outputs/` and `histRun_<name>/job_outputs/`.

### StitchingDerivationTask

Derives binwise MC stitching scale factors from the counter histograms written
by the framework's `CounterService`.  Useful for samples split across multiple
generators or phase-space slices (e.g. HT-binned W+jets, pT-binned DY).

**Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--stitch-config` | *(required)* | Path to the YAML stitch configuration file |
| `--name` | *(required)* | Name for the output directory |

**Stitch config format**

```yaml
groups:
  wjets_ht:
    meta_files:
      wjets_ht_0: skimRun_myRun/outputs/WJets_HT0to70/meta.root
      wjets_ht_1: skimRun_myRun/outputs/WJets_HT70to100/meta.root
      wjets_ht_2: skimRun_myRun/outputs/WJets_HT100to200/meta.root
```

Each key under `meta_files` is the `sampleName` used by `CounterService` when
booking `counter_intWeightSignSum_{sampleName}` histograms.

**Output**: `stitchWeights_<name>/stitch_weights.json` — a
[correctionlib](https://github.com/cms-nanoAOD/correctionlib) `CorrectionSet`
with one `Correction` per stitching group.

**Algorithm**: reads `counter_intWeightSignSum_*` histograms from each meta
ROOT file, computes per-bin signed counts `C_n(k) = P_n(k) − N_n(k)`, and
derives the scale factor:

```
b_n(k) = C_n(k) / Σ_m C_m(k)
```

Usage from Python:

```python
import correctionlib
cset = correctionlib.CorrectionSet.from_file("stitchWeights_myStitch/stitch_weights.json")
sf = cset["wjets_ht"].evaluate("wjets_ht_0", stitch_id)
```

---

## File-List Discovery Tasks

These tasks are `law.LocalWorkflow` tasks that run **one branch per dataset**,
so file discovery across all datasets is **fully parallelized**.

### GetXRDFSFileList

Discovers ROOT files on XRootD storage (EOS, dCache, …) using `xrdfs ls`.
Directory listings are performed at the directory level and sub-directory
traversal is parallelized using a thread pool, making discovery of large
directory trees much faster than serial recursive listing.

```bash
law run GetXRDFSFileList \
  --name myFiles \
  --dataset-manifest datasets.yaml \
  --xrdfs-server root://eosuser.cern.ch/ \
  --workers 4
```

Output: `xrdfsFileList_<name>/<dataset_name>.json` per dataset.

When ``PrepareSkimJobs`` is run with ``--file-source xrdfs
--file-source-name myFiles``, it automatically depends on this task.

### GetNANOFileList

Queries Rucio for CMS NanoAOD file URLs (one branch per sample).  When a
sample specifies multiple DAS paths, the Rucio queries are **parallelized
across DAS entries** for speed.

```bash
law run GetNANOFileList \
  --submit-config cfg/submit_config.txt \
  --name myFiles \
  --exe build/MyAnalysis \
  --x509 /tmp/x509 \
  --workers 4
```

Output: `nanoFileList_<name>/<sample_name>.json` per sample.

### GetOpenDataFileList

Fetches file lists from the CERN Open Data Portal (one branch per sample,
parallelized via LAW's `LocalWorkflow`).

```bash
law run GetOpenDataFileList \
  --submit-config cfg/opendata_config.txt \
  --name myFiles \
  --exe build/MyAnalysis \
  --workers 4
```

Output: `openDataFileList_<name>/<sample_name>.json` per sample.

---

## Overview

Each workflow is split into four law tasks that run in sequence:

1. **Prepare\*Sample** *(LocalWorkflow)* – one branch per sample; discovers input
   files and creates per-job configuration directories.
2. **Build\*Submission** *(Task)* – assembles the condor submission: copies the
   executable, `aux/` directory, x509 proxy, and local shared libraries into
   `shared_inputs/`; creates symlinks `job_0…job_{N-1}`; writes
   `condor_submit.sub` and `condor_runscript.sh`.
3. **Submit\*Jobs** *(Task)* – runs `condor_submit` and records the cluster ID.
4. **Monitor\*Jobs** *(Task)* – blocking polling loop that verifies EOS outputs,
   resubmits held/failed jobs (up to `--max-retries` times), and writes
   `all_outputs_verified.txt` when every job is confirmed complete.

Outputs are always staged to EOS after each job completes (stage-out is always
active).  Stage-in (copying inputs to the worker node before running) is
optional and controlled by `--stage-in`.

---

## Prerequisites

```bash
# From the repository root
source env.sh
pip install --user law luigi requests
```

For NANO submissions, a valid VOMS proxy is also required:

```bash
voms-proxy-init -voms cms -rfc --valid 168:0
```

For Open Data submissions, no proxy is needed (Open Data is public). The
optional `cernopendata-client` speeds up metadata fetching:

```bash
pip install --user cernopendata-client
```

---

## Environment Setup

Run once per shell session from the `law/` directory:

```bash
source law/env.sh
```

This sets `PYTHONPATH`, `LAW_HOME`, and `LAW_CONFIG_FILE` automatically.

After the first run (or after adding new modules), index the task registry:

```bash
law index
```

---

## NANO / Rucio Workflow

### Sample config format

```
# lumi in pb^-1
lumi=59700

# Optional site whitelist / blacklist
WL=T2_US_MIT,T2_US_Wisconsin
BL=T3_US_FNALLPC

# One line per sample
name=ttbar das=/TTTo.../NANOAODSIM xsec=831.76 type=0 norm=1.0 kfac=1.0
name=wjets das=/WJets.../NANOAODSIM xsec=61526.7 type=1 norm=1.0 kfac=1.0
```

### Step-by-step

```bash
# 1. Discover files and create per-job directories (one branch per sample)
law run PrepareNANOSample --workers 4 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --name myRun22 \
  --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --root-setup env.sh \
  --container-setup cmssw-el9 \
  --stage-in \
  --size 30

# 2. Build condor submission files
law run BuildNANOSubmission --workers 1 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --name myRun22 \
  --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --root-setup env.sh \
  --container-setup cmssw-el9 \
  --stage-in \
  --size 30

# 3. Submit jobs to HTCondor
law run SubmitNANOJobs --workers 1 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --name myRun22 \
  --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --root-setup env.sh \
  --container-setup cmssw-el9 \
  --stage-in \
  --size 30

# 4. Monitor jobs (blocking; resubmits failed/held jobs automatically)
law run MonitorNANOJobs --workers 1 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --name myRun22 \
  --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --root-setup env.sh \
  --container-setup cmssw-el9 \
  --stage-in \
  --size 30
```

Because law tracks task outputs, you can run `SubmitNANOJobs` directly and it
will automatically run `PrepareNANOSample` and `BuildNANOSubmission` first if
they have not yet completed.

### NANO-specific parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--x509` | *(required)* | Path to VOMS x509 proxy |
| `--size` | `30` | GB of data per condor job |
| `--max-runtime` | `3600` | Max job runtime (seconds) |

---

## CERN Open Data Workflow

### Sample config format

```
lumi=11580

# Record IDs from CERN Open Data Portal
recids=24119,24120

# One line per sample (das= must match the key in the Open Data file index)
name=ZMuMu das=SMZee_0 type=10 xsec=1.0 norm=1.0
name=ZEE   das=SMZee_1 type=11 xsec=1.0 norm=1.0
```

### Step-by-step

```bash
# 1. Discover files and create per-job directories
law run PrepareOpenDataSample --workers 4 \
  --submit-config analyses/myAnalysis/cfg/opendata_config.txt \
  --name openDataRun \
  --exe build/analyses/myAnalysis/myanalysis \
  --files 20

# 2. Build condor submission files
law run BuildOpenDataSubmission --workers 1 \
  --submit-config analyses/myAnalysis/cfg/opendata_config.txt \
  --name openDataRun \
  --exe build/analyses/myAnalysis/myanalysis \
  --files 20

# 3. Submit jobs
law run SubmitOpenDataJobs --workers 1 \
  --submit-config analyses/myAnalysis/cfg/opendata_config.txt \
  --name openDataRun \
  --exe build/analyses/myAnalysis/myanalysis \
  --files 20

# 4. Monitor
law run MonitorOpenDataJobs --workers 1 \
  --submit-config analyses/myAnalysis/cfg/opendata_config.txt \
  --name openDataRun \
  --exe build/analyses/myAnalysis/myanalysis \
  --files 20
```

### Open Data-specific parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--files` | `30` | Number of ROOT files per condor job |
| `--x509` | `""` | Path to x509 proxy (optional) |
| `--max-runtime` | `1200` | Max job runtime (seconds) |

---

## Combine Workflow

`combine_tasks.py` provides two law tasks for generating CMS Combine datacards
from analysis outputs and running statistical fits.

### Overview

1. **CreateDatacard** – reads histograms from the ROOT files listed in a YAML
   configuration file, combines samples, applies systematics, and writes one
   `datacard_<region>.txt` + `shapes_<region>.root` per control region into
   `combineRun_<name>/datacards/`.

2. **RunCombine** – discovers the datacards produced by `CreateDatacard` (or
   accepts an explicit `--datacard-path`) and runs `combine -M <method>` on
   each one, writing output ROOT files and logs to
   `combineRun_<name>/combine_results/`.

The YAML config format is documented in
[docs/DATACARD_GENERATOR.md](../docs/DATACARD_GENERATOR.md).  An example
configuration is available at
[core/python/example_datacard_config.yaml](../core/python/example_datacard_config.yaml).

### Prerequisites

```bash
# Build with Combine support
cmake -S . -B build -DBUILD_COMBINE=ON
cmake --build build -j$(nproc)

# Or source a CMSSW environment that provides combine
```

### Step-by-step

```bash
source law/env.sh

# 1. Generate datacards from analysis output ROOT files
law run CreateDatacard \
  --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \
  --name myRun

# 2. Run Combine fits (automatically runs CreateDatacard first if needed)
law run RunCombine \
  --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \
  --name myRun \
  --method AsymptoticLimits

# Run on a specific datacard file without requiring CreateDatacard
law run RunCombine \
  --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \
  --name myRun \
  --datacard-path combineRun_myRun/datacards/datacard_signal_region.txt \
  --method FitDiagnostics \
  --combine-options "--saveShapes --saveWithUncertainties"
```

Because law tracks task outputs, running `RunCombine` will automatically
trigger `CreateDatacard` first if it has not yet completed.

### Combine-specific parameters

#### CreateDatacard

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--datacard-config` | *(required)* | Path to the YAML datacard configuration file |
| `--name` | *(required)* | Run name; outputs go to `combineRun_<name>/datacards/` |

#### RunCombine

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--datacard-config` | *(required)* | Path to the YAML datacard configuration file |
| `--name` | *(required)* | Run name; results go to `combineRun_<name>/combine_results/` |
| `--method` | `AsymptoticLimits` | Combine fit method (e.g. `FitDiagnostics`, `Significance`, `MultiDimFit`) |
| `--datacard-path` | `""` | Run Combine on this specific datacard instead of auto-discovering from `CreateDatacard` output |
| `--combine-exe` | `""` | Path to the `combine` binary; auto-detected if empty |
| `--combine-options` | `""` | Extra options forwarded verbatim to `combine` |

### Integration with analysis run tasks

The YAML config passed to `--datacard-config` references the ROOT files
produced by the NANO or Open Data batch workflows.  A typical config looks
like:

```yaml
output_dir: datacards  # overridden by CreateDatacard at runtime

input_files:
  signal:
    path: /eos/user/me/myRun22/signal.root
    type: signal
  ttbar:
    path: /eos/user/me/myRun22/ttbar.root
    type: background
  data_obs:
    path: /eos/user/me/myRun22/data.root
    type: data

control_regions:
  signal_region:
    observable: mT
    processes: [signal, ttbar]
    signal_processes: [signal]
    data_process: data_obs
```

---

## End-to-End Pipeline: From Data to Statistical Results

The LAW workflows cover the **complete** physics analysis pipeline.  The diagram
below shows how the tasks connect, including the new pre-flight test job and
automatic file-list chaining:

```
──── File Discovery ────────────────────────────────────────────────────

GetXRDFSFileList  (parallel, one branch per dataset)
GetNANOFileList   (parallel, one branch per sample; DAS queries parallelized)
GetOpenDataFileList (parallel, one branch per sample)

──── Skim Preparation ──────────────────────────────────────────────────

PrepareSkimJobs    ─── depends on GetXRDFSFileList (automatic)
   │                   or on GetNANO/OpenDataFileList (run manually first)
   ▼
RunSkimTestJob     ─── pre-flight: runs analysis on 1 file; blocks full dispatch
   │
   ▼

──── Skim Dispatch ─────────────────────────────────────────────────────

SkimTask   (LocalWorkflow / HTCondor / Dask)
  │  one branch per dataset; writes skimRun_<name>/outputs/<dataset>/
  │
  OR
  │
NanoTask / OpenDataTask  (HTCondor batch)
  │  (batch jobs, HTCondor)  ─── Skims raw data into per-dataset ROOT files
  ▼
MonitorTask                ─── blocks until all jobs done;
                               EOS file checks done at directory level (fast)

──── Histogram Stage ───────────────────────────────────────────────────

HistFillTask (local)       ── fills N-dimensional histograms per dataset
  │  (reads skim outputs from SkimTask via --skim-name)
  │
  ├─► StitchingDerivationTask  ── derives per-bin stitching scale factors
  │
  ▼
CreateDatacard             ── assembles CMS Combine datacards + shape files
  │  (reads merged histogram ROOT files)
  ▼
RunCombine                 ── runs combine -M <method> for each datacard
     (AsymptoticLimits, FitDiagnostics, Significance, MultiDimFit, ...)
```

### FullAnalysisDAG: one command for everything

`FullAnalysisDAG` in `dag_tasks.py` orchestrates the entire pipeline from
ingestion/skim through fit as a single law task.  When `--file-source` is set,
the DAG forwards it to `SkimTask`, which automatically chains the corresponding
local ingestion task (`GetNANOFileList`, `GetOpenDataFileList`, or
`GetXRDFSFileList`) before dispatching skim jobs.  Histogram filling is wired
to the skim outputs, and manifest-based plot/datacard/fit tasks wait for the
merge stage automatically:

```bash
law run FullAnalysisDAG \
  --name myRun \
  --exe build/analyses/myAnalysis/myanalysis \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
  --hist-config analyses/myAnalysis/cfg/hist_config.txt \
  --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \
  --fitting-backend analysis

# Include NanoAOD ingestion in the same DAG invocation
law run FullAnalysisDAG \
  --name myNanoRun \
  --exe build/analyses/myAnalysis/myanalysis \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
  --hist-config analyses/myAnalysis/cfg/hist_config.txt \
  --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \
  --file-source nano
```

Skip stages you don't need:

```bash
# Start from pre-existing merged outputs
law run FullAnalysisDAG \
  --name myRun \
  --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \
  --manifest-path mergeRun_myRun/histograms/output_manifest.yaml \
  --skip-skim \
  --skip-histfill \
  --skip-merge
```

### Histogram Backend and Downstream Compatibility

The `NDHistogramManager` supports two backends (configured via `histogramBackend`
in the analysis config):

- **`root`** (default) — produces `THnSparseF` objects stored in ROOT files.
  This format is required for `CreateDatacard` and all standard ROOT-based
  downstream tools.
- **`boost`** — uses Boost.Histogram during the event loop for reduced memory
  usage; output is converted to ROOT format before writing, so it is fully
  compatible with downstream tools including Combine.

Both backends write identical output files; `histogramBackend` is purely an
internal performance knob.  See [CONFIG_REFERENCE.md — Histogram Backend](../docs/CONFIG_REFERENCE.md#histogram-backend)
for configuration details.

---

## Common Parameters

These parameters apply to **both** NANO and Open Data workflows:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--submit-config` | *(required)* | Path to submit config file |
| `--name` | *(required)* | Submission name; creates `condorSub_{name}/` |
| `--exe` | *(required)* | Path to the compiled C++ executable |
| `--dataset` | `""` | Restrict to a single named dataset (see [Per-dataset execution](#per-dataset-execution)) |
| `--stage-in` | `False` | xrdcp input files to worker before running |
| `--root-setup` | `""` | Path to a setup script; contents embedded in inner runscript |
| `--container-setup` | `""` | Container command for outer wrapper (e.g. `cmssw-el9`) |
| `--python-env` | `""` | Path to Python environment tarball (see below) |
| `--no-validate` | `False` | Skip submit-config validation |

### Monitor-only parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--max-retries` | `3` | Max resubmission attempts per failed job |
| `--poll-interval` | `120` | Seconds between condor_q polls |

---

## Per-dataset execution

By default, every dataset listed in the `sampleConfig` file is prepared and
submitted as part of a single workflow run.  For large campaigns it is often
preferable to submit each dataset as its own independent Law task so that:

* datasets can be scheduled, monitored, and retried independently;
* a failure in one dataset does not block the others;
* the overall workload scales more easily across batch systems.

Use the `--dataset <name>` parameter to restrict a pipeline run to a single
named dataset.  The dataset name must exactly match the `name=` field in the
sample config (or the YAML dataset `name` key).

When `--dataset` is supplied, outputs are written to a dedicated directory
`condorSub_{name}_{dataset}/` so that concurrent per-dataset runs never
overwrite each other's symlinks or condor submit files.

### Example: running each dataset in parallel

Given a sample config with datasets `ttbar`, `wjets`, and `dy`:

```bash
# Run each dataset independently (can be launched in parallel)
for DS in ttbar wjets dy; do
  law run RunNANOJobs \
    --submit-config analyses/myAnalysis/cfg/submit_config.txt \
    --name myRun22 \
    --dataset "$DS" \
    --x509 x509 \
    --exe build/analyses/myAnalysis/myanalysis \
    --workflow local &
done
wait
```

Or submit all three as HTCondor workflow tasks simultaneously:

```bash
for DS in ttbar wjets dy; do
  law run RunNANOJobs \
    --submit-config analyses/myAnalysis/cfg/submit_config.txt \
    --name myRun22 \
    --dataset "$DS" \
    --x509 x509 \
    --exe build/analyses/myAnalysis/myanalysis \
    --workflow htcondor &
done
wait
```

Each run produces its own self-contained submission directory
`condorSub_myRun22_ttbar/`, `condorSub_myRun22_wjets/`, etc.

### Backward compatibility

Omitting `--dataset` preserves the original behavior: all datasets in the
sample config are processed together in one submission directory
`condorSub_{name}/`.

---

## Python Environment for Remote Jobs

To use the RDFAnalyzerCore Python bindings (`rdfanalyzer`) or any other Python
packages on remote condor worker nodes, you can package a local Python
environment into a tarball and ship it with your jobs.

### Step 1: Set up the environment inside your container

Run `setup_python_env.sh` **inside the same container** you plan to use on
worker nodes.  This ensures binary compatibility.

```bash
# Example: using the cmssw-el9 container
cmssw-el9 --command-to-run "bash law/setup_python_env.sh"

# With extra packages (e.g. numpy, uproot, numba)
cmssw-el9 --command-to-run \
  "bash law/setup_python_env.sh --extras 'numpy uproot numba' --output python_env.tar.gz"
```

The script:
1. Creates a staging directory.
2. Installs `law`, `luigi`, `requests`, and any `--extras` packages using
   `pip install --target` (no virtual-env activation needed).
3. Copies `build/python/rdfanalyzer*.so` into the package directory (skip with
   `--no-rdfanalyzer`).
4. Tars everything into `python_env.tar.gz`.

### Script options

| Option | Default | Description |
|--------|---------|-------------|
| `-o / --output FILE` | `python_env.tar.gz` | Output tarball path |
| `--extras "pkg1 pkg2 ..."` | `""` | Extra pip packages to install |
| `--no-rdfanalyzer` | *(off)* | Skip copying the rdfanalyzer `.so` module |
| `--python PYTHON` | `python3` | Python executable to use |

### Step 2: Pass the tarball to law tasks

```bash
law run BuildNANOSubmission --workers 1 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --name myRun --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --container-setup cmssw-el9 \
  --python-env python_env.tar.gz
```

The Build task copies the tarball into `shared_inputs/` and adds it to
`transfer_input_files` in the condor submit file so every worker receives it.

### What happens on the worker node

The condor runscript automatically:
1. Untars `python_env.tar.gz` into `_python_env/`.
2. Prepends `_python_env` to `PYTHONPATH`, `PATH`, and `LD_LIBRARY_PATH`.
3. Runs your analysis (which can now `import rdfanalyzer` or any packaged module).

```bash
# Happens automatically in the runscript:
tar -xzf python_env.tar.gz -C _python_env
export PYTHONPATH="$PWD/_python_env:${PYTHONPATH:-}"
export PATH="$PWD/_python_env/bin:${PATH:-}"
export LD_LIBRARY_PATH="$PWD/_python_env:${LD_LIBRARY_PATH:-}"
```

---

## Important Behavior Notes

- **Stage-out is always active**: output ROOT files are always xrdcp'd to EOS
  after each job; `__orig_saveFile` / `__orig_metaFile` in each job's
  `submit_config.txt` record the final EOS destinations.
- **`root_setup` is a file path**: its contents are embedded verbatim into the
  inner runscript, not executed as a shell command.
- **`container_setup`** is passed to the outer wrapper script and used to
  launch the inner runscript (e.g. `cmssw-el9 -- bash condor_runscript_inner.sh`).
- **No OS-pinning** by default (`MY.WantOS` is not set).
- **Local shared libraries** required by the executable (resolved via `ldd` and
  located under the repository root) are automatically staged into `shared_inputs/`.

---

## Monitoring and Resubmission

`MonitorNANOJobs` / `MonitorOpenDataJobs` run a blocking poll loop:

1. Every `--poll-interval` seconds, query `condor_q` and `condor_history`.
2. For held jobs: remove from queue and resubmit.
3. For failed jobs (non-zero exit code): resubmit if `--max-retries` not reached.
4. For completed jobs: verify the expected EOS output file exists via `xrdfs stat`.
5. On completion, write `all_outputs_verified.txt`.

State is saved to `condorSub_{name}/monitor_state.json` after each poll cycle,
so the task can be safely interrupted and restarted — it resumes from where it
left off.

---

## Task re-indexing

If task discovery fails, re-run:

```bash
law index
```

---

## Advanced Topics

### Branch Map Policy

`branch_map_policy.py` controls how LAW branch maps are generated from a
dataset manifest.  By default tasks branch **only over datasets** (one branch
per dataset entry).  For large-scale productions additional dimensions can be
enabled via `BranchingPolicy`.

**Available dimensions** (`BranchingDimension` enum)

| Dimension | Description |
|-----------|-------------|
| `DATASET` | One branch per dataset entry (always active) |
| `REGION` | One branch per named region from `OutputManifest.regions` |
| `SYSTEMATIC_SCOPE` | One branch per nuisance-group scope from `OutputManifest.nuisance_groups` |

**Factory methods**

```python
from branch_map_policy import BranchingPolicy

policy = BranchingPolicy.dataset_only()                       # default
policy = BranchingPolicy.dataset_and_regions()                # D × R branches
policy = BranchingPolicy.dataset_regions_and_systematics()    # D × R × S branches
```

**Scaling implications** – the three dimensions multiply.  With 10 datasets,
5 regions, and 8 systematic groups the total is 400 branches.  Use
`BranchingPolicy(max_branches=500)` to impose a hard cap.  The default cap
is `None` (no limit).

> **Note**: multi-dimensional branching is an advanced feature intended for
> large-scale grid productions.  For most analyses, the default
> `dataset_only()` policy is sufficient.  See
> [docs/LAW_TASKS.md](../docs/LAW_TASKS.md) for the full API reference.

### Dask Executor

`workflow_executors.py` provides `DaskWorkflow`, a LAW workflow mixin that
dispatches branches to a [Dask distributed](https://distributed.dask.org/)
cluster instead of HTCondor.

**Usage in a task**

```python
import law
from workflow_executors import DaskWorkflow, _run_analysis_job

class RunMyJobs(MyMixin, law.LocalWorkflow, law.HTCondorWorkflow, DaskWorkflow):

    def run(self):
        # Executed for --workflow local  and  --workflow htcondor (per-branch)
        ...

    def get_dask_work(self, branch_num, branch_data):
        # Executed for --workflow dask
        exe = os.path.join(self._shared_dir, self._exe_relpath)
        return _run_analysis_job, [exe, branch_data, "", ""], {}
```

**Requirements**

- A running Dask scheduler (e.g. `dask-scheduler` + `dask-worker` processes).
- The scheduler address must be reachable from the machine running `law run`.

The Dask proxy integrates with `failure_handler.py` to classify each branch
failure, apply per-category retry policies, and print a
`DiagnosticSummary` at the end of the run.  Retries happen automatically
without resubmitting the whole workflow.

### Performance Records

Every branch of `SkimTask` and `HistFillTask` writes a `.perf.json` file
alongside its `.done` marker in `job_outputs/`.  The file records:

```json
{
  "task_name": "SkimTask[branch=3]",
  "start_time": "2024-05-01T10:00:00+00:00",
  "end_time":   "2024-05-01T10:03:42+00:00",
  "wall_time_s": 222.1,
  "peak_rss_mb": 1843.2,
  "throughput_mbs": 47.6
}
```

| Field | Description |
|-------|-------------|
| `task_name` | Human-readable task + branch label |
| `wall_time_s` | Elapsed wall-clock seconds |
| `peak_rss_mb` | Peak resident-set-size of the analysis subprocess (Linux only; `null` elsewhere) |
| `throughput_mbs` | Input-data throughput in MB/s (`null` when not computed) |

Aggregate all records for a run to understand production performance:

```bash
# Print wall times for all branches of a skim run
python3 -c "
import json, glob
for f in sorted(glob.glob('skimRun_myRun/job_outputs/*.perf.json')):
    d = json.load(open(f))
    print(f\"{d['task_name']:40s}  {d['wall_time_s']:7.1f}s  {d.get('peak_rss_mb') or 'N/A':>8} MB\")
"
```

The helper `perf_path_for(output_path)` (from `performance_recorder.py`)
derives the `.perf.json` path from any `.done`, `.json`, or `.txt` output
path.
