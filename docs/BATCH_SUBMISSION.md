# HTCondor Batch Submission Guide

This guide explains how to submit RDFAnalyzerCore analyses to HTCondor using
the **law-based workflows** in `core/python/law/`. All batch submission is
managed through [law](https://github.com/riga/law) (Luigi Analysis Workflow),
which provides dependency tracking, automatic resubmission, and restart-safe
monitoring.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Rucio Submission (Primary)](#rucio-submission-primary)
- [CERN Open Data Submission](#cern-open-data-submission)
- [Legacy Compatibility Notes](#legacy-compatibility-notes)
- [Common Parameters](#common-parameters)
- [Configuration File Format](#configuration-file-format)
- [Monitoring and Resubmission](#monitoring-and-resubmission)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)

---

## Overview

RDFAnalyzerCore provides one primary skim submission workflow with pluggable
file-discovery backends:

| Workflow Layer | Primary Module | Notes |
|----------------|----------------|-------|
| Skim/build/submit/monitor orchestration | `core/python/law/analysis_tasks.py` | Primary workflow surface for local, HTCondor, and Dask execution |
| Rucio file discovery | `core/python/law/rucio_tasks.py` | Primary CMS NanoAOD discovery backend (`--file-source rucio`) |
| Open Data file discovery | `core/python/law/opendata_tasks.py` | Open Data backend (`--file-source opendata`) |
| XRootD directory discovery | `core/python/law/xrdfs_tasks.py` | XRootD directory listing backend (`--file-source xrdfs`) |

For HTCondor skim production, the primary sequence is:

1. **PrepareSkimJobs** – plans the run and creates per-dataset/per-job
   configuration directories in one task. When `--file-source` is set, it
   requires the corresponding file-discovery task.
2. **BuildSkimSubmission** – assembles `condor_submit.sub`, run scripts, and
   stages the executable, `aux/`, x509 proxy, and local shared libraries into
   `shared_inputs/`.
3. **SubmitSkimJobs** – runs `condor_submit` and records the cluster ID.
4. **MonitorSkimJobs** – blocking polling loop that verifies EOS outputs,
  resubmits held/failed jobs, collects per-job generator-weight counters into
  `normalization.json`, and writes `all_outputs_verified.txt`.

Law tracks task outputs so you can run the final task directly and earlier
steps execute automatically if not yet complete.

---

## Quick Start

```bash
# 1. Set up the law environment (run once per session from the repo root)
source core/python/law/env.sh

# 2. Index the task registry (run once, or after adding new modules)
law index

# 3. Run the primary Rucio-backed skim workflow (PrepareSkimJobs →
#    BuildSkimSubmission → SubmitSkimJobs are run automatically as dependencies)
law run MonitorSkimJobs --workers 4 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
  --name myRun \
  --file-source rucio \
  --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis
```

Because this command uses `--file-source rucio`, its submit config must set
`sampleConfig` to the discovery manifest. It can point to the same YAML file as
`--dataset-manifest` when that file contains the fields required by both task
interfaces. For `xrdfs`, file discovery reads the dataset manifest directly.

---

## Prerequisites

### Common requirements

- **Python 3.8+**
- **law** and **luigi**:
  ```bash
  pip install --user law luigi requests
  ```
- **HTCondor** environment (available on lxplus, cmsconnect, and other compatible clusters)
- **Compiled analysis executable** (see [Analysis Guide](ANALYSIS_GUIDE.md))

### For Rucio submissions

- **VOMS proxy** (minimum 20 minutes validity):
  ```bash
  voms-proxy-init -voms cms -rfc --valid 168:0
  ```
- **Rucio** (configured automatically from CVMFS):
  ```bash
  # Rucio home is set automatically; no manual setup needed
  ```

### For Open Data submissions

- **No authentication required** – Open Data is public.
- Optionally install `cernopendata-client` for faster metadata fetching:
  ```bash
  pip install --user cernopendata-client
  ```

---

## Environment Setup

```bash
# Source the law environment from the repository root (sets PYTHONPATH,
# LAW_HOME, and LAW_CONFIG_FILE automatically)
source core/python/law/env.sh

# Index the task registry
law index
```

---

## Rucio Submission (Primary)

### Sample config format

The primary `SkimTask` workflow reads its typed dataset list from YAML passed
with `--dataset-manifest`. When `--file-source rucio` or `opendata` is
selected, the automatically chained discovery task also reads `sampleConfig`
from the submit config. It can point to the same YAML manifest when its schema
contains the fields required by the discovery task. The `xrdfs` source uses
the dataset manifest directly and does not need `sampleConfig`.

#### Preferred format – YAML dataset manifest

Use a YAML manifest for production workflows.  It provides richer metadata,
supports multi-year analyses, and is validated by the framework at submission
time.  See `core/python/example_dataset_manifest.yaml` for a fully annotated
example.

> Use YAML for `--dataset-manifest` and, when Rucio/Open Data discovery is
> selected, for `sampleConfig` as well. Legacy `.txt` sample configs are
> deprecated compatibility inputs and should not be used for new workflows.

```yaml
# Luminosity in fb^-1 (used as default; lumi_by_year takes precedence when set)
lumi: 59.7

# Optional per-year luminosities
lumi_by_year:
  2022: 38.01
  2023: 21.69

# Optional Rucio site whitelist / blacklist
whitelist: []
blacklist: []

datasets:
  - name: ttbar_powheg_2022
    year: 2022
    process: ttbar
    group: ttbar
    dtype: mc
    das: /TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22NanoAODv12-130X.../NANOAODSIM
    xsec: 98.34
    kfac: 1.0
    sum_weights: ~      # computed on the fly if omitted
  - name: wjets_ht_0to70_2022
    year: 2022
    process: wjets
    group: wjets_ht
    dtype: mc
    das: /WtoLNu-0Jets.../NANOAODSIM
    xsec: 54000.0
    stitch_id: 0
```

The automatically chained discovery task uses `sampleConfig` from the submit
config. The primary `SkimTask` still receives its typed dataset manifest via
`--dataset-manifest`:

```
sampleConfig=analyses/myAnalysis/cfg/samples.yaml
```

#### DEPRECATED: Legacy key=value text format

The legacy flat text format is **DEPRECATED** and maintained only for
backward compatibility with older submission scripts.  Each line contains
space-separated `key=value` pairs.

> **Note on luminosity units**: The YAML manifest uses **fb^-1** for the `lumi`
> field, while the legacy text format uses **pb^-1**.  Ensure you use the
> correct unit for the format you choose.

```
# Luminosity in pb^-1
lumi=59700

# Optional site whitelist / blacklist (comma-separated CMS site names)
WL=T2_US_MIT,T2_US_Wisconsin
BL=T3_US_FNALLPC

# One line per sample
name=ttbar das=/TTToSemiLeptonic.../NANOAODSIM xsec=831.76 type=0 norm=1.0 kfac=1.0
name=wjets das=/WJetsToLNu.../NANOAODSIM     xsec=61526.7 type=1 norm=1.0 kfac=1.0
```

**Sample fields** (legacy format):

| Field | Description |
|-------|-------------|
| `name` | Sample identifier (used in output filenames) |
| `das` | DAS path for Rucio file discovery |
| `xsec` | Cross-section in pb |
| `type` | Sample type integer (analysis-specific) |
| `norm` | Normalization factor |
| `kfac` | K-factor |
| `extraScale` | Additional scaling factor (optional) |
| `site` | Preferred site for this sample (optional) |

### Running the primary workflow

```bash
# Run the full primary sequence (law resolves dependencies automatically)
law run MonitorSkimJobs --workers 4 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/samples.yaml \
  --name myRun22 \
  --file-source rucio \
  --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --root-setup env.sh \
  --container-setup cmssw-el9 \
  --want-os el8 \
  --stage-in \
  --size 30
```

To run only up to the submission file generation (without submitting):

```bash
law run BuildSkimSubmission --workers 4 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/samples.yaml \
  --name myRun22 --x509 x509 \
  --file-source rucio \
  --exe build/analyses/myAnalysis/myanalysis
```

### Rucio-specific parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--x509` | *(required)* | Path to VOMS x509 proxy file |
| `--size` | `30` | GB of data per condor job |
| `--max-runtime` | `3600` | Maximum job runtime in seconds |

---

## CERN Open Data Submission

### Sample config format

The primary `SkimTask` input is a typed YAML manifest. The deprecated text
format is supported only on the legacy discovery path.

#### Preferred format – YAML dataset manifest

For Open Data workflows, each entry's `das` field stores a comma-separated
list of CERN Open Data **record IDs** (instead of DAS paths).

> Use YAML for `--dataset-manifest` and, when Open Data discovery is selected,
> for `sampleConfig` as well. Legacy `.txt` sample configs are deprecated
> compatibility inputs and should not be used for new workflows.

```yaml
lumi: 11.58   # fb^-1

datasets:
  - name: ZMuMu
    dtype: mc
    das: "24119"          # CERN Open Data record ID
    xsec: 1.0
    sum_weights: ~
  - name: ZEE
    dtype: mc
    das: "24120"
    xsec: 1.0
```

#### DEPRECATED: Legacy key=value text format

> field, while the legacy text format uses **pb^-1**.  Ensure you use the
> correct unit for the format you choose.

```
# Luminosity in pb^-1
lumi=11580

# Record IDs from the CERN Open Data Portal
recids=24119,24120

# One line per sample (das= must match the key in the Open Data file index)
name=ZMuMu das=SMZee_0 type=10 xsec=1.0 norm=1.0
name=ZEE   das=SMZee_1 type=11 xsec=1.0 norm=1.0
```

**Fields** (legacy format):

| Field | Description |
|-------|-------------|
| `recids` | Comma-separated CERN Open Data record IDs |
| `das` | Dataset key as it appears in the Open Data file index |
| `name`, `xsec`, `type`, `norm`, `kfac`, `extraScale` | Same as NANO |

### Running the workflow

```bash
law run MonitorSkimJobs --workers 4 \
  --submit-config analyses/myAnalysis/cfg/opendata_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/opendata_manifest.yaml \
  --name openDataRun \
  --file-source opendata \
  --exe build/analyses/myAnalysis/myanalysis \
  --files 20 \
  --container-setup cmssw-el8
```

To run only up to submission file generation:

```bash
law run BuildSkimSubmission --workers 4 \
  --submit-config analyses/myAnalysis/cfg/opendata_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/opendata_manifest.yaml \
  --name openDataRun \
  --file-source opendata \
  --exe build/analyses/myAnalysis/myanalysis \
  --files 20
```

### Open Data-specific parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--files` | `30` | Number of ROOT files per condor job |
| `--x509` | `""` | Path to x509 proxy (optional) |
| `--max-runtime` | `1200` | Maximum job runtime in seconds |

---

## Legacy Compatibility Notes

The primary skim workflow receives its typed dataset list with
`--dataset-manifest`. When `--file-source rucio` or `opendata` is selected,
LAW chains a discovery task that reads `sampleConfig` from the submit config.
Point both parameters at the same YAML file when it has fields required by
both task interfaces. The `sampleConfig` key is also accepted by standalone
discovery tasks. Legacy key/value text is deprecated and supported only by
compatibility discovery paths.

## Common Parameters

These options are shared by the primary skim-submission tasks; discovery and
post-processing tasks can have additional task-specific parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--submit-config` | *(required)* | Path to submit config file |
| `--name` | *(required)* | Submission name; creates `skimRun_{name}/` |
| `--exe` | *(required)* | Path to the compiled C++ executable |
| `--stage-in` | `False` | xrdcp input files to worker node before running |
| `--root-setup` | `""` | Path to a setup script; contents embedded in inner runscript |
| `--container-setup` | `""` | Container image path for `MY.SingularityImage` |
| `--want-os` | `""` | Optional `MY.WantOS` value (for example `el8`) |
| `--python-env` | `""` | Path to Python environment tarball (see below) |
| `--no-validate` | `False` | Skip submit-config validation |

### Monitor task parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--max-retries` | `3` | Maximum resubmission attempts per failed job |
| `--poll-interval` | `120` | Seconds between condor_q polling cycles |

---

## Python Environment for Remote Jobs

To use the RDFAnalyzerCore Python bindings (`rdfanalyzer`) or any other Python
packages on remote condor worker nodes, use `core/python/law/setup_python_env.sh` to
create a portable tarball and pass it to law tasks via `--python-env`.

### Step 1: Set up the environment inside the container

Run the setup script **inside the same container** you use for remote jobs.
This ensures binary compatibility between local and remote nodes.

```bash
# Basic usage (packages law, luigi, requests + rdfanalyzer.so)
cmssw-el9 --command-to-run "bash core/python/law/setup_python_env.sh"

# With extra packages
cmssw-el9 --command-to-run \
  "bash core/python/law/setup_python_env.sh --extras 'numpy uproot numba' --output python_env.tar.gz"
```

The script installs packages into a staging directory using
`pip install --target` (no virtual-env activation scripts, fully portable)
and copies the compiled `rdfanalyzer*.so` module from `build/python/`.

**Setup script options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-o / --output FILE` | `python_env.tar.gz` | Output tarball path |
| `--extras "pkg1 pkg2 ..."` | `""` | Extra pip packages to install |
| `--no-rdfanalyzer` | *(off)* | Skip copying the rdfanalyzer module |
| `--python PYTHON` | `python3` | Python executable to use |

### Step 2: Pass the tarball to law tasks

```bash
law run MonitorSkimJobs --workers 4 \
  --submit-config analyses/myAnalysis/cfg/submit_config.txt \
  --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
  --file-source rucio \
  --name myRun --x509 x509 \
  --exe build/analyses/myAnalysis/myanalysis \
  --container-setup cmssw-el9 \
  --python-env python_env.tar.gz
```

The `BuildSkimSubmission` task copies the tarball into `skimRun_{name}/shared_inputs/`
and adds it to `transfer_input_files` so every worker node receives it.

### What happens on the worker node

The condor runscript automatically untars the environment and sets up paths:

```bash
tar -xzf python_env.tar.gz -C _python_env
export PYTHONPATH="$PWD/_python_env:${PYTHONPATH:-}"
export PATH="$PWD/_python_env/bin:${PATH:-}"
export LD_LIBRARY_PATH="$PWD/_python_env:${LD_LIBRARY_PATH:-}"
```

After this, your analysis can use any packaged module:

```python
import rdfanalyzer
analyzer = rdfanalyzer.Analyzer("submit_config.txt")
# ... use Python bindings as usual
```

---

## Configuration File Format

### Submit config (`submit_config.txt`)

```
saveDirectory=/eos/user/u/username/output
sampleConfig=config/samples.yaml

# Optional plugin configs copied to every job
bdtConfig=config/bdts.txt
onnxConfig=config/onnx_models.txt
```

### Main config file

```
saveDirectory=/eos/user/u/username/output
saveTree=Events
treeList=Events
enableCounters=true
enableSkim=true
```

Set `enableSkim=true` for skim submissions. The framework writes the event
tree only when this option is enabled; LAW does not override it. Use
`enableSkim=false` for histogram-only runs that should not create a skim.

---

## Monitoring and Resubmission

The `Monitor*Jobs` tasks run a blocking poll loop:

1. Every `--poll-interval` seconds, query `condor_q` and `condor_history`.
2. **Held jobs**: remove from queue and resubmit automatically.
3. **Failed jobs** (non-zero exit code): resubmit if `--max-retries` not reached.
4. **Overlong jobs**: remove and resubmit a running job when its
  `RemoteWallClockTime` exceeds 150% of its Condor `MaxRuntime`.
5. **Multi-file retries**: split a retry with more than one input file into two
  balanced file groups, with separate output and metadata files for each part.
6. **Completed jobs**: verify the expected EOS output exists via `xrdfs stat`.
7. Write `skimRun_{name}/all_outputs_verified.txt` when all outputs confirmed.

For MC, generator-weight sums are never scanned over the full dataset during
preparation. Each partition writes its own counter to its metadata ROOT file;
only after every job has finished does `MonitorSkimJobs` sum those counters and
write the process normalization to `skimRun_{name}/normalization.json`.

**State persistence**: monitoring state is saved to
`skimRun_{name}/monitor_state.json` after each poll cycle.  If you interrupt
the monitor task (Ctrl+C), restart it with the same parameters and it will
resume from where it left off.

```bash
# Restart monitoring after an interruption
law run MonitorSkimJobs --workers 1 \
  --submit-config ... --dataset-manifest ... --file-source rucio --name myRun [other params as before]
```

---

## Advanced Features

### Input staging

Copy input files to the worker node before running the analysis:

```bash
--stage-in
```

When enabled, input XRootD URLs are xrdcp'd to local disk on the worker.
The `submit_config.txt` for each job stores the original URLs in
`__orig_fileList` and uses local filenames (`input_0.root`, …) for the
analysis.

### Output staging

The delegated HTCondor workflow enables output staging for each job: ROOT
outputs are copied to their final EOS destinations after the job completes.
The local basenames are stored in `saveFile` / `metaFile`, with final
destinations in `__orig_saveFile` / `__orig_metaFile`. Local and Dask execution
do not use this HTCondor staging mechanism; their output paths follow the
configuration and executor setup.

### ROOT and container setup

```bash
# Embed a setup script into the inner runscript
--root-setup env.sh

# Wrap the inner runscript with a container command
--container-setup 'cmssw-el9'
```

### Parallel file discovery

File-discovery tasks run as law LocalWorkflows with one branch per sample.
Set `--workers N` to discover files for up to N samples in parallel. The
subsequent `PrepareSkimJobs` task writes independent job configs and redirector
maps with a bounded four-thread pool by default; tune it with
`--prepare-workers N` (or use `1` for serial preparation):

```bash
law run PrepareSkimJobs \
  --workers 8 --prepare-workers 4 --file-source rucio [other params]
```

---

## Troubleshooting

### Task outputs not found / law re-runs completed tasks

Delete the offending output and re-run:
```bash
rm rucioFileList_myRun/sample_0.json
law run PrepareSkimJobs --file-source rucio [params]
```

### law index fails

```bash
source core/python/law/env.sh
law index
```

### Rucio connection errors

The law tasks retry Rucio queries with exponential backoff (up to 5 attempts).
If errors persist, check your network connection and proxy validity.

### Proxy expired

```bash
voms-proxy-init -voms cms -rfc --valid 168:0
```

### No files found for a dataset

- Verify the DAS path with `dasgoclient --query="dataset=/DatasetName/..."`.
- Adjust the `WL` / `BL` lists in the sample config.

### Checking generated files

```bash
ls -la skimRun_myRun/jobs/sample/job_0/
cat skimRun_myRun/jobs/sample/job_0/submit_config.txt
cat skimRun_myRun/condor_submissions/sample/condor_runscript.sh
```

### Job logs

```bash
cat skimRun_myRun/condor_submissions/sample/condor_logs/log_<cluster>_<process>.stdout
cat skimRun_myRun/condor_submissions/sample/condor_logs/log_<cluster>_<process>.stderr
```

---

## See Also

- [core/python/law/README.md](../core/python/law/README.md) – law workflow quick reference
- [Configuration Reference](CONFIG_REFERENCE.md) – config file formats
- [Analysis Guide](ANALYSIS_GUIDE.md) – building analyses
- [Getting Started](GETTING_STARTED.md) – setup and installation

---

For questions or issues, please open an issue on GitHub.
