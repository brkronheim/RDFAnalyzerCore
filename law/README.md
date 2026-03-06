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
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [NANO / Rucio Workflow](#nano--rucio-workflow)
- [CERN Open Data Workflow](#cern-open-data-workflow)
- [Combine Workflow](#combine-workflow)
- [Common Parameters](#common-parameters)
- [Important Behavior Notes](#important-behavior-notes)
- [Monitoring and Resubmission](#monitoring-and-resubmission)

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

## Common Parameters

These parameters apply to **both** NANO and Open Data workflows:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--submit-config` | *(required)* | Path to submit config file |
| `--name` | *(required)* | Submission name; creates `condorSub_{name}/` |
| `--exe` | *(required)* | Path to the compiled C++ executable |
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
