# LAW Workflows for Batch Submission

This folder contains two LAW-based HTCondor submission workflows:

- **[nano_tasks.py](nano_tasks.py)** – CMS NanoAOD data via Rucio
- **[opendata_tasks.py](opendata_tasks.py)** – CERN Open Data Portal

Both share the same four-task structure and identical parameter conventions.

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [NANO / Rucio Workflow](#nano--rucio-workflow)
- [CERN Open Data Workflow](#cern-open-data-workflow)
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
| `--no-validate` | `False` | Skip submit-config validation |

### Monitor-only parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--max-retries` | `3` | Max resubmission attempts per failed job |
| `--poll-interval` | `120` | Seconds between condor_q polls |

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
