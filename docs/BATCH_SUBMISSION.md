# HTCondor Batch Submission Guide

RDFAnalyzerCore stages, submits, monitors, and validates HTCondor production.
Use the [Snakemake workflows](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/snakemake/README.md) to start campaigns or consume completed products. Keep running LAW campaigns under the controller that submitted them. The [legacy LAW section](#legacy-law-workflows) documents the older task and monitoring interfaces.

Generated workers also carry an immutable, origin-scoped XRootD ranking
snapshot, preferring the learned user cache over the experiment provider seed.
After successful LAW Condor monitoring, rankings update automatically for future
submissions. The default cache is `~/.cache/rdfanalyzer/xrd_site_rankings.tsv`
(under `XDG_CACHE_HOME` when set). Set
`RDF_XRD_SITE_RANKING_FILE=/path/to/rankings.tsv` before generating scripts to
ship a refined snapshot, or `none` to disable it. The
[ranking tool and format](xrd-adaptor.md#historical-site-preferences-and-open-attempt-logging)
describe log aggregation and per-worker network scope. Rebuild the adaptor and
regenerate scripts to deploy these changes; existing production is untouched.

## Table of Contents

- [Standalone production stages](#standalone-production-stages)
- [Legacy LAW workflows](#legacy-law-workflows)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Rucio Submission (Primary)](#rucio-submission-primary)
- [CERN Open Data Submission](#cern-open-data-submission)
- [Legacy Compatibility Notes](#legacy-compatibility-notes)
- [Common Parameters](#common-parameters)
- [Configuration File Format](#configuration-file-format)
- [ONNX Runtime for HTCondor workers](#onnx-runtime-for-htcondor-workers)
- [Monitoring and Resubmission](#monitoring-and-resubmission)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)

---

## Standalone production stages

Use a [Snakemake campaign](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/snakemake/README.md#configure-a-campaign)
to select eras from a sealed collection and share their runtime and submission
settings. It generates one spec per era and assigns each a distinct run name
and remote output directory. For a standalone run, write the YAML spec
directly. Paths in an explicit spec resolve relative to that spec.
Use `requirements-snakemake.txt` for a controller environment with the
shared batch services and Snakemake. The `rdf_production` CLI also runs directly.
Set `backend: local` to run every prepared partition on the current host
with the same verified output index. Local runs write under
`skimRun_<name>/outputs/` and do not take a Condor output destination or
Condor sizing pilots. See the [Snakemake local-production guide](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/snakemake/README.md#local-production).
Set `backend: dask` to use the Dask executor on a local cluster or an
existing scheduler with a shared filesystem. See the
[Dask production guide](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/snakemake/README.md#dask-production).

```yaml
version: 1
name: vjets_pilot_2022_preEE
exe: runtime/vjet_njetpt
submit_config: 2022_preEE/bundle/analysis.yaml
dataset_manifest: 2022_preEE/bundle/datasets.yaml
options:
  file_source: rucio
  files_per_job: 4
  x509: credentials/x509
  output_directory: root://eosuser.cern.ch//eos/user/u/user/vjets_pilot_2022_preEE
```

From the checkout, use the configured Python/ROOT and Condor environment.
`inspect` verifies the sealed bundle and describes the runtime offline.
`preflight` builds and validates the runtime. It accepts multiple specs
and builds a shared CMake target once. `prepare` can run discovery and
stage workers. `submit` returns when the scheduler has accepted the jobs.
`execute` submits or resumes the run, then publishes an output index after
full integrity and partition checks. Use `smoke` to run a local job against
one input per available data and MC sample type before submitting. It requires
the default `make_test_job: true` and a spec without Condor sizing pilots.

```bash
export PYTHONPATH="$PWD/core/python${PYTHONPATH:+:$PYTHONPATH}"
python -m rdf_production inspect campaign/2022_preEE.yaml --format json
python -m rdf_production preflight campaign/2022_preEE.yaml
python -m rdf_production prepare campaign/2022_preEE.yaml
python -m rdf_production smoke campaign/2022_preEE.yaml
python -m rdf_production submit campaign/2022_preEE.yaml
python -m rdf_production execute campaign/2022_preEE.yaml --output-index results/2022_preEE/index.json
python -m rdf_production status skimRun_vjets_pilot_2022_preEE
python -m rdf_production verify --output-index results/2022_preEE/index.json
```

`status` reports the recorded runtime, stage artifacts and their timestamps.
`verify` checks local evidence, including an optional `--merge-receipt`.
Add `--storage` to compare the recorded file signatures with XRootD.
Keep the spec, bundle and validated runtime at their recorded paths and
repeat `execute` from the same submit environment to resume.
The pilot uses the default local Condor scheduler, a fresh controller-owned
run directory, and the integrity policy with file signatures available at the
stage-out endpoint. The `x509` setting is a path to the credential. Current
preflight and staging policy still applies. Set `root_setup` to the worker
setup script for the executable's ROOT release. For an executable built with
ROOT 6.42.00-rc1, set `RDF_CVMFS_ROOT_RELEASE=6.42.00-rc1` inside that script
so the setting also reaches workers. This release reports `6.41.99` through
`root-config --version`.

### C++ runtime

For a C++ campaign, point `exe` at the output of a named CMake target and
include a `build` object in the spec. CMake paths are relative to the spec
and the configure options are passed as separate arguments.

```json
{
  "source_directory": "/path/to/RDFAnalyzerCore",
  "build_directory": "runtime/build",
  "target": "vjet_njetpt",
  "parallel": 4,
  "configure_args": [
    "-DCMAKE_BUILD_TYPE=Release",
    "-DRDF_ANALYSES=vjet-njetpt",
    "-DRDF_REQUIRED_EXTENSIONS=cms"
  ]
}
```

Preflight checks that CMake reports the requested executable for this target,
checks the executable and its dynamic linkage, and records a digest for
staging. When the target links to ROOT and `ROOTSYS` is set, it also checks
that linkage against the selected installation. Set `ctest_regex` to require
matching CTest tests to pass and use `test_targets` to build any separate test
executables first. Use `inputs` to declare additional build inputs, such as
headers or toolchain files outside the CMake target inventory. Give a changed
runtime a new run name. An `exe` alone uses an existing prebuilt executable.

### Python runtime

A Python spec uses `python` with an `entrypoint` and optional `modules`
mapping from paths inside the archive to local source paths. The worker
receives the staged job config as the first argument. Code that uses the
framework binding can construct `rdfanalyzer.Analyzer(sys.argv[1])`.
Preflight checks the sources and stages a deterministic executable Python
archive. Set `options.python_env` to a worker environment archive to ship
additional dependencies. Use `options.root_setup` when the Python code imports
PyROOT or uses a native binding that depends on ROOT. Create a worker archive
with `bash core/python/setup_python_env.sh`.

### CMS discovery, splitting, and monitoring

`file_source: "rucio"` uses the CMS extension's discovery provider. It accepts
the manifest's explicit file inventory or queries the Rucio catalogue for
datasets with a DAS path. With a file source, `partition: "auto"` uses matching
learned sizing when available and falls back to file grouping for unmeasured
datasets. To calibrate before production, set `sizing_pilots: true` and run
`execute`. It accepts `sizing_pilot_parallel`, `sizing_pilot_prefix_entries`,
`sizing_pilot_target_seconds`, and `sizing_pilot_safety_factor`. Pilots require
a file source, `threads=1`, and partition mode `auto`, `events`, or
`entry_range`. The `prepare` command stays submission-free by rejecting specs
that request Condor sizing pilots. See [job sizing](JOB_SIZING.md) for the
planning and calibration policies.

With the XRootD adaptor enabled, workers check read quality and report site
measurements. After verified completion, look for
`xrd_site_rankings_update.json` and `job_sizing_update.json` in the run
directory. The [XRootD ranking settings](xrd-adaptor.md#historical-site-preferences-and-open-attempt-logging)
also apply to Snakemake runs.

Initial dataset-level submission writes `submission_journal.json` beside
`submitted.json`. A pending journal entry means Condor acceptance is
uncertain. Resume adopts the exact cluster only when matching owner and
submission IDs cover the expected processes across queue and history.
Unresolved entries require investigation on the original scheduler. Adaptive
split retries currently use the existing monitor handoff.

Consume the output index with `python -m rdf_production merge`. Use
`--output-index`, `--destination`, and `--receipt` to specify the verified
input, merge directory, and downstream receipt. The existing `partial_merge`
command also supports previews of a running campaign. Use the output-index
path for production merges requiring full coverage.

## Legacy LAW workflows

Existing LAW campaigns and external analysis wrappers use the following modules. These commands describe the compatibility workflow.

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
   The dataset manifest is authoritative: preparation ignores cached discovery
   entries for other samples/eras and fails if a requested dataset has no
   payload, an empty payload, or duplicate payloads. Reusing a larger cache
   cannot introduce jobs with fallback sample identity. Separate cache names
   per concurrent era are still recommended to avoid concurrent cache writes.
2. **BuildSkimSubmission** – assembles `condor_submit.sub`, run scripts, and
   stages the executable, `aux/`, x509 proxy, and local shared libraries into
   `shared_inputs/`.
3. **SubmitSkimJobs** – runs `condor_submit` and records the cluster ID.
4. **MonitorSkimJobs** – blocking polling loop that verifies EOS outputs,
   resubmits held/failed jobs, and writes `all_outputs_verified.txt`.

Monitor completion rechecks the required ROOT files, skim trees when enabled,
and MC weight counters under the selected validation policy. In `integrity` mode,
an existing completion marker does not make a missing or corrupt output complete;
`exists` mode checks paths without opening ROOT files.

Law tracks task outputs so you can run the final task directly and earlier
steps execute automatically if not yet complete.

Generated worker scripts preserve the CPU affinity inherited from the site
through setup, staging and analysis execution. They do not select or reassign
CPUs, even when the allowed set is larger than `request_cpus`. ROOT, XRootD
helpers and child processes inherit that placement. Common numerical
thread-pool limits are set to the requested count, but these are not an
aggregate CPU quota; CPU enforcement comes from the site's affinity or cgroup
policy. Startup lines report the requested count, allowed CPUs and
`policy=inherited`. This applies to freshly generated scripts; rebuilding the
executable does not change already prepared or running jobs.

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
  --submit-config analyses/myAnalysis/cfg/submit_config.yaml \
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

```yaml
sampleConfig: analyses/myAnalysis/cfg/samples.yaml
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
  --submit-config analyses/myAnalysis/cfg/submit_config.yaml \
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
  --submit-config analyses/myAnalysis/cfg/submit_config.yaml \
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
  --submit-config analyses/myAnalysis/cfg/opendata_config.yaml \
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
  --submit-config analyses/myAnalysis/cfg/opendata_config.yaml \
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

## ONNX Runtime for HTCondor workers

For a submit-host ONNX Runtime package outside the checkout, configure a
build directory inside the framework workspace. Point `ONNXRUNTIME_ROOT_DIR`
at the package root that contains `include/` and `lib/`. For example, from
the checkout root after sourcing the build environment:

```bash
cmake -S . -B build-worker \
  -DONNXRUNTIME_ROOT_DIR=/absolute/path/to/onnxruntime \
  -DONNXRUNTIME_USE_CUDA=OFF
cmake --build build-worker --target myanalysis
ldd build-worker/analyses/myAnalysis/myanalysis | grep libonnxruntime
```

Pass that executable as `--exe` to the LAW skim tasks. When `ldd` resolves
ONNX Runtime through the build's `_rdf_onnx_link/<id>/` directory,
`PrepareSkimJobs` stages the external library under its linker SONAME
(for example, `libonnxruntime.so.1`) and its versioned filename. The worker
loader searches the unpacked archive. Verify the staged names before submission:

```bash
tar -tzf skimRun_myRun/shared_inputs.tar.gz | grep '^libonnxruntime\.so'
```

For a shared CVMFS ONNX Runtime package, point `ONNXRUNTIME_ROOT_DIR` at
the CVMFS package used by the workers. The worker loads it from CVMFS using
its usual runtime environment. For CUDA builds, the worker environment
continues to supply the compatible GPU provider and CUDA dependencies.

## Python Environment for Remote Jobs

To use the RDFAnalyzerCore Python bindings (`rdfanalyzer`) or any other Python
packages on remote condor worker nodes, use `core/python/law/setup_python_env.sh` to
create a portable tarball and pass it to law tasks via `--python-env`.

### Step 1: Set up the environment inside the container

Run the setup script **inside the same container** you use for remote jobs.
Use the worker Python and ROOT runtime when building and checking the binding.

```bash
# Basic usage (packages law, luigi, requests + rdfanalyzer.so)
cmssw-el9 --command-to-run "bash core/python/law/setup_python_env.sh"

# With extra packages
cmssw-el9 --command-to-run \
  "bash core/python/law/setup_python_env.sh --extras 'numpy uproot numba' --output python_env.tar.gz"
```

The script stages packages with `pip install --target`. It selects a binding from `build/python/` using the requested interpreter’s extension suffixes. Set `RDFANALYZER_PYTHON_MODULE_DIR` for another build, such as `$PWD/build-clang/python`. If no matching module is found, the script prints a warning and writes a package-only archive. Workers need compatible native libraries. LAW archive extraction requires a Python runtime with `tarfile.data_filter`.

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
  --submit-config analyses/myAnalysis/cfg/submit_config.yaml \
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
import sys
import rdfanalyzer
analyzer = rdfanalyzer.Analyzer(sys.argv[1])
# ... use Python bindings as usual
```

---

## Configuration File Format

### Submit configuration (`submit_config.yaml`)

```yaml
saveDirectory: /eos/user/u/username/output
sampleConfig: config/samples.yaml

# Optional plugin configs copied to every job
bdtConfig: config/bdts.yaml
onnxConfig: config/onnx_models.yaml
```

### Main config file

```yaml
saveDirectory: /eos/user/u/username/output
saveTree: Events
treeList: Events
enableCounters: true
enableSkim: true
```

Set `enableSkim=true` for skim submissions. The framework writes the event
tree only when this option is enabled; LAW does not override it. Use
`enableSkim=false` for histogram-only runs that should not create a skim.

---

## Monitoring and Resubmission

LAW skim submissions automatically set Condor's `JobBatchName` to
`<sample> [<run name>]`, using the dataset name and `--name`. This applies
to all analyses using `BuildSkimSubmission`, including the vjet-njetpt
submission wrapper. Normal `condor_q` batch summaries show this label in
`BATCH_NAME`; retries and split retries inherit it from the submission template.
The sample comes first so it is visible even when the column is truncated.

Each job also carries the string ClassAd attributes `RDFSample` and `RDFRun`:

```bash
# Show full batch names in the usual summary
condor_q -wide

# Show full labels for individual jobs (including retries)
condor_q -af ClusterId ProcId JobStatus RDFSample RDFRun

# Restrict the query to one sample
condor_q -constraint 'RDFSample == "DYJets"'
```

These labels apply to newly generated submission files. Existing queued jobs
and previously prepared submission templates retain their original attributes.
Python callers of `write_submit_files` or `generate_condor_submit` can opt in
with `sample_name=...` and `run_name=...`.

The `Monitor*Jobs` tasks run a blocking poll loop:

1. Every `--poll-interval` seconds, query `condor_q` and `condor_history`.
2. **Held jobs**: remove from queue and resubmit automatically.
3. **Failed jobs** (non-zero exit code): resubmit if `--max-retries` not reached.
4. **Overlong jobs**: remove and resubmit a running job when its
   current execution exceeds 150% of its Condor `MaxRuntime` (for example,
   over 90 minutes for a 60-minute limit). This check is always enabled in
   `MonitorSkimJobs` and runs on each poll. Elapsed time uses
   `ServerTime - JobCurrentStartDate`, with the local clock used if
   `ServerTime` is unavailable. If the start date is absent, the monitor uses
   `EnteredCurrentStatus`, then `RemoteWallClockTime` as a final fallback.
   The job's positive `MaxRuntime` takes precedence over `--max-runtime`,
   which is used if the ad omits the limit. Resubmission requires successful
   removal and respects `--max-retries`.
5. **Multi-file retries**: split a retry with more than one input file into two
  balanced file groups, with separate output and metadata files for each part.
6. **Completed jobs**: check expected output paths using batched `xrdfs ls`
   (falling back to `stat`). `--output-validation integrity` (the framework
   default) additionally opens ROOT files to check their contents and MC
   counters. `--output-validation exists` skips these per-poll ROOT reads.
7. Write `skimRun_{name}/all_outputs_verified.txt` when all outputs confirmed.

For MC, generator-weight sums are never scanned over the full dataset during
preparation. Each partition writes its own counter to its metadata ROOT file;
downstream consumers read those counters from their input files when needed.
`MonitorSkimJobs` does not aggregate sample counters or write `normalization.json`.
The vjet submit wrapper selects `exists` by default; its
`--output-validation integrity` option
restores the stronger checks. The policy does not change worker payloads,
partitions, saved monitoring state, or the run identity.

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

Workers route public CMS dataset inputs through `cms-xrd-global.cern.ch`.
Streaming and stage-in workers perform no per-site redirector probing and
launch no ROOT probe subprocesses. The XrdAdaptor handles replica discovery
and read races during streaming. Stage-in uses `xrdcp` directly with bounded
retries. Local inputs and private/non-CMS endpoints retain their URLs.

Copy input files to the worker node before running the analysis:

```bash
--stage-in
```

When enabled, input XRootD URLs are xrdcp'd to local disk on the worker.
The worker configuration (`submit_config.yaml`, or an archived `submit_config.txt` source) stores the original URLs in
`__orig_fileList` and uses local filenames (`input_0.root`, …) for the
analysis.

### Refresh XRootD fixes for an existing skim production

Stop the production's LAW monitors before refreshing prepared retry inputs.
Rebuilding the adaptor or restarting the monitor alone does not replace the
scripts and runtime library already staged in the run directory. Preview and
then apply a focused refresh from the repository root:

```bash
python3 core/python/refresh_xrootd_runtime.py \
  --library build/core/xrd/libRDFXrdAdaptor.so skimRun_myRun
python3 core/python/refresh_xrootd_runtime.py --apply \
  --library build/core/xrd/libRDFXrdAdaptor.so skimRun_myRun
```

The refresh replaces legacy embedded worker probes with global URL routing
and updates the adaptor library
in `shared_inputs/` and `shared_inputs.tar.gz`. It verifies that every other
regular archive member has identical contents, including the analysis binary
and physics configuration. Original scripts, library, archive, and a rollback
manifest are saved under `runtime_patches/xrootd_<timestamp>/`. Files are
replaced atomically after validation. Resume the same monitor tasks with the
same run names and parameters; their existing state and completed outputs are
preserved. Jobs that already received their inputs keep those copies; future
resubmissions transfer the refreshed files. The refresh itself does not submit,
cancel, or restart any jobs. Executable changes, including lazy initial chain
validation, require a freshly staged executable; this refresh preserves the
saved analysis binary.

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
cat skimRun_myRun/jobs/sample/job_0/submit_config.yaml
cat skimRun_myRun/condor_submissions/sample/condor_runscript.sh
```

### Job logs

```bash
cat skimRun_myRun/condor_submissions/sample/condor_logs/log_<cluster>_<process>.stdout
cat skimRun_myRun/condor_submissions/sample/condor_logs/log_<cluster>_<process>.stderr
```

---

## See Also

- [core/python/law/README.md](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/python/law/README.md) – law workflow quick reference
- [Configuration Reference](CONFIG_REFERENCE.md) – config file formats
- [Analysis Guide](ANALYSIS_GUIDE.md) – building analyses
- [Getting Started](GETTING_STARTED.md) – setup and installation

---

For questions or issues, please open an issue on GitHub.
