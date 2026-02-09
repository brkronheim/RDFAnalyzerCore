# HTCondor Batch Submission Guide

This guide explains how to submit RDFAnalyzerCore analyses to HTCondor batch systems using the provided Python scripts.

## Table of Contents

- [Overview](#overview)
- [Submission Scripts](#submission-scripts)
- [Prerequisites](#prerequisites)
- [NANO/Rucio Submission](#nanorucio-submission)
- [CERN Open Data Submission](#cern-open-data-submission)
- [Configuration Validation](#configuration-validation)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)

## Overview

RDFAnalyzerCore provides two submission scripts for different data sources:

1. **generateSubmissionFilesNANO.py** - For CMS NanoAOD data via Rucio
2. **generateSubmissionFilesOpenData.py** - For CERN Open Data

Both scripts share common functionality through `submission_backend.py` and include configuration validation via `validate_config.py`.

### What the Scripts Do

1. Parse your analysis configuration
2. Discover input files from Rucio or CERN Open Data
3. Split data into manageable job chunks
4. Generate HTCondor submission files
5. Create per-job configuration files
6. Stage auxiliary files and executables

## Submission Scripts

### generateSubmissionFilesNANO.py

**Purpose**: Submit analyses using CMS NanoAOD datasets discovered via Rucio.

**Location**: `core/python/generateSubmissionFilesNANO.py`

**Key Features**:
- Rucio client integration for dataset discovery
- Automatic file grouping by size (GB)
- Site-based XRootD redirector selection
- X509 proxy authentication
- Input/output staging support

### generateSubmissionFilesOpenData.py

**Purpose**: Submit analyses using CERN Open Data datasets.

**Location**: `core/python/generateSubmissionFilesOpenData.py`

**Key Features**:
- CERN Open Data API integration
- Record ID (recid) based file discovery
- Simpler configuration (no proxy required)
- File grouping by count

### submission_backend.py

**Purpose**: Shared functionality for both submission scripts.

**Location**: `core/python/submission_backend.py`

**Provides**:
- HTCondor submit file generation
- HTCondor run script generation
- Input staging logic (xrdcp)
- Output staging logic (xrdcp)
- Configuration file reading
- Shared directory management

### validate_config.py

**Purpose**: Validate analysis configuration before submission.

**Location**: `core/python/validate_config.py`

**Checks**:
- Required configuration keys present
- File paths exist
- Configuration format correctness
- Mode-specific requirements (NANO vs OpenData)

## Prerequisites

### For NANO/Rucio Submissions

1. **VOMS Proxy**
   ```bash
   voms-proxy-init -voms cms -rfc --valid 168:0
   ```
   The script checks for a valid proxy (minimum 20 minutes remaining).

2. **Rucio Client**
   - Automatically configured from CVMFS: `/cvmfs/cms.cern.ch/rucio/current`
   - Set via `RUCIO_HOME` environment variable

3. **X509 Certificate**
   ```bash
   # Store your proxy in a permanent location
   cp /tmp/x509up_u$(id -u) ~/.globus/x509_proxy
   ```

### For Open Data Submissions

1. **cernopendata-client** (optional but recommended)
   ```bash
   pip install cernopendata-client
   ```
   Falls back to REST API if not available.

2. **No authentication required** - Open Data is public

### Common Requirements

- **HTCondor** submission environment
- **Python 3.6+**
- **Analysis executable** (compiled C++ program)
- **Configuration files** properly formatted

## NANO/Rucio Submission

### Basic Usage

```bash
python core/python/generateSubmissionFilesNANO.py \
    -c config/submit_config.txt \
    -n MyJobName \
    -x ~/.globus/x509_proxy \
    -e build/analyses/MyAnalysis/myanalysis
```

### Command-Line Options

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `-c, --config` | Yes | Path to submit configuration file | - |
| `-n, --name` | Yes | Job submission name (creates `condorSub_<name>/`) | - |
| `-e, --exe` | Yes | Path to compiled executable | - |
| `-x, --x509` | Yes | Path to X509 proxy certificate | - |
| `-s, --size` | No | GB of data per job | 30 |
| `--stage-inputs` | No | Copy input files to worker before running | False |
| `--stage-outputs` | No | Copy outputs to destination after running | False |
| `--root-setup` | No | Command to setup ROOT (e.g., `source /path/to/thisroot.sh`) | "" |
| `--no-validate` | No | Skip configuration validation | False |
| `--make-test-job` | No | Create a local test job with one file | False |
| `--eos-sched` | No | Use EOS scheduling (for special cases) | False |

### Configuration File Format

**Main Config** (`config/submit_config.txt`):
```
# Core settings
saveDirectory=/eos/user/u/username/output
sampleConfig=config/samples.txt

# Analysis settings
bdtConfig=config/bdts.txt
onnxConfig=config/onnx_models.txt
# ... other plugin configs ...
```

**Sample Config** (`config/samples.txt`):
```
# Luminosity
lumi=138000

# Site whitelist (optional)
WL=T2_US_MIT,T2_US_Wisconsin

# Site blacklist (optional)
BL=T3_US_FNALLPC

# Samples
name=ttbar das=/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM xsec=831.76 type=0 norm=1.0 kfac=1.0
name=wjets das=/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM xsec=61526.7 type=1 norm=1.0 kfac=1.0
```

**Sample Configuration Fields**:
- `name`: Sample identifier (used in output filenames)
- `das`: DAS path for Rucio discovery
- `xsec`: Cross-section in pb
- `type`: Sample type integer (analysis-specific)
- `norm`: Normalization factor
- `kfac`: K-factor for corrections
- `extraScale`: Additional scaling factor (optional)
- `site`: Preferred site for this sample (optional)

### Example: Full Submission

```bash
# 1. Ensure proxy is valid
voms-proxy-info -all

# 2. Generate submission files
python core/python/generateSubmissionFilesNANO.py \
    -c config/submit_config.txt \
    -n ttbar_analysis_v1 \
    -x ~/.globus/x509_proxy \
    -e build/analyses/MyAnalysis/myanalysis \
    -s 50 \
    --stage-outputs \
    --make-test-job

# 3. Test locally first
cd condorSub_ttbar_analysis_v1/test_job
./myanalysis submit_config.txt

# 4. If test passes, submit
condor_submit condorSub_ttbar_analysis_v1/condor_submit.sub

# 5. Monitor jobs
condor_q
watch -n 30 condor_q
```

### How File Discovery Works

1. **Query Rucio**: Script queries Rucio for files in the DAS path
2. **Filter Sites**: Applies whitelist/blacklist to available replicas
3. **Group by Size**: Groups files to reach target size per job (e.g., 30 GB)
4. **Generate Redirectors**: Creates XRootD URLs with appropriate redirectors

Example output:
```
checking Rucio for /TTToSemiLeptonic.../NANOAODSIM
Output received
250 files found
groupCounts: {0: 12, 1: 11, 2: 13, ...}
groupSizes: {0: 29.8, 1: 30.1, 2: 30.3, ...}
21 groups found and 250 files found
```

## CERN Open Data Submission

### Basic Usage

```bash
python core/python/generateSubmissionFilesOpenData.py \
    -c config/submit_config.txt \
    -n OpenDataAnalysis \
    -e build/analyses/MyAnalysis/myanalysis
```

### Command-Line Options

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `-c, --config` | Yes | Path to submit configuration file | - |
| `-n, --name` | Yes | Job submission name | - |
| `-e, --exe` | Yes | Path to compiled executable | - |
| `-f, --files` | No | Number of ROOT files per job | 30 |
| `-x, --x509` | No | Path to X509 proxy (optional) | "" |
| `-a, --aux` | No | Copy aux directory into job folders | False |
| `--stage-inputs` | No | Copy input files to worker | False |
| `--stage-outputs` | No | Copy outputs to destination | False |
| `--root-setup` | No | Command to setup ROOT | "" |
| `--no-validate` | No | Skip configuration validation | False |
| `--make-test-job` | No | Create local test job | False |
| `--eos-sched` | No | Use EOS scheduling | False |

### Configuration File Format

**Sample Config** for Open Data:
```
# Luminosity
lumi=11580

# Record IDs from CERN Open Data
recids=24119,24120

# Samples
name=ZMuMu das=SMZee_0 type=10 xsec=1.0 norm=1.0
name=ZEE das=SMZee_1 type=11 xsec=1.0 norm=1.0
```

**Fields**:
- `recids`: Comma-separated CERN Open Data record IDs
- `das`: Dataset identifier in the Open Data records
- Other fields same as NANO submission

### Example: Open Data Submission

```bash
# 1. Generate submission files
python core/python/generateSubmissionFilesOpenData.py \
    -c config/opendata_config.txt \
    -n zmumu_opendata \
    -e build/analyses/ExampleAnalysis/example \
    -f 10 \
    --make-test-job

# 2. Test locally
cd condorSub_zmumu_opendata/test_job
./example submit_config.txt

# 3. Submit
condor_submit condorSub_zmumu_opendata/condor_submit.sub
```

## Configuration Validation

Both scripts automatically validate your configuration before generating jobs.

### What's Validated

**NANO Mode**:
- `saveDirectory` exists and is writable
- `sampleConfig` file exists
- Executable exists and is executable
- All referenced config files exist

**OpenData Mode**:
- Same as NANO mode
- Record IDs are valid

### Validation Output

**Warnings** (non-fatal):
```
Config validation warnings:
- Optional config file 'config/bdts.txt' not found (may be okay)
```

**Errors** (fatal):
```
Config validation failed:
- Required field 'saveDirectory' missing from config
- Executable not found: build/analyses/MyAnalysis/myanalysis
```

### Skipping Validation

```bash
python core/python/generateSubmissionFilesNANO.py \
    --no-validate \
    # ... other options ...
```

**Warning**: Only skip validation if you're sure your configuration is correct.

## Advanced Features

### Input Staging

Copy input files to the worker node before running:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --stage-inputs \
    # ... other options ...
```

**Benefits**:
- Faster data access (local disk vs remote XRootD)
- Reduces network load during processing
- More reliable for flaky network connections

**Process**:
1. Worker node copies files via `xrdcp` before running
2. Analysis reads from local disk
3. Files are cleaned up after job completes

**Configuration automatically updated**:
- Original: `fileList=root://xrootd//path/file.root`
- Staged: `fileList=input_0.root` (with `__orig_fileList` preserved)

### Output Staging

Copy outputs to final destination after analysis completes:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --stage-outputs \
    # ... other options ...
```

**Benefits**:
- Write to local disk during analysis (faster)
- Copy to EOS/XRootD only after success
- Automatic retry logic for failed transfers

**Process**:
1. Analysis writes to local disk (`output.root`)
2. After completion, `xrdcp` copies to final destination
3. Retry up to 3 times with backoff

**Configuration automatically updated**:
- Original: `saveFile=/eos/user/u/username/output.root`
- Staged: `saveFile=output.root` (with `__orig_saveFile` preserved)

### Test Job Creation

Create a local test job to verify your setup:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --make-test-job \
    # ... other options ...
```

**Creates**: `condorSub_<name>/test_job/` directory with:
- Single input file
- All configuration files
- Executable
- Auxiliary files

**To test**:
```bash
cd condorSub_<name>/test_job
./myanalysis submit_config.txt
```

**Always test locally before submitting hundreds of jobs!**

### Shared Input Strategy

Both scripts use a **shared inputs** strategy to optimize file transfers:

**Structure**:
```
condorSub_<name>/
├── shared_inputs/          # Staged once
│   ├── myanalysis         # Executable (copied to all jobs)
│   ├── aux/               # Auxiliary files (copied to all jobs)
│   └── x509               # Proxy (copied to all jobs)
├── job_0/                 # Per-job configs
│   ├── submit_config.txt
│   ├── floats.txt
│   └── ints.txt
├── job_1/
│   └── ...
└── condor_submit.sub
```

**Benefits**:
- Executable and aux copied once, not per job
- Reduces staging directory size
- Works with `condor_submit -spool` for EOS scheduling

### Custom ROOT Setup

Specify a command to setup ROOT on the worker:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --root-setup "source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh" \
    # ... other options ...
```

Inserted into run script before analysis execution.

### EOS Scheduling

For jobs submitted from EOS:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --eos-sched \
    # ... other options ...
```

Creates submission directory in: `/eos/user/<initial>/<username>/RDFAnalyzerCore/condorSub_<name>/`

**Note**: The EOS path is currently hardcoded in the script. For your own use, you may need to modify the script or use your own EOS path.

## Troubleshooting

### Common Issues

#### 1. Rucio Connection Errors

**Problem**:
```
Error: failed to list replicas for '/TTToSemiLeptonic...' after 5 attempts: ChunkedEncodingError
```

**Solution**:
- Check network connection to Rucio
- Script automatically retries with exponential backoff
- If persistent, try again later or use different dataset

#### 2. Proxy Expired

**Problem**:
```
VOMS proxy expired or non-existing: please run `voms-proxy-init -voms cms -rfc --valid 168:0`
```

**Solution**:
```bash
voms-proxy-init -voms cms -rfc --valid 168:0
```

#### 3. Executable Not Found

**Problem**:
```
Executable not found: build/analyses/MyAnalysis/myanalysis
```

**Solution**:
- Check executable path
- Ensure analysis is built: `source build.sh`
- Use absolute path or relative path from submission directory

#### 4. No Files Found for Dataset

**Problem**:
```
Warning: no files found for '/Dataset/Path/NANOAODSIM'
```

**Causes**:
- DAS path is incorrect
- Dataset not available via Rucio
- All files on blacklisted sites

**Solution**:
- Verify DAS path with `dasgoclient`
- Check site availability
- Adjust whitelist/blacklist

#### 5. Configuration Validation Fails

**Problem**:
```
Config validation failed:
- Required field 'saveDirectory' missing
```

**Solution**:
- Add missing field to configuration
- Check configuration file syntax
- Ensure no typos in field names

### Debugging Tips

1. **Enable test job**:
   ```bash
   --make-test-job
   ```
   Test locally before batch submission.

2. **Check generated files**:
   ```bash
   ls -la condorSub_<name>/job_0/
   cat condorSub_<name>/job_0/submit_config.txt
   ```

3. **Examine HTCondor script**:
   ```bash
   cat condorSub_<name>/condor_runscript.sh
   ```

4. **Review job logs**:
   ```bash
   cat condorSub_<name>/condor_logs/log_<cluster>_<process>.stdout
   cat condorSub_<name>/condor_logs/log_<cluster>_<process>.stderr
   ```

5. **Check staging**:
   If using `--stage-inputs` or `--stage-outputs`, check the run script for staging commands.

### Getting Help

- **Check logs**: HTCondor logs in `condorSub_<name>/condor_logs/`
- **Test locally**: Always use `--make-test-job` first
- **Validate config**: Don't skip validation
- **GitHub Issues**: Report bugs with full error messages

## Best Practices

1. **Always test locally first**
   ```bash
   --make-test-job
   ```

2. **Start small**
   - Submit a few jobs first
   - Verify outputs are correct
   - Then submit full dataset

3. **Use output staging for EOS**
   ```bash
   --stage-outputs
   ```
   More reliable than writing directly to EOS during analysis.

4. **Monitor proxy lifetime**
   ```bash
   voms-proxy-info -timeleft
   ```
   Jobs fail if proxy expires during execution.

5. **Check disk quotas**
   ```bash
   eos quota /eos/user/u/username
   ```
   Ensure sufficient space before submitting.

6. **Use appropriate job size**
   - Too small: Many jobs, high overhead
   - Too large: Long jobs, higher failure risk
   - Recommended: 30-50 GB per job (1-4 hours runtime)

7. **Organize output directories**
   ```
   /eos/user/u/username/
   ├── analysis_v1/
   ├── analysis_v2/
   └── analysis_v3/
   ```

8. **Version your submissions**
   ```bash
   -n my_analysis_v3
   ```

## See Also

- [Configuration Reference](CONFIG_REFERENCE.md) - Configuration file formats
- [Analysis Guide](ANALYSIS_GUIDE.md) - Building analyses
- [Getting Started](GETTING_STARTED.md) - Setup and installation

---

For questions or issues, please open an issue on GitHub.
