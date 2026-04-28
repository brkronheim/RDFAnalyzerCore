# Production Manager Guide

The Production Manager provides a unified, cohesive system for managing batch analysis productions in RDFAnalyzerCore. It handles job generation, submission, monitoring, validation, and failure recovery with state persistence to support resilient operations.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Usage Guide](#usage-guide)
- [Monitoring](#monitoring)
- [Resilience and Recovery](#resilience-and-recovery)
- [Backend Support](#backend-support)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

## Overview

The Production Manager is designed to address common challenges in large-scale batch analyses:

- **Unified Interface**: Single system for the entire production lifecycle
- **State Persistence**: Can stop and restart without losing progress
- **Progress Monitoring**: Real-time status updates and progress tracking
- **Output Validation**: Automatic verification of job outputs
- **Failure Recovery**: Automatic resubmission of failed jobs
- **Multiple Backends**: Support for HTCondor and DASK
- **Storage Support**: Works in AFS and EOS areas

## Features

### Job Management

- Automatic job generation from data discovery (Rucio/CERN Open Data)
- Configuration validation before submission
- Per-job configuration management
- Shared executable and auxiliary file staging
- **Automatic shared library (.so) discovery and staging**
- **C++ executable wrapper for DASK compatibility**

### Submission Backends

- **HTCondor**: Traditional batch submission with condor_submit
  - Automatic .so file transfer to worker nodes
  - LD_LIBRARY_PATH setup on execution nodes
- **DASK**: Python-based distributed computing with dask-jobqueue
  - Python wrapper for C++ executables
  - Shared library staging for remote execution

### Monitoring and Validation

- Real-time progress monitoring (text or curses interface)
- Automatic job status updates
- Output file validation (existence, size, ROOT file integrity)
- Progress statistics and reporting

### Resilience

- State persistence to JSON (can resume after disconnection)
- Automatic retry of failed jobs with configurable limits
- Graceful handling of connection failures
- Works in network file systems (AFS/EOS)

## Quick Start

`core/python/production_submit.py` has been removed and is no longer available. New productions should be created via LAW file-discovery tasks such as `law run GetRucioFileList` / `law run SkimTask`, and `core/python/production_manager.py` or `production_monitor.py` should be used for submission, status, validation, and resubmission.

### 1. Create a Production

The current recommended workflow uses LAW tasks to generate job configurations from YAML dataset manifests. For example, use `law run GetRucioFileList` / `law run SkimTask` with `--file-source rucio` or `--file-source opendata`.

### 3. Validate Outputs

```bash
# production_submit.py has been removed; use LAW discovery tasks and production_monitor.py instead.
```

### Permission Denied in AFS/EOS

```bash
# Check AFS token
klist
aklog

# Check EOS authentication
eos whoami

# Verify directory permissions
fs la condorSub_my_prod  # For AFS
eos ls -l /eos/user/...  # For EOS
```

## Best Practices

1. **Use Descriptive Names**: Choose meaningful production names
2. **Stage Outputs for EOS**: Always use `--stage-outputs` when output is in EOS
3. **Monitor Regularly**: Check progress periodically, especially for long productions
4. **Validate Before Merging**: Always validate outputs before merging results
5. **Clean Up**: Remove work directories after successful completion
6. **Test Locally First**: Run test jobs locally before large-scale submission

## Integration with Existing Tools

The Production Manager integrates with existing RDFAnalyzerCore tools:

- **submission_backend.py**: Shared HTCondor submission logic
- **resubmit_jobs.py**: Can still be used for manual resubmission
- **validate_config.py**: Automatic configuration validation
- The legacy `generateSubmissionFilesNANO.py` and `generateSubmissionFilesOpenData.py` scripts have been removed; use LAW workflows instead.

## Law Workflow Integration

The Production Manager integrates with the Law (Luigi Analysis Workflow) task framework for managing complex analysis workflows on batch systems. Law provides a powerful task-based workflow system with automatic dependency resolution, retry logic, and progress tracking.

### Available Law Tasks

#### Analysis and Discovery Tasks

Law tasks for dataset discovery and skim submission:

- **`GetRucioFileList`**: Discover input file lists from Rucio using a dataset manifest YAML or legacy sample-config compatibility layer.
  - Writes per-dataset JSON lists consumed by `SkimTask` / `PrepareSkimJobs`
  - Preserves Rucio group metadata and replica selection
  - Supports `--submit-config`, `--name`, `--x509`, and `--exe`

- **`SkimTask`**: Run skim/extraction jobs over datasets discovered via Rucio, XRootD, or explicit file lists.
  - Accepts `--dataset-manifest .../datasets.yaml`
  - Supports the `--file-source rucio` workflow source
  - Produces per-branch skim outputs and cache sidecars

- **`SubmitSkimJobs`**: Build and submit skim jobs to HTCondor or Dask.
  - Uses the same analysis manifest and submit configuration
  - Generates job directories and shared input staging

- **`MonitorSkimJobs`**: Monitor running skim jobs and report failures for resubmission.

#### Plotting Tasks

Law tasks for creating physics plots:

- **`MakePlot`**: Generate a single stack plot
  - Uses PlottingUtility Python bindings
  - Supports data/MC ratio panels
  - Configurable via PlotRequest objects

- **`MakePlots`**: Batch plotting from configuration
  - Generates multiple plots in parallel
  - Reads plot specifications from config files
  - Automatic output organization

#### Combine Datacard Tasks

Law tasks for CMS combine statistical analysis:

- **`CreateDatacard`**: Generate CMS combine datacards
  - Extracts histograms from analysis outputs
  - Formats systematic uncertainties
  - Writes datacard in combine format

- **`RunCombine`**: Execute statistical fits
  - Runs combine tool with specified method
  - Supports AsymptoticLimits, FitDiagnostics, etc.
  - Collects fit results

### Example Law Workflows

#### Basic LAW Rucio Analysis

```bash
# Discover files via Rucio
law run GetRucioFileList \
    --submit-config analyses/myAnalysis/cfg/submit_config.txt \
    --name myRun --x509 /tmp/x509 --exe build/bin/myanalysis

# Run skim jobs using the discovered Rucio file lists
law run SkimTask \
    --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \
    --submit-config analyses/myAnalysis/cfg/submit_config.txt \
    --name mySkimRun \
    --file-source rucio \
    --file-source-name myRun \
    --exe build/bin/myanalysis
```

#### Statistical Analysis Workflow

```bash
# Create datacards from analysis outputs
law run CreateDatacard \
    --datacard-config cfg/datacard.yaml \
    --name myRun \
    --input-dir outputs/

# Run combine fit
law run RunCombine \
    --name myRun \
    --method AsymptoticLimits \
    --datacard datacards/myRun.txt
```

#### Plotting Workflow

```bash
# Single plot
law run MakePlot \
    --meta-file outputs/meta.root \
    --output-file plots/pt.pdf \
    --histogram-name jet_pt

# Batch plotting from config
law run MakePlots \
    --plot-config cfg/plots.yaml \
    --output-dir plots/
```

### Law Task Dependencies

Law automatically manages task dependencies. For example, `SkimTask` can depend on `GetRucioFileList` when `--file-source rucio` is enabled, so running:

```bash
law run SkimTask \
    --dataset-manifest cfg/datasets.yaml \
    --submit-config cfg/submit_config.txt \
    --name mySkimRun \
    --file-source rucio --file-source-name myRun
```

Will automatically:
1. Discover input files (if configured)
2. Schedule the skim workflow
3. Track job completion and write outputs

### Task Configuration

Law tasks use dataset manifests in YAML and submit-config templates for runtime settings.

Example dataset manifest:

```yaml
datasets:
  - name: mydataset
    files:
      - root://xrootd.example.org//path/to/file1.root
      - root://xrootd.example.org//path/to/file2.root
```

Submit config templates remain compatible with existing backends:

```ini
[submit]
queue = 1
memory = 4GB
```

Additional Law-specific configuration can be provided via command-line parameters or Law config files (`law.cfg`).

### Integration Benefits

Using Law with the Production Manager provides:

- **Dependency Management**: Automatic task ordering and execution
- **Retry Logic**: Failed tasks are automatically retried
- **Caching**: Completed tasks are not re-executed unnecessarily
- **Parallel Execution**: Independent tasks run in parallel
- **Progress Tracking**: Built-in status reporting and logging
- **Workflow Visualization**: Generate workflow graphs with `law run --print-status`

See [Combine Integration](COMBINE_INTEGRATION.md) for detailed datacard and statistical analysis workflows.

## Future Enhancements

Planned improvements:

- [ ] Automatic output merging
- [ ] Email notifications for completion/failures
- [ ] Web-based monitoring dashboard
- [ ] Support for additional batch systems (SLURM, PBS)
- [ ] Job priority management
- [ ] Resource usage statistics

## Support

For issues or questions:
- Check the [troubleshooting section](#troubleshooting)
- Review existing issues on GitHub
- Consult the main [README](../README.md) and [batch submission docs](BATCH_SUBMISSION.md)

---

**Related Documentation:**
- [Batch Submission Guide](BATCH_SUBMISSION.md)
- [Configuration Reference](CONFIG_REFERENCE.md)
- [Analysis Guide](ANALYSIS_GUIDE.md)
