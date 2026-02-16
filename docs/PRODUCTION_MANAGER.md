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

### Submission Backends

- **HTCondor**: Traditional batch submission with condor_submit
- **DASK**: Python-based distributed computing with dask-jobqueue

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

### 1. Create a Production

```bash
python core/python/production_submit.py \
    --name my_analysis \
    --config cfg/analysis_config.txt \
    --sample-config cfg/samples.txt \
    --exe build/analyses/MyAnalysis/myanalysis \
    --submit
```

This will:
1. Discover input files from Rucio
2. Generate job configurations
3. Submit jobs to HTCondor
4. Save state to `condorSub_my_analysis/production_state.json`

### 2. Monitor Progress

```bash
# Interactive monitoring with curses interface
python core/python/production_monitor.py monitor --name my_analysis

# Simple text-based monitoring
python core/python/production_monitor.py monitor --name my_analysis --simple

# One-time status check
python core/python/production_monitor.py status --name my_analysis
```

### 3. Validate Outputs

```bash
python core/python/production_monitor.py validate --name my_analysis
```

### 4. Resubmit Failed Jobs

```bash
python core/python/production_monitor.py resubmit --name my_analysis
```

## Architecture

### Components

```
production_manager.py          # Core ProductionManager class
├── ProductionConfig          # Configuration dataclass
├── Job                       # Job tracking dataclass
├── JobStatus                 # Job status enumeration
└── ProductionManager         # Main manager class

production_submit.py          # Production creation and submission
production_monitor.py         # Monitoring and management CLI
test_production_manager.py    # Test suite
```

### State Management

Production state is persisted to `production_state.json` in the work directory:

```json
{
  "production_name": "my_analysis",
  "timestamp": 1234567890.0,
  "jobs": {
    "0": {
      "job_id": 0,
      "config_path": "/path/to/config.txt",
      "output_path": "/path/to/output.root",
      "status": "completed",
      "condor_job_id": "12345.0",
      "submit_time": 1234567890.0,
      "attempts": 1
    }
  }
}
```

### Job Lifecycle

```
CREATED → SUBMITTED → RUNNING → COMPLETED → VALIDATED
                          ↓
                       FAILED/MISSING_OUTPUT
                          ↓
                      (resubmit)
```

## Usage Guide

### Creating a Production

#### Option 1: Using production_submit.py (Recommended)

```bash
python core/python/production_submit.py \
    --name my_production \
    --config cfg/base_config.txt \
    --sample-config cfg/samples.txt \
    --exe /path/to/analyzer \
    --output-dir /eos/user/u/username/outputs \
    --size 30 \
    --backend htcondor \
    --stage-inputs \
    --stage-outputs \
    --submit
```

Options:
- `--name`: Production name (required)
- `--config`: Base analysis configuration (required)
- `--sample-config`: Sample list for data discovery (required)
- `--exe`: Path to analysis executable (required)
- `--output-dir`: Output directory (default: from config)
- `--size`: GB per job (default: 30)
- `--backend`: htcondor or dask (default: htcondor)
- `--stage-inputs`: Copy input files to worker nodes
- `--stage-outputs`: Copy outputs back from worker nodes
- `--submit`: Submit jobs immediately
- `--dry-run`: Generate but don't submit

#### Option 2: Using ProductionManager API

```python
from pathlib import Path
from production_manager import ProductionManager, ProductionConfig

# Create config
config = ProductionConfig(
    name="my_production",
    work_dir=Path("condorSub_my_production"),
    exe_path=Path("/path/to/analyzer"),
    base_config="cfg/config.txt",
    output_dir=Path("/eos/user/u/username/outputs"),
    backend="htcondor",
    stage_inputs=True,
    stage_outputs=True,
)

# Create manager
manager = ProductionManager(config)

# Generate jobs
file_lists = [
    "root://xrootd/file1.root,root://xrootd/file2.root",
    "root://xrootd/file3.root,root://xrootd/file4.root",
]
manager.generate_jobs(file_lists)

# Submit
manager.submit_jobs()

# Monitor
manager.update_status()
manager.print_progress()
```

### Monitoring Productions

#### Interactive Monitoring

```bash
# Curses interface (default)
python core/python/production_monitor.py monitor --name my_production

# Simple text interface
python core/python/production_monitor.py monitor --name my_production --simple

# Custom refresh interval (seconds)
python core/python/production_monitor.py monitor --name my_production --refresh 60
```

The curses interface shows:
- Total jobs and status breakdown
- Progress bars for completion, validation, and failures
- Recent job activity with runtimes
- Real-time updates

Press `q` to quit the monitor.

#### One-Time Status Check

```bash
python core/python/production_monitor.py status --name my_production
```

#### List All Productions

```bash
# List productions in current directory
python core/python/production_monitor.py list

# List productions in specific directory
python core/python/production_monitor.py list --dir /path/to/submissions
```

### Validating Outputs

```bash
python core/python/production_monitor.py validate --name my_production
```

This will:
1. Check if output files exist
2. Verify files are non-empty
3. Attempt to open with ROOT to verify integrity
4. Update job status to VALIDATED or MISSING_OUTPUT

### Resubmitting Failed Jobs

```bash
# Resubmit with default retry limit (3)
python core/python/production_monitor.py resubmit --name my_production

# Resubmit with custom retry limit
python core/python/production_monitor.py resubmit --name my_production --max-attempts 5
```

Only jobs that haven't exceeded the maximum attempts will be resubmitted.

## Resilience and Recovery

### State Persistence

The Production Manager automatically saves state after every operation:
- Job generation
- Status updates
- Submission
- Validation

If your session is interrupted:

```bash
# Resume by creating a new manager with the same work directory
python core/python/production_monitor.py status --work-dir condorSub_my_production
```

The manager will automatically load the previous state and continue from where it left off.

### Handling Connection Failures

The Production Manager is designed to handle connection failures gracefully:

1. **AFS/EOS Token Expiration**: State is saved locally, so you can renew tokens and resume
2. **Network Interruptions**: Monitor can be restarted anytime
3. **Batch System Issues**: Job status is queried fresh each time

### Working in AFS/EOS

```bash
# Production in EOS
python core/python/production_submit.py \
    --name my_prod \
    --work-dir /eos/user/u/username/productions/my_prod \
    --output-dir /eos/user/u/username/outputs \
    --config cfg/config.txt \
    --sample-config cfg/samples.txt \
    --exe ./analyzer \
    --stage-outputs
```

Note: When working in EOS, use `--stage-outputs` to avoid issues with worker node access.

## Backend Support

### HTCondor Backend

Default backend using traditional HTCondor submission:

```bash
python core/python/production_submit.py \
    --backend htcondor \
    ...
```

Features:
- Uses existing condor submission infrastructure
- Supports input/output staging with xrdcp
- Integrates with existing resubmit_jobs.py
- Well-tested and stable

### DASK Backend

Experimental backend using DASK distributed:

```bash
# Install DASK first
pip install dask distributed dask-jobqueue

python core/python/production_submit.py \
    --backend dask \
    ...
```

Features:
- Pure Python submission
- Dynamic scaling
- Better for interactive analysis
- Integration with Python workflows

Note: DASK backend is experimental and may require additional configuration.

## Examples

### Example 1: Simple Production

```bash
# Generate and submit
python core/python/production_submit.py \
    --name ttbar_analysis \
    --config cfg/ttbar.txt \
    --sample-config cfg/ttbar_samples.txt \
    --exe build/analyses/TTbar/ttbar \
    --submit

# Monitor
python core/python/production_monitor.py monitor --name ttbar_analysis
```

### Example 2: Production with Staging

For analyses with large input files or unreliable xrootd access:

```bash
python core/python/production_submit.py \
    --name large_production \
    --config cfg/config.txt \
    --sample-config cfg/samples.txt \
    --exe ./analyzer \
    --stage-inputs \
    --stage-outputs \
    --submit
```

### Example 3: Resume After Interruption

```bash
# Original submission was interrupted
# Resume by checking status
python core/python/production_monitor.py status --name my_prod

# Submit any jobs that weren't submitted (using production_submit.py --submit)
# Or manually trigger submission:
cd condorSub_my_prod && condor_submit condor_submit.sub

# Continue monitoring
python core/python/production_monitor.py monitor --name my_prod
```

### Example 4: Manage Multiple Productions

```bash
# List all productions
python core/python/production_monitor.py list

# Check status of each
for prod in prod1 prod2 prod3; do
    echo "=== $prod ==="
    python core/python/production_monitor.py status --name $prod
done

# Resubmit failures in all
for prod in prod1 prod2 prod3; do
    python core/python/production_monitor.py resubmit --name $prod
done
```

## Troubleshooting

### Jobs Not Submitting

1. Check HTCondor is available:
   ```bash
   condor_q
   ```

2. Verify executable exists:
   ```bash
   ls -l /path/to/analyzer
   ```

3. Check work directory permissions:
   ```bash
   ls -ld condorSub_*
   ```

### Jobs Failing Immediately

1. Check job logs:
   ```bash
   cat condorSub_my_prod/condor_logs/log_*.stderr
   ```

2. Test job locally (after generation):
   ```bash
   # Test by running the job's config directly
   cd condorSub_my_prod/job_0
   /path/to/analyzer job_config.txt
   ```

3. Check configuration:
   ```bash
   cat condorSub_my_prod/job_0/job_config.txt
   ```

### Output Validation Failing

1. Check output directory exists and is writable:
   ```bash
   ls -ld /path/to/outputs
   ```

2. Check for disk space:
   ```bash
   df -h /path/to/outputs
   ```

3. Verify ROOT files manually:
   ```bash
   root -l /path/to/outputs/output_0.root
   ```

### State File Corruption

If `production_state.json` becomes corrupted:

```bash
# Backup current state
cp condorSub_my_prod/production_state.json condorSub_my_prod/production_state.json.bak

# Try to recover or regenerate
# (This will lose progress information but preserve job definitions)
python core/python/production_submit.py \
    --name my_prod \
    --work-dir condorSub_my_prod \
    ...
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

- **generateSubmissionFilesNANO.py**: Used for data discovery
- **submission_backend.py**: Shared HTCondor submission logic
- **resubmit_jobs.py**: Can still be used for manual resubmission
- **validate_config.py**: Automatic configuration validation

## Future Enhancements

Planned improvements:

- [ ] LAW (Luigi Analysis Workflow) integration
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
