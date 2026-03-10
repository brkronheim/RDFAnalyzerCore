# Performance Monitoring and Failure Handling

RDFAnalyzerCore automatically records execution metrics and classifies failures for every branch task in a LAW workflow.  This gives operators a quick way to identify slow jobs, memory-hungry samples, and systematic failure patterns without diving into individual log files.

---

## Overview

Two complementary systems work together:

- **`PerformanceRecorder`** (in `law/performance_recorder.py`) – a context manager that measures wall-clock time, peak subprocess RSS and data throughput, writing the results to a `.perf.json` sidecar file co-located with the branch output.
- **Failure handler** (in `law/failure_handler.py`) – classifies each exception into a `FailureCategory`, consults a per-category `RetryPolicy`, and accumulates all failure events in a `DiagnosticSummary` that is printed at the end of the run.

Both systems are integrated transparently into `DaskWorkflowProxy` (`law/workflow_executors.py`).  Task authors typically do not need to call them directly.

---

## PerformanceRecorder

### Class: `PerformanceRecorder`

A context manager that captures execution metrics for a named task or job.

```python
from performance_recorder import PerformanceRecorder

with PerformanceRecorder("MyTask[branch=3]") as rec:
    # ... do the work ...
    rec.set_throughput(input_bytes=total_bytes)
rec.save("/path/to/my_output.perf.json")
```

For subprocess jobs, start memory monitoring *after* launching the process:

```python
with PerformanceRecorder("analysis_job:job_7") as rec:
    proc = subprocess.Popen(cmd, shell=True)
    rec.monitor_process(proc.pid)
    proc.communicate()
    rec.set_throughput(input_bytes=estimate_job_input_bytes(job_dir))
rec.save(os.path.join(job_dir, "job.perf.json"))
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `task_name` | `str` | Descriptive label included in all serialised output. |
| `start_time` | `str \| None` | ISO 8601 UTC timestamp recorded at context entry. |
| `end_time` | `str \| None` | ISO 8601 UTC timestamp recorded at context exit. |
| `wall_time_s` | `float \| None` | Elapsed wall-clock seconds between context entry and exit. |
| `peak_rss_mb` | `float \| None` | Peak resident set size in MB (Linux only; `None` on other platforms or when `monitor_process()` was not called). |
| `throughput_mbs` | `float \| None` | Input-data throughput in MB/s; `None` when `set_throughput()` was not called. |

### Methods

#### `monitor_process(pid, poll_interval=0.25)`

Start a background thread that polls `/proc/{pid}/status` and all direct children every `poll_interval` seconds, tracking peak RSS.

- Must be called **inside** the `with` block, after the subprocess has been launched.
- Monitoring stops automatically when the context exits.
- **Linux only** – silently no-ops on macOS and Windows where `/proc` is unavailable.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pid` | — | PID of the process to monitor. |
| `poll_interval` | `0.25` | Seconds between memory samples. |

#### `set_throughput(input_bytes)`

Compute and store processing throughput as `input_bytes / wall_time_s / 1 048 576` (result in MB/s).

- May be called while the context is still active or immediately after exit (once `wall_time_s` is set).
- When `input_bytes ≤ 0`, no throughput is recorded.

#### `to_dict()`

Return a JSON-serialisable `dict` of all recorded metrics (see [Performance JSON Format](#performance-json-format)).

#### `save(path)`

Write metrics to `path` as a pretty-printed JSON file.  Parent directories are created automatically.

---

## Helper Functions

### `estimate_job_input_bytes(job_dir)`

Estimate the total size of input ROOT files for a prepared job by reading the `fileList` key from `submit_config.txt` inside `job_dir`.  Remote XRootD paths (not accessible on the local filesystem) are skipped.

```python
from performance_recorder import estimate_job_input_bytes
total = estimate_job_input_bytes("/eos/cms/store/user/me/jobs/job_3")
```

Returns `0` when `submit_config.txt` is absent, malformed, or all files are remote.

### `perf_path_for(output_path)`

Derive the `.perf.json` sidecar path from a task's primary output file path.  Strips a `.done`, `.json`, or `.txt` suffix (if present) and appends `.perf.json`.

```python
from performance_recorder import perf_path_for

perf_path_for("/jobs/job_3.done")       # → '/jobs/job_3.perf.json'
perf_path_for("/jobs/sample_0.json")    # → '/jobs/sample_0.perf.json'
perf_path_for("/run/submitted.txt")     # → '/run/submitted.perf.json'
```

---

## Performance JSON Format

Each `.perf.json` file is a single JSON object with the following fields:

```json
{
  "task_name": "analysis_job:job_7",
  "start_time": "2024-03-15T10:23:41.027483+00:00",
  "end_time": "2024-03-15T10:29:23.814201+00:00",
  "wall_time_s": 342.787,
  "peak_rss_mb": 1847.3,
  "throughput_mbs": 12.403
}
```

| Field | Type | Notes |
|-------|------|-------|
| `task_name` | string | Label passed to `PerformanceRecorder(task_name)`. |
| `start_time` | string | ISO 8601 UTC; `null` if the recorder was never entered. |
| `end_time` | string | ISO 8601 UTC; `null` if the recorder was never entered. |
| `wall_time_s` | number | Always recorded when the context exits normally. |
| `peak_rss_mb` | number \| null | `null` when not on Linux or `monitor_process()` was not called. |
| `throughput_mbs` | number \| null | `null` when `set_throughput()` was not called or `input_bytes ≤ 0`. |

---

## Automatic Recording in Framework Tasks

`_run_analysis_job()` in `workflow_executors.py` wraps every branch execution in a `PerformanceRecorder` and writes `job.perf.json` inside the job directory on the worker node:

```python
with PerformanceRecorder(task_label) as rec:
    proc = subprocess.Popen(full_cmd, shell=True, ...)
    rec.monitor_process(proc.pid)
    stdout_data, stderr_data = proc.communicate()
    rec.set_throughput(input_bytes)
rec.save(os.path.join(job_dir, "job.perf.json"))
```

After `law run` completes, the `DaskWorkflowProxy` copies `job.perf.json` from the job directory to the branch output directory (alongside the `.done` file) using `perf_path_for()`.  This means performance data is available at a **consistent location for all execution backends** (local, HTCondor, Dask):

```
job_outputs/
  job_0.done          ← LAW branch output marker
  job_0.perf.json     ← performance sidecar (copied by DaskWorkflowProxy)
  job_1.done
  job_1.perf.json
  ...
```

If `performance_recorder.py` cannot be imported on a worker (e.g. the module is not on the worker's `sys.path`), recording is silently skipped and the analysis job continues normally.

---

## Failure Classification System

### `FailureCategory` enum

Every exception caught during branch execution is assigned to one of six categories:

| Category | Value string | Description |
|----------|-------------|-------------|
| `TRANSIENT_IO` | `"transient_io"` | Temporary network or storage errors: XRootD/EOS/CVMFS connectivity, connection timeouts, NFS/AFS errors, GFAL/SRM errors. Usually worth retrying several times. |
| `EXECUTOR` | `"executor"` | Failures in the execution infrastructure: Dask worker crash, HTCondor hold, OOM kill, job preemption, batch-system errors (Slurm, PBS, LSF). Worth retrying with a longer backoff. |
| `CONFIGURATION` | `"configuration"` | Errors caused by bad config files, missing executables, permission denied, YAML/JSON parse errors. Retrying without fixing the root cause is unlikely to help. |
| `CORRUPTED_INPUT` | `"corrupted_input"` | ROOT file errors, checksum mismatches, truncated/malformed files, premature EOF, decompression errors. |
| `ANALYSIS_CRASH` | `"analysis_crash"` | Segfaults, SIGABRT, assertion failures, uncaught `std::exception`, stack overflow, floating-point exceptions, `double free`, heap corruption. |
| `UNKNOWN` | `"unknown"` | Unclassified / unexpected failures. A conservative retry policy is applied. |

---

## Retry Policies

### `RetryPolicy` dataclass

```python
@dataclass
class RetryPolicy:
    max_retries: int         # additional attempts after the first failure (0 = no retries)
    base_delay: float        # initial delay in seconds before the first retry
    backoff_factor: float    # multiplicative factor per retry (default 2.0)
    jitter: bool             # add up to 25 % random jitter to avoid thundering herds (default True)
```

The delay before retry *n* (1-based) is:

```
delay = base_delay × backoff_factor^(n-1)   [+ up to 25 % random jitter if jitter=True]
```

### `DEFAULT_RETRY_POLICIES`

| Category | `max_retries` | `base_delay` | `backoff_factor` | `jitter` |
|----------|:-------------:|:------------:|:----------------:|:--------:|
| `TRANSIENT_IO` | 5 | 10 s | 2.0× | True |
| `EXECUTOR` | 3 | 30 s | 2.0× | True |
| `CONFIGURATION` | 1 | 5 s | 1.0× | True |
| `CORRUPTED_INPUT` | 1 | 5 s | 1.0× | True |
| `ANALYSIS_CRASH` | 1 | 5 s | 1.0× | True |
| `UNKNOWN` | 2 | 15 s | 2.0× | True |

---

## `classify_failure(exc, stderr='')`

Heuristically maps an exception (and optional captured stderr text) to a `FailureCategory`.

```python
from failure_handler import classify_failure, FailureCategory

category = classify_failure(exc, stderr=captured_stderr)
```

### Classification logic

The combined string `str(exc) + "\n" + stderr` (lowercased) is matched against compiled regular-expression sets in **priority order**:

1. **Exception type checks** (fast path):
   - `FileNotFoundError`, `PermissionError` → `CONFIGURATION`
   - `OSError`, `ConnectionError`, `TimeoutError` → `TRANSIENT_IO`
   - `MemoryError` → `EXECUTOR`

2. **Pattern matching** (in priority order):
   1. `ANALYSIS_CRASH` – segfault, SIGABRT/signal 6/11, `std::exception`, `terminate called`, assertion failure, illegal instruction, bus error, double free, heap corruption, etc.
   2. `CORRUPTED_INPUT` – ROOT file errors, TFile corrupt/truncated/invalid, checksum mismatch, not a ROOT file, premature EOF, decompression error, malformed, bad magic/header.
   3. `TRANSIENT_IO` – xrootd/xrdfs, EOS error, connection refused/timed out, no route to host, I/O error, errno 5/11/110/111, CVMFS, NFS/AFS, GFAL, SRM error, temporary failure.
   4. `EXECUTOR` – worker lost/died/crashed, Dask scheduler unavailable, SIGKILL (signal 9), OOM kill, memory limit exceeded, job held/preempted/evicted, Slurm/PBS/LSF error.
   5. `CONFIGURATION` – no such file or directory, permission denied, config error/invalid/missing, YAML/JSON parse error, key error, attribute error, exe not found, setup script not found.

3. Falls back to `UNKNOWN` if no pattern matched.

---

## `run_with_retries(fn, args, kwargs, branch_num, summary, policies, sleep_fn)`

Execute `fn(*args, **kwargs)` with per-category retry logic.  Each failure is classified, a `FailureRecord` is added to `summary`, and the function sleeps for the policy-specified delay before retrying.  If all retries are exhausted, the final exception is re-raised.

```python
from failure_handler import run_with_retries, DiagnosticSummary, DEFAULT_RETRY_POLICIES

summary = DiagnosticSummary()
result = run_with_retries(
    fn=my_job_callable,
    args=[arg1, arg2],
    kwargs={"flag": True},
    branch_num=42,
    summary=summary,
    policies=DEFAULT_RETRY_POLICIES,
)
print(summary.report())
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fn` | — | Callable to execute. |
| `args` | — | Positional arguments for `fn`. |
| `kwargs` | `{}` | Keyword arguments for `fn`. |
| `branch_num` | `-1` | Branch index embedded in each `FailureRecord`. |
| `summary` | new instance | `DiagnosticSummary` to record each failure into. |
| `policies` | `DEFAULT_RETRY_POLICIES` | Per-category retry policies. |
| `sleep_fn` | `time.sleep` | Delay function – override in tests to avoid real sleeps. |

### Stderr extraction

`run_with_retries` automatically extracts `stderr` from `RuntimeError` messages produced by `_run_analysis_job`.  Those messages use the format `"…\nstderr:\n<text>"`, which lets `classify_failure` examine the actual subprocess output.

---

## DiagnosticSummary

Thread-safe accumulator for `FailureRecord` objects produced during a workflow run.

```python
from failure_handler import DiagnosticSummary, FailureRecord, FailureCategory

summary = DiagnosticSummary()
summary.add(FailureRecord(
    branch_num=3,
    attempt=1,
    category=FailureCategory.TRANSIENT_IO,
    message="XRootD timeout on xrootd://cms-xrd.cern.ch//store/…",
))
print(summary.report())
```

### `FailureRecord` fields

| Field | Type | Description |
|-------|------|-------------|
| `branch_num` | `int` | Workflow branch index that failed. |
| `attempt` | `int` | Attempt number (1 = first try, 2 = first retry, …). |
| `category` | `FailureCategory` | Classified failure category. |
| `message` | `str` | `str(exc)` – the exception message. |
| `traceback_text` | `str` | Full Python traceback; empty string if unavailable. |
| `timestamp` | `str` | ISO 8601 UTC timestamp of when the failure was recorded. |

### `DiagnosticSummary` API

| Method / property | Returns | Description |
|-------------------|---------|-------------|
| `add(record)` | `None` | Append a `FailureRecord`. |
| `records` | `list[FailureRecord]` | All recorded failure events (copy). |
| `by_category()` | `dict[FailureCategory, list[FailureRecord]]` | Group records by category. |
| `failed_branches()` | `set[int]` | Set of branch numbers with at least one failure. |
| `total_failures()` | `int` | Total number of recorded failure events. |
| `report()` | `str` | Human-readable multi-line summary (see below). |

### `report()` output format

```
DiagnosticSummary: 7 failure event(s) across 3 branch(es).

Failures by category:
  transient_io        :    5 event(s)
  analysis_crash      :    2 event(s)

Individual failure events:
  [2024-03-15T10:31:02Z] branch=3 attempt=1 category=transient_io
    XRootD timeout on xrootd://cms-xrd.cern.ch//store/…
  [2024-03-15T10:31:22Z] branch=3 attempt=2 category=transient_io
    ...
```

---

## Integration with the Dask Executor

`DaskWorkflowProxy.run()` (in `workflow_executors.py`) integrates both systems transparently:

1. Each branch future is submitted to the Dask cluster.
2. When a future fails, `classify_failure()` determines the category and `run_with_retries()` re-submits new futures for subsequent attempts.
3. The first failure of each branch is recorded manually (before retries) and its category is announced via `task.publish_message()`.
4. All retry failures are recorded automatically by `run_with_retries()`.
5. After all futures have settled, `summary.report()` is printed if any failures were recorded.
6. If any branch ultimately fails (retries exhausted), a `RuntimeError` is raised listing all failed branches.

```python
# Simplified from DaskWorkflowProxy.run():
summary = DiagnosticSummary()
for future in dask_ac(futures):
    try:
        result = future.result()
    except Exception as first_exc:
        # Re-submit with smart retry
        result = run_with_retries(fn=_submit_and_wait, ..., summary=summary)
        category = classify_failure(first_exc, stderr=...)
        task.publish_message(f"classified as {category.value!r}; retrying…")

if summary.total_failures() > 0:
    task.publish_message(summary.report())
```

---

## Custom Retry Policies

Override the `dask_retry_policies` **class attribute** in your task to customise retry behaviour per category:

```python
from failure_handler import DEFAULT_RETRY_POLICIES, FailureCategory, RetryPolicy
from workflow_executors import DaskWorkflow
import law

class RunMyJobs(MyMixin, law.LocalWorkflow, DaskWorkflow):

    # Increase TRANSIENT_IO retries to 10 with a shorter base delay;
    # keep defaults for all other categories.
    dask_retry_policies = {
        **DEFAULT_RETRY_POLICIES,
        FailureCategory.TRANSIENT_IO: RetryPolicy(
            max_retries=10,
            base_delay=5.0,
            backoff_factor=2.0,
        ),
    }

    def get_dask_work(self, branch_num, branch_data):
        ...
```

`dask_retry_policies` must be a plain dict (not a method); the proxy reads it with `getattr(task, "dask_retry_policies", DEFAULT_RETRY_POLICIES)`.
