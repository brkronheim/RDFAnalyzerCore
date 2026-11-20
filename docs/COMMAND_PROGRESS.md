# Compiled-controller progress and status

The optional observer reports the local controller's last state while a compiled
analysis or explicit CMake build runs. It uses the Python standard library in
the parent process. Physics, graph construction and the event loop stay in the
compiled executable and its linked libraries.

```python
from rdfworkflow import read_command_status

# spec is a CppAnalysisSpec; use a fresh log directory.
command = spec.run("/absolute/logs/run-001", progress_interval_s=5)
print(command.status, command.progress)

# From another process while it runs, or later:
status = read_command_status("/absolute/logs/run-001")
print(status["phase"], status["status"], status["terminal"])
```

The same optional `progress_interval_s` argument is supported by
`spec.prepare().run(...)` and `CMakeBuildSpec.build(...)`. `None` (the default)
disables reporting entirely. Enabled intervals must be finite numbers between
0.5 and 3600 seconds. The observer writes a small atomic `progress.json` at
phase transitions and no more often than the interval during the running phase.
It overwrites that file rather than accumulating a history. It reads no logs
and registers no per-event callbacks. Declared build products cannot use the
controller's `progress.json` path.

The command line accepts the same option for explicit `run` and `build`:

```bash
python -S -m rdfworkflow run /absolute/spec.json \
  --directory /absolute/logs/run-001 --progress-interval 5

# In another terminal; no spec or native runtime is required:
python -S -m rdfworkflow status /absolute/logs/run-001
```

The installed [steering example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/compiled-controller/README.md)
accepts `--progress-interval 5` with `--mode run`, including an explicitly
requested build. Inspect and validate modes never start progress reporting.

## States and completion

| Phase | Meaning |
|---|---|
| `launching` | Artifact preparation/verification has finished; child launch is pending |
| `running` | The child has been launched; the controller is waiting |
| `stopping` | The controller has requested cancellation or timeout termination |
| `finalizing` | Child handling is complete; declared outputs/build identity and the result record are being finalized |
| `finished` | The controller committed its completion record |

Snapshots record the operation (`analysis` or `build`), elapsed seconds since
launch handling started, wall-clock update time, sequence, and child PID/return
code when available. A stopping snapshot records `requested_status`. These are
observations, not checks that the PID is still alive. They contain no event
count, percent complete, inferred JIT/graph phase or estimated finish time.
Preparation and hash verification happen before reporting starts. During them,
and during a run with reporting disabled, status may have no record to read.
Long finalization does not receive periodic updates.

`read_command_status()` prefers `run.json`. A valid completion record returns
`terminal=True` and `succeeded`, `failed`, `cancelled` or `timed_out`. Successful
process exit still requires all declared outputs and optional build-identity
checks to pass. This is operational completion, with the existing
[compiled-workflow scope](COMPILED_CPP_WORKFLOWS.md); the reader does not reopen
or physics-validate those outputs. Native and production completion/merge
contracts keep their existing meaning.

Without `run.json`, every returned observation has `terminal=False`. A
`finished` snapshot alone returns `status="unconfirmed"`, preserving its
`reported_status`. Other snapshots retain their recorded status and may be
stale after a controller crash. Missing records raise `FileNotFoundError`;
malformed or inconsistent records raise `ValueError`. A corrupt completion
record is reported rather than hidden by a progress snapshot.

The CLI `status` exits zero when it successfully **reads** a status, including
a recorded failed command. Inspect the JSON `status` and `terminal` fields to
decide what to display or do next. Missing/invalid records produce exit one.
The command never launches, signals, retries or resumes a job.

## Reporting cost and failures

Reporting is disabled by default. Enabled reporting adds synchronous small
filesystem writes in the parent, outside the event loop. Its write frequency
and record size are bounded; I/O latency on a stalled filesystem is not.
The resumed native A/B acceptance is complete. Three measured six-file blocks
gave median whole-worker rates of 644.64 kHz disabled, 641.78 kHz at five seconds
and 642.48 kHz at 0.5 seconds. Enabled-minus-disabled paired wall differences
changed sign (roughly -66 to +114 ms), so this does not establish an isolated
event-loop penalty or zero overhead. Exact outputs, one loop, bounded snapshots
and disabled-mode absence of progress metadata all pass.

A snapshot write error disables further snapshots and is captured in
`CommandResult.progress["write_error"]`, without changing the child's result or
its failure issues. `CommandResult.as_dict()` includes that diagnostic mapping;
the [common result envelope](RUN_RESULTS.md) includes it in `details.progress`.
With reporting disabled the mapping is empty.

The authoritative completion record must still be written successfully.
Failure to commit it raises an error; no terminal success snapshot is emitted.
If the final snapshot fails after completion was committed, refreshing the
record with that diagnostic is best effort. The returned result contains the
error, but a failed refresh can leave the committed record without that last
diagnostic. Neither case changes the recorded child's operational outcome.
