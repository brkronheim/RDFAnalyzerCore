# Recover a recorded compiled-command result

`rdfworkflow.read_command_result(log_directory)` explicitly reads a completed
controller's `run.json` and returns an immutable, detached `RunResult`. Use it
from a fresh Python process after the original controller or steering session
has disconnected:

```python
from rdfworkflow import read_command_result

result = read_command_result('/existing/controller/logs')
print(result.status, result.backend, result.outputs)
document = result.as_dict()  # independent JSON-compatible copy
```

The CLI provides the same explicit recovery without a specification file:

```bash
python -m rdfworkflow result /existing/controller/logs
```

It prints the common `rdf-run-result-v1` document. Exit zero means the record was
successfully read and validated, including when its saved worker status is
`failed`, `timed_out` or `cancelled`. The document retains that worker status.
A missing or invalid record exits one with an error document. The `result`
command never enters specification loading or worker execution.

This function opens only `<log_directory>/run.json`. It never executes a worker,
signals or probes a PID, reads `progress.json`, imports ROOT/NumPy/Numba, opens a
referenced output or decides completion from an output's existence. A missing,
incomplete or invalid completion record raises an exception; there is no
progress fallback. Use the existing observational `read_command_status` when
only a status snapshot is wanted.

The result retains the same process/declared-file completion scope as
`CommandResult.to_run_result()`. It records what the saved controller claimed;
it does not revalidate outputs, establish that a process is still alive, or
certify physics. `succeeded`, `failed`, `timed_out` and `cancelled` remain distinct.
A succeeded record requires return code zero. A failed record with return code
zero remains valid, for example when a child exits successfully but a declared
output is missing. Unavailable return codes remain `None` for other terminal
statuses.

`analysis` and `build` operations map to `compiled-executable` and `cmake-build`.
Other recorded operation strings remain in `details.operation` and use backend
`command`. The receiver preserves recorded command arguments, return code,
original log-directory reference, progress, issues, output references and
provenance/build identity. It does not relocate paths or hash current files.
Counts, sums and histogram summaries remain unavailable (`{}`), and
`event_loops` remains `None`.

The existing writer appends an `artifacts` mapping to the command envelope.
When present, the receiver preserves it in `details.recorded_artifacts` as
recorded context. This extra detail does not supply or repair provenance. A
legacy record with artifacts but no provenance still has `{}` provenance.
`CommandResult.to_run_result()` itself remains unchanged.

The receiver accepts the exact `rdf-command-result-v1` envelope fields emitted
by `CommandResult.as_dict()`, plus the writer's optional `artifacts` mapping.
Older records may omit `operation`, `progress` and `provenance`; their defaults
are `command`, `{}` and `{}`. All original fields, including the explicit
process completion scope, remain required. Unknown fields/schema versions,
nonterminal states, nonboolean/inconsistent success flags, malformed issue or
argument fields, invalid/nonfinite durations, unsupported native values and
duplicate JSON keys fail explicitly. The original command JSON, writer,
controller defaults and existing CLI formats are unchanged.

The standalone controller installation includes this stdlib-only API and its
stub without requiring the native Python stack. Focused installed and actual
stdlib-child evidence is recorded in
ux_recorded_results_20261107.
This checkpoint adds offline recovery to the
[existing result contract](RUN_RESULTS.md); it does not replace earlier frozen
benchmark runtimes or their evidence.
