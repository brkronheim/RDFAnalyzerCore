# Detached run results

The additive `rdfresults.RunResult` envelope provides a common view of Python
analysis, session and compiled-controller results. It uses only the standard
library. Creating or serializing an envelope never starts processing, discovers
providers, opens outputs or imports ROOT, NumPy or Numba.

The optional C++17 [RunResult.h helper](NATIVE_RUN_RESULTS.md) emits the same
schema from caller-owned, already evaluated values. It is installed through
`RDFAnalyzer::NativeAuthoring`; direct C++ execution and Python orchestration
remain separate from serialization.

## Python analyses and sessions

```python
analysis.run()                       # still returns the Analysis
result = analysis.result()           # explicit snapshot after successful run
print(result.counts)
document = result.as_dict()          # detached JSON-compatible mutable copy

session_result = session.run(config, build_analysis)
result = session_result.to_run_result()
```

`Analysis.result()` reads the already completed, booked in-memory counts,
histograms and sums. It makes no new native action or event loop. Calling it
before execution, during execution or after a failed execution raises an error
and does not run the graph. Native exceptions from `run()` still propagate.
An Analysis can be executed once; start a new Analysis after an execution
failure. Repeated successful snapshots are allowed.

Snapshots contain recursively immutable copies of finite JSON-compatible data;
they retain no analysis, native graph or file handle. Modifying the dictionary
returned by `as_dict()` does not affect the snapshot. Signed histogram contents,
variances, flow bins and sum values are preserved without precision conversion.
Nonfinite or unsupported native objects cause a serialization-contract error
on explicit snapshot creation; they are not silently rewritten. Requesting a
large histogram snapshot has a copying/memory cost. Ordinary `run()` does not
copy these results automatically.

Existing session `AnalysisResult` fields and `as_dict()` stay available. Its new
conversion method makes an immutable copy without changing the older object.
Python run completion means the native lifecycle returned successfully; it
does not independently validate persisted files or physics. Metadata and skim
paths are references. Missing provenance is an empty mapping, not an inferred
identity from current files.

## Compiled executables and builds

```python
command = spec.run("/absolute/new/logs", check=False)
result = command.to_run_result()
print(result.status, result.completion_scope)
print(result.outputs, result.provenance)

build_command = spec.build_spec.build("/absolute/new/build-logs", check=False)
build_result = build_command.to_run_result()
```

The compiled controller remains independent of the native Python stack. No
summary JSON format is imposed on the executable, and no output is reread.
Counts and histogram summaries are therefore unavailable unless an explicit,
analysis-owned adapter supplies them in a separate `RunResult`.

An executable may explicitly emit the complete `rdf-run-result-v1` envelope.
The caller chooses when/how to acquire that document and validates it with the
stdlib-only receiver:

```python
import json
from pathlib import Path
from rdfresults import RunResult

command = spec.run('/absolute/new/logs')
document = json.loads(Path(command.outputs['summary']).read_text())
native_result = RunResult.from_dict(document)
print(native_result.counts, native_result.completion_scope)
```

Here `summary` must be an explicitly declared output, produced by an
analysis-owned emitter. Acquisition is separate from `from_dict`: the receiver
only validates supplied values and creates a detached immutable snapshot. It
does not run native actions or open any referenced output. Child-process status
and the emitter's reported scope remain distinct; callers decide how to use both.
No automatic native emitter or Analyzer interception is installed.

The receiver requires every field shown below, exactly the shape returned by
`RunResult.as_dict()`. Unavailable summaries are `{}`, unavailable loop count is
`null`, and issues are an array. Unknown fields/schema versions, incomplete
documents, nonfinite/native values, invalid counts/timings and a boolean
`success` inconsistent with `status` fail explicitly. Extensions belong inside
`details`/`provenance`, rather than adding unversioned top-level fields. Serialized
success remains a caller-reported claim within `completion_scope`; the receiver
does not certify physics, file validity or runtime provenance.

`succeeded`, `failed`, `timed_out` and `cancelled` remain distinct statuses.
Exit zero with a missing declared output is a failure. `check=True` retains the
existing `ExecutionError`, whose `.result` is the `CommandResult` and supports
the same conversion. Recorded executable/assets identity and, when supplied,
build identity carry through from memory. They describe declared artifacts,
not a proof of actual loaded libraries or a validated production output.

`CommandResult.as_dict()` keeps `rdf-command-result-v1` and adds `operation`,
`progress` and `provenance` fields. Existing fields retain their meanings. Optional
[controller reporting](COMMAND_PROGRESS.md) places its diagnostic mapping in
`details.progress` on conversion; it is empty when reporting is disabled. `operation` is
`analysis` for native analysis launches and `build` for CMake launches; generic
manually constructed command results default to `command`. Common backend
labels are respectively `compiled-executable`, `cmake-build` and `command`,
so a successful build is not reported as an analysis execution.

## Envelope fields and timing scope

| Field | Meaning |
|---|---|
| `schema` | `rdf-run-result-v1` in `as_dict()` |
| `backend`, `status`, `success` | Adapter identity and its operational outcome |
| `completion_scope` | Explicit boundary of the success claim |
| `counts`, `histograms`, `sums` | Available completed summaries; empty means unavailable |
| `timings` | Named durations in seconds, with adapter-specific scope below |
| `outputs` | Named output path references, without existence checks during conversion |
| `provenance` | Available recorded identities; never guessed from output paths |
| `details`, `issues` | Compilation/command/log context and reported failure issues |
| `event_loops` | Observed native loop count, or `None` when unavailable |

Direct Python `execute_s` measures the native `Analysis.run()` call, including
native finalization. It excludes construction and kernel setup. Session
`setup_s`, `execute_s`, `total_s` retain their existing session boundaries.
Compiled/build `worker_wall_s` measures the controller's child launch, wait and
completion checks; it excludes `prepare()` artifact hashing and is not a
kernel execution time or whole steering-script latency. These durations must
not be ranked as interchangeable event-loop measurements.

This envelope is not a scheduler, progress callback or framework completion
marker. Existing LAW, provenance, output validation, retry and merge contracts
remain authoritative. Optional compiled-controller progress and status have
their own observational scope. Production adapters and persisted summary
adapters remain later UX-07 work.

For direct CLI consumers, `rdfworkflow build/run --result-format run-result`
selects this common envelope on stdout. Default stdout and recorded `run.json`
remain `rdf-command-result-v1`. The CLI conversion carries the command's process/
declared-file scope, unavailable counts/loops and distinct analysis/build roles;
it never implicitly reads a native summary or changes execution/exit behavior.

After a controller disconnects, use `rdfworkflow.read_command_result(log_directory)`
or `python -m rdfworkflow result LOG_DIRECTORY` to recover its saved command
outcome as a detached common envelope. Only run.json is read; outputs and progress
are not consulted. The read-only CLI exits zero for any valid terminal record,
including recorded failures. [Exact recovery/legacy scope](RECORDED_COMMAND_RESULTS.md).
