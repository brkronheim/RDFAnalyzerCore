# Python steering of a compiled analysis

This example uses the installed `rdfworkflow` controller and optional explicit
CMake build. Physics and graph construction belong in your compiled C++ analysis
and its linked libraries. The Python parent imports no event runtime. It uses
the same executable and literal argv as a direct native launch.

Configure/install the standalone controller from the checkout (no compiler or
ROOT discovery; no build command):

```bash
cmake -S core/python/controller -B /tmp/rdf-controller-build
cmake --install /tmp/rdf-controller-build --prefix /tmp/rdf-controller \
  --component PythonController
export PYTHONPATH="/tmp/rdf-controller/python${PYTHONPATH:+:$PYTHONPATH}"
```

Write an analysis-owned JSON specification. For a native CLI accepting a config
and summary filename, it can look like this (replace the absolute paths with
your existing files and working directory):

```json
{
  "executable": "/path/to/build/analysis",
  "working_directory": "/path/to/work",
  "arguments": ["{asset:config}", "{output:summary}"],
  "assets": {"config": "analysis.txt"},
  "outputs": {"summary": "summary.json"},
  "timeout_s": 600
}
```

Inspect it without executing the analysis or opening its assets:

```bash
python /tmp/rdf-controller/share/rdfanalyzer/examples/compiled-controller/steer.py \
  /path/to/spec.json
```

Add `--mode validate` to check declared files/permissions. Add `--mode run --logs
/path/to/new-logs` to execute. This uses fresh output/log paths and prints the
common result envelope. It does not assume a summary format or read output
contents; successful process completion is separate from physics validation.

Optionally add `--progress-interval 5` with `--mode run` to write small atomic
controller snapshots every five seconds while waiting and at phase changes.
It also applies to an explicitly requested build. Reporting is disabled by
default and runs outside the compiled event loop. Read the last observation
from another terminal:

```bash
python -S -m rdfworkflow status /path/to/new-logs/run
```

Status prefers the final `run.json`; before that it returns a potentially stale
controller observation with `terminal=false`. It does not probe PID liveness,
infer event progress or validate physics output. CLI status exits zero when the
read succeeds, even if the recorded command failed. A missing record can mean
preparation is still underway or reporting is disabled. Snapshot write errors
are diagnostics and do not change the child result.

An explicit `--build --mode run` requires `build_spec` in the JSON. For example:

```json
"build_spec": {
  "build_directory": "/path/to/configured/native-build",
  "target": "analysis",
  "parallel": 2
}
```

Add that member to the specification object, not a separate JSON document.
CMake must already be configured in the native environment. Builds are explicit
and do not run for `inspect`, `validate` or ordinary prebuilt execution. Optional
declared inputs/products and `build_manifest` bind recorded build products.

For explicit local CPU affinity, add another member to the same specification
(replace these example IDs with CPUs available to your controller):

```json
"resources": {"cpus": [0, 1]}
```

Inspection remains metadata-only. Preparing or running this opt-in policy checks
Linux CPU availability and resolves/hashes `taskset`, then pins the owned child.
The parent keeps its affinity. Set your analysis's native thread count through
its own configuration; CPU affinity does not choose ROOT threads, bind NUMA
memory, reserve cores or request scheduler resources. Omit `resources` to retain
the original direct command. The requested policy and tool identity are recorded
in `run.json`. The repository's `docs/COMPILED_CPP_WORKFLOWS.md` describes the
resource contract in detail.

Check the requested affinity and `taskset` availability before preparing files or
building the analysis:

```bash
python /tmp/rdf-controller/share/rdfanalyzer/examples/compiled-controller/steer.py \
  /path/to/spec.json --mode validate --validation-level local-resources
```

This explicit level checks the local resource policy only; it opens no analysis
asset and launches no child. It is available when `resources` is declared. The
default `--mode validate` still checks assets. Unsupported levels return a report
listing the levels advertised by the selected specification.

Relative executable/build paths resolve from the spec file; relative asset and
output paths resolve from the selected child working directory. Input paths
embedded inside analysis configs remain analysis-owned. Installed controller
paths can move together; this does not rebase paths in a saved specification or
manifest. The native child still needs its matching ROOT/compiler/runtime.

The adjacent `rdfworkflow.pyi` and `rdfresults.pyi` files provide IDE signatures.
Add the installation's `python` directory to your IDE's Python search paths.
No runtime dispatch or type-checker dependency is introduced.
