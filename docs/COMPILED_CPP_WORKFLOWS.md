# Compiled C++ analyses with Python steering

`rdfworkflow` provides lightweight local orchestration of analysis executables.
The analysis can use typed C++ callables, native managers and analysis-owned
libraries compiled and linked with CMake. Python prepares settings and launches
the executable; it does not execute callbacks for each event.

This module imports only the standard library and the existing `run_bundle`
helpers. It does not require importing ROOT, the `rdfanalyzer` module, NumPy or
Numba in the controller. The native executable still needs its matching ROOT,
compiler, XRootD and other enabled runtime libraries. Native plugins can retain
their existing ROOT JIT costs.

The direct C++ CLI, Python/Numba frontend, legacy `cpp_wrapper` and production
workflows remain available. The new interface currently covers local building,
description/validation and launching; it does not replace worker staging or LAW.

`CommandResult.to_run_result()` offers the additive [common result envelope](RUN_RESULTS.md),
with explicit process/build completion scope, timings and recorded artifact
identity. It does not reread native outputs or import the authoring runtime.
Optional [progress snapshots and read-only status](COMMAND_PROGRESS.md) report
controller phases outside the event loop, with reporting disabled by default.

For repeated short jobs over a fixed analysis state, collect the existing
[analysis Git snapshot settings](METADATA.md#explicit-analysis-git-snapshots)
once and put both values in the native configuration. A paired control measured
0.844 s median saved startup here by avoiding repeated live Git queries. This
does not change event processing. Refresh the snapshot after source changes and
retain artifact hashes; the controller does not infer or rewrite these settings.

## Build and install the controller

For Python steering of a prebuilt executable, install just the controller. This
standalone CMake entry point uses `LANGUAGES NONE`: no compiler, ROOT, pybind11,
experiment extension or native dependency download is required to configure it.
It does not configure the framework or build the analysis.

```bash
cmake -S core/python/controller -B /tmp/rdf-controller-build
cmake --install /tmp/rdf-controller-build --prefix /tmp/rdf-controller \
  --component PythonController
export PYTHONPATH="/tmp/rdf-controller/python${PYTHONPATH:+:$PYTHONPATH}"
python -S -m rdfworkflow --help
```

The component installs `rdfworkflow`, `rdfresults`, their adjacent IDE `.pyi`
files and the run-bundle/collection helper closure. Controller operations use
only the Python standard library; optional YAML bundle-compilation helpers
still require PyYAML when explicitly called. A full framework build configured
with these rules also offers `--component PythonController` without requiring
the native module to be built. The existing `Python` component retains the same
controller files alongside native authoring. These install paths do not ship
ROOT or the analysis executable; the child retains its native runtime needs.

The [installed steering example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/compiled-controller/README.md)
demonstrates explicit inspect/validate/run/build modes without source-path
manipulation. To enable IDE completion, include the prefix's `python` directory
in its interpreter/search-path configuration. Stubs cover controller and result
APIs. The full Python component additionally includes
[authoring/settings/stage/collection signatures](PYTHON_TYPING_AND_TUTORIAL.md);
native plugin/provider typing remains partial.

To install the native bindings alongside the controller, configure the shipped example and build both targets.

```bash
cmake -S . -B build -DRDF_ANALYSES=ExampleAnalysis
cmake --build build --target analysis rdfanalyzer -j2
cmake --install build --prefix "$PWD/local-python" --component Python
export PYTHONPATH="$PWD/local-python/python:${PYTHONPATH:-}"
```

`CMakeBuildSpec` below builds an already configured target explicitly. It does
not configure the project, fetch dependencies or compile automatically on run.
Use CMake's own incremental dependency handling; after a changed build, prepare
the new executable again. Prebuilt execution requires no compiler on the worker.
The `Python` component includes native authoring bindings, hence the additional
`rdfanalyzer` build target above. The controller does not import those bindings.
Source use through `PYTHONPATH=core/python` remains available as well.

## Describe and run an executable

```python
from rdfworkflow import CMakeBuildSpec, CppAnalysisSpec

spec = CppAnalysisSpec(
    executable="build/my-analysis",
    arguments=("{asset:config}", "{output:summary}"),
    working_directory="/absolute/existing/work",
    assets={"config": "/absolute/analysis.txt"},
    outputs={"summary": "summary.json"},
    timeout_s=600,
    build_spec=CMakeBuildSpec("build", "my-analysis", parallel=2),
)

print(spec.describe().explain())
spec.validate("configuration").raise_if_invalid()

# Optional explicit build; choose a fresh log directory for each operation.
spec.build_spec.build("/absolute/logs/build-001")

prepared = spec.prepare()
result = prepared.run("/absolute/logs/run-001")
print(result.as_dict())
```

For an immediate one-shot launch, use `spec.run(new_log_directory)`. This hashes
each declared artifact once immediately before launching. A retained
`prepared.run(...)` additionally rechecks against the earlier hashes. The CLI
uses the one-shot path, avoiding a redundant immediate verification pass.

Arguments are passed as a list, without a shell. Only complete argument tokens
`{asset:name}` and `{output:name}` are resolved; other strings remain literal.
Different executable argv conventions are supported by specifying different
tokens. Configuration syntax and physics validation belong to the analysis.

For Python construction, the executable and working directory are anchored at
construction time. Relative asset/library/output paths use the child working
directory. The controller does not change the parent's cwd or environment.

The working directory and output parent directories must already exist. Existing
declared outputs are rejected, and each log directory must be new. These checks
prevent old files from being accepted as this invocation's successful output.
The controller does not rewrite paths embedded in native analysis configs;
declare required assets and use paths appropriate for the selected child cwd.

`prepare()` hashes the executable and declared assets/libraries. `run()` verifies
those hashes again and rejects a changed file before launching. This is local
integrity checking, not a hermetic snapshot: it does not discover an arbitrary
config's transitive dependencies, freeze files or verify loaded library paths.
Use the existing [run-bundle/staging contracts](RUN_BUNDLES.md) for deployment.
The `libraries` mapping declares additional files to hash; it does not load them
or override RPATH. Explicit child environment overrides can be supplied through
`environment={...}`; their names, not values, are written into the run record.

## Optional local child CPU affinity

The default compiled workflow retains its original command, description and
artifact records. For an explicit Linux child CPU affinity request, use:

```python
from dataclasses import replace
from rdfworkflow import LocalExecutionResources

pinned = replace(spec, resources=LocalExecutionResources(cpus=(2, 3)))
print(pinned.command())  # Still the analysis executable and its arguments.
pinned.validate("local-resources").raise_if_invalid()  # Optional explicit host preflight.
prepared = pinned.prepare()
result = prepared.run("/absolute/logs/pinned-001")
print(result.command)  # Resolved taskset prefix followed by the analysis argv.
```

CPU IDs must be unique nonnegative integers; choose IDs permitted by the
controller's current Linux affinity. Construction and `describe()` validate
only the request's structure and do not probe the host. `prepare()` checks
availability, resolves the executable `taskset` tool using the child PATH, and
captures its absolute path and SHA-256. Each launch rechecks CPU availability
and the tool's resolution and bytes before creating a child. A changed tool
requires explicit preparation again. Missing tools and unavailable CPUs fail
before launch. JSON specifications can add `"resources": {"cpus": [2, 3]}`.

The explicit `validate("local-resources")` level checks Linux CPU availability
and resolves/hashes taskset without preparing or launching an analysis. It is
advertised only when resources are requested and reports failures at
`resources.cpus`. It does not validate the executable, analysis assets, build
manifest, working/output directories, or existing outputs. Configuration
validation and `describe()` remain free of host probes; preparation and launch
still perform their own current checks after a successful preflight.

The same explicit check is available without launching through the CLI:

```bash
PYTHONPATH=core/python python -m rdfworkflow validate analysis-spec.json --level local-resources
```

This reports an unsupported level when the specification has no resource request.

Only the owned child's argv receives the prefix
`taskset --cpu-list -- 2,3 <analysis> ...`; the controller's affinity, working
directory, environment and signal handling remain unchanged. Build manifests
continue to identify the analysis executable and its libraries. Opt-in launch
records additionally capture the requested CPUs and taskset identity, and
`CommandResult.command` records the actual prefixed argv. Existing cancellation,
timeout and optional progress behavior applies to the same owned process group.
The immediate `CommandResult.provenance` retains its existing artifact/build
identity scope. Resource details are saved in `run.json` under
`artifacts.local_resources`; `read_command_result()` exposes them through
`result.details["recorded_artifacts"]` without probing the host again.

This sets the child's initial affinity. It does not reserve CPUs, enforce a CPU
quota, select ROOT's thread count, bind NUMA memory, or infer batch scheduler
requests. A child may change its own affinity. Tool and declared file hashes
are local integrity observations, with the same non-hermetic limitations as
the default workflow.

## Optional recorded build identity

For a build with an explicit source/dependency inventory, supply both `inputs`
and `products`. Relative paths in these mappings use the build directory:

```python
from pathlib import Path
from rdfworkflow import CMakeBuildSpec, CppAnalysisSpec

builder = CMakeBuildSpec(
    "/project/build", "analysis", parallel=2,
    inputs={"driver": "../src/main.cc", "physics": "../src/physics.cc",
            "cmake": "../CMakeLists.txt"},
    products={"analysis": "analysis", "physics": "libphysics.so"},
)
built = builder.build("/absolute/logs/build-001")
manifest = Path(built.log_directory) / "build_manifest.json"
spec = CppAnalysisSpec(
    "/project/build/analysis", libraries={"physics": "/project/build/libphysics.so"},
    build_manifest=str(manifest),
)
spec.validate("build-inputs").raise_if_invalid()  # explicit current-source check
spec.run("/absolute/logs/run-001")
```

This is opt-in. Builds without the mappings retain the previous build behavior.
Both mappings are frozen and must be supplied together; their paths cannot
overlap. Products cannot overwrite controller logs or the build manifest.
Inputs must be regular files. Declare every relevant analysis source,
header, CMake/toolchain file, compiler or dependency lock that you want recorded.
This layer does **not** discover a compiler's complete transitive include/link
closure or prove that CMake consumed each listed file.

Before building, the controller snapshots the declared inputs, CMake cache and
CMake executable. After a successful command it rejects changed inputs/tools or
missing products, then atomically writes `build_manifest.json` using the existing
`rdf-derived-artifact-v1` contract. Incremental/no-op builds can retain existing
products. A zero process exit is insufficient when identity checks fail:
`run.json` records failure and no successful build manifest is published.
Configure/build again explicitly if a declared input or CMake cache changes during
the build. Generated inputs that change within that build need a separate
preparation step. This is an observation around a local build, not a hermetic or
atomic filesystem snapshot.

With `build_manifest=...`, preparation checks manifest integrity and requires the
selected executable and every declared library to be recorded products. It hashes
each resolved runtime file once, including products not separately listed as
libraries, and rejects a changed product before launch. Retained prepared jobs also
reject a changed manifest. The run record includes its build artifact identity.
Current-source checking is deliberately separate: `validate("build-inputs")`
checks the recorded inputs/tools, while ordinary prebuilt launch needs the manifest
and products at their recorded paths. It can run without the original sources.
Editing source does not silently rebuild or invalidate an intentionally retained
binary; request the explicit source check when current-source correspondence matters.

`validate("assets")` checks manifest structure and product membership, with hash
verification during `prepare()`/launch. Generic `run_bundle.verify_artifact()`
checks the manifest and recorded product bytes; it does not check current sources.
The manifest path is relative to the child working directory when not absolute,
including in a JSON spec. Build mappings remain relative to the build directory.

No loaded-library resolution, RPATH override, automatic manifest rebasing or
production staging is provided. A declared library hash does not prove the dynamic
loader used it. Use the existing bundle/deployment machinery for those later steps.
Do not claim complete source closure or remote-worker acceptance from this local
contract.

Build records separate input-hash preparation time (`identity_preparation_s`)
from command plus post-build verification (`wall_s` includes
`identity_finalization_s`). Native run timing retains its existing scope;
manifest parsing/hash preparation happens before launch and is recorded separately.

## JSON and command-line use

The same compiled specification can be written as JSON:

```json
{
  "executable": "/absolute/build/my-analysis",
  "arguments": ["{asset:config}", "{output:summary}"],
  "working_directory": "/absolute/existing/work",
  "assets": {"config": "/absolute/analysis.txt"},
  "outputs": {"summary": "summary.json"},
  "timeout_s": 600
}
```

```bash
python -S -m rdfworkflow explain analysis.json
python -S -m rdfworkflow validate analysis.json --level configuration
python -S -m rdfworkflow validate analysis.json --level assets
python -S -m rdfworkflow run analysis.json --directory /absolute/logs/run-002
```

`-S` is optional; it demonstrates that the controller can run without site
packages when its installed directory is on PYTHONPATH. `from_file()` rejects
unknown fields. Relative executable, working-directory and build-directory
paths in JSON are relative to the JSON file. Relative assets/outputs/libraries
remain relative to the resolved child working directory. Literal argv strings
are never guessed to be paths.

An optional `build_spec` JSON object accepts `build_directory`, `target`,
`parallel`, `configuration`, `timeout_s`, and optional `inputs`/`products` mappings.
An optional `build_manifest` field binds a run to a recorded build. Then `rdfworkflow build ...
--directory ...` performs that explicit incremental build. Run never implicitly
builds or refreshes prepared production artifacts.

## Validation, completion and cancellation

Configuration validation checks typed settings/references without opening input
files or importing a native module. Asset validation checks declared files,
execute permission, directories and output collisions. Neither level opens a
ROOT input or executes the native graph. Unsupported levels such as `graph`
return a failed structured report; opaque C++ graph dependencies are not inferred.

Each launch produces `stdout.log`, `stderr.log` and an atomically written
`run.json`. Logs are file-backed. A successful result requires zero child exit
status and every declared output file. It does not prove numerical correctness
or create a canonical framework completion marker. Native output metadata and
the existing production completion/merge checks remain authoritative.

Failures, missing outputs, timeouts and cancellation keep their logs/result.
By default they raise `ExecutionError`, whose `result` contains structured
details. Use `check=False` to inspect a failed result directly.

Library calls accept an optional `threading.Event` as `cancel_event`; timeout
and cancellation terminate the owned child process group on POSIX and reap the
direct child. They do not change parent signal handlers. The CLI additionally
handles SIGINT/SIGTERM with cleanup and a corresponding nonzero exit status.
Linux process-group behavior is tested; no cross-platform release claim follows.

`wall_s` measures launch handling, child completion and final output/build checks;
atomic result writing follows. Preparation and artifact verification durations are recorded
separately in `run.json`. Explicit CMake
builds have separate logs/results. This helper does not supply per-job CPU/RSS
measurements; the controlled AGC comparison uses the existing process-tree
measurement helper to include parent and child costs.

## Runnable steering example

Use the [compiled controller example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/compiled-controller/README.md) with your executable and its analysis-owned configuration.

### Choose the CLI result envelope explicitly

Build/run commands keep their existing `rdf-command-result-v1` stdout by default.
Request the detached common envelope when integrating with another controller:

```bash
python -m rdfworkflow run analysis.json --directory /absolute/new/logs \
  --result-format run-result
```

The same option works with `build`. This changes stdout serialization only:
`run.json`, logs, process execution, output checks and exit behavior retain their
existing contracts. The common envelope is accepted by
`rdfresults.RunResult.from_dict`. Its completion scope remains process exit and
declared files; counts/physics summaries and native loop count remain unavailable.
An explicitly acquired analysis-owned native envelope is separate; see
[run results and receiver semantics](RUN_RESULTS.md).

Offline recovery uses `python -m rdfworkflow result /existing/logs` or
`read_command_result('/existing/logs')`. It reads the recorded command result
without launching anything or reopening outputs. A successful read is distinct
from a successful recorded worker. See [recovery semantics](RECORDED_COMMAND_RESULTS.md).
