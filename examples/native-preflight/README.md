# Compiled native input preflight

This example links an analysis-owned C++ library (`inspection.cc`) into a small
native executable (`main.cc`). It uses the optional settings/help/preflight
headers; ROOT input inspection is explicit. The reusable headers themselves
require only C++17's standard library. This particular example requires ROOT.

Build in the matching native environment:

```bash
source env.sh
cmake -S examples/native-preflight -B /tmp/rdf-native-preflight -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/rdf-native-preflight --parallel 1
```

Use `--help` or `--describe-settings` to print generated settings metadata.
They do not open an input. To inspect a chosen local CMS NanoAOD file:

```bash
/tmp/rdf-native-preflight/rdf-native-preflight \
  --check-input /path/to/input.root /path/to/fresh-report.json
```

The program checks the declared local file, opens its `Events` metadata,
requires `Jet_pt` presence and an exact `ULong64_t` `event` type, and verifies
`GetNRuns()==0`. It writes `rdf-validation-v1` JSON and prints `event_loops=0`
to stderr. Validation failures exit 2; CLI/report errors exit 1. An existing
report is refused. No event action or histogram is booked. This validates one
chosen file, not every file in a dataset, correction payloads or physics logic.

## Python orchestration

The existing [compiled controller](../compiled-controller/README.md) can run the
same executable, with its analysis code already compiled and linked. For example,
save a specification with absolute paths:

```json
{
  "executable": "/tmp/rdf-native-preflight/rdf-native-preflight",
  "working_directory": "/path/to/work",
  "arguments": ["--check-input", "{asset:input}", "{output:report}"],
  "assets": {"input": "/path/to/input.root"},
  "outputs": {"report": "/path/to/fresh-report.json"},
  "timeout_s": 60
}
```

After sourcing the native environment and setting `PYTHONPATH` to the installed
controller's `python/` directory (or this checkout's `core/python/`), run:

```bash
python -S examples/compiled-controller/steer.py /path/to/spec.json \
  --mode run --logs /path/to/fresh-controller-logs
```

The parent imports no ROOT, NumPy, Numba or Coffea. It manages argv, files and
process status; native code owns schema inspection. The controller's process
result and the native validation report describe separate completion scopes.
No compiler is needed when launching an already built executable.

See the
[native helper guide](../../docs/NATIVE_SETTINGS_AND_VALIDATION.md) for installing
the independent helper package into external CMake projects.
