# Standalone native authoring contracts

These C++17 executables have no ROOT, framework-library, event-loop or gtest
dependency. The directory can be configured independently, and its target
definitions are also included by the main framework test build.

`testRunResult` includes the public `RunResult.h`. Its compile-time assertions
reject eight implicitly convertible native-handle families for both `Json` and
`EventLoopCount` construction/assignment, and reject boolean/floating event-loop
sources. Its standalone `self-test` exercises nine valid modes and nineteen
invalid-value fixtures. The same fixture executable supplies eight complete
envelopes to the existing Python `rdfresults.RunResult` receiver, plus a separate
formatted-stream JSON case with comma/grouping locale, integer formatting flags,
width and fill. Full expected caller values, integer/double identity, negative
zero, escaped controls, UTF-8, copied ownership and unknown/reset event-loop
counts are checked by the Python receiver test.

Both CTest entry points are registered in the standalone and main builds:

- `NativeRunResultTest`: standalone C++ runtime contract and compile-time guards.
- `NativeRunResultInteropTest`: built fixture selected through
  `RDF_RUN_RESULT_FIXTURE`, consumed by the unchanged Python receiver.

The dedicated `native-result-contracts` GitHub Actions job builds this directory
and runs CTest on Python3.12, then checks installation and relocation. The full
framework CI also runs the registered tests through its ordinary CTest step.
Source inspection or replay of historical JSON does not prove that the current
native target compiles or runs.

After any active timing campaign has released the host, the focused native
validation can run in a fresh private build directory. Use an environment with
a C++17 compiler, CMake and Python3.10 or newer:

```bash
RDF_RESULT_CHECK_BUILD="$(mktemp -d /tmp/rdf-native-contracts.XXXXXX)"
RDF_RESULT_CHECK_PYTHON="$(command -v python3)"
"$RDF_RESULT_CHECK_PYTHON" -c 'import sys; assert sys.version_info >= (3, 10)'
nice -n 19 taskset -c 83 cmake -S core/tests/native_authoring \
  -B "$RDF_RESULT_CHECK_BUILD" -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE="$RDF_RESULT_CHECK_PYTHON"
nice -n 19 taskset -c 83 cmake --build "$RDF_RESULT_CHECK_BUILD" \
  --target testRunResult --parallel 1
nice -n 19 taskset -c 83 ctest --test-dir "$RDF_RESULT_CHECK_BUILD" \
  -R '^NativeRunResult(Test|InteropTest)$' --output-on-failure
```

Building all targets instead and omitting the CTest filter also runs the
existing common-settings, native-preflight and validation-report contracts.
CTest supplies the actual fixture target path, including build paths with
spaces. The Python interoperability test never compiles the fixture itself or
opens output path references stored inside the result.
