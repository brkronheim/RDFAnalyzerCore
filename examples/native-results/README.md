# Linked C++ analysis with a detached run result

This is a standalone C++17/header + ROOT example. The analysis graph and physics
are compiled into `native_results_analysis`, a static library linked into
`rdf-native-results`. Python can launch that executable and receive its detached
`rdf-run-result-v1` document through the installed stdlib `rdfresults.RunResult`.
No Python callback runs in the event loop.

It demonstrates the installed `RDFAnalyzer::NativeAuthoring` result header with
ROOT directly. It does not run `Analyzer`, its plugins or the ND histogram
manager, and is not a framework performance benchmark. The same result header
can hold already-evaluated Analyzer summaries; callers must explicitly extract
and supply those values after execution. Result construction does not extract
summaries or certify persisted outputs automatically.

`analysis.cc` generates eight in-memory events using typed C++ lambdas. It
interleaves definitions and filters, books two counts, three sums and a weighted
histogram before the sole explicit trigger, then reads the completed actions.
The result records `GetNRuns()` supplied by the caller. A compile-time assertion
rejects constructing its JSON values from lazy ROOT action handles.

The signed dyadic fixture has eight inputs and five selected events. Its
histogram includes underflow and overflow; exported `sumw2` comes directly from
ROOT's variance storage, including those flows:

| Quantity | Expected value |
|---|---|
| Input weight sum | 1.75 |
| Selected weight sum | 1 |
| Selected observable sum | 10.5 |
| Edges | `[0, 1, 2, 3]` |
| Sumw, underflow through overflow | `[2, -1, 0.5, 0, -0.5]` |
| Sumw2, same order | `[4, 1, 0.25, 0, 6.25]` |

## Future standalone build and installation

These commands are queued instructions; native compilation/execution is held
while the current HT timing campaign runs. Use an existing ABI-compatible ROOT
environment with CMake package targets `ROOT::ROOTDataFrame` and `ROOT::RIO`.
The external ROOT installation/libraries must remain available after relocation.
This example requires no shared framework configure or framework build.

From the checkout, choose fresh private directories:

```bash
RDF_RESULT_CHECKOUT="$PWD"
cmake -S core/native_authoring -B /tmp/native-results-headers
cmake --install /tmp/native-results-headers --prefix /tmp/native-results-prefix \
  --component NativeAuthoring
cmake -S core/python/controller -B /tmp/native-results-receiver
cmake --install /tmp/native-results-receiver --prefix /tmp/native-results-prefix \
  --component PythonController

# Copy only this example to a fresh private source tree for the consumer build.
mkdir /tmp/native-results-source
cp -R examples/native-results /tmp/native-results-source/example
cmake -S /tmp/native-results-source/example -B /tmp/native-results-build \
  -DCMAKE_PREFIX_PATH=/tmp/native-results-prefix
cmake --build /tmp/native-results-build --target rdf-native-results --parallel 1
cmake --install /tmp/native-results-build --prefix /tmp/native-results-prefix
```

The first two configurations use `LANGUAGES NONE`: they install headers and
stdlib Python without compiler/ROOT discovery. The separate example consumer
uses `find_package(RDFNativeAuthoring CONFIG REQUIRED)` and ROOT. It never adds a
checkout `core/interface` include directory or calls `add_subdirectory` on the
framework. Installation includes the linked executable, static analysis archive,
steering script and finite validator.

## Inspection and explicit execution

```bash
/tmp/native-results-prefix/bin/rdf-native-results --describe
/tmp/native-results-prefix/bin/rdf-native-results --preflight
/tmp/native-results-prefix/bin/rdf-native-results --run
```

The native default is `--describe`. Both inspection modes construct and book
the generated lazy graph and report zero ROOT event loops. They can initialize
ROOT and allocate graph/actions; they do not evaluate events. `--preflight`
currently has the same limited description scope: it does not validate input
assets, a schema from disk or physics results.

Python default inspection launches no executable, imports no ROOT and opens no
result/receiver files:

```bash
python -S /tmp/native-results-prefix/share/rdfanalyzer/examples/native-results/steer.py
python -S /tmp/native-results-prefix/share/rdfanalyzer/examples/native-results/steer.py \
  --executable /tmp/native-results-prefix/bin/rdf-native-results \
  --receiver-dir /tmp/native-results-prefix/python --run
```

Only explicit `--run` invokes the same native `--run` command, with a 30-second
finite timeout, and validates the complete supplied envelope through
`RunResult.from_dict`. Optional `--output /absolute/new.json` writes a fresh JSON
file explicitly. The Python receiver reads supplied JSON values; it does not
open references, materialize ROOT objects or interpret process success as a
physics validation. The separate integration validator supplies that check.

## Installed execution without the private source/build

Before hiding private build inputs, record their paths, exact configure/build/
install commands and source/product hashes in the new validation workspace.
Then relocate the prefix and rename **only the fresh private copies above**:

```bash
mv /tmp/native-results-prefix /tmp/native-results-relocated
mv /tmp/native-results-source /tmp/native-results-source.hidden
mv /tmp/native-results-build /tmp/native-results-build.hidden
env -u PYTHONPATH python -S \
  /tmp/native-results-relocated/share/rdfanalyzer/examples/native-results/tests/validate.py \
  --executable /tmp/native-results-relocated/bin/rdf-native-results \
  --receiver-dir /tmp/native-results-relocated/python \
  --output /tmp/native-results-sourcefree-validation \
  --forbid-source-root "$RDF_RESULT_CHECKOUT" --run
```

Do not rename or remove the real checkout, shared build or existing artifacts.
The selected ROOT runtime remains required. This validates installed selected
paths, a fresh empty child working directory and explicit installed Python
imports under `-S` without checkout `PYTHONPATH`. Renaming a copy and checking
selected paths is not complete filesystem-open attestation. A separately
recorded successful-open trace may strengthen that claim when available.

Without `--run`, `tests/validate.py` prints metadata only. With it, the validator
executes two zero-loop inspection modes and two eight-event runs: direct and
Python-steered. It independently checks all counts, sums, histogram contents,
flows, variances and loop metadata, then compares the entire detached documents
excluding only variable `timings`. It saves `direct.json`, `steered.json`,
`inspections.json` and `ACCEPTANCE.json` in a fresh directory. Acceptance is finite
correctness, with `accepted_for_performance=false`.

Source/mock tests need no native build or ROOT import:

```bash
python -B examples/native-results/tests/test_example.py
```

Those tests mock every subprocess; they do not establish compile/runtime or
installed-example acceptance. Actual standalone compile, relocated-prefix runs
and the finite validator remain queued behind host timing work.
