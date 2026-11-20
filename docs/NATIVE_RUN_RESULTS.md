# Optional C++ run results

`RunResult.h` provides C++17 owned result values in namespace `rdfauthoring`.
It uses the standard library and the existing `rdf-run-result-v1` schema consumed
by Python's `rdfresults.RunResult.from_dict`. Include it explicitly when an
analysis wants to export already evaluated summaries.

```cpp
#include <RunResult.h>
#include <cstdint>
#include <iostream>

rdfauthoring::RunResult result(
    "compiled-cpp", rdfauthoring::Status::Succeeded,
    "caller-supplied evaluated summaries; persistence not verified");
result.counts["input"] = std::uint64_t{1000};
result.counts["selected"] = 42;
result.sums["weight"] = -2.5;
result.histograms["observable"] = rdfauthoring::Json::Object{
    {"contents", rdfauthoring::Json::Array{2.0, -2.0, 0.0}},
    {"sumw2", rdfauthoring::Json::Array{4.0, 4.0, 2.0}}};
result.timings["execute_s"] = 12.5;
result.outputs["metadata"] = "analysis.metadata.root";
result.event_loops = 1;  // explicitly supplied by the analysis
std::cout << result.to_json() << '\n';
```

Read completed counts, sums and histogram cells in the analysis before copying
those primitive values into the result. Native handles, including classes with
implicit conversion operators, are rejected at compile time. Serialization does
not call `GetValue`, execute an event loop, open output files or certify physics.
Output names are references. The caller chooses when and where to write JSON.
This adds no Analyzer/provider/plugin methods or automatic recording.

## Install and consume

The optional `NativeAuthoring` package installs the header beside the existing
settings and validation helpers. A standalone installation needs no compiler or
ROOT discovery:

```bash
cmake -S core/native_authoring -B /tmp/rdf-native-install
cmake --install /tmp/rdf-native-install --prefix /absolute/prefix \
  --component NativeAuthoring
```

A consumer enables C++ and links the existing interface target:

```cmake
find_package(RDFNativeAuthoring CONFIG REQUIRED)
add_executable(my-analysis main.cc)
target_link_libraries(my-analysis PRIVATE RDFAnalyzer::NativeAuthoring)
```

Set `CMAKE_PREFIX_PATH` to the installation prefix. The interface target selects
C++17 and the installed include directory; it adds no runtime library. An
existing Analyzer-based executable retains its existing core/plugin link targets
and may additionally use this header for its evaluated summaries.

The [linked example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/native-results/README.md) separates its compiled
analysis library from its CLI and Python steering. Its ROOT-generated fixture
is a result-helper demonstration, not an Analyzer or throughput benchmark.

## Values, validation and compatibility

`Status` has `Succeeded`, `Failed`, `TimedOut` and `Cancelled`. `RunResult` owns
mutable value snapshots for counts, histograms, sums, timings, output references,
provenance, details and issues. Its completion scope must be explicit. Missing
summaries are empty mappings; unavailable event-loop counts serialize as `null`.

`Json` owns null, bool, signed/unsigned64, double, UTF-8 strings, arrays and objects.
It does not accept arbitrary native-object conversions or raw JSON injection.
Numbers must be finite. Counts must be nonnegative integers; timings must be
nonnegative numeric seconds; output references must be nonempty. Serialization
validates the current mutable state, escapes control characters and ignores
process/stream locales and numeric formatting flags.

`EventLoopCount` accepts exact non-bool integral sources within the nonnegative
signed64 range, or `std::nullopt`. Bool/floating/native-handle sources are rejected
at compile time; negative or overflowing values throw `std::invalid_argument`.
Use `result.event_loops = std::nullopt` to restore unknown. Nothing infers a loop
count or promotes child-process success into analysis completion.

Python results are immutable snapshots; these C++ value fields are mutable.
C++ integer and Unicode limits remain narrower: JSON integers are signed/unsigned64,
loop counts are signed64, and all keys/text require valid UTF-8. Python integers
are unrestricted and isolated Python surrogates are outside this C++ contract.
Floating values use double and retain floating JSON type and negative zero.
The implementation needs floating `std::to_chars` support from the selected
C++ standard library. Separately compiled libraries exchanging these types need
compatible C++ standard-library ABI.

## Validation status

Permanent tests are in `core/tests/native_authoring/`; root and standalone CTest
register the native fixture and Python receiver tests. The lightweight native
CI job also builds these contracts without ROOT and tests installed relocation.
A configured workflow is not evidence of a hosted run.
