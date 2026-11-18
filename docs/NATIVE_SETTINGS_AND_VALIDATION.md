# Native C++ settings and validation reports

`CommonSettings.h` and `ValidationReport.h` provide optional, header-only C++17
setup helpers. They require only the standard library. Existing Analyzer,
configuration-provider and plugin interfaces stay unchanged. No code is added
to an existing event graph, event loop or default initialization path.

The native-authoring helpers cover common input/skim settings, generated help,
configuration diagnostics, explicit local asset checks and comparison of supplied
input-schema metadata. Graph/compile validation, payload-content validation,
automatic Analyzer integration and production adapters remain future work.
The same optional
package supplies [RunResult.h](NATIVE_RUN_RESULTS.md) for explicitly serialized,
already evaluated analysis summaries.

Python orchestration can also explicitly check each input file with
[input schema preflight](INPUT_SCHEMA_PREFLIGHT.md), independently of the compiled
executable. The native helper itself remains supplied-metadata-only.

## Validate and lower common settings

```cpp
#include <CommonSettings.h>
#include <iostream>

int main() {
  rdf::config::CommonSettings settings;
  settings.input.cacheEnabled = true;
  settings.input.cacheSizeMB = 32;
  settings.input.cacheStatistics = false;
  settings.input.floatConstants = "config/cuts.txt";
  settings.skim.tree = "Selected";
  settings.skim.compressionAlgorithm = rdf::config::CompressionAlgorithm::Zstd;
  settings.skim.compressionLevel = 7;

  rdf::config::NativeOptions advanced{
      {"threads", "1"}, {"my_extension.recipe", "analysis-owned grammar"}};
  auto report = settings.validate(advanced);
  std::cout << report.toJson() << '\n';
  report.throwIfInvalid();
  auto options = settings.nativeOptions(advanced);
  // Supply options to the existing native configuration path before creating
  // DataManager/Analyzer. Each key/value is an ordinary native option.
  return options.at("treeCacheSizeMB") == "32" ? 0 : 1;
}
```

An existing `IConfigurationProvider` can receive the returned entries through
its existing `set(key, value)` method **before** dataframe/managers are created.
Alternatively an analysis-owned writer can include the entries in its normal
native config. These helpers do not mutate a provider or rewrite a file.
Calling `validate()` and then `nativeOptions()` explicitly repeats preparation;
ordinary code can call `nativeOptions()` alone, which validates before returning.

Every field is optional. Unset fields emit no key; explicit `false` and zero
remain explicit values. The returned `NativeOptions` is the same string-map
type used by native configuration providers. It preserves unknown advanced
keys and values. A typed field cannot also appear in advanced options, even
with an equal value. A typed fixed cache size also conflicts with the two
legacy cache-size factor options. Failures leave both settings and advanced
options untouched.

The input fields cover cache enable/size/statistics/verbosity and the existing
floating/integer constants, aliases, optional branches and friend config paths.
The skim fields cover tree/metadata destination, systematic output columns and
compression algorithm/level. They match the corresponding bounded
[Python settings](TYPED_SETTINGS_AND_STAGES.md) native-key mappings and limits.
Other options, including async I/O controls, remain available through advanced
options. No I/O default is promoted by this helper.

Paths are retained verbatim so the existing native provider owns relative-path
resolution. This differs from Python constructor/YAML path anchoring, which is
needed when that frontend creates a private native config elsewhere. Supplied
typed local paths reject URL syntax, empty text, NUL and newlines. No file is
opened or checked for existence. Advanced paths/grammars stay native-owned.
Skim compression settings apply to skim output, not histogram compression.

## Generate help from the native settings definitions

```cpp
#include <CommonSettingsSchema.h>

std::cout << rdf::config::commonSettingsHelp();
std::cout << rdf::config::commonSettingsDescriptionJson() << '\n';
```

An analysis may explicitly call these from its own help/description commands.
The framework does not intercept existing executable arguments. The description
uses `rdf-workflow-description-v1`, backend `settings-provider`, and advertises
configuration validation only. It describes all 14 common fields, native keys,
types, nullable/unset policy, integer limits, compression choices and path/asset
roles. Unknown advanced options remain available and native paths stay verbatim.

The native lowering/validator and help consume the same keys, bounds and
compression-choice definitions. Tests compare these with Python settings
schemas; this is not a description of every core/CMS/plugin setting. No model,
input or configuration file is opened by either help function.

## Explicit local assets and input schema preflight

```cpp
#include <NativePreflight.h>

auto assets = rdf::validation::checkAssets({
    {"config/cuts.txt", "input_settings.float_constants",
     rdf::validation::AssetKind::File, {}},
    {"models/classifier.txt", "models.classifier",
     rdf::validation::AssetKind::File, 12345}}, "/analysis/base");
std::cout << assets.toJson() << '\n';
assets.throwIfInvalid();

// Populate this metadata from the explicitly chosen native input node/source.
std::vector<rdf::validation::ColumnInfo> observed{
    {"Jet_pt", "ROOT::VecOps::RVec<float>"}, {"event", "ULong64_t"}};
auto schema = rdf::validation::checkSchema({
    {"Jet_pt", "ROOT::VecOps::RVec<float>", "inputs.jets"},
    {"event", {}, "inputs.event"}}, observed);
schema.throwIfInvalid();
```

`checkAssets()` checks only supplied local paths. Relative names resolve under
the supplied base (or the process working directory); absolute names retain
their meaning. Files must be regular and openable, optionally with the declared
byte count, including zero. Directories can be checked for existence/type.
Ordinary symlinks are followed; missing targets, malformed/remote paths, type
mismatches and size mismatches produce aggregated location-bearing diagnostics.
No directory traversal, config parsing, network access or recursive dependency
discovery occurs. A friend config check does not check its referenced trees.
This point-in-time check is not a checksum, payload-validity or staging guarantee.

`checkSchema()` compares declared required columns with caller-supplied metadata,
without ROOT dependencies, graph construction, compilation or event execution.
Missing columns, exact-type mismatches, duplicate declarations and ambiguous
available names fail explicitly. A requirement with no type checks presence
only; an unknown observed type fails a typed requirement. Types compare as
strings: the caller owns normalization of ROOT typedef spellings and per-file
schema coverage. Supplying one file's schema does not validate a heterogeneous
chain. These checks do not prove a physics kernel compiles or accepts its inputs.

The helpers return separate `rdf-validation-v1` reports advertising only the
phase actually performed (`assets` or `schema`). An existing analysis decides
when to call them; configuration validation and ordinary native launches do not
start preflight implicitly.

## Share diagnostics with Python tooling

```cpp
#include <ValidationReport.h>

rdf::validation::Report report;
report.addIssue("settings.dependency", "Requires an enabled cache policy",
                "input_settings.cache_size_mb");
auto json = report.toJson();
```

The JSON envelope is `rdf-validation-v1`, with `level`, `supported_levels`,
`passed` and `issues`. Each issue carries `code`, `message` and `location`,
matching Python's existing `ValidationReport.as_dict()`. Native settings reports
support **configuration** only. A provider using the generic report type owns
the validation phases it advertises and performs. Serialization performs no
discovery or validation by itself.

Reports are ordinary detached value objects; copying a report does not share
mutable issue state. `throwIfInvalid()` throws `std::invalid_argument` containing
the issue locations and messages. Configuration checks collect applicable range,
dependency, conflict, string/path and compression-enum errors in one report.
JSON serialization escapes control characters, quotes and backslashes.

## Install the optional native helper package

Install just these headers without detecting a compiler or ROOT:

```bash
cmake -S core/native_authoring -B /tmp/rdf-native-helper-install
cmake --install /tmp/rdf-native-helper-install --prefix /path/to/prefix \
  --component NativeAuthoring
```

The same `NativeAuthoring` component is available from a configured root build.
An external C++ project can consume the relocatable package:

```cmake
find_package(RDFNativeAuthoring CONFIG REQUIRED)
target_link_libraries(my_analysis PRIVATE RDFAnalyzer::NativeAuthoring)
```

Add the installation prefix to `CMAKE_PREFIX_PATH`. The target supplies include
paths and the C++17 requirement; it links no library or event runtime. Its four
headers are the optional helpers, not the complete Analyzer/plugin SDK. Headers
remain usable directly from the checkout. A standalone
[compiled ROOT example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/native-preflight/README.md) demonstrates
explicit input metadata checks and Python steering of a linked native library.

## Acceptance and performance boundary

The isolated test project builds without ROOT/framework libraries:

```bash
cmake -S core/tests/native_authoring -B /tmp/rdf-native-settings \
  -DCMAKE_BUILD_TYPE=Release -DPython3_EXECUTABLE=/path/to/python
cmake --build /tmp/rdf-native-settings --parallel 1
ctest --test-dir /tmp/rdf-native-settings --output-on-failure
```

The native contract tests cover omission/default preservation, explicit zero and
false, range/dependency errors, advanced-option conflicts, path handling and
compression enums. Four Python interoperability tests compare the actual native
mapping with Python input/skim settings and round-trip both valid and escaped
diagnostic reports. The tests use nonexistent paths to establish that this phase
does not perform asset discovery. They do not certify input schemas or payloads.

Validation and map construction allocate strings during explicit setup. They
introduce no per-event lookup, callback, action or copying. Existing callers
that do not include/use these headers have identical compiled execution paths.
