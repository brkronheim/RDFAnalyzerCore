---
name: rdfanalyzer-build
description: Configure, build, test, or diagnose RDFAnalyzerCore CMake targets, build wrappers, CI/container environments, optional dependencies, installation, and target-specific performance checks.
---

# Builds, tests, and runtime environments

Read [wrappers and dependencies](references/wrappers-dependencies.md) for build/test/cleanup scripts, presets, optional external builds and installation. Use [CI and containers](references/ci-containers.md) for GitHub workflows and runner lifecycle; source changes do not themselves establish a successful CI run.

The independent [native helper installer](../../../core/native_authoring/CMakeLists.txt)
uses `LANGUAGES NONE` and installs only the `NativeAuthoring` headers/CMake target,
without compiler or ROOT discovery. The root build shares
[InstallNativeAuthoring.cmake](../../../cmake/InstallNativeAuthoring.cmake).
External consumers use `find_package(RDFNativeAuthoring CONFIG REQUIRED)` and
`RDFAnalyzer::NativeAuthoring`; this is not the complete Analyzer/plugin SDK.
It includes the optional [RunResult.h](../../../core/interface/RunResult.h)
value serializer. Standalone result contracts/receiver tests live beside the
settings checks; the [linked result example](../../../examples/native-results/README.md)
separately requires ROOT and uses the installed helper target. Source changes
and language-free installation alone do not establish compiled/runtime acceptance.
The [native preflight example](../../../examples/native-preflight/README.md)
explicitly requires ROOT, and can be built independently of the shared framework
build. Focused contracts live in `core/tests/native_authoring/` and
`core/tests/python/test_native_authoring_install.py`.

The separately selected [Analyzer SDK installer](../../../cmake/InstallAnalyzerSDK.cmake)
provides `RDFAnalyzerCore`, `RDFAnalyzer::Core` and component `AnalyzerSDK` for
its validated core-only CPU configuration. It preserves the existing Analyzer
contract and static archive rescan semantics; it adds no provider/compiler ABI.
[SDK usage and acceptance limits](../../../docs/ANALYZER_SDK.md) distinguish
private installed scalar onboarding from broader HT10/IMT/deployment acceptance.
The [three-route example](../../../examples/analyzer-sdk/README.md) uses genuine
Analyzer-linked C++, steering of that executable and Python-authored kernels.

## Select the actual target and environment

For native ROOT registry compaction and its ABI/version guards, use
[ROOT registry compatibility](references/root-registry.md).

Inspect existing `build*/CMakeCache.txt`, [root CMakeLists](../../../CMakeLists.txt), [core/CMakeLists](../../../core/CMakeLists.txt), [src/CMakeLists](../../../core/src/CMakeLists.txt), and [env.sh](../../../env.sh). Reuse the appropriate configured build where practical. Check the ROOT/compiler/Python/XRootD paths before diagnosing ABI, import, or linker failures.

`BUILD_TESTS`, `BUILD_COMBINE`, `BUILD_COMBINE_HARVESTER`, `USE_CUDA`, and `RDF_ENABLE_XRD_ADAPTOR` are separate build choices. Read [cmake](../../../cmake) setup modules and [core/extern/CMakeLists.txt](../../../core/extern/CMakeLists.txt) to learn whether configuring fetches dependencies. Avoid editing vendored dependencies for a local runtime mismatch.

`coreAll` links the core and plugin archives in a rescan group. New plugin OBJECT files must reach the plugin archive and public includes/link dependencies; inspect [plugins/CMakeLists](../../../core/plugins/CMakeLists.txt). [analyses/CMakeLists](../../../analyses/CMakeLists.txt) includes available analysis directories with CMake files; unrelated external analysis dependencies can therefore affect configuration. Do not remove another analysis to hide that dependency failure.

## Bound verification to the change

Discover registered checks before choosing a filter:

```bash
ctest --test-dir build -N
cmake --build build --target <affected-target> -j<available-cpus>
ctest --test-dir build -R '<registered-test-pattern>' --output-on-failure
```

Replace placeholders with targets and available resources. Read [test CMake](../../../core/tests/cpp/CMakeLists.txt) and [framework test registration](../../../core/tests/CMakeLists.txt) for environment and names. Python unit suites are under `core/tests/python` and `core/tests/law`; select files relevant to the change and use the matching interpreter/virtualenv. Generated-config or plugin wiring changes need an execution that actually consumes that config, not just compilation.

For performance work, choose a representative bounded workload and record baseline/result timing plus numerical agreement. [PERFORMANCE_TUNING](../../../docs/PERFORMANCE_TUNING.md), [PERFORMANCE_MONITORING](../../../docs/PERFORMANCE_MONITORING.md), and [MEMORY_MODEL](../../../docs/MEMORY_MODEL.md) help locate existing instrumentation. Distinguish configure/build, synthetic tests, event runs, and staged worker validation in the result.

The IRIS-HEP AGC comparison harness is maintained in a separate analysis repository.

For the skill tree itself, run the [skill validator](../rdfanalyzer-development/scripts/validate_skill_tree.py); it checks metadata, local links, routing reachability, and plugin coverage without building or starting jobs.

CMS support is an optional separate source checkout. See [CMS extension integration](../../../docs/CMS_EXTENSION.md) for ownership, AUTO/ON/OFF builds, public targets, Python providers, and worker packaging. Combine integration remains in core.

Optional source repositories use the [generic extension registry](../../../docs/EXTENSIONS.md). Enable flags belong to extension configure hooks; core discovers `rdf-extension.json` and aggregates registered targets, tests, factories, and provenance. Use `RDF_REQUIRED_EXTENSIONS` for missing-checkout errors and `RDF_BUILD_EXTENSIONS=OFF` to disable all extensions. Analysis requirements belong in each analysis's `rdf-dependencies.cmake`.
