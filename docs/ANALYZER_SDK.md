# Optional installed Analyzer SDK

The optional SDK delivers the existing Analyzer and core manager contracts as
`find_package(RDFAnalyzerCore CONFIG REQUIRED)` and `RDFAnalyzer::Core`.
It adds no Analyzer method, compiler ABI or mandatory plugin/provider interface.
The independently installable `RDFNativeAuthoring` and Python components remain
available.

The first validated SDK configuration is a Linux CPU, core-only build with the
standalone XRootD adaptor disabled. CUDA, extension factories, custom dictionaries
and XRootD-enabled SDK packaging reject clearly at configuration; their existing
framework build paths remain available. CMS SDK delivery is separate work.
`RDF_BUILD_ANALYZER_SDK` selects optional installation packaging, not a performance
knob. The SDK bundles the selected CPU ONNX runtime; it requires no GPU provider.

Use a private build and prefix with the repository's matching compiler/runtime
environment and an existing ONNX installation to avoid a producer download:

```bash
cmake -S . -B /tmp/rdf-sdk-build \
  -DRDF_BUILD_ANALYZER_SDK=ON -DRDF_BUILD_EXTENSIONS=OFF \
  -DRDF_ENABLE_XRD_ADAPTOR=OFF -DUSE_CUDA=OFF \
  -DONNXRUNTIME_USE_CUDA=OFF -DBUILD_TESTS=OFF \
  -DONNXRUNTIME_ROOT_DIR=/path/to/onnxruntime
cmake --build /tmp/rdf-sdk-build --target analyzerSDKArtifacts --parallel 1
cmake --install /tmp/rdf-sdk-build --prefix /tmp/rdf-sdk-prefix --component AnalyzerSDK
```

An external C++17 consumer uses:

```cmake
find_package(RDFAnalyzerCore CONFIG REQUIRED)
add_executable(my-analysis analysis.cc)
target_link_libraries(my-analysis PRIVATE RDFAnalyzer::Core)
```

Pass the prefix through `CMAKE_PREFIX_PATH`. The target supplies the core/plugin
static rescan group, required include trees, bundled matching YAML and ONNX
libraries, ROOT components, Boost, zlib, threads and platform dl. ROOT and Boost
must match the producer package versions, and the compiler must match the
recorded producer compiler identity. This initial ABI policy is deliberately
bounded; compatibility with a different compiler/version is not certified.
External ROOT runtime dependencies must be available in the consumer environment.
Finding the SDK performs no download or compilation. Missing archives reject at
configuration with an incomplete-installation diagnostic.

The optional `RDFAnalyzer::NativeAuthoring` target can also be installed using
component `NativeAuthoring`; it remains a separate header-only package. Python
installation uses the existing `Python` component and requires building the
matching `rdfanalyzer` target first.

## Run the installed example

Build the consumer under `share/rdfanalyzer/examples/analyzer-sdk` in a separate build directory. Its [instructions](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/analyzer-sdk/README.md) cover direct C++, Python steering and Python-authored event processing. The CPU SDK bundles ONNX Runtime. Supply the matching ROOT, Boost and compiler runtime when relocating the prefix. [Acceptance checks](AUTHORING_ACCEPTANCE.md).
