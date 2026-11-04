# Source extensions

RDFAnalyzerCore discovers optional repositories under `extensions/*` using a
standard `rdf-extension.json` manifest. Additional checkouts can be supplied as
a semicolon-separated `RDF_EXTENSION_PATHS` CMake argument. Explicit paths
override automatically discovered checkouts with the same extension ID.

The framework owns only generic controls:

- `RDF_BUILD_EXTENSIONS=OFF` disables discovery and all extension contributions.
- `RDF_EXTENSION_PATHS=/path/one;/path/two` selects extra source checkouts.
- `RDF_REQUIRED_EXTENSIONS=one;two` requires those IDs to be enabled, producing
  an early error if their checkout is missing or their own option disables them.

Each extension defines its own enable flag in its configure hook. The framework
does not define `RDF_ENABLE_CMS` or any other extension-specific option. If an
extension is absent, its hook is not loaded and its options are not introduced.
Use `RDF_REQUIRED_EXTENSIONS` when a missing checkout must be an error.

## Minimal extension

For a repository checked out at `extensions/example`:

```json
{
  "name": "example",
  "core_api_version": 1,
  "configure": "cmake/Configure.cmake",
  "enable_variable": "EXAMPLE_ENABLED"
}
```

The configure hook owns the option and registers the build contribution:

```cmake
option(EXAMPLE_ENABLED "Build the example extension" ON)
rdf_register_extension(
    NAME example ENABLED "${EXAMPLE_ENABLED}" TARGET ExampleExtension
    TEST_DIRECTORY tests)
```

The extension's `CMakeLists.txt` defines the registered target:

```cmake
add_library(ExampleExtension STATIC src/Example.cc)
target_link_libraries(ExampleExtension PUBLIC core corePlugins)
target_include_directories(ExampleExtension PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
```

Configure hooks run before framework dependency discovery, so they can supply
optional toolchain hints. Build directories are added after generic `core` and
`corePlugins` targets exist. Targets are aggregated into `coreAll` inside its
existing linker rescan group. Do not link an extension back through `coreAll`.
Configure hooks, build directories and test directories receive their own
`RDF_EXTENSION_NAME` and `RDF_EXTENSION_SOURCE_DIR` context.

The registered test directory is added after framework tests and dependencies
are configured. An extension can omit tests or provide an aggregate INTERFACE
target backed by multiple component libraries. Multiple extensions can be
enabled together without adding cases to the framework's CMake files.

## Optional contributions

`rdf_register_extension` also accepts:

- `PYTHON_PLUGIN_HEADER` and `PYTHON_PLUGIN_FACTORY`: a public header and a C++
  function taking `Analyzer&`, a role string, and a normalized type string.
  Return false for types the extension does not own. The framework generates
  the dispatch calls and compiles them into the existing native Python module.
- `PYTHON_CAPABILITY`: an optional module attribute such as `has_example`.
- `TEST_ENVIRONMENT`: environment assignments appended to framework Python
  tests, for example the extension's enable option and selected source path.
- `XRD_OPEN_POLICY_HEADER`: an optional header implementing the transport's
  initial-URL policy. Conflicting enabled providers are rejected.

The native module exposes `enabled_extensions` and `has_extension("example")`.
The `has_<id>` attribute convention also reports false for absent extensions.
Provenance is generated from the same registry: `extensions.enabled` lists
enabled IDs, and discovered extensions contribute enabled state and, when
enabled, their revision and dirty state.

The manifest can additionally declare `python_package`, an `environment`
script path, and a `python_services` mapping. `env.sh` discovers declared
scripts generically. Python service discovery uses the same manifests and
extension-owned enable-variable names. Configuration schemas can have multiple
providers; the transport `io_policy` service permits a single enabled provider.
Errors inside a provider are not silently ignored.

LAW snapshots the marked extension-environment block in the framework
`env.sh` before embedding it in local or Condor setup scripts. Enabled hook
contents travel with the setup, so relocated scripts do not invoke the
repository's discovery helper on workers. Presets used this way should be
self-contained shell code, with external dependencies addressed through
worker-visible paths such as CVMFS. Prepared execution identities and preflight
markers include the rendered setup, including the selected hook contents.

For source-based Python and shell workflows, export `RDF_BUILD_EXTENSIONS`,
`RDF_EXTENSION_PATHS` (using the platform path separator), or the extension's
own enable variable as appropriate. CMake cache arguments select the C++ build;
environment variables select source-based Python/shell behavior. Restart
Python after changing providers because normal import caching applies.

## Analysis dependencies

Analyses declare required extension IDs in their own `rdf-dependencies.cmake`:

```cmake
set(RDF_ANALYSIS_REQUIRES example)
```

Automatic discovery skips unavailable analyses. Explicit selection with
`RDF_ANALYSES` fails with a missing-extension error. Analysis libraries and
executables continue to link `coreAll`.

The contract tests build two independent non-CMS extensions, exercise their
flags, libraries, Python factories, metadata and test directories, and check
empty discovery, missing requirements, incompatible APIs, and external paths.
