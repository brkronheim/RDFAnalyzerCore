# Optional CMS extension

CMS support lives in the independently versioned `extensions/cms` checkout
(RDFAnalyzerCMS). RDFAnalyzerCore configures and builds without that checkout.
Combine, CombineHarvester, datacard generation, and generic statistical workflows
remain in RDFAnalyzerCore and do not require the CMS extension.

## Download and build

The [CMS repository](https://gitlab.cern.ch/bkronhei/RDFAnalyzerCMS) is private.
Authenticate to CERN GitLab with an account that has read access.
The fetch helper clones the `main` branch configured in `extensions/cms.lock.json`.

From the core checkout on a supported CVMFS-backed machine:

```bash
python3 scripts/fetch_cms.py
source env.sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-ci.txt
source build.sh -DRDF_REQUIRED_EXTENSIONS=cms -DPython3_EXECUTABLE="$VIRTUAL_ENV/bin/python"
```

Run the helper again to fast-forward a clean `main` checkout from its `origin`.
Use `--revision <commit>` for a reproducible checkout. An existing destination
must already be at that commit when `--revision` is set. For a new checkout,
`--repository` selects a mirror or another URL such as
`ssh://git@gitlab.cern.ch:7999/bkronhei/RDFAnalyzerCMS.git`.

For direct CMake builds, run
`git submodule update --init --recursive -- core/extern` before configuring.
For an external checkout, pass its path through `RDF_EXTENSION_PATHS` when sourcing `env.sh`, and pass
`-DRDF_EXTENSION_PATHS=/path/to/RDFAnalyzerCMS` to CMake. The extension repository owns its tests.

`RDF_ENABLE_CMS` is declared by the CMS extension's `cmake/Configure.cmake`,
which the framework loads through `rdf-extension.json`. It accepts `AUTO`
(default), `ON`, and `OFF`; a discovered checkout is enabled by AUTO or ON.
It is not a framework-owned option and is not introduced when the checkout is
absent. Use `-DRDF_REQUIRED_EXTENSIONS=cms` to require CMS and fail early if its
checkout is missing or disabled. Use `RDF_EXTENSION_PATHS` for external checkouts.
An incompatible discovered extension is an error.

The generic [extension registration contract](EXTENSIONS.md) also supports
other source extensions without adding cases to the framework's build files.

For a generic build:

```bash
git submodule update --init --recursive -- core/extern
RDF_BUILD_EXTENSIONS=OFF source env.sh
cmake -S . -B build-core-only -DRDF_BUILD_EXTENSIONS=OFF
cmake --build build-core-only
ctest --test-dir build-core-only --output-on-failure
```

`build.sh` accepts the same CMake arguments, such as `./build.sh -DRDF_BUILD_EXTENSIONS=OFF`.
Its submodule initialization only downloads core dependencies.

## Analysis compatibility

Analyses continue to link `coreAll`. With CMS enabled this includes the CMS
archive, public include directories, and transitive dependencies in the static
library rescan group. `RDFAnalyzer::CMS` is also available for explicit use.
The extension itself depends on generic targets, never on `coreAll`.

Public header names, C++ types, namespaces, configuration keys, and plugin roles
are preserved. Prefer `#include <CmsCorrections.h>` over relative source paths.
Hardcoded includes into the former `core/plugins/cms` layout must be replaced
by public header includes. Hardcoded include-directory entries in an existing
analysis are unnecessary; `coreAll` supplies the new directories transitively.

An analysis can declare its dependencies in `rdf-dependencies.cmake`:

```cmake
set(RDF_ANALYSIS_REQUIRES cms)
```

Automatic discovery skips such analyses with a configure message when CMS is
unavailable. `-DRDF_ANALYSES=vjet-njetpt` explicitly selects an analysis and
fails early if CMS is absent. Dependency declarations are analysis-owned metadata. The CMS DoubleMuon,
VH-RDF, and vjet-njetpt projects each carry this small file; their CMake linking
and analysis source remain unchanged. Core contains no list of CMS analyses.

## Ownership

The extension owns CMS era/payload registries, NanoAOD compatibility, correction
workflows and scale factors, golden JSON filtering, jet veto maps, Rochester,
concrete object-correction managers and their corrected-collection wrappers,
CMS Rucio discovery. It also owns CMS federation/site policy and the CMS XRootD environment preset.

Core retains correctionlib evaluation, configurable generator-weight handling,
configurable tagging/working-point algorithms, the object-energy base class,
object collections, trigger-group mechanics, transport/retry/probing engines,
batch execution, histogramming, plotting, generic STXS classification/weights,
shared analysis preflight primitives, sample-format conversion, and all statistical integrations.
CMS recipes and payload selection are provided by the extension.

## Python, batch jobs, and provenance

The existing `rdfanalyzer` native module exposes `has_extension("cms")`,
`enabled_extensions`, and the compatibility `has_cms` attribute. CMS manager factory
implementations are compiled from the extension only when enabled. Existing
`rucio_discovery` and `rucio_tasks` imports and module
entry points forward to the optional `rdfanalyzer_cms` package. Generic imports
and Combine tasks do not load Rucio or CMS credentials.

For source-based Python workflows, `RDF_BUILD_EXTENSIONS=OFF` disables all
providers and `RDF_ENABLE_CMS=OFF` disables just CMS;
`RDF_EXTENSION_PATHS` selects external checkouts (`RDF_CMS_SOURCE_DIR` remains
a Python compatibility override). These are environment
variables for Python and `env.sh`, and CMake cache arguments for C++ builds.
Set both when switching an entire mixed C++/Python workflow. Restart Python
after changing providers; imported modules retain normal Python caching.

Submission scripts embed resolved site policy as Python literals, so stage-in
and site ranking do not require the extension on workers. The XRootD library
compiles its policy and carries the policy header alongside its dictionary
payload. Existing config/aux staging and shared-library collection continue to
package the analysis executable and its runtime inputs. CMS analysis code is
statically linked; workers do not clone a source repository.

Output provenance records the CMS revision, dirty state, and enabled state
alongside framework provenance. Rebuild after changing either checkout; prepare
new worker bundles to deploy rebuilt binaries. Existing submitted workers are
not changed by this extraction.

See [extraction validation](CMS_EXTRACTION_VALIDATION.md) for the checked build
matrix, event tests, checkout portability, and existing unrelated test failures.

## Skills and analysis policy

CMS workflows ship in the extension's `.agents/skills/` with an `AGENTS.md`
entry point. Era resolution, corrections/MET/cleaning, concrete object managers,
weight/tagging recipes, and CMS discovery policy are documented there. Their
links work inside an independently cloned CMS repository. The core skill tree
keeps lightweight routing and documents generic framework contracts.

Analysis-specific readiness policies, sample process inference and migration
conventions belong to the analysis checkout. The local extraction preserves
the historical tools in `analyses/VHbbSTXS`; that tools repository does not
include the missing full analysis sources or configs.

STXS is always built by core, including with `RDF_BUILD_EXTENSIONS=OFF`.
Its decay-channel input defaults to `analysis_channel`. Analyses must set
`stxsDecayChannelBranch` for a different input. Migration histogram lookup
uses explicit analysis-supplied mappings; no analysis category convention is
chosen by core.
