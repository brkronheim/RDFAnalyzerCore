# Building with CMS Combine

## Quick Start

From a checkout of RDFAnalyzerCore, initialize the pinned core submodules and
use Python from the same environment as ROOT. Combine 11.1.0 also imports
pandas when converting a datacard.

```bash
source env.sh  # On a supported CVMFS host. Otherwise activate your ROOT environment.
git submodule update --init --recursive -- core/extern
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-ci.txt pandas

# Configure one of these two variants.
cmake -S . -B build -DBUILD_COMBINE=ON -DBUILD_COMBINE_HARVESTER=OFF
# cmake -S . -B build -DBUILD_COMBINE=ON -DBUILD_COMBINE_HARVESTER=ON

cmake --build build --parallel "$(nproc)"
```

## Requirements

Building Combine requires:
- ROOT 6.30+ with development headers
- A Python interpreter that matches PyROOT
- Boost libraries
- libxml2 development headers for the CombineHarvester build
- Git

These should already be available on CVMFS-backed HEP hosts or with a properly configured standalone ROOT installation.

## What Gets Built

After building with `BUILD_COMBINE=ON`:
- CMS Combine is cloned and built in: `build/external/HiggsAnalysis/CombinedLimit/`
- The `combine` executable is at: `build/external/HiggsAnalysis/CombinedLimit/bin/combine`

After building with `BUILD_COMBINE_HARVESTER=ON`:
- CombineHarvester is cloned, patched for modern ROOT compatibility, and built in: `build/external/CombineHarvester/`
- Libraries are at: `build/external/CombineHarvester/lib/`
- Tools are at: `build/external/CombineHarvester/CombineTools/bin/`

## Usage

From the repository root with the build environment and virtualenv active:

```bash
export COMBINE_PREFIX="$PWD/build/external/HiggsAnalysis/CombinedLimit"
export PATH="$COMBINE_PREFIX/bin:$PATH"
export LD_LIBRARY_PATH="$COMBINE_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="$COMBINE_PREFIX/python:$ROOTSYS/lib${PYTHONPATH:+:$PYTHONPATH}"
combine -M AsymptoticLimits datacards/datacard.txt
```

For standalone CombineHarvester tools, also add their library directory:

```bash
export HARVESTER_PREFIX="$PWD/build/external/CombineHarvester"
export LD_LIBRARY_PATH="$HARVESTER_PREFIX/lib:$LD_LIBRARY_PATH"
"$HARVESTER_PREFIX/CombineTools/bin/RoundTrip" \
    --input datacards/datacard.txt --output roundtrip.txt --root roundtrip.root --mass 125
```

## Complete Workflow

See [Combine Integration Guide](COMBINE_INTEGRATION.md) for the complete analysis workflow from data processing to limit extraction.

## Troubleshooting

### Build Failures

If the Combine build fails:
1. Ensure ROOT is properly sourced before running cmake
2. Check that you have network access (Combine is cloned from GitHub)
3. Look at build logs in: `build/external/HiggsAnalysis/CombinedLimit/src/CombineTool-stamp/`

### Clean Rebuild

To rebuild Combine from scratch:

```bash
# Remove the external builds
rm -rf build/external/

# Reconfigure and rebuild
cmake -S . -B build -DBUILD_COMBINE=ON
cmake --build build -j$(nproc)
```

## Disabling Combine Build

By default, Combine is not built. To build only the core framework:

```bash
cmake -S . -B build
# or explicitly:
cmake -S . -B build -DBUILD_COMBINE=OFF
```

## Build Time

Approximate build times:
- Core RDFAnalyzerCore: 1-2 minutes
- CMS Combine: 5-10 minutes
- CombineHarvester: 2-3 minutes

Total with all components: ~15 minutes

## Documentation

For complete usage documentation, see:
- [Combine Integration Guide](COMBINE_INTEGRATION.md) - Complete workflow
- [Datacard Generator](DATACARD_GENERATOR.md) - Creating datacards
- [CMS Combine Documentation](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/)
