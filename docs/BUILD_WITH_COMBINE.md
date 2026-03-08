# Building with CMS Combine

## Quick Start

To build RDFAnalyzerCore with CMS Combine support:

```bash
# Setup ROOT environment first
source env.sh  # on lxplus, or source your ROOT installation

# Configure with Combine
cmake -S . -B build -DBUILD_COMBINE=ON

# Or with both Combine and CombineHarvester
cmake -S . -B build -DBUILD_COMBINE=ON -DBUILD_COMBINE_HARVESTER=ON

# Build (this will take several minutes)
cmake --build build -j$(nproc)
```

## Requirements

Building Combine requires:
- ROOT 6.24+ with development headers
- Python 2.7 or 3.6+
- Boost libraries
- Git

These should already be available if you're on lxplus or have a proper ROOT installation.

## What Gets Built

After building with `BUILD_COMBINE=ON`:
- CMS Combine is cloned and built in: `build/external/HiggsAnalysis/CombinedLimit/`
- The `combine` executable is at: `build/external/HiggsAnalysis/CombinedLimit/bin/combine`

After building with `BUILD_COMBINE_HARVESTER=ON`:
- CombineHarvester is cloned, patched for modern ROOT compatibility, and built in: `build/external/CombineHarvester/`
- Libraries are at: `build/external/CombineHarvester/lib/`
- Tools are at: `build/external/CombineHarvester/CombineTools/bin/`

## Usage

After building, run combine directly:

```bash
# From datacards directory
../build/external/HiggsAnalysis/CombinedLimit/bin/combine -M AsymptoticLimits datacard.txt
```

Or add to your PATH:

```bash
export PATH="$PWD/build/external/HiggsAnalysis/CombinedLimit/bin:$PATH"
combine -M AsymptoticLimits datacards/datacard.txt
```

## Complete Workflow

See `docs/COMBINE_INTEGRATION.md` for the complete analysis workflow from data processing to limit extraction.

## Troubleshooting

### Build Failures

If the Combine build fails:
1. Ensure ROOT is properly sourced before running cmake
2. Check that you have network access (Combine is cloned from GitHub)
3. Look at build logs in: `build/external/src/CombineTool-stamp/`

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
- [Combine Integration Guide](docs/COMBINE_INTEGRATION.md) - Complete workflow
- [Datacard Generator](docs/DATACARD_GENERATOR.md) - Creating datacards
- [CMS Combine Documentation](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/)
