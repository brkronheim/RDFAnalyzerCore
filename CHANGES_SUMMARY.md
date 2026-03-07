# Summary of Changes: Optional CMS Combine Package Building

## Overview

This document summarizes the changes made to add optional building of CMS Combine and CombineHarvester packages, along with configurable test building.

## New Features

### 1. Build Options

Three new CMake options have been added to `CMakeLists.txt`:

- **`BUILD_TESTS`** (default: `ON`) - Control whether analysis tests are built
- **`BUILD_COMBINE`** (default: `OFF`) - Build CMS Combine package
- **`BUILD_COMBINE_HARVESTER`** (default: `OFF`) - Build CombineHarvester libraries and tools (requires `BUILD_COMBINE=ON`)

### 2. CMake Infrastructure

**New file: `cmake/SetupCombine.cmake`**
- Handles downloading and building of Combine and CombineHarvester
- Uses CMake's ExternalProject_Add to clone and build external repositories
- Creates targets `CombineTool` and `CombineHarvester`
- Exports paths for easy access after building
- Applies compatibility patches needed for newer ROOT releases when building CombineHarvester standalone

**Modified: `CMakeLists.txt`**
- Added option definitions at the top
- Conditionally includes `SetupCombine.cmake` when needed
- No impact on default build behavior

**Modified: `core/CMakeLists.txt`**
- Tests subdirectory only added when `BUILD_TESTS=ON`
- Maintains backward compatibility (tests built by default)

### 3. Documentation

**New: `docs/COMBINE_INTEGRATION.md`**
Comprehensive guide covering:
- Building with Combine support
- Complete analysis workflow from data processing to statistical inference
- Detailed examples of:
  - Running analyses with RDFAnalyzerCore
  - Creating datacards with the datacard generator
  - Running Combine for limits, fits, and scans
  - Using CombineHarvester for advanced analyses
- Troubleshooting and best practices
- Example shell scripts for automation

**New: `docs/BUILD_WITH_COMBINE.md`**
Quick reference for:
- Build requirements
- Installation paths
- Usage after building
- Troubleshooting build issues

**New: `examples/complete_analysis_workflow.sh`**
Template script demonstrating:
- Running analysis to create histograms
- Generating datacards
- Running Combine statistical analysis
- Extracting results

**Modified: `README.md`**
- Added "Statistical Analysis" to features list
- Added "Combine Integration" to documentation links
- New "Build Options" section explaining all CMake options
- Clear instructions for building with Combine

**Modified: `docs/DATACARD_GENERATOR.md`**
- Added section on running CMS Combine after datacard creation
- Links to comprehensive Combine integration guide
- Quick examples of common Combine commands

## Usage Examples

### Building Without Changes (Default)

```bash
# Default build - tests enabled, Combine disabled
cmake -S . -B build
cmake --build build -j$(nproc)
```

### Building Without Tests

```bash
# Faster build for production
cmake -S . -B build -DBUILD_TESTS=OFF
cmake --build build -j$(nproc)
```

### Building With Combine

```bash
# Enable Combine for statistical analysis
cmake -S . -B build -DBUILD_COMBINE=ON
cmake --build build -j$(nproc)

# Combine executable will be at:
# build/external/HiggsAnalysis/CombinedLimit/bin/combine
```

### Building With All Features

```bash
# Complete build with tests, Combine, and CombineHarvester
cmake -S . -B build \
    -DBUILD_TESTS=ON \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON
cmake --build build -j$(nproc)
```

## Complete Analysis Workflow

The framework now supports a complete end-to-end analysis:

### 1. Run Analysis
```bash
./build/analyses/MyAnalysis/myanalysis config.txt
```
Produces ROOT files with histograms.

### 2. Create Datacards
```bash
python core/python/create_datacards.py datacard_config.yaml
```
Generates CMS Combine datacards and shape files.

### 3. Run Statistical Analysis
```bash
cd datacards
../build/external/HiggsAnalysis/CombinedLimit/bin/combine \
    -M AsymptoticLimits datacard_signal_region.txt
```
Calculates limits, performs fits, extracts results.

## Integration Points

### With Existing Features

- **NDHistogramManager**: Produces histograms for datacard creation
- **Systematic uncertainties**: Automatically handled in datacards
- **Configuration system**: YAML configs for datacard generation
- **Python utilities**: Datacard creation script integrates seamlessly

### External Dependencies

- Combine requires ROOT (already a framework dependency)
- CombineHarvester requires Combine
- All dependencies handled automatically by CMake
- Standalone CombineHarvester build installs libraries to `build/external/CombineHarvester/lib`

## Backward Compatibility

✅ **Fully backward compatible:**
- Default build behavior unchanged (tests enabled, Combine disabled)
- Existing build scripts continue to work
- No changes to core framework functionality
- All existing features remain accessible

## File Summary

### Added Files
- `cmake/SetupCombine.cmake` - CMake module for Combine setup
- `docs/COMBINE_INTEGRATION.md` - Complete integration guide
- `docs/BUILD_WITH_COMBINE.md` - Build instructions
- `examples/complete_analysis_workflow.sh` - Example workflow script

### Modified Files
- `CMakeLists.txt` - Added build options and Combine setup
- `core/CMakeLists.txt` - Conditional test building
- `README.md` - Updated features and documentation links
- `docs/DATACARD_GENERATOR.md` - Added Combine usage section

### Impact
- Core framework: No changes
- Build system: Optional features added
- Documentation: Comprehensive coverage of new capabilities
- Examples: Complete workflow demonstrations

## Benefits

### For Users
1. **Complete workflow**: Analysis → Datacards → Statistical inference in one framework
2. **Flexibility**: Choose which components to build
3. **Performance**: Skip tests for faster production builds
4. **Documentation**: Clear, comprehensive guides with examples

### For Development
1. **Faster iteration**: Skip test builds during development
2. **Modular**: Optional components don't affect core functionality
3. **Maintainable**: External projects managed by CMake
4. **Extensible**: Easy to add more optional components

## Testing Recommendations

To verify the changes:

### 1. Test Default Build
```bash
cmake -S . -B build_default
cmake --build build_default
# Should build successfully with tests
```

### 2. Test Without Tests
```bash
cmake -S . -B build_no_tests -DBUILD_TESTS=OFF
cmake --build build_no_tests
# Should build without test directory
```

### 3. Test With Combine (requires ROOT environment)
```bash
source env.sh  # or your ROOT setup
cmake -S . -B build_combine -DBUILD_COMBINE=ON
cmake --build build_combine
# Should download and build Combine
# Verify: ls build_combine/external/HiggsAnalysis/CombinedLimit/bin/combine
```

### 4. Test Complete Workflow (manual)
Follow the examples in `docs/COMBINE_INTEGRATION.md` to verify the complete analysis chain.

## Future Enhancements

Potential future additions:
- More statistical tools (ROOT RooFit, custom limit calculators)
- Automated workflow scripts
- Result visualization tools
- Additional CombineHarvester features

## Questions or Issues

For help with:
- **Build issues**: See `docs/BUILD_WITH_COMBINE.md`
- **Combine usage**: See `docs/COMBINE_INTEGRATION.md`
- **Datacard creation**: See `docs/DATACARD_GENERATOR.md`
- **Framework usage**: See main `README.md` and other documentation

---

**Implementation Status**: ✅ Complete

All requested features have been implemented:
- ✅ Optional Combine building with `BUILD_COMBINE` flag
- ✅ Optional CombineHarvester build with `BUILD_COMBINE_HARVESTER` flag
- ✅ Configurable test building with `BUILD_TESTS` flag
- ✅ Complete documentation showing full analysis workflow
- ✅ Example scripts demonstrating usage
- ✅ Backward compatibility maintained
