# RDFAnalyzerCore

A powerful, config-driven framework for building physics analyses with ROOT RDataFrame.

[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://brkronheim.github.io/RDFAnalyzerCore/)
[![ROOT](https://img.shields.io/badge/ROOT-6.30%2B-blue.svg)](https://root.cern/)
[![C++](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/)

## Overview

RDFAnalyzerCore provides a core, analysis-agnostic framework for constructing and running Analyzer pipelines using ROOT RDataFrame. The framework features:

- **Config-Driven Architecture**: Separate configuration from code for reproducibility
- **Plugin System**: Extensible design with BDT, ONNX, correction, and histogram managers
- **Lazy Evaluation**: Leverages RDataFrame's efficient event processing
- **Systematic Support**: Built-in handling of systematic variations
- **Analysis Modularity**: Analyses live in separate repositories, automatically discovered at build time
- **Python Bindings**: Use the framework from Python with numba and numpy integration
- **Statistical Analysis**: Optional CMS Combine integration for limit setting and fits

## Quick Start

### NDHistogramManager
Books and fills N-dimensional histograms with support for systematics, regions, and categories. Supports both manual histogram booking and config-driven histogram definitions.

**Config-Driven Histograms**: Define histograms in a configuration file for dynamic runtime booking. See `docs/CONFIG_HISTOGRAMS.md` for detailed documentation.

Quick example:
```cpp
// Enable histogram manager
auto histManager = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());
analyzer.addPlugin("histogramManager", std::move(histManager));

// Define variables and apply filters
analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"});
analyzer.Filter("quality", isGood, {"jet_quality"});

// Book histograms from config file (after all defines/filters)
analyzer.bookConfigHistograms();

// Save results
analyzer.save();
```

Config file format (`histograms.txt`):
```
name=pt_hist variable=jet_pt weight=event_weight bins=50 lowerBound=0.0 upperBound=500.0
```

## Installing
Clone the repository:
```
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
cd RDFAnalyzerCore
source env.sh  # On lxplus
source build.sh

# Run example
cd build/analyses/ExampleAnalysis
./example cfg.txt
```

**New to the framework?** Check out the [Getting Started Guide](docs/GETTING_STARTED.md).

## Documentation

### For Users

- **[Getting Started](docs/GETTING_STARTED.md)** - Installation and first steps
- **[Configuration Reference](docs/CONFIG_REFERENCE.md)** - Complete config file documentation
- **[Analysis Guide](docs/ANALYSIS_GUIDE.md)** - Building analyses step-by-step
- **[Python Bindings](docs/PYTHON_BINDINGS.md)** - Using the framework from Python
- **[API Reference](docs/API_REFERENCE.md)** - Detailed API documentation
- **[Datacard Generator](docs/DATACARD_GENERATOR.md)** - Creating CMS combine datacards
- **[Combine Integration](docs/COMBINE_INTEGRATION.md)** - Complete workflow from analysis to statistical inference

### For Developers

- **[Architecture](docs/ARCHITECTURE.md)** - Internal design and structure
- **[Plugin Development](docs/PLUGIN_DEVELOPMENT.md)** - Creating custom plugins
- **[ONNX Implementation](docs/ONNX_IMPLEMENTATION.md)** - ONNX manager details
- **[ONNX Multi-Output](docs/ONNX_MULTI_OUTPUT.md)** - Multi-output model support

## Requirements

- ROOT 6.30/02 or later (progress bar support requires 6.28+)
- CMake 3.19.0 or later
- C++17 compatible compiler
- Git

**For Python bindings (optional):**
- Python 3.8+
- pybind11, numpy, numba (install with `pip install pybind11 numpy numba`)

**Self-hosted CI runner Dockerfile**
- A ready-to-build runner image including ROOT, Python, `numpy` and `numba` is provided at `docker/gh-runner.Dockerfile`.
- See `docs/CI_DOCKERFILE.md` for build/run instructions and details.

## Repository Structure

```
RDFAnalyzerCore/
├── core/               # Framework code
│   ├── interface/     # Public headers and interfaces
│   ├── src/          # Core implementations
│   ├── plugins/      # Plugin managers (BDT, ONNX, etc.)
│   ├── bindings/     # Python bindings (pybind11)
│   ├── python/       # HTCondor submission scripts
│   └── test/         # Unit tests
├── analyses/          # Analysis repositories (git submodules/clones)
├── examples/          # Python binding examples
├── docs/             # Documentation
├── cmake/            # CMake modules
└── build/            # Build artifacts (generated)
```

## Features

### Plugin System

The framework includes several built-in plugins for common analysis tasks:

#### BDTManager
Manages Boosted Decision Trees using the FastForest library.
- Load BDT models from text files
- Apply models with sigmoid activation
- Conditional execution for efficiency

#### OnnxManager
Manages ONNX machine learning models from any ML framework.
- Automatic ONNX Runtime setup (no manual installation)
- Support for multi-output models (e.g., ParticleTransformer)
- Thread-safe inference with ROOT ImplicitMT
- **See**: [ONNX Implementation Guide](docs/ONNX_IMPLEMENTATION.md)

#### SofieManager
Manages SOFIE (System for Optimized Fast Inference code Emit) models from ROOT TMVA.
- Build-time compilation from ONNX for maximum performance
- Zero runtime overhead (compiled C++ code)
- Eliminates runtime model loading overhead compared to ONNX Runtime
- Manual registration required (rebuild for model updates)
- **See**: [SOFIE Implementation Guide](docs/SOFIE_IMPLEMENTATION.md)

#### CorrectionManager
Applies scale factors and corrections using correctionlib.
- JSON-based correction definitions
- Automatic application of configured corrections
- Support for multi-dimensional lookups

#### TriggerManager
Handles trigger logic and trigger menu configuration.
- Configurable trigger groups with OR logic
- Trigger veto support
- Sample-specific trigger configurations

#### NDHistogramManager
Books and fills N-dimensional histograms.
- Support for systematics, regions, and categories
- Automatic systematic axis generation
- Vector-based filling with scalar expansion

### Configuration-Driven Design

All framework behavior is controlled through text configuration files:
- Main configuration: I/O, performance, plugin configs
- Plugin configs: Model definitions, corrections, triggers
- Output configs: Branch selection, histogram definitions

**Example**:
```
# Main config
fileList=data.root
saveFile=output.root
threads=-1
bdtConfig=cfg/bdts.txt
onnxConfig=cfg/onnx_models.txt
```

See [Configuration Reference](docs/CONFIG_REFERENCE.md) for complete documentation.

## Installation and Building

### On lxplus (CERN)

```bash
# Clone repository
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
cd RDFAnalyzerCore

# Setup environment
source env.sh

# Build
source build.sh
```

### Local Installation

Ensure ROOT and CMake are available:

```bash
# Setup ROOT
source /path/to/root/bin/thisroot.sh

# Build
cmake -S . -B build
cmake --build build -j$(nproc)
```

### Build Options

The framework supports optional features that can be enabled at build time:

```bash
# Build with all features (default: tests enabled, Combine disabled)
cmake -S . -B build

# Disable tests (faster build for production)
cmake -S . -B build -DBUILD_TESTS=OFF

# Enable CMS Combine for statistical analysis
cmake -S . -B build -DBUILD_COMBINE=ON

# Enable both Combine and CombineHarvester
cmake -S . -B build \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON

# Complete build with all options
cmake -S . -B build \
    -DBUILD_TESTS=ON \
    -DBUILD_COMBINE=ON \
    -DBUILD_COMBINE_HARVESTER=ON

cmake --build build -j$(nproc)
```

**Available Options:**
- `BUILD_TESTS` (default: `ON`) - Build analysis tests
- `BUILD_COMBINE` (default: `OFF`) - Build CMS Combine package
- `BUILD_COMBINE_HARVESTER` (default: `OFF`) - Build CombineHarvester (requires `BUILD_COMBINE=ON`)

**Note**: Building Combine and CombineHarvester takes several minutes and requires an internet connection.

See [Combine Integration Guide](docs/COMBINE_INTEGRATION.md) for complete statistical analysis workflows.

### Testing

```bash
source test.sh
```

Or run specific tests:
```bash
cd build
ctest -R TestName -V
```

## Adding Your Analysis

Analyses are developed in separate repositories and automatically discovered during build:

```bash
# Clone your analysis into analyses/
cd analyses
git clone <your-analysis-repo> MyAnalysis
cd ..

# Rebuild - your analysis is automatically found
source build.sh

# Run
cd build/analyses/MyAnalysis
./myanalysis config.txt
```

**Requirements for analysis repositories**:
- Must contain a `CMakeLists.txt` at the root
- Should link against `RDFCore` library
- Configuration files typically in `cfg/` subdirectory

See [Analysis Guide](docs/ANALYSIS_GUIDE.md) for step-by-step instructions.

## Framework Architecture

### Core Concepts

The framework is built around several key components that work together:

1. **Analyzer**: Central orchestrator providing a simplified API
2. **ConfigurationManager**: Loads and provides access to configuration
3. **DataManager**: Wraps ROOT::RDataFrame with systematic support
4. **SystematicManager**: Tracks and propagates systematic variations
5. **Plugins**: Extensible managers for specific tasks (ML, corrections, histograms)
6. **OutputSinks**: Abstract destinations for skims and metadata

All managers use **compile-time wiring** through C++ - the Analyzer is agnostic to concrete plugin types and interacts only through interfaces.

### Data Flow

```
Configuration Files
        ↓
ConfigurationManager → Plugins Load Configs
        ↓
DataManager builds TChain & RDataFrame
        ↓
User Code: Define Variables, Apply Filters
        ↓
Plugins: Apply Models, Corrections, Book Histograms
        ↓
RDataFrame Event Loop (Lazy Evaluation)
        ↓
Output Sinks: Write Skims & Metadata
```

**Key Design Principles**:
- **Interface-based**: Components depend on interfaces, not implementations
- **Plugin architecture**: Extensible without modifying core
- **Config-driven**: Behavior controlled by text files
- **Lazy evaluation**: Efficient processing via RDataFrame

See [Architecture Documentation](docs/ARCHITECTURE.md) for detailed internals.

## Usage Example

Here's a minimal analysis using the framework:

```cpp
#include <analyzer.h>

int main(int argc, char **argv) {
    // Create analyzer from config file
    Analyzer analyzer(argv[1]);
    
    // Define variables
    analyzer.Define("good_jets",
        [](const RVec<float>& pt, const RVec<float>& eta) {
            return pt > 25.0 && abs(eta) < 2.5;
        },
        {"jet_pt", "jet_eta"}
    );
    
    analyzer.Define("n_good_jets",
        [](const RVec<bool>& good) { return Sum(good); },
        {"good_jets"}
    );
    
    // Apply selection
    analyzer.Filter("jet_selection",
        [](int n_jets) { return n_jets >= 4; },
        {"n_good_jets"}
    );
    
    // Apply ML model (from config)
    auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");
    onnxMgr->applyAllModels();
    
    // Save outputs
    analyzer.save();
    
    return 0;
}
```

**Configuration** (`config.txt`):
```
fileList=data1.root,data2.root
saveFile=output.root
threads=-1
onnxConfig=cfg/onnx_models.txt
saveConfig=cfg/output_branches.txt
```

See [Analysis Guide](docs/ANALYSIS_GUIDE.md) for complete examples.

## Python Bindings

The framework can also be used from Python with high performance:

```python
import rdfanalyzer

# Create analyzer from config file
analyzer = rdfanalyzer.Analyzer("config.txt")

# Define variables using C++ expressions (ROOT JIT)
analyzer.Define("pt_gev", "pt / 1000.0", ["pt"])
analyzer.Define("delta_r", 
                   "sqrt(delta_eta*delta_eta + delta_phi*delta_phi)",
                   ["delta_eta", "delta_phi"])

# Or use numba-compiled functions
import numba, ctypes

@numba.cfunc("float64(float64)")
def convert_to_gev(pt):
    return pt / 1000.0

func_ptr = ctypes.cast(convert_to_gev.address, ctypes.c_void_p).value
analyzer.DefineFromPointer("pt_gev", func_ptr, "double(double)", ["pt"])

# Apply filters and save
analyzer.Filter("high_pt", "pt_gev > 25.0", ["pt_gev"])
analyzer.save()
```

**Key Features:**
- String-based expressions (ROOT JIT compilation)
- Numba function pointers for custom logic
- Numpy array integration
- Full systematic variation support

See [Python Bindings Guide](docs/PYTHON_BINDINGS.md) for complete documentation and examples.

## Advanced Features

### Machine Learning Integration

The framework supports multiple ML backends:

- **ONNX**: Runtime evaluation of models from any framework (PyTorch, TensorFlow, scikit-learn)
- **BDT**: FastForest-based boosted decision trees
- **SOFIE**: Build-time compiled models for maximum performance

All managers support:
- Conditional execution (skip expensive inference when not needed)
- Multi-output models
- Thread-safe inference with ROOT ImplicitMT

### Systematic Uncertainties

Built-in support for systematic variations:
```cpp
sysMgr->registerSystematic("jes_up");
sysMgr->registerSystematic("jes_down");

analyzer.Define("corrected_pt",
    [](float pt, const std::string& sys) {
        if (sys == "jes_up") return pt * 1.02;
        if (sys == "jes_down") return pt * 0.98;
        return pt;
    },
    {"jet_pt"},
    sysMgr
);
```

Histograms automatically include systematic axes.

### CMS Combine Datacard Generation

Framework includes a Python script for generating CMS combine datacards from analysis outputs:

```bash
python core/python/create_datacards.py config.yaml
```

Features:
- YAML-based configuration for datacards
- Multiple control region support
- Sample combination (binned/stitched samples)
- Observable rebinning
- Systematic uncertainties (rate and shape)
- Automatic correlation handling

**See**: [Datacard Generator Guide](docs/DATACARD_GENERATOR.md) for complete documentation.

### HTCondor Submission

Framework includes Python scripts for batch submission:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --exe build/analyses/MyAnalysis/myanalysis \
    --stage-outputs \
    --spool
```

Features:
- Rucio-based dataset discovery (NANO) or CERN Open Data
- Automatic input/output staging
- XRootD support
- Shared executable staging
- Configuration validation

**See**: [Batch Submission Guide](docs/BATCH_SUBMISSION.md) for complete documentation.

### Custom ROOT Dictionaries

Support for custom C++ objects:

```bash
cmake -S . -B build \
  -DRDF_CUSTOM_DICT_HEADERS="MyEvent.h;MyObject.h" \
  -DRDF_CUSTOM_DICT_LINKDEF="MyLinkDef.h" \
  -DRDF_CUSTOM_DICT_INCLUDE_DIRS="/path/to/headers"
```

Dictionaries are automatically built and linked.

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

For new plugins, see [Plugin Development Guide](docs/PLUGIN_DEVELOPMENT.md).

## Support

- **Documentation**: Check the `docs/` directory
- **Issues**: Open an issue on GitHub
- **Examples**: See `analyses/ExampleAnalysis/`

## License

This project is licensed under the terms specified in the repository.

## Acknowledgments

- Built on [ROOT](https://root.cern/) RDataFrame
- Uses [ONNX Runtime](https://onnxruntime.ai/) for ML inference
- Corrections via [correctionlib](https://github.com/cms-nanoAOD/correctionlib)
- BDT support via [FastForest](https://github.com/guitargeek/FastForest)

---

**Full Documentation**: https://brkronheim.github.io/RDFAnalyzerCore/
