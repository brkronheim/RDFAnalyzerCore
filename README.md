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

## Quick Start

```bash
# Clone and build
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
- **[API Reference](docs/API_REFERENCE.md)** - Detailed API documentation

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

## Repository Structure

```
RDFAnalyzerCore/
├── core/               # Framework code
│   ├── interface/     # Public headers and interfaces
│   ├── src/          # Core implementations
│   ├── plugins/      # Plugin managers (BDT, ONNX, etc.)
│   ├── python/       # HTCondor submission scripts
│   └── test/         # Unit tests
├── analyses/          # Analysis repositories (git submodules/clones)
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
- Build-time compilation of models for maximum performance
- Generated C++ code integrated into framework
- Faster inference than runtime ONNX evaluation

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

### HTCondor Submission

Framework includes Python scripts for batch submission:

```bash
python core/python/generateSubmissionFilesNANO.py \
    --exe build/analyses/MyAnalysis/myanalysis \
    --stage-outputs \
    --spool
```

Features:
- Rucio-based dataset discovery
- Automatic input/output staging
- XRootD support
- Shared executable staging

See `core/python/` for submission tools.

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

## Citation

If you use RDFAnalyzerCore in your research, please cite:

```bibtex
@software{rdfanalyzercore,
  author = {Kronheim, Benjamin},
  title = {RDFAnalyzerCore: A Framework for ROOT RDataFrame Analysis},
  url = {https://github.com/brkronheim/RDFAnalyzerCore},
  year = {2024}
}
```

## Acknowledgments

- Built on [ROOT](https://root.cern/) RDataFrame
- Uses [ONNX Runtime](https://onnxruntime.ai/) for ML inference
- Corrections via [correctionlib](https://github.com/cms-nanoAOD/correctionlib)
- BDT support via [FastForest](https://github.com/guitargeek/FastForest)

---

**Full Documentation**: https://brkronheim.github.io/RDFAnalyzerCore/
