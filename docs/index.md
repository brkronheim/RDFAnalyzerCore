# RDFAnalyzerCore Documentation

Welcome to the RDFAnalyzerCore documentation! This framework provides a powerful, config-driven system for building physics analyses with ROOT RDataFrame.

## Getting Started

New to RDFAnalyzerCore? Start here:

- **[Getting Started Guide](GETTING_STARTED.md)** - Installation, setup, and your first analysis
- **[Quick Start](GETTING_STARTED.md#quick-start)** - Get running in 5 minutes

## User Documentation

### Building Analyses

- **[Analysis Guide](ANALYSIS_GUIDE.md)** - Comprehensive guide to building analyses
  - Step-by-step tutorial
  - Working with data
  - Event selection
  - Machine learning integration
  - Histogramming
  - Complete examples

- **[Configuration Reference](CONFIG_REFERENCE.md)** - Complete configuration documentation
  - Main configuration options
  - Plugin configurations (BDT, ONNX, corrections, triggers, WeightManager, RegionManager, GoldenJsonManager, CutflowManager)
  - Configuration file format and YAML config overview
  - Example configurations

- **[Configuration Validation](CONFIGURATION_VALIDATION.md)** - YAML analysis config schema and validation
  - Full JSON Schema reference for the analysis YAML config
  - Validation error messages and troubleshooting
  - Nuisance group, region, and histogram config schema

### Python Analysis

- **[Python Bindings](PYTHON_BINDINGS.md)** - Full Python API and usage patterns
- **[Python Bindings Testing](PYTHON_BINDINGS_TESTING.md)** - Validation and troubleshooting guide

### Framework Features

- **[Machine Learning](ANALYSIS_GUIDE.md#machine-learning-integration)**
  - **[ONNX Models](ONNX_IMPLEMENTATION.md)** - Runtime ML from any framework (PyTorch, TensorFlow, scikit-learn)
  - **[SOFIE Models](SOFIE_IMPLEMENTATION.md)** - Build-time compiled models for maximum performance
  - **[BDT Models](CONFIG_REFERENCE.md#bdt-manager-configuration)** - FastForest boosted decision trees
  - **[ONNX Multi-Output](ONNX_MULTI_OUTPUT.md)** - Models with multiple outputs

- **[Batch Submission](BATCH_SUBMISSION.md)** - HTCondor job submission guide
- **[Production Manager](PRODUCTION_MANAGER.md)** - Production-scale job management and monitoring

### Data Management & Outputs

- **[Dataset Manifest](DATASET_MANIFEST.md)** - Describing and managing input datasets
  - Dataset manifest file format and schema
  - Sample metadata, cross-sections, and grouping

- **[Output Schema](OUTPUT_SCHEMA.md)** - Structure of framework output files
  - ROOT output file layout (event tree, histograms, metadata)
  - Histogram naming conventions and cutflow outputs

- **[Validation Reports](VALIDATION_REPORTS.md)** - Automated output validation
  - Interpreting validation report contents
  - Checking histogram integrity and cutflow consistency

### Systematics & Physics Objects

- **[Nuisance Groups](NUISANCE_GROUPS.md)** - Configuring systematic nuisance groups
  - Weight and shape systematic definitions
  - Nuisance group YAML schema and examples

- **[Region Binding](REGION_BINDING.md)** - Region-aware histogram and cutflow auto-binding
  - Defining analysis regions and hierarchies
  - Automatic per-region histogram booking

- **[Physics Objects](PHYSICS_OBJECTS.md)** - Physics object collection reference
  - Standard column names for leptons, jets, MET, and other objects
  - Collection conventions and branch naming

- **[Jet Energy Corrections](JET_ENERGY_CORRECTIONS.md)** - JES/JER correction plugin reference
  - CMS NanoAOD JEC workflow (strip, apply, systematic sets)
  - Type-1 MET propagation
  - PhysicsObjectCollection integration and variation maps

### Batch Processing & Performance

- **[LAW Tasks](LAW_TASKS.md)** - Luigi/LAW workflow task reference
  - Available LAW tasks and their parameters
  - Running and monitoring large-scale batch workflows

- **[Performance Monitoring](PERFORMANCE_MONITORING.md)** - Profiling and tuning analysis jobs
  - Identifying bottlenecks in RDataFrame processing
  - Memory and CPU usage guidelines

### API Reference

- **[API Reference](API_REFERENCE.md)** - Complete API documentation
  - Analyzer class
  - Configuration interfaces
  - Data management
  - Plugin interfaces
  - Manager implementations

## Developer Documentation

### Extending the Framework

- **[Architecture](ARCHITECTURE.md)** - Internal design and structure
  - Design philosophy
  - Core components
  - Plugin system
  - Data flow
  - Build system

- **[Plugin Development](PLUGIN_DEVELOPMENT.md)** - Creating custom plugins
  - When to create a plugin
  - Plugin types
  - Step-by-step guide
  - Advanced patterns
  - Testing

## Key Concepts

### Config-Driven Design

RDFAnalyzerCore separates configuration from code:

```
# config.txt
fileList=data.root
saveFile=output.root
threads=-1
onnxConfig=cfg/onnx_models.txt
```

Same binary, different configs = different analyses. Version control your configs with Git for reproducibility.

### Plugin Architecture

Framework is extensible via plugins:

- **BDTManager**: Boosted decision trees
- **OnnxManager**: Neural networks and ML models
- **SofieManager**: Build-time compiled models
- **CorrectionManager**: Scale factors and corrections
- **JetEnergyScaleManager**: JES/JER corrections, CMS systematic sets, MET propagation, and PhysicsObjectCollection integration
- **TriggerManager**: Trigger logic
- **NDHistogramManager**: N-dimensional histograms
- **WeightManager**: Nominal and varied event weights
- **RegionManager**: Named analysis regions with hierarchy
- **CutflowManager**: Sequential cutflow and N-1 tables
- **GoldenJsonManager**: Data certification via golden JSON

Add custom plugins without modifying core code.

### Lazy Evaluation

Built on ROOT RDataFrame's lazy evaluation:

```cpp
analyzer.Define("var", ...);    // Queued
analyzer.Filter("cut", ...);    // Queued
analyzer.save();                // Executes all at once
```

Efficient processing with automatic optimization.

## Quick Links

### For New Users

1. [Installation](GETTING_STARTED.md#installation-and-building)
2. [First Analysis](GETTING_STARTED.md#run-the-example-analysis)
3. [Configuration Basics](CONFIG_REFERENCE.md#main-configuration-file)
4. [Analysis Tutorial](ANALYSIS_GUIDE.md#step-by-step-tutorial)

### For Advanced Users

1. [Architecture Overview](ARCHITECTURE.md#architecture-overview)
2. [Creating Plugins](PLUGIN_DEVELOPMENT.md#step-by-step-creating-a-plugin)
3. [Systematic Uncertainties](ANALYSIS_GUIDE.md#systematics)
4. [HTCondor Submission](ANALYSIS_GUIDE.md#advanced-topics)

### Common Tasks

- **Add ML model**: [ONNX Configuration](CONFIG_REFERENCE.md#onnx-manager-configuration)
- **Apply corrections**: [CorrectionManager](CONFIG_REFERENCE.md#correction-manager-configuration)
- **Apply JES/JER corrections**: [Jet Energy Corrections](JET_ENERGY_CORRECTIONS.md)
- **Book histograms**: [Histogramming Guide](ANALYSIS_GUIDE.md#histogramming)
- **Handle systematics**: [Systematics Guide](ANALYSIS_GUIDE.md#systematics)
- **Submit to batch**: [HTCondor Scripts](ANALYSIS_GUIDE.md#advanced-topics)
- **Validate my config**: [Configuration Validation](CONFIGURATION_VALIDATION.md)
- **Understanding outputs**: [Output Schema](OUTPUT_SCHEMA.md)
- **Physics object collections**: [Physics Objects](PHYSICS_OBJECTS.md)
- **LAW task reference**: [LAW Tasks](LAW_TASKS.md)

## Examples

### Minimal Analysis

```cpp
#include <analyzer.h>

int main(int argc, char **argv) {
    Analyzer analyzer(argv[1]);
    
    // Define variable
    analyzer.Define("pt_gev",
        [](float pt) { return pt / 1000.0; },
        {"jet_pt"}
    );
    
    // Apply selection
    analyzer.Filter("pt_cut",
        [](float pt) { return pt > 25.0; },
        {"pt_gev"}
    );
    
    // Apply ML model
    auto* onnx = analyzer.getPlugin<IOnnxManager>("onnx");
    onnx->applyAllModels();
    
    // Save
    analyzer.save();
    return 0;
}
```

### Full Analysis Example

See [Complete Example](ANALYSIS_GUIDE.md#complete-example) in the Analysis Guide for a production-ready analysis with:
- Event selection
- Machine learning
- Corrections
- Histograms
- Systematics

## Requirements

- **ROOT**: 6.30/02 or later
- **CMake**: 3.19.0 or later
- **Compiler**: C++17 compatible
- **OS**: Linux, macOS

ONNX Runtime is downloaded automatically during build.

## Support

- **Documentation**: Browse this site
- **GitHub**: [RDFAnalyzerCore Repository](https://github.com/brkronheim/RDFAnalyzerCore)
- **Issues**: [Report a bug](https://github.com/brkronheim/RDFAnalyzerCore/issues)
- **Examples**: Check `analyses/ExampleAnalysis/` in the repository

## Contributing

Contributions welcome! See [Plugin Development Guide](PLUGIN_DEVELOPMENT.md) to get started.

For implementation details and design notes for major features, see the [DEVELOPMENT](DEVELOPMENT/) directory.

## License

See repository for license information.

---

**Repository**: https://github.com/brkronheim/RDFAnalyzerCore

**Last Updated**: March 2026
