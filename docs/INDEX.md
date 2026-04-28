# Documentation Index

## Quick Links

- **[Architecture](./ARCHITECTURE.md)** - Core C++ architecture and manager lifecycle
- **[API Reference](./API_REFERENCE.md)** - Canonical C++ API surface
- **[Getting Started](./GETTING_STARTED.md)** - Installation and first steps
- **[Python Bindings](./PYTHON_BINDINGS_QUICKSTART.md)** - Using RDFAnalyzerCore from Python
- **[Configuration Reference](./CONFIG_REFERENCE.md)** - All config options
- **[Error Handling](./ERRORS_AND_TRACING.md)** - Common errors and solutions

## Documentation Structure

### User Guides

**Getting Started**
- [Installation](./GETTING_STARTED.md) - Build requirements
- [Quick Start](./GETTING_STARTED.md) - Minimal example
- [Examples](./examples/config_histograms/README.md) - Working examples

**Analysis Guide**
- [Building Analyses](./ANALYSIS_GUIDE.md) - Step-by-step tutorials
- [Configuration](./CONFIG_REFERENCE.md) - All options explained
- [Histograms](./CONFIG_HISTOGRAMS.md) - ND Histogram Manager

**Python**
- [Quick Start](./PYTHON_BINDINGS_QUICKSTART.md) - Minimal Python usage
- [Deep Dive](./PYTHON_BINDINGS_DEEP.md) - Advanced patterns
- [Testing](./PYTHON_BINDINGS_TESTING_DETAILED.md) - Comprehensive tests

### Developer Guides

**Architecture**
- [Overview](./ARCHITECTURE.md) - C++ core design, plugin wiring, lifecycle hooks
- [Plugins](./PLUGIN_DEVELOPMENT.md) - Extensibility and plugin lifecycle

**C++ Development**
- [API Reference](./API_REFERENCE.md) - Class documentation
- [Plugin Development](./PLUGIN_DEVELOPMENT.md) - Creating plugins
- [Doxygen Guide](./DOXYGEN_GUIDE.md) - Header documentation

### Statistical Analysis

**CMS Combine**
- [Integration](./COMBINE_INTEGRATION.md) - Complete workflow
- [Datacards](./DATACARD_GENERATOR.md) - Generating datacards
- [Systematics](./DATACARD_SYSTEMATICS_EXAMPLE.md) - Handling variations

**Validation**
- [Validation](./CONFIGURATION_VALIDATION.md) - Config checking
- [Reports](./VALIDATION_REPORTS.md) - Analysis reports

### Advanced Features

**Plugins**
- [ONNX](./ONNX_IMPLEMENTATION.md) - ML models
- [Correction](./CMS_CORRECTIONS.md) - Scale factors
- [Jet Energy Scale](./JET_ENERGY_CORRECTIONS.md) - JES corrections
- [SOFIE](./SOFIE_IMPLEMENTATION.md) - Compiled models

**Systematics**
- [Nuisance Groups](./NUISANCE_GROUPS.md) - Grouping variations
- [Region Binding](./REGION_BINDING.md) - Systematic regions

**Production**
- [Production Manager](./PRODUCTION_MANAGER.md) - Batch job management
- [Batch Submission](./BATCH_SUBMISSION.md) - HTCondor scripts
- [Data Manifest](./DATASET_MANIFEST.md) - Dataset metadata

### Technical Details

**Configuration**
- [YAML Support](./YAML_CONFIG_SUPPORT.md) - YAML configs
- [Config Histograms](./CONFIG_HISTOGRAMS.md) - Config-driven histograms

**Output**
- [Schema](./OUTPUT_SCHEMA.md) - Output format
- [Validation](./VALIDATION_REPORTS.md) - Validation

## Documentation Quality

Documentation is maintained as code-adjacent guidance. For API truth, treat C++ headers in `core/interface/` as canonical and use these docs as usage-oriented references.

Current high-priority core areas:
- Analyzer/plugin lifecycle details in `ARCHITECTURE.md`
- Public C++ API signatures in `API_REFERENCE.md`
- Plugin extension patterns in `PLUGIN_DEVELOPMENT.md`

## Contributing

**Writing New Docs**
1. Follow existing style (headers, code blocks, cross-references)
2. Verify with actual code
3. Update index
4. Run validation

**Reporting Issues**
- Broken links: File in documentation issues
- Outdated content: Update or request review
- Missing features: Request documentation

## Search Tips

**Find by Topic:**
- "Python": [PYTHON_BINDINGS_QUICKSTART](./PYTHON_BINDINGS_QUICKSTART.md), [PYTHON_BINDINGS_DEEP](./PYTHON_BINDINGS_DEEP.md)
- "Error": [ERRORS_AND_TRACING](./ERRORS_AND_TRACING.md)
- "Performance": [PERFORMANCE_TUNING](./PERFORMANCE_TUNING.md)
- "Combine": [COMBINE_INTEGRATION](./COMBINE_INTEGRATION.md), [DATACARD_GENERATOR](./DATACARD_GENERATOR.md)

**Find by API:**
- `Analyzer`: [API_REFERENCE](./API_REFERENCE.md)
- `HistogramManager`: [CONFIG_HISTOGRAMS](./CONFIG_HISTOGRAMS.md)
- `Plugin`: [PLUGIN_DEVELOPMENT](./PLUGIN_DEVELOPMENT.md)

**Find by Config:**
- `threads`: [CONFIG_REFERENCE](./CONFIG_REFERENCE.md)
- `histogram`: [CONFIG_HISTOGRAMS](./CONFIG_HISTOGRAMS.md)
- `systematic`: [NUISANCE_GROUPS](./NUISANCE_GROUPS.md)

---

**Last Updated:** 2026-04-26
**Compatibility:** ROOT 6.30+, C++17, optional Python 3.8+
