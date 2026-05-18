# Plugin Development Guide

This guide explains how to build new C++ plugins for RDFAnalyzerCore using the current `Analyzer` and `IPluggableManager` APIs.

## Overview

Create a plugin when you need reusable framework behavior that should live next to the C++ backbone rather than inside one analysis executable.

Good plugin use cases:

- loading and managing configured external resources
- defining reusable RDataFrame columns or actions
- collecting metadata, summaries, or provenance
- managing systematic-variation families shared across analyses

Do not create a plugin for:

- one-off analysis selections or variable definitions
- helper math utilities
- logic that is simpler as ordinary `Analyzer::Define()` code in one analysis

## Plugin Shapes

All plugins inherit from `IPluggableManager`. The main implementation patterns are:

### 1. Named Object Manager

Base class: `NamedObjectManager<T>`

Use this when the plugin owns a keyed collection of configured objects plus associated feature lists.

Examples:

- `BDTManager`
- `OnnxManager`
- `SofieManager`
- `CorrectionManager`
- `TriggerManager`

### 2. Direct Graph/Output Plugin

Base class: `IPluggableManager`

Use this when the plugin mutates the RDF graph, books actions, or writes structured output.

Examples:

- `NDHistogramManager`
- `RegionManager`
- `CutflowManager`
- `WeightManager`
- `GoldenJsonManager`

### 3. Shared Physics-Algorithm Base

Some plugin families use a project-specific intermediate base class.

Examples:

- `ObjectEnergyManagerBase` for electron/photon/tau/muon energy-scale plugins
- corrected object-collection wrappers in `CorrectedObjectCollectionManagers`

These are still ordinary `IPluggableManager` plugins from the framework's perspective.

## Lifecycle Contract

`Analyzer` wires plugins in dependency order.

The current lifecycle is:

1. construction in user code
2. `setContext()`
3. `setupFromConfigFile()`
4. `initialize()`
5. `execute()`
6. `finalize()`
7. `reportMetadata()`
8. `collectProvenanceEntries()`

Use each hook intentionally:

- `setContext()`: store references from `ManagerContext`
- `setupFromConfigFile()`: parse config and stage work
- `initialize()`: validate dependencies and precompute state
- `execute()`: define RDF columns and book lazy actions
- `finalize()`: consume booked results after the event loop
- `reportMetadata()`: emit human-readable summaries
- `collectProvenanceEntries()`: emit stable structured metadata

## Minimal Interface

```cpp
#include <api/IPluggableManager.h>

class IFlagManager : public IPluggableManager {
public:
    virtual ~IFlagManager() = default;
    virtual void defineFlagColumn(const std::string& outputColumn) = 0;
};
```

Notes:

- do not add separate `IContextAware` inheritance; it is already part of `IPluggableManager`
- use `type()`, not `getName()`

## Minimal Plugin Example

```cpp
#include <api/ManagerContext.h>
#include <stdexcept>

class FlagManager : public IFlagManager {
public:
    std::string type() const override { return "FlagManager"; }

    void setContext(ManagerContext& ctx) override {
        ctx_ = &ctx;
    }

    void setupFromConfigFile() override {
        if (!ctx_) {
            throw std::runtime_error("FlagManager: context not set");
        }
        threshold_ = std::stof(ctx_->config.get("flagThreshold"));
    }

    void initialize() override {
        if (threshold_ <= 0.0f) {
            throw std::runtime_error("FlagManager: flagThreshold must be positive");
        }
    }

    void defineFlagColumn(const std::string& outputColumn) override {
        outputColumn_ = outputColumn;
    }

    void execute() override {
        if (!ctx_ || outputColumn_.empty()) {
            return;
        }

        ctx_->data.Define(outputColumn_,
                          [threshold = threshold_](float pt) {
                              return pt > threshold;
                          },
                          {"pt"},
                          ctx_->systematics);
    }

private:
    ManagerContext* ctx_ = nullptr;
    float threshold_ = 0.0f;
    std::string outputColumn_;
};
```

This example uses the actual `IDataFrameProvider::Define()` template that takes the current `ISystematicManager` reference.

## Declaring Dependencies

If your plugin needs another plugin role to exist before initialization, override `getDependencies()`.

```cpp
std::vector<std::string> getDependencies() const override {
    return {"correctionManager", "weightManager"};
}
```

Important:

- dependencies are role names, not C++ type names
- `Analyzer` validates them before initialization
- circular dependency chains throw during wiring

## Accessing The Context

`ManagerContext` contains:

- `config` for configuration reads and parses
- `data` for RDF graph mutation and access
- `systematics` for systematic registration and lookup
- `logger` for diagnostics
- `skimSink` and `metaSink` for output coordination

In current code, many plugins keep raw non-owning pointers copied from `ManagerContext` in `setContext()`.

## Configuration Patterns

Use `IConfigurationProvider` as the canonical API.

Common operations:

```cpp
auto value = ctx_->config.get("myKey");
auto list = ctx_->config.getList("inputColumns");

auto rows = ctx_->config.parseMultiKeyConfig(
    ctx_->config.get("myConfigFile"),
    {"name", "file"});
```

Prefer parsing configuration in `setupFromConfigFile()` and storing strongly typed internal structs rather than repeatedly reparsing config in `execute()`.

## Registration Patterns

### Constructor-Time Bulk Registration

```cpp
std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
plugins["histogramManager"] = std::make_shared<NDHistogramManager>(config);
plugins["regionManager"] = std::make_shared<RegionManager>();

Analyzer analyzer("config.txt", std::move(plugins));
```

### Incremental Registration

```cpp
Analyzer analyzer("config.txt");

analyzer.addPlugin("histogramManager",
    std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider()));
```

Use incremental registration when plugin construction depends on already-created analyzer services.

## Provenance And Metadata

Use `reportMetadata()` for readable summaries and `collectProvenanceEntries()` for stable machine-readable output.

Example:

```cpp
std::unordered_map<std::string, std::string>
collectProvenanceEntries() const override {
    return {
        {"threshold", std::to_string(threshold_)},
        {"output_column", outputColumn_}
    };
}
```

`Analyzer` stores these under `plugin.<role>.*` and computes a deterministic `config_hash` automatically.

## Existing Plugins To Use As References

Recommended reference implementations by pattern:

- `core/plugins/OnnxManager/OnnxManager.h`
- `core/plugins/CorrectionManager/CorrectionManager.h`
- `core/plugins/NDHistogramManager/NDHistogramManager.h`
- `core/plugins/RegionManager/RegionManager.h`
- `core/plugins/CutflowManager/CutflowManager.h`
- `core/plugins/WeightManager/WeightManager.h`
- `core/plugins/JetEnergyScaleManager/JetEnergyScaleManager.h`
- `core/plugins/ObjectEnergyManagerBase/ObjectEnergyManagerBase.h`
- `core/plugins/TaggerWorkingPointManager/TaggerWorkingPointManager.h`

## Multi-Output Patterns

Use existing implementations as the source of truth for output naming conventions.

Examples:

- `OnnxManager`: single-output models create one column; multi-output models create `{modelName}_output0`, `{modelName}_output1`, and so on
- `NDHistogramManager`: books lazy histogram results and saves them in `saveHists()` or `bookConfigHistograms()` workflows
- `CutflowManager`: books count actions in `execute()` and materializes summary histograms in `finalize()`

## Testing Checklist

For a new plugin, add tests under `core/tests/cpp/` and verify:

- dependency validation behavior
- config parsing and missing-key failure modes
- correct column names or output object names
- execution through `Analyzer::run()` or `Analyzer::save()`
- provenance keys when the plugin contributes structured metadata

## Documentation Checklist

For each new plugin or public plugin API change:

- update the public header docstrings first
- update `docs/API_REFERENCE.md` only for stable public usage patterns
- update plugin-specific guides when behavior changes user-facing workflows

## Best Practices

- keep role names explicit and consistent
- prefer staged work in `setupFromConfigFile()` and lazy RDF booking in `execute()`
- treat public headers in `core/interface/`, `core/interface/api/`, and `core/plugins/` as canonical
- avoid undocumented side effects in `initialize()`
- keep provenance entries deterministic and low-noise