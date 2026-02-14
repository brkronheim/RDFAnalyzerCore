# Plugin Development Guide

This guide explains how to add new features to RDFAnalyzerCore by creating custom plugins.

## Table of Contents

- [Overview](#overview)
- [When to Create a Plugin](#when-to-create-a-plugin)
- [Plugin Types](#plugin-types)
- [Step-by-Step: Creating a Plugin](#step-by-step-creating-a-plugin)
- [Advanced Plugin Patterns](#advanced-plugin-patterns)
- [Testing Your Plugin](#testing-your-plugin)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Overview

The plugin system allows you to extend RDFAnalyzerCore with new functionality without modifying the core framework. Plugins are:

- **Modular**: Self-contained with clear interfaces
- **Discoverable**: Accessed via Analyzer's plugin system
- **Configurable**: Driven by text configuration files
- **Reusable**: Can be shared across multiple analyses

## When to Create a Plugin

Create a plugin when you need to:

- **Load and manage external resources** (ML models, correction files, etc.)
- **Apply systematic operations** across multiple analyses
- **Collect metadata** or statistics during the event loop
- **Provide reusable functionality** that fits the manager pattern

**Don't create a plugin for**:
- Analysis-specific variable definitions (use regular Analyzer::Define)
- One-off calculations (use lambda functions)
- Simple helper utilities (use standalone functions)

## Plugin Types

### 1. Named Object Manager

Manages a collection of named objects (models, corrections, etc.).

**Base Class**: `NamedObjectManager<T>`

**Examples**: BDTManager, OnnxManager, CorrectionManager

**When to use**: You need to load, store, and apply multiple configured objects.

### 2. Service Plugin

Performs a specific service during analysis (counting, validation, etc.).

**Base Class**: `IPluggableManager` + `IContextAware`

**Examples**: CounterService, TriggerManager

**When to use**: You need to perform systematic operations or collect information.

### 3. Output Plugin

Manages specialized output or metadata collection.

**Base Class**: `IPluggableManager` + `IContextAware`

**Examples**: NDHistogramManager

**When to use**: You need custom output formats or complex metadata.

## Step-by-Step: Creating a Plugin

Let's create a complete example plugin: **WeightManager** that applies event weights from external JSON files.

### Step 1: Define the Interface

Create `core/plugins/WeightManager/IWeightManager.h`:

```cpp
#pragma once

#include <api/IPluggableManager.h>
#include <string>
#include <vector>

/**
 * Interface for weight management
 */
class IWeightManager : public IPluggableManager {
public:
    virtual ~IWeightManager() = default;
    
    /**
     * Apply a specific weight to the DataFrame
     * @param weightName Name of the weight configuration
     */
    virtual void applyWeight(const std::string& weightName) = 0;
    
    /**
     * Apply all configured weights
     */
    virtual void applyAllWeights() = 0;
    
    /**
     * Get the list of all available weight names
     */
    virtual std::vector<std::string> getWeightNames() const = 0;
    
    /**
     * Check if a weight exists
     */
    virtual bool hasWeight(const std::string& name) const = 0;
};
```

### Step 2: Implement the Manager

Create `core/plugins/WeightManager/WeightManager.h`:

```cpp
#pragma once

#include "IWeightManager.h"
#include <api/IConfigurationProvider.h>
#include <api/IContextAware.h>
#include <api/ManagerContext.h>
#include <unordered_map>
#include <memory>

// External library for reading weights (hypothetical)
#include <nlohmann/json.hpp>

/**
 * Manager for applying event weights from JSON configurations
 */
class WeightManager : public IWeightManager, public IContextAware {
public:
    /**
     * Constructor
     * @param config Configuration provider
     */
    explicit WeightManager(IConfigurationProvider& config);
    
    // IPluggableManager interface
    void initialize() override;
    void finalize() override;
    std::string getName() const override { return "WeightManager"; }
    
    // IContextAware interface
    void setContext(const ManagerContext& ctx) override;
    
    // IWeightManager interface
    void applyWeight(const std::string& weightName) override;
    void applyAllWeights() override;
    std::vector<std::string> getWeightNames() const override;
    bool hasWeight(const std::string& name) const override;

private:
    struct WeightConfig {
        std::string name;           // Output column name
        std::string file;           // JSON file path
        std::string weightKey;      // Key in JSON
        std::vector<std::string> inputVariables;  // Input column names
    };
    
    // Load weight configurations from config file
    void loadWeights();
    
    // Parse a single weight configuration
    WeightConfig parseWeightConfig(const std::map<std::string, std::string>& cfg);
    
    // Load JSON weight file
    nlohmann::json loadWeightFile(const std::string& path);
    
    IConfigurationProvider* config_;
    ManagerContext* context_;
    
    std::unordered_map<std::string, WeightConfig> weights_;
    std::unordered_map<std::string, nlohmann::json> loadedFiles_;
};
```

### Step 3: Implement the Manager

Create `core/plugins/WeightManager/WeightManager.cc`:

```cpp
#include "WeightManager.h"
#include <fstream>
#include <stdexcept>
#include <sstream>

WeightManager::WeightManager(IConfigurationProvider& config)
    : config_(&config), context_(nullptr) {
    loadWeights();
}

void WeightManager::setContext(const ManagerContext& ctx) {
    context_ = const_cast<ManagerContext*>(&ctx);
}

void WeightManager::initialize() {
    if (!context_) {
        throw std::runtime_error("WeightManager: Context not set");
    }
    context_->logger.info("WeightManager initialized with " + 
                         std::to_string(weights_.size()) + " weights");
}

void WeightManager::finalize() {
    context_->logger.info("WeightManager finalized");
}

void WeightManager::loadWeights() {
    if (!config_->has("weightConfig")) {
        return;  // No weights configured
    }
    
    std::string weightConfigFile = config_->get("weightConfig");
    auto weightConfigs = config_->getMultiKeyConfigs(weightConfigFile);
    
    for (const auto& cfg : weightConfigs) {
        WeightConfig wc = parseWeightConfig(cfg);
        weights_[wc.name] = wc;
        
        // Load JSON file if not already loaded
        if (loadedFiles_.find(wc.file) == loadedFiles_.end()) {
            loadedFiles_[wc.file] = loadWeightFile(wc.file);
        }
    }
}

WeightManager::WeightConfig WeightManager::parseWeightConfig(
    const std::map<std::string, std::string>& cfg) {
    
    WeightConfig wc;
    wc.name = cfg.at("name");
    wc.file = cfg.at("file");
    wc.weightKey = cfg.at("weightKey");
    
    // Parse comma-separated input variables
    std::string inputVarsStr = cfg.at("inputVariables");
    std::stringstream ss(inputVarsStr);
    std::string var;
    while (std::getline(ss, var, ',')) {
        wc.inputVariables.push_back(var);
    }
    
    return wc;
}

nlohmann::json WeightManager::loadWeightFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open weight file: " + path);
    }
    
    nlohmann::json j;
    file >> j;
    return j;
}

void WeightManager::applyWeight(const std::string& weightName) {
    auto it = weights_.find(weightName);
    if (it == weights_.end()) {
        throw std::runtime_error("Weight not found: " + weightName);
    }
    
    const WeightConfig& wc = it->second;
    const nlohmann::json& weightData = loadedFiles_.at(wc.file);
    
    // Create lambda that captures weight data
    auto weightFunc = [weightData, wc](double pt, double eta) -> double {
        // Look up weight in JSON based on pt/eta
        // (Simplified example - real implementation would be more complex)
        std::string key = std::to_string(int(pt / 10)) + "_" + 
                         std::to_string(int(eta * 10));
        
        if (weightData[wc.weightKey].contains(key)) {
            return weightData[wc.weightKey][key].get<double>();
        }
        return 1.0;  // Default weight
    };
    
    // Apply weight to DataFrame
    context_->dataManager.Define(
        wc.name,
        weightFunc,
        wc.inputVariables,
        context_->systematicManager
    );
    
    context_->logger.info("Applied weight: " + weightName);
}

void WeightManager::applyAllWeights() {
    for (const auto& [name, _] : weights_) {
        applyWeight(name);
    }
}

std::vector<std::string> WeightManager::getWeightNames() const {
    std::vector<std::string> names;
    for (const auto& [name, _] : weights_) {
        names.push_back(name);
    }
    return names;
}

bool WeightManager::hasWeight(const std::string& name) const {
    return weights_.find(name) != weights_.end();
}
```

### Step 4: Create CMakeLists.txt

Create `core/plugins/WeightManager/CMakeLists.txt`:

```cmake
# WeightManager Plugin

add_library(WeightManager SHARED
    WeightManager.cc
)

target_link_libraries(WeightManager
    PRIVATE
    RDFCore
    # Add any external dependencies (e.g., nlohmann_json)
)

target_include_directories(WeightManager
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Install the library
install(TARGETS WeightManager
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

# Install headers
install(FILES
    WeightManager.h
    IWeightManager.h
    DESTINATION include/plugins/WeightManager
)
```

### Step 5: Register in Core Build

Add to `core/plugins/CMakeLists.txt`:

```cmake
add_subdirectory(WeightManager)

# Link to core
target_link_libraries(RDFCore PUBLIC WeightManager)
```

### Step 6: Create Configuration Format

Document the configuration format. Create `docs/WEIGHT_MANAGER.md`:

```markdown
# WeightManager Configuration

## Config File Format

Create a weight configuration file (e.g., `cfg/weights.txt`):

```
file=path/to/weights.json weightKey=scale_factors name=my_weight inputVariables=pt,eta
```

Add to main config:
```
weightConfig=cfg/weights.txt
```

## JSON Weight File Format

```json
{
  "scale_factors": {
    "pt_eta_bin": weight_value,
    "20_0": 1.05,
    "20_5": 1.03,
    ...
  }
}
```
```

### Step 7: Use in Analysis

In your analysis code:

```cpp
#include <analyzer.h>
#include <plugins/WeightManager/IWeightManager.h>

int main(int argc, char** argv) {
    // Create analyzer with plugin
    Analyzer analyzer(argv[1]);
    
    // Define input variables
    analyzer.Define("pt", ...);
    analyzer.Define("eta", ...);
    
    // Get and use plugin
    auto* weightMgr = analyzer.getPlugin<IWeightManager>("weight");
    if (weightMgr) {
        weightMgr->applyAllWeights();
        // Now "my_weight" column exists
    }
    
    // Use the weight
    analyzer.Define("total_weight",
        [](double gen_weight, double my_weight) {
            return gen_weight * my_weight;
        },
        {"genWeight", "my_weight"}
    );
    
    analyzer.save();
    return 0;
}
```

## Advanced Plugin Patterns

### Pattern 1: Deferred Application

Load configurations at construction, but defer DataFrame operations:

```cpp
class MyManager {
public:
    MyManager(IConfigurationProvider& config) {
        // Load configs early
        loadConfigs();
    }
    
    void applyModel(const std::string& name) {
        // Apply to DataFrame later
        context_->dataManager.Define(...);
    }
};
```

**Why**: Allows user to control when operations are applied, useful for dependency management.

### Pattern 2: Multi-Output Operations

Create multiple output columns from one operation:

```cpp
void MyManager::applyMultiOutput(const std::string& name) {
    // Define multiple outputs
    context_->dataManager.Define(name + "_output0", ...);
    context_->dataManager.Define(name + "_output1", ...);
    context_->dataManager.Define(name + "_output2", ...);
    
    // Optional: Create aggregate column
    context_->dataManager.Define(name + "_all",
        [](float out0, float out1, float out2) {
            return std::vector<float>{out0, out1, out2};
        },
        {name + "_output0", name + "_output1", name + "_output2"}
    );
}
```

### Pattern 3: Conditional Execution

Skip expensive operations based on a condition:

```cpp
void MyManager::applyConditional(const std::string& name, 
                                 const std::string& runVar) {
    context_->dataManager.Define(name,
        [this, name](bool should_run, auto&&... inputs) {
            if (!should_run) return -1.0f;  // Skip
            return this->expensiveComputation(inputs...);
        },
        {runVar, /* input columns */}
    );
}
```

### Pattern 4: Systematic Variations

Support systematic uncertainties:

```cpp
void MyManager::applyWithSystematics(const std::string& name) {
    // Get systematic variations
    auto systematics = context_->systematicManager.getSystematicNames();
    
    for (const auto& sys : systematics) {
        std::string varName = name + "_" + sys;
        
        context_->dataManager.Define(varName,
            [sys](auto&&... inputs) {
                // Apply systematic variation
                if (sys == "up") return compute(inputs...) * 1.1;
                if (sys == "down") return compute(inputs...) * 0.9;
                return compute(inputs...);  // Nominal
            },
            {/* input columns */}
        );
    }
}
```

### Pattern 5: Metadata Collection

Collect information during finalize:

```cpp
class MetadataCollector : public IPluggableManager, public IContextAware {
public:
    void collectData() {
        // Store data for later
        metadata_["event_count"] = event_count_;
        metadata_["sum_weights"] = sum_weights_;
    }
    
    void finalize() override {
        // Write to output
        TTree* tree = new TTree("metadata", "Analysis Metadata");
        for (const auto& [key, value] : metadata_) {
            tree->Branch(key.c_str(), &value);
        }
        tree->Fill();
        
        context_->metaSink.writeObject(tree, "metadata_tree");
    }

private:
    std::map<std::string, double> metadata_;
    int event_count_ = 0;
    double sum_weights_ = 0.0;
};
```

### Pattern 6: Thread-Safe Operations

Ensure thread safety for parallel processing:

```cpp
class ThreadSafeManager {
public:
    void applyOperation(const std::string& name) {
        // Capture thread-safe objects
        auto sharedResource = std::make_shared<ThreadSafeResource>();
        
        context_->dataManager.Define(name,
            [sharedResource](auto&&... inputs) {
                // Each thread gets a copy of the shared_ptr
                // Resource is thread-safe internally
                return sharedResource->compute(inputs...);
            },
            {/* columns */}
        );
    }
};
```

## Testing Your Plugin

### Unit Tests

Create `core/plugins/WeightManager/test_WeightManager.cc`:

```cpp
#include <gtest/gtest.h>
#include "WeightManager.h"
#include <ConfigurationManager.h>

TEST(WeightManagerTest, Construction) {
    ConfigurationManager config("test_config.txt");
    WeightManager mgr(config);
    EXPECT_EQ(mgr.getName(), "WeightManager");
}

TEST(WeightManagerTest, LoadWeights) {
    ConfigurationManager config("test_config.txt");
    WeightManager mgr(config);
    
    EXPECT_TRUE(mgr.hasWeight("test_weight"));
    EXPECT_FALSE(mgr.hasWeight("nonexistent"));
}

TEST(WeightManagerTest, GetWeightNames) {
    ConfigurationManager config("test_config.txt");
    WeightManager mgr(config);
    
    auto names = mgr.getWeightNames();
    EXPECT_EQ(names.size(), 1);
    EXPECT_EQ(names[0], "test_weight");
}

TEST(WeightManagerTest, ApplyWeight) {
    // Create test configuration
    ConfigurationManager config("test_config.txt");
    WeightManager mgr(config);
    
    // Create mock context
    // ... set up DataManager, etc.
    
    EXPECT_NO_THROW(mgr.applyWeight("test_weight"));
}
```

### Integration Tests

Test with full analysis workflow:

```cpp
TEST(WeightManagerIntegrationTest, EndToEnd) {
    // Create analyzer with plugin
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
    plugins["weight"] = std::make_unique<WeightManager>(config);
    
    Analyzer analyzer("test_config.txt", std::move(plugins));
    
    // Define variables
    analyzer.Define("pt", ...);
    analyzer.Define("eta", ...);
    
    // Apply weight
    auto* weightMgr = analyzer.getPlugin<IWeightManager>("weight");
    ASSERT_NE(weightMgr, nullptr);
    weightMgr->applyAllWeights();
    
    // Check output exists
    auto df = analyzer.getDataFrame();
    EXPECT_TRUE(df.HasColumn("my_weight"));
}
```

### Test Configuration Files

Create `core/test/cfg/test_weights.txt`:

```
file=core/test/cfg/test_weight_data.json weightKey=factors name=test_weight inputVariables=pt,eta
```

Create `core/test/cfg/test_weight_data.json`:

```json
{
  "factors": {
    "20_0": 1.05,
    "30_5": 1.03
  }
}
```

### Running Tests

```bash
cd build
ctest -R WeightManager -V
```

## Best Practices

### 1. Interface Design

- **Keep interfaces minimal**: Only expose necessary methods
- **Use const correctness**: Mark read-only methods as const
- **Return by const reference**: Avoid unnecessary copies
- **Provide query methods**: `hasX()`, `getXNames()`, etc.

### 2. Configuration Handling

- **Validate early**: Check configuration in constructor
- **Provide clear errors**: Include context in error messages
- **Support optional configs**: Gracefully handle missing configs
- **Document format**: Create clear documentation for config syntax

### 3. Resource Management

- **Use RAII**: Let destructors clean up resources
- **Prefer smart pointers**: Use `unique_ptr` and `shared_ptr`
- **Avoid raw pointers**: Except for non-owning references
- **Clean up in finalize**: Release resources before destruction

### 4. Error Handling

- **Throw on construction errors**: Don't create invalid objects
- **Throw on usage errors**: Don't silently fail
- **Log important events**: Use the logger for informational messages
- **Provide context**: Include relevant information in exceptions

### 5. Thread Safety

- **Document thread safety**: Clearly state if operations are thread-safe
- **Use thread-safe containers**: Or add synchronization
- **Avoid shared mutable state**: Prefer immutable or thread-local
- **Test with ImplicitMT**: Verify behavior with parallel execution

### 6. Performance

- **Load once, use many**: Load resources in constructor
- **Cache expensive operations**: Store computed results
- **Lazy evaluation**: Defer work until needed
- **Minimize copies**: Use references and moves

### 7. Documentation

- **Document interface**: Clear docstrings for all public methods
- **Provide examples**: Show typical usage patterns
- **Explain configuration**: Document all config options
- **Write user guide**: Create markdown documentation

## Examples

### Example 1: Simple Service Plugin

```cpp
// Header
class ValidationService : public IPluggableManager, public IContextAware {
public:
    void initialize() override;
    void finalize() override;
    std::string getName() const override { return "ValidationService"; }
    void setContext(const ManagerContext& ctx) override { context_ = &ctx; }
    
    void validateColumn(const std::string& column, 
                       std::function<bool(double)> validator);

private:
    ManagerContext* context_;
    std::vector<std::string> failures_;
};

// Implementation
void ValidationService::validateColumn(
    const std::string& column,
    std::function<bool(double)> validator) {
    
    auto df = context_->dataManager.getDataFrame();
    
    auto validationResult = df.Define("__valid__",
        [validator](double value) { return validator(value); },
        {column}
    ).Filter([](bool valid) { return valid; }, {"__valid__"});
    
    auto passed = validationResult.Count();
    auto total = df.Count();
    
    if (*passed != *total) {
        std::string msg = "Validation failed for " + column + 
                         ": " + std::to_string(*passed) + "/" + 
                         std::to_string(*total) + " passed";
        context_->logger.warn(msg);
        failures_.push_back(msg);
    }
}

void ValidationService::finalize() {
    if (!failures_.empty()) {
        context_->logger.error("Validation failures occurred:");
        for (const auto& failure : failures_) {
            context_->logger.error("  " + failure);
        }
    }
}
```

### Example 2: Named Object Manager

```cpp
// Simple scale factor manager
class ScaleFactorManager : public NamedObjectManager<std::function<double(double, double)>> {
public:
    ScaleFactorManager(IConfigurationProvider& config);
    
    void loadObjects(const std::vector<std::map<std::string, std::string>>& configs) override;
    void applyScaleFactor(const std::string& name);
    void applyAllScaleFactors();

private:
    std::unordered_map<std::string, std::vector<std::string>> inputs_;
};

void ScaleFactorManager::loadObjects(
    const std::vector<std::map<std::string, std::string>>& configs) {
    
    for (const auto& cfg : configs) {
        std::string name = cfg.at("name");
        
        // Create scale factor function
        // (Simplified - real implementation would load from file)
        auto sfFunc = [](double pt, double eta) {
            return 0.95 + 0.001 * pt;  // Example
        };
        
        objects_[name] = sfFunc;
        
        // Store input variable names
        std::string inputStr = cfg.at("inputVariables");
        inputs_[name] = parseCommaSeparated(inputStr);
    }
}

void ScaleFactorManager::applyScaleFactor(const std::string& name) {
    auto sfFunc = getObject(name);
    const auto& inputs = inputs_.at(name);
    
    context_->dataManager.Define(name, sfFunc, inputs, 
                                context_->systematicManager);
}
```

## Common Pitfalls

### 1. Not Calling setContext

**Problem**: Context is null when trying to use it.

**Solution**: Ensure Analyzer calls setContext before initialize.

### 2. Applying Before Inputs Exist

**Problem**: DataFrame column doesn't exist when defining operation.

**Solution**: Document that input variables must be defined first, or defer operation.

### 3. Non-Thread-Safe Operations

**Problem**: Crashes or wrong results with ImplicitMT enabled.

**Solution**: Use thread-safe objects or synchronization.

### 4. Memory Leaks

**Problem**: Resources not cleaned up.

**Solution**: Use RAII, smart pointers, and implement finalize correctly.

### 5. Missing Error Handling

**Problem**: Silent failures or cryptic error messages.

**Solution**: Validate inputs and provide clear error messages.

## Next Steps

- **Study existing plugins**: Look at OnnxManager, BDTManager for examples
- **Read architecture docs**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Understand interfaces**: [API_REFERENCE.md](API_REFERENCE.md)
- **Build something**: Start with a simple plugin and iterate

Happy plugin development!
