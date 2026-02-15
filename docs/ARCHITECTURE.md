# Architecture and Internals

This document explains how RDFAnalyzerCore works internally, its design principles, and how different components interact.

## Table of Contents

- [Design Philosophy](#design-philosophy)
- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Plugin System](#plugin-system)
- [Data Flow](#data-flow)
- [Manager Lifecycle](#manager-lifecycle)
- [Build System](#build-system)
- [Advanced Features](#advanced-features)

## Design Philosophy

RDFAnalyzerCore is built around several key principles:

### 1. Plugin-Based Architecture

**Goal**: Extensibility without modifying core code.

- All managers (BDT, ONNX, Correction, etc.) are plugins
- Analyzer is agnostic to specific plugin types
- New plugins can be added without changing Analyzer
- Plugins interact through well-defined interfaces

### 2. Configuration-Driven Behavior

**Goal**: Separate configuration from code.

- All runtime behavior controlled by text files
- Same binary can run different analyses
- Configuration is versioned alongside code
- No recompilation needed for config changes

### 3. Interface-Based Design

**Goal**: Loose coupling and testability.

- Components depend on interfaces, not implementations
- Enables mocking for unit tests
- Supports multiple implementations of same interface
- Clear contracts between components

### 4. Compile-Time Wiring

**Goal**: Explicit dependencies and type safety.

- Plugins are created and wired in C++
- No runtime type registration or reflection
- Compiler enforces correct usage
- Minimal runtime overhead for plugin system

### 5. Framework vs. Library

**Goal**: Provide structure while allowing flexibility.

- Framework provides the structure (Analyzer, managers, flow)
- User code provides analysis-specific logic
- Framework handles orchestration, user handles physics
- Inversion of control pattern

## Architecture Overview

### High-Level Structure

```
┌─────────────────────────────────────────────────────────┐
│                    User Analysis Code                    │
│                                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Analyzer (Facade)                   │   │
│  │  ┌────────────────────────────────────────┐     │   │
│  │  │     Core Managers                       │     │   │
│  │  │  • ConfigurationManager                 │     │   │
│  │  │  • DataManager (RDataFrame wrapper)     │     │   │
│  │  │  • SystematicManager                    │     │   │
│  │  │  • Logger                               │     │   │
│  │  │  • OutputSinks (Skim, Meta)            │     │   │
│  │  └────────────────────────────────────────┘     │   │
│  │                                                   │   │
│  │  ┌────────────────────────────────────────┐     │   │
│  │  │     Plugin Managers (Optional)          │     │   │
│  │  │  • BDTManager                           │     │   │
│  │  │  • OnnxManager                          │     │   │
│  │  │  • SofieManager                         │     │   │
│  │  │  • CorrectionManager                    │     │   │
│  │  │  • TriggerManager                       │     │   │
│  │  │  • NDHistogramManager                   │     │   │
│  │  │  • Custom plugins...                    │     │   │
│  │  └────────────────────────────────────────┘     │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
                 ┌──────────────────┐
                 │  ROOT RDataFrame  │
                 │   (Lazy Execution)│
                 └──────────────────┘
```

### Component Relationships

```
User Analysis
      │
      ├─► Analyzer (Facade)
      │       │
      │       ├─► ConfigurationManager (reads config files)
      │       ├─► DataManager (wraps RDataFrame)
      │       ├─► SystematicManager (tracks variations)
      │       ├─► Logger (logging output)
      │       ├─► OutputSinks (skim, metadata)
      │       └─► Plugin Map
      │               │
      │               ├─► BDTManager ──────► NamedObjectManager<BDT>
      │               ├─► OnnxManager ─────► NamedObjectManager<Session>
      │               ├─► CorrectionManager ► NamedObjectManager<Correction>
      │               └─► ... (other plugins)
      │
      └─► Define(), Filter(), ... (analysis logic)
```

## Core Components

### Analyzer

**Location**: `core/src/analyzer.cc`, `core/interface/analyzer.h`

**Purpose**: Central orchestrator and facade for the framework.

**Responsibilities**:
- Manage lifecycle of all managers
- Provide simplified API to user code
- Coordinate plugin execution
- Handle output writing

**Key Methods**:
```cpp
class Analyzer {
public:
    // Construction
    Analyzer(const std::string& configFile, 
             std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins = {});
    
    // Analysis operations
    Analyzer* Define(const std::string& name, ...);
    Analyzer* Filter(const std::string& name, ...);
    Analyzer* DefinePerSample(...);
    
    // Plugin access
    template<typename T>
    T* getPlugin(const std::string& role);
    
    // Execution
    void save();
    
    // Access to underlying managers
    IConfigurationProvider* getConfigProvider();
    IDataFrameProvider* getDataManager();
    ISystematicManager* getSystematicManager();
};
```

**Design Notes**:
- Returns `this` for method chaining: `analyzer.Define(...)->Filter(...)->Define(...)`
- Template method `getPlugin<T>` provides type-safe plugin access
- Owns all managers via unique_ptr (RAII)

### ConfigurationManager

**Location**: `core/src/ConfigurationManager.cc`, `core/interface/ConfigurationManager.h`

**Purpose**: Load and provide access to configuration values.

**Responsibilities**:
- Parse configuration files (key=value format)
- Support nested configurations (plugin configs)
- Provide type-safe value retrieval
- Handle defaults and missing keys

**Key Interface** (`IConfigurationProvider`):
```cpp
class IConfigurationProvider {
public:
    virtual std::string get(const std::string& key) const = 0;
    virtual bool has(const std::string& key) const = 0;
    virtual std::map<std::string, std::string> getAll() const = 0;
    
    // Specialized parsers
    virtual std::vector<std::pair<std::string, std::string>> 
        getPairs(const std::string& configFile) const = 0;
    virtual std::vector<std::map<std::string, std::string>> 
        getMultiKeyConfigs(const std::string& configFile) const = 0;
};
```

**Configuration Adapter Pattern**:
```
ConfigurationManager ──uses──► IConfigAdapter (interface)
                                       │
                                       ├─► TextConfigAdapter (text files)
                                       └─► (Future: JsonConfigAdapter, etc.)
```

### DataManager

**Location**: `core/src/DataManager.cc`, `core/interface/DataManager.h`

**Purpose**: Manage ROOT RDataFrame and column definitions.

**Responsibilities**:
- Construct RDataFrame from input files
- Track current dataframe node (after Define/Filter operations)
- Provide dataframe manipulation methods
- Handle systematic variations in definitions

**Key Interface** (`IDataFrameProvider`):
```cpp
class IDataFrameProvider {
public:
    virtual ROOT::RDF::RNode& getDataFrame() = 0;
    virtual void Define(const std::string& name, ...) = 0;
    virtual void Filter(...) = 0;
    virtual void DefinePerSample(...) = 0;
    virtual void DefineVector(...) = 0;
    // ... other RDataFrame operations
};
```

**Node Tracking**:
```cpp
// Internally, DataManager tracks the current node
ROOT::RDF::RNode currentNode;

// Each Define/Filter returns a new node
currentNode = currentNode.Define("var", ...);
currentNode = currentNode.Filter(...);

// User always gets the current node
auto& df = dataManager->getDataFrame();  // Returns currentNode
```

### SystematicManager

**Location**: `core/src/SystematicManager.cc`, `core/interface/SystematicManager.h`

**Purpose**: Track systematic variations and propagate through analysis.

**Responsibilities**:
- Register systematic variation names
- Generate per-sample systematic indices
- Provide lists of active variations
- Support systematic-aware variable definitions

**Key Interface** (`ISystematicManager`):
```cpp
class ISystematicManager {
public:
    virtual void registerSystematic(const std::string& name) = 0;
    virtual std::vector<std::string> getSystematicNames() const = 0;
    virtual bool hasSystematic(const std::string& name) const = 0;
    // ... per-sample systematic handling
};
```

**Usage Pattern**:
```cpp
// Register variations
sysMgr->registerSystematic("jes_up");
sysMgr->registerSystematic("jes_down");

// Define variable with systematic awareness
dataManager->Define("corrected_pt",
    [](float pt, const std::string& systematic) {
        if (systematic == "jes_up") return pt * 1.02;
        if (systematic == "jes_down") return pt * 0.98;
        return pt;
    },
    {"jet_pt"},
    *sysMgr  // Systematic manager propagates variations
);
```

### OutputSink

**Location**: `core/interface/api/IOutputSink.h`, `core/src/RootOutputSink.cc`

**Purpose**: Abstract output destinations for skims and metadata.

**Responsibilities**:
- Write event-level data (skims)
- Write histograms and metadata
- Support different output formats

**Interface**:
```cpp
class IOutputSink {
public:
    virtual void write(const std::string& treeName, 
                      const ROOT::RDF::RNode& df,
                      const std::vector<std::string>& columns) = 0;
    virtual void writeObject(TObject* obj, const std::string& name) = 0;
    virtual TFile* getFile() = 0;
};
```

**Implementations**:
- `RootOutputSink`: Write to ROOT files
- `NullOutputSink`: Discard output (for testing)

### Logger

**Location**: `core/interface/api/ILogger.h`, `core/interface/DefaultLogger.h`

**Purpose**: Provide logging abstraction.

**Interface**:
```cpp
class ILogger {
public:
    virtual void log(const std::string& message, 
                    LogLevel level = LogLevel::INFO) = 0;
    virtual void error(const std::string& message) = 0;
    virtual void warn(const std::string& message) = 0;
    virtual void info(const std::string& message) = 0;
    virtual void debug(const std::string& message) = 0;
};
```

## Plugin System

### Plugin Architecture

The plugin system enables extensibility without modifying Analyzer.

#### Core Interfaces

**IPluggableManager** - Base interface for all plugins:
```cpp
class IPluggableManager {
public:
    virtual ~IPluggableManager() = default;
    virtual void initialize() = 0;
    virtual void finalize() = 0;
    virtual std::string getName() const = 0;
};
```

**IContextAware** - Plugins that need access to core managers:
```cpp
struct ManagerContext {
    IConfigurationProvider& config;
    IDataFrameProvider& dataManager;
    ISystematicManager& systematicManager;
    ILogger& logger;
    IOutputSink& skimSink;
    IOutputSink& metaSink;
};

class IContextAware {
public:
    virtual void setContext(const ManagerContext& ctx) = 0;
};
```

#### Plugin Base Classes

**NamedObjectManager<T>** - Base class for plugins managing named objects:

```cpp
template<typename T>
class NamedObjectManager : public IPluggableManager, public IContextAware {
protected:
    std::unordered_map<std::string, T> objects_;
    ManagerContext* context_;
    
public:
    virtual void loadObjects(const std::vector<std::map<std::string, std::string>>& configs) = 0;
    T getObject(const std::string& key) const;
    std::vector<std::string> getAllKeys() const;
    // ...
};
```

**Used by**:
- `BDTManager` - manages `FastForest` objects
- `OnnxManager` - manages `Ort::Session` objects
- `SofieManager` - manages `SofieInferenceFunction` objects
- `CorrectionManager` - manages `correctionlib::Correction` objects

### Plugin Lifecycle

1. **Construction**: Plugins are created in user code
   ```cpp
   auto onnxMgr = std::make_unique<OnnxManager>(*config);
   ```

2. **Context Setting**: Plugins receive references to core managers
   ```cpp
   ManagerContext ctx{config, dataManager, sysMgr, logger, skimSink, metaSink};
   onnxMgr->setContext(ctx);
   ```

3. **Registration**: Plugins are registered with Analyzer
   ```cpp
   plugins["onnx"] = std::move(onnxMgr);
   Analyzer analyzer(configFile, std::move(plugins));
   ```

4. **Initialization**: Framework calls `initialize()` on all plugins
   ```cpp
   // In Analyzer constructor
   for (auto& [role, plugin] : plugins_) {
       plugin->initialize();
   }
   ```

5. **Usage**: Analysis code retrieves and uses plugins
   ```cpp
   auto* onnx = analyzer.getPlugin<IOnnxManager>("onnx");
   onnx->applyAllModels();
   ```

6. **Finalization**: Framework calls `finalize()` on all plugins
   ```cpp
   // In Analyzer destructor or save()
   for (auto& [role, plugin] : plugins_) {
       plugin->finalize();
   }
   ```

### Plugin Discovery and Access

**Type-Safe Access**:
```cpp
template<typename T>
T* Analyzer::getPlugin(const std::string& role) {
    auto it = plugins_.find(role);
    if (it == plugins_.end()) return nullptr;
    return dynamic_cast<T*>(it->second.get());
}
```

**Usage**:
```cpp
// Request specific interface
auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");
if (onnxMgr) {
    onnxMgr->applyModel("my_model");
}
```

### Common Plugin Patterns

#### 1. Configuration-Loaded Objects

Plugins like BDTManager and OnnxManager load objects from configuration:

```cpp
class OnnxManager : public NamedObjectManager<std::shared_ptr<Ort::Session>> {
public:
    OnnxManager(IConfigurationProvider& config) {
        // Read onnxConfig from main config
        if (config.has("onnxConfig")) {
            auto modelConfigs = config.getMultiKeyConfigs(
                config.get("onnxConfig"));
            loadObjects(modelConfigs);
        }
    }
    
protected:
    void loadObjects(const std::vector<std::map<...>>& configs) override {
        for (const auto& cfg : configs) {
            // Load ONNX model
            auto session = std::make_shared<Ort::Session>(...);
            objects_[cfg.at("name")] = session;
            // Store metadata
            features_[cfg.at("name")] = parseFeatures(cfg.at("inputVariables"));
            runVars_[cfg.at("name")] = cfg.at("runVar");
        }
    }
};
```

#### 2. DataFrame Augmentation

Plugins that add columns to the dataframe:

```cpp
void OnnxManager::applyModel(const std::string& modelName) {
    auto session = getObject(modelName);
    const auto& features = features_.at(modelName);
    const auto& runVar = runVars_.at(modelName);
    
    // Add column via DataManager
    context_->dataManager.Define(modelName,
        [session, features, runVar](auto&&... args) {
            if (!runVar_value) return -1.0f;
            // Run inference
            return runInference(session, features, args...);
        },
        features
    );
}
```

#### 3. Metadata Collection

Plugins that collect information for output:

```cpp
class CounterService : public IPluggableManager {
public:
    void finalize() override {
        // Write counters to metadata sink
        TH1D* hist = new TH1D("counters", "Event Counts", ...);
        for (const auto& [sample, count] : counters_) {
            hist->Fill(sample.c_str(), count);
        }
        context_->metaSink.writeObject(hist, "event_counters");
    }
};
```

## Data Flow

### Typical Analysis Flow

```
1. Construction
   ├─► Parse configuration file
   ├─► Create DataManager (build TChain)
   ├─► Create SystematicManager
   ├─► Create plugins
   └─► Build RDataFrame

2. Variable Definitions
   ├─► Define base variables
   ├─► Load plugin configurations
   ├─► Apply aliases
   └─► Register optional branches

3. Event Selection
   ├─► Apply trigger filters
   ├─► Apply physics cuts
   └─► Define regions/categories

4. Plugin Application
   ├─► Apply BDT/ONNX models
   ├─► Apply corrections
   └─► Book histograms

5. Execution Trigger
   ├─► Call analyzer.save() or
   ├─► Access histogram pointers or
   └─► Trigger other RDataFrame actions

6. Lazy Evaluation
   ├─► RDataFrame event loop executes
   ├─► All Define/Filter/Apply operations happen
   └─► Results are computed

7. Output Writing
   ├─► Write skim to saveFile
   ├─► Write histograms to metaFile
   └─► Call plugin finalize() methods

8. Cleanup
   └─► RAII destroys all managers
```

### RDataFrame Integration

The framework wraps RDataFrame's lazy execution model:

```cpp
// User code
analyzer.Define("var1", ...);    // Returns Analyzer*, queues operation
analyzer.Filter("cut1", ...);    // Returns Analyzer*, queues operation
analyzer.Define("var2", ...);    // Returns Analyzer*, queues operation

// Internally, DataManager:
currentNode_ = currentNode_.Define("var1", ...);  // Returns new RNode
currentNode_ = currentNode_.Filter("cut1", ...);  // Returns new RNode
currentNode_ = currentNode_.Define("var2", ...);  // Returns new RNode

// Execution
analyzer.save();
// Triggers: currentNode_.Snapshot(...) 
// RDataFrame event loop runs now
```

### Plugin Integration Points

Plugins integrate at specific points in the flow:

1. **Construction**: Load configurations
2. **Pre-Execution**: Apply models, book histograms
3. **During Event Loop**: Define operations execute
4. **Post-Execution**: Write metadata, counters

```cpp
// Pre-execution
auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");
onnxMgr->applyAllModels();  // Adds Define operations to dataframe

// Execution
analyzer.save();  // Triggers event loop

// Post-execution
// In Analyzer::save():
for (auto& [role, plugin] : plugins_) {
    plugin->finalize();  // Plugins write metadata
}
```

## Manager Lifecycle

### Creation and Initialization

```cpp
// 1. User creates Analyzer
Analyzer analyzer("config.txt");

// Inside Analyzer constructor:
//   a. Create ConfigurationManager
config_ = ManagerFactory::createConfigurationManager(configFile);

//   b. Create DataManager
dataManager_ = ManagerFactory::createDataManager(*config_);

//   c. Create SystematicManager
sysMgr_ = ManagerFactory::createSystematicManager();

//   d. Create OutputSinks
skimSink_ = std::make_unique<RootOutputSink>(config_->get("saveFile"));
metaSink_ = std::make_unique<RootOutputSink>(config_->get("metaFile"));

//   e. Create Logger
logger_ = std::make_unique<DefaultLogger>();

//   f. Initialize plugins
for (auto& [role, plugin] : plugins_) {
    // Set context if plugin is context-aware
    if (auto* ctxAware = dynamic_cast<IContextAware*>(plugin.get())) {
        ManagerContext ctx{*config_, *dataManager_, *sysMgr_, *logger_, 
                          *skimSink_, *metaSink_};
        ctxAware->setContext(ctx);
    }
    // Initialize
    plugin->initialize();
}
```

### Usage Phase

```cpp
// 2. User defines analysis
analyzer.Define("myvar", ...);
analyzer.Filter("mycut", ...);

// Get and use plugins
auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");
onnxMgr->applyModel("model");
```

### Execution and Cleanup

```cpp
// 3. Trigger execution
analyzer.save();

// Inside Analyzer::save():
//   a. Finalize plugins
for (auto& [role, plugin] : plugins_) {
    plugin->finalize();
}

//   b. Write skim
auto columns = getOutputColumns();
skimSink_->write("Events", dataManager_->getDataFrame(), columns);

//   c. Close output files
skimSink_->close();
metaSink_->close();

// 4. Destruction (RAII)
// Analyzer destructor runs
// All unique_ptrs are destroyed in reverse order
```

## Build System

### CMake Structure

```
RDFAnalyzerCore/
├── CMakeLists.txt           # Top-level CMake
├── cmake/
│   ├── SetupOnnxRuntime.cmake   # ONNX Runtime download
│   └── ... (other CMake modules)
├── core/
│   ├── CMakeLists.txt       # Core library build
│   ├── plugins/
│   │   ├── BDTManager/
│   │   │   └── CMakeLists.txt
│   │   ├── OnnxManager/
│   │   │   └── CMakeLists.txt
│   │   └── ... (other plugins)
│   └── test/
│       └── CMakeLists.txt
└── analyses/
    ├── CMakeLists.txt       # Analysis discovery
    └── ExampleAnalysis/
        └── CMakeLists.txt   # Specific analysis
```

### Build Process

1. **Configure**:
   ```bash
   cmake -S . -B build
   ```
   - Finds ROOT, Boost
   - Downloads ONNX Runtime
   - Discovers analyses in `analyses/`

2. **Core Library**:
   ```cmake
   # In core/CMakeLists.txt
   add_library(RDFCore SHARED
       src/Analyzer.cc
       src/ConfigurationManager.cc
       src/DataManager.cc
       # ...
   )
   target_link_libraries(RDFCore PUBLIC ROOT::ROOTDataFrame ...)
   ```

3. **Plugins**:
   ```cmake
   # In core/plugins/OnnxManager/CMakeLists.txt
   add_library(OnnxManager SHARED
       OnnxManager.cc
   )
   target_link_libraries(OnnxManager
       PRIVATE RDFCore
       PRIVATE onnxruntime::onnxruntime
   )
   ```

4. **Analyses**:
   ```cmake
   # In analyses/CMakeLists.txt
   file(GLOB ANALYSIS_DIRS LIST_DIRECTORIES true *)
   foreach(dir ${ANALYSIS_DIRS})
       if(EXISTS "${dir}/CMakeLists.txt")
           add_subdirectory(${dir})
       endif()
   endforeach()
   ```

### Analysis Discovery

New analyses are automatically discovered:

1. Clone analysis repo into `analyses/`
2. Ensure it has a `CMakeLists.txt`
3. Re-run build

```bash
cd analyses
git clone <your-analysis-repo> MyAnalysis
cd ..
source build.sh
```

The build system finds `analyses/MyAnalysis/CMakeLists.txt` and builds it.

## Advanced Features

### Custom ROOT Dictionaries

Support for custom C++ objects in ROOT files:

```cmake
# User provides via CMake variables
cmake -S . -B build \
  -DRDF_CUSTOM_DICT_HEADERS="MyEvent.h;MyObject.h" \
  -DRDF_CUSTOM_DICT_LINKDEF="MyLinkDef.h" \
  -DRDF_CUSTOM_DICT_INCLUDE_DIRS="/path/to/headers" \
  -DRDF_CUSTOM_DICT_SOURCES="MyEvent.cc"

# Core CMakeLists.txt generates dictionary
if(RDF_CUSTOM_DICT_HEADERS AND RDF_CUSTOM_DICT_LINKDEF)
    root_generate_dictionary(CustomDict
        ${RDF_CUSTOM_DICT_HEADERS}
        LINKDEF ${RDF_CUSTOM_DICT_LINKDEF}
        OPTIONS -I${RDF_CUSTOM_DICT_INCLUDE_DIRS}
    )
    add_library(${RDF_CUSTOM_DICT_TARGET} SHARED
        ${RDF_CUSTOM_DICT_SOURCES}
        CustomDict.cxx
    )
    target_link_libraries(RDFCore PRIVATE ${RDF_CUSTOM_DICT_TARGET})
endif()
```

### Counter Service

Automatic event counting and weight summation:

```cpp
class CounterService : public IAnalysisService {
    void run(IDataFrameProvider& df, IOutputSink& sink) override {
        // Count events
        auto count = df.getDataFrame().Count();
        
        // Sum weights
        std::shared_ptr<double> sumW;
        if (!weightBranch_.empty()) {
            sumW = df.getDataFrame().Sum(weightBranch_);
        }
        
        // Trigger computation
        std::cout << "Events: " << *count << std::endl;
        if (sumW) {
            std::cout << "Sum weights: " << *sumW << std::endl;
        }
        
        // Write histogram
        TH1D* hist = new TH1D("counters", "Counters", 2, 0, 2);
        hist->SetBinContent(1, *count);
        if (sumW) hist->SetBinContent(2, *sumW);
        sink.writeObject(hist, "event_counters");
    }
};
```

### Multithreading Support

Framework supports ROOT's ImplicitMT:

```cpp
// In Analyzer constructor
int nThreads = std::stoi(config_->get("threads"));
if (nThreads > 0) {
    ROOT::EnableImplicitMT(nThreads);
} else if (nThreads < 0) {
    ROOT::EnableImplicitMT();  // Use all cores
}
```

Thread-safety considerations:
- RDataFrame handles event-level parallelism
- Plugin model loading happens once (thread-safe)
- Plugin inference functions must be thread-safe
- ONNX Runtime configured with inter/intra-op threads

### Systematic Variations

Framework propagates systematics through Define operations:

```cpp
// SystematicManager generates systematic variations
auto variations = sysMgr->getSystematicNames();  // ["nominal", "jes_up", "jes_down"]

// DataManager::Define with SystematicManager
for (const auto& variation : variations) {
    std::string varName = name + "_" + variation;
    currentNode_ = currentNode_.Define(varName,
        [func, variation](auto&&... args) {
            return func(args..., variation);
        },
        columns
    );
}
```

## Design Patterns Used

### 1. Facade Pattern
**Analyzer** provides simplified interface to complex subsystem.

### 2. Factory Pattern
**ManagerFactory** creates concrete implementations of interfaces.

### 3. Strategy Pattern
Plugins are interchangeable strategies for specific tasks.

### 4. Template Method
**NamedObjectManager** defines skeleton, subclasses fill in specifics.

### 5. Dependency Injection
Managers receive dependencies via **ManagerContext**.

### 6. RAII
All resources managed via `unique_ptr`, automatic cleanup.

### 7. Interface Segregation
Multiple small interfaces (`IPluggableManager`, `IContextAware`) instead of one large interface.

## Performance Considerations

### Lazy Evaluation

RDataFrame's lazy evaluation means:
- No computation happens until action is triggered
- All operations are optimized together
- No intermediate data is materialized unnecessarily

### Memory Management

- Use `unique_ptr` for ownership
- Pass interfaces by reference
- Avoid unnecessary copies
- Let RDataFrame manage event data

### Parallel Processing

- Enable ImplicitMT for parallel event processing
- Ensure plugin operations are thread-safe
- Configure ML frameworks (ONNX) for single-threaded inference per event

## Testing Strategy

### Unit Tests

Test individual managers in isolation:

```cpp
TEST(OnnxManagerTest, ModelLoading) {
    ConfigurationManager config("test_config.txt");
    OnnxManager mgr(config);
    EXPECT_TRUE(mgr.hasModel("test_model"));
}
```

### Integration Tests

Test full analysis workflows:

```cpp
TEST(AnalyzerTest, EndToEnd) {
    Analyzer analyzer("test_analysis.txt");
    analyzer.Define(...);
    analyzer.Filter(...);
    EXPECT_NO_THROW(analyzer.save());
}
```

### Mock Objects

Use interfaces for mocking:

```cpp
class MockDataManager : public IDataFrameProvider {
    MOCK_METHOD(void, Define, (const std::string&, ...), (override));
    // ...
};
```

## See Also

- [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md) - Guide for adding new plugins
- [API_REFERENCE.md](API_REFERENCE.md) - Detailed API documentation
- [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md) - Using the framework
