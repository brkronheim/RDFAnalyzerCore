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

### 2. Configuration-Driven Settings

**Goal**: Keep runtime settings and analysis algorithms in the right places.

- Text/YAML files hold I/O, performance, plugin, and output settings
- Analysis code defines physics logic and constructs the plugins it uses
- Compatible configs can reuse an executable without changing its code
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
    // Construction (config-file and dependency-injected variants)
    Analyzer(std::string configFile,
             std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins = {});
    
    // Analysis operations
    Analyzer* Define(std::string name, ...);
    Analyzer* Filter(std::string name, ...);
    Analyzer* DefinePerSample(...);
    Analyzer* DefineVector(...);
    Analyzer* bookConfigHistograms();
    Analyzer* bookCounterIntHistogram(...);
    
    // Plugin access
    template<typename T>
    std::shared_ptr<T> getPlugin(const std::string& role) const;
    
    // C++ execution entry point
    Analyzer* run();
    
    // Access to underlying managers
    IConfigurationProvider& getConfigurationProvider() const;
    IDataFrameProvider& getDataFrameProvider() const;
    ISystematicManager& getSystematicManager() const;
};
```

**Design Notes**:
- Returns `this` for method chaining: `analyzer.Define(...)->Filter(...)->Define(...)`
- Template method `getPlugin<T>` provides type-safe plugin access via `std::shared_ptr`
- Owns core providers, the logger, output sinks, and ordinary analysis services with `std::unique_ptr`; plugins and the provenance service use `std::shared_ptr`.

### ConfigurationManager

**Location**: `core/src/ConfigurationManager.cc`, `core/interface/ConfigurationManager.h`

**Purpose**: Load and provide access to configuration values.

**Responsibilities**:
- Parse YAML (`.yaml`/`.yml`) and key/value text files
- Parse referenced plugin and analysis configuration files
- Return configuration values as strings and parse pair-, multi-key-, and vector-based config files
- Handle defaults and missing keys

`ConfigurationManager::get()` returns an empty string for a missing key after
logging a warning. It does not perform typed conversion. Callers validate and
convert values as needed. Relative config-file references resolve from the
process working directory when present, then from the main config file's
directory.

**Key Interface** (`IConfigurationProvider`):
```cpp
class IConfigurationProvider {
public:
    virtual std::string get(const std::string& key) const = 0;
    virtual void set(const std::string& key, const std::string& value) = 0;
    virtual const std::unordered_map<std::string, std::string>& getConfigMap() const = 0;
    
    // Specialized parsers
    virtual std::vector<std::unordered_map<std::string, std::string>>
        parseMultiKeyConfig(const std::string& configFile,
                            const std::vector<std::string>& requiredEntryKeys) const = 0;
    virtual std::unordered_map<std::string, std::string>
        parsePairBasedConfig(const std::string& configFile) const = 0;
    virtual std::vector<std::string>
        parseVectorConfig(const std::string& configFile) const = 0;
};
```

**Configuration Adapter Pattern**:
```
ConfigurationManager ──uses──► IConfigAdapter (interface)
                                       │
                                       ├─► TextConfigAdapter (.txt and other non-YAML files)
                                       └─► YamlConfigAdapter (.yaml / .yml)
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
    virtual ROOT::RDF::RNode getDataFrame() = 0;
    virtual void setDataFrame(const ROOT::RDF::RNode& node) = 0;
    virtual void DefineVector(std::string name,
                              const std::vector<std::string>& columns,
                              std::string type,
                              ISystematicManager& systematics) = 0;

    // Define, Filter, DefinePerSample, and Redefine are template helpers
    // implemented on IDataFrameProvider rather than virtual interface calls.
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
auto df = dataManager->getDataFrame();
```

### SystematicManager

**Location**: `core/src/SystematicManager.cc`, `core/interface/SystematicManager.h`

**Purpose**: Track systematic variations and propagate through analysis.

**Responsibilities**:
- Register systematic families and the nominal variables they affect
- Map variables to explicit Up/Down columns or other named variation columns
- Discover complete Up/Down pairs from dataframe column names
- Build and cache shared systematic-counter columns when requested

**Key Interface** (`ISystematicManager`):
```cpp
class ISystematicManager {
public:
    static constexpr const char* CANONICAL_SYST_BRANCH_NAME = "SystematicCounter";
    virtual void registerSystematic(
        const std::string& name,
        const std::set<std::string>& affectedVariables) = 0;
    virtual void registerVariationColumns(
        const std::string& variable, const std::string& systematicName,
        const std::string& upColumn, const std::string& downColumn) = 0;
    virtual void registerVariationColumn(
        const std::string& variable, const std::string& variationLabel,
        const std::string& column) = 0;
    virtual SystematicValidationResult autoRegisterSystematics(
        const std::vector<std::string>& columnNames) = 0;
};
```

**Usage Pattern**:
```cpp
// Map the nominal input variable to its existing shifted columns.
auto& sysMgr = analyzer.getSystematicManager();
sysMgr.registerVariationColumns(
    "jet_pt", "jes", "jet_pt_jesUp", "jet_pt_jesDown");

// Analyzer::Define uses this mapping when defining derived columns.
analyzer.Define("corrected_pt", correctJetPt, {"jet_pt"});
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
    virtual void writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) = 0;
    virtual void writeDataFrame(ROOT::RDF::RNode& df,
                                const IConfigurationProvider& configProvider,
                                const ISystematicManager* systematicManager,
                                OutputChannel channel) = 0;
    virtual std::string resolveOutputFile(const IConfigurationProvider& configProvider,
                                          OutputChannel channel) = 0;
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
    enum class Level { Trace, Debug, Info, Warn, Error };
    virtual void log(Level level, const std::string& message) = 0;
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
    virtual std::string type() const = 0;
    virtual void setupFromConfigFile() = 0;
    virtual std::vector<std::string> getDependencies() const { return {}; }
    virtual void initialize() {}
    virtual void execute() {}
    virtual void finalize() {}
    virtual void reportMetadata() {}
};
```

**IContextAware** - Plugins that need access to core managers:
```cpp
struct ManagerContext {
    IConfigurationProvider& config;
    IDataFrameProvider& data;
    ISystematicManager& systematics;
    ILogger& logger;
    IOutputSink& skimSink;
    IOutputSink& metaSink;
};

class IContextAware {
public:
    virtual void setContext(ManagerContext& ctx) = 0;
};
```

#### Plugin Base Classes

**NamedObjectManager<T>** - Base class for plugins managing named objects:

```cpp
template<typename T>
class NamedObjectManager : public IPluggableManager {
public:
    const T& getObject(const std::string& key) const;
    const std::vector<std::string>& getFeatures(const std::string& key) const;
    void setContext(ManagerContext& ctx) override;
    void setupFromConfigFile() override;

protected:
    std::unordered_map<std::string, T> objects_m;
    std::unordered_map<std::string, std::vector<std::string>> features_m;
    IConfigurationProvider* configManager_m = nullptr;
    IDataFrameProvider* dataManager_m = nullptr;
    ISystematicManager* systematicManager_m = nullptr;
    ILogger* logger_m = nullptr;
};
```

`NamedObjectManager` provides named-object and feature lookup plus access to
context services for derived plugins. It does not prescribe how a derived
manager parses or loads its objects.

**Used by**:
- `BDTManager` - manages `FastForest` objects
- `OnnxManager` - manages `Ort::Session` objects
- `SofieManager` - manages `SofieInferenceFunction` objects
- `CorrectionManager` - manages `correctionlib::Correction` objects

### Plugin Lifecycle

1. **Construction and registration**: Analysis code creates and registers a
   plugin, either with its factory or `Analyzer::addPlugin()`.
   ```cpp
   Analyzer analyzer("config.yaml");
   auto onnxMgr = OnnxManager::create(analyzer, "onnxManager");
   ```

2. **Wiring and initialization**: `addPlugin()` checks declared dependencies,
   recomputes the dependency order, then calls `setContext()`,
   `setupFromConfigFile()`, and `initialize()` for the new manager. Plugins
   supplied in the Analyzer constructor map are wired in dependency order
   during construction. Context injection uses a mutable `ManagerContext&`;
   plugins should not build a temporary context themselves.

3. **Usage**: Analysis code invokes the registered manager after wiring.
   ```cpp
   onnxMgr->applyAllModels();
   analyzer.run();
   ```

4. **Run and finalization**: `Analyzer::run()` invokes plugin `execute()`
   hooks, triggers configured output actions, finalizes analysis services and
   plugins, asks plugins to report metadata, then finalizes provenance. A
   plugin's `execute()` usually adds columns or books lazy actions before ROOT
   evaluates the dataframe.

### Plugin Discovery and Access

`getPlugin<T>()` is for required plugins: it throws `std::out_of_range` if the role is absent and `std::bad_cast` if the registered object has another type. `findPlugin<T>()` is for optional plugins and returns `nullptr` when the role is absent or the type does not match.

**Usage**:
```cpp
// Request a required manager plugin
auto onnxMgr = analyzer.getPlugin<OnnxManager>("onnxManager");
onnxMgr->applyModel("my_model");
```

### Common Plugin Patterns

#### 1. Configuration-Loaded Objects

Managers such as `OnnxManager` load named objects from configuration. The
base `NamedObjectManager` supplies object/feature lookup; the concrete manager
owns its config schema and loading logic. For example, the current
`OnnxManager` requires `file`, `name`, `inputVariables`, and `runVar` entries
when it parses `onnxConfig`:

```cpp
const auto modelConfigs = configProvider.parseMultiKeyConfig(
    configProvider.get("onnxConfig"),
    {"file", "name", "inputVariables", "runVar"});
loadModelsFromConfig(configProvider, modelConfigs);
```

#### 2. DataFrame Augmentation

Plugins that add columns to the dataframe:

```cpp
auto onnxMgr = analyzer.getPlugin<OnnxManager>("onnxManager");
onnxMgr->applyModel("my_model");
```

The manager uses its injected `IDataFrameProvider` and `ISystematicManager` to
define inference output columns. The analysis must define each configured
feature column before applying the model and must run the Analyzer to evaluate
the resulting lazy graph.

#### 3. Metadata Collection

After execution, plugins can emit human-readable metadata from
`reportMetadata()` and structured provenance entries from
`collectProvenanceEntries()`. Analysis services can also return provenance
entries. `Analyzer::run()` gathers these contributions and finalizes the
`ProvenanceService` last so it can write a complete metadata record.

## Data Flow

### Typical Analysis Flow

```
1. **Analyzer construction**
   ├─► Parse the main config and construct core providers, sinks, and logger
   ├─► Build the input dataframe and load configured DataManager helpers
   └─► Initialize enabled services

2. **Plugin registration**
   ├─► Analysis code creates each manager it needs
   ├─► Analyzer validates dependencies and orders plugin roles
   └─► Inject context, load plugin config, and initialize plugins

3. **Analysis graph construction**
   ├─► Define derived columns and event filters
   ├─► Apply manager operations and corrections
   └─► Book histograms and other lazy actions

4. **Execution and output**
   ├─► Call analyzer.run()
   ├─► Execute plugin hooks and configured output actions
   ├─► Write skim only when enableSkim is true; save booked histograms
   └─► Finalize services, plugins, metadata, and provenance

5. **Cleanup**
   └─► Analyzer releases owned managers and services
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
analyzer.run();
// Runs the actions booked by the analysis, plugins, and services.
// A skim Snapshot is booked only when enableSkim is true.
```

### Plugin Integration Points

Plugins integrate at specific points in the flow:

1. **Construction**: Load configurations
2. **Pre-Execution**: Apply models, book histograms
3. **During Event Loop**: Define operations execute
4. **Post-Execution**: Write metadata, counters

```cpp
// Pre-execution when this analysis registered the optional ONNX manager
auto onnxMgr = analyzer.findPlugin<OnnxManager>("onnxManager");
if (onnxMgr) onnxMgr->applyAllModels();  // Adds Define operations to dataframe

// Execution
analyzer.run();  // Triggers event loop

// Post-execution (inside Analyzer::run())
// 1) finalize() services (except provenance)
// 2) finalize() plugins
// 3) reportMetadata() plugins
// 4) collect plugin/service provenance contributions
// 5) finalize provenance service last
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

//   d. Create default-constructed OutputSinks
skimSink_ = std::make_unique<RootOutputSink>();
metaSink_ = std::make_unique<RootOutputSink>();

//   e. Create Logger
logger_ = std::make_unique<DefaultLogger>();

//   f. Validate dependencies and topologically order plugin roles
//   g. Call setContext() and setupFromConfigFile() for each role in order
//   h. Call initialize() for each role after setup is complete
```

### Usage Phase

```cpp
// 2. User defines analysis
analyzer.Define("myvar", ...);
analyzer.Filter("mycut", ...);

// Get and use a required plugin
auto onnxMgr = analyzer.getPlugin<OnnxManager>("onnxManager");
onnxMgr->applyModel("model");
```

### Execution and Cleanup

```cpp
// 3. Trigger execution
analyzer.run();

// Inside Analyzer::run():
//   a. execute() all plugins in dependency order
//   b. write skim (controlled by enableSkim config)
//   c. save histograms (via NDHistogramManager)
//   d. finalize non-provenance services
//   e. finalize() all plugins, then reportMetadata() all plugins
//   f. collect plugin/service provenance entries
//   g. finalize provenance service last

// 4. Destruction (RAII)
// Analyzer destructor runs
// Owned core dependencies and services are released; shared plugin/service
// objects remain alive while another shared_ptr still refers to them.
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
│   └── tests/
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
   - Configures optional dependencies such as ONNX Runtime
   - Discovers analyses in `analyses/`

2. **Core Library**:
   ```cmake
   # core/src/CMakeLists.txt creates the central static library.
   add_library(core STATIC ${CORE_SRC})
   # core/CMakeLists.txt adds plugins and defines `coreAll`, an interface
   # target that links the core and plugin archives together.
   ```

3. **Plugins**:
   ```cmake
   # In core/plugins/OnnxManager/CMakeLists.txt
   add_library(OnnxManager OBJECT OnnxManager.cc)
   target_link_libraries(OnnxManager
       PUBLIC ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core
              OnnxRuntime::OnnxRuntime
   )
   # core/plugins/CMakeLists.txt adds plugin objects to corePlugins.
   ```

4. **Analyses**:
   ```cmake
   # In analyses/CMakeLists.txt
   file(GLOB analyses_children RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*")
   list(SORT analyses_children)
   foreach(child ${analyses_children})
       if(IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${child}" AND
          EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${child}/CMakeLists.txt")
           add_subdirectory(${child})
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
    ROOT_GENERATE_DICTIONARY(G__${RDF_CUSTOM_DICT_TARGET}
        ${RDF_CUSTOM_DICT_HEADERS}
        LINKDEF ${RDF_CUSTOM_DICT_LINKDEF}
    )
    add_library(${RDF_CUSTOM_DICT_TARGET} SHARED
        G__${RDF_CUSTOM_DICT_TARGET}.cxx
        ${RDF_CUSTOM_DICT_SOURCES}
    )
    target_include_directories(${RDF_CUSTOM_DICT_TARGET} PUBLIC
        ${RDF_CUSTOM_DICT_INCLUDE_DIRS})
    target_link_libraries(${RDF_CUSTOM_DICT_TARGET} PUBLIC
        ROOT::Core ROOT::RIO ROOT::Tree)
    target_link_libraries(core PUBLIC ${RDF_CUSTOM_DICT_TARGET})
endif()
```

### Counter Service

`CounterService` is an optional `IAnalysisService`, enabled with
`enableCounters=true`. Its `initialize(ManagerContext&)` hook books the
pre-filter event count and any configured weight sums. `onPreFilter()` captures
the dataframe before the first filter; `finalize(RNode&)` consumes the booked
results and writes counter metadata. An integer-category histogram can be
booked with `Analyzer::bookCounterIntHistogram()` after defining that column
and before the event loop runs.

```cpp
class CounterService : public IAnalysisService {
public:
    void initialize(ManagerContext& ctx) override;
    void onPreFilter(ROOT::RDF::RNode& df) override;
    void finalize(ROOT::RDF::RNode& df) override;
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

`Analyzer::Define(name, callable, columns)` passes its
`ISystematicManager` to the dataframe provider. When an input column has a
registered variation mapping, the provider defines the nominal output and
matching `Up`/`Down` output columns by calling the same callable with the
mapped input columns. The callable does not receive a systematic-name
argument. Register explicit input-column mappings when they do not follow the
default `baseVariable_systematicNameUp/Down` convention:

```cpp
auto& systematics = analyzer.getSystematicManager();
systematics.registerVariationColumns(
    "jet_pt", "jes", "jet_pt_jesUp", "jet_pt_jesDown");

analyzer.Define("jet_pt_corrected", correctJetPt, {"jet_pt"});
// The provider also defines jet_pt_corrected_jesUp and
// jet_pt_corrected_jesDown using the mapped input columns.
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
Core providers, sinks, logger, and ordinary analysis services use
`std::unique_ptr`; registered plugins and the provenance service use
`std::shared_ptr`. This keeps ownership explicit while allowing analysis code
to retain a plugin handle.

### 7. Interface Segregation
Multiple small interfaces (`IPluggableManager`, `IContextAware`) instead of one large interface.

## Performance Considerations

### Lazy Evaluation

RDataFrame's lazy evaluation means:
- No computation happens until action is triggered
- All operations are optimized together
- No intermediate data is materialized unnecessarily

### Memory Management

- Use smart pointers according to the API's ownership contract
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
    EXPECT_NO_THROW(mgr.getModel("test_model"));
}
```

### Integration Tests

Test full analysis workflows:

```cpp
TEST(AnalyzerTest, EndToEnd) {
    Analyzer analyzer("test_analysis.txt");
    analyzer.Define(...);
    analyzer.Filter(...);
    EXPECT_NO_THROW(analyzer.run());
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
