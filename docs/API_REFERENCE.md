# API Reference

Complete API documentation for RDFAnalyzerCore interfaces and key classes.

## Table of Contents

- [Analyzer](#analyzer)
- [Configuration Interfaces](#configuration-interfaces)
- [Data Management](#data-management)
- [Plugin Interfaces](#plugin-interfaces)
- [Manager Implementations](#manager-implementations)
- [Utility Classes](#utility-classes)

## Analyzer

**Header**: `core/interface/analyzer.h`

The main facade class that orchestrates the analysis.

### Construction

```cpp
Analyzer(const std::string& configFile,
         std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins = {});
```

**Parameters**:
- `configFile`: Path to main configuration file
- `plugins`: Optional map of plugin role names to plugin instances

**Example**:
```cpp
// Simple construction
Analyzer analyzer("config.txt");

// With custom plugins
std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
plugins["custom"] = std::make_unique<MyPlugin>(...);
Analyzer analyzer("config.txt", std::move(plugins));
```

### Variable Definition

#### Define

```cpp
Analyzer* Define(const std::string& name,
                 F function,
                 const std::vector<std::string>& columns = {},
                 ISystematicManager* sysMgr = nullptr);
```

Define a new DataFrame column.

**Parameters**:
- `name`: Name of the new column
- `function`: Lambda or function to compute the column
- `columns`: Input column names (empty for no dependencies)
- `sysMgr`: Optional systematic manager for variation support

**Returns**: `this` for method chaining

**Example**:
```cpp
// Simple scalar
analyzer.Define("weight", []() { return 1.0; }, {});

// Derived variable
analyzer.Define("pt_gev",
    [](float pt) { return pt / 1000.0; },
    {"pt"}
);

// With systematics
analyzer.Define("corrected_pt",
    [](float pt, const std::string& sys) {
        if (sys == "up") return pt * 1.1;
        if (sys == "down") return pt * 0.9;
        return pt;
    },
    {"pt"},
    analyzer.getSystematicManager()
);
```

#### DefinePerSample

```cpp
template<typename F>
Analyzer* DefinePerSample(const std::string& name, F function);
```

Define a column that depends on the sample being processed.

**Example**:
```cpp
analyzer.DefinePerSample("cross_section",
    [](unsigned int slot, const ROOT::RDF::RSampleInfo& info) {
        std::string sample = info.AsString();
        if (sample.find("ttbar") != std::string::npos) return 831.76;
        return 1.0;
    }
);
```

### Event Selection

#### Filter

```cpp
Analyzer* Filter(const std::string& name,
                 F function,
                 const std::vector<std::string>& columns = {});

Analyzer* Filter(F function,
                 const std::vector<std::string>& columns = {});
```

Apply an event filter.

**Parameters**:
- `name`: Optional name for the filter (appears in reports)
- `function`: Lambda returning bool (true = keep event)
- `columns`: Input column names

**Example**:
```cpp
// Named filter
analyzer.Filter("pt_cut",
    [](float pt) { return pt > 25.0; },
    {"jet_pt"}
);

// Unnamed filter
analyzer.Filter(
    [](int n_jets) { return n_jets >= 4; },
    {"n_jets"}
);
```

### Plugin Access

#### getPlugin

```cpp
template<typename T>
T* getPlugin(const std::string& role);
```

Retrieve a plugin by role name with type checking.

**Template Parameter**: Interface type to cast to

**Returns**: Pointer to plugin, or nullptr if not found

**Example**:
```cpp
auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");
if (onnxMgr) {
    onnxMgr->applyAllModels();
}

auto* histMgr = analyzer.getPlugin<INDHistogramManager>("histogram");
```

### Manager Access

```cpp
IConfigurationProvider* getConfigProvider();
IDataFrameProvider* getDataManager();
ISystematicManager* getSystematicManager();
```

Access core managers.

**Example**:
```cpp
auto* config = analyzer.getConfigProvider();
std::string outputFile = config->get("saveFile");

auto df = analyzer.getDataManager()->getDataFrame();
```

### Execution

#### save

```cpp
void save();
```

Trigger the event loop and save outputs.

This method:
1. Finalizes all plugins
2. Writes the skim output to `saveFile`
3. Writes histograms/metadata to `metaFile`

**Example**:
```cpp
analyzer.Define(...);
analyzer.Filter(...);
// ... define analysis ...
analyzer.save();  // Execute and write outputs
```

## Configuration Interfaces

### IConfigurationProvider

**Header**: `core/interface/api/IConfigurationProvider.h`

Interface for accessing configuration values.

#### Methods

```cpp
virtual std::string get(const std::string& key) const = 0;
virtual bool has(const std::string& key) const = 0;
virtual std::map<std::string, std::string> getAll() const = 0;
```

Basic key-value retrieval.

**Example**:
```cpp
std::string saveFile = config->get("saveFile");
bool hasThreads = config->has("threads");
auto allConfig = config->getAll();
```

#### Specialized Parsers

```cpp
virtual std::vector<std::pair<std::string, std::string>> 
    getPairs(const std::string& configFile) const = 0;
```

Parse a config file with key=value pairs.

**Example**:
```cpp
// For file with: "const1=1.0\nconst2=2.0"
auto pairs = config->getPairs("constants.txt");
// Returns: {{"const1", "1.0"}, {"const2", "2.0"}}
```

```cpp
virtual std::vector<std::map<std::string, std::string>> 
    getMultiKeyConfigs(const std::string& configFile) const = 0;
```

Parse a config file with multi-key lines (plugin configs).

**Example**:
```cpp
// For file with: "file=model.onnx name=score inputVariables=pt,eta"
auto configs = config->getMultiKeyConfigs("onnx_models.txt");
// Returns: [{{"file": "model.onnx", "name": "score", 
//             "inputVariables": "pt,eta"}}]
```

### ConfigurationManager

**Header**: `core/interface/ConfigurationManager.h`

Main implementation of IConfigurationProvider.

```cpp
ConfigurationManager(const std::string& configFile);
```

**Constructor**: Loads configuration from file.

## Data Management

### IDataFrameProvider

**Header**: `core/interface/api/IDataFrameProvider.h`

Interface for DataFrame operations.

#### DataFrame Access

```cpp
virtual ROOT::RDF::RNode& getDataFrame() = 0;
```

Get the current DataFrame node.

**Example**:
```cpp
auto df = dataManager->getDataFrame();
auto count = df.Count();
std::cout << "Events: " << *count << std::endl;
```

#### Column Definition

```cpp
template<typename F>
void Define(const std::string& name,
            F function,
            const std::vector<std::string>& columns,
            ISystematicManager& sysMgr);
```

Define a new column with systematic support.

```cpp
template<typename F>
void DefinePerSample(const std::string& name, F function);
```

Define a per-sample column.

```cpp
void DefineVector(const std::string& name,
                  const std::vector<std::string>& columns,
                  const std::string& type,
                  ISystematicManager& sysMgr);
```

Create a vector column from scalar or RVec inputs.

**Example**:
```cpp
// Combine scalars into vector
dataManager->DefineVector("lepton_pts", 
    {"lep1_pt", "lep2_pt"}, 
    "Float_t", 
    sysMgr);

// Result: column "lepton_pts" of type ROOT::VecOps::RVec<float>
```

#### Filtering

```cpp
template<typename F>
void Filter(F function, const std::vector<std::string>& columns);

template<typename F>
void Filter(const std::string& name, F function, 
            const std::vector<std::string>& columns);
```

Apply event filters.

### DataManager

**Header**: `core/interface/DataManager.h`

Main implementation of IDataFrameProvider.

```cpp
DataManager(IConfigurationProvider& config);
```

Constructor automatically:
- Builds TChain from `fileList`
- Creates RDataFrame
- Loads constants from `floatConfig`, `intConfig`
- Applies aliases from `aliasConfig`
- Registers optional branches from `optionalBranchesConfig`

## Plugin Interfaces

### IPluggableManager

**Header**: `core/interface/api/IPluggableManager.h`

Base interface for all plugins.

```cpp
class IPluggableManager {
public:
    virtual ~IPluggableManager() = default;
    virtual void initialize() = 0;
    virtual void finalize() = 0;
    virtual std::string getName() const = 0;
};
```

#### Methods

- `initialize()`: Called after construction and context setting
- `finalize()`: Called before saving outputs
- `getName()`: Returns plugin identifier

### IContextAware

**Header**: `core/interface/api/IContextAware.h`

Interface for plugins that need access to core managers.

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

**Example**:
```cpp
class MyPlugin : public IPluggableManager, public IContextAware {
public:
    void setContext(const ManagerContext& ctx) override {
        context_ = &ctx;
    }
    
    void initialize() override {
        context_->logger.info("MyPlugin initialized");
    }
    
private:
    ManagerContext* context_;
};
```

### NamedObjectManager<T>

**Header**: `core/interface/NamedObjectManager.h`

Base class for plugins managing collections of named objects.

```cpp
template<typename T>
class NamedObjectManager : public IPluggableManager, public IContextAware {
protected:
    std::unordered_map<std::string, T> objects_;
    ManagerContext* context_;
    
public:
    virtual void loadObjects(
        const std::vector<std::map<std::string, std::string>>& configs) = 0;
    
    T getObject(const std::string& key) const;
    bool hasObject(const std::string& key) const;
    std::vector<std::string> getAllKeys() const;
};
```

#### Methods

```cpp
T getObject(const std::string& key) const;
```

Retrieve an object by name. Throws if not found.

```cpp
bool hasObject(const std::string& key) const;
```

Check if an object exists.

```cpp
std::vector<std::string> getAllKeys() const;
```

Get all object names.

## Manager Implementations

### IBDTManager

**Header**: `core/plugins/BDTManager/BDTManager.h`

Interface for BDT model management.

```cpp
class IBDTManager : public IPluggableManager {
public:
    virtual void applyModel(const std::string& modelName, 
                           const std::string& outputSuffix = "") = 0;
    virtual void applyAllModels(const std::string& outputSuffix = "") = 0;
    virtual std::vector<std::string> getAllModelNames() const = 0;
    virtual std::vector<std::string> getModelFeatures(
        const std::string& modelName) const = 0;
    virtual std::string getRunVar(const std::string& modelName) const = 0;
};
```

#### Methods

```cpp
void applyModel(const std::string& modelName, 
                const std::string& outputSuffix = "");
```

Apply a specific BDT model to the DataFrame.

**Parameters**:
- `modelName`: Name of the configured model
- `outputSuffix`: Optional suffix for output column name

```cpp
void applyAllModels(const std::string& outputSuffix = "");
```

Apply all configured BDT models.

**Example**:
```cpp
auto* bdtMgr = analyzer.getPlugin<IBDTManager>("bdt");
bdtMgr->applyModel("signal_bdt");  // Creates "signal_bdt" column
bdtMgr->applyAllModels();          // Apply all configured models
```

### IOnnxManager

**Header**: `core/plugins/OnnxManager/OnnxManager.h`

Interface for ONNX model management.

```cpp
class IOnnxManager : public IPluggableManager {
public:
    virtual void applyModel(const std::string& modelName, 
                           const std::string& outputSuffix = "") = 0;
    virtual void applyAllModels(const std::string& outputSuffix = "") = 0;
    virtual std::vector<std::string> getAllModelNames() const = 0;
    virtual std::vector<std::string> getModelFeatures(
        const std::string& modelName) const = 0;
    virtual std::string getRunVar(const std::string& modelName) const = 0;
    virtual std::vector<std::string> getModelInputNames(
        const std::string& modelName) const = 0;
    virtual std::vector<std::string> getModelOutputNames(
        const std::string& modelName) const = 0;
};
```

**Note**: Models with multiple outputs create columns named `{modelName}_output0`, `{modelName}_output1`, etc.

**Example**:
```cpp
auto* onnxMgr = analyzer.getPlugin<IOnnxManager>("onnx");

// Apply single-output model
onnxMgr->applyModel("classifier");  // Creates "classifier" column

// Apply multi-output model
onnxMgr->applyModel("transformer");  
// Creates: transformer_output0, transformer_output1, ...
```

### ISofieManager

**Header**: `core/plugins/SofieManager/SofieManager.h`

Interface for SOFIE model management.

```cpp
class ISofieManager : public IPluggableManager {
public:
    virtual void registerModel(
        const std::string& name,
        std::shared_ptr<SofieInferenceFunction> func,
        const std::vector<std::string>& features,
        const std::string& runVar) = 0;
    
    virtual void applyModel(const std::string& modelName) = 0;
    virtual void applyAllModels() = 0;
    virtual std::vector<std::string> getAllModelNames() const = 0;
    virtual std::vector<std::string> getModelFeatures(
        const std::string& modelName) const = 0;
    virtual std::string getRunVar(const std::string& modelName) const = 0;
};
```

**Key Difference**: SOFIE models must be manually registered.

**Example**:
```cpp
#include "MyGeneratedModel.hxx"  // SOFIE-generated code

std::vector<float> myInference(const std::vector<float>& input) {
    TMVA_SOFIE_MyModel::Session session;
    return session.infer(input.data());
}

auto* sofieMgr = analyzer.getPlugin<ISofieManager>("sofie");
auto func = std::make_shared<SofieInferenceFunction>(myInference);
sofieMgr->registerModel("my_model", func, {"pt", "eta"}, "run_model");
sofieMgr->applyModel("my_model");
```

### ICorrectionManager

**Header**: `core/plugins/CorrectionManager/CorrectionManager.h`

Interface for applying scale factors via correctionlib.

```cpp
class ICorrectionManager : public IPluggableManager {
public:
    virtual void applyCorrection(const std::string& correctionName) = 0;
    virtual void applyAllCorrections() = 0;
    virtual std::vector<std::string> getCorrectionNames() const = 0;
};
```

**Example**:
```cpp
auto* corrMgr = analyzer.getPlugin<ICorrectionManager>("correction");
corrMgr->applyAllCorrections();
// Creates columns for each configured correction
```

### ITriggerManager

**Header**: `core/plugins/TriggerManager/TriggerManager.h`

Interface for trigger logic.

```cpp
class ITriggerManager : public IPluggableManager {
public:
    virtual void defineTriggerFlags() = 0;
    virtual std::vector<std::string> getTriggerGroupNames() const = 0;
};
```

### INDHistogramManager

**Header**: `core/plugins/NDHistogramManager/NDHistogramManager.h`

Interface for N-dimensional histogram management.

```cpp
class INDHistogramManager : public IPluggableManager {
public:
    virtual void bookND(
        const std::vector<histInfo>& infos,
        const std::vector<selectionInfo>& selections,
        const std::string& suffix,
        const std::vector<std::vector<std::string>>& regionNames) = 0;
    
    virtual void saveHists(
        const std::vector<std::vector<histInfo>>& fullHistList,
        const std::vector<std::vector<std::string>>& allRegionNames) = 0;
    
    virtual std::vector<THnMulti>& GetHistos() = 0;
};
```

#### Histogram Structures

```cpp
struct histInfo {
    std::string name;          // Histogram name
    std::string variable;      // DataFrame column to fill
    std::string title;         // Axis title
    std::string weight;        // Weight column
    int nbins;                 // Number of bins
    double xmin, xmax;        // Axis range
    
    histInfo(const std::string& name,
             const std::string& variable,
             const std::string& title,
             const std::string& weight,
             int nbins, double xmin, double xmax);
};

struct selectionInfo {
    std::string name;          // Selection axis name
    int nbins;                 // Number of bins
    double xmin, xmax;        // Axis range
    
    selectionInfo(const std::string& name,
                 int nbins, double xmin, double xmax);
};
```

**Example**:
```cpp
#include <plots.h>

auto* histMgr = analyzer.getPlugin<INDHistogramManager>("histogram");

std::vector<histInfo> hists = {
    histInfo("h_pt", "jet_pt", "p_{T}", "weight", 50, 0, 500),
    histInfo("h_eta", "jet_eta", "#eta", "weight", 50, -2.5, 2.5),
};

std::vector<selectionInfo> selections = {
    selectionInfo("region", 2, 0, 2),
};

std::vector<std::vector<std::string>> regionNames = {
    {"SR", "CR"},
};

histMgr->bookND(hists, selections, "", regionNames);

// Trigger computation
auto& histos = histMgr->GetHistos();
for (auto& h : histos) {
    h.GetPtr();
}

// Save
std::vector<std::vector<histInfo>> fullHistList = {hists};
histMgr->saveHists(fullHistList, regionNames);
```

## Utility Classes

### ISystematicManager

**Header**: `core/interface/api/ISystematicManager.h`

Interface for tracking systematic variations.

```cpp
class ISystematicManager {
public:
    virtual void registerSystematic(const std::string& name) = 0;
    virtual std::vector<std::string> getSystematicNames() const = 0;
    virtual bool hasSystematic(const std::string& name) const = 0;
};
```

**Example**:
```cpp
auto* sysMgr = analyzer.getSystematicManager();
sysMgr->registerSystematic("jes_up");
sysMgr->registerSystematic("jes_down");

auto systematics = sysMgr->getSystematicNames();
// Returns: ["nominal", "jes_up", "jes_down"]
```

### IOutputSink

**Header**: `core/interface/api/IOutputSink.h`

Interface for output destinations.

```cpp
class IOutputSink {
public:
    virtual void write(const std::string& treeName,
                      const ROOT::RDF::RNode& df,
                      const std::vector<std::string>& columns) = 0;
    virtual void writeObject(TObject* obj, const std::string& name) = 0;
    virtual TFile* getFile() = 0;
    virtual void close() = 0;
};
```

### ILogger

**Header**: `core/interface/api/ILogger.h`

Interface for logging.

```cpp
enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR
};

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

**Example**:
```cpp
auto* logger = context_->logger;
logger->info("Processing started");
logger->warn("Missing optional branch: " + branchName);
logger->error("Configuration error: " + error);
```

## Factory Classes

### ManagerFactory

**Header**: `core/interface/ManagerFactory.h`

Factory for creating core managers.

```cpp
class ManagerFactory {
public:
    static std::unique_ptr<IConfigurationProvider> 
        createConfigurationManager(const std::string& configFile);
    
    static std::unique_ptr<IDataFrameProvider> 
        createDataManager(IConfigurationProvider& config);
    
    static std::unique_ptr<ISystematicManager> 
        createSystematicManager();
};
```

**Example**:
```cpp
auto config = ManagerFactory::createConfigurationManager("config.txt");
auto dataManager = ManagerFactory::createDataManager(*config);
auto sysMgr = ManagerFactory::createSystematicManager();
```

## Helper Functions

### functions.h

**Header**: `core/interface/functions.h`

Common physics functions.

```cpp
// Sum Lorentz vectors
template<typename T>
T sumLorentzVec(const T& v1, const T& v2);

// Extract Lorentz vector mass
template<typename U, typename T>
U getLorentzVecM(const T& vec);

// Extract Lorentz vector pT
template<typename U, typename T>
U getLorentzVecPt(const T& vec);

// Extract Lorentz vector eta
template<typename U, typename T>
U getLorentzVecEta(const T& vec);

// Extract Lorentz vector phi
template<typename U, typename T>
U getLorentzVecPhi(const T& vec);
```

**Example**:
```cpp
analyzer.Define("dilepton",
    sumLorentzVec<ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<float>>>,
    {"lep1_vec", "lep2_vec"}
);

analyzer.Define("dilepton_mass",
    getLorentzVecM<float, ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<float>>>,
    {"dilepton"}
);
```

## Type Aliases

Common type aliases used throughout the framework:

```cpp
// RDataFrame node
using RNode = ROOT::RDF::RNode;

// RVec (ROOT vector)
template<typename T>
using RVec = ROOT::VecOps::RVec<T>;

// Lorentz vector
using LorentzVector = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<float>>;
```

## Error Handling

The framework uses exceptions for error reporting:

```cpp
// Configuration errors
if (!config->has("required_key")) {
    throw std::runtime_error("Missing required configuration: required_key");
}

// Plugin errors
if (!hasModel(name)) {
    throw std::invalid_argument("Model not found: " + name);
}

// File errors
if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + filename);
}
```

## Thread Safety

Guidelines for thread safety:

1. **Model Loading**: Load once in constructor (thread-safe)
2. **Inference Functions**: Must be thread-safe for ImplicitMT
3. **Shared Resources**: Use `std::shared_ptr` or synchronization
4. **Logging**: Logger implementations should be thread-safe

**Example Thread-Safe Inference**:
```cpp
// Shared resource (ONNX session)
auto session = std::make_shared<Ort::Session>(...);

// Define with shared_ptr (safe for parallel execution)
dataManager->Define("score",
    [session](float pt, float eta) {
        // Each thread gets a copy of shared_ptr
        // Session is accessed concurrently (ensure session is thread-safe)
        return runInference(session, pt, eta);
    },
    {"pt", "eta"}
);
```

## Performance Tips

1. **Filter Early**: Apply filters before expensive operations
2. **Lazy Evaluation**: RDataFrame optimizes the full chain
3. **Enable Multithreading**: Set `threads=-1` in config
4. **Minimize Copies**: Use references in lambdas
5. **Conditional Execution**: Use `runVar` to skip unnecessary computation

## See Also

- [GETTING_STARTED.md](GETTING_STARTED.md) - Quick start guide
- [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md) - Analysis examples
- [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md) - Creating plugins
- [ARCHITECTURE.md](ARCHITECTURE.md) - Internal architecture
