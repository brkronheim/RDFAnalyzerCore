# API Reference

Complete API documentation for RDFAnalyzerCore interfaces and key classes.

## Table of Contents

- [Analyzer](#analyzer)
- [Configuration Interfaces](#configuration-interfaces)
- [Data Management](#data-management)
- [Plugin Interfaces](#plugin-interfaces)
- [Manager Implementations](#manager-implementations)
  - [WeightManager](#weightmanager)
  - [RegionManager](#regionmanager)
  - [CutflowManager](#cutflowmanager)
  - [PhysicsObjectCollection](#physicsobjectcollection)
  - [ProvenanceService](#provenanceservice)
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
{% raw %}
```cpp
// For file with: "const1=1.0\nconst2=2.0"
auto pairs = config->getPairs("constants.txt");
// Returns: {{"const1", "1.0"}, {"const2", "2.0"}}
```
{% endraw %}

```cpp
virtual std::vector<std::map<std::string, std::string>> 
    getMultiKeyConfigs(const std::string& configFile) const = 0;
```

Parse a config file with multi-key lines (plugin configs).

**Example**:
{% raw %}
```cpp
// For file with: "file=model.onnx name=score inputVariables=pt,eta"
auto configs = config->getMultiKeyConfigs("onnx_models.txt");
// Returns: [{{"file": "model.onnx", "name": "score", 
//             "inputVariables": "pt,eta"}}]
```
{% endraw %}

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

### CorrectionManager

**Header**: `core/plugins/CorrectionManager/CorrectionManager.h`

Applies scale factors and other corrections loaded from correctionlib JSON
files.

#### `applyCorrection`

```cpp
void applyCorrection(const std::string& correctionName,
                     const std::vector<std::string>& stringArguments);
```

Evaluates the named correction once per event using **scalar** input columns
and defines a new `Float_t` column called `correctionName` in the dataframe.

- `correctionName`: Key registered in the configuration (the `name` field).
- `stringArguments`: Constant string values for all `string`-typed inputs
  declared in the correctionlib JSON, supplied in the order they appear in the
  JSON.

**Example**:
```cpp
// muon_pt and muon_eta are scalar float columns
correctionManager.applyCorrection("muon_sf", {"nominal"});
// Adds a Float_t column "muon_sf" to the dataframe
```

#### `applyCorrectionVec`

```cpp
void applyCorrectionVec(const std::string& correctionName,
                        const std::vector<std::string>& stringArguments);
```

Evaluates the named correction **for every object in a collection** (e.g. all
jets in an event) and defines a new `ROOT::VecOps::RVec<Float_t>` column
called `correctionName` in the dataframe.

Use this method instead of `applyCorrection` when the input columns registered
via `inputVariables` are **RVec** columns (one vector per event, one element
per object).

- `correctionName`: Key registered in the configuration.
- `stringArguments`: Constant string values applied to every object in the
  collection (same semantics as in `applyCorrection`).

The method internally creates a temporary column that packs all per-object
feature vectors into a `RVec<RVec<double>>`, then applies the correction
lambda element-wise.

**Example**:
```cpp
// jet_pt and jet_eta are RVec<float> columns (one entry per jet per event)
correctionManager.applyCorrectionVec("jet_sf", {"nominal"});
// Adds an RVec<Float_t> column "jet_sf" (one scale factor per jet)

// Use the per-jet scale factors downstream:
analyzer.Define("corrected_jet_pt",
    [](const ROOT::VecOps::RVec<float>& pt,
       const ROOT::VecOps::RVec<Float_t>& sf) { return pt * sf; },
    {"jet_pt", "jet_sf"}
);
```

#### `getCorrection` / `getCorrectionFeatures`

```cpp
correction::Correction::Ref getCorrection(const std::string& key) const;
const std::vector<std::string>& getCorrectionFeatures(const std::string& key) const;
```

Low-level accessors for the loaded correction objects and their registered
input-variable lists. Typically not needed in analysis code.

### IKinematicFitManager

**Header**: `core/plugins/KinematicFitManager/KinematicFitManager.h`

Interface for kinematic fitting of reconstructed decay topologies.

Performs kinematic fitting using mass, momentum, and recoil constraints to improve four-momentum resolution of reconstructed particles. Supports flexible constraint configuration, collection-indexed particle selection, and optional resonance width (soft mass constraints).

#### Configuration

Create a kinematic fit configuration file with particle definitions and constraints:

```
# kfit.txt
name=VH_fit outputPrefix=kfit_
particles=lep1,lep2,jet1,jet2,MET
lep1.type=collection lep1.collection=Muon lep1.index=0
lep2.type=collection lep2.collection=Muon lep2.index=1
jet1.type=collection jet1.collection=Jet jet1.index=0
jet2.type=collection jet2.collection=Jet jet2.index=1
MET.type=recoil MET.collection=MET

# Mass constraints
constraint1.type=mass constraint1.particles=lep1,lep2
constraint1.targetMass=91188.0 constraint1.massSigma=2495.0
constraint2.type=mass constraint2.particles=jet1,jet2
constraint2.targetMass=125090.0 constraint2.massSigma=3000.0

# Run control
runVar=do_kfit
```

**Configuration Parameters**:
- `name`: Fit name (identifier)
- `outputPrefix`: Prefix for output columns
- `particles`: Comma-separated list of particle names
- `{particle}.type`: Either `collection` (from branch) or `recoil` (MET)
- `{particle}.collection`: Branch name to read from
- `{particle}.index`: Collection index (for collection-type particles)
- `constraint{N}.type`: Constraint type (`mass`, `pT`, or `recoil`)
- `constraint{N}.particles`: Particles involved in constraint
- `constraint{N}.targetMass`: Target mass value (MeV)
- `constraint{N}.massSigma`: Resonance width for soft mass constraints (optional)
- `runVar`: Optional column name to control per-event fit execution

#### Methods

```cpp
class IKinematicFitManager : public IPluggableManager {
public:
    virtual void applyAllFits() = 0;
    virtual void applyFit(const std::string& fitName) = 0;
    virtual std::vector<std::string> getAllFitNames() const = 0;
};
```

#### Usage

```cpp
#include <KinematicFitManager.h>

// Add plugin
auto kfitMgr = std::make_unique<KinematicFitManager>(
    analyzer.getConfigurationProvider()
);
analyzer.addPlugin("kinematicFit", std::move(kfitMgr));

// Apply fits (defines output columns)
analyzer.getPlugin<IKinematicFitManager>("kinematicFit")->applyAllFits();

// Fitted four-momentum columns are now available:
// kfit_lep1_pt, kfit_lep1_eta, kfit_lep1_phi, kfit_lep1_mass, etc.
// Also: kfit_chi2, kfit_status
```

#### Output Columns

For each fit with `outputPrefix={prefix}`, the following columns are defined:
- `{prefix}{particle}_pt` - Fitted transverse momentum
- `{prefix}{particle}_eta` - Fitted pseudorapidity
- `{prefix}{particle}_phi` - Fitted azimuthal angle
- `{prefix}{particle}_mass` - Fitted mass
- `{prefix}chi2` - Fit χ² value
- `{prefix}status` - Fit status (0 = success)

### IGoldenJsonManager

**Header**: `core/plugins/GoldenJsonManager/GoldenJsonManager.h`

Interface for CMS golden JSON certification filtering.

Filters data events based on run/luminosity-section validity from CMS golden JSON certification files. Automatically skips MC (when `type != "data"`). Supports multiple JSON files for different eras with automatic merging.

#### Configuration

Create a file listing paths to golden JSON files:

```
# golden_json_files.txt
cfg/Cert_2022.json
cfg/Cert_2023.json
```

Reference this file in your main configuration:

```
# config.txt
type=data
goldenJsonConfig=cfg/golden_json_files.txt
```

#### Methods

```cpp
class IGoldenJsonManager : public IPluggableManager {
public:
    virtual void applyGoldenJson() = 0;
    virtual bool isGoodLumiSection(unsigned int run,
                                  unsigned int lumiSection) const = 0;
};
```

#### Usage

```cpp
#include <GoldenJsonManager.h>

// Add plugin
auto goldenJson = std::make_unique<GoldenJsonManager>(
    analyzer.getConfigurationProvider()
);
analyzer.addPlugin("goldenJson", std::move(goldenJson));

// Apply filter (automatic - reads run/luminosityBlock branches)
analyzer.getPlugin<IGoldenJsonManager>("goldenJson")->applyGoldenJson();
```

The plugin automatically filters events where `(run, luminosityBlock)` is not certified. For MC samples (when config has `type != "data"`), the plugin does nothing.

**Features**:
- Data-only filtering (automatic MC skip)
- Multi-file support (per-era JSONs merged automatically)
- Embedded JSON parser (no external dependencies)
- Fast lookup via hash-based data structure

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

### WeightManager

**Header**: `core/plugins/WeightManager/WeightManager.h`

Plugin that computes nominal and varied event weights from registered scale
factors and scalar normalizations. Supports systematic weight variations and
produces per-component audit statistics written to the meta ROOT file.

**Access pattern**:
```cpp
auto& wm = *analyzer.getPlugin<WeightManager>("weights");
```

#### Base Weight (Generator Weight)

The **first** component you should register is always the per-event generator
weight (e.g. `genWeight` in NanoAOD).  This is the "base weight" that all
other scale factors multiply on top of:

```cpp
wm.addScaleFactor("genWeight", "genWeight");
```

The generator weight branch is also tracked **independently** by `CounterService`
via the `counterWeightBranch` config key.  CounterService writes the total sum
of weights (`counter_weightSum_<sample>`) and the sum of weight signs
(`counter_weightSignSum_<sample>`) to the meta ROOT file.  These are consumed by
`StitchingDerivationTask` to derive per-bin stitching scale factors.  The two
roles are complementary:

| Mechanism | Purpose |
|-----------|---------|
| `counterWeightBranch` in cfg.txt | Accumulate sum-of-weights for normalisation |
| `wm.addScaleFactor("genWeight", "genWeight")` | Include generator weight in the per-event weight product |

Both must reference the same branch for the analysis to be consistent.

#### WeightAuditEntry

```cpp
struct WeightAuditEntry {
    std::string name;          // Human-readable label
    std::string column;        // DataFrame column that was audited
    double sumWeights;         // Sum of per-event weight values
    double meanWeight;         // Arithmetic mean
    double minWeight;          // Minimum value seen
    double maxWeight;          // Maximum value seen
    long long negativeCount;   // Events with weight < 0
    long long zeroCount;       // Events with weight == 0
};
```

Audit entries are populated after the event loop completes (in `finalize()`).

#### Methods

```cpp
void addScaleFactor(const std::string& name, const std::string& column);
```

Register a per-event scale factor column. The column must be defined on the
DataFrame before `defineNominalWeight()` is called.

```cpp
void addNormalization(const std::string& name, double value);
```

Register a scalar normalization factor applied uniformly to all events
(e.g. lumi × cross-section / sum_weights).

```cpp
void addWeightVariation(const std::string& name,
                        const std::string& upColumn,
                        const std::string& downColumn);
```

Register a systematic weight variation. The up/down columns must be defined on
the DataFrame before `defineVariedWeight()` is called for this variation.

```cpp
void defineNominalWeight(const std::string& outputColumn = "weight_nominal");
```

Schedule definition of the nominal weight column (product of all scale factor
columns × all scalar normalizations).

```cpp
void defineVariedWeight(const std::string& variationName,
                        const std::string& direction,
                        const std::string& outputColumn);
```

Schedule definition of a varied weight column. The named variation's scale
factor is replaced by its up/down variant; all other components remain at
nominal. `direction` is `"up"` or `"down"`.

```cpp
const std::string& getNominalWeightColumn() const;
```

Return the nominal weight column name (empty string if not yet defined).

```cpp
std::string getWeightColumn(const std::string& variationName,
                            const std::string& direction) const;
```

Return a varied weight column name (empty string if not registered).

```cpp
double getTotalNormalization() const;
```

Return the product of all values registered via `addNormalization()`.

```cpp
const std::vector<WeightAuditEntry>& getAuditEntries() const;
```

Return per-component audit statistics (populated after the event loop). One
entry is produced for the nominal weight column and for each varied weight
column that was defined.

**Example**:
```cpp
#include <WeightManager.h>

auto weightPlugin = std::make_unique<WeightManager>();
analyzer.addPlugin("weights", std::move(weightPlugin));

auto& wm = *analyzer.getPlugin<WeightManager>("weights");

// Register scale factors (columns already defined)
wm.addScaleFactor("pileup_sf", "pu_weight");
wm.addScaleFactor("btag_sf",   "btag_weight");

// Register scalar normalization: lumi * xsec / sum_weights
wm.addNormalization("lumi_xsec", 0.0412);

// Register systematic weight variations
wm.addWeightVariation("pileup", "pu_weight_up", "pu_weight_down");

// Define nominal weight column
wm.defineNominalWeight("weight_nominal");

// Define varied weight columns for systematic histograms
wm.defineVariedWeight("pileup", "up",   "weight_pileup_up");
wm.defineVariedWeight("pileup", "down", "weight_pileup_down");

analyzer.save();  // Execute event loop

// Inspect audit entries after run
for (const auto& e : wm.getAuditEntries()) {
    std::cout << e.name << ": mean=" << e.meanWeight
              << " neg=" << e.negativeCount << "\n";
}
```

---

### RegionManager

**Header**: `core/plugins/RegionManager/RegionManager.h`

Plugin that manages named analysis regions with optional hierarchy. Each
region is associated with a boolean filter column and an optional parent
region. Child regions are strict subsets of their parents; the main analysis
DataFrame is not modified.

> **Note**: In `finalize()`, RegionManager writes a region-summary `TNamed`
> to the `regions` TDirectory in the meta ROOT file.

**Access pattern**:
```cpp
auto& rm = *analyzer.getPlugin<RegionManager>("regions");
```

#### Methods

```cpp
void declareRegion(const std::string& name,
                   const std::string& filterColumn,
                   const std::string& parent = "");
```

Declare a named region. `filterColumn` is the boolean DataFrame column
selecting events in this region. `parent` is the name of an already-declared
parent region, or empty for a root region.

- Throws `std::runtime_error` if `name` is already declared or if `parent`
  has not yet been declared.

```cpp
ROOT::RDF::RNode getRegionDataFrame(const std::string& name) const;
```

Return the filtered RDataFrame node for the named region (all ancestor
filters applied). Throws if the region has not been declared.

```cpp
const std::vector<std::string>& getRegionNames() const;
```

Return all declared region names in declaration order.

```cpp
std::vector<std::string> validate() const;
```

Validate the region hierarchy without throwing. Returns a list of validation
error strings (duplicate names, missing parents, cycles). An empty list means
the hierarchy is valid.

```cpp
std::vector<std::string> getFilterChain(const std::string& name) const;
```

Return the ordered chain of boolean filter columns from the root region down
to (and including) the named region. For example, if `"signal"` has parent
`"presel"`, the chain is `{"pass_presel", "pass_signal"}`.

**Example**:
```cpp
#include <RegionManager.h>
#include <NDHistogramManager/NDHistogramManager.h>

auto rmPlugin  = std::make_unique<RegionManager>();
auto ndhPlugin = std::make_unique<NDHistogramManager>(configProvider);
analyzer.addPlugin("regions", std::move(rmPlugin));
analyzer.addPlugin("NDHistogramManager", std::move(ndhPlugin));

// Define boolean selection columns first
analyzer.Define("pass_presel", [](float pt){ return pt > 20.f; }, {"jet_pt"});
analyzer.Define("pass_sr",     [](float mva){ return mva > 0.8f; }, {"mva_score"});
analyzer.Define("pass_cr",     [](float mva){ return mva < 0.4f; }, {"mva_score"});

auto& rm  = *analyzer.getPlugin<RegionManager>("regions");
auto& ndh = *analyzer.getPlugin<NDHistogramManager>("NDHistogramManager");

// Declare regions (parent before child)
rm.declareRegion("presel",  "pass_presel");
rm.declareRegion("signal",  "pass_sr", "presel");
rm.declareRegion("control", "pass_cr", "presel");

// Bind NDHistogramManager — it will fill histograms for each declared region
// automatically using its internal region axis.  No raw DataFrames needed.
ndh.bindToRegionManager(&rm);

// Validate hierarchy (also called automatically in initialize())
auto errors = rm.validate();
if (!errors.empty()) {
    for (const auto& e : errors) std::cerr << e << "\n";
}

// Book and save — NDHistogramManager fills all regions in one event-loop pass.
ndh.bookConfigHistograms();
ndh.saveHists();
```

> **Note**: Prefer binding plugins to `RegionManager` over retrieving raw per-region
> DataFrames.  Plugins bound to `RegionManager` iterate over all regions
> internally and guarantee that the complete event loop is executed only once.

---

### CutflowManager

**Header**: `core/plugins/CutflowManager/CutflowManager.h`

Plugin that computes sequential cutflow and N-1 event count tables. Cuts are
registered programmatically; each `addCut()` captures the pre-filter DataFrame
state and applies the filter on the main analysis DataFrame. When bound to a
`RegionManager`, per-region cutflows are computed in a single event-loop pass.

> **Note**: In `finalize()`, CutflowManager writes `cutflow` and
> `cutflow_nminus1` TH1D histograms to the meta ROOT file. When regions are
> bound a `cutflow_regions` TH2D histogram is also written.

**Access pattern**:
```cpp
auto& cfm = *analyzer.getPlugin<CutflowManager>("cutflow");
```

#### Methods

```cpp
void addCut(const std::string& name, const std::string& boolColumn);
```

Register a cut. Captures the current DataFrame state before applying the
cut filter. **All boolean columns for every cut must be defined on the
DataFrame before the first `addCut()` call.**

```cpp
void bindToRegionManager(RegionManager* rm);
```

Bind this manager to a `RegionManager` for per-region cutflows. Must be
called before the event loop starts. Passing `nullptr` is a no-op.

```cpp
const std::vector<std::pair<std::string, ULong64_t>>& getCutflowCounts() const;
```

Return the sequential cutflow counts (populated after run). Each element is
`{cut_label, event_count}` in registration order.

```cpp
const std::vector<std::pair<std::string, ULong64_t>>& getNMinusOneCounts() const;
```

Return the N-1 counts: events passing all cuts except the named one.

```cpp
ULong64_t getTotalCount() const;
```

Return the total event count before any registered cuts.

```cpp
const std::vector<std::pair<std::string, ULong64_t>>&
getRegionCutflowCounts(const std::string& regionName) const;

const std::vector<std::pair<std::string, ULong64_t>>&
getRegionNMinusOneCounts(const std::string& regionName) const;

ULong64_t getRegionTotalCount(const std::string& regionName) const;
```

Per-region variants of the above. Only available after `bindToRegionManager()`
has been called. Throw `std::runtime_error` if `regionName` is unknown.

**Example**:
```cpp
#include <CutflowManager.h>
#include <RegionManager.h>

auto cfmPlugin = std::make_unique<CutflowManager>();
analyzer.addPlugin("cutflow", std::move(cfmPlugin));

// Define all boolean cut columns before first addCut()
analyzer.Define("pass_pt",  [](float pt) { return pt > 30.f; },             {"jet_pt"});
analyzer.Define("pass_eta", [](float eta){ return std::abs(eta) < 2.4f; },  {"jet_eta"});
analyzer.Define("pass_btag",[](float b)  { return b > 0.7f; },              {"btag_score"});

auto& cfm = *analyzer.getPlugin<CutflowManager>("cutflow");
cfm.addCut("pT > 30 GeV",   "pass_pt");
cfm.addCut("|eta| < 2.4",   "pass_eta");
cfm.addCut("b-tag",         "pass_btag");

// Optional: bind to RegionManager for per-region cutflows
cfm.bindToRegionManager(analyzer.getPlugin<RegionManager>("regions"));

analyzer.save();

// Inspect results
std::cout << "Total events: " << cfm.getTotalCount() << "\n";
for (const auto& [name, count] : cfm.getCutflowCounts()) {
    std::cout << name << ": " << count << "\n";
}
for (const auto& [name, count] : cfm.getNMinusOneCounts()) {
    std::cout << name << " (N-1): " << count << "\n";
}
// Per-region
for (const auto& [name, count] : cfm.getRegionCutflowCounts("signal")) {
    std::cout << "[signal] " << name << ": " << count << "\n";
}
```

---

### PhysicsObjectCollection

**Header**: `core/interface/PhysicsObjectCollection.h`

Wrapper for a selected subset of physics objects (jets, leptons, etc.)
designed for use inside RDataFrame `Define` lambdas. Stores one ROOT
`LorentzVector` (PxPyPzM4D) per selected object together with its original
index in the full collection.

#### Constructors

```cpp
// Default – empty collection
PhysicsObjectCollection();

// From pt/eta/phi/mass columns and a boolean selection mask
PhysicsObjectCollection(const RVec<Float_t>& pt,
                        const RVec<Float_t>& eta,
                        const RVec<Float_t>& phi,
                        const RVec<Float_t>& mass,
                        const RVec<bool>&    mask);

// From pt/eta/phi/mass columns and an explicit index list
// (out-of-bounds indices are silently skipped)
PhysicsObjectCollection(const RVec<Float_t>& pt,
                        const RVec<Float_t>& eta,
                        const RVec<Float_t>& phi,
                        const RVec<Float_t>& mass,
                        const RVec<Int_t>&   indices);
```

#### Core Access

```cpp
std::size_t size() const;           // Number of selected objects
bool        empty() const;          // True if no objects selected

const LorentzVec& at(std::size_t i) const;         // 4-vector of i-th object
const std::vector<LorentzVec>& vectors() const;    // All 4-vectors

Int_t index(std::size_t i) const;                  // Original index of i-th object
const std::vector<Int_t>& indices() const;         // All original indices

// Extract values for selected objects from a full-collection branch.
// Out-of-bound entries are filled with T(-9999).
template<typename T>
RVec<T> getValue(const RVec<T>& branch) const;
```

#### Feature Caching

```cpp
template<typename T>
void cacheFeature(const std::string& name, T value);

template<typename T>
const T& getCachedFeature(const std::string& name) const;

bool hasCachedFeature(const std::string& name) const;
```

Store and retrieve arbitrary derived quantities (e.g. `RVec<float>` of
b-tag scores) by name, avoiding recomputation.

#### Overlap Removal

```cpp
PhysicsObjectCollection removeOverlap(const PhysicsObjectCollection& other,
                                      float deltaRMin) const;

static float deltaR(const LorentzVec& v1, const LorentzVec& v2);
```

Return a new collection with objects within ΔR < `deltaRMin` of any object
in `other` removed. The cached-feature store is not propagated to the result.

#### Combinatoric Free Functions

```cpp
// All unique same-collection pairs (i < j)
std::vector<ObjectPair> makePairs(const PhysicsObjectCollection& col);

// All cross-collection pairs (every col1 × every col2)
std::vector<ObjectPair> makeCrossPairs(const PhysicsObjectCollection& col1,
                                       const PhysicsObjectCollection& col2);

// All unique same-collection triplets (i < j < k)
std::vector<ObjectTriplet> makeTriplets(const PhysicsObjectCollection& col);
```

`ObjectPair` and `ObjectTriplet` carry:
- `p4` – combined Lorentz 4-vector (sum of individual 4-vectors)
- `first`, `second` (and `third` for triplets) – within-collection indices

#### TypedPhysicsObjectCollection\<T\>

```cpp
template<typename ObjectType>
class TypedPhysicsObjectCollection : public PhysicsObjectCollection {
public:
    // Same constructors as the base class, plus an objectsAll argument
    TypedPhysicsObjectCollection(
        const RVec<Float_t>& pt, const RVec<Float_t>& eta,
        const RVec<Float_t>& phi, const RVec<Float_t>& mass,
        const RVec<bool>& mask,
        const std::vector<ObjectType>& objectsAll);

    TypedPhysicsObjectCollection(
        const RVec<Float_t>& pt, const RVec<Float_t>& eta,
        const RVec<Float_t>& phi, const RVec<Float_t>& mass,
        const RVec<Int_t>& indices,
        const std::vector<ObjectType>& objectsAll);

    const ObjectType& object(std::size_t i) const;        // i-th user object
    const std::vector<ObjectType>& objects() const;       // all user objects
};
```

Extends the base class to attach a user-defined type per selected object
(e.g. a custom calibration struct or decorated object record).

#### PhysicsObjectVariationMap

```cpp
using PhysicsObjectVariationMap =
    std::unordered_map<std::string, PhysicsObjectCollection>;
```

Convenience type alias for holding systematic variations of the same object
collection (e.g. `"nominal"`, `"JEC_up"`, `"JEC_down"`).

**Example**:
```cpp
#include <PhysicsObjectCollection.h>

// Build a selected jet collection inside a Define lambda
analyzer.Define("goodJets",
    [](const RVec<float>& pt, const RVec<float>& eta,
       const RVec<float>& phi, const RVec<float>& mass) {
        RVec<bool> mask = (pt > 30.f) && (ROOT::VecOps::abs(eta) < 2.4f);
        return PhysicsObjectCollection(pt, eta, phi, mass, mask);
    },
    {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
);

// Use inside another lambda
analyzer.Define("dijet_mass",
    [](const PhysicsObjectCollection& jets,
       const RVec<float>& btagScore) {
        // Cache b-tag scores for the selected jets
        auto scores = jets.getValue(btagScore);
        // Build all unique pairs
        auto pairs = makePairs(jets);
        if (pairs.empty()) return -1.f;
        return static_cast<float>(pairs[0].p4.M());
    },
    {"goodJets", "Jet_btagScore"}
);

// Variation map for systematics
PhysicsObjectVariationMap jetVars;
jetVars["nominal"] = PhysicsObjectCollection(pt_nom, eta, phi, mass_nom, mask);
jetVars["JEC_up"]  = PhysicsObjectCollection(pt_up,  eta, phi, mass_up,  mask_up);
```

---

### ProvenanceService

**Header**: `core/interface/ProvenanceService.h`

Analysis service that automatically records complete provenance metadata into
the `provenance` TDirectory of the meta ROOT file. All entries are stored as
`TNamed` objects (name = key, title = value) and can be read back from Python
with PyROOT or uproot.

> **Note**: For task-level provenance, use
> `analyzer.setTaskMetadata(key, value)` rather than accessing
> `ProvenanceService` directly.

#### Automatically Recorded Entries

| Key | Description |
|-----|-------------|
| `framework.git_hash` | Git commit SHA of RDFAnalyzerCore (captured at CMake configure time) |
| `framework.git_dirty` | Whether the framework tree had uncommitted changes at configure time |
| `framework.build_timestamp` | UTC timestamp when the framework was configured |
| `framework.compiler` | Compiler ID and version |
| `root.version` | ROOT version string |
| `analysis.git_hash` | Git commit SHA of the analysis repository (queried at run time) |
| `analysis.git_dirty` | Whether the analysis tree had uncommitted changes at run time |
| `env.container_tag` | Container/runtime tag (`CONTAINER_TAG`, `APPTAINER_NAME`, `SINGULARITY_NAME`, or `DOCKER_IMAGE`) |
| `executor.num_threads` | Number of ROOT implicit-MT threads at finalize() time |
| `config.hash` | MD5 digest of the serialised configuration map (sorted key=value pairs) |
| `filelist.hash` | MD5 digest of the file referenced by the `fileList` config key |
| `plugin.<role>` | Type name of each registered plugin, keyed by its role |
| `file.hash.<cfg_key>` | MD5 digest of any config value that looks like a file path (`.json`, `.root`, `.onnx`, `.bdt`, `.pt`, `.pb`, `.xml`, `.yaml`, `.yml`) |

#### Methods

```cpp
void addEntry(const std::string& key, const std::string& value);
```

Manually add or overwrite a provenance entry. Can be called at any time
before `finalize()`.

```cpp
void recordDatasetManifestProvenance(
    const std::string& manifestFileHash,
    const std::string& queryParamsJson,
    const std::string& resolvedEntries);
```

Record dataset manifest identity. Stores three entries:
- `dataset_manifest.file_hash` – hash of the manifest file
- `dataset_manifest.query_params` – JSON-encoded query filter
- `dataset_manifest.resolved_entries` – comma-separated selected entry names

Any parameter may be an empty string when not available.

```cpp
const std::unordered_map<std::string, std::string>& getProvenance() const;
```

Read-only access to the full provenance map.

```cpp
static std::string hashString(const std::string& data);
```

Compute the MD5 hex digest of an arbitrary string. Exposed as a public
static helper so plugins can produce consistent hashes for their own
`config_hash` entries without duplicating hashing logic.

**Example**:
```cpp
// Add custom task-level metadata before the event loop
ProvenanceService* prov =
    analyzer.getService<ProvenanceService>("provenance");
prov->addEntry("task.workflow_id", "wf_12345");
prov->addEntry("task.attempt",     "1");

// Record the dataset manifest used
prov->recordDatasetManifestProvenance(
    manifestHash,       // e.g. from DatasetManifest.file_hash
    "{\"era\":\"2022\", \"type\":\"data\"}",
    "Run2022C,Run2022D"
);

analyzer.save();
```

```python
# Read provenance from Python
import ROOT
f = ROOT.TFile("output_meta.root")
d = f.Get("provenance")
for key in d.GetListOfKeys():
    print(key.GetName(), "=", key.ReadObj().GetTitle())
```

---

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
