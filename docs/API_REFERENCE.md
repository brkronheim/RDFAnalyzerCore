# API Reference

Complete API documentation for RDFAnalyzerCore interfaces and key classes.

## Table of Contents

- [Analyzer](#analyzer)
- [Configuration Interfaces](#configuration-interfaces)
- [Data Management](#data-management)
- [Plugin Interfaces](#plugin-interfaces)
- [Manager Implementations](#manager-implementations)
    - [CorrectionManager](#correctionmanager)
    - [CorrectedObjectCollectionManagers](#correctedobjectcollectionmanagers)
  - [WeightManager](#weightmanager)
  - [RegionManager](#regionmanager)
  - [CutflowManager](#cutflowmanager)
    - [ObjectEnergyManagerBase](#objectenergymanagerbase)
    - [ElectronEnergyScaleManager](#electronenergyscalemanager)
    - [MuonRochesterManager](#muonrochestermanager)
  - [JetEnergyScaleManager](#jetenergyscalemanager)
  - [PhysicsObjectCollection](#physicsobjectcollection)
  - [ProvenanceService](#provenanceservice)
- [Utility Classes](#utility-classes)

## Analyzer

**Header**: `core/interface/analyzer.h`

The main facade class that orchestrates the analysis.

### Construction

```cpp
Analyzer(std::string configFile,
         std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins = {});

Analyzer(std::unique_ptr<IConfigurationProvider> configProvider,
         std::unique_ptr<IDataFrameProvider> dataFrameProvider,
         std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins,
         std::unique_ptr<ISystematicManager> systematicManager,
         std::unique_ptr<ILogger> logger,
         std::unique_ptr<IOutputSink> skimSink,
         std::unique_ptr<IOutputSink> metaSink);
```

**Parameters**:
- `configFile`: Path to main configuration file
- `plugins`: Optional map of plugin role names to plugin instances

**Example**:
```cpp
// Simple construction
Analyzer analyzer("config.txt");

// With custom plugins
std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
plugins["custom"] = std::make_shared<MyPlugin>(...);
Analyzer analyzer("config.txt", std::move(plugins));
```

### Variable Definition

#### Define

```cpp
template<typename F>
Analyzer* Define(std::string name,
                 F function,
                 const std::vector<std::string>& columns = {});
```

Define a new DataFrame column.

**Parameters**:
- `name`: Name of the new column
- `function`: Lambda or function to compute the column
- `columns`: Input column names (empty for no dependencies)

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
// Register existing shifted inputs before defining their derived output.
auto& systematics = analyzer.getSystematicManager();
systematics.registerVariationColumns("pt", "jes", "pt_jesUp", "pt_jesDown");
analyzer.Define("pt_gev_systematic", [](float pt) { return pt / 1000.0; }, {"pt"});
// The provider also defines pt_gev_systematic_jesUp and
// pt_gev_systematic_jesDown from the mapped
// input columns; the callable receives only the listed input values.
```

#### DefinePerSample

```cpp
template<typename F>
Analyzer* DefinePerSample(std::string name, F function);
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
template<typename F>
Analyzer* Filter(std::string name,
                 F function,
                 const std::vector<std::string>& columns = {});
```

Apply an event filter.
When systematic variations are registered, the predicate is evaluated for
each variation and combined with the prior cumulative decision for that same
variation. The dataframe retains an event if at least one variation passes all
filters. NDHistogramManager receives the cumulative per-variation mask and skips
histogram fills for variations that fail. Register variations before filters
that should use them; variations added later cannot recover rows already
removed by earlier filters. The current mask column for a label can be
inspected with `analyzer.getSystematicManager().getSelectionMaskColumn(label)`.

**Parameters**:
- `name`: Filter label (stored internally as `pass_<name>`)
- `function`: Lambda returning bool (true = keep event)
- `columns`: Input column names

**Example**:
```cpp
// Named filter
analyzer.Filter("pt_cut",
    [](float pt) { return pt > 25.0; },
    {"jet_pt"}
);

// Another named filter
analyzer.Filter("jet_mult",
    [](int n_jets) { return n_jets >= 4; },
    {"n_jets"}
);
```

### Plugin Access

#### getPlugin

```cpp
template<typename T>
std::shared_ptr<T> getPlugin(const std::string& role) const;
```

Retrieve a plugin by role name with type checking.

**Template Parameter**: Interface type to cast to

**Returns**: Shared pointer to plugin

**Throws**: `std::out_of_range` if the role is not registered, or `std::bad_cast` if the registered plugin cannot be converted to `T`. Use `findPlugin<T>()` for an optional lookup; it returns `nullptr` when the role is absent or has another type.

**Example**:
```cpp
auto onnxMgr = OnnxManager::create(analyzer);  // Registers as "onnxManager"
onnxMgr->applyAllModels();

auto optionalOnnxMgr = analyzer.findPlugin<OnnxManager>("onnxManager");
if (optionalOnnxMgr) optionalOnnxMgr->applyAllModels();

auto histMgr = NDHistogramManager::create(analyzer);
```

### Manager Access

```cpp
IConfigurationProvider& getConfigurationProvider() const;
IDataFrameProvider& getDataFrameProvider() const;
ISystematicManager& getSystematicManager() const;
```

Access core managers.

**Example**:
```cpp
auto& config = analyzer.getConfigurationProvider();
std::string outputFile = config.get("saveFile");

auto df = analyzer.getDataFrameProvider().getDataFrame();
```

### Execution

#### run

```cpp
Analyzer* run();
```

Unified run entry point with optional skim writing.

This method:
1. Executes plugin pre-run hooks
2. Writes skim only if `enableSkim=1|true|True`
3. Saves ND histograms when the `histogramManager` role is registered
4. Finalizes services, then plugins, reports plugin metadata, and writes provenance

The skim snapshot is written only when `enableSkim` is set to `1`, `true`, or `True`. `saveFile` is the skim path; the metadata/histogram output uses `metaFile` when configured and otherwise derives a sibling `<stem>_meta.root` path from `saveFile`. Configured skim branches are nominal-only by default; set `skimSystematics=true` to include their registered paired Up/Down columns. Set `enableSystematics=false` to keep the complete Analyzer graph nominal-only.

**Example**:
```cpp
analyzer.Define(...);
analyzer.Filter(...);
// ... define analysis ...
analyzer.run();  // Execute unified flow
```

## Configuration Interfaces

### IConfigurationProvider

**Header**: `core/interface/api/IConfigurationProvider.h`

Interface for accessing configuration values.

#### Methods

```cpp
virtual std::string get(const std::string& key) const = 0;
virtual void set(const std::string& key, const std::string& value) = 0;
virtual const std::unordered_map<std::string, std::string>& getConfigMap() const = 0;
virtual std::vector<std::string> getList(const std::string& key,
                                         const std::vector<std::string>& defaultValue = {},
                                         const std::string& delimiter = ",") const = 0;
```

Basic key-value retrieval.

**Example**:
```cpp
std::string saveFile = config->get("saveFile");
config->set("threads", "8");
auto allConfig = config->getConfigMap();
```

#### Specialized Parsers

```cpp
virtual std::vector<std::unordered_map<std::string, std::string>>
    parseMultiKeyConfig(const std::string& configFile,
                        const std::vector<std::string>& requiredEntryKeys) const = 0;

virtual std::unordered_map<std::string, std::string>
    parsePairBasedConfig(const std::string& configFile) const = 0;
```

Parse a config file with key=value pairs.

**Example**:
{% raw %}
```cpp
// For file with: "const1=1.0\nconst2=2.0"
auto pairs = config->parsePairBasedConfig("constants.txt");
// Returns: {{"const1", "1.0"}, {"const2", "2.0"}}
```
{% endraw %}

Parse a config file with multi-key lines (plugin configs). It returns a vector of unordered maps, one per parsed entry.

**Example**:
{% raw %}
```cpp
// For file with: "file=model.onnx name=score inputVariables=pt,eta runVar=has_jets"
auto configs = config->parseMultiKeyConfig(
    config->get("onnxConfig"),
    {"file", "name", "inputVariables", "runVar"});
// Returns: [{{"file": "model.onnx", "name": "score", 
//             "inputVariables": "pt,eta", "runVar": "has_jets"}}]
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
virtual ROOT::RDF::RNode getDataFrame() = 0;
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
void Define(std::string name,
            F function,
            const std::vector<std::string>& columns,
            ISystematicManager& sysMgr);
```

Define a new column with systematic support.

```cpp
template<typename F>
void DefinePerSample(std::string name, F function);
```

Define a per-sample column.

```cpp
void DefineVector(std::string name,
                  const std::vector<std::string>& columns,
                  std::string type,
                  ISystematicManager& sysMgr);
```

Create a vector column from scalar or RVec inputs.
For each registered paired systematic affecting an input, the provider also
creates `<name>_<systematic>Up` and `<name>_<systematic>Down`. Each varied
vector uses shifted inputs for that family and nominal inputs for unaffected
columns, and the output is registered so later `Define` calls can propagate
the same variation. If a custom output mapping was registered for `name`, its
mapped output branch names are used. Explicit unpaired labels are not expanded
by this API.

Framework-generated packed variation vectors use the separate
`DefineVectorNominal(...)` helper so a vector that already contains one entry
per variation is not expanded a second time. Analysis code should normally use
`Analyzer::DefineVector` or `DefineVector` above.

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
```

Apply event filters.
For `Analyzer::Filter`, predicate columns are created for affected paired
systematics and explicitly registered labels. The row filter keeps events
passing at least one full variation path. Cumulative per-variation masks are
passed to NDHistogramManager, which suppresses fills for failed variations.

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
class IPluggableManager : public IContextAware {
public:
    virtual ~IPluggableManager() = default;
    virtual std::string type() const = 0;
    virtual void setupFromConfigFile() = 0;
    virtual std::vector<std::string> getDependencies() const { return {}; }
    virtual std::vector<std::string> getRequiredColumns() const { return {}; }
    virtual std::vector<std::string> getProducedColumns() const { return {}; }
    virtual void initialize() {}
    virtual void execute() {}
    virtual void finalize() {}
    virtual void reportMetadata() {}
    virtual std::unordered_map<std::string, std::string>
    collectProvenanceEntries() const { return {}; }
};
```

#### Methods

- `type()`: Returns the plugin type name for metadata and diagnostics.
- `setupFromConfigFile()`: Load plugin-specific configuration after context injection.
- `getDependencies()`: Declare plugin role dependencies for wiring order.
- `getRequiredColumns()`: Declare dataframe inputs used by the plugin.
- `getProducedColumns()`: Declare dataframe outputs produced by the plugin.
- `initialize()`: Called after all plugins are wired and configured.
- `execute()`: Called immediately before the RDataFrame computation.
- `finalize()`: Called after the RDataFrame computation completes.
- `reportMetadata()`: Called after finalize() for metadata emission.
- `collectProvenanceEntries()`: Contribute structured provenance entries.

### IContextAware

**Header**: `core/interface/api/IContextAware.h`

Interface for plugins that need access to core managers.

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

**Example**:
```cpp
class MyPlugin : public IPluggableManager {
public:
    void setContext(ManagerContext& ctx) override {
        context_ = &ctx;
    }
    
    void initialize() override {
        context_->logger.log(ILogger::Level::Info, "MyPlugin initialized");
    }
    
private:
    ManagerContext* context_ = nullptr;
};
```

### NamedObjectManager<T>

**Header**: `core/plugins/NamedObjectManager/NamedObjectManager.h`

Base class for plugins managing collections of named objects.

```cpp
template<typename T>
class NamedObjectManager : public IPluggableManager {
public:
    const T& getObject(const std::string& key) const;
    const std::vector<std::string>& getFeatures(const std::string& key) const;
    std::string type() const override;
    void setContext(ManagerContext& ctx) override;
    void setupFromConfigFile() override;
};
```

#### Methods

```cpp
const T& getObject(const std::string& key) const;
```

Retrieve an object by name. Throws `std::runtime_error` if the key is not found.

```cpp
const std::vector<std::string>& getFeatures(const std::string& key) const;
```

Retrieve the feature names associated with an object. Throws `std::runtime_error` if the key is not found.

## Manager Implementations

### BDTManager

**Header**: `core/plugins/BDTManager/BDTManager.h`

Class for BDT model management.

`BDTManager::create(analyzer)` immediately parses `bdtConfig`; set that key in
the main config before creating the manager. Each model row requires `file`,
`name`, `inputVariables`, and `runVar`.

```cpp
class BDTManager : public NamedObjectManager<std::shared_ptr<fastforest::FastForest>> {
public:
    void applyBDT(const std::string& bdtName);
    void applyAllBDTs();
    std::shared_ptr<fastforest::FastForest> getBDT(const std::string& key) const;
    const std::vector<std::string>& getBDTFeatures(const std::string& key) const;
    const std::string& getRunVar(const std::string& bdtName) const;
};
```

#### Methods

```cpp
void applyBDT(const std::string& bdtName);
```

Apply a specific BDT model to the DataFrame.

**Parameters**:
- `bdtName`: Name of the configured model

```cpp
void applyAllBDTs();
```

Apply all configured BDT models.

**Example**:
```cpp
auto bdtMgr = BDTManager::create(analyzer);  // Registers as "bdtManager"
bdtMgr->applyBDT("signal_bdt");   // Creates "signal_bdt" column
bdtMgr->applyAllBDTs();             // Apply all configured models
```

### OnnxManager

**Header**: `core/plugins/OnnxManager/OnnxManager.h`

Class for ONNX model management.

`OnnxManager::create(analyzer)` immediately parses `onnxConfig`; set that key
in the main config before creating the manager. Each model row requires
`file`, `name`, `inputVariables`, and `runVar`.

```cpp
class OnnxManager : public NamedObjectManager<std::shared_ptr<Ort::Session>> {
public:
    void applyModel(const std::string& modelName, const std::string& outputSuffix = "");
    void applyAllModels(const std::string& outputSuffix = "");
    std::shared_ptr<Ort::Session> getModel(const std::string& key) const;
    const std::vector<std::string>& getModelFeatures(const std::string& key) const;
    const std::string& getRunVar(const std::string& modelName) const;
    const std::vector<std::string>& getModelInputNames(const std::string& modelName) const;
    const std::vector<std::string>& getModelOutputNames(const std::string& modelName) const;
};
```

**Note**: Models with multiple outputs create columns named `{modelName}_output0`, `{modelName}_output1`, etc.

**Example**:
```cpp
auto onnxMgr = OnnxManager::create(analyzer);  // Registers as "onnxManager"

// Apply single-output model
onnxMgr->applyModel("classifier");  // Creates "classifier" column

// Apply multi-output model
onnxMgr->applyModel("transformer");
// Creates: transformer_output0, transformer_output1, ...
```

### SofieManager

**Header**: `core/plugins/SofieManager/SofieManager.h`

Class for SOFIE model management.

```cpp
class SofieManager : public NamedObjectManager<std::shared_ptr<SofieInferenceFunction>> {
public:
    void registerModel(
        const std::string& name,
        std::shared_ptr<SofieInferenceFunction> inferenceFunc,
        const std::vector<std::string>& features,
        const std::string& runVar);
    void applyModel(const std::string& modelName);
    void applyAllModels();
    std::shared_ptr<SofieInferenceFunction> getModel(const std::string& key) const;
    const std::vector<std::string>& getModelFeatures(const std::string& key) const;
    const std::string& getRunVar(const std::string& modelName) const;
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

auto sofieMgr = SofieManager::create(analyzer);  // Registers as "sofieManager"
auto func = std::make_shared<SofieInferenceFunction>(myInference);
sofieMgr->registerModel("my_model", func, {"pt", "eta"}, "run_model");
sofieMgr->applyModel("my_model");
```

### CorrectionManager

**Header**: `core/plugins/CorrectionManager/CorrectionManager.h`

Applies scale factors and other corrections loaded from correctionlib JSON
files.

> **See also**: [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)

The manager now supports both correctionlib `corrections` and `compound_corrections` through the same registration and application APIs.

#### `applyCorrection`

```cpp
void applyCorrection(const std::string& correctionName,
                     const std::vector<std::string>& stringArguments,
                     const std::vector<std::string>& inputColumns = {},
                     const std::string& outputBranch = "");
```

Evaluates the named correction once per event using scalar input columns and
defines a `Float_t` output column.

- `correctionName`: Key registered in the configuration or via `registerCorrection(...)`.
- `stringArguments`: Constant string values for all `string`-typed inputs
  declared in the correctionlib JSON, supplied in the order they appear in the
  JSON.
- `inputColumns`: Optional override for the numeric RDF input columns.
- `outputBranch`: Optional explicit output column name.

When `outputBranch` is empty, the derived name is:

- `correctionName` if there are no string arguments
- `correctionName_<arg1>_<arg2>_...` otherwise

**Example**:
```cpp
correctionManager.applyCorrection("muon_sf", {"nominal"});
// Adds a Float_t column "muon_sf_nominal" to the dataframe

correctionManager.applyCorrection(
    "pileup_weight", {"up"}, {}, "pileup_weight_up");
```

#### `applyCorrectionVec`

```cpp
void applyCorrectionVec(const std::string& correctionName,
                        const std::vector<std::string>& stringArguments,
                        const std::vector<std::string>& inputColumns = {},
                        const std::string& outputBranch = "");
```

Evaluates the named correction for every object in a collection and defines a
`ROOT::VecOps::RVec<Float_t>` output column.

Use this method instead of `applyCorrection` when at least one registered
numeric input is an `RVec`.

- `correctionName`: Key registered in the configuration or via `registerCorrection(...)`.
- `stringArguments`: Constant string values applied to every object in the
  collection (same semantics as in `applyCorrection`).
- `inputColumns`: Optional override for the numeric RDF input columns.
- `outputBranch`: Optional explicit output column name.

The method packs vector inputs per object and broadcasts scalar event inputs
across the object loop automatically. This matches CMS payloads that mix
per-jet features with per-event quantities like `rho`.

**Example**:
```cpp
correctionManager.applyCorrectionVec("jet_sf", {"nominal"});
// Adds an RVec<Float_t> column "jet_sf_nominal"

// Use the per-jet scale factors downstream:
analyzer.Define("corrected_jet_pt",
    [](const ROOT::VecOps::RVec<float>& pt,
       const ROOT::VecOps::RVec<Float_t>& sf) { return pt * sf; },
    {"jet_pt", "jet_sf_nominal"}
);

// Mixed vector + scalar inputs are also supported.
correctionManager.applyCorrectionVec(
    "jec_nominal", {},
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"},
    "Jet_jec_sf_nominal");
```

#### `registerCorrection`

```cpp
void registerCorrection(const std::string& name,
                        const std::string& file,
                        const std::string& correctionlibName,
                        const std::vector<std::string>& inputVariables);
```

Registers either a regular correction or a compound correction from a JSON file.

#### `getCorrection` / `getCompoundCorrection` / `getCorrectionFeatures`

```cpp
correction::Correction::Ref getCorrection(const std::string& key) const;
correction::CompoundCorrection::Ref getCompoundCorrection(const std::string& key) const;
const std::vector<std::string>& getCorrectionFeatures(const std::string& key) const;
```

Low-level accessors for the loaded correction objects and their registered
input-variable lists. Typically not needed in analysis code.

---

### ObjectEnergyManagerBase

**Header**: `core/plugins/ObjectEnergyManagerBase/ObjectEnergyManagerBase.h`

Shared base implementation for electron, photon, tau, and muon object-energy corrections.

> **See also**: [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)

Key capabilities:

- `applyCorrection(...)` for multiplicative object-scale updates
- `applyResolutionSmearing(...)` for additive Gaussian smearing
- `applyCorrectionlib(...)` for direct `CorrectionManager` integration
- `defineReproducibleGaussian(...)` for deterministic MC smearing
- `propagateMET(...)` for Type-1 style MET updates
- `addVariation(...)` and variation-collection outputs

`execute()` now clears queued correction and variation steps after materializing them. This makes explicit pre-materialization safe when downstream `Define(...)` calls need corrected columns immediately.

### CorrectedObjectCollectionManagers

**Header**: `core/plugins/CorrectedObjectCollectionManagers/CorrectedObjectCollectionManagers.h`

Thin wrapper plugins that expose corrected full-object collections directly to
analysis code.

Available concrete managers:

- `CorrectedJetCollectionManager`
- `CorrectedFatJetCollectionManager`
- `CorrectedElectronCollectionManager`
- `CorrectedMuonCollectionManager`
- `CorrectedTauCollectionManager`
- `CorrectedPhotonCollectionManager`

Shared behavior:

- optionally auto-build an input `PhysicsObjectCollection` from raw component
    branches
- optionally replay an ordered `workflowConfig` into `CorrectionManager` and
    the underlying correction manager
- schedule nominal corrected collection materialization on the underlying
    correction manager
- schedule varied collection outputs and optional
    `PhysicsObjectVariationMap`
- register the nominal collection base name with `SystematicManager`

Systematic behavior:

- the underlying correction manager registers a base systematic family such as
    `jes_total`, `ees`, `stat`, or `pes`
- explicit nominal-to-directional mappings are recorded through
    `registerVariationColumns(...)`
- generic `Define(...)` systematic expansion only sees mappings registered
  before that `Define(...)` call. Corrected-collection managers create their
  outputs in plugin `execute()` hooks during `Analyzer::run()`, after the
  analysis body has made its `Define(...)` calls, so do not assume those later
  outputs will propagate through earlier definitions.

Typical config keys:

```text
correctedJetCollectionConfig=cfg/corrected_jets.txt
correctedElectronCollectionConfig=cfg/corrected_electrons.txt
correctedMuonCollectionConfig=cfg/corrected_muons.txt
```

Pair-based config files may also include:

```text
workflowConfig=cfg/corrected_jets_workflow.txt
```

When present, `workflowConfig` is parsed as a multi-entry config whose rows are
executed in order during `setupFromConfigFile()`. Each row must define
`type=...`. Optional `sample=mc|data|all` gates a row on the current sample
class, inferred from the top-level `type` / `primaryDataset` config. Values of
the form `${key}` resolve against top-level config keys and corrected-wrapper
spec values like `${correctedPtColumn}` and `${outputCollection}`.

Supported workflow action families:

- Common: `registerCorrection`, `applyCorrectionVec`, `defineRelativeUncertaintyScaleFactors`
- Compact high-level actions: `applyRelativePtUncertaintySystematic`, `applyResolutionSmearingSystematic`, `applyScaleResolutionSystematics`, `applyCorrectionlibVariation`, `applyIndexedCorrectionlibVariations`
- Jet / fatjet low-level actions: `setJetColumns`, `setMETColumns`, `removeExistingCorrections`, `setRawPtColumn`, `setJERSmearingColumns`, `applyCorrection`, `applyCorrectionlib`, `applyJERSmearing`, `addVariation`, `propagateMET`, `registerSystematicSources`, `applySystematicSet`
- Electron / photon / tau low-level actions: `setObjectColumns`, `setMETColumns`, `defineReproducibleGaussian`, `applyCorrection`, `applyCorrectionlib`, `applyResolutionSmearing`, `addVariation`, `propagateMET`, `registerSystematicSources`
- Muon low-level additions: `setRochesterInputColumns`, `setScaleResolutionEventColumns`, `applyScaleAndResolution`, `applyRochesterCorrection`, `applyRochesterSystematicSet`

Recommended usage:

- Put stable column wiring such as `ptColumn`, `etaColumn`, `phiColumn`,
    `massColumn`, MET columns, and Rochester/JER setup columns in the corrected
    wrapper spec file.
- Use the compact high-level actions in `workflowConfig` for the actual
    correction families.
- Drop back to the low-level row-by-row actions only when a workflow does not
    match one of the compact patterns.

Compact-action defaults:

- `applyRelativePtUncertaintySystematic` can omit `inputPtColumn`,
    `uncertaintyColumn`, `scaleFactorPrefix`, and `outputPtPrefix`.
- `applyResolutionSmearingSystematic` can omit `inputPtColumn`, nominal output,
    `outputPtPrefix`, `sigmaColumnPrefix`, and string args.
- `applyScaleResolutionSystematics` can omit `inputPtColumn`, nominal output,
    and scale/smear naming fields.
- Low-level `applyCorrectionlib` rows in corrected object workflows can omit
    `inputPtColumn`/`outputPtColumn`; wrappers default these from the corrected
    collection spec.

See [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md) for the full configuration format
and usage pattern.

### ElectronEnergyScaleManager

**Header**: `core/plugins/ElectronEnergyScaleManager/ElectronEnergyScaleManager.h`

Thin concrete wrapper over `ObjectEnergyManagerBase` for electron workflows.

Typical CMS usage combines:

- `CorrectionManager` for `SmearAndSyst` or compound `Scale` payload evaluation
- `defineReproducibleGaussian(...)` for MC smearing
- `applyResolutionSmearing(...)` and `applyCorrection(...)` for nominal and shifted outputs

See [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md) for a full Run 3 pattern.

### MuonRochesterManager

**Header**: `core/plugins/MuonRochesterManager/MuonRochesterManager.h`

Concrete object-energy manager for CMS muon momentum corrections.

Additional public methods beyond the shared object-energy API:

```cpp
void setRochesterInputColumns(charge, genPt, nLayers, u1, u2);
void setScaleResolutionEventColumns(lumiColumn, eventColumn);
void applyRochesterCorrection(cm, correctionName, inputPtColumn, outputPtColumn);
void applyRochesterSystematicSet(cm, correctionName, setName, inputPtColumn, outputPtPrefix);
void applyScaleAndResolution(jsonFile, isData, inputPtColumn, outputPtColumn,
                             scaleVariation = "nom",
                             resolutionVariation = "nom");
```

Use `applyScaleAndResolution(...)` for the newer Run 3 split-schema payloads that expose separate `a_*`, `m_*`, `k_*`, `cb_params`, and `poly_params` corrections.

---

### KinematicFitManager

**Header**: `core/plugins/KinematicFitManager/KinematicFitManager.h`

Concrete manager for kinematic fitting of reconstructed decay topologies.

Performs kinematic fitting using mass, scalar-pT, Cartesian balance, and recoil constraints to improve four-momentum resolution of reconstructed particles. Supports flexible constraint configuration, collection-indexed particle selection, and optional Gaussian widths for mass and transverse constraints.

#### Configuration

Set `kinematicFitConfig` in the main config. Each line in the referenced file
defines one fit using required `name`, `particles`, and `constraints` fields.
Particle specs use `label:ptCol:etaCol:phiCol:massCol:type[:collectionIndex]`;
constraint indices are zero-based. Mass values and widths are in GeV.

```
# main config
kinematicFitConfig=cfg/kinematic_fits.txt

# cfg/kinematic_fits.txt: a two-lepton Z-mass fit
name=dileptonFit particles=mu1:mu1_pt:mu1_eta:mu1_phi:mu1_mass:lepton,mu2:mu2_pt:mu2_eta:mu2_phi:mu2_mass:lepton constraints=0+1:91.2:2.495
```

**Configuration Parameters**:
- `name`: Fit name (identifier)
- `particles`: Comma-separated particle specs in the format shown above
- `constraints`: Comma-separated mass constraints (`0+1:mass[:sigma]`,
  `0+1+2:mass[:sigma]`), pT constraints (`pt:idx:targetPt[:ptSigma]`), or
  Cartesian component constraints (`px:idx1+idx2+...:target[:sigma]` and
  `py:idx1+idx2+...:target[:sigma]`). A zero width is a hard constraint;
  positive widths are Gaussian widths in GeV. Cartesian constraints can express
  global transverse-momentum balance or soft zero-component priors.
- `metCartesianResolution`: Optional absolute isotropic MET px/py resolution
  in GeV. When positive, it replaces the fractional MET pT resolution for the
  MET particle and avoids the nonphysical high-pT turnover of a scalar MET
  constraint with a fractional uncertainty.
- `metUseMeasurement`: Optional boolean, default `true`. Set to `false` to
  treat the MET measurement as effectively unmeasured while retaining hard
  balance and configured soft MET-prior constraints.
- `runVar`: Optional boolean column that enables the fit per event
- `maxIterations`, `convergenceTolerance`, and resolution fields: optional
  fit controls with defaults defined by `KinFitConfig`
- `useGPU`: Optional GPU execution flag; requires a CUDA-enabled build

The `{fit}_chi2` output includes the measurement pulls and Gaussian penalties
from all soft mass, pT, px, and py constraints. Hard constraints contribute no
separate penalty because they are enforced directly.

#### Methods

```cpp
class KinematicFitManager : public NamedObjectManager<KinFitConfig> {
public:
    static std::shared_ptr<KinematicFitManager> create(
        Analyzer& an, const std::string& role = "kinematicFitManager");
    void applyAllFits();
    void applyFit(const std::string& fitName);
    std::vector<std::string> getAllFitNames() const;
};
```

#### Usage

```cpp
#include <KinematicFitManager.h>

// The factory registers the manager and loads its configured fit definitions.
auto kfitMgr = KinematicFitManager::create(analyzer);

// Apply fits (defines output columns)
kfitMgr->applyAllFits();

// For this config, outputs include:
// dileptonFit_chi2, dileptonFit_converged,
// dileptonFit_mu1_pt_fitted, dileptonFit_mu1_eta_fitted,
// dileptonFit_mu1_phi_fitted, and corresponding mu2 columns.
```

For a fit named `{fit}`, the manager defines `{fit}_chi2`,
`{fit}_converged`, and `{fit}_{particle}_{pt,eta,phi}_fitted` columns.

### GoldenJsonManager

**Header**: `core/plugins/cms/GoldenJsonManager/GoldenJsonManager.h`

Concrete manager for CMS golden JSON certification filtering.

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
void applyGoldenJson();
bool isValid(unsigned int run, unsigned int lumiSection) const;
```

#### Usage

```cpp
#include <GoldenJsonManager.h>

// The factory registers the manager and loads goldenJsonConfig.
auto goldenJson = GoldenJsonManager::create(analyzer);

// Apply filter (automatic - reads run/luminosityBlock branches)
goldenJson->applyGoldenJson();
```

The plugin automatically filters events where `(run, luminosityBlock)` is not certified. For MC samples (when config has `type != "data"`), the plugin does nothing.

**Features**:
- Data-only filtering (automatic MC skip)
- Multi-file support (per-era JSONs merged automatically)
- Embedded JSON parser (no external dependencies)
- Fast lookup via hash-based data structure

### TriggerManager

**Header**: `core/plugins/TriggerManager/TriggerManager.h`

Concrete trigger manager. `TriggerManager::create(analyzer)` requires
`triggerConfig` in the main config; each row requires `name`, `sample`, and
`triggers`, with optional `triggerVetos`.

```cpp
class TriggerManager : public NamedObjectManager<std::vector<std::string>> {
public:
    static std::shared_ptr<TriggerManager> create(
        Analyzer& an, const std::string& role = "triggerManager");
    const std::vector<std::string>& getTriggers(const std::string& group) const;
    const std::vector<std::string>& getVetoes(const std::string& group) const;
    std::vector<std::string> getAllGroups() const;
    void applyAllTriggers();
};
```

```cpp
auto triggerMgr = TriggerManager::create(analyzer);
triggerMgr->applyAllTriggers();
```

### NDHistogramManager

**Header**: `core/plugins/NDHistogramManager/NDHistogramManager.h`

Concrete `IPluggableManager` plugin for N-dimensional histogram management. `Analyzer::run()` automatically saves its booked histograms when it is registered under the `histogramManager` role.

```cpp
class NDHistogramManager : public IPluggableManager {
public:
    void bookND(std::vector<histInfo>& infos,
                std::vector<selectionInfo>& selections,
                const std::string& suffix,
                std::vector<std::vector<std::string>>& regionNames);
    void bookConfigHistograms();
    void saveHists();
};
```

#### Histogram Description Classes

```cpp
class histInfo {
public:
    histInfo(const char name[], const char variable[], const char label[],
             const char weight[], int bins, float lowerBound, float upperBound);
    const std::string& name() const;
    const std::string& variable() const;
    const std::string& label() const;
    const std::string& weight() const;
    const int& bins() const;
    const float& lowerBound() const;
    const float& upperBound() const;
};

class selectionInfo {
public:
    selectionInfo();
    selectionInfo(std::string variable, int bins, double lowerBound,
                  double upperBound);
    selectionInfo(std::string variable, int bins, double lowerBound,
                  double upperBound, std::vector<std::string> regions);
    const std::string& variable() const;
    const int& bins() const;
    const float& lowerBound() const;
    const float& upperBound() const;
    const std::vector<std::string>& regions() const;
};
```

These classes store immutable histogram and selection descriptions. Read their
values through the getters; `histInfo` uses `label()` for the axis label.

**Example**:
```cpp
#include <plots.h>

auto histMgr = NDHistogramManager::create(analyzer);

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

// Analyzer::run() executes and saves histograms booked under this role.
analyzer.run();
```

### WeightManager

**Header**: `core/plugins/WeightManager/WeightManager.h`

Plugin that computes nominal and varied event weights from registered scale
factors and scalar normalizations. Supports systematic weight variations and
produces per-component audit statistics written to the meta ROOT file.

**Access pattern**:
```cpp
// After WeightManager::create(analyzer) or addPlugin(..., "weightManager"):
auto& wm = *analyzer.getPlugin<WeightManager>("weightManager");
```

#### Base Weight (Generator Weight)

The **first** component you should register is always the per-event generator
weight (e.g. `genWeight` in NanoAOD).  This is the "base weight" that all
other scale factors multiply on top of:

```cpp
wm.addScaleFactor("genWeight", "genWeight");
```

The generator weight branch can also be tracked **independently** by
`CounterService` via the `counterWeightBranch` config key. Enable the service
with `enableCounters=true`; it is disabled by default, and setting only
`counterWeightBranch` does not create it. CounterService writes the total sum
of weights (`counter_weightSum_<sample>`) and the sum of weight signs
(`counter_weightSignSum_<sample>`) to the meta ROOT file.  These are consumed by
`StitchingDerivationTask` to derive per-bin stitching scale factors.  The two
roles are complementary:

| Mechanism | Purpose |
|-----------|---------|
| `counterWeightBranch` in cfg.txt | Accumulate sum-of-weights for normalisation |
| `wm.addScaleFactor("genWeight", "genWeight")` | Include generator weight in the per-event weight product |

Both should reference the same branch for the analysis to be consistent. For
example, set `enableCounters=true` and `counterWeightBranch=genWeight` in the
main config when normalization counters are needed.

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

WeightManager::create(analyzer);

auto& wm = *analyzer.getPlugin<WeightManager>("weightManager");

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

analyzer.run();  // Execute event loop

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
// After RegionManager::create(analyzer) or addPlugin(..., "regionManager"):
auto& rm = *analyzer.getPlugin<RegionManager>("regionManager");
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
#include <NDHistogramManager.h>

auto rmPlugin  = RegionManager::create(analyzer);
auto ndhPlugin = NDHistogramManager::create(analyzer);

// Define boolean selection columns first
analyzer.Define("pass_presel", [](float pt){ return pt > 20.f; }, {"jet_pt"});
analyzer.Define("pass_sr",     [](float mva){ return mva > 0.8f; }, {"mva_score"});
analyzer.Define("pass_cr",     [](float mva){ return mva < 0.4f; }, {"mva_score"});

auto& rm  = *rmPlugin;
auto& ndh = *ndhPlugin;

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

// Book histograms; Analyzer::run() saves them after the event loop.
ndh.bookConfigHistograms();
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
// After CutflowManager::create(analyzer) or addPlugin(..., "cutflowManager"):
auto& cfm = *analyzer.getPlugin<CutflowManager>("cutflowManager");
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

auto cfmPlugin = CutflowManager::create(analyzer);
auto rmPlugin = RegionManager::create(analyzer);

// Define all boolean cut columns before first addCut()
analyzer.Define("pass_pt",  [](float pt) { return pt > 30.f; },             {"jet_pt"});
analyzer.Define("pass_eta", [](float eta){ return std::abs(eta) < 2.4f; },  {"jet_eta"});
analyzer.Define("pass_btag",[](float b)  { return b > 0.7f; },              {"btag_score"});

// Declare a region from an already-defined boolean selection column.
analyzer.Define("pass_signal",
    [](bool passPt, bool passEta, bool passBtag) {
        return passPt && passEta && passBtag;
    }, {"pass_pt", "pass_eta", "pass_btag"});
rmPlugin->declareRegion("signal", "pass_signal");

auto& cfm = *analyzer.getPlugin<CutflowManager>("cutflowManager");
cfm.addCut("pT > 30 GeV",   "pass_pt");
cfm.addCut("|eta| < 2.4",   "pass_eta");
cfm.addCut("b-tag",         "pass_btag");

// Optional: bind to RegionManager for per-region cutflows
cfm.bindToRegionManager(rmPlugin.get());

analyzer.run();

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

### JetEnergyScaleManager

**Header**: `core/plugins/JetEnergyScaleManager/JetEnergyScaleManager.h`

> **Full reference**: [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md)

Plugin for applying CMS Jet Energy Scale (JES) and Jet Energy Resolution
(JER) corrections to jet collections, with support for named systematic source
sets, Type-1 MET propagation, and clean `PhysicsObjectCollection` integration.

> **See also**: [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md) and [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)

#### Configuration Methods

```cpp
// Declare input column names
void setJetColumns(ptCol, etaCol, phiCol, massCol);
void setMETColumns(metPtCol, metPhiCol);

// Stripping existing corrections
void removeExistingCorrections(rawFactorColumn);
void setRawPtColumn(rawPtColumn);                  // alternative
void setJERSmearingColumns(genJetPtColumn, rhoColumn, eventColumn);

// Applying scale-factor corrections
void applyCorrection(inputPt, sfCol, outputPt,
                     applyToMass=true, inputMass="", outputMass="");
void applyCorrectionlib(cm, correctionName, stringArgs, inputPt, outputPt, ...);
void applyJERSmearing(cm, ptResolutionCorrection, scaleFactorCorrection,
                      inputPt, outputPt, systematic="nom", ...);

// CMS systematic source sets
void registerSystematicSources(setName, sources);
void applySystematicSet(cm, correctionName, setName, inputPt, outputPtPrefix, ...);

// Type-1 MET propagation
void propagateMET(basePt, basePhi, nomPt, varPt,
                  outPt, outPhi, threshold=15.f);

// PhysicsObjectCollection integration
void setInputJetCollection(collectionColumn);
void defineCollectionOutput(correctedPt, outputCol, correctedMass="");
void defineVariationCollections(nominalCol, prefix, mapCol="");

// Direct variation registration
void addVariation(name, upPt, downPt, upMass="", downMass="");
```

#### Accessors

```cpp
const std::string& getRawPtColumn()              const;
const std::string& getPtColumn()                 const;
const std::string& getMassColumn()               const;
const std::string& getMETPtColumn()              const;
const std::string& getMETPhiColumn()             const;
const std::string& getInputJetCollectionColumn() const;
const std::vector<JESVariationEntry>& getVariations() const;
const std::vector<std::string>& getSystematicSources(setName) const;
```

#### CMS NanoAOD Usage

```cpp
auto jes = JetEnergyScaleManager::create(analyzer);
auto cm = CorrectionManager::create(analyzer);

// 1. Column declarations
jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
jes->setMETColumns("MET_pt", "MET_phi");

// 2. Strip NanoAOD JEC → raw pT
jes->removeExistingCorrections("Jet_rawFactor");

// 3. Evaluate the nominal compound JEC via CorrectionManager
cm->registerCorrection(
    "jec_nominal", "jet_jerc.json.gz",
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
cm->applyCorrectionVec("jec_nominal", {}, {}, "Jet_jec_sf_nominal");

// 4. Apply the resulting scale factors to jet pt and mass
jes->applyCorrection("Jet_pt_raw", "Jet_jec_sf_nominal", "Jet_pt_jec",
                     true, "Jet_mass_raw", "Jet_mass_jec");

// 5. Register reduced JES set (Total only) and apply
jes->registerSystematicSources("reduced", {"Total"});
jes->applySystematicSet(*cm, "jes_unc", "reduced",
                        "Jet_pt_jec", "Jet_pt_jes");

// 6. Configure and apply JER smearing
jes->setJERSmearingColumns("Jet_genMatchedPt",
                           "Rho_fixedGridRhoFastjetAll",
                           "event");
jes->applyJERSmearing(*cm, "jer_pt_resolution", "jer_scale_factor",
                      "Jet_pt_jec", "Jet_pt_jer_nominal");

// 7. Propagate JEC to MET
jes->propagateMET("MET_pt", "MET_phi",
                  "Jet_pt_raw", "Jet_pt_jec",
                  "MET_pt_jec", "MET_phi_jec");

// 8. PhysicsObjectCollection integration
jes->setInputJetCollection("goodJets");
jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
jes->defineVariationCollections("goodJets_jec", "goodJets",
                                "goodJets_variations");
// After execute():
//   "goodJets_jec"         : PhysicsObjectCollection (nominal JEC)
//   "goodJets_TotalUp"     : PhysicsObjectCollection (JES up)
//   "goodJets_TotalDown"   : PhysicsObjectCollection (JES down)
//   "goodJets_variations"  : PhysicsObjectVariationMap
```

`Analyzer::Define` takes exactly three arguments and expands over variation
mappings that already exist when the analysis calls it. `JetEnergyScaleManager`
registers and materializes its corrected variations in `execute()`, which runs
inside `Analyzer::run()` after those analysis-body calls. The code above
therefore stops at the manager outputs rather than promising that a pre-run
`Define` will consume and propagate these later-created collections.

See [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md) for the complete
reference including the full systematic source list, MET propagation details,
and a production-ready end-to-end example.

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

#### Sub-Collection Filtering

```cpp
PhysicsObjectCollection withFilter(const RVec<bool>& mask) const;
```

Returns a new collection containing only the objects where `mask[i]` is
`true`.  The mask is indexed relative to *this* collection (0 to
`size()-1`), not the original full collection.  Throws `std::runtime_error`
on size mismatch.

```cpp
// Keep only b-tagged jets (btag score > 0.5)
auto bJets = jets.withFilter(jets.getValue(Jet_btagDeepFlavB) > 0.5f);
```

#### Correction Application

```cpp
// Replace all four kinematic components (full-collection arrays, same
// size as the original branches used to build this collection)
PhysicsObjectCollection withCorrectedKinematics(
    const RVec<Float_t>& correctedPt,
    const RVec<Float_t>& correctedEta,
    const RVec<Float_t>& correctedPhi,
    const RVec<Float_t>& correctedMass) const;

// Replace pt only; eta/phi/mass are taken from existing 4-vectors
PhysicsObjectCollection withCorrectedPt(
    const RVec<Float_t>& correctedPt) const;
```

Each corrected array must be indexed by position in the *original* (full,
unfiltered) collection.  The stored original indices are used to look up
each object's corrected value.  Throws `std::runtime_error` on size
mismatch; throws `std::out_of_range` if an index is out of range.

```cpp
// Apply jet energy corrections from correctionlib
auto corrJets = goodJets.withCorrectedKinematics(
    correctedPt, Jet_eta, Jet_phi, correctedMass);
```

`TypedPhysicsObjectCollection<T>` provides same-name methods that return a
typed collection and carry user-defined objects unchanged. These methods hide
the base methods; they do not override virtual functions.

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
| `filelist.hash` | MD5 digest of a cheap fingerprint for the `fileList` value; local files use path, size, and modification time, while remote or non-stat'able paths fall back to the path or URI string |
| `plugin.<role>` | Type name of each registered plugin, keyed by its role |
| `file.hash.<cfg_key>` | MD5 digest of a cheap fingerprint for any config value that looks like a file path (`.json`, `.root`, `.onnx`, `.bdt`, `.pt`, `.pb`, `.xml`, `.yaml`, `.yml`); local files use path, size, and modification time, while remote or non-stat'able paths fall back to the path or URI string |

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
// Add custom task-level metadata before the event loop. Analyzer stores these
// under task.workflow_id and task.attempt in the provenance output.
analyzer.setTaskMetadata("workflow_id", "wf_12345")
        ->setTaskMetadata("attempt", "1");

analyzer.run();
```

`Analyzer` owns its `ProvenanceService` and does not expose a public accessor.
`setTaskMetadata()` is the supported analysis/workflow API for adding task
metadata. `recordDatasetManifestProvenance()` is a service-level helper; user
code cannot call it through `Analyzer` in the current public API.

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

Interface for tracking systematic families, affected variables, and explicit
nominal-to-variation column mappings.

```cpp
class ISystematicManager {
public:
    virtual ~ISystematicManager() = default;
    static constexpr const char* CANONICAL_SYST_BRANCH_NAME = "SystematicCounter";

    virtual void setEnabled(bool enabled) { (void)enabled; }
    virtual bool isEnabled() const { return true; }

    virtual void registerSystematic(const std::string& syst,
                                    const std::set<std::string>& affectedVariables) = 0;
    virtual void registerVariationColumns(const std::string& variable,
                                          const std::string& systematicName,
                                          const std::string& upColumn,
                                          const std::string& downColumn) = 0;
    virtual void registerVariationColumn(const std::string& variable,
                                         const std::string& variationLabel,
                                         const std::string& column) = 0;
    virtual const std::set<std::string>& getSystematics() const = 0;
    virtual std::set<std::string> getVariablesForSystematic(const std::string& syst) const = 0;
    virtual std::set<std::string> getSystematicsForVariable(const std::string& var) const = 0;
    virtual void registerExistingSystematics(
        const std::vector<std::string>& systConfig,
        const std::vector<std::string>& columnList) = 0;
    virtual SystematicValidationResult autoRegisterSystematics(
        const std::vector<std::string>& columnNames) = 0;
    virtual std::vector<std::string> makeSystList(
        const std::string& branchName, IDataFrameProvider& dataManager) = 0;
    virtual bool isBranchNameMaterialized(const std::string& branchName) const = 0;

    // These two methods have default implementations in the interface.
    virtual bool isVariableAffectedBySystematic(const std::string& variable,
                                                const std::string& syst) const {
        return getVariablesForSystematic(syst).count(variable) != 0;
    }
    virtual std::string getVariationColumnName(const std::string& variable,
                                               const std::string& syst) const {
        return isVariableAffectedBySystematic(variable, syst)
            ? variable + "_" + syst
            : variable;
    }
};
```

`SystematicValidationResult` and `IDataFrameProvider` are declared in the
same public API headers. See
[`ISystematicManager.h`](../core/interface/api/ISystematicManager.h) for the
complete documented contract and method semantics.

**Example**:
```cpp
auto& sysMgr = analyzer.getSystematicManager();
sysMgr.registerSystematic("jes_total", {"Jet_pt_corr_nominal"});
sysMgr.registerVariationColumns(
    "Jet_pt_corr_nominal",
    "jes_total",
    "Jet_pt_jes_total_up",
    "Jet_pt_jes_total_down");

const auto& systematics = sysMgr.getSystematics();
// Contains: "jes_total"

sysMgr.getVariationColumnName("Jet_pt_corr_nominal", "jes_totalUp");
// Returns: "Jet_pt_jes_total_up"
```

### IOutputSink

**Header**: `core/interface/api/IOutputSink.h`

Interface for output destinations.

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

### ILogger

**Header**: `core/interface/api/ILogger.h`

Interface for logging.

```cpp
class ILogger {
public:
    enum class Level { Trace, Debug, Info, Warn, Error };
    virtual ~ILogger() = default;
    virtual void log(Level level, const std::string& message) = 0;
};
```

**Example**:
```cpp
auto& logger = context_->logger;
logger.log(ILogger::Level::Info, "Processing started");
logger.log(ILogger::Level::Warn, "Missing optional branch: " + branchName);
logger.log(ILogger::Level::Error, "Configuration error: " + error);
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

These aliases are local conveniences for analysis or plugin code; they are not
framework-wide declarations. Framework APIs use the qualified ROOT types.

```cpp
// Optional shorthand for ROOT types in local code
using RNode = ROOT::RDF::RNode;
template<typename T>
using RVec = ROOT::VecOps::RVec<T>;

// This is the Lorentz-vector type used by PhysicsObjectCollection.
using LorentzVector = PhysicsObjectCollection::LorentzVec;
```

## Error Handling

The framework uses exceptions for error reporting:

```cpp
// Configuration errors
if (config->getConfigMap().find("required_key") == config->getConfigMap().end()) {
    throw std::runtime_error("Missing required configuration: required_key");
}

// Model lookup: use the manager API, which reports a missing model.
auto model = onnxManager->getModel(name);

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
