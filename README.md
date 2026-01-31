## Dev Branch
This branch is for active development and may be unstable. The repository provides the core, analysis‑agnostic framework for constructing and running an Analyzer pipeline using ROOT RDataFrame. Analysis-specific code lives in separate repos under analyses/.

## Requirements
- ROOT 6.30/02 (progress bar support was added around 6.28).

## Repository Organization
- core: framework code (Analyzer, managers, interfaces, plugins, utilities).
- analyses: analysis-specific code (kept in separate repos and cloned here).
- runners: concrete applications that build and execute analyses.
- python: helper scripts for job submission and orchestration.

## Installing
Clone the repository:
```
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
```

## Building
On lxplus:
```
source env.sh
```

Build via the wrapper script:
```
source build.sh
```

---

# How the Framework Works

## Core Concepts
The framework is centered around an Analyzer that orchestrates:
- A configuration provider (ConfigurationManager).
- A dataframe provider (DataManager, wrapping ROOT::RDataFrame).
- A systematic manager (SystematicManager).
- Plugins (BDTManager, CorrectionManager, TriggerManager, NDHistogramManager, etc.).
- Output sinks (skim output and metadata/hist output).

All managers are wired in C++ (compile‑time wiring). The Analyzer is agnostic to concrete plugin types and interacts through interfaces.

## Data Flow
1. Configuration is loaded and parsed by ConfigurationManager.
2. DataManager builds a TChain and exposes a ROOT::RDataFrame.
3. Variables are defined, aliases and optional branches are registered.
4. Plugins attach logic (corrections, triggers, BDTs, histograms).
5. The event loop executes via RDataFrame actions.
6. Outputs are written via output sinks and histogram save paths.

---

# Defining Variables and Cuts

## Defining Variables
Variables are defined on the DataManager, which updates the current RDataFrame node. All user-defined variables become columns in the dataframe.

### Scalar variable
```
dataManager->Define("myVar", []() { return 42.0; }, {}, *systematicManager);
```

### Derived variable with dependencies
```
dataManager->Define("ptOverMass",
										[](double pt, double mass) { return pt / mass; },
										{"pt", "mass"},
										*systematicManager);
```

### Per-sample constants
```
dataManager->DefinePerSample("sampleWeight",
														 [](unsigned int, const ROOT::RDF::RSampleInfo&) { return 1.0f; });
```

### Vectors
To create a vector column from scalars or concatenate RVecs, use DefineVector:
```
dataManager->DefineVector("lepPts", {"lep1_pt", "lep2_pt"}, "Float_t", *systematicManager);
```

DefineVector enforces that all input columns are either scalar or RVec. Mixed inputs are not supported.

## Cuts / Filters
Filters are applied to the current dataframe node and can be chained:
```
dataManager->Filter([](double pt) { return pt > 25.0; }, {"pt"});
```

Filters can be used by plugins or analysis code to restrict event selection prior to histogramming or output.

---

# Systematics
Systematics are tracked by SystematicManager. When variables are defined via Define with a SystematicManager, systematic variations are registered and can be propagated through the pipeline. SystematicManager can generate per-sample systematic indices and exposes lists of active variations.

---

# Plugin System

## Overview
The core uses a plugin-based architecture for all manager components:
- Analyzer is agnostic to specific types (BDT, Correction, Trigger, NDHistogram, etc.).
- Any manager implementing IPluggableManager can be used as a plugin.
- New plugins can be added without modifying Analyzer.

## Instantiating Plugins (C++ Wiring)
Plugins are created directly in C++ and passed to Analyzer:
```cpp
auto bdt = std::make_unique<BDTManager>(*configProvider);
auto corr = std::make_unique<CorrectionManager>(*configProvider);
auto trig = std::make_unique<TriggerManager>(*configProvider);
```

## Using Plugins with Analyzer
Analyzer is constructed with a map of plugin role names to plugin instances:
```cpp
std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
plugins["bdt"] = std::move(bdt);
plugins["correction"] = std::move(corr);
plugins["trigger"] = std::move(trig);
Analyzer analyzer(configFile, std::move(plugins));
```

Access plugins via:
```cpp
IBDTManager* bdt = analyzer.getPlugin<IBDTManager>("bdt");
```

## Extending with New Plugins
- Implement a manager that inherits IPluggableManager.
- Register it with REGISTER_MANAGER_TYPE (if used by factory creation).
- Add it to the plugin map when constructing Analyzer.
- Access it via getPlugin<YourInterface>("your_role").

---

# Histogram Output

## NDHistogramManager
NDHistogramManager books and saves N-dimensional histograms using a custom THnMulti action. It supports:
- Scalar or RVec inputs for base variables, weights, and region/category axes.
- Systematic axes via SystematicManager.
- Multi-fill handling with scalar expansion to match vector sizes.

### Defining Histograms
Define histogram metadata using histInfo and selectionInfo:
```cpp
std::vector<histInfo> infos = {
	histInfo("jet_pt", "jet_pt", "Jet pT", "weight", 50, 0.0, 500.0)
};

std::vector<selectionInfo> selections = {
	selectionInfo("channel", 3, 0, 3),
	selectionInfo("region",  2, 0, 2),
	selectionInfo("sample",  5, 0, 5)
};

std::vector<std::vector<std::string>> regionNames = {
	{"chanA", "chanB", "chanC"},
	{"SR", "CR"},
	{"sample1", "sample2", "sample3", "sample4", "sample5"}
};

histogramManager->bookND(infos, selections, "", regionNames);
```

The systematic axis is appended automatically if not already present in regionNames.

### Saving Histograms
Histograms are saved with:
```
histogramManager->saveHists(fullHistList, allRegionNames);
```

The file is chosen using metaFile in the config, with saveFile as a fallback.

---

# End-to-End Example: Cuts + Histograms + Outputs

Below is a minimal example that:
1) Defines variables and cuts
2) Books histograms
3) Writes both skim and metadata/hist outputs

```cpp
#include <Analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <SystematicManager.h>
#include <DefaultLogger.h>
#include <RootOutputSink.h>
#include <NDHistogramManager.h>
#include <api/ManagerContext.h>
#include <plots.h>

// Configure and wire core services
auto config = std::make_unique<ConfigurationManager>("cfg/analysis.txt");
auto dataManager = std::make_unique<DataManager>(*config);
auto systematicManager = std::make_unique<SystematicManager>();
auto logger = std::make_unique<DefaultLogger>();

// Outputs (skim + metadata/hist)
auto skimSink = std::make_unique<RootOutputSink>(config->get("saveFile"));
auto metaSink = std::make_unique<RootOutputSink>(config->get("metaFile"));

// Histogram manager
auto histManager = std::make_unique<NDHistogramManager>(*config);
ManagerContext ctx{*config, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
histManager->setContext(ctx);

// Define variables
dataManager->Define("weight", []() { return 1.0; }, {}, *systematicManager);
dataManager->Define("pt", []() { return 42.0; }, {}, *systematicManager);
dataManager->Define("eta", []() { return 0.1; }, {}, *systematicManager);

// Define cuts (filters)
dataManager->Filter([](double pt) { return pt > 25.0; }, {"pt"});
dataManager->Filter([](double eta) { return std::abs(eta) < 2.4; }, {"eta"});

// Book histograms
std::vector<histInfo> infos = {
	histInfo("h_pt", "pt", "pT", "weight", 50, 0.0, 500.0),
	histInfo("h_eta", "eta", "eta", "weight", 48, -2.4, 2.4)
};

std::vector<selectionInfo> selections = {
	selectionInfo("zero__", 1, 0.0, 1.0) // default channel axis
};

std::vector<std::vector<std::string>> regionNames = {
	{"all"}
};

histManager->bookND(infos, selections, "", regionNames);

// Run the event loop by triggering histogram computation
auto &histos = histManager->GetHistos();
for (auto &h : histos) {
	h.GetPtr();
}

// Save histograms (metadata/hist output)
std::vector<std::vector<histInfo>> fullHistList = {infos};
histManager->saveHists(fullHistList, regionNames);

// Save skim output (example: write via sink as needed for your runner)
// skimSink->write(...);
```

---

# Output Sinks
Analyzer uses output sinks to separate skim output (event-level or tree output) from metadata/hist output. The default sink for skims is RootOutputSink; metadata/hist output is routed through meta sinks. This allows clean separation between event outputs and histogram/metadata files.

---

# Configuration
ConfigurationManager reads configuration files and exposes a uniform API for values and config parsing. It supports:
- Key/value retrieval.
- Pair-based configs (e.g., constants).
- Multi-key configs (e.g., aliases, optional branches).

Typical configuration keys include:
- saveFile: default output file for skims.
- metaFile: output file for metadata/histograms.
- Input file and tree name settings (analysis-specific).

---

# Tests
Tests live under core/test and cover managers, configuration, and histogramming. Run the test suite with:
```
source test.sh
```

---

# Notes
- All core managers are plugins. Analyzer does not depend on concrete types.
- Wiring is compile-time C++ construction.
- NDHistogramManager supports multi-fill inputs by expanding scalars to the reference vector length when needed.

