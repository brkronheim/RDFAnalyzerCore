## Dev Branch
This is the branch on which active development is performed, as such it may not always be stable or work. Below is the general readme.

This contains the Core, analysis agnostic version of RDFAnalyzer. This isn't enough on its own to run an analysis yet, but it contains all the code used to construct an Analyzer object.

## Requirements:
This has been tested and works with ROOT = 6.30/02. The progress bar was added around ROOT 6.28. There is currently a macro to conditionally compile it, but this hasn't been extensively tested and the version cutoff may be wrong.

## Organization
### core
This is where the code from this repo will live.
### analyses
This is where code specific to certain analyses will live. It should be contained in its own repo and cloned here.
### runners
This is where specific applications which actually run the code will live. Most of these should live in their own repos.
### python
This will contain various helper python scripts to help launch jobs.

## Installing
Install with git:
```
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
```
There are also subpackages which will be installed, but this can be done in the build step. At the moment this covers everything, though eventually ONNX will be incorporated, likely through directly downloading some binary files as building the whole package into this project will be complicated. That will most likely also go in the build script.

## Building
To run on lxplus, first run
```
source env.sh
```

This is built using cmake but the build commands are wrapped in a build script. Run
```
source build.sh
```
to create the build files and compile them in the build directory. Note that clang can be used instead by replacing g++ with clang in the cmake statement.

---

# Plugin System (NEW)

## Overview
The core framework now uses a **plugin-based architecture** for all manager components. This means:
- Analyzer is agnostic to the specific types of managers (BDT, Correction, Trigger, NDHistogram, etc.).
- Any manager that inherits from `IPluggableManager` can be registered and used as a plugin.
- New plugins can be added without modifying Analyzer or recompiling the core logic.

## Registering Plugins
All core managers (BDTManager, CorrectionManager, TriggerManager, NDHistogramManager) are registered as plugins at startup. To add a new plugin, inherit from `IPluggableManager` and use the `REGISTER_MANAGER_TYPE` macro:
```cpp
REGISTER_MANAGER_TYPE(MyCustomManager, ArgType1, ArgType2)
```

## Instantiating Plugins
Plugins are created via the `ManagerRegistry`:
```cpp
std::vector<void*> args = { /* constructor arguments as pointers */ };
auto plugin = ManagerRegistry::instance().create("MyCustomManager", args);
```

## Using Plugins with Analyzer
Analyzer is constructed with a map of plugin role names to plugin instances, or with a map of plugin specs for dynamic instantiation:

### Example: Direct Construction (for tests)
```cpp
std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
plugins["bdt"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(bdtManager.release()));
plugins["correction"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(correctionManager.release()));
plugins["trigger"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(triggerManager.release()));
plugins["ndhist"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(ndHistManager.release()));
Analyzer analyzer(std::move(configProvider), std::move(dataFrameProvider), std::move(plugins), std::move(systematicManager));
```

### Example: Backward-Compatible Construction (for runners)
```cpp
std::unordered_map<std::string, std::pair<std::string, std::vector<void*>>> pluginSpecs;
pluginSpecs["bdt"] = {"BDTManager", {configProvider.get()}};
pluginSpecs["correction"] = {"CorrectionManager", {configProvider.get()}};
pluginSpecs["trigger"] = {"TriggerManager", {configProvider.get()}};
pluginSpecs["ndhist"] = {"NDHistogramManager", {dataFrameProvider.get(), configProvider.get(), systematicManager.get()}};
Analyzer analyzer(configFile, pluginSpecs);
```

### Accessing Plugins
Use the generic accessor:
```cpp
IBDTManager* bdt = analyzer.getPlugin<IBDTManager>("bdt");
```

## Extending with New Plugins
- Implement your manager inheriting from `IPluggableManager`.
- Register it with `REGISTER_MANAGER_TYPE`.
- Add it to the plugin map/specs when constructing Analyzer.
- Access it via `getPlugin<YourInterface>("your_role")`.

## Notes
- All core managers are now plugins. Analyzer does not depend on their concrete types.
- You can add new plugin types without modifying Analyzer.

