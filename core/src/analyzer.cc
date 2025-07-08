#include <analyzer.h>
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <functions.h>
#include <util.h>
#include <memory>
#include <iostream>
#include <SystematicManager.h>
#include <ManagerRegistry.h>

// Dependency-injected constructor
Analyzer::Analyzer(
    std::unique_ptr<IConfigurationProvider> configProvider,
    std::unique_ptr<IDataFrameProvider> dataFrameProvider,
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins,
    std::unique_ptr<ISystematicManager> systematicManager)
    : configProvider_m(std::move(configProvider)),
      dataFrameProvider_m(std::move(dataFrameProvider)),
      plugins(std::move(plugins)),
      systematicManager_m(std::move(systematicManager))
{
    if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    wirePluginManagers();
    initialize();
}

// Backward-compatible constructor
Analyzer::Analyzer(std::string configFile,
                   const std::unordered_map<std::string, std::pair<std::string, std::vector<void*>>>& pluginSpecs)
    : configProvider_m(ManagerFactory::createConfigurationManager(configFile)),
      systematicManager_m(ManagerFactory::createSystematicManager()),
      dataFrameProvider_m(ManagerFactory::createDataManager(*configProvider_m))
{
    // Use plugin registry for pluggable managers
    for (const auto& [role, spec] : pluginSpecs) {
        plugins[role] = ManagerRegistry::instance().create(spec.first, spec.second);
    }
    if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    wirePluginManagers();
    initialize();
}

/**
 * @brief Construct a new Analyzer object with a config file and optional plugins.
 *
 * This constructor creates default configuration, data, and systematic managers using the provided config file.
 * Plugins can be optionally provided as a map; if omitted, no plugins are set.
 *
 * @param configFile Path to the configuration file.
 * @param plugins Map of plugin role names to pluggable manager instances (default: empty).
 */
Analyzer::Analyzer(std::string configFile,
                   std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins)
    : configProvider_m(ManagerFactory::createConfigurationManager(configFile)),
      dataFrameProvider_m(ManagerFactory::createDataManager(*configProvider_m)),
      plugins(std::move(plugins)),
      systematicManager_m(ManagerFactory::createSystematicManager())
{
    if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    wirePluginManagers();
    initialize();
}

void Analyzer::initialize() {
    if (verbosityLevel_m >= 1) {
        std::cout << "Analyzer initialized." << std::endl;
    }
}

void Analyzer::wirePluginManagers() {
    for (auto& [role, plugin] : plugins) {
        if (plugin) {
            plugin->setConfigManager(configProvider_m.get());
            plugin->setDataManager(dataFrameProvider_m.get());
            plugin->setSystematicManager(systematicManager_m.get());
        }
    }
}

Analyzer *Analyzer::DefineVector(std::string name, const std::vector<std::string> &columns, std::string type) {
    dataFrameProvider_m->DefineVector(name, columns, type, *systematicManager_m);
    return this;
}



Analyzer *Analyzer::save() {
    auto df = dataFrameProvider_m->getDataFrame();
    auto* concreteDataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get());
    if (concreteDataManager) {
        saveDF(df, *configProvider_m, *concreteDataManager, systematicManager_m.get());
    } else {
        throw std::runtime_error("DataManager-specific saveDF requires DataManager instance");
    }
    return this;
}

ROOT::RDF::RNode Analyzer::getDF() {
    return dataFrameProvider_m->getDataFrame();
}

std::string Analyzer::configMap(std::string key) {
    return configProvider_m->get(key);
}

 