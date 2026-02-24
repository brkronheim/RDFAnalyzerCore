#include <analyzer.h>
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <RootOutputSink.h>
#include <CounterService.h>
#include <NDHistogramManager.h>
#include <functions.h>
#include <util.h>
#include <memory>
#include <SystematicManager.h>
#include <api/ManagerContext.h> // for wiring plugins and services

// Dependency-injected constructor
Analyzer::Analyzer(
    std::unique_ptr<IConfigurationProvider> configProvider,
    std::unique_ptr<IDataFrameProvider> dataFrameProvider,
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins,
    std::unique_ptr<ISystematicManager> systematicManager,
    std::unique_ptr<ILogger> logger,
    std::unique_ptr<IOutputSink> skimSink,
    std::unique_ptr<IOutputSink> metaSink)
    : configProvider_m(std::move(configProvider)),
      dataFrameProvider_m(std::move(dataFrameProvider)),
      systematicManager_m(std::move(systematicManager)),
      logger_m(std::move(logger)),
      skimSink_m(std::move(skimSink)),
      metaSink_m(std::move(metaSink)),
      managerContext_m{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m},
      plugins(std::move(plugins))
{
    if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m || !logger_m || !skimSink_m || !metaSink_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    if (auto* dataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get())) {
        dataManager->finalizeSetup(*configProvider_m);
    }
    wirePluginManagers();
    //initialize();
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
            systematicManager_m(ManagerFactory::createSystematicManager()),
            logger_m(std::make_unique<DefaultLogger>()),
    skimSink_m(std::make_unique<RootOutputSink>()),
            metaSink_m(std::make_unique<RootOutputSink>()),            managerContext_m{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m},            plugins(std::move(plugins))
{
        if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    if (auto* dataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get())) {
        dataManager->finalizeSetup(*configProvider_m);
    }
    wirePluginManagers();
    //initialize();
}

/*
void Analyzer::initialize() {
    if (verbosityLevel_m >= 1) {
        std::cout << "Analyzer initialized." << std::endl;
    }
}
*/

void Analyzer::wirePluginManagers() {
    for (auto& [role, plugin] : plugins) {
        if (plugin) {
            plugin->setContext(managerContext_m);
            plugin->setupFromConfigFile();
        }
    }
    initializeServices(managerContext_m);
}

Analyzer *Analyzer::addPlugin(const std::string &role, std::unique_ptr<IPluggableManager> plugin) {
    if (!plugin) return this;
    // Wire the plugin immediately with the persisted ManagerContext
    plugin->setContext(managerContext_m);
    plugin->setupFromConfigFile();
    plugins.emplace(role, std::move(plugin));
    return this;
}

Analyzer *Analyzer::addPlugins(std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& newPlugins) {
    for (auto &kv : newPlugins) {
        addPlugin(kv.first, std::move(kv.second));
    }
    return this;
}

void Analyzer::initializeServices(ManagerContext& ctx) {
    const auto& configMap = configProvider_m->getConfigMap();
    auto it = configMap.find("enableCounters");
    if (it != configMap.end()) {
        const auto& val = it->second;
        const bool enabled = (val == "1" || val == "true" || val == "True");
        if (enabled) {
            auto service = std::make_unique<CounterService>();
            service->initialize(ctx);
            services_m.emplace_back(std::move(service));
        }
    }
}

Analyzer *Analyzer::DefineVector(std::string name, const std::vector<std::string> &columns, std::string type) {
    dataFrameProvider_m->DefineVector(name, columns, type, *systematicManager_m);
    return this;
}

Analyzer *Analyzer::bookConfigHistograms() {
    // Get the NDHistogramManager plugin if it exists
    auto* histogramManager = getPlugin<NDHistogramManager>("histogramManager");
    if (histogramManager) {
        histogramManager->bookConfigHistograms();
    }
    return this;
}

Analyzer *Analyzer::bookCounterIntHistogram(const std::string& branch, int nBins,
                                             double low, double high) {
    auto df = dataFrameProvider_m->getDataFrame();
    for (auto& service : services_m) {
        if (auto* cs = dynamic_cast<CounterService*>(service.get())) {
            cs->bookIntWeightHistogram(df, branch, nBins, low, high);
        }
    }
    return this;
}


Analyzer *Analyzer::save() {
    auto df = dataFrameProvider_m->getDataFrame();

    skimSink_m->writeDataFrame(df,
                               *configProvider_m,
                               dataFrameProvider_m.get(),
                               systematicManager_m.get(),
                               OutputChannel::Skim);

    for (auto& service : services_m) {
        service->finalize(df);
    }
    return this;
}

Analyzer *Analyzer::run() {
    auto df = dataFrameProvider_m->getDataFrame();

    // Conditionally write a skim when enableSkim=1/true/True is set in config
    const auto& cfgMap = configProvider_m->getConfigMap();
    const auto skimIt = cfgMap.find("enableSkim");
    if (skimIt != cfgMap.end()) {
        const auto& val = skimIt->second;
        if (val == "1" || val == "true" || val == "True") {
            skimSink_m->writeDataFrame(df,
                                       *configProvider_m,
                                       dataFrameProvider_m.get(),
                                       systematicManager_m.get(),
                                       OutputChannel::Skim);
        }
    }

    // Save ND histograms (uses internally tracked histInfo / regionNames)
    if (auto* histogramManager = getPlugin<NDHistogramManager>("histogramManager")) {
        histogramManager->saveHists();
    }

    // Finalize all analysis services (CounterService writes to meta sink here)
    for (auto& service : services_m) {
        service->finalize(df);
    }
    return this;
}

ROOT::RDF::RNode Analyzer::getDF() {
    return dataFrameProvider_m->getDataFrame();
}

std::string Analyzer::configMap(std::string key) {
    return configProvider_m->get(key);
}

 