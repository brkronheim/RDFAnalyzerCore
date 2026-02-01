#include <analyzer.h>
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <RootOutputSink.h>
#include <CounterService.h>
#include <functions.h>
#include <util.h>
#include <memory>
#include <SystematicManager.h>

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
            metaSink_m(std::make_unique<NullOutputSink>()),
            plugins(std::move(plugins))
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
    ManagerContext ctx{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m};
    for (auto& [role, plugin] : plugins) {
        if (plugin) {
            plugin->setContext(ctx);
            plugin->setupFromConfigFile();
        }
    }
    initializeServices(ctx);
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



Analyzer *Analyzer::save() {
    auto df = dataFrameProvider_m->getDataFrame();

    std::string saveConfig;
    std::string saveFile;
    std::string saveTree;

    std::vector<std::string> missingKeys;
    const auto& configMap = configProvider_m->getConfigMap();

    if (configMap.find("saveConfig") != configMap.end()) {
        saveConfig = configMap.at("saveConfig");
    } else {
        missingKeys.emplace_back("saveConfig");
    }

    if (configMap.find("saveFile") != configMap.end()) {
        saveFile = configMap.at("saveFile");
    } else {
        missingKeys.emplace_back("saveFile");
    }

    if (configMap.find("saveTree") != configMap.end()) {
        saveTree = configMap.at("saveTree");
    } else {
        missingKeys.emplace_back("saveTree");
    }

    if (!missingKeys.empty()) {
        std::string msg = "Error: Missing required config keys: ";
        for (size_t i = 0; i < missingKeys.size(); ++i) {
            msg += missingKeys[i];
            if (i + 1 < missingKeys.size()) {
                msg += ", ";
            }
        }
        msg += ". Please include them in the config file.";
        throw std::runtime_error(msg);
    }

    std::vector<std::string> saveVectorInit = configProvider_m->parseVectorConfig(saveConfig);
    std::vector<std::string> saveVector;
    for (auto val : saveVectorInit) {
        val = val.substr(0, val.find(" "));
        if (!val.empty()) {
            saveVector.push_back(val);
        }
    }

    const unsigned int vecSize = saveVector.size();
    for (unsigned int i = 0; i < vecSize; i++) {
        const auto &systs = systematicManager_m->getSystematicsForVariable(saveVector[i]);
        for (const auto &syst : systs) {
            saveVector.push_back(saveVector[i] + "_" + syst + "Up");
            saveVector.push_back(saveVector[i] + "_" + syst + "Down");
        }
    }

    OutputSpec spec{saveFile, saveTree, saveVector};
    skimSink_m->writeDataFrame(df, spec);

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

 