#include <analyzer.h>
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <BDTManager.h>
#include <CorrectionManager.h>
#include <TriggerManager.h>
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
    initialize();
}

void Analyzer::initialize() {
    if (verbosityLevel_m >= 1) {
        std::cout << "Analyzer initialized." << std::endl;
    }
}

Analyzer *Analyzer::DefineVector(std::string name, const std::vector<std::string> &columns, std::string type) {
    dataFrameProvider_m->DefineVector(name, columns, type, *systematicManager_m);
    return this;
}

Analyzer *Analyzer::ApplyBDT(std::string BDTName) {
    auto* bdtManager = getPlugin<IBDTManager>("bdt");
    if (!bdtManager) throw std::runtime_error("BDTManager plugin not found");
    bdtManager->applyBDT(*dataFrameProvider_m, *systematicManager_m, BDTName);
    return this;
}

Analyzer *Analyzer::ApplyAllBDTs() {
    auto* bdtManager = getPlugin<IBDTManager>("bdt");
    if (!bdtManager) throw std::runtime_error("BDTManager plugin not found");
    for (const auto &bdtName : bdtManager->getAllBDTNames()) {
        ApplyBDT(bdtName);
    }
    return this;
}

Analyzer *Analyzer::ApplyCorrection(std::string correctionName, std::vector<std::string> stringArguments) {
    auto* correctionManager = getPlugin<ICorrectionManager>("correction");
    if (!correctionManager) throw std::runtime_error("CorrectionManager plugin not found");
    correctionManager->applyCorrection(*dataFrameProvider_m, *systematicManager_m, correctionName, stringArguments);
    return this;
}

Analyzer *Analyzer::ApplyAllTriggers() {
    auto* triggerManager = getPlugin<ITriggerManager>("trigger");
    if (!triggerManager) throw std::runtime_error("TriggerManager plugin not found");
    std::string sampleType;
    try {
        sampleType = configProvider_m->get("type");
    } catch (...) {
        throw std::runtime_error("Config does not contain 'type' key for trigger logic");
    }
    std::string group = triggerManager->getGroupForSample(sampleType);
    auto passTrigger = [](const ROOT::VecOps::RVec<bool>& triggerVec) {
        for (bool v : triggerVec) if (v) return true;
        return false;
    };
    auto passTriggerAndVeto = [](const ROOT::VecOps::RVec<bool>& passVec, const ROOT::VecOps::RVec<bool>& vetoVec) {
        bool pass = false;
        for (bool v : passVec) if (v) pass = true;
        for (bool v : vetoVec) if (v) return false;
        return pass;
    };
    if (!group.empty()) {
        const auto& triggers = triggerManager->getTriggers(group);
        const auto& vetoes = triggerManager->getVetoes(group);
        if (vetoes.empty()) {
            DefineVector("allTriggersPassVector", triggers, "Bool_t");
            Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
        } else {
            DefineVector(group + "_passVector", triggers, "Bool_t");
            DefineVector(group + "_vetoVector", vetoes, "Bool_t");
            Filter("applyTrigger", passTriggerAndVeto, {group + "_passVector", group + "_vetoVector"});
        }
    } else {
        std::vector<std::string> allTriggers;
        for (const auto& g : triggerManager->getAllGroups()) {
            const auto& triggers = triggerManager->getTriggers(g);
            allTriggers.insert(allTriggers.end(), triggers.begin(), triggers.end());
        }
        DefineVector("allTriggersPassVector", allTriggers, "Bool_t");
        Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
    }
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

correction::Correction::Ref Analyzer::correctionMap(std::string key) {
    auto* correctionManager = getPlugin<ICorrectionManager>("correction");
    if (!correctionManager) throw std::runtime_error("CorrectionManager plugin not found");
    return correctionManager->getCorrection(key);
}

void Analyzer::BookND(std::vector<histInfo> &infos,
                      std::vector<selectionInfo> &selection,
                      const std::string &suffix,
                      std::vector<std::vector<std::string>> &allRegionNames) {
    auto* concreteSystMgr = dynamic_cast<SystematicManager*>(systematicManager_m.get());
    auto* concreteDataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get());
    const std::vector<std::string> systList = concreteDataManager->makeSystList("Systematic", *systematicManager_m);
    selectionInfo systematicBounds("Systematic", systList.size(), 0.0, systList.size());
    selection.emplace_back(systematicBounds);
    allRegionNames.emplace_back(systList);
    auto* ndHistManager = getPlugin<INDHistogramManager>("ndhist");
    if (!ndHistManager) throw std::runtime_error("NDHistogramManager plugin not found");
    ndHistManager->BookND(infos, selection, suffix, allRegionNames, *dataFrameProvider_m, *systematicManager_m);
}

void Analyzer::SaveHists(std::vector<std::vector<histInfo>> &fullHistList,
                         std::vector<std::vector<std::string>> &allRegionNames) {
    auto* ndHistManager = getPlugin<INDHistogramManager>("ndhist");
    if (!ndHistManager) throw std::runtime_error("NDHistogramManager plugin not found");
    ndHistManager->SaveHists(fullHistList, allRegionNames, *configProvider_m);
} 