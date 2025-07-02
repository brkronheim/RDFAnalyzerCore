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

// Dependency-injected constructor
Analyzer::Analyzer(
    std::unique_ptr<IConfigurationProvider> configProvider,
    std::unique_ptr<IDataFrameProvider> dataFrameProvider,
    std::unique_ptr<IBDTManager> bdtManager,
    std::unique_ptr<ICorrectionManager> correctionManager,
    std::unique_ptr<ITriggerManager> triggerManager,
    std::unique_ptr<INDHistogramManager> ndHistManager,
    std::unique_ptr<ISystematicManager> systematicManager)
    : configProvider_m(std::move(configProvider)),
      dataFrameProvider_m(std::move(dataFrameProvider)),
      bdtManager_m(std::move(bdtManager)),
      correctionManager_m(std::move(correctionManager)),
      triggerManager_m(std::move(triggerManager)),
      ndHistManager_m(std::move(ndHistManager)),
      systematicManager_m(std::move(systematicManager))
{
    if (!configProvider_m || !dataFrameProvider_m || !bdtManager_m || !correctionManager_m || !triggerManager_m || !ndHistManager_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: All manager dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    initialize();
}

// Backward-compatible constructor
Analyzer::Analyzer(std::string configFile)
    : configProvider_m(ManagerFactory::createConfigurationManager(configFile)),
      systematicManager_m(ManagerFactory::createSystematicManager()),
      dataFrameProvider_m(ManagerFactory::createDataManager(*configProvider_m)),
      bdtManager_m(ManagerFactory::createBDTManager(*configProvider_m)),
      correctionManager_m(ManagerFactory::createCorrectionManager(*configProvider_m)),
      triggerManager_m(ManagerFactory::createTriggerManager(*configProvider_m)),
      ndHistManager_m(ManagerFactory::createNDHistogramManager(*dataFrameProvider_m, *configProvider_m, *systematicManager_m))
{
    if (!configProvider_m || !dataFrameProvider_m || !bdtManager_m || !correctionManager_m || !triggerManager_m || !ndHistManager_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: All manager dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    initialize();
}

void Analyzer::initialize() {
    // Any additional setup can be done here
    // For now, just print a message for debug
    if (verbosityLevel_m >= 1) {
        std::cout << "Analyzer initialized." << std::endl;
    }
}

Analyzer *Analyzer::DefineVector(std::string name, const std::vector<std::string> &columns, std::string type) {
    dataFrameProvider_m->DefineVector(name, columns, type, *systematicManager_m);
    return this;
}

Analyzer *Analyzer::ApplyBDT(std::string BDTName) {
    bdtManager_m->applyBDT(*dataFrameProvider_m, BDTName, *systematicManager_m);
    return this;
}

Analyzer *Analyzer::ApplyAllBDTs() {
    for (const auto &bdtName : bdtManager_m->getAllBDTNames()) {
        ApplyBDT(bdtName);
    }
    return this;
}

Analyzer *Analyzer::ApplyCorrection(std::string correctionName, std::vector<std::string> stringArguments) {
    correctionManager_m->applyCorrection(*dataFrameProvider_m, correctionName, stringArguments, *systematicManager_m);
    return this;
}

Analyzer *Analyzer::ApplyAllTriggers() {
    // Get sample type (e.g., "data" or "MC") from config
    std::string sampleType;
    try {
        sampleType = configProvider_m->get("type");
    } catch (...) {
        throw std::runtime_error("Config does not contain 'type' key for trigger logic");
    }
    
    // Get the trigger group for this sample type
    std::string group = triggerManager_m->getGroupForSample(sampleType);
    
    // Helper lambdas for pass logic
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

    if (!group.empty()) { // Data: use group for this sample type
        const auto& triggers = triggerManager_m->getTriggers(group);
        const auto& vetoes = triggerManager_m->getVetoes(group);
        if (vetoes.empty()) {
            DefineVector("allTriggersPassVector", triggers, "Bool_t");
            Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
        } else {
            DefineVector(group + "_passVector", triggers, "Bool_t");
            DefineVector(group + "_vetoVector", vetoes, "Bool_t");
            Filter("applyTrigger", passTriggerAndVeto, {group + "_passVector", group + "_vetoVector"});
        }
    } else { // MC: merge all triggers from all groups
        std::vector<std::string> allTriggers;
        for (const auto& g : triggerManager_m->getAllGroups()) {
            const auto& triggers = triggerManager_m->getTriggers(g);
            allTriggers.insert(allTriggers.end(), triggers.begin(), triggers.end());
        }
        DefineVector("allTriggersPassVector", allTriggers, "Bool_t");
        Filter("applyTrigger", passTrigger, {"allTriggersPassVector"});
    }
    return this;
}

Analyzer *Analyzer::save() {
    auto df = dataFrameProvider_m->getDataFrame();
    // If saveDF requires DataManager, dynamic_cast here
    auto* concreteDataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get());
    if (concreteDataManager) {
        saveDF(df, *configProvider_m, *concreteDataManager, systematicManager_m.get());
    } else {
        // Handle error or provide alternative
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
    return correctionManager_m->getCorrection(key);
}

// Implementation of pass-through histogram booking and saving
void Analyzer::BookND(std::vector<histInfo> &infos,
                      std::vector<selectionInfo> &selection,
                      const std::string &suffix,
                      std::vector<std::vector<std::string>> &allRegionNames) {
    // Handle systematics internally for axis binning, etc.
    auto* concreteSystMgr = dynamic_cast<SystematicManager*>(systematicManager_m.get());
    auto* concreteDataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get());

    const std::vector<std::string> systList = concreteDataManager->makeSystList("Systematic", *systematicManager_m);
    selectionInfo systematicBounds("Systematic", systList.size(), 0.0,
                                 systList.size());
    selection.emplace_back(systematicBounds);
    allRegionNames.emplace_back(systList);


    ndHistManager_m->BookND(infos, selection, suffix, allRegionNames, *systematicManager_m);
}

void Analyzer::SaveHists(std::vector<std::vector<histInfo>> &fullHistList,
                         std::vector<std::vector<std::string>> &allRegionNames) {
    ndHistManager_m->SaveHists(fullHistList, allRegionNames);
} 