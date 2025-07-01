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
      ndHistManager_m(ManagerFactory::createNDHistogramManager(*dataFrameProvider_m, *configProvider_m))
{
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
    dataFrameProvider_m->DefineVector(name, columns, type);
    return this;
}

Analyzer *Analyzer::ApplyBDT(std::string BDTName) {
    bdtManager_m->applyBDT(*dataFrameProvider_m, BDTName);
    return this;
}

Analyzer *Analyzer::ApplyAllBDTs() {
    for (const auto &bdtName : bdtManager_m->getAllBDTNames()) {
        ApplyBDT(bdtName);
    }
    return this;
}

Analyzer *Analyzer::ApplyCorrection(std::string correctionName, std::vector<std::string> stringArguments) {
    correctionManager_m->applyCorrection(*dataFrameProvider_m, correctionName, stringArguments);
    return this;
}

Analyzer *Analyzer::ApplyAllTriggers() {
    // This is a stub. Implement trigger logic as needed.
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