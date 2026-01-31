#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <SystematicManager.h>

//std::unique_ptr<IBDTManager> ManagerFactory::createBDTManager(
//    IConfigurationProvider& configProvider) {
//    return std::make_unique<BDTManager>(configProvider);
//}

//std::unique_ptr<ICorrectionManager> ManagerFactory::createCorrectionManager(
//    IConfigurationProvider& configProvider) {
//    return std::make_unique<CorrectionManager>(configProvider);
//}

//std::unique_ptr<ITriggerManager> ManagerFactory::createTriggerManager(
//    IConfigurationProvider& configProvider) {
//    return std::make_unique<TriggerManager>(configProvider);
//}

//std::unique_ptr<INDHistogramManager> ManagerFactory::createNDHistogramManager(
//    IConfigurationProvider& configProvider) {
//    return std::make_unique<NDHistogramManager>(configProvider);
//}

std::unique_ptr<IConfigurationProvider> ManagerFactory::createConfigurationManager(
    const std::string& configFile) {
    return std::make_unique<ConfigurationManager>(configFile);
}

std::unique_ptr<ISystematicManager> ManagerFactory::createSystematicManager() {
    return std::make_unique<SystematicManager>();
}

std::unique_ptr<IDataFrameProvider> ManagerFactory::createDataManager(
    IConfigurationProvider& configProvider) {
    return std::make_unique<DataManager>(configProvider);
} 