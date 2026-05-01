#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <SystematicManager.h>

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