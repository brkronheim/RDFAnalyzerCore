#ifndef MANAGERFACTORY_H_INCLUDED
#define MANAGERFACTORY_H_INCLUDED

#include <api/IBDTManager.h>
#include <api/IConfigurationProvider.h>
#include <api/ICorrectionManager.h>
#include <api/IDataFrameProvider.h>
#include <api/INDHistogramManager.h>
#include <api/ITriggerManager.h>
#include <api/ISystematicManager.h>
#include <memory>

/**
 * @brief Factory class for creating manager instances
 * 
 * This factory provides a centralized way to create manager instances,
 * enabling dependency injection and better testability. It abstracts
 * the creation logic and allows for easy swapping of implementations.
 */
class ManagerFactory {
public:
    /**
     * @brief Create a BDT manager instance
     * @param configProvider Reference to the configuration provider
     * @return Unique pointer to the BDT manager interface
     */
    static std::unique_ptr<IBDTManager> createBDTManager(
        IConfigurationProvider& configProvider);
    
    /**
     * @brief Create a correction manager instance
     * @param configProvider Reference to the configuration provider
     * @return Unique pointer to the correction manager interface
     */
    static std::unique_ptr<ICorrectionManager> createCorrectionManager(
        IConfigurationProvider& configProvider);
    
    /**
     * @brief Create a trigger manager instance
     * @param configProvider Reference to the configuration provider
     * @return Unique pointer to the trigger manager interface
     */
    static std::unique_ptr<ITriggerManager> createTriggerManager(
        IConfigurationProvider& configProvider);
    
    /**
     * @brief Create an ND histogram manager instance
     * @param dataFrameProvider Reference to the dataframe provider
     * @param configProvider Reference to the configuration provider
     * @return Unique pointer to the ND histogram manager interface
     */
    static std::unique_ptr<INDHistogramManager> createNDHistogramManager(
        IDataFrameProvider& dataFrameProvider,
        IConfigurationProvider& configProvider);

    /**
     * @brief Create a configuration manager instance
     * @param configFile Path to the configuration file
     * @return Unique pointer to the configuration provider interface
     */
    static std::unique_ptr<IConfigurationProvider> createConfigurationManager(
        const std::string& configFile);

    /**
     * @brief Create a systematic manager instance
     * @return Unique pointer to the systematic manager interface
     */
    static std::unique_ptr<ISystematicManager> createSystematicManager();

    /**
     * @brief Create a data manager instance
     * @param configProvider Reference to the configuration provider
     * @return Unique pointer to the dataframe provider interface
     */
    static std::unique_ptr<IDataFrameProvider> createDataManager(
        IConfigurationProvider& configProvider);
};

#endif // MANAGERFACTORY_H_INCLUDED 