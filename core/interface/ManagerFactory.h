#ifndef MANAGERFACTORY_H_INCLUDED
#define MANAGERFACTORY_H_INCLUDED

#include <memory>
#include <string>

class IConfigurationProvider;
class IDataFrameProvider;
class ISystematicManager;

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