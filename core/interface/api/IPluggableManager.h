#ifndef IPLUGGABLEMANAGER_H_INCLUDED
#define IPLUGGABLEMANAGER_H_INCLUDED

#include <string>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>

/**
 * @brief Abstract base class for all pluggable manager types.
 *
 * All managers that can be registered and created via the plugin factory
 * should inherit from this interface.
 */
class IPluggableManager {
public:
    virtual ~IPluggableManager() = default;
    /**
     * @brief Get the type name of the manager (for registry and identification).
     * @return Type name as a string.
     */
    virtual std::string type() const = 0;

    /**
     * @brief Set the configuration manager for the manager.
     * @param configManager The configuration manager to set.
     */
    virtual void setConfigManager(IConfigurationProvider* configManager) = 0;

    /**
     * @brief Set the data manager for the manager.
     * @param dataManager The data manager to set.
     */
    virtual void setDataManager(IDataFrameProvider* dataManager) = 0;

    /**
     * @brief Set the systematic manager for the manager.
     * @param systematicManager The systematic manager to set.
     */
    virtual void setSystematicManager(ISystematicManager* systematicManager) = 0;
};

#endif // IPLUGGABLEMANAGER_H_INCLUDED 