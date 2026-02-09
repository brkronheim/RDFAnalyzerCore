#ifndef IPLUGGABLEMANAGER_H_INCLUDED
#define IPLUGGABLEMANAGER_H_INCLUDED

#include <string>
#include <api/IContextAware.h>

/**
 * @brief Abstract base class for all pluggable manager types.
 *
 * All managers that can be registered and created via the plugin factory
 * should inherit from this interface.
 */
class IPluggableManager : public IContextAware {
public:
    virtual ~IPluggableManager() = default;
    /**
     * @brief Get the type name of the manager (for registry and identification).
     * @return Type name as a string.
     */
    virtual std::string type() const = 0;

    /**
     * @brief Setup the manager from a configuration file.
     */
    virtual void setupFromConfigFile() = 0;
};

#endif // IPLUGGABLEMANAGER_H_INCLUDED 