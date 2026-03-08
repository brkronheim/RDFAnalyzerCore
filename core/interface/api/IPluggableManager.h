#ifndef IPLUGGABLEMANAGER_H_INCLUDED
#define IPLUGGABLEMANAGER_H_INCLUDED

#include <string>
#include <vector>
#include <api/IContextAware.h>

/**
 * @brief Abstract base class for all pluggable manager types.
 *
 * All managers that can be registered and created via the plugin factory
 * should inherit from this interface.
 *
 * ## Lifecycle
 * The framework invokes methods in the following order:
 *  1. Construction (by user code)
 *  2. setContext()          – inject shared services (IContextAware)
 *  3. setupFromConfigFile() – load plugin-specific configuration
 *  4. initialize()          – post-wiring setup; all declared dependencies
 *                             are guaranteed to be registered at this point
 *  5. execute()             – called immediately before the RDataFrame
 *                             computation is triggered (inside save()/run())
 *  6. finalize()            – called after the RDataFrame computation completes
 *  7. reportMetadata()      – called after finalize() for metadata emission
 *
 * ## Dependency and resource advertisement
 * Plugins may declare ordering constraints and column requirements via:
 *  - getDependencies()    – plugin roles this plugin depends on
 *  - getRequiredColumns() – dataframe columns this plugin reads
 *  - getProducedColumns() – dataframe columns this plugin writes
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
     * @brief Load plugin-specific configuration.
     *
     * Called after setContext(). Use this to read configuration keys and
     * prepare internal state. Do not access other plugins here; use
     * initialize() instead.
     */
    virtual void setupFromConfigFile() = 0;

    // -----------------------------------------------------------------------
    // Dependency and resource advertisement (default: no dependencies/columns)
    // -----------------------------------------------------------------------

    /**
     * @brief Declare the plugin roles this plugin depends on.
     *
     * The Analyzer uses this list to determine initialization order and to
     * validate that all required plugins are registered before initialize()
     * is called. An exception is thrown if any declared dependency is absent.
     *
     * @return Vector of role names (keys in the Analyzer's plugin map).
     */
    virtual std::vector<std::string> getDependencies() const { return {}; }

    /**
     * @brief Declare the dataframe columns this plugin reads.
     * @return Vector of column names.
     */
    virtual std::vector<std::string> getRequiredColumns() const { return {}; }

    /**
     * @brief Declare the dataframe columns this plugin writes/defines.
     * @return Vector of column names.
     */
    virtual std::vector<std::string> getProducedColumns() const { return {}; }

    // -----------------------------------------------------------------------
    // Lifecycle hooks (default: no-op)
    // -----------------------------------------------------------------------

    /**
     * @brief Post-wiring initialization.
     *
     * Called after setContext() and setupFromConfigFile() have been invoked
     * on *all* registered plugins and all dependency ordering constraints
     * have been satisfied. Plugins may safely look up their declared
     * dependencies here.
     */
    virtual void initialize() {}

    /**
     * @brief Pre-execution hook.
     *
     * Called inside Analyzer::save()/run(), immediately before the RDataFrame
     * computation is triggered.
     */
    virtual void execute() {}

    /**
     * @brief Post-execution hook.
     *
     * Called inside Analyzer::save()/run(), after the RDataFrame computation
     * has completed.
     */
    virtual void finalize() {}

    /**
     * @brief Metadata-reporting hook.
     *
     * Called after finalize(). Plugins may use this to write summary
     * information to the metadata output sink or to the logger.
     */
    virtual void reportMetadata() {}
};

#endif // IPLUGGABLEMANAGER_H_INCLUDED 