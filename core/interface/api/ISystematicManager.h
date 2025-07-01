#ifndef ISYSTEMATICMANAGER_H_INCLUDED
#define ISYSTEMATICMANAGER_H_INCLUDED

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Interface for systematic managers to enable dependency injection
 *
 * This interface abstracts systematic operations, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class ISystematicManager {
public:
    virtual ~ISystematicManager() = default;

    /**
     * @brief Register a systematic and its affected variables
     * @param syst Name of the systematic
     * @param affectedVariables Set of affected variable names
     */
    virtual void registerSystematic(const std::string &syst,
                                    const std::set<std::string> &affectedVariables) = 0;

    /**
     * @brief Get the set of all registered systematics
     * @return Reference to the set of systematic names
     */
    virtual const std::set<std::string> &getSystematics() const = 0;

    /**
     * @brief Get the set of variables affected by a given systematic
     * @param syst Name of the systematic
     * @return Reference to the set of variable names
     */
    virtual const std::set<std::string> &getVariablesForSystematic(const std::string &syst) const = 0;

    /**
     * @brief Get the set of systematics affecting a given variable
     * @param var Name of the variable
     * @return Reference to the set of systematic names
     */
    virtual const std::set<std::string> &getSystematicsForVariable(const std::string &var) const = 0;

    /**
     * @brief Register existing systematics from configuration
     * @param systConfig Vector of systematic names
     * @param columnList Vector of column names
     */
    virtual void registerExistingSystematics(const std::vector<std::string> &systConfig,
                                             const std::vector<std::string> &columnList) = 0;
};

#endif // ISYSTEMATICMANAGER_H_INCLUDED 