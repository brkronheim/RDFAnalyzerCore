#ifndef ISYSTEMATICMANAGER_H_INCLUDED
#define ISYSTEMATICMANAGER_H_INCLUDED

#include <set>
#include <string>
#include <vector>
class IDataFrameProvider;

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

    /**
     * @brief Make a list of systematic variations for a branch
     * @param branchName Name of the branch
     * @return Vector of systematic variation names
     */
    virtual std::vector<std::string> makeSystList(const std::string &branchName, IDataFrameProvider &dataManager) = 0;

    /**
     * @brief Check whether a given systematic variation is registered for a variable
     * @param variable Name of the variable
     * @param syst Name of the systematic
     * @return True if the variable is affected by the systematic, false otherwise
     */
    virtual bool isVariableAffectedBySystematic(const std::string &variable,
                                                const std::string &syst) const {
        return getVariablesForSystematic(syst).count(variable) != 0;
    }

    /**
     * @brief Get the column name for a variable under a given systematic variation
     *
     * Returns @p variable + "_" + @p syst when the variable is affected by the
     * systematic, and @p variable unchanged otherwise.  This consolidates the
     * repeated conditional column-name computation that would otherwise appear
     * at every call site.
     *
     * @param variable Name of the base variable
     * @param syst Name of the systematic
     * @return Column name to use for this variable/systematic combination
     */
    virtual std::string getVariationColumnName(const std::string &variable,
                                               const std::string &syst) const {
        if (isVariableAffectedBySystematic(variable, syst)) {
            return variable + "_" + syst;
        }
        return variable;
    }
};

#endif // ISYSTEMATICMANAGER_H_INCLUDED 