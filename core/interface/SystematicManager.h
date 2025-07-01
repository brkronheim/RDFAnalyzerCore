#ifndef SYSTEMATICMANAGER_H_INCLUDED
#define SYSTEMATICMANAGER_H_INCLUDED

#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <api/ISystematicManager.h>

/**
 * @brief SystematicManager: Handles tracking and applying systematic
 * variations. Implements ISystematicManager.
 *
 * This manager registers systematics, tracks affected variables, and provides
 * interfaces for systematic-aware operations. It is intended to be injected and owned at a high level (e.g., Analyzer), not by DataManager.
 */
class SystematicManager : public ISystematicManager {
public:
  /**
   * @brief Register a systematic and its affected variables
   * @param syst Name of the systematic
   * @param affectedVariables Set of affected variable names
   */
  void registerSystematic(const std::string &syst,
                          const std::set<std::string> &affectedVariables);

  /**
   * @brief Get the set of all registered systematics
   * @return Reference to the set of systematic names
   */
  const std::set<std::string> &getSystematics() const;

  /**
   * @brief Get the set of variables affected by a given systematic
   * @param syst Name of the systematic
   * @return Reference to the set of variable names
   */
  const std::set<std::string> &
  getVariablesForSystematic(const std::string &syst) const;

  /**
   * @brief Get the set of systematics affecting a given variable
   * @param var Name of the variable
   * @return Reference to the set of systematic names
   */
  const std::set<std::string> &
  getSystematicsForVariable(const std::string &var) const;

  /**
   * @brief Register existing systematics from configuration
   * @param systConfig Vector of systematic names
   * @param columnList Vector of column names
   */
  void registerExistingSystematics(const std::vector<std::string> &systConfig,
                                   const std::vector<std::string> &columnList);

private:
  std::set<std::string> systematics_m;
  std::unordered_map<std::string, std::set<std::string>>
      systematicToVariableMap_m;
  std::unordered_map<std::string, std::set<std::string>>
      variableToSystematicMap_m;
};

#endif // SYSTEMATICMANAGER_H_INCLUDED