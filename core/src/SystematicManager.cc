#include <SystematicManager.h>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Register a systematic and its affected variables
 * @param syst Name of the systematic
 * @param affectedVariables Set of affected variable names
 */
void SystematicManager::registerSystematic(
    const std::string &syst, const std::set<std::string> &affectedVariables) {
  for (const auto &var : affectedVariables) {
    systematicToVariableMap_m[syst].insert(var);
    variableToSystematicMap_m[var].insert(syst);
  }
  systematics_m.insert(syst);
}

/**
 * @brief Get the set of all registered systematics
 * @return Reference to the set of systematic names
 */
const std::set<std::string> &SystematicManager::getSystematics() const {
  return systematics_m;
}

/**
 * @brief Get the set of variables affected by a given systematic
 * @param syst Name of the systematic
 * @return Reference to the set of variable names
 */
const std::set<std::string> &
SystematicManager::getVariablesForSystematic(const std::string &syst) const {
  static const std::set<std::string> empty;
  auto it = systematicToVariableMap_m.find(syst);
  return it != systematicToVariableMap_m.end() ? it->second : empty;
}

/**
 * @brief Get the set of systematics affecting a given variable
 * @param var Name of the variable
 * @return Reference to the set of systematic names
 */
const std::set<std::string> &
SystematicManager::getSystematicsForVariable(const std::string &var) const {
  static const std::set<std::string> empty;
  auto it = variableToSystematicMap_m.find(var);
  return it != variableToSystematicMap_m.end() ? it->second : empty;
}

/**
 * @brief Register existing systematics from configuration and column list
 * @param systConfig Vector of systematic names from configuration
 * @param columnList Vector of column names
 */
void SystematicManager::registerExistingSystematics(
    const std::vector<std::string> &systConfig,
    const std::vector<std::string> &columnList) {
  for (const auto &syst : systConfig) {
    const std::string upSyst = syst + "Up";
    if (syst.size() > 0) {
      std::set<std::string> systVariableSet;
      for (const auto &existingVars : columnList) {
        const size_t index = existingVars.find(upSyst);
        if (index != std::string::npos &&
            index + upSyst.size() == existingVars.size() && index != 0) {
          const std::string systVar = existingVars.substr(0, index - 1);
          registerSystematic(syst, {systVar});
          systVariableSet.insert(systVar);
        }
      }
    }
  }
}