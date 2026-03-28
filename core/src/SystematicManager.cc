#include <SystematicManager.h>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <api/IDataFrameProvider.h>

namespace {

bool endsWith(const std::string &value, const std::string &suffix) {
  return value.size() >= suffix.size() &&
         value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

std::string normalizeSystematicName(const std::string &syst) {
  if (endsWith(syst, "Up")) {
    return syst.substr(0, syst.size() - 2);
  }
  if (endsWith(syst, "Down")) {
    return syst.substr(0, syst.size() - 4);
  }
  return syst;
}

} // namespace

/**
 * @brief Register a systematic and its affected variables
 * @param syst Name of the systematic
 * @param affectedVariables Set of affected variable names
 */
void SystematicManager::registerSystematic(
    const std::string &syst, const std::set<std::string> &affectedVariables) {
  const std::string normalizedSyst = normalizeSystematicName(syst);
  for (const auto &var : affectedVariables) {
    systematicToVariableMap_m[normalizedSyst].insert(var);
    variableToSystematicMap_m[var].insert(normalizedSyst);
  }
  systematics_m.insert(normalizedSyst);
}

void SystematicManager::registerVariationColumns(
    const std::string &variable, const std::string &systematicName,
    const std::string &upColumn, const std::string &downColumn) {
  if (variable.empty() || systematicName.empty() || upColumn.empty() ||
      downColumn.empty()) {
    return;
  }

  const std::string normalizedSyst = normalizeSystematicName(systematicName);
  registerSystematic(normalizedSyst, {variable});
  variationColumnMap_m[variable][normalizedSyst + "Up"] = upColumn;
  variationColumnMap_m[variable][normalizedSyst + "Down"] = downColumn;
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
  auto it = systematicToVariableMap_m.find(normalizeSystematicName(syst));
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

std::string SystematicManager::getVariationColumnName(
    const std::string &variable, const std::string &syst) const {
  auto varIt = variationColumnMap_m.find(variable);
  if (varIt != variationColumnMap_m.end()) {
    auto explicitIt = varIt->second.find(syst);
    if (explicitIt != varIt->second.end()) {
      return explicitIt->second;
    }
  }

  if (isVariableAffectedBySystematic(variable, syst)) {
    return variable + "_" + syst;
  }
  return variable;
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

/**
 * @brief Automatically discover and register systematic variations from column names.
 *
 * Scans @p columnNames for pairs of columns following the naming convention
 * `baseVariable_systematicNameUp` / `baseVariable_systematicNameDown`.  For
 * every complete pair found, registers the systematic as affecting the base
 * variable.  Incomplete pairs are reported in the returned result.
 */
SystematicValidationResult SystematicManager::autoRegisterSystematics(
    const std::vector<std::string> &columnNames) {

  const std::unordered_set<std::string> colSet(columnNames.begin(), columnNames.end());
  SystematicValidationResult result;

  // Track (baseVar, systName) keys we have already processed to avoid
  // reporting or registering the same pair twice.
  std::unordered_set<std::string> processedPairs;

  // --- Pass 1: scan for columns ending with "Up" ---
  for (const auto &col : columnNames) {
    static constexpr std::size_t kUpLen = 2; // "Up"
    if (col.size() <= kUpLen) {
      continue;
    }
    if (col.compare(col.size() - kUpLen, kUpLen, "Up") != 0) {
      continue;
    }

    // prefix = "baseVar_systName"
    const std::string prefix = col.substr(0, col.size() - kUpLen);
    const std::size_t lastUnderscore = prefix.rfind('_');
    if (lastUnderscore == std::string::npos || lastUnderscore == 0) {
      continue;
    }

    const std::string baseVar  = prefix.substr(0, lastUnderscore);
    const std::string systName = prefix.substr(lastUnderscore + 1);
    if (baseVar.empty() || systName.empty()) {
      continue;
    }

    const std::string pairKey = baseVar + "_" + systName;
    if (processedPairs.count(pairKey)) {
      continue;
    }
    processedPairs.insert(pairKey);

    const std::string downCol = prefix + "Down";
    if (colSet.count(downCol)) {
      registerSystematic(systName, {baseVar});
      result.registered.emplace_back(baseVar, systName);
    } else {
      result.missingDown.push_back(col);
    }
  }

  // --- Pass 2: scan for orphaned "Down" columns (no matching "Up") ---
  for (const auto &col : columnNames) {
    static constexpr std::size_t kDownLen = 4; // "Down"
    if (col.size() <= kDownLen) {
      continue;
    }
    if (col.compare(col.size() - kDownLen, kDownLen, "Down") != 0) {
      continue;
    }

    const std::string prefix = col.substr(0, col.size() - kDownLen);
    const std::size_t lastUnderscore = prefix.rfind('_');
    if (lastUnderscore == std::string::npos || lastUnderscore == 0) {
      continue;
    }

    const std::string baseVar  = prefix.substr(0, lastUnderscore);
    const std::string systName = prefix.substr(lastUnderscore + 1);
    if (baseVar.empty() || systName.empty()) {
      continue;
    }

    const std::string pairKey = baseVar + "_" + systName;
    if (processedPairs.count(pairKey)) {
      continue; // already handled (complete pair) in Pass 1
    }

    // Up column does not exist and we haven't processed this key yet.
    result.missingUp.push_back(col);
    processedPairs.insert(pairKey); // avoid duplicate missingUp entries
  }

  return result;
}


/**
 * @brief Make a list of systematic variations and define counter columns for branchName
 *
 * On the first call for a given branchName, builds the variation list from
 * the currently registered systematics, defines the corresponding counter
 * columns in @p dataManager, and caches the result.  Subsequent calls with
 * the same branchName are no-ops for column definition and return the cached
 * list, making the method safe to call from multiple plugins or services.
 */
 std::vector<std::string>
 SystematicManager::makeSystList(const std::string &branchName, IDataFrameProvider &dataManager) {

   // Return the cached list if this branchName has already been materialized.
   auto it = materializedSystLists_m.find(branchName);
   if (it != materializedSystLists_m.end()) {
     return it->second;
   }

   // Build the variation list from the currently registered systematics.
   std::vector<std::string> systList = {"Nominal"};
   for (const auto &syst : getSystematics()) {
     systList.push_back(syst + "Up");
     systList.push_back(syst + "Down");
   }

   // Define integer counter columns for this branchName namespace.
   // Columns that already exist in the dataframe are silently skipped.
   auto df = dataManager.getDataFrame();
   const auto existingColumns = df.GetColumnNames();
   std::unordered_set<std::string> columnSet(existingColumns.begin(), existingColumns.end());
   auto ensureColumn = [&](const std::string &name, int index) {
     if (columnSet.find(name) != columnSet.end()) {
       return;
     }
     dataManager.Define(
         name,
         [index]() -> float {
           return index;
         },
         {},
         *this);
     columnSet.insert(name);
   };

   int var = 0;
   ensureColumn(branchName, var);

   for (const auto &syst : getSystematics()) {
     var++;
     ensureColumn(branchName + "_" + syst + "Up", var);
     var++;
     ensureColumn(branchName + "_" + syst + "Down", var);
   }

   // Cache the list for this branchName so future calls are no-ops.
   materializedSystLists_m[branchName] = systList;
   return systList;
 }

/**
 * @brief Check whether counter columns have already been materialized for branchName
 * @param branchName The branch namespace to check
 * @return True if makeSystList has already been called for this branchName
 */
bool SystematicManager::isBranchNameMaterialized(const std::string &branchName) const {
  return materializedSystLists_m.count(branchName) != 0;
}