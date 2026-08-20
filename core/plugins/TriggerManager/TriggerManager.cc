#include <api/IConfigurationProvider.h>
#include <analyzer.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <TriggerManager.h>
#include <ROOT/RVec.hxx>
#include <algorithm>
#include <stdexcept>

/**
 * @brief Construct a new TriggerManager object
 * @param configProvider Reference to the configuration provider
 */
TriggerManager::TriggerManager(IConfigurationProvider const& configProvider) {
  // Initialize from configProvider if needed
  registerTriggers(configProvider);
}

/**
 * @brief Register triggers and vetoes from the configuration
 * @param configProvider Reference to the configuration provider
 */
void TriggerManager::registerTriggers(
    const IConfigurationProvider &configProvider) {

  if (configProvider.get("triggerConfig") == "") {
    throw std::runtime_error(
        "TriggerManager: 'triggerConfig' key not found or empty in config. Add 'triggerConfig=<path>' to your configuration.");
  }

  const auto triggerConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("triggerConfig"), {"name", "sample", "triggers"});

  for (const auto &entryKeys : triggerConfig) {

    auto triggerList = configProvider.splitString(entryKeys.at("triggers"), ",");

    if (entryKeys.find("triggerVetos") != entryKeys.end()) {
      auto triggerVetoList =
          configProvider.splitString(entryKeys.at("triggerVetos"), ",");
      vetoes_m.emplace(entryKeys.at("name"), triggerVetoList);
    } else {
      vetoes_m[entryKeys.at("name")] = {};
    }

    objects_m.emplace(entryKeys.at("name"), triggerList);
    sampleToGroup_m.emplace(entryKeys.at("sample"), entryKeys.at("name"));
  }

  // Expand group-name references in veto lists.
  // If a veto entry matches a registered group name, it is replaced by the
  // full trigger list of that group.  This lets the config use concise
  // references like  triggerVetos: EGamma, DoubleMuon  instead of repeating
  // every individual trigger path.
  for (auto &kv : vetoes_m) {
    auto &vetoList = kv.second;
    std::vector<std::string> expanded;
    expanded.reserve(vetoList.size());
    for (const auto &veto : vetoList) {
      const auto it = objects_m.find(veto);
      if (it != objects_m.end()) {
        // 'veto' is the name of another group — insert all its triggers.
        const auto &groupTriggers = it->second;
        expanded.insert(expanded.end(),
                        groupTriggers.begin(), groupTriggers.end());
      } else {
        // 'veto' is a plain trigger column name — keep as-is.
        expanded.push_back(veto);
      }
    }
    vetoList = std::move(expanded);
  }
}

/**
 * @brief Get the triggers for a given group
 * @param group Name of the trigger group
 * @return Reference to the vector of trigger names
 */
const std::vector<std::string> &
TriggerManager::getTriggers(const std::string &group) const {
  return getObject(group);
}

/**
 * @brief Get the vetoes for a given group
 * @param group Name of the trigger group
 * @return Reference to the vector of veto names
 */
const std::vector<std::string> &
TriggerManager::getVetoes(const std::string &group) const {
  auto it = vetoes_m.find(group);
  if (it != vetoes_m.end()) {
    return it->second;
  }
  static const std::vector<std::string> empty;
  return empty;
}

/**
 * @brief Get the trigger group for a given sample
 * @param sample Name of the sample
 * @return Name of the trigger group
 */
std::string TriggerManager::getGroupForSample(const std::string &sample) const {
  auto it = sampleToGroup_m.find(sample);
  if (it != sampleToGroup_m.end()) {
    return it->second;
  }
  return "";
}

/**
 * @brief Get all trigger groups
 * @return Vector of all trigger group names
 */
std::vector<std::string> TriggerManager::getAllGroups() const {
  std::vector<std::string> groups;
  for (const auto &pair : objects_m) {
    groups.push_back(pair.first);
  }
  return groups;
}

/**
 * @brief Apply all triggers for the current sample type
 *
 * Behaviour:
 *   1. Defines per-group pass columns for ALL registered groups
 *      (pass_<group>) — these are available downstream for e.g. event
 *      classification that needs per-type trigger flags.  Only trigger
 *      columns that actually exist in the input dataframe are used.
 *   2. Applies a filter:
 *      - Data  (primaryDataset matches a trigger group):
 *        keeps only events that pass the dataset's own triggers
 *        (with vetoes if configured).
 *      - MC    (no matching group):
 *        keeps events that pass ANY trigger group (logical OR).
 */
void TriggerManager::applyAllTriggers() {
  if (!dataManager_m || !systematicManager_m || !configManager_m) {
    throw std::runtime_error("TriggerManager: DataManager, SystematicManager, or ConfigManager not set");
  }

  std::string primaryDataset;
  std::string sampleType;
  try {
    primaryDataset = configManager_m->get("primaryDataset");
    sampleType = configManager_m->get("type");
  } catch (...) {
    throw std::runtime_error(
        "Config does not contain 'primaryDataset' and/or 'type' key for trigger logic");
  }

  // Look up the trigger group by primaryDataset first, then fall back
  // to 'type' for legacy configs that use the old per-year groups.
  std::string group;
  if (!primaryDataset.empty()) {
    group = getGroupForSample(primaryDataset);
  }
  if (group.empty()) {
    group = getGroupForSample(sampleType);
  }

  auto anyPass = [](const ROOT::VecOps::RVec<bool>& v) {
    for (bool b : v) if (b) return true;
    return false;
  };

  auto anyPassNoVeto = [](const ROOT::VecOps::RVec<bool>& passVec,
                           const ROOT::VecOps::RVec<bool>& vetoVec) {
    bool pass = false;
    for (bool b : passVec) if (b) pass = true;
    for (bool b : vetoVec) if (b) return false;
    return pass;
  };

  // -------------------------------------------------------------------
  // Helper: filter a list of column names to only those that actually
  // exist in the current dataframe.  This lets us define groups
  // that contain triggers from *all* years without crashing when a
  // trigger column is absent in a specific year's file.
  // -------------------------------------------------------------------
  const auto existingCols = dataManager_m->getDataFrame().GetColumnNames();
  auto existingOnly = [&](const std::vector<std::string>& cols) {
    std::vector<std::string> out;
    out.reserve(cols.size());
    for (const auto& c : cols) {
      if (std::find(existingCols.begin(), existingCols.end(), c) != existingCols.end()) {
        out.push_back(c);
      }
    }
    return out;
  };

  // -------------------------------------------------------------------
  // Step 1 — define per-group pass columns for ALL groups.
  // These make per-type trigger information available downstream
  // (e.g. channel classification that needs individual trigger flags).
  // -------------------------------------------------------------------
  const auto allGroups = getAllGroups();
  for (const auto& g : allGroups) {
    const auto& triggers = getTriggers(g);
    const auto validTriggers = existingOnly(triggers);
    if (validTriggers.empty()) continue;
    const std::string passVecName = g + "_passVector";
    dataManager_m->DefineVector(passVecName, validTriggers, "Bool_t",
                                *systematicManager_m);
    dataManager_m->Define("pass_" + g, anyPass, {passVecName},
                          *systematicManager_m);
  }

  // -------------------------------------------------------------------
  // Step 2 — apply the appropriate filter.
  // -------------------------------------------------------------------
  if (!group.empty()) {
    // ---- Data path: use the dataset's own trigger group (with veto if present).
    const auto& vetoes = getVetoes(group);
    if (vetoes.empty()) {
      dataManager_m->Filter([](bool v) { return v; }, {"pass_" + group});
    } else {
      const auto validVetoes = existingOnly(vetoes);
      if (!validVetoes.empty()) {
        const std::string vetoVecName = group + "_vetoVector";
        dataManager_m->DefineVector(vetoVecName, validVetoes, "Bool_t",
                                    *systematicManager_m);
        const std::string filterCol = "pass_" + group + "_withVeto";
        dataManager_m->Define(filterCol, anyPassNoVeto,
                              {group + "_passVector", vetoVecName},
                              *systematicManager_m);
        dataManager_m->Filter([](bool v) { return v; }, {filterCol});
      } else {
        // All veto columns missing — fall back to trigger-only filter.
        dataManager_m->Filter([](bool v) { return v; }, {"pass_" + group});
      }
    }
  } else {
    // ---- MC path: keep events that pass ANY trigger group.
    std::vector<std::string> allTriggers;
    for (const auto& g : allGroups) {
      const auto& triggers = getTriggers(g);
      const auto validTriggers = existingOnly(triggers);
      allTriggers.insert(allTriggers.end(), validTriggers.begin(),
                         validTriggers.end());
    }
    // Deduplicate: a trigger may belong to multiple groups (e.g.
    // DoubleElectron and EGamma both contain the same electron paths).
    std::sort(allTriggers.begin(), allTriggers.end());
    allTriggers.erase(std::unique(allTriggers.begin(), allTriggers.end()),
                      allTriggers.end());
    if (!allTriggers.empty()) {
      dataManager_m->DefineVector("allTriggersPassVector", allTriggers,
                                   "Bool_t", *systematicManager_m);
      dataManager_m->Define("pass_anyTrigger", anyPass,
                            {"allTriggersPassVector"}, *systematicManager_m);
      dataManager_m->Filter([](bool v) { return v; }, {"pass_anyTrigger"});
    }
  }

  // Debug counting — only when TRIGGER_MANAGER_DEBUG is set (avoids an
  // eager event loop in normal operation).
  if (std::getenv("TRIGGER_MANAGER_DEBUG") != nullptr) {
    auto dfAfter = dataManager_m->getDataFrame();
    auto totalAfter = dfAfter.Count();
    std::cout << "TriggerManager: rows after applying triggers: "
              << totalAfter.GetValue() << std::endl;
  }
}


void TriggerManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("TriggerManager: ConfigManager not set");
  }
  if(configManager_m->get("triggerConfig")=="") {
    throw std::runtime_error("TriggerManager: 'triggerConfig' key not found or empty in config. Add 'triggerConfig=<path>' to your configuration.");
  }


  const auto triggerConfig = configManager_m->parseMultiKeyConfig(
    configManager_m->get("triggerConfig"), {"name", "sample", "triggers"});

  for (const auto &entryKeys : triggerConfig) {

    auto triggerList = configManager_m->splitString(entryKeys.at("triggers"), ",");

    if (entryKeys.find("triggerVetos") != entryKeys.end()) {
      auto triggerVetoList =
          configManager_m->splitString(entryKeys.at("triggerVetos"), ",");
      vetoes_m.emplace(entryKeys.at("name"), triggerVetoList);
    } else {
      vetoes_m[entryKeys.at("name")] = {};
    }

    objects_m.emplace(entryKeys.at("name"), triggerList);
    sampleToGroup_m.emplace(entryKeys.at("sample"), entryKeys.at("name"));
  }

  // Expand group-name references in veto lists (same logic as registerTriggers).
  for (auto &kv : vetoes_m) {
    auto &vetoList = kv.second;
    std::vector<std::string> expanded;
    expanded.reserve(vetoList.size());
    for (const auto &veto : vetoList) {
      const auto it = objects_m.find(veto);
      if (it != objects_m.end()) {
        const auto &groupTriggers = it->second;
        expanded.insert(expanded.end(),
                        groupTriggers.begin(), groupTriggers.end());
      } else {
        expanded.push_back(veto);
      }
    }
    vetoList = std::move(expanded);
  }
}
void TriggerManager::initialize() {
  std::cout << "TriggerManager: initialized with " << getAllGroups().size()
            << " trigger group(s)." << std::endl;
}

void TriggerManager::reportMetadata() {
  if (!logger_m) return;
  std::string msg = "TriggerManager loaded groups: ";
  bool first = true;
  for (const auto& group : getAllGroups()) {
    if (!first) msg += ", ";
    msg += group;
    first = false;
  }
  logger_m->log(ILogger::Level::Info, msg);
}

std::shared_ptr<TriggerManager> TriggerManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<TriggerManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}
