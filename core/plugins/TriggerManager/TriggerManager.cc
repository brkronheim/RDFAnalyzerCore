#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <TriggerManager.h>
#include <ROOT/RVec.hxx>
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
 */
void TriggerManager::applyAllTriggers() {
  if (!dataManager_m || !systematicManager_m || !configManager_m) {
    throw std::runtime_error("TriggerManager: DataManager, SystematicManager, or ConfigManager not set");
  }
  
  std::string sampleType;
  try {
    sampleType = configManager_m->get("type");
  } catch (...) {
    throw std::runtime_error("Config does not contain 'type' key for trigger logic");
  }
  
  std::string group = getGroupForSample(sampleType);
  
  auto passTrigger = [](const ROOT::VecOps::RVec<bool>& triggerVec) {
    for (bool v : triggerVec) {
      if (v) return true;
    }
    return false;
  };
  
  auto passTriggerAndVeto = [](const ROOT::VecOps::RVec<bool>& passVec, const ROOT::VecOps::RVec<bool>& vetoVec) {
    bool pass = false;
    for (bool v : passVec) {
      if (v) pass = true;
    }
    for (bool v : vetoVec) {
      if (v) return false;
    }
    return pass;
  };
  
  // Avoid forcing a dataframe evaluation here; counting entries would
  // trigger an event loop before the main analysis execution, which doubles
  // runtime.  Counters/logging can be added later if needed, but the
  // default behaviour should be lazy.
  auto dfBefore = dataManager_m->getDataFrame();

  if (!group.empty()) {
    const auto& triggers = getTriggers(group);
    const auto& vetoes = getVetoes(group);
    if (vetoes.empty()) {
      dataManager_m->DefineVector("allTriggersPassVector", triggers, "Bool_t", *systematicManager_m);
      // Define the filter variable
      dataManager_m->Define("pass_applyTrigger", passTrigger, {"allTriggersPassVector"}, *systematicManager_m);
      // Optional debug: count rows passing pass_applyTrigger pre-apply. Only runs
      // if TRIGGER_MANAGER_DEBUG is set (to avoid an eager event loop).
      if (std::getenv("TRIGGER_MANAGER_DEBUG") != nullptr) {
        auto dfDbg = dataManager_m->getDataFrame();
        auto cntDbg = dfDbg.Filter([](bool val){ return val; }, {"pass_applyTrigger"}).Count();
        std::cout << "TriggerManager debug: rows passing pass_applyTrigger (pre-apply): " << cntDbg.GetValue() << std::endl;
      }
      // Apply the filter
      dataManager_m->Filter([](bool val) { return val; }, {"pass_applyTrigger"});
    } else {
      dataManager_m->DefineVector(group + "_passVector", triggers, "Bool_t", *systematicManager_m);
      dataManager_m->DefineVector(group + "_vetoVector", vetoes, "Bool_t", *systematicManager_m);
      // Define the filter variable
      dataManager_m->Define("pass_applyTrigger", passTriggerAndVeto, {group + "_passVector", group + "_vetoVector"}, *systematicManager_m);
      // Optional debug: count rows passing pass_applyTrigger pre-apply, only when
      // TRIGGER_MANAGER_DEBUG is set.
      if (std::getenv("TRIGGER_MANAGER_DEBUG") != nullptr) {
        auto dfDbg = dataManager_m->getDataFrame();
        auto cntDbg = dfDbg.Filter([](bool val){ return val; }, {"pass_applyTrigger"}).Count();
        std::cout << "TriggerManager debug: rows passing pass_applyTrigger (pre-apply): " << cntDbg.GetValue() << std::endl;
      }
      // Apply the filter
      dataManager_m->Filter([](bool val) { return val; }, {"pass_applyTrigger"});
    }
  } else {
    std::vector<std::string> allTriggers;
    for (const auto& g : getAllGroups()) {
      const auto& triggers = getTriggers(g);
      allTriggers.insert(allTriggers.end(), triggers.begin(), triggers.end());
    }
    dataManager_m->DefineVector("allTriggersPassVector", allTriggers, "Bool_t", *systematicManager_m);
    // Define the filter variable
    dataManager_m->Define("pass_applyTrigger", passTrigger, {"allTriggersPassVector"}, *systematicManager_m);
    // Apply the filter
    dataManager_m->Filter([](bool val) { return val; }, {"pass_applyTrigger"});
  }

  // Similarly, avoid counting after applying triggers unless debugging.
  if (std::getenv("TRIGGER_MANAGER_DEBUG") != nullptr) {
    auto dfAfter = dataManager_m->getDataFrame();
    auto totalAfter = dfAfter.Count();
    std::cout << "TriggerManager: rows after applying triggers: " << totalAfter.GetValue() << std::endl;
  }
}


void TriggerManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("TriggerManager: ConfigManager not set");
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
