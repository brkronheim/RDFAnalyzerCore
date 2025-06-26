#include <ConfigurationManager.h>
#include <TriggerManager.h>

/**
 * @brief Construct a new TriggerManager object
 * @param configManager Reference to the configuration manager
 */
TriggerManager::TriggerManager(const ConfigurationManager &configManager) {
  registerTriggers(configManager);
}

/**
 * @brief Register triggers and vetoes from the configuration
 * @param configManager Reference to the configuration manager
 */
void TriggerManager::registerTriggers(
    const ConfigurationManager &configManager) {
  const auto triggerConfig = configManager.parseMultiKeyConfig(
      "triggerConfig", {"name", "sample", "triggers"});

  for (const auto &entryKeys : triggerConfig) {

    auto triggerList = configManager.splitString(entryKeys.at("triggers"), ",");

    if (entryKeys.find("triggerVetos") != entryKeys.end()) {
      auto triggerVetoList =
          configManager.splitString(entryKeys.at("triggerVetos"), ",");
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