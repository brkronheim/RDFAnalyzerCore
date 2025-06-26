#include <BDTManager.h>
#include <ConfigurationManager.h>

/**
 * @brief Construct a new BDTManager object
 * @param configManager Reference to the ConfigurationManager
 */
BDTManager::BDTManager(const ConfigurationManager &configManager) {
  registerBDTs(configManager);
}

/**
 * @brief Get a BDT object by key
 * @param key BDT key
 * @return Shared pointer to the FastForest object
 */
std::shared_ptr<fastforest::FastForest>
BDTManager::getBDT(const std::string &key) const {
  return getObject(key);
}

/**
 * @brief Get the features for a BDT by key
 * @param key BDT key
 * @return Reference to the vector of feature names
 */
const std::vector<std::string> &
BDTManager::getBDTFeatures(const std::string &key) const {
  return getFeatures(key);
}

/**
 * @brief Get the run variable for a BDT
 * @param bdtName Name of the BDT
 * @return Reference to the run variable string
 */
const std::string &BDTManager::getRunVar(const std::string &bdtName) const {
  auto it = bdt_runVars_m.find(bdtName);
  if (it != bdt_runVars_m.end()) {
    return it->second;
  }
  throw std::runtime_error("RunVar not found for BDT: " + bdtName);
}

/**
 * @brief Get all BDT names
 * @return Vector of all BDT names
 */
std::vector<std::string> BDTManager::getAllBDTNames() const {
  std::vector<std::string> names;
  for (const auto &pair : bdt_runVars_m) {
    names.push_back(pair.first);
  }
  return names;
}

/**
 * @brief Register BDTs from configuration
 * @param configManager Reference to the ConfigurationManager
 */
void BDTManager::registerBDTs(const ConfigurationManager &configManager) {
  const auto bdtConfig = configManager.parseMultiKeyConfig(
      "bdtConfig", {"file", "name", "inputVariables", "runVar"});

  for (const auto &entryKeys : bdtConfig) {

    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configManager.splitString(entryKeys.at("inputVariables"), ",");

    // Add BDT

    std::vector<std::string> features;
    for (long unsigned int i = 0; i < inputVariableVector.size(); i++) {
      features.push_back("f" + std::to_string(i));
    }

    // Load the BDT
    auto bdt = fastforest::load_txt(entryKeys.at("file"), features);

    // Add the BDT and feature list to their maps
    objects_m.emplace(entryKeys.at("name"), &bdt);
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
    bdt_runVars_m.emplace(entryKeys.at("name"), entryKeys.at("runVar"));
  }
}