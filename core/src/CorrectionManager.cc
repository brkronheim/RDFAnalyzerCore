#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <iostream>

/**
 * @brief Construct a new CorrectionManager object
 * @param configManager Reference to the ConfigurationManager
 */
CorrectionManager::CorrectionManager(
    const ConfigurationManager &configManager) {
  registerCorrectionlib(configManager);
}

/**
 * @brief Get a correction object by key
 * @param key Correction key
 * @return Correction reference
 */
correction::Correction::Ref
CorrectionManager::getCorrection(const std::string &key) const {
  return getObject(key);
}

/**
 * @brief Get the features for a correction by key
 * @param key Correction key
 * @return Reference to the vector of feature names
 */
const std::vector<std::string> &
CorrectionManager::getCorrectionFeatures(const std::string &key) const {
  return getFeatures(key);
}

/**
 * @brief Register corrections from correctionlib using the configuration
 * @param configManager Reference to the ConfigurationManager
 */
void CorrectionManager::registerCorrectionlib(
    const ConfigurationManager &configManager) {
  const auto correctionConfig = configManager.parseMultiKeyConfig(
      "correctionlibConfig",
      {"file", "correctionName", "name", "inputVariables"});

  for (const auto &entryKeys : correctionConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configManager.splitString(entryKeys.at("inputVariables"), ",");

    // load correction object from json
    auto correctionF =
        correction::CorrectionSet::from_file(entryKeys.at("file"));
    auto correction = correctionF->at(entryKeys.at("correctionName"));

    // Add the correction and feature list to their maps
    std::cout << "Adding correction " << entryKeys.at("name") << "!"
              << std::endl;
    objects_m.emplace(entryKeys.at("name"), correction);
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
  }
}