#include <CorrectionManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <iostream>

/**
 * @brief Construct a new CorrectionManager object
 * @param configProvider Reference to the configuration provider
 */
CorrectionManager::CorrectionManager(IConfigurationProvider const& configProvider) {
  registerCorrectionlib(configProvider);
  initialized_m = true;
}

/**
 * @brief Apply a correction to a set of input features
 * @param correctionName Name of the correction
 * @param stringArguments String arguments for the correction
 */
void CorrectionManager::applyCorrection(const std::string &correctionName,
                                        const std::vector<std::string> &stringArguments) {  
  std::cout << "Applying correction " << correctionName << std::endl;

  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  }

  auto df = dataManager_m->getDataFrame();

  const auto &inputFeatures = getCorrectionFeatures(correctionName);
  dataManager_m->DefineVector("input_" + correctionName, inputFeatures, "double", *systematicManager_m);
  auto correction = this->objects_m.at(correctionName);
  auto stringArgs = stringArguments;
  auto correctionLambda =
      [correction,
       stringArgs](ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
    std::vector<std::variant<int, double, std::string>> values;
    auto stringArgIt = stringArgs.begin();
    auto doubleArgIt = inputVector.begin();
    for (const auto &varType : correction->inputs()) {
      if (varType.typeStr() == "string") {
        values.push_back(*stringArgIt);
        ++stringArgIt;
      } else if (varType.typeStr() == "int") {
        values.push_back(int(*doubleArgIt));
        ++doubleArgIt;
      } else {
        values.push_back(*doubleArgIt);
        ++doubleArgIt;
      }
    }
    return correction->evaluate(values);
  };
  dataManager_m->Define(correctionName, correctionLambda, {"input_" + correctionName}, *systematicManager_m);
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
 * @param configProvider Reference to the configuration provider
 */
void CorrectionManager::registerCorrectionlib(
    const IConfigurationProvider &configProvider) {
  auto correctionConfigFile = configProvider.get("correctionConfig");
  if (correctionConfigFile.empty()) {
    correctionConfigFile = "correctionlibConfig";
  }

  const auto correctionConfig = configProvider.parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables"});

  for (const auto &entryKeys : correctionConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

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

void CorrectionManager::setupFromConfigFile() {
  if (initialized_m) {
    return;
  }

  if (!configManager_m) {
    throw std::runtime_error("CorrectionManager: ConfigManager not set");
  }

  auto correctionConfigFile = configManager_m->get("correctionConfig");
  if (correctionConfigFile.empty()) {
    correctionConfigFile = "correctionlibConfig";
  }

  const auto correctionConfig = configManager_m->parseMultiKeyConfig(
    correctionConfigFile,
    {"file", "correctionName", "name", "inputVariables"});

  for (const auto &entryKeys : correctionConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configManager_m->splitString(entryKeys.at("inputVariables"), ",");

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

  initialized_m = true;
}