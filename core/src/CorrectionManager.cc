#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <CorrectionManager.h>
#include <iostream>

/**
 * @brief Construct a new CorrectionManager object
 * @param configProvider Reference to the configuration provider
 */
CorrectionManager::CorrectionManager(
    const IConfigurationProvider &configProvider) {
  registerCorrectionlib(configProvider);
}

/**
 * @brief Apply a correction to a set of input features
 * @param dataFrameProvider Reference to the dataframe provider for dataframe operations
 * @param correctionName Name of the correction
 * @param stringArguments String arguments for the correction
 * @param inputFeatures Input features for the correction
 */
void CorrectionManager::applyCorrection(IDataFrameProvider& dataFrameProvider,
                                        const std::string &correctionName,
                                        const std::vector<std::string> &stringArguments) {  
  const auto &inputFeatures = getCorrectionFeatures(correctionName);
  dataFrameProvider.DefineVector("input_" + correctionName, inputFeatures, "double");
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
  dataFrameProvider.Define(correctionName, correctionLambda, {"input_" + correctionName});
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
  const auto correctionConfig = configProvider.parseMultiKeyConfig(
      "correctionlibConfig",
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