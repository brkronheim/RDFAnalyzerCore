#include <CorrectionManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <algorithm>
#include <iostream>

/**
 * @brief Construct a new CorrectionManager object
 * @param configProvider Reference to the configuration provider
 */
CorrectionManager::CorrectionManager(IConfigurationProvider const& configProvider) {
  std::cout  << "Constructing CorrectionManager with config provider" << std::endl;
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
  std::cout << "Getting input features" << std::endl;
  const auto &inputFeatures = getCorrectionFeatures(correctionName);
  std::cout << "Defining input features for correction " << correctionName << std::endl;
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
 * @brief Apply a correction over a vector of objects and store per-object
 *        results as an RVec<Float_t> column in the dataframe.
 *
 * Each input variable registered for the correction must be an RVec column
 * whose i-th element holds the value of that variable for the i-th object.
 * String arguments are constant per event and are passed via @p stringArguments
 * in the same order they appear in the correctionlib JSON.
 *
 * @param correctionName Name of the correction
 * @param stringArguments Constant string arguments for the correction
 */
void CorrectionManager::applyCorrectionVec(
    const std::string &correctionName,
    const std::vector<std::string> &stringArguments) {
  std::cout << "Applying vector correction " << correctionName << std::endl;
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error(
        "CorrectionManager: DataManager or SystematicManager not set");
  }

  const auto &inputFeatures = getCorrectionFeatures(correctionName);

  // Validate that all required input columns exist in the dataframe.
  {
    const auto existingColumns = dataManager_m->getDataFrame().GetColumnNames();
    std::vector<std::string> missing;
    for (const auto &f : inputFeatures) {
      if (std::find(existingColumns.begin(), existingColumns.end(), f) ==
          existingColumns.end()) {
        missing.push_back(f);
      }
    }
    if (!missing.empty()) {
      std::string msg = "applyCorrectionVec: missing columns: ";
      for (const auto &m : missing) {
        msg += m + " ";
      }
      throw std::runtime_error(msg);
    }
  }

  // Build an intermediate column of type RVec<RVec<double>> that packs the
  // per-object feature values.  Element [i][j] holds the value of input
  // feature j for the i-th object in the collection.
  if (inputFeatures.empty()) {
    throw std::runtime_error(
        "applyCorrectionVec: correction '" + correctionName +
        "' has no registered input variables");
  }
  const std::string inputVecName = "input_vec_" + correctionName;
  {
    auto dfNode = dataManager_m->getDataFrame();
    const auto existingColumns = dfNode.GetColumnNames();
    if (std::find(existingColumns.begin(), existingColumns.end(),
                  inputVecName) == existingColumns.end()) {
      std::string expr =
          "ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> _out(" +
          inputFeatures[0] + ".size());\n";
      for (const auto &f : inputFeatures) {
        expr += "for (size_t _i = 0; _i < _out.size(); ++_i)"
                " _out[_i].push_back(static_cast<double>(" +
                f + "[_i]));\n";
      }
      expr += "return _out;";
      dfNode = dfNode.Define(inputVecName, expr);
      dataManager_m->setDataFrame(dfNode);
    }
  }

  // Lambda that applies the correction to every object in the collection.
  auto correction = this->objects_m.at(correctionName);
  auto stringArgs = stringArguments;
  auto correctionLambda =
      [correction, stringArgs](
          const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>>
              &inputMatrix) -> ROOT::VecOps::RVec<Float_t> {
    ROOT::VecOps::RVec<Float_t> result(inputMatrix.size());
    for (size_t i = 0; i < inputMatrix.size(); ++i) {
      std::vector<std::variant<int, double, std::string>> values;
      auto stringArgIt = stringArgs.begin();
      auto doubleArgIt = inputMatrix[i].begin();
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
      result[i] = correction->evaluate(values);
    }
    return result;
  };
  dataManager_m->Define(correctionName, correctionLambda, {inputVecName},
                        *systematicManager_m);
}

/**
 * @brief Register corrections from correctionlib using the configuration
 * @param configProvider Reference to the configuration provider
 */
void CorrectionManager::registerCorrectionlib(
    const IConfigurationProvider &configProvider) {
  // configProvider.get() returns an empty string when the key is absent.
  // Prefer "correctionConfig"; fall back to the legacy "correctionlibConfig".
  auto correctionConfigFile = configProvider.get("correctionConfig");
  if (correctionConfigFile.empty()) {
    correctionConfigFile = configProvider.get("correctionlibConfig");
  }
  std::cout << "CorrectionManager: Registering corrections from config file: " << correctionConfigFile << std::endl;

  const auto correctionConfig = configProvider.parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables"});
  
  std::cout << "CorrectionManager: Found " << correctionConfig.size() << " corrections in config file." << std::endl;

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
void CorrectionManager::initialize() {
  std::cout << "CorrectionManager: initialized with " << objects_m.size()
            << " correction(s)." << std::endl;
}

void CorrectionManager::reportMetadata() {
  if (!logger_m) return;
  std::string msg = "CorrectionManager loaded corrections: ";
  bool first = true;
  for (const auto& kv : objects_m) {
    if (!first) msg += ", ";
    msg += kv.first;
    first = false;
  }
  logger_m->log(ILogger::Level::Info, msg);
}
