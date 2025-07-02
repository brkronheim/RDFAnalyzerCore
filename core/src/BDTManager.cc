#include <BDTManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>

/**
 * @brief Construct a new BDTManager object
 * @param configProvider Reference to the configuration provider
 */
BDTManager::BDTManager(const IConfigurationProvider &configProvider) {
  registerBDTs(configProvider);
}

/**
 * @brief Apply a BDT to the input features
 * @param dataFrameProvider Reference to the dataframe provider for dataframe operations
 * @param bdtName Name of the BDT
 */
void BDTManager::applyBDT(IDataFrameProvider& dataFrameProvider,
                          const std::string &bdtName,
                          ISystematicManager &systematicManager) {
  const auto &inputFeatures = getBDTFeatures(bdtName);
  const auto &runVar = getRunVar(bdtName);
  dataFrameProvider.DefineVector("input_" + bdtName, inputFeatures, "Float_t", systematicManager);
  auto bdt = this->objects_m.at(bdtName);
  auto bdtLambda = [bdt](ROOT::VecOps::RVec<Float_t> &inputVector,
                         bool runVar) -> Float_t {
    if (runVar) {
      return (1. / (1. + std::exp(-((*bdt.get())(inputVector.data())))));
    } else {
      return (-1);
    }
  };
  dataFrameProvider.Define(bdtName, bdtLambda, {"input_" + bdtName, runVar}, systematicManager);
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
 * @param configProvider Reference to the configuration provider
 */
void BDTManager::registerBDTs(const IConfigurationProvider &configProvider) {
  const auto bdtConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("bdtConfig"),
      {"file", "name", "inputVariables", "runVar"});

  for (const auto &entryKeys : bdtConfig) {

    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

    // Add BDT
    std::vector<std::string> features = inputVariableVector;

    // Load the BDT
    auto bdt = fastforest::load_txt(entryKeys.at("file"), features);

    // Add the BDT and feature list to their maps
    objects_m.emplace(entryKeys.at("name"),
                      std::make_shared<fastforest::FastForest>(bdt));
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
    bdt_runVars_m.emplace(entryKeys.at("name"), entryKeys.at("runVar"));
  }
}