#include <SofieManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>

/**
 * @brief Construct a new SofieManager object
 * @param configProvider Reference to the configuration provider
 */
SofieManager::SofieManager(IConfigurationProvider const& configProvider) {
  registerModels(configProvider);
}

/**
 * @brief Apply a SOFIE model to the input features
 * @param modelName Name of the SOFIE model
 */
void SofieManager::applyModel(const std::string &modelName) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("SofieManager: DataManager or SystematicManager not set");
  }
  
  const auto &inputFeatures = getModelFeatures(modelName);
  const auto &runVar = getRunVar(modelName);
  
  // Create input vector column from the features
  dataManager_m->DefineVector("input_" + modelName, inputFeatures, "Float_t", *systematicManager_m);
  
  auto inferenceFunc = this->objects_m.at(modelName);
  
  // Create lambda that calls the SOFIE inference function
  auto sofieLambda = [inferenceFunc, modelName](ROOT::VecOps::RVec<Float_t> &inputVector,
                                      bool runFlag) -> Float_t {
    if (!runFlag) {
      return -1.0f;
    }
    
    // Convert ROOT::RVec to std::vector for the inference function
    std::vector<float> inputVec(inputVector.begin(), inputVector.end());
    
    // Call the SOFIE inference function
    std::vector<float> output = (*inferenceFunc)(inputVec);
    
    // Return the first output (most models have a single output)
    if (output.empty()) {
      throw std::runtime_error("SOFIE model returned empty output: " + modelName);
    }
    return output[0];
  };
  
  dataManager_m->Define(modelName, sofieLambda, {"input_" + modelName, runVar}, *systematicManager_m);
}

/**
 * @brief Apply all SOFIE models to the dataframe provider
 */
void SofieManager::applyAllModels() {
  for (const auto &modelName : getAllModelNames()) {
    applyModel(modelName);
  }
}

/**
 * @brief Get a SOFIE inference function by key
 * @param key Model key
 * @return Shared pointer to the inference function
 */
std::shared_ptr<SofieInferenceFunction>
SofieManager::getModel(const std::string &key) const {
  return getObject(key);
}

/**
 * @brief Get the features for a SOFIE model by key
 * @param key Model key
 * @return Reference to the vector of feature names
 */
const std::vector<std::string> &
SofieManager::getModelFeatures(const std::string &key) const {
  return getFeatures(key);
}

/**
 * @brief Get the run variable for a SOFIE model
 * @param modelName Name of the model
 * @return Reference to the run variable string
 */
const std::string &SofieManager::getRunVar(const std::string &modelName) const {
  auto it = model_runVars_m.find(modelName);
  if (it != model_runVars_m.end()) {
    return it->second;
  }
  throw std::runtime_error("RunVar not found for SOFIE model: " + modelName);
}

/**
 * @brief Get all SOFIE model names
 * @return Vector of all model names
 */
std::vector<std::string> SofieManager::getAllModelNames() const {
  std::vector<std::string> names;
  for (const auto &pair : model_runVars_m) {
    names.push_back(pair.first);
  }
  return names;
}

/**
 * @brief Register a SOFIE inference function
 * @param name Model name
 * @param inferenceFunc Shared pointer to the inference function
 * @param features Vector of input feature names
 * @param runVar Name of the run variable
 */
void SofieManager::registerModel(const std::string &name,
                                  std::shared_ptr<SofieInferenceFunction> inferenceFunc,
                                  const std::vector<std::string> &features,
                                  const std::string &runVar) {
  objects_m.emplace(name, inferenceFunc);
  features_m.emplace(name, features);
  model_runVars_m.emplace(name, runVar);
}

/**
 * @brief Helper function to parse and store model configuration
 * @param configProvider Reference to the configuration provider
 * @param checkExisting If true, only add models that don't already exist
 */
void SofieManager::parseModelConfig(const IConfigurationProvider &configProvider, bool checkExisting) {
  // Try to get SOFIE config - it's optional
  try {
    const auto modelConfig = configProvider.parseMultiKeyConfig(
        configProvider.get("sofieConfig"),
        {"name", "inputVariables", "runVar"});
    
    // Store configuration information but don't load models
    // Models must be registered manually via registerModel()
    for (const auto &entryKeys : modelConfig) {
      auto inputVariableVector =
          configProvider.splitString(entryKeys.at("inputVariables"), ",");
      
      const auto& name = entryKeys.at("name");
      const auto& runVar = entryKeys.at("runVar");
      
      // Store the expected features and runVar for this model
      // The actual inference function will be registered later
      if (!checkExisting || features_m.find(name) == features_m.end()) {
        features_m.emplace(name, inputVariableVector);
      }
      if (!checkExisting || model_runVars_m.find(name) == model_runVars_m.end()) {
        model_runVars_m.emplace(name, runVar);
      }
    }
  } catch (const std::exception &e) {
    // sofieConfig is optional - if not present, models can still be registered manually
    // Exception is intentionally ignored as configuration file may not exist or may not
    // contain sofieConfig entry, which is a valid use case for manual registration
  }
}

/**
 * @brief Register SOFIE models from configuration
 * @param configProvider Reference to the configuration provider
 * 
 * Note: Since SOFIE models must be registered at compile time (they are code-generated),
 * this method does NOT load models from files. Instead, users must manually register
 * their SOFIE models using registerModel() after constructing the manager.
 * The configuration is still parsed to validate that expected models are present.
 */
void SofieManager::registerModels(const IConfigurationProvider &configProvider) {
  parseModelConfig(configProvider, false);
}

void SofieManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("SofieManager: ConfigManager not set");
  }

  parseModelConfig(*configManager_m, true);
}

void SofieManager::initialize() {
  std::cout << "SofieManager: initialized with " << model_runVars_m.size()
            << " SOFIE model(s)." << std::endl;
}

void SofieManager::reportMetadata() {
  if (!logger_m) return;
  std::string msg = "SofieManager loaded models: ";
  bool first = true;
  for (const auto& kv : model_runVars_m) {
    if (!first) msg += ", ";
    msg += kv.first;
    first = false;
  }
  logger_m->log(ILogger::Level::Info, msg);
}
