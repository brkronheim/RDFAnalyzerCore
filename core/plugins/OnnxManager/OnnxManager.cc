#include <OnnxManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <cstring>

/**
 * @brief Construct a new OnnxManager object
 * @param configProvider Reference to the configuration provider
 */
OnnxManager::OnnxManager(IConfigurationProvider const& configProvider) {
  // Initialize ONNX Runtime environment
  env_m = std::make_shared<Ort::Env>(ORT_LOGGING_LEVEL_WARNING, "RDFAnalyzer");
  registerModels(configProvider);
}

/**
 * @brief Apply an ONNX model to the input features
 * @param modelName Name of the ONNX model
 */
void OnnxManager::applyModel(const std::string &modelName) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("OnnxManager: DataManager or SystematicManager not set");
  }
  
  const auto &inputFeatures = getModelFeatures(modelName);
  const auto &runVar = getRunVar(modelName);
  
  // Define the input vector for the model
  dataManager_m->DefineVector("input_" + modelName, inputFeatures, "Float_t", *systematicManager_m);
  
  auto session = this->objects_m.at(modelName);
  auto inputNames = model_inputNames_m.at(modelName);
  auto outputNames = model_outputNames_m.at(modelName);
  
  // Create a lambda that performs ONNX inference
  auto onnxLambda = [session, inputNames, outputNames](
      ROOT::VecOps::RVec<Float_t> &inputVector,
      bool runVar) -> Float_t {
    if (!runVar) {
      return -1.0f;
    }
    
    // Create ONNX memory info
    auto memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
    
    // Prepare input tensor
    std::vector<int64_t> input_shape = {1, static_cast<int64_t>(inputVector.size())};
    std::vector<float> input_data(inputVector.begin(), inputVector.end());
    
    Ort::Value input_tensor = Ort::Value::CreateTensor<float>(
        memory_info, input_data.data(), input_data.size(),
        input_shape.data(), input_shape.size());
    
    // Prepare input and output names as C strings
    std::vector<const char*> input_names_cstr;
    for (const auto& name : inputNames) {
      input_names_cstr.push_back(name.c_str());
    }
    std::vector<const char*> output_names_cstr;
    for (const auto& name : outputNames) {
      output_names_cstr.push_back(name.c_str());
    }
    
    // Run inference
    auto output_tensors = session->Run(
        Ort::RunOptions{nullptr},
        input_names_cstr.data(), &input_tensor, 1,
        output_names_cstr.data(), output_names_cstr.size());
    
    // Extract the first output value
    float* output_data = output_tensors[0].GetTensorMutableData<float>();
    return output_data[0];
  };
  
  dataManager_m->Define(modelName, onnxLambda, {"input_" + modelName, runVar}, *systematicManager_m);
}

/**
 * @brief Apply all ONNX models to the dataframe provider
 */
void OnnxManager::applyAllModels() {
  for (const auto &modelName : getAllModelNames()) {
    applyModel(modelName);
  }
}

/**
 * @brief Get an ONNX session object by key
 * @param key Model key
 * @return Shared pointer to the ONNX Session object
 */
std::shared_ptr<Ort::Session>
OnnxManager::getModel(const std::string &key) const {
  return getObject(key);
}

/**
 * @brief Get the features for an ONNX model by key
 * @param key Model key
 * @return Reference to the vector of feature names
 */
const std::vector<std::string> &
OnnxManager::getModelFeatures(const std::string &key) const {
  return getFeatures(key);
}

/**
 * @brief Get the run variable for an ONNX model
 * @param modelName Name of the model
 * @return Reference to the run variable string
 */
const std::string &OnnxManager::getRunVar(const std::string &modelName) const {
  auto it = model_runVars_m.find(modelName);
  if (it != model_runVars_m.end()) {
    return it->second;
  }
  throw std::runtime_error("RunVar not found for ONNX model: " + modelName);
}

/**
 * @brief Get all ONNX model names
 * @return Vector of all model names
 */
std::vector<std::string> OnnxManager::getAllModelNames() const {
  std::vector<std::string> names;
  for (const auto &pair : model_runVars_m) {
    names.push_back(pair.first);
  }
  return names;
}

/**
 * @brief Register ONNX models from configuration
 * @param configProvider Reference to the configuration provider
 */
void OnnxManager::registerModels(const IConfigurationProvider &configProvider) {
  const auto modelConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("onnxConfig"),
      {"file", "name", "inputVariables", "runVar"});

  Ort::SessionOptions session_options;
  session_options.SetIntraOpNumThreads(1);
  session_options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_EXTENDED);

  for (const auto &entryKeys : modelConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

    // Load the ONNX model
    auto session = std::make_shared<Ort::Session>(*env_m, entryKeys.at("file").c_str(), session_options);

    // Get input and output names from the model
    Ort::AllocatorWithDefaultOptions allocator;
    
    // Get input names
    size_t num_input_nodes = session->GetInputCount();
    std::vector<std::string> input_names;
    for (size_t i = 0; i < num_input_nodes; i++) {
      auto input_name = session->GetInputNameAllocated(i, allocator);
      input_names.push_back(std::string(input_name.get()));
    }
    
    // Get output names
    size_t num_output_nodes = session->GetOutputCount();
    std::vector<std::string> output_names;
    for (size_t i = 0; i < num_output_nodes; i++) {
      auto output_name = session->GetOutputNameAllocated(i, allocator);
      output_names.push_back(std::string(output_name.get()));
    }

    // Add the model and feature list to their maps
    objects_m.emplace(entryKeys.at("name"), session);
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
    model_runVars_m.emplace(entryKeys.at("name"), entryKeys.at("runVar"));
    model_inputNames_m.emplace(entryKeys.at("name"), input_names);
    model_outputNames_m.emplace(entryKeys.at("name"), output_names);
  }
}

void OnnxManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("OnnxManager: ConfigManager not set");
  }

  const auto modelConfig = configManager_m->parseMultiKeyConfig(
    configManager_m->get("onnxConfig"),
    {"file", "name", "inputVariables", "runVar"});

  Ort::SessionOptions session_options;
  session_options.SetIntraOpNumThreads(1);
  session_options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_EXTENDED);

  for (const auto &entryKeys : modelConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
      configManager_m->splitString(entryKeys.at("inputVariables"), ",");

    // Load the ONNX model
    auto session = std::make_shared<Ort::Session>(*env_m, entryKeys.at("file").c_str(), session_options);

    // Get input and output names from the model
    Ort::AllocatorWithDefaultOptions allocator;
    
    // Get input names
    size_t num_input_nodes = session->GetInputCount();
    std::vector<std::string> input_names;
    for (size_t i = 0; i < num_input_nodes; i++) {
      auto input_name = session->GetInputNameAllocated(i, allocator);
      input_names.push_back(std::string(input_name.get()));
    }
    
    // Get output names
    size_t num_output_nodes = session->GetOutputCount();
    std::vector<std::string> output_names;
    for (size_t i = 0; i < num_output_nodes; i++) {
      auto output_name = session->GetOutputNameAllocated(i, allocator);
      output_names.push_back(std::string(output_name.get()));
    }

    // Add the model and feature list to their maps
    objects_m.emplace(entryKeys.at("name"), session);
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
    model_runVars_m.emplace(entryKeys.at("name"), entryKeys.at("runVar"));
    model_inputNames_m.emplace(entryKeys.at("name"), input_names);
    model_outputNames_m.emplace(entryKeys.at("name"), output_names);
  }
}
