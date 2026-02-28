#include <OnnxManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>

namespace {
/**
 * @brief Zero-pad a float vector to a target size.
 * @param data Vector to pad in-place
 * @param paddingSize Target size; no-op if 0 or already large enough
 */
void padInputData(std::vector<float>& data, int64_t paddingSize) {
  if (paddingSize > 0 && static_cast<int64_t>(data.size()) < paddingSize) {
    data.resize(paddingSize, 0.0f);
  }
}
} // namespace

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
 * @param outputSuffix Optional suffix to append to output column names
 */
void OnnxManager::applyModel(const std::string &modelName, const std::string &outputSuffix) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("OnnxManager: DataManager or SystematicManager not set");
  }
  
  const auto &inputFeatures = getModelFeatures(modelName);
  const auto &runVar = getRunVar(modelName);
  auto session = this->objects_m.at(modelName);
  auto inputNames = model_inputNames_m.at(modelName);
  auto outputNames = model_outputNames_m.at(modelName);
  int64_t paddingSize = getPaddingSize(modelName);
  
  // For models with a single input, create an input vector from the features
  // For models with multiple inputs, we assume the features are already vectors or scalars
  if (inputNames.size() == 1) {
    // Single input: combine all features into one vector
    dataManager_m->DefineVector("input_" + modelName, inputFeatures, "Float_t", *systematicManager_m);
  }
  
  // Determine how many outputs we have
  size_t numOutputs = outputNames.size();
  
  if (numOutputs == 1) {
    // Single output case - create one column with the model name
    auto onnxLambda = [session, inputNames, outputNames, paddingSize](
        ROOT::VecOps::RVec<Float_t> &inputVector,
        bool runVar) -> Float_t {
      if (!runVar) {
        return -1.0f;
      }
      
      // Create ONNX memory info
      auto memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
      
      // Prepare input data, applying zero-padding if configured
      std::vector<float> input_data(inputVector.begin(), inputVector.end());
      padInputData(input_data, paddingSize);
      std::vector<int64_t> input_shape = {1, static_cast<int64_t>(input_data.size())};
      
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
      
      // Extract the first (and only) output value
      float* output_data = output_tensors[0].GetTensorMutableData<float>();
      return output_data[0];
    };
    
    std::string outputColName = modelName + outputSuffix;
    dataManager_m->Define(outputColName, onnxLambda, {"input_" + modelName, runVar}, *systematicManager_m);
    
  } else {
    // Multiple outputs case - create separate columns for each output
    // We need to run inference once and capture all outputs
    
    // Create a lambda that returns a vector of all outputs
    auto onnxLambdaMulti = [session, inputNames, outputNames, numOutputs, paddingSize](
        ROOT::VecOps::RVec<Float_t> &inputVector,
        bool runVar) -> ROOT::VecOps::RVec<Float_t> {
      
      ROOT::VecOps::RVec<Float_t> outputs(numOutputs, -1.0f);
      
      if (!runVar) {
        return outputs;
      }
      
      // Create ONNX memory info
      auto memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
      
      // Prepare input data, applying zero-padding if configured
      std::vector<float> input_data(inputVector.begin(), inputVector.end());
      padInputData(input_data, paddingSize);
      std::vector<int64_t> input_shape = {1, static_cast<int64_t>(input_data.size())};
      
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
      
      // Extract all output values
      for (size_t i = 0; i < numOutputs; i++) {
        float* output_data = output_tensors[i].GetTensorMutableData<float>();
        outputs[i] = output_data[0];
      }
      
      return outputs;
    };
    
    // Define a column that contains all outputs as a vector
    std::string multiOutputColName = modelName + "_outputs" + outputSuffix;
    dataManager_m->Define(multiOutputColName, onnxLambdaMulti, {"input_" + modelName, runVar}, *systematicManager_m);
    
    // Now define individual columns for each output by indexing into the vector
    for (size_t i = 0; i < numOutputs; i++) {
      std::string outputColName = modelName + "_output" + std::to_string(i) + outputSuffix;
      auto indexLambda = [i](const ROOT::VecOps::RVec<Float_t> &outputs) -> Float_t {
        return outputs[i];
      };
      dataManager_m->Define(outputColName, indexLambda, {multiOutputColName}, *systematicManager_m);
    }
  }
}

/**
 * @brief Apply all ONNX models to the dataframe provider
 * @param outputSuffix Optional suffix to append to output column names
 */
void OnnxManager::applyAllModels(const std::string &outputSuffix) {
  for (const auto &modelName : getAllModelNames()) {
    applyModel(modelName, outputSuffix);
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
 * @brief Get the input names for an ONNX model
 * @param modelName Name of the model
 * @return Reference to the vector of input names
 */
const std::vector<std::string> &OnnxManager::getModelInputNames(const std::string &modelName) const {
  auto it = model_inputNames_m.find(modelName);
  if (it != model_inputNames_m.end()) {
    return it->second;
  }
  throw std::runtime_error("Input names not found for ONNX model: " + modelName);
}

/**
 * @brief Get the output names for an ONNX model
 * @param modelName Name of the model
 * @return Reference to the vector of output names
 */
const std::vector<std::string> &OnnxManager::getModelOutputNames(const std::string &modelName) const {
  auto it = model_outputNames_m.find(modelName);
  if (it != model_outputNames_m.end()) {
    return it->second;
  }
  throw std::runtime_error("Output names not found for ONNX model: " + modelName);
}

/**
 * @brief Get the padding size for an ONNX model
 * @param modelName Name of the model
 * @return Padding size (0 if no padding configured)
 */
int64_t OnnxManager::getPaddingSize(const std::string &modelName) const {
  auto it = model_paddingSize_m.find(modelName);
  if (it != model_paddingSize_m.end()) {
    return it->second;
  }
  return 0;
}

/**
 * @brief Register ONNX models from configuration
 * @param configProvider Reference to the configuration provider
 */
void OnnxManager::registerModels(const IConfigurationProvider &configProvider) {
  const auto modelConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("onnxConfig"),
      {"file", "name", "inputVariables", "runVar"});

  loadModelsFromConfig(configProvider, modelConfig);
}

/**
 * @brief Load models from parsed configuration
 * @param configProvider Reference to the configuration provider
 * @param modelConfig Parsed configuration entries
 */
void OnnxManager::loadModelsFromConfig(
    const IConfigurationProvider &configProvider,
    const std::vector<std::unordered_map<std::string, std::string>> &modelConfig) {
  
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

    // Parse optional paddingSize
    int64_t paddingSize = 0;
    auto paddingIt = entryKeys.find("paddingSize");
    if (paddingIt != entryKeys.end()) {
      try {
        paddingSize = std::stoll(paddingIt->second);
      } catch (const std::exception &e) {
        throw std::runtime_error("OnnxManager: Invalid paddingSize value '" +
                                 paddingIt->second + "' for model '" +
                                 entryKeys.at("name") + "': " + e.what());
      }
    }
    model_paddingSize_m.emplace(entryKeys.at("name"), paddingSize);
  }
}

void OnnxManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("OnnxManager: ConfigManager not set");
  }

  const auto modelConfig = configManager_m->parseMultiKeyConfig(
    configManager_m->get("onnxConfig"),
    {"file", "name", "inputVariables", "runVar"});

  loadModelsFromConfig(*configManager_m, modelConfig);
}
