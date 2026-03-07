#include <OnnxManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>

#include <algorithm>
#include <cctype>
#include <numeric>
#include <sstream>

namespace {
std::string trim(const std::string &value) {
  const auto begin = std::find_if_not(value.begin(), value.end(),
                                      [](unsigned char c) { return std::isspace(c); });
  if (begin == value.end()) {
    return "";
  }
  const auto end = std::find_if_not(value.rbegin(), value.rend(),
                                    [](unsigned char c) { return std::isspace(c); })
                       .base();
  return std::string(begin, end);
}

std::vector<std::string> splitString(const std::string &value, char delimiter) {
  std::vector<std::string> tokens;
  std::stringstream stream(value);
  std::string item;
  while (std::getline(stream, item, delimiter)) {
    const auto cleaned = trim(item);
    if (!cleaned.empty()) {
      tokens.push_back(cleaned);
    }
  }
  return tokens;
}

std::vector<int64_t> parseShape(const std::string &shapeString,
                                const std::string &modelName,
                                size_t inputIndex) {
  const auto dimTokens = splitString(shapeString, 'x');
  if (dimTokens.empty()) {
    throw std::runtime_error("OnnxManager: Empty input shape for model '" +
                             modelName + "' input index " +
                             std::to_string(inputIndex));
  }

  std::vector<int64_t> shape;
  shape.reserve(dimTokens.size());
  for (const auto &token : dimTokens) {
    try {
      shape.push_back(std::stoll(token));
    } catch (const std::exception &e) {
      throw std::runtime_error("OnnxManager: Invalid dimension '" + token +
                               "' in shape '" + shapeString + "' for model '" +
                               modelName + "' input index " +
                               std::to_string(inputIndex) + ": " + e.what());
    }
  }
  return shape;
}

std::vector<std::vector<int64_t>> parseInputShapesConfig(
    const std::string &inputShapesConfig,
    size_t expectedInputs,
    const std::string &modelName) {
  if (inputShapesConfig.empty()) {
    return {};
  }

  const auto shapeTokens = splitString(inputShapesConfig, ';');
  if (shapeTokens.size() != expectedInputs) {
    throw std::runtime_error("OnnxManager: inputShapes for model '" + modelName +
                             "' must provide exactly " +
                             std::to_string(expectedInputs) +
                             " shapes separated by ';' (got " +
                             std::to_string(shapeTokens.size()) + ")");
  }

  std::vector<std::vector<int64_t>> shapes;
  shapes.reserve(shapeTokens.size());
  for (size_t i = 0; i < shapeTokens.size(); ++i) {
    shapes.push_back(parseShape(shapeTokens[i], modelName, i));
  }
  return shapes;
}

std::vector<int64_t> resolveShape(std::vector<int64_t> shape,
                                  int64_t paddingSize,
                                  const std::string &modelName,
                                  size_t inputIndex) {
  if (shape.empty()) {
    throw std::runtime_error("OnnxManager: ONNX input shape is empty for model '" +
                             modelName + "' input index " +
                             std::to_string(inputIndex));
  }

  if (shape[0] <= 0) {
    shape[0] = 1;
  }

  for (size_t d = 1; d < shape.size(); ++d) {
    if (shape[d] <= 0) {
      if (paddingSize > 0) {
        shape[d] = paddingSize;
      } else {
        throw std::runtime_error(
            "OnnxManager: Dynamic ONNX dimension found for model '" + modelName +
            "' input index " + std::to_string(inputIndex) +
            " with no paddingSize/inputShapes provided.");
      }
    }
  }

  return shape;
}

int64_t elementCount(const std::vector<int64_t> &shape,
                     const std::string &modelName,
                     size_t inputIndex) {
  if (shape.empty()) {
    throw std::runtime_error("OnnxManager: Cannot compute element count for empty shape in model '" +
                             modelName + "' input index " + std::to_string(inputIndex));
  }

  return std::accumulate(shape.begin(), shape.end(), int64_t{1},
                         [&](int64_t acc, int64_t dim) {
                           if (dim <= 0) {
                             throw std::runtime_error(
                                 "OnnxManager: Non-positive resolved dimension in model '" +
                                 modelName + "' input index " +
                                 std::to_string(inputIndex));
                           }
                           return acc * dim;
                         });
}

const Ort::MemoryInfo &cpuMemoryInfo() {
  static const Ort::MemoryInfo memoryInfo =
      Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
  return memoryInfo;
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
  const auto &session = this->objects_m.at(modelName);
  const auto &inputShapes = model_inputShapes_m.at(modelName);
  const auto &inputElementCounts = model_inputElementCounts_m.at(modelName);
  const auto &inputNames = model_inputNames_m.at(modelName);
  const auto &outputNames = model_outputNames_m.at(modelName);
  const auto &inputNamePtrs = model_inputNamePtrs_m.at(modelName);
  const auto &outputNamePtrs = model_outputNamePtrs_m.at(modelName);

  // Auto-create packed model input from configured inputVariables for all models.
  dataManager_m->DefineVector("input_" + modelName, inputFeatures, "Float_t", *systematicManager_m);

  const int64_t totalExpectedElements = std::accumulate(
      inputElementCounts.begin(), inputElementCounts.end(), int64_t{0});
  const size_t numOutputs = outputNames.size();

  if (numOutputs == 1) {
    auto onnxLambda = [session, inputShapes, inputElementCounts,
                       inputNamePtrs, outputNamePtrs, totalExpectedElements](
        const ROOT::VecOps::RVec<Float_t> &inputVector,
        bool runVar) -> Float_t {
      if (!runVar) {
        return -1.0f;
      }

      if (static_cast<int64_t>(inputVector.size()) > totalExpectedElements) {
        throw std::runtime_error(
            "OnnxManager: Packed input size exceeds expected ONNX input size.");
      }

      const auto &memoryInfo = cpuMemoryInfo();
      std::vector<std::vector<float>> ownedInputs;
      ownedInputs.reserve(inputShapes.size());
      std::vector<Ort::Value> inputTensors;
      inputTensors.reserve(inputShapes.size());

      size_t cursor = 0;
      for (size_t i = 0; i < inputShapes.size(); ++i) {
        const int64_t expectedElements = inputElementCounts[i];
        const size_t available = inputVector.size() - cursor;
        const size_t toCopy =
            std::min<size_t>(available, static_cast<size_t>(expectedElements));

        auto &buffer = ownedInputs.emplace_back(
            static_cast<size_t>(expectedElements), 0.0f);
        std::copy_n(inputVector.begin() + static_cast<std::ptrdiff_t>(cursor),
                    static_cast<std::ptrdiff_t>(toCopy),
                    buffer.begin());
        cursor += toCopy;

        inputTensors.emplace_back(Ort::Value::CreateTensor<float>(
            memoryInfo, buffer.data(), buffer.size(),
            inputShapes[i].data(), inputShapes[i].size()));
      }

      auto output_tensors = session->Run(
          Ort::RunOptions{nullptr},
          inputNamePtrs.data(), inputTensors.data(), inputTensors.size(),
          outputNamePtrs.data(), outputNamePtrs.size());

      float *output_data = output_tensors[0].GetTensorMutableData<float>();
      return output_data[0];
    };

    std::string outputColName = modelName + outputSuffix;
    dataManager_m->Define(outputColName, onnxLambda, {"input_" + modelName, runVar}, *systematicManager_m);

  } else {
    auto onnxLambdaMulti = [session, inputShapes, inputElementCounts,
                            inputNamePtrs, outputNamePtrs,
                            numOutputs, totalExpectedElements](
        const ROOT::VecOps::RVec<Float_t> &inputVector,
        bool runVar) -> ROOT::VecOps::RVec<Float_t> {
      ROOT::VecOps::RVec<Float_t> outputs(numOutputs, -1.0f);

      if (!runVar) {
        return outputs;
      }

      if (static_cast<int64_t>(inputVector.size()) > totalExpectedElements) {
        throw std::runtime_error(
            "OnnxManager: Packed input size exceeds expected ONNX input size.");
      }

      const auto &memoryInfo = cpuMemoryInfo();
      std::vector<std::vector<float>> ownedInputs;
      ownedInputs.reserve(inputShapes.size());
      std::vector<Ort::Value> inputTensors;
      inputTensors.reserve(inputShapes.size());

      size_t cursor = 0;
      for (size_t i = 0; i < inputShapes.size(); ++i) {
        const int64_t expectedElements = inputElementCounts[i];
        const size_t available = inputVector.size() - cursor;
        const size_t toCopy =
            std::min<size_t>(available, static_cast<size_t>(expectedElements));

        auto &buffer = ownedInputs.emplace_back(
            static_cast<size_t>(expectedElements), 0.0f);
        std::copy_n(inputVector.begin() + static_cast<std::ptrdiff_t>(cursor),
                    static_cast<std::ptrdiff_t>(toCopy),
                    buffer.begin());
        cursor += toCopy;

        inputTensors.emplace_back(Ort::Value::CreateTensor<float>(
            memoryInfo, buffer.data(), buffer.size(),
            inputShapes[i].data(), inputShapes[i].size()));
      }

      auto output_tensors = session->Run(
          Ort::RunOptions{nullptr},
          inputNamePtrs.data(), inputTensors.data(), inputTensors.size(),
          outputNamePtrs.data(), outputNamePtrs.size());

      for (size_t i = 0; i < numOutputs; i++) {
        float *output_data = output_tensors[i].GetTensorMutableData<float>();
        outputs[i] = output_data[0];
      }

      return outputs;
    };

    std::string multiOutputColName = modelName + "_outputs" + outputSuffix;
    dataManager_m->Define(multiOutputColName, onnxLambdaMulti, {"input_" + modelName, runVar}, *systematicManager_m);

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
 * @brief Get whether an ONNX model uses the CUDA execution provider
 * @param modelName Name of the model
 * @return true if the model uses CUDA, false otherwise
 */
bool OnnxManager::getUseCuda(const std::string &modelName) const {
  auto it = model_useCuda_m.find(modelName);
  if (it != model_useCuda_m.end()) {
    return it->second;
  }
  return false;
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
    const std::string &modelName = entryKeys.at("name");

    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

    // Build per-model session options
    Ort::SessionOptions session_options;
    session_options.SetIntraOpNumThreads(1);
    session_options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_EXTENDED);

    // Parse optional useCuda flag
    bool useCuda = false;
    auto cudaIt = entryKeys.find("useCuda");
    if (cudaIt != entryKeys.end() && cudaIt->second == "true") {
      useCuda = true;
#if ONNXRUNTIME_USE_CUDA
      OrtCUDAProviderOptions cuda_options{};
      // Default device_id is 0; configure with cudaDeviceId to override
      auto deviceIt = entryKeys.find("cudaDeviceId");
      if (deviceIt != entryKeys.end()) {
        int deviceId = 0;
        try {
          deviceId = std::stoi(deviceIt->second);
        } catch (const std::exception &e) {
          throw std::runtime_error("OnnxManager: Invalid cudaDeviceId value '" +
                                   deviceIt->second + "' for model '" +
                                   entryKeys.at("name") + "': " + e.what());
        }
        if (deviceId < 0) {
          throw std::runtime_error("OnnxManager: Invalid cudaDeviceId value '" +
                                   deviceIt->second + "' for model '" +
                                   entryKeys.at("name") +
                                   "': device id must be non-negative");
        }
        cuda_options.device_id = deviceId;
      }
      session_options.AppendExecutionProvider_CUDA(cuda_options);
#else
      throw std::runtime_error(
          "OnnxManager: model '" + entryKeys.at("name") +
          "' requested useCuda=true but ONNX Runtime was not built with CUDA "
          "support. Reconfigure with -DONNXRUNTIME_USE_CUDA=ON.");
#endif
    }

    // Load the ONNX model
    int64_t paddingSize = 0;
    auto paddingIt = entryKeys.find("paddingSize");
    if (paddingIt != entryKeys.end()) {
      try {
        paddingSize = std::stoll(paddingIt->second);
      } catch (const std::exception &e) {
        throw std::runtime_error("OnnxManager: Invalid paddingSize value '" +
                                 paddingIt->second + "' for model '" +
                                 modelName + "': " + e.what());
      }
    }

    auto session = std::make_shared<Ort::Session>(*env_m, entryKeys.at("file").c_str(), session_options);

    Ort::AllocatorWithDefaultOptions allocator;

    size_t num_input_nodes = session->GetInputCount();
    std::vector<std::string> input_names;
    std::vector<std::vector<int64_t>> inferredInputShapes;
    inferredInputShapes.reserve(num_input_nodes);

    for (size_t i = 0; i < num_input_nodes; i++) {
      auto input_name = session->GetInputNameAllocated(i, allocator);
      input_names.push_back(std::string(input_name.get()));

      auto typeInfo = session->GetInputTypeInfo(i);
      auto tensorInfo = typeInfo.GetTensorTypeAndShapeInfo();
      inferredInputShapes.push_back(tensorInfo.GetShape());
    }

    if (num_input_nodes > 1 && inputVariableVector.size() != num_input_nodes) {
      throw std::runtime_error(
          "OnnxManager: Model '" + modelName + "' has " +
          std::to_string(num_input_nodes) +
          " ONNX inputs but inputVariables has " +
          std::to_string(inputVariableVector.size()) +
          ". For multi-input models provide one variable per ONNX input.");
    }

    std::vector<std::vector<int64_t>> configuredShapes;
    auto shapesIt = entryKeys.find("inputShapes");
    if (shapesIt != entryKeys.end()) {
      configuredShapes = parseInputShapesConfig(shapesIt->second, num_input_nodes, modelName);
    }

    std::vector<std::vector<int64_t>> resolvedInputShapes;
    resolvedInputShapes.reserve(num_input_nodes);
    std::vector<int64_t> inputElementCounts;
    inputElementCounts.reserve(num_input_nodes);
    for (size_t i = 0; i < num_input_nodes; ++i) {
      const auto &baseShape = configuredShapes.empty() ? inferredInputShapes[i] : configuredShapes[i];
      auto resolvedShape = resolveShape(baseShape, paddingSize, modelName, i);
      resolvedInputShapes.push_back(resolvedShape);
      inputElementCounts.push_back(elementCount(resolvedShape, modelName, i));
    }

    size_t num_output_nodes = session->GetOutputCount();
    std::vector<std::string> output_names;
    for (size_t i = 0; i < num_output_nodes; i++) {
      auto output_name = session->GetOutputNameAllocated(i, allocator);
      output_names.push_back(std::string(output_name.get()));
    }

    objects_m.emplace(modelName, session);
    features_m.emplace(modelName, inputVariableVector);
    model_runVars_m.emplace(modelName, entryKeys.at("runVar"));
    model_inputNames_m.emplace(modelName, input_names);
    model_outputNames_m.emplace(modelName, output_names);
    model_paddingSize_m.emplace(modelName, paddingSize);
    model_inputShapes_m.emplace(modelName, resolvedInputShapes);
    model_inputElementCounts_m.emplace(modelName, inputElementCounts);
    model_useCuda_m.emplace(modelName, useCuda);


    const auto &storedInputNames = model_inputNames_m.at(modelName);
    std::vector<const char *> inputNamePtrs;
    inputNamePtrs.reserve(storedInputNames.size());
    for (const auto &name : storedInputNames) {
      inputNamePtrs.push_back(name.c_str());
    }
    model_inputNamePtrs_m.emplace(modelName, std::move(inputNamePtrs));

    const auto &storedOutputNames = model_outputNames_m.at(modelName);
    std::vector<const char *> outputNamePtrs;
    outputNamePtrs.reserve(storedOutputNames.size());
    for (const auto &name : storedOutputNames) {
      outputNamePtrs.push_back(name.c_str());
    }
    model_outputNamePtrs_m.emplace(modelName, std::move(outputNamePtrs));
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
