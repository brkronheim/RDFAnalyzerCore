#include <OnnxManager.h>
#include <SystematicBundle.h>
#include <analyzer.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
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

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

SystematicBundleMode parseBundleMode(const std::string &rawValue,
                                     const std::string &modelName) {
  if (rawValue.empty()) {
    return SystematicBundleMode::Off;
  }

  const auto normalized = toLowerCopy(trim(rawValue));
  if (normalized == "off" || normalized == "false" || normalized == "0") {
    return SystematicBundleMode::Off;
  }
  if (normalized == "auto" || normalized == "on" || normalized == "true" ||
      normalized == "1") {
    return SystematicBundleMode::Auto;
  }
  if (normalized == "required" || normalized == "require") {
    return SystematicBundleMode::Required;
  }

  throw std::runtime_error("OnnxManager: Invalid systematicBundle value '" +
                           rawValue + "' for model '" + modelName + "'.");
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

std::vector<int64_t> resolveOutputShape(std::vector<int64_t> shape,
                                        const std::string &modelName,
                                        size_t outputIndex) {
  if (shape.empty()) {
    throw std::runtime_error("OnnxManager: ONNX output shape is empty for model '" +
                             modelName + "' output index " +
                             std::to_string(outputIndex));
  }

  for (size_t d = 0; d < shape.size(); ++d) {
    if (shape[d] <= 0) {
      shape[d] = 1;
    }
  }
  return shape;
}

int64_t outputElementCount(const std::vector<int64_t> &shape,
                           const std::string &modelName,
                           size_t outputIndex) {
  if (shape.empty()) {
    throw std::runtime_error("OnnxManager: Cannot compute element count for empty shape in model '" +
                             modelName + "' output index " + std::to_string(outputIndex));
  }

  return std::accumulate(shape.begin(), shape.end(), int64_t{1},
                         [&](int64_t acc, int64_t dim) {
                           if (dim <= 0) {
                             throw std::runtime_error(
                                 "OnnxManager: Non-positive resolved dimension in model '" +
                                 modelName + "' output index " +
                                 std::to_string(outputIndex));
                           }
                           return acc * dim;
                         });
}

int64_t trailingElementCount(const std::vector<int64_t> &shape,
                             const std::string &modelName,
                             size_t tensorIndex,
                             const std::string &tensorKind) {
  if (shape.empty()) {
    throw std::runtime_error("OnnxManager: Cannot compute trailing element count for empty " +
                             tensorKind + " shape in model '" + modelName +
                             "' tensor index " + std::to_string(tensorIndex));
  }

  if (shape.size() == 1) {
    return 1;
  }

  return std::accumulate(shape.begin() + 1, shape.end(), int64_t{1},
                         [&](int64_t acc, int64_t dim) {
                           if (dim <= 0) {
                             throw std::runtime_error(
                                 "OnnxManager: Non-positive trailing dimension in " +
                                 tensorKind + " shape for model '" + modelName +
                                 "' tensor index " + std::to_string(tensorIndex));
                           }
                           return acc * dim;
                         });
}

std::vector<int64_t> resolveRuntimeInputShape(std::vector<int64_t> shape,
                                              int64_t activeBatchSize,
                                              int64_t paddingSize,
                                              const std::string &modelName,
                                              size_t inputIndex) {
  if (shape.empty()) {
    throw std::runtime_error("OnnxManager: ONNX input shape is empty for model '" +
                             modelName + "' input index " +
                             std::to_string(inputIndex));
  }

  if (shape[0] <= 0) {
    shape[0] = std::max<int64_t>(activeBatchSize, int64_t{1});
  } else if (activeBatchSize > shape[0]) {
    throw std::runtime_error("OnnxManager: Active systematic batch size " +
                             std::to_string(activeBatchSize) +
                             " exceeds fixed ONNX batch dimension " +
                             std::to_string(shape[0]) + " for model '" +
                             modelName + "' input index " +
                             std::to_string(inputIndex));
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

bool supportsScalarPerVariationOutputs(
    const std::vector<std::vector<int64_t>> &outputShapes,
    const std::string &modelName) {
  for (size_t i = 0; i < outputShapes.size(); ++i) {
    const auto &shape = outputShapes[i];
    if (shape.empty()) {
      continue;
    }
    const auto rowWidth = trailingElementCount(shape, modelName, i, "output");
    if (rowWidth != 1) {
      return false;
    }
  }
  return true;
}

bool supportsBundledInputLayout(IDataFrameProvider &dataManager,
                                const std::vector<std::string> &columns) {
  if (columns.empty()) {
    return true;
  }

  auto df = dataManager.getDataFrame();
  size_t vectorColumns = 0;
  for (const auto &column : columns) {
    if (df.GetColumnType(column).find("RVec") != std::string::npos) {
      ++vectorColumns;
    }
  }

  if (vectorColumns == 0) {
    return true;
  }

  // The bundled path supports exactly one vector-valued ONNX input. Multiple
  // vector-valued inputs still need a dedicated packer because the current
  // SystematicBundle helper rejects mixed vector/scalar multi-input layouts.
  return columns.size() == 1 && vectorColumns == 1;
}

const Ort::MemoryInfo &cpuMemoryInfo() {
  static const Ort::MemoryInfo memoryInfo =
      Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
  return memoryInfo;
}

struct OnnxScratchBuffers {
  std::vector<std::vector<float>> ownedInputs;
  std::vector<Ort::Value> inputTensors;
  std::vector<float> outputs;
  std::vector<std::vector<float>> outputVectors;
};

const std::vector<float> &runBundledModelOutputs(
    Ort::Session &session,
    const std::vector<std::vector<int64_t>> &inputShapes,
    const std::vector<int64_t> &inputRowElementCounts,
    int64_t paddingSize,
    const std::vector<const char *> &inputNamePtrs,
    const std::vector<const char *> &outputNamePtrs,
    const ROOT::VecOps::RVec<Float_t> &inputVector,
    const ROOT::VecOps::RVec<bool> &activeMask,
    float disabledValue,
    const std::string &modelName) {
  const size_t nVariations = activeMask.size();
  const size_t nOutputs = outputNamePtrs.size();
  const int64_t perVariationInputElements = std::accumulate(
      inputRowElementCounts.begin(), inputRowElementCounts.end(), int64_t{0});

  thread_local OnnxScratchBuffers scratch;
  scratch.outputs.assign(nOutputs * nVariations, disabledValue);

  if (nVariations == 0) {
    return scratch.outputs;
  }

  const int64_t expectedInputSize = perVariationInputElements *
                                    static_cast<int64_t>(nVariations);
  if (static_cast<int64_t>(inputVector.size()) != expectedInputSize) {
    throw std::runtime_error(
        "OnnxManager: Systematic bundle input size does not match the expected variation-major layout for model '" +
        modelName + "'. Got " + std::to_string(inputVector.size()) +
        " elements, expected " + std::to_string(expectedInputSize) + ".");
  }

  std::vector<size_t> activeIndices;
  activeIndices.reserve(nVariations);
  for (size_t i = 0; i < nVariations; ++i) {
    if (activeMask[i]) {
      activeIndices.push_back(i);
    }
  }

  if (activeIndices.empty()) {
    return scratch.outputs;
  }

  const auto &memoryInfo = cpuMemoryInfo();
  if (scratch.ownedInputs.size() < inputShapes.size()) {
    scratch.ownedInputs.resize(inputShapes.size());
  }
  scratch.inputTensors.clear();
  scratch.inputTensors.reserve(inputShapes.size());

  std::vector<int64_t> perInputOffsets(inputRowElementCounts.size(), 0);
  int64_t runningOffset = 0;
  for (size_t i = 0; i < inputRowElementCounts.size(); ++i) {
    perInputOffsets[i] = runningOffset;
    runningOffset += inputRowElementCounts[i];
  }

  const int64_t activeBatchSize = static_cast<int64_t>(activeIndices.size());
  for (size_t i = 0; i < inputShapes.size(); ++i) {
    const auto runtimeShape = resolveRuntimeInputShape(
        inputShapes[i], activeBatchSize, paddingSize, modelName, i);
    const int64_t expectedElements = elementCount(runtimeShape, modelName, i);
    const int64_t rowElements = inputRowElementCounts[i];

    auto &buffer = scratch.ownedInputs[i];
    buffer.assign(static_cast<size_t>(expectedElements), 0.0f);

    for (size_t row = 0; row < activeIndices.size(); ++row) {
      const size_t variationIndex = activeIndices[row];
      const int64_t sourceOffset =
          static_cast<int64_t>(variationIndex) * perVariationInputElements +
          perInputOffsets[i];
      const int64_t destinationOffset =
          static_cast<int64_t>(row) * rowElements;
      std::copy_n(inputVector.begin() + sourceOffset, rowElements,
                  buffer.begin() + destinationOffset);
    }

    scratch.inputTensors.emplace_back(Ort::Value::CreateTensor<float>(
        memoryInfo, buffer.data(), buffer.size(), runtimeShape.data(),
        runtimeShape.size()));
  }

  auto outputTensors = session.Run(
      Ort::RunOptions{nullptr}, inputNamePtrs.data(), scratch.inputTensors.data(),
      scratch.inputTensors.size(), outputNamePtrs.data(), outputNamePtrs.size());

  for (size_t outputIndex = 0; outputIndex < outputTensors.size(); ++outputIndex) {
    auto tensorInfo = outputTensors[outputIndex].GetTensorTypeAndShapeInfo();
    const auto runtimeShape = tensorInfo.GetShape();
    const int64_t totalElements = tensorInfo.GetElementCount();
    const float *outputData = outputTensors[outputIndex].GetTensorData<float>();

    int64_t runtimeBatchSize = 1;
    int64_t rowElements = 1;
    if (!runtimeShape.empty()) {
      runtimeBatchSize = runtimeShape[0];
      if (runtimeBatchSize <= 0) {
        throw std::runtime_error("OnnxManager: Invalid runtime output batch dimension for model '" +
                                 modelName + "' output index " +
                                 std::to_string(outputIndex));
      }
      if (runtimeBatchSize < activeBatchSize) {
        throw std::runtime_error("OnnxManager: Runtime output batch dimension " +
                                 std::to_string(runtimeBatchSize) +
                                 " is smaller than the active systematic batch size " +
                                 std::to_string(activeBatchSize) + " for model '" +
                                 modelName + "' output index " +
                                 std::to_string(outputIndex));
      }
      if (totalElements % runtimeBatchSize != 0) {
        throw std::runtime_error("OnnxManager: Runtime output shape for model '" +
                                 modelName + "' output index " +
                                 std::to_string(outputIndex) +
                                 " is not divisible by its batch dimension.");
      }
      rowElements = totalElements / runtimeBatchSize;
    }

    if (rowElements != 1) {
      throw std::runtime_error("OnnxManager: Bundled inference currently supports only scalar-per-variation outputs; model '" +
                               modelName + "' output index " +
                               std::to_string(outputIndex) + " has row width " +
                               std::to_string(rowElements) + ".");
    }

    for (size_t row = 0; row < activeIndices.size(); ++row) {
      const size_t variationIndex = activeIndices[row];
      scratch.outputs[outputIndex * nVariations + variationIndex] =
          outputData[row * rowElements];
    }
  }

  return scratch.outputs;
}

const std::vector<std::vector<float>> &runModelOutputs(
    Ort::Session &session,
    const std::vector<std::vector<int64_t>> &inputShapes,
    const std::vector<int64_t> &inputElementCounts,
    const std::vector<int64_t> &outputElementCounts,
    const std::vector<const char *> &inputNamePtrs,
    const std::vector<const char *> &outputNamePtrs,
    const ROOT::VecOps::RVec<Float_t> &inputVector,
    int64_t totalExpectedElements) {
  if (static_cast<int64_t>(inputVector.size()) > totalExpectedElements) {
    throw std::runtime_error(
        "OnnxManager: Packed input size exceeds expected ONNX input size.");
  }

  const auto &memoryInfo = cpuMemoryInfo();
  thread_local OnnxScratchBuffers scratch;
  if (scratch.ownedInputs.size() < inputShapes.size()) {
    scratch.ownedInputs.resize(inputShapes.size());
  }
  scratch.inputTensors.clear();
  scratch.inputTensors.reserve(inputShapes.size());

  size_t cursor = 0;
  for (size_t i = 0; i < inputShapes.size(); ++i) {
    const int64_t expectedElements = inputElementCounts[i];
    const size_t available = inputVector.size() - cursor;

    // Zero-copy path: create Ort tensor directly from the RVec data buffer
    // when the input is fully available (no padding needed).
    // ONNX Runtime does not modify input tensors during forward inference,
    // so const_cast is safe here.
    if (static_cast<int64_t>(available) >= expectedElements) {
      scratch.inputTensors.emplace_back(Ort::Value::CreateTensor<float>(
          memoryInfo,
          const_cast<float *>(inputVector.data() +
                              static_cast<std::ptrdiff_t>(cursor)),
          static_cast<size_t>(expectedElements), inputShapes[i].data(),
          inputShapes[i].size()));
      cursor += static_cast<size_t>(expectedElements);
    } else {
      // Fallback: copy available data and zero-fill the remainder.
      auto &buffer = scratch.ownedInputs[i];
      if (buffer.size() != static_cast<size_t>(expectedElements)) {
        buffer.resize(static_cast<size_t>(expectedElements));
      }
      const size_t toCopy = available;
      if (toCopy > 0) {
        std::copy_n(inputVector.begin() + static_cast<std::ptrdiff_t>(cursor),
                    static_cast<std::ptrdiff_t>(toCopy), buffer.begin());
      }
      if (toCopy < static_cast<size_t>(expectedElements)) {
        std::fill(buffer.begin() + static_cast<std::ptrdiff_t>(toCopy),
                  buffer.end(), 0.0f);
      }
      cursor += toCopy;
      scratch.inputTensors.emplace_back(Ort::Value::CreateTensor<float>(
          memoryInfo, buffer.data(), buffer.size(), inputShapes[i].data(),
          inputShapes[i].size()));
    }
  }

  auto outputTensors = session.Run(
      Ort::RunOptions{nullptr}, inputNamePtrs.data(), scratch.inputTensors.data(),
      scratch.inputTensors.size(), outputNamePtrs.data(), outputNamePtrs.size());

  scratch.outputVectors.resize(outputTensors.size());
  for (size_t i = 0; i < outputTensors.size(); ++i) {
    auto tensorInfo = outputTensors[i].GetTensorTypeAndShapeInfo();
    const int64_t runtimeElements = tensorInfo.GetElementCount();
    const int64_t expectedElements =
        (i < outputElementCounts.size()) ? outputElementCounts[i] : runtimeElements;
    if (runtimeElements < expectedElements) {
      throw std::runtime_error(
          "OnnxManager: Runtime output tensor has fewer elements than expected.");
    }
    const float *outputData = outputTensors[i].GetTensorData<float>();
    auto &output = scratch.outputVectors[i];
    if (output.capacity() < static_cast<size_t>(expectedElements)) {
      output.reserve(static_cast<size_t>(expectedElements));
    }
    output.assign(outputData, outputData + expectedElements);
  }

  return scratch.outputVectors;
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
  const auto &outputElementCounts = model_outputElementCounts_m.at(modelName);
  const auto &inputNames = model_inputNames_m.at(modelName);
  const auto &outputNames = model_outputNames_m.at(modelName);
  const auto &outputShapes = model_outputShapes_m.at(modelName);
  const auto &inputNamePtrs = model_inputNamePtrs_m.at(modelName);
  const auto &outputNamePtrs = model_outputNamePtrs_m.at(modelName);
  const int64_t paddingSize = model_paddingSize_m.at(modelName);

  const auto bundleModeIt = model_bundleModes_m.find(modelName);
  const SystematicBundleMode bundleMode =
      (bundleModeIt != model_bundleModes_m.end())
          ? bundleModeIt->second
          : SystematicBundleMode::Off;
  const bool bundleRequested = bundleMode != SystematicBundleMode::Off;

  if (bundleRequested) {
    const bool scalarInputs = supportsBundledInputLayout(*dataManager_m, inputFeatures);
    const bool scalarOutputs = supportsScalarPerVariationOutputs(outputShapes, modelName);
    const bool bundleSupported = scalarInputs && scalarOutputs;

    if (!bundleSupported) {
      if (bundleMode == SystematicBundleMode::Required) {
        throw std::runtime_error(
            "OnnxManager: systematicBundle is required for model '" + modelName +
            "' but the model uses unsupported input/output shapes for the bundled path.");
      }
    } else {
      const auto variationLabels = resolveVariationLabels(
          *systematicManager_m, *dataManager_m,
          ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
      const size_t nVariations = variationLabels.size();
      const auto &selectionMaskColumn = model_selectionMaskColumns_m.at(modelName);
      const bool hasInputSystematics = std::any_of(
          inputFeatures.begin(), inputFeatures.end(),
          [&](const std::string &feature) {
            return hasUsableSystematicColumns(*dataManager_m, *systematicManager_m,
                                              feature, variationLabels);
          });
      const bool hasRunVarSystematics = hasUsableSystematicColumns(
          *dataManager_m, *systematicManager_m, runVar, variationLabels);
      const bool hasSelectionMaskSystematics =
          !selectionMaskColumn.empty() &&
          hasUsableSystematicColumns(*dataManager_m, *systematicManager_m,
                                     selectionMaskColumn, variationLabels);

      const std::string bundleTag = modelName + outputSuffix;
      const std::string inputBundleName =
          makeBundleColumnName(bundleTag, "input");
      const std::string runMaskBundleName =
          makeBundleColumnName(bundleTag, "run_mask");
      const std::string selectionMaskBundleName =
          makeBundleColumnName(bundleTag, "selection_mask");
      const std::string resultBundleName =
          makeBundleColumnName(bundleTag, "result");

      definePackedInputBundle(*dataManager_m, *systematicManager_m, inputFeatures,
                              variationLabels, inputBundleName,
                              hasInputSystematics);
      defineSelectionMaskBundle(*dataManager_m, *systematicManager_m, runVar,
                                variationLabels, runMaskBundleName,
                                hasRunVarSystematics);
      if (!selectionMaskColumn.empty()) {
        defineSelectionMaskBundle(*dataManager_m, *systematicManager_m,
                                  selectionMaskColumn, variationLabels,
                                  selectionMaskBundleName,
                                  hasSelectionMaskSystematics);
      }

      auto df = dataManager_m->getDataFrame();
      if (!selectionMaskColumn.empty()) {
        auto bundledLambda = [session, inputShapes, inputRowElementCounts = model_inputRowElementCounts_m.at(modelName),
                              paddingSize, inputNamePtrs, outputNamePtrs,
                              modelName, numOutputs = outputNames.size(),
                              nVariations](const ROOT::VecOps::RVec<Float_t> &inputVector,
                                           const ROOT::VecOps::RVec<bool> &runMask,
                                           const ROOT::VecOps::RVec<bool> &selectionMask)
            -> ROOT::VecOps::RVec<Float_t> {
          ROOT::VecOps::RVec<bool> activeMask(nVariations, false);
          for (size_t i = 0; i < nVariations; ++i) {
            const bool runEnabled = (i < runMask.size()) ? runMask[i] : false;
            const bool selectionEnabled =
                (i < selectionMask.size()) ? selectionMask[i] : false;
            activeMask[i] = runEnabled && selectionEnabled;
          }
          const auto &outputs = runBundledModelOutputs(
              *session, inputShapes, inputRowElementCounts, paddingSize,
              inputNamePtrs, outputNamePtrs, inputVector, activeMask, -1.0f,
              modelName);
          return ROOT::VecOps::RVec<Float_t>(outputs.begin(), outputs.end());
        };
        df = df.Define(resultBundleName, bundledLambda,
                       {inputBundleName, runMaskBundleName,
                        selectionMaskBundleName});
      } else {
        auto bundledLambda = [session, inputShapes, inputRowElementCounts = model_inputRowElementCounts_m.at(modelName),
                              paddingSize, inputNamePtrs, outputNamePtrs,
                              modelName, numOutputs = outputNames.size(),
                              nVariations](const ROOT::VecOps::RVec<Float_t> &inputVector,
                                           const ROOT::VecOps::RVec<bool> &runMask)
            -> ROOT::VecOps::RVec<Float_t> {
          ROOT::VecOps::RVec<bool> activeMask(nVariations, false);
          for (size_t i = 0; i < nVariations; ++i) {
            activeMask[i] = (i < runMask.size()) ? runMask[i] : false;
          }
          const auto &outputs = runBundledModelOutputs(
              *session, inputShapes, inputRowElementCounts, paddingSize,
              inputNamePtrs, outputNamePtrs, inputVector, activeMask, -1.0f,
              modelName);
          return ROOT::VecOps::RVec<Float_t>(outputs.begin(), outputs.end());
        };
        df = df.Define(resultBundleName, bundledLambda,
                       {inputBundleName, runMaskBundleName});
      }
      dataManager_m->setDataFrame(df);

      if (outputNames.size() == 1) {
        fanOutScalarResultBundle(*dataManager_m, *systematicManager_m,
                                 modelName + outputSuffix, variationLabels,
                                 resultBundleName, true);
      } else {
        std::vector<std::string> outputBaseNames;
        outputBaseNames.reserve(outputNames.size());
        for (size_t i = 0; i < outputNames.size(); ++i) {
          outputBaseNames.push_back(modelName + "_output" +
                                    std::to_string(i) + outputSuffix);
        }
        fanOutMultiOutputResultBundle(*dataManager_m, *systematicManager_m,
                                      outputBaseNames, variationLabels,
                                      resultBundleName, true);
      }
      return;
    }
  }

  // Build a single-pass packed input column using a JIT expression,
  // avoiding the O(N×total) sequential-append chain in DefineVector’s
  // compiled RVec path (each intermediate Define copies all prior data).
  {
    auto df = dataManager_m->getDataFrame();
    const auto existingColumns = df.GetColumnNames();
    const std::string packedColName = "input_" + modelName;
    if (std::find(existingColumns.begin(), existingColumns.end(),
                  packedColName) == existingColumns.end()) {
      std::string expr = "ROOT::VecOps::RVec<Float_t> __v; __v.reserve(";
      for (size_t j = 0; j < inputFeatures.size(); ++j) {
        if (j > 0) expr += " + ";
        expr += inputFeatures[j] + ".size()";
      }
      expr += ");\n";
      for (const auto &f : inputFeatures) {
        expr += "for (auto &__x : " + f +
                ") __v.push_back(static_cast<Float_t>(__x));\n";
      }
      expr += "return __v;";
      df = df.Define(packedColName, expr);
      dataManager_m->setDataFrame(df);
    }
  }

  const int64_t totalExpectedElements = std::accumulate(
      inputElementCounts.begin(), inputElementCounts.end(), int64_t{0});
  const size_t numOutputs = outputNames.size();

  if (numOutputs == 1 && outputElementCounts[0] == 1) {
    auto onnxLambda = [session, inputShapes, inputElementCounts,
                       outputElementCounts, inputNamePtrs, outputNamePtrs,
                       totalExpectedElements](
        const ROOT::VecOps::RVec<Float_t> &inputVector,
        bool runVar) -> Float_t {
      if (!runVar) {
        return -1.0f;
      }
      const auto &outputs =
          runModelOutputs(*session, inputShapes, inputElementCounts,
                          outputElementCounts, inputNamePtrs, outputNamePtrs,
                          inputVector, totalExpectedElements);
      if (outputs.empty() || outputs[0].empty()) {
        return -1.0f;
      }
      return outputs[0][0];
    };

    std::string outputColName = modelName + outputSuffix;
    dataManager_m->Define(outputColName, onnxLambda, {"input_" + modelName, runVar}, *systematicManager_m);

  } else {
    auto onnxLambdaMulti = [session, inputShapes, inputElementCounts,
                            outputElementCounts, inputNamePtrs, outputNamePtrs,
                            numOutputs, totalExpectedElements](
        const ROOT::VecOps::RVec<Float_t> &inputVector,
        bool runVar) -> std::vector<ROOT::VecOps::RVec<Float_t>> {
      std::vector<ROOT::VecOps::RVec<Float_t>> outputs(numOutputs);

      if (!runVar) {
        for (size_t i = 0; i < outputs.size(); ++i) {
          const auto width = (i < outputElementCounts.size() && outputElementCounts[i] > 0)
                                 ? static_cast<size_t>(outputElementCounts[i])
                                 : size_t{1};
          outputs[i] = ROOT::VecOps::RVec<Float_t>(width, -1.0f);
        }
        return outputs;
      }
      const auto &modelOutputs =
          runModelOutputs(*session, inputShapes, inputElementCounts,
                          outputElementCounts, inputNamePtrs, outputNamePtrs,
                          inputVector, totalExpectedElements);
      for (size_t i = 0; i < numOutputs; i++) {
        if (i >= modelOutputs.size()) {
          outputs[i] = ROOT::VecOps::RVec<Float_t>(1, -1.0f);
          continue;
        }
        outputs[i] = ROOT::VecOps::RVec<Float_t>(modelOutputs[i].begin(),
                                                 modelOutputs[i].end());
      }

      return outputs;
    };

    std::string multiOutputColName = modelName + "_outputs" + outputSuffix;
    dataManager_m->Define(multiOutputColName, onnxLambdaMulti, {"input_" + modelName, runVar}, *systematicManager_m);

    for (size_t i = 0; i < numOutputs; i++) {
      const bool scalarOutput = outputElementCounts[i] == 1;
      const bool singleVectorOutput = numOutputs == 1 && !scalarOutput;
      const std::string outputColName = singleVectorOutput
          ? modelName + outputSuffix
          : modelName + "_output" + std::to_string(i) + outputSuffix;

      if (scalarOutput) {
        auto indexLambda = [i](const std::vector<ROOT::VecOps::RVec<Float_t>> &outputs) -> Float_t {
          if (i >= outputs.size() || outputs[i].empty()) {
            return -1.0f;
          }
          return outputs[i][0];
        };
        dataManager_m->Define(outputColName, indexLambda, {multiOutputColName}, *systematicManager_m);
      } else {
        auto vectorLambda = [i](const std::vector<ROOT::VecOps::RVec<Float_t>> &outputs)
            -> ROOT::VecOps::RVec<Float_t> {
          if (i >= outputs.size()) {
            return ROOT::VecOps::RVec<Float_t>{-1.0f};
          }
          return outputs[i];
        };
        dataManager_m->Define(outputColName, vectorLambda, {multiOutputColName}, *systematicManager_m);
      }
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

Float_t OnnxManager::runScalarModel(
    const std::string &modelName,
    const ROOT::VecOps::RVec<Float_t> &inputVector) const {
  const auto &session = this->objects_m.at(modelName);
  const auto &inputShapes = model_inputShapes_m.at(modelName);
  const auto &inputElementCounts = model_inputElementCounts_m.at(modelName);
  const auto &outputElementCounts = model_outputElementCounts_m.at(modelName);
  const auto &inputNamePtrs = model_inputNamePtrs_m.at(modelName);
  const auto &outputNamePtrs = model_outputNamePtrs_m.at(modelName);

  if (outputNamePtrs.size() != 1) {
    throw std::runtime_error("OnnxManager: Model '" + modelName +
                             "' does not have exactly one output.");
  }

  const int64_t totalExpectedElements = std::accumulate(
      inputElementCounts.begin(), inputElementCounts.end(), int64_t{0});
  const auto &outputs =
      runModelOutputs(*session, inputShapes, inputElementCounts,
                      outputElementCounts, inputNamePtrs, outputNamePtrs,
                      inputVector, totalExpectedElements);
  if (outputs.empty() || outputs[0].empty()) {
    return -1.0f;
  }
  return outputs[0][0];
}

void OnnxManager::setModelFeatures(const std::string &modelName,
                                   const std::vector<std::string> &features) {
  if (objects_m.find(modelName) == objects_m.end()) {
    throw std::runtime_error("OnnxManager: Model not found: " + modelName);
  }
  features_m[modelName] = features;
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

const std::vector<std::vector<int64_t>> &OnnxManager::getModelOutputShapes(const std::string &modelName) const {
  auto it = model_outputShapes_m.find(modelName);
  if (it != model_outputShapes_m.end()) {
    return it->second;
  }
  throw std::runtime_error("Output shapes not found for ONNX model: " + modelName);
}

const std::vector<int64_t> &OnnxManager::getModelOutputElementCounts(const std::string &modelName) const {
  auto it = model_outputElementCounts_m.find(modelName);
  if (it != model_outputElementCounts_m.end()) {
    return it->second;
  }
  throw std::runtime_error("Output element counts not found for ONNX model: " + modelName);
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

  for (const auto &entryKeys : modelConfig) {
    const std::string &modelName = entryKeys.at("name");

    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

    // Build per-model session options
    Ort::SessionOptions session_options;
    session_options.SetIntraOpNumThreads(1);
    session_options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_EXTENDED);
    session_options.EnableCpuMemArena();

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
    std::vector<std::vector<int64_t>> resolvedOutputShapes;
    std::vector<int64_t> outputElementCounts;
    resolvedOutputShapes.reserve(num_output_nodes);
    outputElementCounts.reserve(num_output_nodes);
    for (size_t i = 0; i < num_output_nodes; i++) {
      auto output_name = session->GetOutputNameAllocated(i, allocator);
      output_names.push_back(std::string(output_name.get()));

      auto typeInfo = session->GetOutputTypeInfo(i);
      auto tensorInfo = typeInfo.GetTensorTypeAndShapeInfo();
      auto resolvedShape = resolveOutputShape(tensorInfo.GetShape(), modelName, i);
      resolvedOutputShapes.push_back(resolvedShape);
      outputElementCounts.push_back(outputElementCount(resolvedShape, modelName, i));
    }

    auto bundleModeIt = entryKeys.find("systematicBundle");
    const auto bundleMode = parseBundleMode(
        (bundleModeIt != entryKeys.end()) ? bundleModeIt->second : "",
        modelName);
    auto selectionMaskIt = entryKeys.find("selectionMaskColumn");
    const std::string selectionMaskColumn =
        (selectionMaskIt != entryKeys.end()) ? selectionMaskIt->second : "";

    objects_m.emplace(modelName, session);
    features_m.emplace(modelName, inputVariableVector);
    model_runVars_m.emplace(modelName, entryKeys.at("runVar"));
    model_inputNames_m.emplace(modelName, input_names);
    model_outputNames_m.emplace(modelName, output_names);
    model_outputShapes_m.emplace(modelName, resolvedOutputShapes);
    model_paddingSize_m.emplace(modelName, paddingSize);
    model_inputShapes_m.emplace(modelName, resolvedInputShapes);
    model_inputElementCounts_m.emplace(modelName, inputElementCounts);
    model_outputElementCounts_m.emplace(modelName, outputElementCounts);
    std::vector<int64_t> inputRowElementCounts;
    inputRowElementCounts.reserve(resolvedInputShapes.size());
    for (size_t i = 0; i < resolvedInputShapes.size(); ++i) {
      inputRowElementCounts.push_back(
          trailingElementCount(resolvedInputShapes[i], modelName, i, "input"));
    }
    model_inputRowElementCounts_m.emplace(modelName, inputRowElementCounts);
    model_useCuda_m.emplace(modelName, useCuda);
    model_selectionMaskColumns_m.emplace(modelName, selectionMaskColumn);
    model_bundleModes_m.emplace(modelName, bundleMode);


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

void OnnxManager::initialize() {
  std::cout << "OnnxManager: initialized with " << model_runVars_m.size()
            << " ONNX model(s)." << std::endl;
}

void OnnxManager::reportMetadata() {
  if (!logger_m) return;
  std::string msg = "OnnxManager loaded models: ";
  bool first = true;
  for (const auto& name : getAllModelNames()) {
    if (!first) msg += ", ";
    msg += name;
    first = false;
  }
  logger_m->log(ILogger::Level::Info, msg);
}

std::shared_ptr<OnnxManager> OnnxManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<OnnxManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}
