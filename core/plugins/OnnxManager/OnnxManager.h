#ifndef ONNXMANAGER_H_INCLUDED
#define ONNXMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <onnxruntime_cxx_api.h>
#include <memory>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @class OnnxManager
 * @brief Handles loading, storing, and applying ONNX models.
 *
 * This manager encapsulates the logic for managing ONNX models,
 * including loading from configuration, storing, and applying them to data.
 * Similar to BDTManager but for ONNX model inference.
 */
class OnnxManager
    : public NamedObjectManager<std::shared_ptr<Ort::Session>> {
public:
  /**
   * @brief Construct a new OnnxManager object
   * @param configProvider Reference to the configuration provider
   */
  OnnxManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Apply an ONNX model to the dataframe provider
   * @param modelName Name of the ONNX model
   * @param outputSuffix Optional suffix to append to output column names (default: empty)
   * 
   * For models with multiple outputs, each output tensor creates a separate column:
   * - Single output: column named "{modelName}{outputSuffix}"
   * - Multiple outputs: columns named "{modelName}_output0{outputSuffix}", "{modelName}_output1{outputSuffix}", etc.
   *
   * If a paddingSize is configured for the model, the input vector is zero-padded
   * to that size before inference (e.g. for transformers with a fixed number of
   * attention particles).
   */
  void applyModel(const std::string &modelName, const std::string &outputSuffix = "");

  /**
   * @brief Apply all ONNX models to the dataframe provider
   * @param outputSuffix Optional suffix to append to output column names (default: empty)
   */
  void applyAllModels(const std::string &outputSuffix = "");

  /**
   * @brief Get an ONNX session object by key
   * @param key Model key
   * @return Shared pointer to the ONNX Session object
   */
  std::shared_ptr<Ort::Session> getModel(const std::string &key) const;

  /**
   * @brief Get the features for an ONNX model by key
   * @param key Model key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &getModelFeatures(const std::string &key) const;

  /**
   * @brief Get the run variable for an ONNX model
   * @param modelName Name of the model
   * @return Reference to the run variable string
   */
  const std::string &getRunVar(const std::string &modelName) const;

  /**
   * @brief Get all ONNX model names
   * @return Vector of all model names
   */
  std::vector<std::string> getAllModelNames() const;

  /**
   * @brief Get the input names for an ONNX model (from the model itself)
   * @param modelName Name of the model
   * @return Reference to the vector of input names
   */
  const std::vector<std::string> &getModelInputNames(const std::string &modelName) const;

  /**
   * @brief Get the output names for an ONNX model (from the model itself)
   * @param modelName Name of the model
   * @return Reference to the vector of output names
   */
  const std::vector<std::string> &getModelOutputNames(const std::string &modelName) const;

  /**
   * @brief Get the padding size for an ONNX model
   * @param modelName Name of the model
   * @return Padding size (0 if no padding configured)
   */
  int64_t getPaddingSize(const std::string &modelName) const;

  /**
   * @brief Get whether an ONNX model is configured to use the CUDA runtime
   * @param modelName Name of the model
   * @return true if the model uses the CUDA execution provider, false otherwise
   */
  bool getUseCuda(const std::string &modelName) const;

  /**
   * @brief Return the type of the manager
   */
  std::string type() const override { return "OnnxManager"; }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs the number of loaded ONNX models.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports loaded model names to the logger.
   */
  void reportMetadata() override;

private:
  /**
   * @brief Register ONNX models from the configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerModels(const IConfigurationProvider &configProvider);

  /**
   * @brief Load models from parsed configuration
   * @param configProvider Reference to the configuration provider
   * @param modelConfig Parsed configuration entries
   */
  void loadModelsFromConfig(
      const IConfigurationProvider &configProvider,
      const std::vector<std::unordered_map<std::string, std::string>> &modelConfig);

  /**
   * @brief Shared ONNX Runtime environment
   */
  std::shared_ptr<Ort::Env> env_m;

  /**
   * @brief Map from model name to run variable name.
   */
  std::unordered_map<std::string, std::string> model_runVars_m;

  /**
   * @brief Map from model name to input names (for ONNX runtime)
   */
  std::unordered_map<std::string, std::vector<std::string>> model_inputNames_m;

  /**
   * @brief Map from model name to output names (for ONNX runtime)
   */
  std::unordered_map<std::string, std::vector<std::string>> model_outputNames_m;

  /**
   * @brief Map from model name to padding size (0 = no padding)
   */
  std::unordered_map<std::string, int64_t> model_paddingSize_m;

  /**
   * @brief Map from model name to CUDA runtime usage flag
   */
  std::unordered_map<std::string, bool> model_useCuda_m;
  
  /**
   * @brief Map from model name to resolved ONNX input shapes.
   */
  std::unordered_map<std::string, std::vector<std::vector<int64_t>>> model_inputShapes_m;

  /**
   * @brief Map from model name to flattened element count per ONNX input tensor.
   */
  std::unordered_map<std::string, std::vector<int64_t>> model_inputElementCounts_m;

  /**
   * @brief Cached C-string pointers for ONNX input names (to avoid per-event allocation).
   */
  std::unordered_map<std::string, std::vector<const char *>> model_inputNamePtrs_m;

  /**
   * @brief Cached C-string pointers for ONNX output names (to avoid per-event allocation).
   */
  std::unordered_map<std::string, std::vector<const char *>> model_outputNamePtrs_m;
};

#endif // ONNXMANAGER_H_INCLUDED
