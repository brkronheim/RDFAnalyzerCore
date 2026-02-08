#ifndef ONNXMANAGER_H_INCLUDED
#define ONNXMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <onnxruntime_cxx_api.h>
#include <memory>
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
   */
  void applyModel(const std::string &modelName);

  /**
   * @brief Apply all ONNX models to the dataframe provider
   */
  void applyAllModels();

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
   * @brief Return the type of the manager
   */
  std::string type() const override { return "OnnxManager"; }

  void setupFromConfigFile() override;

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
};

#endif // ONNXMANAGER_H_INCLUDED
