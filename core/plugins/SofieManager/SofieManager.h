#ifndef SOFIEMANAGER_H_INCLUDED
#define SOFIEMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @class SofieManager
 * @brief Handles storing and applying SOFIE models.
 *
 * This manager encapsulates the logic for managing SOFIE (System for Optimized 
 * Fast Inference code Emit) models from ROOT TMVA. Unlike ONNX or BDT managers,
 * SOFIE models are compiled into C++ code at build time and linked directly.
 * 
 * This manager stores function pointers to the generated inference functions
 * and applies them to data in the same way as BDT and ONNX managers.
 */

// Type alias for SOFIE inference function
// Takes a vector of floats (input features) and returns a vector of floats (outputs)
using SofieInferenceFunction = std::function<std::vector<float>(const std::vector<float>&)>;

class SofieManager
    : public NamedObjectManager<std::shared_ptr<SofieInferenceFunction>> {
public:
  /**
   * @brief Construct a new SofieManager object
   * @param configProvider Reference to the configuration provider
   */
  SofieManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Apply a SOFIE model to the dataframe provider
   * @param modelName Name of the SOFIE model
   */
  void applyModel(const std::string &modelName);

  /**
   * @brief Apply all SOFIE models to the dataframe provider
   */
  void applyAllModels();

  /**
   * @brief Get a SOFIE inference function by key
   * @param key Model key
   * @return Shared pointer to the inference function
   */
  std::shared_ptr<SofieInferenceFunction> getModel(const std::string &key) const;

  /**
   * @brief Get the features for a SOFIE model by key
   * @param key Model key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &getModelFeatures(const std::string &key) const;

  /**
   * @brief Get the run variable for a SOFIE model
   * @param modelName Name of the model
   * @return Reference to the run variable string
   */
  const std::string &getRunVar(const std::string &modelName) const;

  /**
   * @brief Get all SOFIE model names
   * @return Vector of all model names
   */
  std::vector<std::string> getAllModelNames() const;

  /**
   * @brief Register a SOFIE inference function
   * @param name Model name
   * @param inferenceFunc Shared pointer to the inference function
   * @param features Vector of input feature names
   * @param runVar Name of the run variable
   */
  void registerModel(const std::string &name,
                     std::shared_ptr<SofieInferenceFunction> inferenceFunc,
                     const std::vector<std::string> &features,
                     const std::string &runVar);

  /**
   * @brief Return the type of the manager
   */
  std::string type() const override { return "SofieManager"; }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs the number of loaded SOFIE models.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports loaded SOFIE model names to the logger.
   */
  void reportMetadata() override;

private:
  /**
   * @brief Register SOFIE models from the configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerModels(const IConfigurationProvider &configProvider);

  /**
   * @brief Helper function to parse and store model configuration
   * @param configProvider Reference to the configuration provider
   * @param checkExisting If true, only add models that don't already exist
   */
  void parseModelConfig(const IConfigurationProvider &configProvider, bool checkExisting);

  /**
   * @brief Map from model name to run variable name.
   */
  std::unordered_map<std::string, std::string> model_runVars_m;
};


// ---------------------------------------------------------------------------
// Helper: create, register with analyzer, and return as shared_ptr
// ---------------------------------------------------------------------------
#include <memory>
class Analyzer;
std::shared_ptr<SofieManager> makeSofieManager(
    Analyzer& an, const std::string& role = "sofieManager");

#endif // SOFIEMANAGER_H_INCLUDED
