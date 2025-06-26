#ifndef BDTMANAGER_H_INCLUDED
#define BDTMANAGER_H_INCLUDED

#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <fastforest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

class ConfigurationManager;

/**
 * @brief BDTManager: Handles loading, storing, and applying BDTs.
 */
class BDTManager
    : public NamedObjectManager<std::shared_ptr<fastforest::FastForest>> {
public: // TODO: add doxygen documentation for all of these functions
  /**
   * @brief Construct a new BDTManager object
   * @param configManager Reference to the ConfigurationManager
   */
  BDTManager(const ConfigurationManager &configManager);

  /**
   * @brief Apply a BDT to the input features
   * @param bdtName Name of the BDT
   * @param inputFeatures Vector of input feature names
   * @param runVar Name of the run variable which is used to determine if the
   * BDT should be applied
   * @param defineVector Function to define the input vector
   * @param define Function to define the BDT
   */
  template <typename DefineVectorFunc, typename DefineFunc>
  void applyBDT(const std::string &bdtName,
                const std::vector<std::string> &inputFeatures,
                const std::string &runVar, DefineVectorFunc defineVector,
                DefineFunc define) {
    defineVector("input_" + bdtName, inputFeatures, "Float_t");
    auto bdt = this->objects_m.at(bdtName);
    auto bdtLambda = [bdt](ROOT::VecOps::RVec<Float_t> &inputVector,
                           bool runVar) -> Float_t {
      if (runVar) {
        return (1. / (1. + std::exp(-((*bdt.get())(inputVector.data())))));
      } else {
        return (-1);
      }
    };
    define(bdtName, bdtLambda, {"input_" + bdtName, runVar});
  }

  /**
   * @brief Get a BDT object by key
   * @param key BDT key
   * @return Shared pointer to the FastForest object
   */
  std::shared_ptr<fastforest::FastForest> getBDT(const std::string &key) const;

  /**
   * @brief Get the features for a BDT by key
   * @param key BDT key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &getBDTFeatures(const std::string &key) const;

  /**
   * @brief Get the run variable for a BDT
   * @param bdtName Name of the BDT
   * @return Reference to the run variable string
   */
  const std::string &getRunVar(const std::string &bdtName) const;

  /**
   * @brief Get all BDT names
   * @return Vector of all BDT names
   */
  std::vector<std::string> getAllBDTNames() const;

private:
  /**
   * @brief Register BDTs from the configuration
   * @param configManager Reference to the ConfigurationManager
   */
  void registerBDTs(const ConfigurationManager &configManager);
  std::unordered_map<std::string, std::string> bdt_runVars_m;
};

#endif // BDTMANAGER_H_INCLUDED