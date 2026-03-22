#ifndef BDTMANAGER_H_INCLUDED
#define BDTMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <fastforest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

class Analyzer;

/**
 * @class BDTManager
 * @brief Handles loading, storing, and applying BDTs.
 *
 * This manager encapsulates the logic for managing Boosted Decision Trees (BDTs),
 * including loading from configuration, storing, and applying them to data.
 * Implements the IBDTManager interface for dependency injection.
 */
class BDTManager
    : public NamedObjectManager<std::shared_ptr<fastforest::FastForest>> {
public:

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<BDTManager> create(
      Analyzer& an, const std::string& role = "bdtManager");

  /**
   * @brief Construct a new BDTManager object
   * @param configProvider Reference to the configuration provider
   */
  BDTManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Apply a BDT to the dataframe provider
   * @param bdtName Name of the BDT
   */
  void applyBDT(const std::string &bdtName);

  /**
   * @brief Apply all BDTs to the dataframe provider
   */
  void applyAllBDTs();

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

  /**
   * @brief Return the type of the manager
   */
  std::string type() const override { return "BDTManager"; }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs loaded BDT names.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports the list of loaded BDTs to the logger.
   */
  void reportMetadata() override;

private:
  /**
   * @brief Register BDTs from the configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerBDTs(const IConfigurationProvider &configProvider);
  /**
   * @brief Map from BDT name to run variable name.
   */
  std::unordered_map<std::string, std::string> bdt_runVars_m;
};



#endif // BDTMANAGER_H_INCLUDED 