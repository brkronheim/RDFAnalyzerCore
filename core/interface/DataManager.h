#ifndef DATAMANAGER_H_INCLUDED
#define DATAMANAGER_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <SystematicManager.h>
#include <TChain.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class ConfigurationManager;

/**
 * @brief DataManager: Handles TChain creation, RDataFrame setup, and data
 * access. Suggested methods: getDataFrame(), getChains(), setupDataFrame(),
 * etc.
 */
class DataManager {
public:
  /**
   * @brief Construct a new DataManager object
   * @param configManager Reference to the ConfigurationManager
   */
  DataManager(const ConfigurationManager &configManager);

  /**
   * @brief Get the current RDataFrame node
   * @return The current RNode
   */
  ROOT::RDF::RNode getDataFrame();
  void setDataFrame(const ROOT::RDF::RNode &node);

  /**
   * @brief Get the main TChain pointer
   * @return Pointer to the main TChain
   */
  TChain *getChain() const;

  template <typename F>
  void Define(std::string name, F f,
              const std::vector<std::string> &columns = {}) {
    df_m = df_m.Define(name, f, columns);
  }

  /**
   * @brief Define a vector variable in the dataframe
   * @param name Name of the variable
   * @param columns Input columns
   * @param type Data type (default: Float_t)
   */
  void DefineVector(std::string name,
                    const std::vector<std::string> &columns = {},
                    std::string type = "Float_t");

  /**
   * @brief Get the systematic manager
   * @return Reference to the systematic manager
   */
  SystematicManager &getSystematicManager() { return systematicManager_m; }
  const SystematicManager &getSystematicManager() const {
    return systematicManager_m;
  }

  /**
   * @brief Filter the dataframe
   * @param f Filter function
   * @param columns Input columns
   */
  template <typename F>
  void Filter_m(F f, const std::vector<std::string> &columns = {}) {
    df_m = df_m.Filter(f, columns);
  }

  /**
   * @brief Define a per-sample variable in the dataframe
   * @param name Name of the variable
   * @param f Function to define the variable
   */
  template <typename F> void DefinePerSample_m(std::string name, F f) {
    df_m = df_m.DefinePerSample(name, f);
  }

  /**
   * @brief Redefine a variable in the dataframe
   * @param name Name of the variable
   * @param f Function to redefine the variable
   * @param columns Input columns
   */
  template <typename F>
  void Redefine(std::string name, F f,
                const std::vector<std::string> &columns = {}) {
    df_m = df_m.Redefine(name, f, columns);
  }

  /**
   * @brief Make a list of systematic variations for a branch
   * @param branchName Name of the branch
   * @return Vector of systematic variation names
   */
  std::vector<std::string> makeSystList(const std::string &branchName);

  /**
   * @brief Register constant variables from configuration
   * @param configManager Reference to the ConfigurationManager
   */
  void registerConstants(const ConfigurationManager &configManager);

  /**
   * @brief Register aliases from configuration
   * @param configManager Reference to the ConfigurationManager
   */
  void registerAliases(const ConfigurationManager &configManager);

  /**
   * @brief Register optional branches from configuration
   * @param configManager Reference to the ConfigurationManager
   */
  void registerOptionalBranches(const ConfigurationManager &configManager);

  /**
   * @brief Finalize setup after all configuration is loaded
   * @param configManager Reference to the ConfigurationManager
   */
  void finalizeSetup(const ConfigurationManager &configManager);

private:
  std::vector<std::unique_ptr<TChain>> chain_vec_m;
  ROOT::RDF::RNode df_m;
  SystematicManager systematicManager_m;
};

#endif // DATAMANAGER_H_INCLUDED