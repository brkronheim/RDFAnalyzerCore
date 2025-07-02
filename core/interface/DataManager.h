#ifndef DATAMANAGER_H_INCLUDED
#define DATAMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <ROOT/RDataFrame.hxx>
#include <SystematicManager.h>
#include <TChain.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief DataManager: Handles TChain creation, RDataFrame setup, and data
 * access. Suggested methods: getDataFrame(), getChains(), setupDataFrame(),
 * etc.
 * 
 * Implements IDataFrameProvider interface for better dependency injection.
 */
class DataManager : public IDataFrameProvider {
public:
  /**
   * @brief Construct a new DataManager object
   * @param configProvider Reference to the configuration provider
   */
  DataManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Construct a new DataManager object for testing with an in-memory RDataFrame
   * @param nEntries Number of entries for the in-memory RDataFrame
   */
  DataManager(size_t nEntries);

  /**
   * @brief Get the current RDataFrame node
   * @return The current RNode
   */
  ROOT::RDF::RNode getDataFrame() override;
  void setDataFrame(const ROOT::RDF::RNode &node) override;

  /**
   * @brief Get the main TChain pointer
   * @return Pointer to the main TChain
   */
  TChain *getChain() const;

  template <typename F>
  void Define(std::string name, F f,
              const std::vector<std::string> &columns, ISystematicManager &systematicManager) {
    
    std::vector<std::string> systList(systematicManager.getSystematics().begin(), systematicManager.getSystematics().end());
    if(systList.size() > 0) {
      for (const auto &syst : systList) {
        Int_t nAffected = 0;
        std::vector<std::string> newColumnsUp = {};
        std::vector<std::string> newColumnsDown = {};
        for (const auto &col : columns) {
          if(systematicManager.getSystematicsForVariable(col).find(syst) != systematicManager.getSystematicsForVariable(col).end()) {
            newColumnsUp.push_back(col + "_up");
            newColumnsDown.push_back(col + "_down");
            nAffected++;
          } else {
            newColumnsUp.push_back(col);
            newColumnsDown.push_back(col);
          }
        }
        if(nAffected > 0) {
          df_m = df_m.Define(name + "_"+syst+"Up", f, newColumnsUp);
          df_m = df_m.Define(name + "_"+syst+"Down", f, newColumnsDown);
          systematicManager.registerSystematic(syst, {name});
        } 
      }
    }
    // define the nominal column
    df_m = df_m.Define(name, f, columns);

    std::cout << "All columns:" << std::endl;
    for (const auto &col : df_m.GetColumnNames()) {
      std::cout << col << std::endl;
    }
  }

  /**
   * @brief Define a vector variable in the dataframe
   * @param name Name of the variable
   * @param columns Input columns
   * @param type Data type (default: Float_t)
   */
  void DefineVector(std::string name,
                    const std::vector<std::string> &columns,
                    std::string type,
                    ISystematicManager &systematicManager) override;

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
    std::cout << "Defining per-sample variable: " << name << std::endl;
    df_m = df_m.DefinePerSample(name, f);
  }

  /**
   * @brief Template function to define a constant value for all samples
   * @tparam T The type of the constant value
   * @param name Name of the variable
   * @param value The constant value to assign
   */
  template<typename T>
  void defineConstant(const std::string& name, const T& value) {
      df_m = df_m.DefinePerSample(
      name, [value](unsigned int, const ROOT::RDF::RSampleInfo) -> T {
        return value;
      });
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
   * @param systematicManager Pointer to the systematic manager (can be nullptr)
   * @return Vector of systematic variation names
   */
  std::vector<std::string> makeSystList(const std::string &branchName, ISystematicManager &systematicManager);

  /**
   * @brief Register constant variables from configuration
   * @param configProvider Reference to the configuration provider
   * @param configKey The key in the configuration for the constants file (default: "floatConfig")
   */
  void registerConstants(const IConfigurationProvider &configProvider, const std::string& floatConfigKey = "floatConfig", const std::string& intConfigKey = "intConfig");

  /**
   * @brief Register aliases from configuration
   * @param configProvider Reference to the configuration provider
   * @param configKey The key in the configuration for the alias file (default: "aliasConfig")
   */
  void registerAliases(const IConfigurationProvider &configProvider, const std::string& aliasConfigKey = "aliasConfig");

  /**
   * @brief Register optional branches from configuration
   * @param configProvider Reference to the configuration provider
   * @param configKey The key in the configuration for the optional branches file (default: "optionalBranchesConfig")
   */
  void registerOptionalBranches(const IConfigurationProvider &configProvider, const std::string& optionalBranchesConfigKey = "optionalBranchesConfig");

  /**
   * @brief Finalize setup after all configuration is loaded
   * @param configProvider Reference to the configuration provider
   * @param floatConfigKey The key for float constants (default: "floatConfig")
   * @param intConfigKey The key for int constants (default: "intConfig")
   * @param aliasConfigKey The key for aliases (default: "aliasConfig")
   * @param optionalBranchesConfigKey The key for optional branches (default: "optionalBranchesConfig")
   */
  void finalizeSetup(const IConfigurationProvider &configProvider,
                    const std::string& floatConfigKey = "floatConfig",
                    const std::string& intConfigKey = "intConfig",
                    const std::string& aliasConfigKey = "aliasConfig",
                    const std::string& optionalBranchesConfigKey = "optionalBranchesConfig");

  virtual ~DataManager();

private:
  std::vector<std::unique_ptr<TChain>> chain_vec_m;
  ROOT::RDF::RNode df_m;
};

#endif // DATAMANAGER_H_INCLUDED