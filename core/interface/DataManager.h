#ifndef DATAMANAGER_H_INCLUDED
#define DATAMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <ROOT/RDataFrame.hxx>
#include <SystematicManager.h>
#include <TChain.h>
#include <util.h>
#include <memory>
#include <string>
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
   *
   * Reads input files and the optional ``friendConfig`` key from
   * @p configProvider. Any friend trees declared in that file are attached to
   * the main TChain *before* the RDataFrame is created so that all friend
   * branches are immediately available.
   *
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


  /**
   * @brief Define a vector variable in the dataframe. If all columns are scalars, creates a vector from them. If all columns are RVecs, concatenates and casts them to the target type. Mixed types are not supported and will throw an error at runtime. Uses a JIT lambda for type deduction and concatenation.
   * @param name Name of the variable
   * @param columns Input columns (scalars or vectors)
   * @param type Data type (default: Float_t)
   * @param systematicManager Reference to the systematic manager
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

  /**
   * @brief Define a per-sample variable in the dataframe
   * @param name Name of the variable
   * @param f Function to define the variable
   */

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
   * @brief Attach friend trees or sidecar files declared in a YAML config.
   *
   * Reads the config file referenced by @p friendConfigKey from
   * @p configProvider, parses each ``FriendTreeSpec`` entry, and attaches the
   * corresponding TChain as a named friend of the main chain.  After all
   * friends are attached the internal RDataFrame is rebuilt from the main
   * chain so that the new columns are immediately accessible.
   *
   * This method can be called before or after construction.  When called after
   * the DataManager has already been built (e.g. programmatically in analysis
   * code) it must be invoked *before* any ``Define`` / ``Filter`` operations,
   * because the RDataFrame is recreated internally.
   *
   * Supported config keys per friend entry (see FriendTreeSpec):
   *   - ``alias``         – name used to prefix friend branches (required)
   *   - ``treeName``      – TTree name inside the friend files (default: "Events")
   *   - ``fileList``      – YAML sequence of file paths / XRootD URLs
   *   - ``directory``     – directory to scan (alternative to fileList)
   *   - ``globs``         – include patterns for directory scan
   *   - ``antiglobs``     – exclude patterns for directory scan
   *   - ``indexBranches`` – branch names for index-based event matching
   *
   * @param configProvider Reference to the configuration provider.
   * @param friendConfigKey Config key that maps to the YAML file path
   *                        (default: ``"friendConfig"``).
   */
  void registerFriendTrees(const IConfigurationProvider &configProvider,
                           const std::string &friendConfigKey = "friendConfig");

  /**
   * @brief Attach a single friend tree to the main TChain.
   *
   * Creates a new TChain from the specification in @p spec, optionally builds
   * an event-matching index, and registers it as a named friend of the main
   * chain.  Ownership of the new TChain is retained internally so that the
   * main chain's pointer remains valid for the lifetime of this DataManager.
   *
   * @note After calling this method you should recreate the RDataFrame (e.g.
   *       by calling registerFriendTrees which does so automatically) before
   *       the event loop starts, otherwise the new friend branches will not be
   *       visible to the existing RDataFrame node.
   *
   * @param spec Description of the friend tree to attach.
   */
  void attachFriendTree(const FriendTreeSpec &spec);

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
  /// Owns TChain objects attached as ROOT friend trees.
  /// Must be declared before chain_vec_m so that friend chains are destroyed
  /// AFTER the main TChain (C++ destroys members in reverse declaration order).
  /// This guarantees ROOT can safely unregister friends during main-chain teardown.
  std::vector<std::unique_ptr<TChain>> friend_chains_m;
  std::vector<std::unique_ptr<TChain>> chain_vec_m;
  ROOT::RDF::RNode df_m;
};

#endif // DATAMANAGER_H_INCLUDED