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
  /// Supply programmatic friends in addition to those in friendConfig.
  /// Each alias must be unique across both sources.
  DataManager(const IConfigurationProvider &configProvider,
              const std::vector<FriendTreeSpec> &friends);

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
  void markInputDependenciesIncomplete(const std::string &reason) override;

  /**
   * @brief Get the main TChain reference
   * @return Reference to the main TChain
   *
   * The returned reference is valid for the lifetime of this DataManager.
   */
  TChain &getChain() const;


  /**
   * @brief Define a vector variable in the dataframe.
   *
   * If all input columns are scalars, creates a vector from them. If all input
   * columns are RVecs, concatenates and casts them to the target type. Mixed
   * scalar/RVec input is rejected. Multi-input packing queries RDF input types
   * and uses a cached typed JIT callable with one output Define and no packing
   * helper columns. Common zero-/one-input cases use native compiled callables.
   * For each registered paired systematic affecting an input, also defines
   * ``name_<systematic>Up`` and ``name_<systematic>Down`` with only the
   * affected inputs shifted, then registers ``name`` as affected by that
   * systematic so later derived definitions can propagate it. Explicit output
   * column mappings already registered for ``name`` are honored.
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
   * @brief Define a vector from already-resolved nominal or packed inputs.
   *
   * Unlike DefineVector(), this does not create additional Up/Down outputs.
   * It is intended for framework helper bundles whose input ordering already
   * encodes the variations.
   */
  void DefineVectorNominal(std::string name,
                           const std::vector<std::string> &columns,
                           std::string type,
                           ISystematicManager &systematicManager) override;

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
   * @brief Register constant variables from configuration
   * @param configProvider Reference to the configuration provider
   * @param floatConfigKey Configuration key for the floating-point constants file
   * @param intConfigKey Configuration key for the integer constants file
   */
  void registerConstants(const IConfigurationProvider &configProvider, const std::string& floatConfigKey = "floatConfig", const std::string& intConfigKey = "intConfig");

  /**
   * @brief Register aliases from configuration
   * @param configProvider Reference to the configuration provider
   * @param aliasConfigKey Configuration key for the alias file
   */
  void registerAliases(const IConfigurationProvider &configProvider, const std::string& aliasConfigKey = "aliasConfig");

  /**
   * @brief Register optional branches from configuration
   * @param configProvider Reference to the configuration provider
   * @param optionalBranchesConfigKey Configuration key for the optional branches file
   */
  void registerOptionalBranches(const IConfigurationProvider &configProvider, const std::string& optionalBranchesConfigKey = "optionalBranchesConfig");

  /// Legacy entry points retained for ABI compatibility. Friends must be supplied
  /// at construction, before ROOT creates the source and any action captures it.
  [[deprecated("Use friendConfig or DataManager(config, friends) at construction")]]
  void registerFriendTrees(const IConfigurationProvider &configProvider,
                           const std::string &friendConfigKey = "friendConfig");
  [[deprecated("Pass FriendTreeSpec values to DataManager(config, friends)")]]
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
  void attachSourceFriend(const FriendTreeSpec &spec);
  void defineVectorImpl(std::string name,
                        const std::vector<std::string> &columns,
                        std::string type,
                        ISystematicManager &systematicManager,
                        bool propagateSystematics);

  /// Owns TChain objects attached as ROOT friend trees.
  /// Must be declared before chain_vec_m so that friend chains are destroyed
  /// AFTER the main TChain (C++ destroys members in reverse declaration order).
  /// This guarantees ROOT can safely unregister friends during main-chain teardown.
  std::vector<std::unique_ptr<TChain>> friend_chains_m;
  std::vector<std::unique_ptr<TChain>> chain_vec_m;
  ROOT::RDF::RNode df_m;
};

#endif // DATAMANAGER_H_INCLUDED
