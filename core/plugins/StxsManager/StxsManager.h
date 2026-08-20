#ifndef STXSMANAGER_H_INCLUDED
#define STXSMANAGER_H_INCLUDED

//==============================================================================
// StxsManager — STXS uncertainty derivation plugin
//
// Purpose:
//   A pluggable manager (IPluggableManager) that:
//     1. Loads STXS bin configuration and auxiliary histograms via stxs::Manager
//     2. Defines all STXS classification and weight columns on the dataframe
//        (in initialize(), before any user code runs)
//     3. Auto-books the standard STXS uncertainty histograms (in execute())
//     4. Optionally registers STXS weight variations with WeightManager
//     5. Writes histograms and provenance to the meta output (in finalize())
//
// Usage:
//   auto stxsMgr = StxsManager::create(analyzer, "stxsManager");
//
//   // The rest happens automatically in the plugin lifecycle.
//   // STXS columns are available on the dataframe after Analyzer construction.
//
// Configuration (read from global config map in setupFromConfigFile()):
//   stxsEnabled: "true|false" (default: false)
//   stxsBinConfig: "cfg/stxs_bins.txt"
//   stxsAuxDirectory: "aux/stxs"
//   stxsCoarseBranch: "HTXS_stage1_1_cat_pTjet30GeV"
//   stxsFineBranch: "HTXS_stage1_1_fine_cat_pTjet30GeV"
//   stxsAnalysisCoarseBranch: "" (optional)
//   stxsAnalysisFineBranch: "" (optional)
//   stxsDecayChannelBranch: "vhbb_channel" (optional)
//   stxsSignalProcess: "zh" (used with stxsSampleName/stxsGroupName)
//   stxsSampleName: "zh"
//   stxsGroupName: "signal"
//   stxsSignalProcessId: "4" (alternative: direct integer ID)
//   stxsProductionModeId: "2" (alternative: direct integer ID, auto-derived if absent)
//   stxsSignalProcessRules: "cfg/stxs_signal_process_rules.yaml" (optional)
//   stxsAcceptanceSpecs: "cfg/stxs_acceptance.yaml" (optional YAML)
//   stxsMigrationSpecs: "cfg/stxs_migration.yaml" (optional YAML)
//   stxsNBins: "46" (number of analysis bins for auto-booked histograms)
//   stxsHistogramLow: "0.0"
//   stxsHistogramHigh: "46.0"
//==============================================================================

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RResultPtr.hxx>
#include <TH1D.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class Analyzer;
class WeightManager;

namespace stxs {
class Manager;
struct StxsConfig;
} // namespace stxs

/**
 * @class StxsManager
 * @brief Pluggable manager for STXS uncertainty derivation.
 *
 * Automates the entire STXS uncertainty workflow: column definitions,
 * auxiliary histogram loading, weight computation, histogram booking,
 * and WeightManager integration.
 */
class StxsManager : public IPluggableManager {
public:
  // -------------------------------------------------------------------------
  // Factory
  // -------------------------------------------------------------------------
  static std::shared_ptr<StxsManager> create(
      Analyzer &an, const std::string &role = "stxsManager");

  StxsManager() = default;
  ~StxsManager() override = default;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------
  std::string type() const override { return "StxsManager"; }

  void setContext(ManagerContext &ctx) override;
  void setupFromConfigFile() override;
  void initialize() override;
  void execute() override;
  void finalize() override;
  void reportMetadata() override;

  // -------------------------------------------------------------------------
  // Dependency and resource advertisement
  // -------------------------------------------------------------------------
  std::vector<std::string> getDependencies() const override {
    // WeightManager is optional — we check for it at runtime.
    return {};
  }

  std::vector<std::string> getRequiredColumns() const override;
  std::vector<std::string> getProducedColumns() const override;

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /// Access the underlying STXS Manager.
  std::shared_ptr<stxs::Manager> getStxsManager() const { return stxsManager_m; }

  /// Access the StxsConfig used for column definitions.
  const stxs::StxsConfig &getStxsConfig() const { return *stxsConfig_m; }

  /// Whether the plugin is enabled.
  bool isEnabled() const { return enabled_m; }

private:
  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------
  void configureFromConfigMap();
  void loadAcceptanceSpecsFromYaml(const std::string &path);
  void loadMigrationSpecsFromYaml(const std::string &path);
  void loadSignalProcessRulesFromYaml(const std::string &path);
  void registerWithWeightManager();
  void bookStandardHistograms();

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------
  ManagerContext *ctx_m = nullptr;
  Analyzer *analyzer_m = nullptr; ///< Back-reference for column definitions
  bool enabled_m = false;

  // Configuration values (raw)
  std::string binConfigPath_m;
  std::string auxDirectoryPath_m;
  std::string coarseBranch_m;
  std::string fineBranch_m;
  std::string analysisCoarseBranch_m;
  std::string analysisFineBranch_m;
  std::string decayChannelBranch_m = "vhbb_channel";
  std::string signalProcessStr_m;
  std::string sampleNameStr_m;
  std::string groupNameStr_m;
  int signalProcessId_m = 0;
  int productionModeId_m = 0;
  bool hasExplicitProcessId_m = false;
  bool hasExplicitModeId_m = false;
  std::string acceptanceSpecsPath_m;
  std::string migrationSpecsPath_m;
  std::string signalProcessRulesPath_m;
  int nHistogramBins_m = 46;
  double histogramLow_m = 0.0;
  double histogramHigh_m = 46.0;

  // STXS Manager and config
  std::shared_ptr<stxs::Manager> stxsManager_m;
  std::unique_ptr<stxs::StxsConfig> stxsConfig_m;

  // Auto-booked histogram results
  struct HistEntry {
    std::string name;
    std::string variable;
    std::string weightColumn;
    ROOT::RDF::RResultPtr<TH1D> result;
  };
  std::vector<HistEntry> bookedHistograms_m;

  // Sample name for histogram naming
  std::string sampleName_m = "unknown";
};

#endif // STXSMANAGER_H_INCLUDED
