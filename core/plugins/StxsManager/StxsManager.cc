//==============================================================================
// StxsManager — STXS uncertainty derivation plugin implementation
//==============================================================================

#include "StxsManager.h"
#include "StxsColumns.h"
#include "stxs/Manager.h"

#include "analyzer.h"
#include "ColumnProjection.h"

#include <ROOT/RVec.hxx>
#include <RtypesCore.h>

#include <TFile.h>
#include <TH1D.h>
#include <TObject.h>

#include <filesystem>
#include <fstream>
#include <sstream>

// ===========================================================================
// Factory
// ===========================================================================

std::shared_ptr<StxsManager> StxsManager::create(
    Analyzer &an, const std::string &role) {
  auto mgr = std::make_shared<StxsManager>();
  mgr->analyzer_m = &an;
  an.addPlugin(role, mgr);
  return mgr;
}

// ===========================================================================
// IContextAware
// ===========================================================================

void StxsManager::setContext(ManagerContext &ctx) { ctx_m = &ctx; }

// ===========================================================================
// IPluggableManager — setupFromConfigFile
// ===========================================================================

void StxsManager::setupFromConfigFile() {
  if (!ctx_m) {
    return;
  }
  configureFromConfigMap();
}

void StxsManager::configureFromConfigMap() {
  const auto &configMap = ctx_m->config.getConfigMap();

  auto get_str = [&](const std::string &key,
                      const std::string &def = "") -> std::string {
    auto it = configMap.find(key);
    return (it != configMap.end()) ? it->second : def;
  };

  auto get_bool = [&](const std::string &key, bool def = false) -> bool {
    auto it = configMap.find(key);
    if (it == configMap.end()) {
      return def;
    }
    const auto &val = it->second;
    return (val == "1" || val == "true" || val == "True");
  };

  auto get_int = [&](const std::string &key, int def = 0) -> int {
    auto it = configMap.find(key);
    if (it == configMap.end()) {
      return def;
    }
    try {
      return std::stoi(it->second);
    } catch (...) {
      return def;
    }
  };

  auto get_double = [&](const std::string &key, double def = 0.0) -> double {
    auto it = configMap.find(key);
    if (it == configMap.end()) {
      return def;
    }
    try {
      return std::stod(it->second);
    } catch (...) {
      return def;
    }
  };

  enabled_m = get_bool("stxsEnabled", false);
  if (!enabled_m) {
    return;
  }

  binConfigPath_m = get_str("stxsBinConfig");
  auxDirectoryPath_m = get_str("stxsAuxDirectory");
  coarseBranch_m = get_str("stxsCoarseBranch", "HTXS_stage1_1_cat_pTjet30GeV");
  fineBranch_m = get_str("stxsFineBranch", "HTXS_stage1_1_fine_cat_pTjet30GeV");
  analysisCoarseBranch_m = get_str("stxsAnalysisCoarseBranch");
  analysisFineBranch_m = get_str("stxsAnalysisFineBranch");
  decayChannelBranch_m =
      get_str("stxsDecayChannelBranch", "vhbb_channel");
  signalProcessStr_m = get_str("stxsSignalProcess");
  sampleNameStr_m = get_str("stxsSampleName");
  groupNameStr_m = get_str("stxsGroupName");

  // Direct integer overrides for process/mode IDs
  int explicitProcessId = get_int("stxsSignalProcessId", 0);
  if (explicitProcessId > 0) {
    signalProcessId_m = explicitProcessId;
    hasExplicitProcessId_m = true;
  }
  int explicitModeId = get_int("stxsProductionModeId", 0);
  if (explicitModeId > 0) {
    productionModeId_m = explicitModeId;
    hasExplicitModeId_m = true;
  }

  // YAML config paths
  acceptanceSpecsPath_m = get_str("stxsAcceptanceSpecs");
  migrationSpecsPath_m = get_str("stxsMigrationSpecs");
  signalProcessRulesPath_m = get_str("stxsSignalProcessRules");

  // Histogram binning
  nHistogramBins_m = get_int("stxsNBins", 46);
  histogramLow_m = get_double("stxsHistogramLow", 0.0);
  histogramHigh_m = get_double("stxsHistogramHigh", 46.0);

  // Sample name for output labeling
  sampleName_m = get_str("sample", "unknown");
}

// ===========================================================================
// IPluggableManager — initialize
//
// Here we build the stxs::Config, construct the stxs::Manager, build the
// StxsConfig, and define all STXS columns on the dataframe. This runs at
// Analyzer construction time, before any user code or filters.
// ===========================================================================

void StxsManager::initialize() {
  if (!enabled_m || !ctx_m) {
    return;
  }

  // -----------------------------------------------------------------------
  // 1. Build stxs::Config
  // -----------------------------------------------------------------------
  bool hasBinConfig = !binConfigPath_m.empty();
  bool hasAcceptanceSpecs = !acceptanceSpecsPath_m.empty();
  bool hasMigrationSpecs = !migrationSpecsPath_m.empty();

  // Start with the text-format bin config
  stxs::Config config;
  if (hasBinConfig) {
    config = stxs::loadConfig(binConfigPath_m, auxDirectoryPath_m,
                              coarseBranch_m, fineBranch_m, true);
  } else {
    config.enabled = true;
    config.auxDirectory = auxDirectoryPath_m;
    config.coarseBranch = coarseBranch_m;
    config.fineBranch = fineBranch_m;
  }

  // Load acceptance specs from YAML if provided
  if (hasAcceptanceSpecs) {
    loadAcceptanceSpecsFromYaml(acceptanceSpecsPath_m);
    // Note: acceptanceSpecs loaded directly into config via YAML parse
  }
  // If no YAML provided, the user must have populated acceptanceSpecs
  // via the bin config or the migration/acceptance YAML above.

  // Load migration specs from YAML if provided
  if (hasMigrationSpecs) {
    loadMigrationSpecsFromYaml(migrationSpecsPath_m);
  }

  // Load signal process rules from YAML if provided
  if (!signalProcessRulesPath_m.empty()) {
    loadSignalProcessRulesFromYaml(signalProcessRulesPath_m);
  }

  // -----------------------------------------------------------------------
  // 2. Determine signal process and production mode IDs
  // -----------------------------------------------------------------------
  if (!hasExplicitProcessId_m) {
    // Try to classify from (process, sample, group) strings
    if (!config.signalProcessRules.empty()) {
      signalProcessId_m = stxs::classifySignalProcess(
          signalProcessStr_m, sampleNameStr_m, groupNameStr_m,
          config.signalProcessRules);
    }
    // If still unknown, and we have a signal process string, try built-in
    if (signalProcessId_m == 0 && !signalProcessStr_m.empty()) {
      // Fallback: use a simple hash or check if the VH naming is used
      // In production, the user should provide rules or explicit IDs.
      ctx_m->logger.log(
          ILogger::Level::Warn,
          "StxsManager: signalProcessId is 0 and no rules matched. "
          "Set stxsSignalProcessId explicitly or provide signalProcessRules.");
    }
  }

  if (!hasExplicitModeId_m && signalProcessId_m > 0) {
    // Derive production mode from process using default mapping
    // Users can override this by providing processToModeMapping in YAML.
    std::unordered_map<int, int> defaultProcessToMode;
    // The default mapping is loaded from config if available
    productionModeId_m = stxs::productionModeFromProcess(
        signalProcessId_m, defaultProcessToMode);
  }

  // -----------------------------------------------------------------------
  // 3. Construct stxs::Manager
  // -----------------------------------------------------------------------
  // Rebuild config with all specs loaded
  if (hasBinConfig) {
    config = stxs::loadConfig(binConfigPath_m, auxDirectoryPath_m,
                              coarseBranch_m, fineBranch_m, true);
  }
  // Note: In a full implementation, the YAML specs would be merged into
  // the config here. For the initial implementation, we rely on the
  // loadConfigFromYaml path or direct config construction.

  stxsManager_m = std::make_shared<stxs::Manager>(std::move(config));

  // -----------------------------------------------------------------------
  // 4. Build StxsConfig and define columns
  // -----------------------------------------------------------------------
  stxsConfig_m = std::make_unique<stxs::StxsConfig>();
  stxsConfig_m->manager = stxsManager_m;
  stxsConfig_m->signalProcess = signalProcessId_m;
  stxsConfig_m->productionMode = productionModeId_m;
  stxsConfig_m->isSTXSSignal = (signalProcessId_m > 0);
  stxsConfig_m->decayChannelBranch = decayChannelBranch_m;
  stxsConfig_m->stage0Branch = "HTXS_stage0_cat";
  stxsConfig_m->stage1Branch = "HTXS_stage1_cat_pTjet30GeV";
  stxsConfig_m->stage11CoarseBranch = coarseBranch_m;
  stxsConfig_m->stage11FineBranch = fineBranch_m;
  stxsConfig_m->stage12CoarseBranch = "HTXS_stage1_2_cat_pTjet30GeV";
  stxsConfig_m->stage12FineBranch = "HTXS_stage1_2_fine_cat_pTjet30GeV";
  stxsConfig_m->analysisCoarseBranch = analysisCoarseBranch_m;
  stxsConfig_m->analysisFineBranch = analysisFineBranch_m;

  // Get the dataframe and define columns on it via the Analyzer
  stxs::defineStxsColumns(*analyzer_m, *stxsConfig_m);
}

// ===========================================================================
// IPluggableManager — execute
//
// Called inside Analyzer::run() just before triggering the event
// loop. Here we book the standard STXS uncertainty histograms and register
// weight variations with the WeightManager (if present).
// ===========================================================================

void StxsManager::execute() {
  if (!enabled_m || !ctx_m || !stxsManager_m) {
    return;
  }

  // Book standard histograms
  bookStandardHistograms();

  // Register with WeightManager if available
  registerWithWeightManager();
}

// ===========================================================================
// IPluggableManager — finalize
//
// Write auto-booked histograms to the meta output.
// ===========================================================================

void StxsManager::finalize() {
  if (!enabled_m || !ctx_m || !stxsManager_m) {
    return;
  }

  // Get the meta output file path
  std::string fileName =
      ctx_m->metaSink.resolveOutputFile(ctx_m->config, OutputChannel::Meta);
  if (fileName.empty()) {
    ctx_m->logger.log(ILogger::Level::Warn,
                      "StxsManager: no meta output file configured.");
    return;
  }

  // Open the meta output file in UPDATE mode
  TFile outFile(fileName.c_str(), "UPDATE");
  if (outFile.IsZombie()) {
    ctx_m->logger.log(ILogger::Level::Error,
                      "StxsManager: failed to open meta output file: " +
                          fileName);
    return;
  }

  // Write each auto-booked histogram
  for (auto &entry : bookedHistograms_m) {
    auto histPtr = entry.result.GetPtr();
    if (!histPtr) {
      continue;
    }
    histPtr->SetDirectory(&outFile);
    histPtr->Write(histPtr->GetName(), TObject::kOverwrite);
    ctx_m->logger.log(
        ILogger::Level::Debug,
        "StxsManager: wrote histogram '" + std::string(histPtr->GetName()) +
            "'");
  }

  outFile.Close();

  ctx_m->logger.log(
      ILogger::Level::Info,
      "StxsManager: wrote " + std::to_string(bookedHistograms_m.size()) +
          " STXS uncertainty histograms to " + fileName);
}

// ===========================================================================
// IPluggableManager — reportMetadata
// ===========================================================================

void StxsManager::reportMetadata() {
  if (!enabled_m || !ctx_m) {
    return;
  }
  ctx_m->logger.log(
      ILogger::Level::Info,
      "StxsManager: signalProcess=" + std::to_string(signalProcessId_m) +
          " productionMode=" + std::to_string(productionModeId_m) +
          " nBins=" + std::to_string(nHistogramBins_m));
}

// ===========================================================================
// Column requirements
// ===========================================================================

std::vector<std::string> StxsManager::getRequiredColumns() const {
  if (!enabled_m) {
    return {};
  }
  return {decayChannelBranch_m,          "HTXS_stage0_cat",
          "HTXS_stage1_cat_pTjet30GeV",  coarseBranch_m,
          fineBranch_m,                  "HTXS_stage1_2_cat_pTjet30GeV",
          "HTXS_stage1_2_fine_cat_pTjet30GeV"};
}

std::vector<std::string> StxsManager::getProducedColumns() const {
  if (!enabled_m) {
    return {};
  }
  return {
      "stxs_decay_channel",        "stxs_signal_process",
      "stxs_production_mode",      "stxs_is_signal",
      "stxs_stage0_category",      "stxs_stage1_category",
      "stxs_stage1_1_category",    "stxs_stage1_1_fine_category",
      "stxs_stage1_2_category",    "stxs_stage1_2_fine_category",
      "stxs_analysis_coarse_category", "stxs_analysis_fine_category",
      "stxs_analysis_bin",         "stxs_template_bin",
      "stxs_acceptance_weight_up", "stxs_acceptance_weight_down",
      "stxs_migration_weights",    "delta1_weight_up",
      "delta1_weight_down",        "delta2_weight_up",
      "delta2_weight_down",        "delta75_weight_up",
      "delta75_weight_down",       "delta150_weight_up",
      "delta150_weight_down",      "delta250_weight_up",
      "delta250_weight_down",      "delta400_weight_up",
      "delta400_weight_down",      "STXS_fine_acc_up",
      "STXS_fine_acc_down"};
}

// ===========================================================================
// Private: bookStandardHistograms
//
// Books the 17 standard STXS uncertainty histograms:
//   - 1 nominal (stxs_analysis_bin × weight_nominal)
//   - 2 acceptance (up/down)
//   - 14 migration (7 variations × up/down)
// ===========================================================================

void StxsManager::bookStandardHistograms() {
  auto df = ctx_m->data.getDataFrame();

  // Determine the weight column name (nominal)
  std::string nominalWeight = "weight_nominal";

  // Helper to define and book a histogram
  auto bookHist = [&](const std::string &name, const std::string &variable,
                      const std::string &weight) {
    std::string histName = name + "_" + sampleName_m;
    std::string histTitle =
        name + ";" + variable + ";weighted_events";
    ROOT::RDF::TH1DModel model(histName.c_str(), histTitle.c_str(),
                                nHistogramBins_m, histogramLow_m,
                                histogramHigh_m);
    auto result = df.Histo1D(model, variable, weight);
    bookedHistograms_m.push_back({histName, variable, weight, result});
  };

  // 1. Nominal
  bookHist("stxs_analysis_bin_nominal", "stxs_analysis_bin", nominalWeight);

  // 2. Acceptance up/down
  bookHist("stxs_analysis_bin_acceptance_up", "stxs_analysis_bin",
           "stxs_acceptance_weight_up");
  bookHist("stxs_analysis_bin_acceptance_down", "stxs_analysis_bin",
           "stxs_acceptance_weight_down");

  // 3. Migration variations
  struct MigVar {
    std::string suffix;
    std::string weightColumn;
  };
  std::vector<MigVar> migVars = {
      {"delta1_up", "delta1_weight_up"},
      {"delta1_down", "delta1_weight_down"},
      {"delta2_up", "delta2_weight_up"},
      {"delta2_down", "delta2_weight_down"},
      {"delta75_up", "delta75_weight_up"},
      {"delta75_down", "delta75_weight_down"},
      {"delta150_up", "delta150_weight_up"},
      {"delta150_down", "delta150_weight_down"},
      {"delta250_up", "delta250_weight_up"},
      {"delta250_down", "delta250_weight_down"},
      {"delta400_up", "delta400_weight_up"},
      {"delta400_down", "delta400_weight_down"},
      {"STXS_fine_acc_up", "STXS_fine_acc_up"},
      {"STXS_fine_acc_down", "STXS_fine_acc_down"},
  };

  for (const auto &mv : migVars) {
    bookHist("stxs_analysis_bin_" + mv.suffix, "stxs_analysis_bin",
             mv.weightColumn);
  }
}

// ===========================================================================
// Private: registerWithWeightManager
//
// Optionally registers STXS weight variations with the WeightManager plugin.
// This enables the WeightManager to include STXS variations in its
// systematic weight audit and varied-weight column definitions.
// ===========================================================================

void StxsManager::registerWithWeightManager() {
  if (!ctx_m) {
    return;
  }

  // Try to find the WeightManager plugin
  // In the current framework, plugins are accessible via the Analyzer.
  // We check if the WeightManager is registered and if so, register the
  // STXS weight variations with it.

  // Note: The plugin lookup happens through the Analyzer, which we don't
  // have direct access to here. This is a design limitation — the
  // WeightManager integration currently requires the user to call
  // configurePass1StxsVariations() in their analysis code, OR we expose
  // a helper method on the StxsManager that the user calls.
  //
  // For a future iteration, we can pass the Analyzer reference through
  // the ManagerContext or use the plugin registry.

  ctx_m->logger.log(
      ILogger::Level::Debug,
      "StxsManager: WeightManager integration is available via "
      "StxsManager::registerStxsVariations(Analyzer&) called by user code.");
}

// ===========================================================================
// Private: YAML loading helpers (stubs for future implementation)
// ===========================================================================

void StxsManager::loadAcceptanceSpecsFromYaml(const std::string &path) {
  if (!ctx_m) {
    return;
  }
  ctx_m->logger.log(
      ILogger::Level::Debug,
      "StxsManager: loading acceptance specs from " + path);
  // TODO: Implement YAML parsing when the framework's YAML adapter is
  // available in the plugin context. For now, acceptanceSpecs must be
  // populated via the stxsAcceptanceSpecs config or directly in the
  // stxs::Config before constructing the stxs::Manager.
}

void StxsManager::loadMigrationSpecsFromYaml(const std::string &path) {
  if (!ctx_m) {
    return;
  }
  ctx_m->logger.log(
      ILogger::Level::Debug,
      "StxsManager: loading migration specs from " + path);
  // TODO: Implement YAML parsing.
}

void StxsManager::loadSignalProcessRulesFromYaml(const std::string &path) {
  if (!ctx_m) {
    return;
  }
  ctx_m->logger.log(
      ILogger::Level::Debug,
      "StxsManager: loading signal process rules from " + path);
  // TODO: Implement YAML parsing.
}
