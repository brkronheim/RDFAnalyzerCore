#ifndef STXS_MANAGER_H_INCLUDED
#define STXS_MANAGER_H_INCLUDED

//==============================================================================
// stxs::Manager — Generic STXS (Simplified Template Cross Section) Manager
//
// Purpose:
//   Provides a data-driven manager for STXS classification and acceptance/
//   migration uncertainty weights. Fully configurable: signal processes,
//   production modes, decay channels, bin definitions, and auxiliary
//   histograms are all loaded from configuration files — no hardcoded
//   VH-specific logic.
//
// Usage:
//   1. Load config via stxs::loadConfig() or stxs::loadConfigFromYaml()
//   2. Construct stxs::Manager(config)
//   3. Use analysisBin(), acceptanceWeights(), migrationWeights()
//
// Configuration files:
//   - Bin config (TXT): defines analysis bins and fine-category mappings
//   - Acceptance spec (YAML): maps (process, channel) pairs to ROOT histograms
//   - Migration spec (YAML): maps production modes to ROOT histograms
//   - Signal process rules (YAML): string-matching rules for classifySignalProcess()
//==============================================================================

#include <string>
#include <unordered_map>
#include <vector>

class TH1;

namespace stxs {

// ===========================================================================
// Generic STXS enums (integer-based, values from config)
// ===========================================================================

/// Signal process identifier (configurable integer, not VH-specific).
enum class SignalProcess : int {
  kUnknown = 0,
  kUserDefinedStart = 100  ///< User-defined processes start here
};

/// Production mode identifier (configurable integer).
enum class ProductionMode : int {
  kUnknown = 0,
  kUserDefinedStart = 100
};

/// Decay channel identifier (configurable integer).
enum class DecayChannel : int {
  kUnknown = 0,
  kUserDefinedStart = 100
};

// ===========================================================================
// Weight structs
// ===========================================================================

/// Acceptance weight pair (muR=2/muF=2 asym → up, muR=0.5/muF=0.5 → down).
struct AcceptanceWeights {
  float up = 1.0f;
  float down = 1.0f;
};

/// Migration weight variations (14 components per event).
struct MigrationWeights {
  float delta1Up = 1.0f;
  float delta1Down = 1.0f;
  float delta2Up = 1.0f;
  float delta2Down = 1.0f;
  float delta75Up = 1.0f;
  float delta75Down = 1.0f;
  float delta150Up = 1.0f;
  float delta150Down = 1.0f;
  float delta250Up = 1.0f;
  float delta250Down = 1.0f;
  float delta400Up = 1.0f;
  float delta400Down = 1.0f;
  float fineAcceptanceUp = 1.0f;
  float fineAcceptanceDown = 1.0f;
};

// ===========================================================================
// Config structures
// ===========================================================================

/// A single analysis bin definition.
struct BinDefinition {
  int analysisBin = 0;
  std::string label;
  std::vector<int> fineCategories;
};

/// Specification for an acceptance weight histogram pair.
struct AcceptanceSpec {
  int processId = 0;       ///< SignalProcess as integer
  int channelId = 0;       ///< DecayChannel as integer
  std::string upFile;      ///< ROOT file for the up (muR=2) histogram
  std::string upHistogram; ///< Histogram name in upFile
  std::string downFile;    ///< ROOT file for the down (muR=0.5) histogram
  std::string downHistogram; ///< Histogram name in downFile
};

/// Specification for migration weight histograms for one production mode.
struct MigrationSpec {
  int productionModeId = 0; ///< ProductionMode as integer
  std::string delta75File;
  std::string delta75Histogram;
  std::string delta150File;
  std::string delta150Histogram;
  std::string delta250File;
  std::string delta250Histogram;
  std::string delta400File;
  std::string delta400Histogram;
  std::string delta1File;
  std::string delta1Histogram;
  std::string delta2File;
  std::string delta2Histogram;
  std::string scaleUpFile;
  std::string scaleUpHistogram;
  std::string scaleDownFile;
  std::string scaleDownHistogram;
};

/// Rule for classifying a signal process from string labels.
struct SignalProcessRule {
  std::string keyword;     ///< Substring to search for (case-insensitive)
  int processId = 0;       ///< SignalProcess integer to assign on match
};

/// Top-level STXS configuration.
struct Config {
  bool enabled = false;
  std::string auxDirectory;
  std::string coarseBranch = "HTXS_stage1_1_cat_pTjet30GeV";
  std::string fineBranch = "HTXS_stage1_1_fine_cat_pTjet30GeV";
  std::string analysisCoarseBranch;
  std::string analysisFineBranch;
  int defaultBin = 0;
  std::vector<BinDefinition> bins;
  std::vector<AcceptanceSpec> acceptanceSpecs;
  std::vector<MigrationSpec> migrationSpecs;
  std::vector<SignalProcessRule> signalProcessRules;
  std::unordered_map<int, int> fineCategoryToAnalysisBin;
  std::unordered_map<int, std::string> analysisBinLabels;
};

// ===========================================================================
// Config loading functions
// ===========================================================================

/// Load STXS configuration from a text-format bin config file.
/// The bin config file has one line per analysis bin:
///   <bin_number> <label> <fine_cat1,fine_cat2,...>
/// Comments (lines starting with #) and inline comments are supported.
Config loadConfig(const std::string &binConfigPath,
                  const std::string &auxDirectory,
                  const std::string &coarseBranch,
                  const std::string &fineBranch,
                  bool enabled);

/// Load the full STXS configuration from a YAML file (preferred for genericity).
/// The YAML file specifies:
///   - enabled, bin_config, aux_directory
///   - branches (coarse, fine, analysis_coarse, analysis_fine)
///   - acceptance_weights: list of {process, channel, up: {file, hist}, down: {file, hist}}
///   - migration_weights: list of {production_mode, ...histogram specs...}
///   - signal_process_rules: list of {keyword, process_id}
Config loadConfigFromYaml(const std::string &yamlConfigPath);

// ===========================================================================
// Signal process helpers (config-driven string matching)
// ===========================================================================

/// Classify a signal process by matching (process, sample, group) strings
/// against the configured rules. Returns kUnknown if no rule matches.
int classifySignalProcess(const std::string &process,
                          const std::string &sample,
                          const std::string &group,
                          const std::vector<SignalProcessRule> &rules);

/// Derive production mode from signal process using configurable mapping.
int productionModeFromProcess(int signalProcess,
                              const std::unordered_map<int, int> &processToModeMap);

/// Human-readable name helpers (loaded from config or fallback).
std::string signalProcessName(int process,
                              const std::unordered_map<int, std::string> &names);
std::string productionModeName(int mode,
                               const std::unordered_map<int, std::string> &names);

// ===========================================================================
// Manager class
// ===========================================================================

class Manager {
public:
  /// Construct the manager and load auxiliary histograms from the config.
  explicit Manager(Config config);

  /// Whether STXS processing is enabled.
  bool enabled() const;

  /// Access the configuration.
  const Config &config() const;

  /// Map a fine category to its analysis bin. Returns defaultBin (0) if unmapped.
  int analysisBin(int fineCategory) const;

  /// Compute acceptance weights for a (process, channel, coarseCategory) triple.
  /// Returns {1.0, 1.0} if not applicable or no histograms loaded.
  AcceptanceWeights acceptanceWeights(int processId,
                                      int channelId,
                                      int coarseCategory) const;

  /// Compute migration weights for a (productionMode, fineCategory) pair.
  /// The fallback values are used when fineCategory is a boundary that lacks
  /// its own fine-acceptance histogram (e.g. fine categories 403, 503).
  MigrationWeights migrationWeights(int productionModeId,
                                    int fineCategory,
                                    float fallbackUp,
                                    float fallbackDown) const;

private:
  struct AcceptanceSet {
    TH1 *up = nullptr;
    TH1 *down = nullptr;
  };

  struct MigrationSet {
    TH1 *delta75 = nullptr;
    TH1 *delta150 = nullptr;
    TH1 *delta250 = nullptr;
    TH1 *delta400 = nullptr;
    TH1 *delta1 = nullptr;
    TH1 *delta2 = nullptr;
    TH1 *scaleUp = nullptr;
    TH1 *scaleDown = nullptr;
  };

  void loadAuxiliaryHistograms();

  /// Evaluate an acceptance set for a given coarse category bin.
  AcceptanceWeights evaluateSet(const AcceptanceSet &set, int coarseCategory) const;

  Config config_m;
  std::vector<TH1 *> ownedHistograms_m;
  std::unordered_map<std::string, AcceptanceSet> acceptanceSets_m;
  std::unordered_map<int, MigrationSet> migrationSets_m;
};

} // namespace stxs

#endif // STXS_MANAGER_H_INCLUDED
