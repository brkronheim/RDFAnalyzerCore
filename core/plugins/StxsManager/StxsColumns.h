#ifndef STXS_COLUMNS_H_INCLUDED
#define STXS_COLUMNS_H_INCLUDED

//==============================================================================
// StxsColumns — Generic STXS column definitions for RDataFrame
//
// Purpose:
//   Defines the standard STXS classification and weight columns on an
//   RDataFrame. These columns are analysis-agnostic and driven by a
//   StxsConfig struct that bundles the stxs::Manager, signal-process
//   identifiers, and branch names.
//
// Columns defined:
//   stxs_decay_channel        : integer (from vhbb_channel or configured input)
//   stxs_signal_process       : integer signal-process identifier
//   stxs_production_mode      : integer production-mode identifier
//   stxs_is_signal            : true if STXS signal
//   stxs_stage0_category      : HTXS_stage0_cat
//   stxs_stage1_category      : composite (stage1 fallback to stage1.1)
//   stxs_stage1_1_category    : stage-1.1 coarse bin
//   stxs_stage1_1_fine_category : stage-1.1 fine bin
//   stxs_stage1_2_category    : stage-1.2 coarse bin
//   stxs_stage1_2_fine_category : stage-1.2 fine bin
//   stxs_analysis_coarse_category : analysis-specific coarse bin (optional)
//   stxs_analysis_fine_category   : analysis-specific fine bin (optional)
//   stxs_analysis_bin         : STXS analysis bin (0 for non-signal)
//   stxs_template_bin         : histogram template bin
//   stxs_acceptance_weight_up/down : acceptance SF ±1σ
//   stxs_migration_weights    : vector of 14 migration weight variations
//   delta1_weight_up/down, delta2_weight_up/down, delta75_weight_up/down,
//     delta150_weight_up/down, delta250_weight_up/down,
//     delta400_weight_up/down, STXS_fine_acc_up/down
//
// Usage:
//   StxsConfig cfg;
//   cfg.manager = ...;
//   cfg.signalProcess = ...;
//   ...
//   defineStxsColumns(analyzer, cfg);
//==============================================================================

#include <string>
#include <vector>
#include <memory>

class Analyzer;

namespace stxs {
class Manager;
}

namespace stxs {

/// Configuration bundle for defineStxsColumns().
struct StxsConfig {
  /// The STXS Manager instance (loaded with aux histograms).
  std::shared_ptr<stxs::Manager> manager;

  /// Signal process identifier (as returned by classifySignalProcess()).
  int signalProcess = 0;

  /// Production mode identifier (derived from signalProcess).
  int productionMode = 0;

  /// Whether this sample is an STXS signal sample.
  bool isSTXSSignal = false;

  /// Input branch for decay channel classification (e.g. "vhbb_channel").
  std::string decayChannelBranch = "vhbb_channel";

  /// NanoAOD branch for stage-0 category.
  std::string stage0Branch = "HTXS_stage0_cat";

  /// NanoAOD branch for stage-1 category.
  std::string stage1Branch = "HTXS_stage1_cat_pTjet30GeV";

  /// NanoAOD branch for stage-1.1 coarse category.
  std::string stage11CoarseBranch = "HTXS_stage1_1_cat_pTjet30GeV";

  /// NanoAOD branch for stage-1.1 fine category.
  std::string stage11FineBranch = "HTXS_stage1_1_fine_cat_pTjet30GeV";

  /// NanoAOD branch for stage-1.2 coarse category.
  std::string stage12CoarseBranch = "HTXS_stage1_2_cat_pTjet30GeV";

  /// NanoAOD branch for stage-1.2 fine category.
  std::string stage12FineBranch = "HTXS_stage1_2_fine_cat_pTjet30GeV";

  /// Analysis-specific category branches (optional, can be empty).
  std::string analysisCoarseBranch;
  std::string analysisFineBranch;
};

/// Define all STXS classification and weight columns on the analyzer.
/// Uses the StxsConfig to determine branch names and signal properties.
void defineStxsColumns(Analyzer &an, const StxsConfig &cfg);

/// Map (isSignal, analysisBin) → template bin index for histogram filling.
/// Non-signal events always fall into bin 1; signal events use analysisBin + 1.
int stxsTemplateBin(bool isSignal, int analysisBin);

} // namespace stxs

#endif // STXS_COLUMNS_H_INCLUDED
