//==============================================================================
// StxsColumns — Generic STXS column definitions for RDataFrame
//
// Ported from VHqqSTXS.cc. Made fully generic: branch names, signal process
// IDs, and weight computations are driven by the StxsConfig struct rather
// than hardcoded VH-specific values.
//==============================================================================

#include "StxsColumns.h"
#include "stxs/Manager.h"

#include "ColumnProjection.h"
#include "analyzer.h"

#include <ROOT/RVec.hxx>
#include <RtypesCore.h>

using FloatVec = ROOT::VecOps::RVec<Float_t>;

namespace stxs {

// ---------------------------------------------------------------------------
// defineStxsColumns — define all STXS classification and weight columns
// ---------------------------------------------------------------------------

void defineStxsColumns(Analyzer &an, const StxsConfig &cfg) {
  if (!cfg.manager || !cfg.manager->enabled()) {
    // Define minimal stxs_is_signal column even when disabled, so downstream
    // code that references it doesn't break.
    an.Define("stxs_is_signal", [isSignal = cfg.isSTXSSignal]() {
      return isSignal;
    }, {});
    return;
  }

  /// Step 1: Basic STXS identifiers
  an.Define("stxs_decay_channel", [](int channel) { return channel; },
            {cfg.decayChannelBranch})
      ->Define("stxs_signal_process",
               [signalProcess = cfg.signalProcess]() {
                 return signalProcess;
               },
               {})
      ->Define("stxs_production_mode",
               [productionMode = cfg.productionMode]() {
                 return productionMode;
               },
               {})
      ->Define("stxs_is_signal", [isSignal = cfg.isSTXSSignal]() {
        return isSignal;
      }, {})

      /// Step 2: Stage-0 category (from NanoAOD)
      ->Define("stxs_stage0_category", [](int value) { return value; },
               {cfg.stage0Branch})

      /// Step 3: Stage-1 category (composite: stage1 fallback to stage1.1)
      ->Define(
          "stxs_stage1_category",
          [](int stage1Value, int stage11Value) {
            return stage1Value != 0 ? stage1Value : stage11Value;
          },
          {cfg.stage1Branch, cfg.stage11CoarseBranch})

      /// Step 4: Stage-1.1 categories (coarse and fine)
      ->Define("stxs_stage1_1_category", [](int value) { return value; },
               {cfg.stage11CoarseBranch})
      ->Define("stxs_stage1_1_fine_category", [](int value) { return value; },
               {cfg.stage11FineBranch})

      /// Step 5: Stage-1.2 categories (recoil-based)
      ->Define("stxs_stage1_2_category", [](int value) { return value; },
               {cfg.stage12CoarseBranch})
      ->Define("stxs_stage1_2_fine_category", [](int value) { return value; },
               {cfg.stage12FineBranch})

      /// Step 6: Analysis-specific categories (optional)
      ->Define("stxs_analysis_coarse_category",
               [](int value) { return value; },
               {cfg.analysisCoarseBranch.empty() ? cfg.stage11CoarseBranch
                                                  : cfg.analysisCoarseBranch})
      ->Define("stxs_analysis_fine_category",
               [](int value) { return value; },
               {cfg.analysisFineBranch.empty() ? cfg.stage11FineBranch
                                                : cfg.analysisFineBranch})

      /// Step 7: Analysis bin (only for signal events)
      ->Define(
          "stxs_analysis_bin",
          [manager = cfg.manager, isSignal = cfg.isSTXSSignal](
              int fineCategory) {
            return isSignal ? manager->analysisBin(fineCategory) : 0;
          },
          {"stxs_analysis_fine_category"})

      /// Step 8: Template bin for histogram filling
      ->Define("stxs_template_bin", stxsTemplateBin,
               {"stxs_is_signal", "stxs_analysis_bin"})

      /// Step 9: Acceptance weights (up/down) for STXS signal
      ->Define(
          "stxs_acceptance_weight_up",
          [manager = cfg.manager,
           signalProcess = cfg.signalProcess](int decayChannel,
                                               int coarseCategory) {
            return manager
                ->acceptanceWeights(signalProcess, decayChannel,
                                    coarseCategory)
                .up;
          },
          {"stxs_decay_channel", "stxs_stage1_1_category"})
      ->Define(
          "stxs_acceptance_weight_down",
          [manager = cfg.manager,
           signalProcess = cfg.signalProcess](int decayChannel,
                                               int coarseCategory) {
            return manager
                ->acceptanceWeights(signalProcess, decayChannel,
                                    coarseCategory)
                .down;
          },
          {"stxs_decay_channel", "stxs_stage1_1_category"})

      /// Step 10: Migration weight vector (14 components)
      ->Define(
          "stxs_migration_weights",
          [manager = cfg.manager,
           productionMode = cfg.productionMode](
              int fineCategory, float fallbackUp, float fallbackDown) {
            const auto weights = manager->migrationWeights(
                productionMode, fineCategory, fallbackUp, fallbackDown);
            return FloatVec{
                weights.delta1Up,       weights.delta1Down,
                weights.delta2Up,       weights.delta2Down,
                weights.delta75Up,      weights.delta75Down,
                weights.delta150Up,     weights.delta150Down,
                weights.delta250Up,     weights.delta250Down,
                weights.delta400Up,     weights.delta400Down,
                weights.fineAcceptanceUp, weights.fineAcceptanceDown};
          },
          {"stxs_stage1_1_fine_category", "stxs_acceptance_weight_up",
           "stxs_acceptance_weight_down"});

  /// Step 11: Project individual migration weights into separate columns
  rdfanalysis::column::defineProjectedColumns<FloatVec>(
      an, "stxs_migration_weights",
      rdfanalysis::column::indexProjection<FloatVec>("delta1_weight_up", 0),
      rdfanalysis::column::indexProjection<FloatVec>("delta1_weight_down", 1),
      rdfanalysis::column::indexProjection<FloatVec>("delta2_weight_up", 2),
      rdfanalysis::column::indexProjection<FloatVec>("delta2_weight_down", 3),
      rdfanalysis::column::indexProjection<FloatVec>("delta75_weight_up", 4),
      rdfanalysis::column::indexProjection<FloatVec>("delta75_weight_down", 5),
      rdfanalysis::column::indexProjection<FloatVec>("delta150_weight_up", 6),
      rdfanalysis::column::indexProjection<FloatVec>("delta150_weight_down", 7),
      rdfanalysis::column::indexProjection<FloatVec>("delta250_weight_up", 8),
      rdfanalysis::column::indexProjection<FloatVec>("delta250_weight_down", 9),
      rdfanalysis::column::indexProjection<FloatVec>("delta400_weight_up", 10),
      rdfanalysis::column::indexProjection<FloatVec>(
          "delta400_weight_down", 11),
      rdfanalysis::column::indexProjection<FloatVec>("STXS_fine_acc_up", 12),
      rdfanalysis::column::indexProjection<FloatVec>("STXS_fine_acc_down",
                                                      13));
}

// ---------------------------------------------------------------------------
// stxsTemplateBin helper
// ---------------------------------------------------------------------------

int stxsTemplateBin(bool isSignal, int analysisBin) {
  return isSignal && analysisBin > 0 ? analysisBin + 1 : 1;
}

} // namespace stxs
