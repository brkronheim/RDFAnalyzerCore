#ifndef TAGGEREFFICIENCYSERVICE_H_INCLUDED
#define TAGGEREFFICIENCYSERVICE_H_INCLUDED

//==============================================================================
// TaggerEfficiencyService — Per-process tagger efficiency histogram inputs
//
// Purpose:
//   An IAnalysisService that auto-books histograms of tagger discriminator
//   scores (or working-point categories) for flavor-efficiency measurements.
//   Supports two binning modes:
//
//   1. "histogram" mode — bin the raw discriminator score with configurable
//      bin edges (same as the current pass1 config-driven approach).
//   2. "working_point" mode — bin by WP category (0 = fail all, 1 = pass
//      loose, 2 = pass medium, ...) using a TaggerWorkingPointManager.
//
//   Optional flavor splitting adds a per-flavour axis (e.g. b/c/light/other
//   using Jet_hadronFlavour or a configurable flavour branch).
//
// Configuration (YAML file specified by `taggerEfficiencyConfig` key):
//
//   tagger_efficiency:
//     - mode: "histogram"
//       taggerColumn: "Jet_btagB"
//       weightColumn: "weight_nominal"
//       bins: 20
//       lowerBound: 0.0
//       upperBound: 1.0
//       channelVariable: "vhbb_channel"
//       channelBins: 3
//       channelLabels: ["2L", "1L", "0L"]
//       flavorSplit: "Jet_hadronFlavour"     # optional
//
//     - mode: "working_point"
//       taggerColumn: "Jet_btagB"
//       weightColumn: "weight_nominal"
//       taggerManagerRole: "btagManager"
//       workingPoints: ["loose", "medium", "tight"]
//       channelVariable: "vhbb_channel"
//       channelBins: 3
//       channelLabels: ["2L", "1L", "0L"]
//       flavorSplit: "Jet_hadronFlavour"     # optional
//
// Inline config keys (alternative to YAML file):
//   enableTaggerEfficiency: "true"
//   taggerEfficiencyMode: "histogram" | "working_point"
//   taggerEfficiencyColumns: "Jet_btagB"
//   taggerEfficiencyWeight: "weight_nominal"
//   taggerEfficiencyNBins: 20
//   taggerEfficiencyLow: 0.0
//   taggerEfficiencyHigh: 1.0
//   taggerEfficiencyChannelVariable: "vhbb_channel"
//   taggerEfficiencyChannelBins: 3
//   taggerEfficiencyChannelRegions: "2L,1L,0L"
//   taggerEfficiencyFlavorSplit: "Jet_hadronFlavour"   # optional
//   taggerEfficiencyWPManagerRole: "btagManager"
//   taggerEfficiencyWorkingPoints: "loose,medium,tight"
//
// Service lifecycle:
//   initialize()  — Parse config, store tagger specifications
//   onPreFilter() — Book histograms for each configured tagger
//   finalize()    — Write histograms to meta output, log summary
//==============================================================================

#include "api/IAnalysisService.h"
#include "api/ManagerContext.h"

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RResultPtr.hxx>
#include <TH1D.h>
#include <TH2D.h>

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

/**
 * @brief Specification for one tagger efficiency histogram.
 */
struct TaggerEfficiencySpec {
  /// Binning mode: "histogram" (raw score) or "working_point" (WP category).
  std::string mode = "histogram";

  /// Tagger discriminator column name (e.g. "Jet_btagB").
  std::string taggerColumn;

  /// Event weight column name (default: "weight_nominal").
  std::string weightColumn = "weight_nominal";

  /// Histogram-mode: number of bins.
  int nBins = 20;
  double lowerBound = 0.0;
  double upperBound = 1.0;

  /// Channel-splitting variable (e.g. "vhbb_channel"). Empty = no channel axis.
  std::string channelVariable;
  int channelBins = 0;
  double channelLowerBound = 0.0;
  double channelUpperBound = 0.0;
  std::vector<std::string> channelLabels;

  /// Working-point mode: role name of the TaggerWorkingPointManager plugin.
  std::string taggerManagerRole;

  /// Working-point mode: list of WP names to categorize by (e.g. "loose", "medium", "tight").
  std::vector<std::string> workingPoints;

  /// Optional flavor-split branch (e.g. "Jet_hadronFlavour"). Empty = no flavor axis.
  std::string flavorSplitBranch;

  /// Number of flavor categories (only used if flavorSplitBranch is non-empty).
  int flavorBins = 4;
  double flavorLowerBound = 0.0;
  double flavorUpperBound = 5.0;
};

/**
 * @class TaggerEfficiencyService
 * @brief Analysis service for booking tagger efficiency histograms.
 *
 * Follows the CounterService pattern: reads config in initialize(),
 * books histograms in onPreFilter(), writes to meta output in finalize().
 */
class TaggerEfficiencyService : public IAnalysisService {
public:
  void initialize(ManagerContext &ctx) override;
  void onPreFilter(ROOT::RDF::RNode &df) override;
  void finalize(ROOT::RDF::RNode &df) override;

  /**
   * @brief Contribute structured provenance metadata.
   */
  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  void configureFromConfigMap();
  void configureFromYaml(const std::string &path);
  void bookHistogramModeHistogram(ROOT::RDF::RNode &df,
                                   const TaggerEfficiencySpec &spec);
  void bookWorkingPointHistogram(ROOT::RDF::RNode &df,
                                  const TaggerEfficiencySpec &spec);

  ManagerContext *ctx_m = nullptr;
  bool enabled_m = false;

  /// Parsed tagger specifications (from YAML or inline config).
  std::vector<TaggerEfficiencySpec> specs_m;

  /// Booked histogram results.
  struct HistEntry {
    std::string name;
    ROOT::RDF::RResultPtr<TH1D> result1D;
    ROOT::RDF::RResultPtr<TH2D> result2D; // for channel-split histograms
    bool is2D = false;
  };
  std::vector<HistEntry> bookedHistograms_m;

  /// Sample name for output naming.
  std::string sampleName_m = "unknown";

  /// YAML config path (if used).
  std::string yamlConfigPath_m;

  /// Inline config values (used if YAML is not provided).
  std::string inlineMode_m = "histogram";
  std::string inlineTaggerColumns_m;
  std::string inlineWeightColumn_m = "weight_nominal";
  int inlineNBins_m = 20;
  double inlineLow_m = 0.0;
  double inlineHigh_m = 1.0;
  std::string inlineChannelVariable_m;
  int inlineChannelBins_m = 0;
  std::string inlineChannelRegions_m;
  std::string inlineFlavorSplit_m;
  std::string inlineWPManagerRole_m = "btagManager";
  std::string inlineWorkingPoints_m;
};

#endif // TAGGEREFFICIENCYSERVICE_H_INCLUDED
