//==============================================================================
// TaggerEfficiencyService — Per-process tagger efficiency histogram inputs
//==============================================================================

#include "TaggerEfficiencyService.h"

#include "api/IConfigurationProvider.h"
#include "api/IDataFrameProvider.h"
#include "api/ILogger.h"
#include "api/IOutputSink.h"
#include "NullOutputSink.h"

#include <TFile.h>
#include <TObject.h>

#include <fstream>
#include <sstream>

// ===========================================================================
// IAnalysisService — initialize
// ===========================================================================

void TaggerEfficiencyService::initialize(ManagerContext &ctx) {
  ctx_m = &ctx;

  const auto &configMap = ctx.config.getConfigMap();

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

  enabled_m = get_bool("enableTaggerEfficiency", false);
  if (!enabled_m) {
    return;
  }

  sampleName_m = get_str("sample", "unknown");

  // Check for YAML config first
  yamlConfigPath_m = get_str("taggerEfficiencyConfig");
  if (!yamlConfigPath_m.empty()) {
    configureFromYaml(yamlConfigPath_m);
    return;
  }

  // Fall back to inline config
  configureFromConfigMap();
}

void TaggerEfficiencyService::configureFromConfigMap() {
  const auto &configMap = ctx_m->config.getConfigMap();

  auto get_str = [&](const std::string &key,
                      const std::string &def = "") -> std::string {
    auto it = configMap.find(key);
    return (it != configMap.end()) ? it->second : def;
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

  auto splitComma = [](const std::string &text) -> std::vector<std::string> {
    std::vector<std::string> parts;
    std::stringstream stream(text);
    std::string token;
    while (std::getline(stream, token, ',')) {
      // Trim whitespace
      auto first = token.find_first_not_of(" \t");
      auto last = token.find_last_not_of(" \t");
      if (first != std::string::npos) {
        parts.push_back(token.substr(first, last - first + 1));
      }
    }
    return parts;
  };

  inlineMode_m = get_str("taggerEfficiencyMode", "histogram");
  inlineTaggerColumns_m = get_str("taggerEfficiencyColumns", "Jet_btagB");
  inlineWeightColumn_m = get_str("taggerEfficiencyWeight", "weight_nominal");
  inlineNBins_m = get_int("taggerEfficiencyNBins", 20);
  inlineLow_m = get_double("taggerEfficiencyLow", 0.0);
  inlineHigh_m = get_double("taggerEfficiencyHigh", 1.0);
  inlineChannelVariable_m = get_str("taggerEfficiencyChannelVariable");
  inlineChannelBins_m = get_int("taggerEfficiencyChannelBins", 0);
  inlineChannelRegions_m = get_str("taggerEfficiencyChannelRegions");
  inlineFlavorSplit_m = get_str("taggerEfficiencyFlavorSplit");
  inlineWPManagerRole_m =
      get_str("taggerEfficiencyWPManagerRole", "btagManager");
  inlineWorkingPoints_m = get_str("taggerEfficiencyWorkingPoints");

  // Build a single spec from inline config
  TaggerEfficiencySpec spec;
  spec.mode = inlineMode_m;
  spec.taggerColumn = inlineTaggerColumns_m;
  spec.weightColumn = inlineWeightColumn_m;
  spec.nBins = inlineNBins_m;
  spec.lowerBound = inlineLow_m;
  spec.upperBound = inlineHigh_m;
  spec.channelVariable = inlineChannelVariable_m;
  spec.channelBins = inlineChannelBins_m;
  spec.channelLowerBound = 1.0;
  spec.channelUpperBound =
      static_cast<double>(inlineChannelBins_m) + 1.0;
  if (!inlineChannelRegions_m.empty()) {
    spec.channelLabels = splitComma(inlineChannelRegions_m);
  }
  spec.flavorSplitBranch = inlineFlavorSplit_m;
  spec.taggerManagerRole = inlineWPManagerRole_m;
  if (!inlineWorkingPoints_m.empty()) {
    spec.workingPoints = splitComma(inlineWorkingPoints_m);
  }

  specs_m.push_back(std::move(spec));
}

void TaggerEfficiencyService::configureFromYaml(const std::string &path) {
  // TODO: Implement YAML parsing for full tagger efficiency configuration.
  // For now, log a warning and fall through to inline config.
  if (ctx_m) {
    ctx_m->logger.log(
        ILogger::Level::Warn,
        "TaggerEfficiencyService: YAML config parsing not yet implemented. "
        "Falling back to inline config keys.");
  }
  configureFromConfigMap();
}

// ===========================================================================
// IAnalysisService — onPreFilter
//
// Book all tagger efficiency histograms on the dataframe before any filters
// are applied (so the histograms run over all events).
// ===========================================================================

void TaggerEfficiencyService::onPreFilter(ROOT::RDF::RNode &df) {
  if (!enabled_m || !ctx_m) {
    return;
  }

  ctx_m->logger.log(
      ILogger::Level::Info,
      "TaggerEfficiencyService: booking " + std::to_string(specs_m.size()) +
          " tagger efficiency histograms for sample '" + sampleName_m + "'");

  for (const auto &spec : specs_m) {
    if (spec.mode == "working_point") {
      bookWorkingPointHistogram(df, spec);
    } else {
      bookHistogramModeHistogram(df, spec);
    }
  }
}

// ===========================================================================
// Histogram-mode booking
//
// Books a 1D or 2D histogram of the raw discriminator score, optionally
// split by channel.
// ===========================================================================

void TaggerEfficiencyService::bookHistogramModeHistogram(
    ROOT::RDF::RNode &df, const TaggerEfficiencySpec &spec) {
  const std::string baseName =
      "taggerEff_" + spec.taggerColumn + "_" + sampleName_m;

  if (spec.channelVariable.empty()) {
    // 1D: discriminator score only
    ROOT::RDF::TH1DModel model(
        baseName.c_str(),
        (baseName + ";" + spec.taggerColumn + ";weighted_events").c_str(),
        spec.nBins, spec.lowerBound, spec.upperBound);
    bookedHistograms_m.push_back(
        {baseName, df.Histo1D(model, spec.taggerColumn, spec.weightColumn),
         {}, false});
  } else {
    // 2D: discriminator score × channel
    // We need a column for the channel bin index. If the channel variable
    // provides integer values directly, we can use it. Otherwise we define
    // a computed column.
    const std::string channelCol = spec.channelVariable;
    const int nChanBins = (spec.channelBins > 0) ? spec.channelBins : 1;
    const double chanLow = spec.channelLowerBound;
    const double chanHigh = spec.channelUpperBound;

    ROOT::RDF::TH2DModel model(
        baseName.c_str(),
        (baseName + ";" + spec.taggerColumn + ";" + channelCol +
         ";weighted_events")
            .c_str(),
        spec.nBins, spec.lowerBound, spec.upperBound, nChanBins, chanLow,
        chanHigh);

    bookedHistograms_m.push_back(
        {baseName, {}, df.Histo2D(model, spec.taggerColumn, channelCol,
                                  spec.weightColumn),
         true});
  }
}

// ===========================================================================
// Working-point-mode booking
//
// Books a 1D or 2D histogram of the WP category (0 = fail all, 1 = pass
// loose, 2 = pass medium, ...). The WP category is determined by looking
// up the TaggerWorkingPointManager plugin.
//
// Note: This mode requires that the user has already configured a
// TaggerWorkingPointManager with the appropriate working points, and that
// the WP category column (e.g. "Jet_btagB_wp_category") exists on the
// dataframe.
// ===========================================================================

void TaggerEfficiencyService::bookWorkingPointHistogram(
    ROOT::RDF::RNode &df, const TaggerEfficiencySpec &spec) {
  // The WP category column is expected to follow the naming convention
  // of TaggerWorkingPointManager: "<taggerColumn>_wp_category".
  const std::string wpCategoryCol = spec.taggerColumn + "_wp_category";
  const int nCategories = static_cast<int>(spec.workingPoints.size()) + 1;

  const std::string baseName =
      "taggerEffWP_" + spec.taggerColumn + "_" + sampleName_m;

  if (spec.channelVariable.empty()) {
    // 1D: WP category only
    ROOT::RDF::TH1DModel model(
        baseName.c_str(),
        (baseName + ";WP_category;weighted_events").c_str(),
        nCategories, -0.5, static_cast<double>(nCategories) - 0.5);
    bookedHistograms_m.push_back(
        {baseName, df.Histo1D(model, wpCategoryCol, spec.weightColumn), {},
         false});
  } else {
    // 2D: WP category × channel
    const std::string channelCol = spec.channelVariable;
    const int nChanBins = (spec.channelBins > 0) ? spec.channelBins : 1;
    const double chanLow = spec.channelLowerBound;
    const double chanHigh = spec.channelUpperBound;

    ROOT::RDF::TH2DModel model(
        baseName.c_str(),
        (baseName + ";WP_category;" + channelCol + ";weighted_events")
            .c_str(),
        nCategories, -0.5, static_cast<double>(nCategories) - 0.5,
        nChanBins, chanLow, chanHigh);
    bookedHistograms_m.push_back(
        {baseName, {},
         df.Histo2D(model, wpCategoryCol, channelCol, spec.weightColumn),
         true});
  }

  if (ctx_m) {
    ctx_m->logger.log(
        ILogger::Level::Debug,
        "TaggerEfficiencyService: booked WP-based histogram '" + baseName +
            "' using column '" + wpCategoryCol + "' with " +
            std::to_string(nCategories) + " categories");
  }
}

// ===========================================================================
// IAnalysisService — finalize
//
// Write all booked histograms to the meta output ROOT file.
// ===========================================================================

void TaggerEfficiencyService::finalize(ROOT::RDF::RNode &df) {
  if (!enabled_m || !ctx_m) {
    return;
  }

  // Check that meta sink is not null
  if (dynamic_cast<NullOutputSink *>(&ctx_m->metaSink) != nullptr) {
    ctx_m->logger.log(ILogger::Level::Warn,
                      "TaggerEfficiencyService: meta output sink is null.");
    return;
  }

  std::string fileName =
      ctx_m->metaSink.resolveOutputFile(ctx_m->config, OutputChannel::Meta);
  if (fileName.empty()) {
    ctx_m->logger.log(ILogger::Level::Warn,
                      "TaggerEfficiencyService: no meta output file configured.");
    return;
  }

  TFile outFile(fileName.c_str(), "UPDATE");
  if (outFile.IsZombie()) {
    ctx_m->logger.log(
        ILogger::Level::Error,
        "TaggerEfficiencyService: failed to open meta output file: " +
            fileName);
    return;
  }

  int nWritten = 0;
  for (auto &entry : bookedHistograms_m) {
    if (entry.is2D) {
      auto histPtr = entry.result2D.GetPtr();
      if (!histPtr) {
        continue;
      }
      histPtr->SetDirectory(&outFile);
      histPtr->Write(histPtr->GetName(), TObject::kOverwrite);
      nWritten++;
    } else {
      auto histPtr = entry.result1D.GetPtr();
      if (!histPtr) {
        continue;
      }
      histPtr->SetDirectory(&outFile);
      histPtr->Write(histPtr->GetName(), TObject::kOverwrite);
      nWritten++;
    }
  }

  outFile.Close();

  ctx_m->logger.log(
      ILogger::Level::Info,
      "TaggerEfficiencyService: wrote " + std::to_string(nWritten) +
          " tagger efficiency histograms to " + fileName);
}

// ===========================================================================
// Provenance
// ===========================================================================

std::unordered_map<std::string, std::string>
TaggerEfficiencyService::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;
  entries["service.tagger_efficiency.enabled"] = enabled_m ? "true" : "false";
  entries["service.tagger_efficiency.sample"] = sampleName_m;
  entries["service.tagger_efficiency.n_specs"] =
      std::to_string(specs_m.size());
  if (!specs_m.empty()) {
    entries["service.tagger_efficiency.columns"] = specs_m[0].taggerColumn;
    entries["service.tagger_efficiency.mode"] = specs_m[0].mode;
  }
  return entries;
}
