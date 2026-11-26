#include "ConfigBoolean.h"
//==============================================================================
// TaggerEfficiencyService — Per-process tagger efficiency histogram inputs
//==============================================================================

#include "TaggerEfficiencyService.h"
#include "ConfigDocument.h"

#include "api/IConfigurationProvider.h"
#include "api/IDataFrameProvider.h"
#include "api/ILogger.h"
#include "api/IOutputSink.h"
#include "NullOutputSink.h"

#include <TFile.h>
#include <TObject.h>

#include <yaml-cpp/yaml.h>
#include <cmath>
#include <set>
#include <type_traits>
#include <stdexcept>
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

  const auto enableValue = get_str("enableTaggerEfficiency", "false");
  enabled_m = rdf_config_detail::parseBooleanConfig("enableTaggerEfficiency", enableValue, false);
  if (!enabled_m) return;

  sampleName_m = get_str("sample", "unknown");

  yamlConfigPath_m = get_str("taggerEfficiencyConfig");
  if (yamlConfigPath_m.empty()) {
    configureFromConfigMap();
  } else {
    configureFromYaml(ctx.config.resolveConfigPath(yamlConfigPath_m));
  }
}

namespace {

TaggerEfficiencySpec parseSpec(const YAML::Node& node) {
  static const std::set<std::string> allowed{
      "mode", "taggerColumn", "weightColumn", "bins", "lowerBound", "upperBound",
      "channelVariable", "channelBins", "channelLabels", "workingPoints"};
  if (!node.IsMap()) throw std::invalid_argument("expected a mapping");
  std::set<std::string> seen;
  for (const auto& field : node) {
    const auto key = field.first.as<std::string>();
    if (!allowed.count(key)) throw std::invalid_argument("unsupported key '" + key + "'");
    if (!seen.insert(key).second) throw std::invalid_argument("duplicate key '" + key + "'");
  }
  TaggerEfficiencySpec spec;
  auto read = [&](const char* key, auto& value) {
    if (node[key]) {
      try {
        value = node[key].as<std::decay_t<decltype(value)>>();
      } catch (const YAML::Exception& e) {
        throw std::invalid_argument(std::string(key) + ": " + e.what());
      }
    }
  };
  read("mode", spec.mode);
  read("taggerColumn", spec.taggerColumn);
  read("weightColumn", spec.weightColumn);
  read("bins", spec.nBins);
  read("lowerBound", spec.lowerBound);
  read("upperBound", spec.upperBound);
  read("channelVariable", spec.channelVariable);
  read("channelBins", spec.channelBins);
  read("channelLabels", spec.channelLabels);
  read("workingPoints", spec.workingPoints);
  if (spec.taggerColumn.empty() || spec.weightColumn.empty())
    throw std::invalid_argument("taggerColumn and weightColumn must name columns");
  if (spec.mode != "histogram" && spec.mode != "working_point")
    throw std::invalid_argument("mode must be histogram or working_point");
  if (spec.nBins < 1 || !std::isfinite(spec.lowerBound) ||
      !std::isfinite(spec.upperBound) || spec.upperBound <= spec.lowerBound)
    throw std::invalid_argument("bins must be positive with finite lowerBound < upperBound");
  if (spec.mode == "working_point" && (node["bins"] || node["lowerBound"] || node["upperBound"]))
    throw std::invalid_argument("working_point mode derives bins from workingPoints. Remove histogram bounds.");
  std::set<std::string> wpNames;
  for (const auto& point : spec.workingPoints)
    if (point.empty() || !wpNames.insert(point).second)
      throw std::invalid_argument("workingPoints must contain distinct nonempty names");
  if (spec.mode == "working_point" && spec.workingPoints.empty())
    throw std::invalid_argument("working_point mode requires workingPoints");
  if (spec.mode == "histogram" && !spec.workingPoints.empty())
    throw std::invalid_argument("workingPoints requires working_point mode");
  if (spec.channelBins < 0 || (spec.channelBins > 0) != !spec.channelVariable.empty())
    throw std::invalid_argument("channelVariable requires positive channelBins and vice versa");
  if (!spec.channelLabels.empty() && spec.channelLabels.size() != static_cast<size_t>(spec.channelBins))
    throw std::invalid_argument("channelLabels must contain one label per channel bin");
  spec.channelLowerBound = 1.;
  spec.channelUpperBound = 1. + spec.channelBins;
  return spec;
}

} // namespace

void TaggerEfficiencyService::configureFromConfigMap() {
  const auto& values = ctx_m->config.getConfigMap();
  YAML::Node row(YAML::NodeType::Map);
  row["taggerColumn"] = "Jet_btagB";
  for (const auto& [key, field] : std::initializer_list<std::pair<const char*, const char*>>{
         {"taggerEfficiencyMode","mode"}, {"taggerEfficiencyColumns","taggerColumn"},
         {"taggerEfficiencyWeight","weightColumn"}, {"taggerEfficiencyNBins","bins"},
         {"taggerEfficiencyLow","lowerBound"}, {"taggerEfficiencyHigh","upperBound"},
         {"taggerEfficiencyChannelVariable","channelVariable"}, {"taggerEfficiencyChannelBins","channelBins"},
         {"taggerEfficiencyFlavorSplit","flavorSplit"}, {"taggerEfficiencyWPManagerRole","taggerManagerRole"}}) {
    auto it = values.find(key);
    if (it != values.end() && !it->second.empty()) row[field] = it->second;
  }
  for (const auto& [key, field] : std::initializer_list<std::pair<const char*, const char*>>{
         {"taggerEfficiencyChannelRegions","channelLabels"}, {"taggerEfficiencyWorkingPoints","workingPoints"}}) {
    auto it = values.find(key);
    if (it == values.end()) continue;
    std::istringstream csv(it->second);
    std::string item;
    while (std::getline(csv, item, ',')) {
      auto start = item.find_first_not_of(" \t");
      if (start != std::string::npos)
        row[field].push_back(item.substr(start, item.find_last_not_of(" \t") - start + 1));
    }
  }
  try { specs_m.push_back(parseSpec(row)); }
  catch (const std::exception& e) {
    throw std::invalid_argument("inline tagger efficiency configuration: " + std::string(e.what()));
  }
}

void TaggerEfficiencyService::configureFromYaml(const std::string& path) {
  size_t rowNumber = 0;
  try {
    const auto root = rdf_config_detail::loadDocument(path);
    if (!root.IsMap() || root.size() != 1 || !root["tagger_efficiency"].IsSequence() ||
        root["tagger_efficiency"].size() == 0)
      throw std::invalid_argument("expected a nonempty tagger_efficiency sequence");
    std::set<std::pair<std::string, std::string>> outputs;
    for (const auto& row : root["tagger_efficiency"]) {
      ++rowNumber;
      auto spec = parseSpec(row);
      if (!outputs.emplace(spec.mode, spec.taggerColumn).second)
        throw std::invalid_argument("duplicate histogram for '" + spec.taggerColumn + "'");
      specs_m.push_back(std::move(spec));
    }
  } catch (const std::exception& e) {
    throw std::invalid_argument(path + (rowNumber ? " tagger_efficiency entry " + std::to_string(rowNumber) : "") +
                                ": " + e.what());
  }
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

  for (const auto& spec : specs_m) {
    const auto score = spec.mode == "working_point" ? spec.taggerColumn + "_wp_category" : spec.taggerColumn;
    for (const auto& column : {score, spec.weightColumn, spec.channelVariable})
      if (!column.empty() && !df.HasColumn(column))
        throw std::invalid_argument("Tagger efficiency column '" + column +
            "' is unavailable on the inclusive dataframe. Define it before the first filter.");
  }
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
    throw std::runtime_error("Cannot open tagger metadata output '" + fileName + "' for writing");
  }

  int nWritten = 0;
  for (size_t index = 0; index < bookedHistograms_m.size(); ++index) {
    auto& entry = bookedHistograms_m[index];
    if (entry.is2D) {
      auto histPtr = entry.result2D.GetPtr();
      if (!histPtr) {
        continue;
      }
      const auto& labels = specs_m.at(index).channelLabels;
      for (size_t bin = 0; bin < labels.size(); ++bin)
        histPtr->GetYaxis()->SetBinLabel(static_cast<int>(bin + 1), labels[bin].c_str());
      if (outFile.WriteTObject(histPtr, histPtr->GetName(), "Overwrite") <= 0)
        throw std::runtime_error("Failed to write tagger histogram '" + entry.name + "' to " + fileName);
      ++nWritten;
    } else {
      auto histPtr = entry.result1D.GetPtr();
      if (!histPtr) {
        continue;
      }
      if (outFile.WriteTObject(histPtr, histPtr->GetName(), "Overwrite") <= 0)
        throw std::runtime_error("Failed to write tagger histogram '" + entry.name + "' to " + fileName);
      ++nWritten;
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
