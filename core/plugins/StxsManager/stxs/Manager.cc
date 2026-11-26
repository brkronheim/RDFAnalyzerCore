//==============================================================================
// stxs::Manager — Generic STXS Manager implementation
//
// Histogram-bin mappings and special-category policies are supplied by the
// consuming analysis through Config::migrationBins.
//==============================================================================

#include "stxs/Manager.h"

#include <yaml-cpp/yaml.h>
#include <TFile.h>
#include <TH1.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

namespace stxs {
namespace {

// ---------------------------------------------------------------------------
// String helpers
// ---------------------------------------------------------------------------

std::string trimCopy(const std::string &value) {
  const auto first = value.find_first_not_of(" \t\n\r");
  if (first == std::string::npos) {
    return "";
  }
  const auto last = value.find_last_not_of(" \t\n\r");
  return value.substr(first, last - first + 1);
}

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char character) {
                   return static_cast<char>(std::tolower(character));
                 });
  return value;
}

// ---------------------------------------------------------------------------
// Bin-config parsing
// ---------------------------------------------------------------------------

std::vector<int> parseCategoryList(const std::string &text) {
  std::vector<int> categories;
  std::stringstream stream(text);
  std::string token;
  while (std::getline(stream, token, ',')) {
    token = trimCopy(token);
    if (token.empty()) {
      continue;
    }
    categories.push_back(std::stoi(token));
  }
  return categories;
}

// ---------------------------------------------------------------------------
// ROOT histogram cloning
// ---------------------------------------------------------------------------

TH1 *cloneHistogram(const std::string &filePath, const std::string &name) {
  std::unique_ptr<TFile> file(TFile::Open(filePath.c_str(), "READ"));
  if (!file || file->IsZombie()) {
    throw std::runtime_error("stxs::Manager: unable to open ROOT file '" +
                             filePath + "'.");
  }

  auto *histogram = dynamic_cast<TH1 *>(file->Get(name.c_str()));
  if (!histogram) {
    throw std::runtime_error("stxs::Manager: histogram '" + name +
                             "' was not found in '" + filePath + "'.");
  }

  auto *clone = dynamic_cast<TH1 *>(histogram->Clone());
  if (!clone) {
    throw std::runtime_error("stxs::Manager: failed to clone histogram '" +
                             name + "'.");
  }
  clone->SetDirectory(nullptr);
  return clone;
}

// ---------------------------------------------------------------------------
// Symmetric shift helper
// ---------------------------------------------------------------------------

float symmetricShift(TH1 *histogram, int binIndex, bool upDirection) {
  if (!histogram) {
    return 1.0f;
  }
  const auto content =
      static_cast<float>(histogram->GetBinContent(binIndex));
  return upDirection ? 1.0f + content : 1.0f - content;
}

// ---------------------------------------------------------------------------
// Acceptance key builder
// ---------------------------------------------------------------------------

std::string acceptanceKey(int processId, int channelId) {
  return std::to_string(processId) + ":" + std::to_string(channelId);
}

bool isYaml(const std::string &path) {
  const auto ext = std::filesystem::path(path).extension();
  return ext == ".yaml" || ext == ".yml";
}

YAML::Node loadTable(const std::string &path) {
  auto document = YAML::LoadFile(path);
  if (!document.IsSequence() || document.size() == 0)
    throw std::runtime_error(path + ": expected a nonempty sequence of STXS bin mappings");
  return document;
}

void addBin(Config &config, BinDefinition definition) {
  config.analysisBinLabels[definition.analysisBin] = definition.label;
  for (int category : definition.fineCategories)
    config.fineCategoryToAnalysisBin[category] = definition.analysisBin;
  config.bins.push_back(std::move(definition));
}

} // anonymous namespace

// ===========================================================================
// Config loading — text format
// ===========================================================================

Config loadConfig(const std::string &binConfigPath,
                  const std::string &auxDirectory,
                  const std::string &coarseBranch,
                  const std::string &fineBranch,
                  bool enabled) {
  Config config;
  config.enabled = enabled;
  config.auxDirectory = auxDirectory;
  if (!coarseBranch.empty()) {
    config.coarseBranch = coarseBranch;
  }
  if (!fineBranch.empty()) {
    config.fineBranch = fineBranch;
  }

  if (!enabled) {
    return config;
  }

  if (isYaml(binConfigPath)) {
    try {
      for (const auto &row : loadTable(binConfigPath)) {
        BinDefinition bin{row["analysis_bin"].as<int>(), row["label"].as<std::string>(),
                          row["fine_categories"].as<std::vector<int>>()};
        if (bin.label.empty() || bin.fineCategories.empty())
          throw std::runtime_error(binConfigPath + ": bin label and fine_categories must be nonempty");
        addBin(config, std::move(bin));
      }
    } catch (const YAML::Exception &error) {
      throw std::runtime_error(binConfigPath + ": expected analysis_bin, label and fine_categories: " + error.what());
    }
    return config;
  }

  std::ifstream input(binConfigPath);
  if (!input) {
    throw std::runtime_error("stxs::Manager: could not open STXS bin config '" +
                             binConfigPath + "'.");
  }

  std::string line;
  while (std::getline(input, line)) {
    line = trimCopy(line.substr(0, line.find('#')));
    if (line.empty()) {
      continue;
    }
    std::istringstream parser(line);
    BinDefinition definition;
    std::string categoryList;
    if (!(parser >> definition.analysisBin >> definition.label >> categoryList)) {
      throw std::runtime_error(
          "stxs::Manager: invalid STXS bin config line '" + line +
          "'. Expected '<bin> <label> <fine1,fine2,...>'.");
    }
    definition.fineCategories = parseCategoryList(categoryList);
    addBin(config, std::move(definition));
  }

  if (config.bins.empty()) {
    throw std::runtime_error(
        "stxs::Manager: no STXS bin definitions were loaded from '" +
        binConfigPath + "'.");
  }

  return config;
}

// ===========================================================================
// Config loading — YAML format (generic, preferred)
// ===========================================================================

void loadMigrationBinConfig(const std::string &path, Config &config) {
  std::unordered_map<int, MigrationBin> bins;
  auto add = [&](int category, const MigrationBin &bin) {
    if (category <= 0 || bin.ptBin <= 0 || bin.jetPtBin <= 0 || bin.scaleBin <= 0)
      throw std::runtime_error(path + ": migration category and bin indices must be positive");
    if (!bins.emplace(category, bin).second)
      throw std::runtime_error(path + ": duplicate migration category " + std::to_string(category));
  };
  if (isYaml(path)) {
    try {
      for (const auto &row : loadTable(path)) {
        const int category = row["fine_category"].as<int>();
        add(category, {row["pt_bin"].as<int>(), row["jet_pt_bin"].as<int>(),
                       row["scale_bin"].as<int>(), row["acceptance_fallback"].as<bool>()});
      }
    } catch (const YAML::Exception &error) {
      throw std::runtime_error(path + ": invalid STXS migration bin fields: " + error.what());
    }
    config.migrationBins = std::move(bins);
    return;
  }
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("stxs::Manager: cannot open migration bin config '" + path + "'.");
  }
  std::string line;
  while (std::getline(input, line)) {
    line = trimCopy(line.substr(0, line.find('#')));
    if (line.empty()) continue;
    std::istringstream row(line);
    int category = 0, fallback = 0;
    MigrationBin bin;
    std::string extra;
    if (!(row >> category >> bin.ptBin >> bin.jetPtBin >> bin.scaleBin >> fallback) ||
        (row >> extra) || category <= 0 || bin.ptBin <= 0 || bin.jetPtBin <= 0 ||
        bin.scaleBin <= 0 || (fallback != 0 && fallback != 1)) {
      throw std::runtime_error("stxs::Manager: invalid migration bin row '" + line + "'.");
    }
    bin.useAcceptanceFallback = fallback != 0;
    add(category, bin);
  }
  if (bins.empty()) {
    throw std::runtime_error("stxs::Manager: migration bin config is empty: '" + path + "'.");
  }
  config.migrationBins = std::move(bins);
}

Config loadConfigFromYaml(const std::string &yamlConfigPath) {
  throw std::runtime_error(yamlConfigPath +
      ": the combined STXS document is unsupported. Configure stxsBinConfig and "
      "stxsMigrationBinConfig in the analysis YAML, or construct stxs::Config directly.");
}

// ===========================================================================
// Signal process helpers
// ===========================================================================

int classifySignalProcess(const std::string &process,
                          const std::string &sample,
                          const std::string &group,
                          const std::vector<SignalProcessRule> &rules) {
  const std::string haystack =
      toLowerCopy(process + " " + sample + " " + group);
  for (const auto &rule : rules) {
    if (rule.processId == 0) {
      continue; // skip kUnknown
    }
    if (haystack.find(toLowerCopy(rule.keyword)) != std::string::npos) {
      return rule.processId;
    }
  }
  return static_cast<int>(SignalProcess::kUnknown);
}

int productionModeFromProcess(
    int signalProcess,
    const std::unordered_map<int, int> &processToModeMap) {
  const auto match = processToModeMap.find(signalProcess);
  if (match != processToModeMap.end()) {
    return match->second;
  }
  return static_cast<int>(ProductionMode::kUnknown);
}

std::string signalProcessName(
    int process,
    const std::unordered_map<int, std::string> &names) {
  const auto match = names.find(process);
  if (match != names.end()) {
    return match->second;
  }
  return "unknown";
}

std::string productionModeName(
    int mode, const std::unordered_map<int, std::string> &names) {
  const auto match = names.find(mode);
  if (match != names.end()) {
    return match->second;
  }
  return "unknown";
}

// ===========================================================================
// Manager implementation
// ===========================================================================

Manager::Manager(Config config) : config_m(std::move(config)) {
  if (config_m.enabled) {
    if (!config_m.migrationSpecs.empty() && config_m.migrationBins.empty()) {
      throw std::runtime_error("stxs::Manager: migration histograms require explicit migrationBins.");
    }
    for (const auto &entry : config_m.migrationBins) {
      if (entry.first <= 0 || entry.second.ptBin <= 0 ||
          entry.second.jetPtBin <= 0 || entry.second.scaleBin <= 0) {
        throw std::runtime_error("stxs::Manager: migration bin indices must be positive.");
      }
    }
    loadAuxiliaryHistograms();
  }
}

bool Manager::enabled() const { return config_m.enabled; }

const Config &Manager::config() const { return config_m; }

int Manager::analysisBin(int fineCategory) const {
  const auto match = config_m.fineCategoryToAnalysisBin.find(fineCategory);
  if (match != config_m.fineCategoryToAnalysisBin.end()) {
    return match->second;
  }
  return config_m.defaultBin;
}

AcceptanceWeights Manager::acceptanceWeights(int processId,
                                              int channelId,
                                              int coarseCategory) const {
  if (!enabled() || coarseCategory <= 0) {
    return {};
  }

  const auto match = acceptanceSets_m.find(acceptanceKey(processId, channelId));
  if (match == acceptanceSets_m.end()) {
    return {};
  }

  return evaluateSet(match->second, coarseCategory);
}

AcceptanceWeights Manager::evaluateSet(const AcceptanceSet &set,
                                        int coarseCategory) const {
  const auto upBin = set.up ? set.up->FindBin(static_cast<double>(coarseCategory)) : -1;
  const auto downBin = set.down ? set.down->FindBin(static_cast<double>(coarseCategory)) : -1;

  auto reciprocalBinContent = [](TH1 *histogram, int bin) -> float {
    if (!histogram || bin < 0) {
      return 1.0f;
    }
    const auto content = static_cast<float>(histogram->GetBinContent(bin));
    if (!std::isfinite(content) || std::fabs(content) < 1.0e-6f) {
      return 1.0f;
    }
    return 1.0f / content;
  };

  return AcceptanceWeights{reciprocalBinContent(set.up, upBin),
                           reciprocalBinContent(set.down, downBin)};
}

MigrationWeights Manager::migrationWeights(int productionModeId,
                                            int fineCategory,
                                            float fallbackUp,
                                            float fallbackDown) const {
  if (!enabled() || fineCategory <= 0) {
    return {};
  }
  const auto match = migrationSets_m.find(productionModeId);
  if (match == migrationSets_m.end()) {
    return {};
  }
  // Unmapped categories are outside this analysis's migration prescription.
  const auto mapped = config_m.migrationBins.find(fineCategory);
  if (mapped == config_m.migrationBins.end()) {
    return {};
  }
  const auto &bins = mapped->second;
  const int localPtBin = bins.ptBin;
  const int localJetPtBin = bins.jetPtBin;

  const auto &set = match->second;
  MigrationWeights weights;
  weights.delta75Up = symmetricShift(set.delta75, localPtBin, true);
  weights.delta75Down = symmetricShift(set.delta75, localPtBin, false);
  weights.delta150Up = symmetricShift(set.delta150, localPtBin, true);
  weights.delta150Down = symmetricShift(set.delta150, localPtBin, false);
  weights.delta250Up = symmetricShift(set.delta250, localPtBin, true);
  weights.delta250Down = symmetricShift(set.delta250, localPtBin, false);
  weights.delta400Up = symmetricShift(set.delta400, localPtBin, true);
  weights.delta400Down = symmetricShift(set.delta400, localPtBin, false);
  weights.delta1Up = symmetricShift(set.delta1, localJetPtBin, true);
  weights.delta1Down = symmetricShift(set.delta1, localJetPtBin, false);
  weights.delta2Up = symmetricShift(set.delta2, localJetPtBin, true);
  weights.delta2Down = symmetricShift(set.delta2, localJetPtBin, false);

  // Compute combined uncertainty for fine-acceptance correction
  float delta1 = set.delta1
                     ? static_cast<float>(
                           set.delta1->GetBinContent(localJetPtBin))
                     : 0.0f;
  float delta2 = set.delta2
                     ? static_cast<float>(
                           set.delta2->GetBinContent(localJetPtBin))
                     : 0.0f;
  if (std::fabs(delta1) > 1.0f) {
    delta1 = 0.9f;
  }
  if (std::fabs(delta2) > 1.0f) {
    delta2 = 0.9f;
  }

  const float delta400 =
      set.delta400
          ? static_cast<float>(set.delta400->GetBinContent(localPtBin))
          : 0.0f;
  const float combined =
      std::sqrt(delta1 * delta1 + delta2 * delta2 + delta400 * delta400);

  const int scaleBin = bins.scaleBin;
  const float scaleUp = set.scaleUp
                            ? static_cast<float>(
                                  set.scaleUp->GetBinContent(scaleBin))
                            : 1.0f;
  const float scaleDown =
      set.scaleDown
          ? static_cast<float>(set.scaleDown->GetBinContent(scaleBin))
          : 1.0f;

  // The analysis marks categories without independent scale histograms.
  if (bins.useAcceptanceFallback) {
    weights.fineAcceptanceUp = fallbackUp;
    weights.fineAcceptanceDown = fallbackDown;
    return weights;
  }

  weights.fineAcceptanceUp =
      std::fabs(scaleUp) > 1.0e-6f ? (1.0f + combined) / scaleUp : 1.0f;
  weights.fineAcceptanceDown =
      std::fabs(scaleDown) > 1.0e-6f ? (1.0f - combined) / scaleDown : 1.0f;
  return weights;
}

void Manager::loadAuxiliaryHistograms() {
  const auto auxDirectory = std::filesystem::path(config_m.auxDirectory);
  if (!std::filesystem::exists(auxDirectory)) {
    throw std::runtime_error(
        "stxs::Manager: STXS auxiliary directory does not exist: '" +
        auxDirectory.string() + "'.");
  }

  // Lambda to clone a histogram from a file in the aux directory
  auto own = [&](const std::string &fileName, const std::string &histogramName) {
    auto *histogram =
        cloneHistogram((auxDirectory / fileName).string(), histogramName);
    ownedHistograms_m.push_back(histogram);
    return histogram;
  };

  // Load acceptance histograms from config specs
  for (const auto &spec : config_m.acceptanceSpecs) {
    const auto key = acceptanceKey(spec.processId, spec.channelId);
    acceptanceSets_m.emplace(
        key,
        AcceptanceSet{own(spec.upFile, spec.upHistogram),
                      own(spec.downFile, spec.downHistogram)});
  }

  // Load migration histograms from config specs
  for (const auto &spec : config_m.migrationSpecs) {
    MigrationSet set;
    if (!spec.delta75File.empty()) {
      set.delta75 = own(spec.delta75File, spec.delta75Histogram);
    }
    if (!spec.delta150File.empty()) {
      set.delta150 = own(spec.delta150File, spec.delta150Histogram);
    }
    if (!spec.delta250File.empty()) {
      set.delta250 = own(spec.delta250File, spec.delta250Histogram);
    }
    if (!spec.delta400File.empty()) {
      set.delta400 = own(spec.delta400File, spec.delta400Histogram);
    }
    if (!spec.delta1File.empty()) {
      set.delta1 = own(spec.delta1File, spec.delta1Histogram);
    }
    if (!spec.delta2File.empty()) {
      set.delta2 = own(spec.delta2File, spec.delta2Histogram);
    }
    if (!spec.scaleUpFile.empty()) {
      set.scaleUp = own(spec.scaleUpFile, spec.scaleUpHistogram);
    }
    if (!spec.scaleDownFile.empty()) {
      set.scaleDown = own(spec.scaleDownFile, spec.scaleDownHistogram);
    }
    migrationSets_m.emplace(spec.productionModeId, std::move(set));
  }
}

} // namespace stxs
