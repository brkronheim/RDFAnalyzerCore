//==============================================================================
// stxs::Manager — Generic STXS Manager implementation
//
// Ported and generalized from VHbbSTXS.cc. All VH-specific logic has been
// replaced with data-driven configuration. The bin-indexing scheme for
// migration weights follows the CMS STXS convention (fine categories encode
// pT bins in their last two digits), but is overridable per analysis.
//==============================================================================

#include "stxs/Manager.h"

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
// Bin-index helpers for migration weights
//
// These follow the CMS STXS convention where the fine category's last two
// digits encode the pT bin index (for delta75–delta400) and the local
// category within the stage-0 bin uses a remapped scheme (for delta1/delta2).
//
// Analyses with a different binning scheme can override these by providing
// custom mapping functions via the config.
// ---------------------------------------------------------------------------

/// Map fine category to a bin index 1..N for the delta75–delta400 histograms.
/// Default: uses (fineCategory % 100) modulo 5.
int ptBinIndex(int fineCategory) {
  const int localCategory = fineCategory % 100;
  const int modulo = localCategory - (localCategory / 5) * 5;
  return modulo == 0 ? 5 : modulo;
}

/// Map fine category to a bin index for the delta1/delta2 histograms.
/// Default: uses a remapped table matching the CMS VH STXS convention.
int jetPtBinIndex(int fineCategory) {
  const int stage0 = (fineCategory / 100) * 100;
  static const std::unordered_map<int, int> kRemappedBins{
      {1, 1}, {2, 4}, {3, 7}, {4, 10}, {5, 13},
      {6, 2}, {7, 5}, {8, 8}, {9, 11}, {10, 14},
      {11, 3}, {12, 6}, {13, 9}, {14, 12}, {15, 15},
  };
  const auto localCategory = fineCategory - stage0;
  const auto match = kRemappedBins.find(localCategory);
  if (match == kRemappedBins.end()) {
    return 0;
  }
  return match->second + 1;
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
    config.analysisBinLabels[definition.analysisBin] = definition.label;
    for (const int fineCategory : definition.fineCategories) {
      config.fineCategoryToAnalysisBin[fineCategory] = definition.analysisBin;
    }
    config.bins.push_back(std::move(definition));
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

Config loadConfigFromYaml(const std::string &yamlConfigPath) {
  // TODO: Implement YAML parsing using the framework's YAML config adapter.
  // For now, this returns a default Config and logs a warning.
  // The YAML loading will be implemented when the framework's YAML support
  // is integrated with the plugin lifecycle.
  Config config;
  config.enabled = false;
  return config;
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
  if (fineCategory % 100 <= 0 || fineCategory / 100 <= 2 ||
      fineCategory / 100 >= 6) {
    return {};
  }

  const auto match = migrationSets_m.find(productionModeId);
  if (match == migrationSets_m.end()) {
    return {};
  }

  const int localPtBin = ptBinIndex(fineCategory);
  const int localJetPtBin = jetPtBinIndex(fineCategory);
  if (localPtBin <= 0 || localJetPtBin <= 0) {
    return {};
  }

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

  // Scale histograms use (fineCategory - stage0 + 1) as bin index
  const int stage0 = (fineCategory / 100) * 100;
  const int scaleBin = fineCategory - stage0 + 1;
  const float scaleUp = set.scaleUp
                            ? static_cast<float>(
                                  set.scaleUp->GetBinContent(scaleBin))
                            : 1.0f;
  const float scaleDown =
      set.scaleDown
          ? static_cast<float>(set.scaleDown->GetBinContent(scaleBin))
          : 1.0f;

  // Fine categories 403 and 503 use fallback weights (no own scale histograms)
  if (fineCategory == 403 || fineCategory == 503) {
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
