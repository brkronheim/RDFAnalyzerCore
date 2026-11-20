#include <RootOutputSink.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <filesystem>
#include <fnmatch.h>
#include <cctype>
#include <iostream>
#include <unordered_set>
#include <algorithm>
#include <utility>
#include <ROOT/RSnapshotOptions.hxx>

namespace {
// ROOT Snapshot rejects an alias and its resolved target in the same list.
// Use the same protected validation routine as Snapshot, through an ordinary
// copied RNode, to obtain canonical names for the implicit full-output case.
// Explicit column lists retain their chosen names and ROOT's duplicate checks.
struct SnapshotColumnView : ROOT::RDF::RNode {
  explicit SnapshotColumnView(ROOT::RDF::RNode node) : ROOT::RDF::RNode(std::move(node)) {}
  std::vector<std::string> canonicalColumns() {
    auto names=GetColumnNames();
    names.erase(std::remove_if(names.begin(),names.end(),[](const std::string& name) {
      return name.rfind("R_rdf_sizeof_",0)==0;
    }),names.end());
    auto resolved=GetValidatedColumnNames(names.size(),names);
    std::unordered_set<std::string> seen;
    std::vector<std::string> unique;unique.reserve(resolved.size());
    for(auto& name:resolved)if(seen.insert(name).second)unique.push_back(std::move(name));
    return unique;
  }
};
}

static std::string makeMetaFileName(const std::string& saveFile) {
  if (saveFile.empty()) {
    return "";
  }
  const auto pos = saveFile.rfind('.');
  if (pos != std::string::npos) {
    return saveFile.substr(0, pos) + "_meta" + saveFile.substr(pos);
  }
  return saveFile + "_meta.root";
}

static bool isGlobPattern(const std::string& val) {
  return val.find('*') != std::string::npos || val.find('?') != std::string::npos;
}

static bool parseBooleanConfig(const std::string& key, const std::string& value,
                               bool defaultValue) {
  if (value.empty()) {
    return defaultValue;
  }

  std::string normalized;
  normalized.reserve(value.size());
  for (const unsigned char character : value) {
    normalized.push_back(static_cast<char>(std::tolower(character)));
  }

  if (normalized == "1" || normalized == "true" || normalized == "yes" ||
      normalized == "on") {
    return true;
  }
  if (normalized == "0" || normalized == "false" || normalized == "no" ||
      normalized == "off") {
    return false;
  }

  throw std::invalid_argument("RootOutputSink: invalid boolean value '" + value +
                              "' for configuration key '" + key + "'.");
}

static std::vector<std::string> parseSaveColumns(const IConfigurationProvider& configProvider,
                                                  const std::vector<std::string>& availableColumns) {
  const auto& configMap = configProvider.getConfigMap();
  auto it = configMap.find("saveConfig");
  if (it == configMap.end()) {
    return {};
  }

  std::vector<std::string> saveVector;
  std::unordered_set<std::string> seen;
  const auto saveVectorInit = configProvider.parseVectorConfig(it->second);

  // Quick check: if there's a single "*" entry, return all columns.
  if (saveVectorInit.size() == 1) {
    std::string single = saveVectorInit[0];
    single = single.substr(0, single.find(" "));
    if (single == "*") {
      saveVector.reserve(availableColumns.size());
      for (const auto& col : availableColumns) {
        if (seen.insert(col).second) {
          saveVector.push_back(col);
        }
      }
      return saveVector;
    }
  }

  for (auto val : saveVectorInit) {
    val = val.substr(0, val.find(" "));
    if (val.empty()) {
      continue;
    }
    if (isGlobPattern(val)) {
      for (const auto& col : availableColumns) {
        if (fnmatch(val.c_str(), col.c_str(), 0) == 0 && seen.insert(col).second) {
          saveVector.push_back(col);
        }
      }
    } else if (seen.insert(val).second) {
      saveVector.push_back(val);
    }
  }
  return saveVector;
}

static void expandSystematicColumns(std::vector<std::string>& columns,
                                    const ISystematicManager* systematicManager) {
  if (!systematicManager) {
    return;
  }

  const unsigned int baseSize = columns.size();
  std::unordered_set<std::string> seen(columns.begin(), columns.end());

  // Pre-allocate to avoid repeated reallocation during the expansion loop.
  size_t extra = 0;
  for (unsigned int i = 0; i < baseSize; ++i) {
    extra += 2 * systematicManager->getSystematicsForVariable(columns[i]).size();
  }
  columns.reserve(baseSize + extra);

  for (unsigned int i = 0; i < baseSize; ++i) {
    const auto appendVariation = [&](const std::string& label) {
      const auto column =
          systematicManager->getVariationColumnName(columns[i], label);
      if (seen.insert(column).second) {
        columns.push_back(column);
      }
    };

    for (const auto& syst : systematicManager->getSystematics()) {
      if (systematicManager->getVariablesForSystematic(syst).count(
              columns[i]) == 0) {
        continue;
      }
      for (const auto* direction : {"Up", "Down"}) {
        appendVariation(syst + direction);
      }
    }

    for (const auto& label :
         systematicManager->getExplicitVariationLabelsForVariable(columns[i])) {
      appendVariation(label);
    }
  }
}

static ROOT::RDF::RSnapshotOptions snapshotOptions(
    const IConfigurationProvider* configProvider = nullptr) {
  ROOT::RDF::RSnapshotOptions options;
  options.fCompressionAlgorithm = ROOT::RCompressionSetting::EAlgorithm::kZSTD;
  options.fCompressionLevel = 5;
  if (!configProvider) return options;
  const auto& config = configProvider->getConfigMap();
  const auto algorithm = config.find("skimCompressionAlgorithm");
  if (algorithm != config.end()) {
    std::string name = algorithm->second;
    for (auto& c : name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    using Algorithm = ROOT::RCompressionSetting::EAlgorithm;
    if (name == "zstd") options.fCompressionAlgorithm = Algorithm::kZSTD;
    else if (name == "lzma") options.fCompressionAlgorithm = Algorithm::kLZMA;
    else if (name == "zlib") options.fCompressionAlgorithm = Algorithm::kZLIB;
    else if (name == "lz4") options.fCompressionAlgorithm = Algorithm::kLZ4;
    else throw std::invalid_argument("RootOutputSink: skimCompressionAlgorithm must be zstd, lzma, zlib or lz4");
  }
  const auto level = config.find("skimCompressionLevel");
  if (level != config.end()) {
    try {
      size_t parsed = 0;
      const int value = std::stoi(level->second, &parsed);
      if (parsed != level->second.size() || value < 0 || value > 9)
        throw std::invalid_argument("invalid level");
      options.fCompressionLevel = value;
    } catch (const std::exception&) {
      throw std::invalid_argument("RootOutputSink: skimCompressionLevel must be an integer from 0 to 9");
    }
  }
  return options;
}

static void writeSnapshot(ROOT::RDF::RNode& df, const OutputSpec& spec,
                          const ROOT::RDF::RSnapshotOptions& options) {
  if (spec.outputFile.empty()) {
    throw std::runtime_error("RootOutputSink: outputFile is empty");
  }
  if (spec.treeName.empty()) {
    throw std::runtime_error("RootOutputSink: treeName is empty");
  }

  std::cout << "Executing Snapshot" << std::endl;
  std::cout << "Tree: " << spec.treeName << std::endl;
  std::cout << "SaveFile: " << spec.outputFile << std::endl;

  const std::filesystem::path outputPath(spec.outputFile);
  if (outputPath.has_parent_path()) {
    std::filesystem::create_directories(outputPath.parent_path());
  }

  if (spec.columns.empty()) {
    const auto columns=SnapshotColumnView(df).canonicalColumns();
    df.Snapshot(spec.treeName, spec.outputFile, columns, options);
  } else {
    df.Snapshot(spec.treeName, spec.outputFile, spec.columns, options);
  }

  std::cout << "Done Saving" << std::endl;
}

void RootOutputSink::writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) {
  writeSnapshot(df, spec, snapshotOptions());
}

void RootOutputSink::writeDataFrame(ROOT::RDF::RNode& df,
                                    const IConfigurationProvider& configProvider,
                                    const ISystematicManager* systematicManager,
                                    OutputChannel channel) {
  const auto& configMap = configProvider.getConfigMap();

  const std::string saveTree = configProvider.get("saveTree");
  if (saveTree.empty()) {
    throw std::runtime_error("RootOutputSink: saveTree is empty");
  }

  std::string outputFile = resolveOutputFile(configProvider, channel);
  if (outputFile.empty()) {
    throw std::runtime_error("RootOutputSink: outputFile is empty");
  }

  // Resolve columns while the cache is cold. Full snapshots also canonicalize
  // aliases; subsequent writes reuse the cached column list.
  if (!columnsCached_) {
    cachedColumns_ = parseSaveColumns(configProvider, df.GetColumnNames());
    const auto save=configMap.find("saveConfig");
    if(save!=configMap.end()) {
      const auto patterns=configProvider.parseVectorConfig(save->second);
      if(patterns.size()==1 && patterns.front().substr(0,patterns.front().find(' '))=="*")
        cachedColumns_=SnapshotColumnView(df).canonicalColumns();
    }
    const auto skimSystematicsIt = configMap.find("skimSystematics");
    const bool includeSystematics =
        channel == OutputChannel::Skim &&
        skimSystematicsIt != configMap.end() &&
        parseBooleanConfig("skimSystematics", skimSystematicsIt->second, false);
    if (includeSystematics) {
      expandSystematicColumns(cachedColumns_, systematicManager);
    }
    columnsCached_ = true;
  }

  if (cachedColumns_.empty() && configMap.find("saveConfig") == configMap.end()) {
    std::cout << "Warning: No 'saveConfig' provided. Snapshotting full dataframe." << std::endl;
  }

  OutputSpec spec{outputFile, saveTree, cachedColumns_};
  writeSnapshot(df, spec, snapshotOptions(channel == OutputChannel::Skim ? &configProvider : nullptr));
}

std::string RootOutputSink::resolveOutputFile(const IConfigurationProvider& configProvider,
                                              OutputChannel channel) {
  // Return cached path if already resolved for this channel.
  if (channel == OutputChannel::Skim && !cachedSkimFile_.empty()) {
    return cachedSkimFile_;
  }
  if (channel == OutputChannel::Meta && !cachedMetaFile_.empty()) {
    return cachedMetaFile_;
  }

  std::string result;
  if (channel == OutputChannel::Meta) {
    std::string metaFile = configProvider.get("metaFile");
    if (!metaFile.empty()) {
      result = metaFile;
    } else {
      const std::string saveFile = configProvider.get("saveFile");
      result = makeMetaFileName(saveFile);
    }
    cachedMetaFile_ = result;
  } else {
    result = configProvider.get("saveFile");
    cachedSkimFile_ = result;
  }
  return result;
}
