#include <RootOutputSink.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <filesystem>
#include <fnmatch.h>
#include <iostream>
#include <unordered_set>
#include <ROOT/RSnapshotOptions.hxx>

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
  // Pre-allocate to avoid repeated reallocation during the expansion loop.
  size_t extra = 0;
  for (unsigned int i = 0; i < baseSize; ++i) {
    extra += 2 * systematicManager->getSystematicsForVariable(columns[i]).size();
  }
  columns.reserve(baseSize + extra);

  for (unsigned int i = 0; i < baseSize; ++i) {
    const auto& systs = systematicManager->getSystematicsForVariable(columns[i]);
    for (const auto& syst : systs) {
      columns.push_back(columns[i] + "_" + syst + "Up");
      columns.push_back(columns[i] + "_" + syst + "Down");
    }
  }
}

void RootOutputSink::writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) {
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

  ROOT::RDF::RSnapshotOptions options;
  options.fCompressionAlgorithm = ROOT::RCompressionSetting::EAlgorithm::kZSTD;
  options.fCompressionLevel = 5;  // 505 ZSTD compression level, good balance between speed and size

  if (spec.columns.empty()) {
    df.Snapshot(spec.treeName, spec.outputFile, ".*", options);
  } else {
    df.Snapshot(spec.treeName, spec.outputFile, spec.columns, options);
  }

  std::cout << "Done Saving" << std::endl;
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

  // Resolve and cache columns once.  GetColumnNames() is called only when
  // the cache is cold, which in practice means exactly once per sink lifetime.
  if (!columnsCached_) {
    cachedColumns_ = parseSaveColumns(configProvider, df.GetColumnNames());
    expandSystematicColumns(cachedColumns_, systematicManager);
    columnsCached_ = true;
  }

  if (cachedColumns_.empty() && configMap.find("saveConfig") == configMap.end()) {
    std::cout << "Warning: No 'saveConfig' provided. Snapshotting full dataframe." << std::endl;
  }

  OutputSpec spec{outputFile, saveTree, cachedColumns_};
  writeDataFrame(df, spec);
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
