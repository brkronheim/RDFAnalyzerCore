#include <RootOutputSink.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <iostream>

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

static std::vector<std::string> parseSaveColumns(const IConfigurationProvider& configProvider) {
  const auto& configMap = configProvider.getConfigMap();
  auto it = configMap.find("saveConfig");
  if (it == configMap.end()) {
    return {};
  }

  std::vector<std::string> saveVector;
  const auto saveVectorInit = configProvider.parseVectorConfig(it->second);
  for (auto val : saveVectorInit) {
    val = val.substr(0, val.find(" "));
    if (!val.empty()) {
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

  if (spec.columns.empty()) {
    df.Snapshot(spec.treeName, spec.outputFile);
  } else {
    df.Snapshot(spec.treeName, spec.outputFile, spec.columns);
  }

  std::cout << "Done Saving" << std::endl;
}

void RootOutputSink::writeDataFrame(ROOT::RDF::RNode& df,
                                    const IConfigurationProvider& configProvider,
                                    const IDataFrameProvider*,
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

  std::vector<std::string> columns = parseSaveColumns(configProvider);
  expandSystematicColumns(columns, systematicManager);

  if (columns.empty() && configMap.find("saveConfig") == configMap.end()) {
    std::cout << "Warning: No 'saveConfig' provided. Snapshotting full dataframe." << std::endl;
  }

  OutputSpec spec{outputFile, saveTree, columns};
  writeDataFrame(df, spec);
}

std::string RootOutputSink::resolveOutputFile(const IConfigurationProvider& configProvider,
                                              OutputChannel channel) {
  if (channel == OutputChannel::Meta) {
    std::string metaFile = configProvider.get("metaFile");
    if (!metaFile.empty()) {
      return metaFile;
    }
    const std::string saveFile = configProvider.get("saveFile");
    return makeMetaFileName(saveFile);
  }
  return configProvider.get("saveFile");
}
