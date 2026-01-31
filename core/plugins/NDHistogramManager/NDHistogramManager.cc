#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <api/IPluggableManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <TFile.h>
#include <TH1F.h>
#include <THnSparse.h>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <util.h>
#include <plots.h>
#include <string>
#include <vector>

/**
 * @brief Construct a new NDHistogramManager object
 */
NDHistogramManager::NDHistogramManager(IConfigurationProvider const& configProvider) {}

// Helper function to handle per-axis logic for varVector and DefineVector
static void HandleAxisVarVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    const std::string& variable,
    bool hasSystematic,
    bool hasMultiFill,
    const std::vector<std::string>& allSystematics,
    std::vector<std::string>& varVector,
    bool isWeight=false,
    std::vector<std::string> baseVariables={}) {

    const std::string refVector = baseVariables.empty() ? std::string() : baseVariables.front();
    auto ensureExpandedVector = [&](const std::string& column) -> std::string {
      if (refVector.empty()) {
        return column;
      }
      const std::string colType = df.GetColumnType(column);
      if (colType.find("RVec") != std::string::npos) {
        return column;
      }
      const std::string expandedName = column + "_FillRVec";
      const auto colNames = df.GetColumnNames();
      if (std::find(colNames.begin(), colNames.end(), expandedName) == colNames.end()) {
        const std::string expr = "ROOT::VecOps::RVec<Float_t>(" + refVector + ".size(), static_cast<Float_t>(" + column + "))";
        df = df.Define(expandedName, expr);
        dataManager_m->setDataFrame(df);
      }
      return expandedName;
    };

    if (hasSystematic) {
        std::vector<std::string> systematicVariations;
        for (const auto& syst : allSystematics) {
        std::string columnName = variable;
        if (systematicManager_m->getVariablesForSystematic(syst).count(variable) != 0) {
          columnName = variable + "_" + syst;
            } else {
          columnName = variable;
            }
        if (!refVector.empty()) {
          columnName = ensureExpandedVector(columnName);
        }
        systematicVariations.push_back(columnName);
        }
        const auto columnNames = df.GetColumnNames();
        if (std::find(columnNames.begin(), columnNames.end(), variable + "_systVector") == columnNames.end()) {
            dataManager_m->DefineVector(variable + "_systVector", systematicVariations, "Float_t", *systematicManager_m);
          df = dataManager_m->getDataFrame();
        }
        varVector.push_back(variable + "_systVector");
    } else {
        if (hasMultiFill) {
            varVector.push_back(variable);
        } else {
        if (!refVector.empty()) {
          varVector.push_back(ensureExpandedVector(variable));
        } else {
          const auto columnNames = df.GetColumnNames();
          if (std::find(columnNames.begin(), columnNames.end(), variable + "_RVec") == columnNames.end()) {
            dataManager_m->DefineVector(variable + "_RVec", {variable}, "Float_t", *systematicManager_m);
            df = dataManager_m->getDataFrame();
          }
          varVector.push_back(variable + "_RVec");
        }
        }
    }
}

static std::string buildSystematicVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    const std::vector<std::string>& systList) {
  const std::string baseName = "SystematicCounter";
  std::vector<std::string> systBranches;
  systBranches.reserve(systList.size());

  const auto existingColumns = df.GetColumnNames();
  if (std::find(existingColumns.begin(), existingColumns.end(), baseName) == existingColumns.end()) {
    for (size_t i = 0; i < systList.size(); ++i) {
      const auto &syst = systList[i];
      const std::string branchName = (syst == "Nominal") ? baseName : baseName + "_" + syst;
      const int index = static_cast<int>(i);
      dataManager_m->DefinePerSample(
          branchName,
          [index](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
            return index;
          });
    }
    df = dataManager_m->getDataFrame();
  }

  for (const auto& syst : systList) {
    if (syst == "Nominal") {
      systBranches.push_back(baseName);
    } else {
      systBranches.push_back(baseName + "_" + syst);
    }
  }

  const std::string systVectorName = baseName + "_systVector";
  const auto columnNames = df.GetColumnNames();
  if (std::find(columnNames.begin(), columnNames.end(), systVectorName) == columnNames.end()) {
    dataManager_m->DefineVector(systVectorName, systBranches, "Float_t", *systematicManager_m);
    df = dataManager_m->getDataFrame();
  }
  return systVectorName;
}

/**
 * @brief Book N-dimensional histograms
 * @param infos Vector of histogram info objects
 * @param selection Vector of selection info objects
 * @param suffix Suffix to append to histogram names
 * @param allRegionNames Vector of region name vectors
 */
void NDHistogramManager::BookSingleHistogram(
    histInfo &info, // histogram info (name, bins, lowerBound, upperBound)
    selectionInfo &&sampleCategoryInfo, // selection info (variable, bins, lowerBound, upperBound)
    selectionInfo &&controlRegionInfo, // selection info (variable, bins, lowerBound, upperBound)
    selectionInfo &&channelInfo, // selection info (variable, bins, lowerBound, upperBound)
    std::string suffix){ // suffix to append to histogram names

  // get the dataframe
  if(dataManager_m == nullptr) {
    throw std::runtime_error("NDHistogramManager::BookSingleHistogram: DataManager not set");
  }

  ROOT::RDF::RNode df = dataManager_m->getDataFrame();

  // set the basic information
  histFillInfo fillInfo = histFillInfo();

  fillInfo.name = info.name() + "_" + suffix;
  fillInfo.title = info.name() + " " + suffix;
  fillInfo.nSlots = df.GetNSlots();
  // Systematic axis will be inserted between sampleCategory and the histogram variable.
  fillInfo.nbins = {channelInfo.bins(), controlRegionInfo.bins(), sampleCategoryInfo.bins(), 1, info.bins()};
  fillInfo.xmin = {channelInfo.lowerBound(), controlRegionInfo.lowerBound(), sampleCategoryInfo.lowerBound(), 0.0, info.lowerBound()};
  fillInfo.xmax = {channelInfo.upperBound(), controlRegionInfo.upperBound(), sampleCategoryInfo.upperBound(), 1.0, info.upperBound()};

  // define zero vector if needed
  std::vector<std::string> dfVariableList = df.GetColumnNames();

  if(std::find(dfVariableList.begin(), dfVariableList.end(), "zero__") == dfVariableList.end()) {
    dataManager_m->Define("zero__", []() -> Float_t { return 0.0f; }, {}, *systematicManager_m);
    df = dataManager_m->getDataFrame();
  }


  // determine if the variables are scalars or vectors
  const std::string channelType = df.GetColumnType(channelInfo.variable());
  const std::string controlRegionType = df.GetColumnType(controlRegionInfo.variable());
  const std::string sampleCategoryType = df.GetColumnType(sampleCategoryInfo.variable());

  if(channelType.find("RVec") != std::string::npos) {
    fillInfo.channel_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  if(controlRegionType.find("RVec") != std::string::npos) {
    fillInfo.controlRegion_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  if(sampleCategoryType.find("RVec") != std::string::npos) {
    fillInfo.sampleCategory_hasMultiFill = true; 
    fillInfo.hasMultiFill = true;
  }

  const std::string weightType = df.GetColumnType(info.weight());
  if(weightType.find("RVec") != std::string::npos) {
    fillInfo.weight_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  // determine if the variables have systematic variations
  if(systematicManager_m->getSystematicsForVariable(channelInfo.variable()).size() > 0) {
    fillInfo.channel_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(controlRegionInfo.variable()).size() > 0) {
    fillInfo.controlRegion_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(sampleCategoryInfo.variable()).size() > 0) {
    fillInfo.sampleCategory_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(info.weight()).size() > 0) {
    fillInfo.weight_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  // For now, we don't support multi-fill for systematic variations
  fillInfo.systematic_hasMultiFill = false;
  
  const std::vector<std::string> systList = systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  fillInfo.nbins[3] = static_cast<Int_t>(systList.size());
  fillInfo.xmax[3] = static_cast<Double_t>(systList.size());

  std::vector<std::string> varVector;

  // We're going to fill for each systematic variation so we can reuse a lot of the same input vectors between the different histograms
  // But we will set the weights to 0 for the non-systematic variations
  std::vector<std::string> usedSystematics(systList.begin(), systList.end());

  // Build column vectors in the order expected by THnMulti::Exec:
  // baseValues, baseWeights, systematic, sampleCategory, controlRegion, channel, nFills
  std::vector<std::string> baseValsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, info.variable(), fillInfo.hasSystematic, fillInfo.hasMultiFill, usedSystematics, baseValsVec);
  std::vector<std::string> baseWeightsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, info.weight(), fillInfo.weight_hasSystematic, fillInfo.weight_hasMultiFill, usedSystematics, baseWeightsVec, true, {baseValsVec.front()});

  const std::string systVectorName = buildSystematicVector(df, dataManager_m, systematicManager_m, systList);

  std::vector<std::string> sampleCategoryVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, sampleCategoryInfo.variable(), fillInfo.sampleCategory_hasSystematic, fillInfo.sampleCategory_hasMultiFill, usedSystematics, sampleCategoryVec, false, {baseValsVec.front()});
  std::vector<std::string> controlRegionVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, controlRegionInfo.variable(), fillInfo.controlRegion_hasSystematic, fillInfo.controlRegion_hasMultiFill, usedSystematics, controlRegionVec, false, {baseValsVec.front()});
  std::vector<std::string> channelVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, channelInfo.variable(), fillInfo.channel_hasSystematic, fillInfo.channel_hasMultiFill, usedSystematics, channelVec, false, {baseValsVec.front()});

  const std::string nFillsName = "SystematicCounter_nFills";
  const auto columnNames = df.GetColumnNames();
  if (std::find(columnNames.begin(), columnNames.end(), nFillsName) == columnNames.end()) {
    dataManager_m->Define(
        nFillsName,
        [](const ROOT::VecOps::RVec<Float_t>& baseVals,
           const ROOT::VecOps::RVec<Float_t>& systVals) -> ROOT::VecOps::RVec<Int_t> {
          ROOT::VecOps::RVec<Int_t> out(systVals.size());
          const auto systSize = static_cast<Int_t>(systVals.size());
          Int_t fills = static_cast<Int_t>(baseVals.size());
          if (systSize > 0 && fills % systSize == 0) {
            fills = fills / systSize;
          }
          for (size_t i = 0; i < out.size(); ++i) {
            out[i] = fills;
          }
          return out;
        },
        {baseValsVec.front(), systVectorName},
        *systematicManager_m);
      df = dataManager_m->getDataFrame();
  }

  varVector = {
    baseValsVec.front(),
    baseWeightsVec.front(),
    systVectorName,
    sampleCategoryVec.front(),
    controlRegionVec.front(),
    channelVec.front(),
    nFillsName
  };

  df = dataManager_m->getDataFrame();
  THnMulti tempModel(fillInfo);
    histos_m.push_back(df.Book<
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Int_t>>(
      std::move(tempModel), varVector));
}

void NDHistogramManager::bookND(std::vector<histInfo> &infos,
                                std::vector<selectionInfo> &selection,
                                const std::string &suffix,
                                std::vector<std::vector<std::string>> &allRegionNames) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("NDHistogramManager: DataManager or SystematicManager not set");
  }

  if (allRegionNames.empty()) {
    throw std::invalid_argument("NDHistogramManager: region names must not be empty");
  }

  // Ensure we have channel/control/sample category selections (use defaults if missing)
  selectionInfo channelInfo = selection.size() > 0 ? selection[0] : selectionInfo();
  selectionInfo controlInfo = selection.size() > 1 ? selection[1] : selectionInfo();
  selectionInfo sampleInfo = selection.size() > 2 ? selection[2] : selectionInfo();

  // Append systematic axis info if not already appended
  const std::vector<std::string> systList = systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  if (allRegionNames.empty() || allRegionNames.back() != systList) {
    allRegionNames.emplace_back(systList);
  }

  for (auto &info : infos) {
    BookSingleHistogram(info,
                        selectionInfo(sampleInfo),
                        selectionInfo(controlInfo),
                        selectionInfo(channelInfo),
                        suffix);
  }
}

void NDHistogramManager::saveHists(std::vector<std::vector<histInfo>> &fullHistList,
                                   std::vector<std::vector<std::string>> &allRegionNames) {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigurationManager not set");
  }
  SaveHists(fullHistList, allRegionNames, *configManager_m);
}

void NDHistogramManager::SaveHists(
    std::vector<std::vector<histInfo>> &fullHistList,
    std::vector<std::vector<std::string>> &allRegionNames,
    const IConfigurationProvider &configProvider) {
  std::string fileName = configProvider.get("metaFile");
  if (fileName.empty()) {
    fileName = configProvider.get("saveFile");
  }

  std::vector<std::string> allNames;
  std::vector<std::string> allVariables;
  std::vector<std::string> allLabels;
  std::vector<int> allBins;
  std::vector<float> allLowerBounds;
  std::vector<float> allUpperBounds;
  for (auto const &histList : fullHistList) {
    for (auto const &info : histList) {
      allNames.push_back(info.name());
      allVariables.push_back(info.variable());
      allLabels.push_back(info.label());
      allBins.push_back(info.bins());
      allLowerBounds.push_back(info.lowerBound());
      allUpperBounds.push_back(info.upperBound());
    }
  }

  TFile saveFile(fileName.c_str(), "RECREATE");

  std::vector<Int_t> commonAxisSize = {};
  for (const auto &regionNameList : allRegionNames) {
    commonAxisSize.push_back(regionNameList.size());
  }

  int histIndex = 0;
  std::unordered_map<std::string, TH1F> histMap;
  std::unordered_set<std::string> dirSet;
  for (auto &histo_m : histos_m) {
    auto hist = histo_m.GetPtr();
    const Int_t currentHistogramSize = hist->GetNbins();
    std::vector<Int_t> indices(commonAxisSize.size() + 1);
    for (int i = 0; i < currentHistogramSize; i++) {
      Float_t content = hist->GetBinContent(i, indices.data());
      if (content == 0) {
        continue;
      }
      Float_t error = hist->GetBinError2(i);

      std::string dirName = "";
      const Int_t size = commonAxisSize.size() - 2;
      for (int j = 0; j < size; j++) {
        if (indices[j] > 0 && j < static_cast<Int_t>(allRegionNames.size()) &&
            indices[j] - 1 < static_cast<Int_t>(allRegionNames[j].size())) {
          dirName += allRegionNames[j][indices[j] - 1] + "/";
        }
      }

      if (commonAxisSize.size() - 2 >= 0 &&
          commonAxisSize.size() - 2 < allRegionNames.size() &&
          indices[commonAxisSize.size() - 2] > 0 &&
          indices[commonAxisSize.size() - 2] - 1 < allRegionNames[commonAxisSize.size() - 2].size()) {
        dirName += allRegionNames[commonAxisSize.size() - 2][indices[commonAxisSize.size() - 2] - 1];
      }

      std::string histName = allVariables[histIndex];
      if (commonAxisSize.size() - 1 >= 0 &&
          commonAxisSize.size() - 1 < allRegionNames.size() &&
          indices[commonAxisSize.size() - 1] > 0 &&
          indices[commonAxisSize.size() - 1] - 1 < allRegionNames[commonAxisSize.size() - 1].size()) {
        if (allRegionNames[commonAxisSize.size() - 1][indices[commonAxisSize.size() - 1] - 1] != "Nominal") {
          histName += "_" + allRegionNames[commonAxisSize.size() - 1][indices[commonAxisSize.size() - 1] - 1];
        }
      }

      if (histMap.count(dirName + "/" + histName) == 0) {
        histMap[dirName + "/" + histName] =
            TH1F(histName.c_str(),
                 (allVariables[histIndex] + ";" + allVariables[histIndex] +
                  ";Counts").c_str(),
                 allBins[histIndex], allLowerBounds[histIndex],
                 allUpperBounds[histIndex]);
        dirSet.emplace(dirName);
      }

      histMap[dirName + "/" + histName].SetBinContent(
          indices[commonAxisSize.size()], content);
      histMap[dirName + "/" + histName].SetBinError(
          indices[commonAxisSize.size()], std::sqrt(error));
    }

    histIndex++;
  }

  for (const auto &dirName : dirSet) {
    std::string newDir(dirName);
    if (newDir.find('/') != std::string::npos) {
      newDir[newDir.find('/')] = '_';
    }
    saveFile.cd();
    if (!newDir.empty()) {
      saveFile.mkdir(newDir.c_str());
      saveFile.cd(newDir.c_str());
    }
    for (auto &pair : histMap) {
      if (pair.first.rfind(dirName + "/", 0) == 0) {
        pair.second.Write();
      }
    }
    saveFile.cd();
  }
}



/**
 * @brief Get the vector of histogram result pointers
 * @return Reference to the vector of RResultPtr<THnSparseF>
 */
std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseF>> &
NDHistogramManager::GetHistos() {
  return histos_m;
}

/**
 * @brief Clear all stored histograms
 */
void NDHistogramManager::Clear() { histos_m.clear(); }

/**
 * @brief Setup the manager from a configuration file
 */
void NDHistogramManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigManager not set");
  }
  
  // NDHistogramManager doesn't require specific configuration setup
  // but we need to implement this virtual function for the interface
  // Any specific configuration can be added here if needed in the future
}