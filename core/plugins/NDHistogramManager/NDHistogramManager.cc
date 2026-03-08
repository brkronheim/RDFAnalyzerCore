#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <api/IPluggableManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <CounterService.h>
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
#include <sstream>

/**
 * @brief Construct a new NDHistogramManager object
 */
NDHistogramManager::NDHistogramManager(IConfigurationProvider const& configProvider) {}

struct ColumnCache {
  ROOT::RDF::RNode& df;
  std::vector<std::string> names;
  std::unordered_map<std::string, std::string> types;

  explicit ColumnCache(ROOT::RDF::RNode& node)
      : df(node), names(node.GetColumnNames()) {}

  bool Has(const std::string& name) const {
    return std::find(names.begin(), names.end(), name) != names.end();
  }

  void Refresh() {
    names = df.GetColumnNames();
    types.clear();
  }

  std::string GetType(const std::string& name) {
    auto it = types.find(name);
    if (it != types.end()) {
      return it->second;
    }
    auto type = df.GetColumnType(name);
    types.emplace(name, type);
    return type;
  }
};

// Helper function to handle per-axis logic for varVector and DefineVector
static void HandleAxisVarVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    ColumnCache& cache,
    const std::string& variable,
    bool hasSystematic,
    bool hasMultiFill,
    const std::vector<std::string>& allSystematics,
    std::vector<std::string>& varVector,
    bool isWeight=false,
    std::vector<std::string> baseVariables={},
    const std::string& uniqueTag=std::string()) {

    const std::string refVector = baseVariables.empty() ? std::string() : baseVariables.front();
    const std::string tagSuffix = uniqueTag.empty() ? std::string() : "_" + uniqueTag;
    auto ensureExpandedVector = [&](const std::string& column) -> std::string {
      if (refVector.empty()) {
        return column;
      }
      const std::string colType = cache.GetType(column);
      if (colType.find("RVec") != std::string::npos) {
        return column;
      }
      const std::string expandedName = column + "_FillRVec_" + refVector + tagSuffix;
      if (!cache.Has(expandedName)) {
        const std::string expr = "ROOT::VecOps::RVec<Float_t>(" + refVector + ".size(), static_cast<Float_t>(" + column + "))";
        df = df.Define(expandedName, expr);
        dataManager_m->setDataFrame(df);
        cache.Refresh();
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
        const std::string systVectorName = variable + "_systVector" + tagSuffix;
        if (!cache.Has(systVectorName)) {
            dataManager_m->DefineVector(systVectorName, systematicVariations, "Float_t", *systematicManager_m);
          df = dataManager_m->getDataFrame();
          cache.Refresh();
        }
        varVector.push_back(systVectorName);
    } else {
        if (hasMultiFill) {
            varVector.push_back(variable);
        } else {
        if (!refVector.empty()) {
          varVector.push_back(ensureExpandedVector(variable));
        } else {
          const std::string rvecName = variable + "_RVec" + tagSuffix;
          if (!cache.Has(rvecName)) {
            dataManager_m->DefineVector(rvecName, {variable}, "Float_t", *systematicManager_m);
            df = dataManager_m->getDataFrame();
            cache.Refresh();
          }
          varVector.push_back(rvecName);
        }
        }
    }
}

static std::string buildSystematicVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    const std::vector<std::string>& systList,
    ColumnCache& cache) {
  const std::string baseName = "SystematicCounter";
  std::vector<std::string> systBranches;
  systBranches.reserve(systList.size());

  bool definedAny = false;
  for (size_t i = 0; i < systList.size(); ++i) {
    const auto &syst = systList[i];
    const std::string branchName = (syst == "Nominal") ? baseName : baseName + "_" + syst;
    if (!cache.Has(branchName)) {
      const int index = static_cast<int>(i);
      dataManager_m->DefinePerSample(
          branchName,
          [index](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
            return index;
          });
      definedAny = true;
    }
  }
  if (definedAny) {
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }

  for (const auto& syst : systList) {
    if (syst == "Nominal") {
      systBranches.push_back(baseName);
    } else {
      systBranches.push_back(baseName + "_" + syst);
    }
  }

  const std::string systVectorName = baseName + "_systVector";
  if (!cache.Has(systVectorName)) {
    dataManager_m->DefineVector(systVectorName, systBranches, "Float_t", *systematicManager_m);
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }
  return systVectorName;
}

static bool HasSystematicColumns(
    ColumnCache& cache,
    ISystematicManager* systematicManager_m,
    const std::string& variable,
    const std::vector<std::string>& systList) {
  for (const auto& syst : systList) {
    if (syst == "Nominal") {
      continue;
    }
    if (systematicManager_m->getVariablesForSystematic(syst).count(variable) == 0) {
      continue;
    }
    const std::string columnName = variable + "_" + syst;
    if (cache.Has(columnName)) {
      return true;
    }
  }
  return false;
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

  if (dataManager_m == nullptr) {
    throw std::runtime_error("NDHistogramManager::BookSingleHistogram: DataManager not set");
  }

  const std::vector<std::string> systList =
      systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  BookSingleHistogramWithSystList(info,
                                  std::move(sampleCategoryInfo),
                                  std::move(controlRegionInfo),
                                  std::move(channelInfo),
                                  std::move(suffix),
                                  systList);
}

void NDHistogramManager::BookSingleHistogramWithSystList(
    histInfo &info,
    selectionInfo &&sampleCategoryInfo,
    selectionInfo &&controlRegionInfo,
    selectionInfo &&channelInfo,
    std::string suffix,
    const std::vector<std::string> &systList) {

  // get the dataframe
  if(dataManager_m == nullptr) {
    throw std::runtime_error("NDHistogramManager::BookSingleHistogram: DataManager not set");
  }

  ROOT::RDF::RNode df = dataManager_m->getDataFrame();
  ColumnCache cache(df);

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
  if(!cache.Has("zero__")) {
    dataManager_m->Define("zero__", []() -> Float_t { return 0.0f; }, {}, *systematicManager_m);
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }


  // determine if the variables are scalars or vectors
  const std::string channelType = cache.GetType(channelInfo.variable());
  const std::string controlRegionType = cache.GetType(controlRegionInfo.variable());
  const std::string sampleCategoryType = cache.GetType(sampleCategoryInfo.variable());

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

  const std::string weightType = cache.GetType(info.weight());
  if(weightType.find("RVec") != std::string::npos) {
    fillInfo.weight_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  // determine if the variables have systematic variations
  if(systematicManager_m->getSystematicsForVariable(channelInfo.variable()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, channelInfo.variable(), systList)) {
    fillInfo.channel_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(controlRegionInfo.variable()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, controlRegionInfo.variable(), systList)) {
    fillInfo.controlRegion_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(sampleCategoryInfo.variable()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, sampleCategoryInfo.variable(), systList)) {
    fillInfo.sampleCategory_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(info.weight()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, info.weight(), systList)) {
    fillInfo.weight_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  // For now, we don't support multi-fill for systematic variations
  fillInfo.systematic_hasMultiFill = false;
  
  fillInfo.nbins[3] = static_cast<Int_t>(systList.size());
  fillInfo.xmax[3] = static_cast<Double_t>(systList.size());

  std::vector<std::string> varVector;
  std::string uniqueTag = fillInfo.name;
  std::replace(uniqueTag.begin(), uniqueTag.end(), ' ', '_');

  // We're going to fill for each systematic variation so we can reuse a lot of the same input vectors between the different histograms
  // But we will set the weights to 0 for the non-systematic variations
  std::vector<std::string> usedSystematics(systList.begin(), systList.end());

  // Build column vectors in the order expected by THnMulti::Exec:
  // baseValues, baseWeights, systematic, sampleCategory, controlRegion, channel, nFills
  std::vector<std::string> baseValsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, info.variable(), fillInfo.hasSystematic, fillInfo.hasMultiFill, usedSystematics, baseValsVec, false, {}, uniqueTag);
  std::vector<std::string> baseWeightsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, info.weight(), fillInfo.weight_hasSystematic, fillInfo.weight_hasMultiFill, usedSystematics, baseWeightsVec, true, {baseValsVec.front()}, uniqueTag);

  const std::string systVectorName = buildSystematicVector(df, dataManager_m, systematicManager_m, systList, cache);

  std::vector<std::string> sampleCategoryVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, sampleCategoryInfo.variable(), fillInfo.sampleCategory_hasSystematic, fillInfo.sampleCategory_hasMultiFill, usedSystematics, sampleCategoryVec, false, {baseValsVec.front()}, uniqueTag);
  std::vector<std::string> controlRegionVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, controlRegionInfo.variable(), fillInfo.controlRegion_hasSystematic, fillInfo.controlRegion_hasMultiFill, usedSystematics, controlRegionVec, false, {baseValsVec.front()}, uniqueTag);
  std::vector<std::string> channelVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, channelInfo.variable(), fillInfo.channel_hasSystematic, fillInfo.channel_hasMultiFill, usedSystematics, channelVec, false, {baseValsVec.front()}, uniqueTag);

  const std::string nFillsName = "SystematicCounter_nFills_" + baseValsVec.front() + "_" + uniqueTag;
  if (!cache.Has(nFillsName)) {
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
      cache.Refresh();
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

  if (std::getenv("RDF_NDHIST_DEBUG") != nullptr) {
    std::cout << "[NDHistogramManager] Book " << fillInfo.name << " columns: ";
    for (const auto& name : varVector) {
      std::cout << name << " ";
    }
    std::cout << std::endl;
  }

  df = dataManager_m->getDataFrame();
  if (histogramBackend_m == "boost") {
    BHnMulti tempModel(fillInfo);
    histos_m.push_back(df.Book<
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Int_t>>(
      std::move(tempModel), varVector));
  } else {
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
  histNodes_m.push_back(df);
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

  if (infos.empty()) {
    return;
  }

  // Ensure we have channel/control/sample category selections (use defaults if missing)
  selectionInfo channelInfo = selection.size() > 0 ? selection[0] : selectionInfo();
  selectionInfo controlInfo = selection.size() > 1 ? selection[1] : selectionInfo();
  selectionInfo sampleInfo = selection.size() > 2 ? selection[2] : selectionInfo();

  // Normalize region names to match axes: channel, control, sample, systematic
  std::vector<std::vector<std::string>> normalizedRegionNames;
  normalizedRegionNames.reserve(4);
  normalizedRegionNames.emplace_back(allRegionNames.size() > 0 ? allRegionNames[0] : channelInfo.regions());
  normalizedRegionNames.emplace_back(allRegionNames.size() > 1 ? allRegionNames[1] : controlInfo.regions());
  normalizedRegionNames.emplace_back(allRegionNames.size() > 2 ? allRegionNames[2] : sampleInfo.regions());

  // Append systematic axis info if not already appended
  const std::vector<std::string> systList = systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  if (allRegionNames.size() > 3) {
    for (size_t i = 3; i < allRegionNames.size(); ++i) {
      normalizedRegionNames.emplace_back(allRegionNames[i]);
    }
  }
  if (normalizedRegionNames.empty() || normalizedRegionNames.back() != systList) {
    normalizedRegionNames.emplace_back(systList);
  }
  allRegionNames = normalizedRegionNames;

  // Track booked infos and region names for the no-args saveHists() overload.
  trackedHistInfos_m.push_back(infos);
  trackedRegionNames_m = normalizedRegionNames;

  histos_m.reserve(histos_m.size() + infos.size());

  for (auto &info : infos) {
    BookSingleHistogramWithSystList(info,
                                    selectionInfo(sampleInfo),
                                    selectionInfo(controlInfo),
                                    selectionInfo(channelInfo),
                                    suffix,
                                    systList);
  }
}

void NDHistogramManager::saveHists() {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigurationManager not set");
  }
  if (trackedHistInfos_m.empty()) {
    return;
  }
  SaveHists(trackedHistInfos_m, trackedRegionNames_m, *configManager_m);

  if (countersFinalized_m || !logger_m || !skimSink_m || !metaSink_m) {
    return;
  }
  const auto& configMap = configManager_m->getConfigMap();
  auto cit = configMap.find("enableCounters");
  if (cit == configMap.end()) { return; }
  const auto& val = cit->second;
  if (val == "1" || val == "true" || val == "True") {
    countersFinalized_m = true;
  }
}

void NDHistogramManager::saveHists(std::vector<std::vector<histInfo>> &fullHistList,
                                   std::vector<std::vector<std::string>> &allRegionNames) {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigurationManager not set");
  }
  SaveHists(fullHistList, allRegionNames, *configManager_m);

  if (countersFinalized_m || !logger_m || !skimSink_m || !metaSink_m) {
    return;
  }

  const auto& configMap = configManager_m->getConfigMap();
  auto it = configMap.find("enableCounters");
  if (it == configMap.end()) {
    return;
  }
  const auto& val = it->second;
  const bool enabled = (val == "1" || val == "true" || val == "True");
  if (!enabled) {
    return;
  }

  // Counters are enabled globally and managed by Analyzer. Do not run a local
  // CounterService here (that would cause duplicate counting); mark counters
  // as finalized for this manager so subsequent calls are no-ops.
  countersFinalized_m = true;
}

void NDHistogramManager::SaveHists(
    std::vector<std::vector<histInfo>> &fullHistList,
    std::vector<std::vector<std::string>> &allRegionNames,
    const IConfigurationProvider &configProvider) {
  std::string fileName;
  if (metaSink_m) {
    fileName = metaSink_m->resolveOutputFile(configProvider, OutputChannel::Meta);
  }
  if (fileName.empty()) {
    fileName = configProvider.get("metaFile");
    if (fileName.empty()) {
      fileName = configProvider.get("saveFile");
    }
  }

  std::vector<std::string> allNames;
  std::vector<std::string> allVariables;
  std::vector<std::string> allLabels;
  std::vector<int> allBins;
  std::vector<float> allLowerBounds;
  std::vector<float> allUpperBounds;
  for (auto const &histList : fullHistList) {
    for (auto const &info : histList) {
      //std::cout << "Storing histogram: " << info.name() << std::endl;
      allNames.push_back(info.name());
      allVariables.push_back(info.variable());
      allLabels.push_back(info.label());
      allBins.push_back(info.bins());
      allLowerBounds.push_back(info.lowerBound());
      allUpperBounds.push_back(info.upperBound());
    }
  }

  // Open the meta file for update so we don't clobber histograms (e.g. counters)
  // written by Analyzer/CounterService earlier.
  TFile saveFile(fileName.c_str(), "UPDATE");

  std::vector<Int_t> commonAxisSize = {};
  for (const auto &regionNameList : allRegionNames) {
    commonAxisSize.push_back(regionNameList.size());
  }

  int histIndex = 0;
  std::unordered_map<std::string, TH1F> histMap;
  std::unordered_set<std::string> dirSet;
  std::cout << "Processing " << histos_m.size() << " histograms for saving..." << std::endl;
  for (auto &histo_m : histos_m) {
    auto hist = histo_m.GetPtr();
    const Int_t currentHistogramSize = hist->GetNbins();
    const Int_t dim = hist->GetNdimensions();
    std::vector<Int_t> indices(dim);
      
    std::string histName = allNames[histIndex];
    //std::cout << "Processing histogram name: " << histName << std::endl;
    for (int i = 0; i < currentHistogramSize; i++) {
      Float_t content = hist->GetBinContent(i, indices.data());
      if (content == 0) {
        continue;
      }
      Float_t error = hist->GetBinError2(i);

      std::string dirName = "";
      const Int_t regionAxes = std::min(static_cast<Int_t>(allRegionNames.size()), dim - 1);
      const Int_t size = regionAxes - 2;
      for (int j = 0; j < size; j++) {
        if (indices[j] > 0 && j < static_cast<Int_t>(allRegionNames.size()) &&
            indices[j] - 1 < static_cast<Int_t>(allRegionNames[j].size())) {
          dirName += allRegionNames[j][indices[j] - 1] + "/";
        }
      }

      if (regionAxes - 2 >= 0 &&
          regionAxes - 2 < static_cast<Int_t>(allRegionNames.size()) &&
          indices[regionAxes - 2] > 0 &&
          indices[regionAxes - 2] - 1 < static_cast<Int_t>(allRegionNames[regionAxes - 2].size())) {
        dirName += allRegionNames[regionAxes - 2][indices[regionAxes - 2] - 1];
      }

      
      
      if (regionAxes - 1 >= 0 &&
          regionAxes - 1 < static_cast<Int_t>(allRegionNames.size()) &&
          indices[regionAxes - 1] > 0 &&
          indices[regionAxes - 1] - 1 < static_cast<Int_t>(allRegionNames[regionAxes - 1].size())) {
        if (allRegionNames[regionAxes - 1][indices[regionAxes - 1] - 1] != "Nominal") {
          histName += "_" + allRegionNames[regionAxes - 1][indices[regionAxes - 1] - 1];
        }
      }

      if (histMap.count(dirName + "/" + histName) == 0) {
        // std::cout << "Booking histogram: " << dirName + "/" + histName << std::endl;
        histMap[dirName + "/" + histName] =
            TH1F(histName.c_str(),
                 (allNames[histIndex] + ";" + allNames[histIndex] +
                  ";Counts").c_str(),
                 allBins[histIndex], allLowerBounds[histIndex],
                 allUpperBounds[histIndex]);
        dirSet.emplace(dirName);
      }

      const Int_t valueAxisIndex = regionAxes;
      if (valueAxisIndex < dim) {
        // std::cout << "Booking histogram: " << dirName + "/" + histName << std::endl;
        histMap[dirName + "/" + histName].SetBinContent(
          indices[valueAxisIndex], content);
        histMap[dirName + "/" + histName].SetBinError(
          indices[valueAxisIndex], std::sqrt(error));
        }
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
      //std::cout << "Saving histogram: " << pair.first << std::endl;
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
void NDHistogramManager::Clear() {
  histos_m.clear();
  histNodes_m.clear();
}

/**
 * @brief Setup the manager from a configuration file
 */
void NDHistogramManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigManager not set");
  }

  // Read optional histogram backend selection (default: "root", option: "boost")
  const std::string backend = configManager_m->get("histogramBackend");
  if (!backend.empty()) {
    if (backend != "root" && backend != "boost") {
      throw std::runtime_error(
          "NDHistogramManager: invalid histogramBackend '" + backend +
          "'. Valid values are 'root' (default) or 'boost'.");
    }
    histogramBackend_m = backend;
  }

  // Parse histogram configuration if present
  std::string histogramConfigFile = configManager_m->get("histogramConfig");
  if (histogramConfigFile.empty()) {
    // No histogram config file specified - this is fine
    if (logger_m) {
      logger_m->log(ILogger::Level::Info, "NDHistogramManager: No histogramConfig specified, config-driven histograms disabled");
    }
    return;
  }

  try {
    // Parse the histogram configuration file
    // Required keys: name, variable, weight, bins, lowerBound, upperBound
    // Optional keys: label, suffix, channelVariable, channelBins, channelLowerBound, channelUpperBound, 
    //                channelRegions, controlRegionVariable, controlRegionBins, controlRegionLowerBound,
    //                controlRegionUpperBound, controlRegionRegions, sampleCategoryVariable, 
    //                sampleCategoryBins, sampleCategoryLowerBound, sampleCategoryUpperBound, sampleCategoryRegions
    auto histogramEntries = configManager_m->parseMultiKeyConfig(
        histogramConfigFile,
        {"name", "variable", "weight", "bins", "lowerBound", "upperBound"});

    configHistograms_m.reserve(histogramEntries.size());

    for (const auto &entry : histogramEntries) {
      HistogramConfig config;
      config.name = entry.at("name");
      config.variable = entry.at("variable");
      config.weight = entry.at("weight");
      config.bins = std::stoi(entry.at("bins"));
      config.lowerBound = std::stof(entry.at("lowerBound"));
      config.upperBound = std::stof(entry.at("upperBound"));

      // Optional fields
      auto labelIt = entry.find("label");
      config.label = (labelIt != entry.end()) ? labelIt->second : config.variable;

      auto suffixIt = entry.find("suffix");
      config.suffix = (suffixIt != entry.end()) ? suffixIt->second : "";

      // Channel selection info
      auto channelVarIt = entry.find("channelVariable");
      if (channelVarIt != entry.end()) {
        try {
          config.channelVariable = channelVarIt->second;
          config.channelBins = std::stoi(entry.at("channelBins"));
          config.channelLowerBound = std::stof(entry.at("channelLowerBound"));
          config.channelUpperBound = std::stof(entry.at("channelUpperBound"));
          
          auto channelRegionsIt = entry.find("channelRegions");
          if (channelRegionsIt != entry.end()) {
            config.channelRegions = configManager_m->splitString(channelRegionsIt->second, ",");
          }
        } catch (const std::exception &e) {
          throw std::runtime_error("NDHistogramManager: Error parsing channel config for histogram '" + 
                                 config.name + "': " + e.what());
        }
      } else {
        // Use default "zero__" variable for no channel selection
        config.channelVariable = "zero__";
        config.channelBins = 1;
        config.channelLowerBound = 0.0;
        config.channelUpperBound = 1.0;
        config.channelRegions = {"Default"};
      }

      // Control region selection info
      auto controlRegionVarIt = entry.find("controlRegionVariable");
      if (controlRegionVarIt != entry.end()) {
        try {
          config.controlRegionVariable = controlRegionVarIt->second;
          config.controlRegionBins = std::stoi(entry.at("controlRegionBins"));
          config.controlRegionLowerBound = std::stof(entry.at("controlRegionLowerBound"));
          config.controlRegionUpperBound = std::stof(entry.at("controlRegionUpperBound"));
          
          auto controlRegionRegionsIt = entry.find("controlRegionRegions");
          if (controlRegionRegionsIt != entry.end()) {
            config.controlRegionRegions = configManager_m->splitString(controlRegionRegionsIt->second, ",");
          }
        } catch (const std::exception &e) {
          throw std::runtime_error("NDHistogramManager: Error parsing control region config for histogram '" + 
                                 config.name + "': " + e.what());
        }
      } else {
        // Use default "zero__" variable for no control region selection
        config.controlRegionVariable = "zero__";
        config.controlRegionBins = 1;
        config.controlRegionLowerBound = 0.0;
        config.controlRegionUpperBound = 1.0;
        config.controlRegionRegions = {"Default"};
      }

      // Sample category selection info
      auto sampleCategoryVarIt = entry.find("sampleCategoryVariable");
      if (sampleCategoryVarIt != entry.end()) {
        try {
          config.sampleCategoryVariable = sampleCategoryVarIt->second;
          config.sampleCategoryBins = std::stoi(entry.at("sampleCategoryBins"));
          config.sampleCategoryLowerBound = std::stof(entry.at("sampleCategoryLowerBound"));
          config.sampleCategoryUpperBound = std::stof(entry.at("sampleCategoryUpperBound"));
          
          auto sampleCategoryRegionsIt = entry.find("sampleCategoryRegions");
          if (sampleCategoryRegionsIt != entry.end()) {
            config.sampleCategoryRegions = configManager_m->splitString(sampleCategoryRegionsIt->second, ",");
          }
        } catch (const std::exception &e) {
          throw std::runtime_error("NDHistogramManager: Error parsing sample category config for histogram '" + 
                                 config.name + "': " + e.what());
        }
      } else {
        // Use default "zero__" variable for no sample category selection
        config.sampleCategoryVariable = "zero__";
        config.sampleCategoryBins = 1;
        config.sampleCategoryLowerBound = 0.0;
        config.sampleCategoryUpperBound = 1.0;
        config.sampleCategoryRegions = {"Default"};
      }

      configHistograms_m.push_back(config);
    }

    if (logger_m) {
      std::stringstream msg;
      msg << "NDHistogramManager: Loaded " << configHistograms_m.size() 
          << " histogram configurations from " << histogramConfigFile;
      logger_m->log(ILogger::Level::Info, msg.str());
    }

  } catch (const std::exception &e) {
    if (logger_m) {
      std::stringstream msg;
      msg << "NDHistogramManager: Error parsing histogram config file: " << e.what();
      logger_m->log(ILogger::Level::Error, msg.str());
    }
    throw;
  }
}

/**
 * @brief Book histograms defined in config file
 */
void NDHistogramManager::bookConfigHistograms() {
  if (configHistograms_m.empty()) {
    return;
  }

  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("NDHistogramManager: DataManager or SystematicManager not set");
  }

  if (logger_m) {
    std::stringstream msg;
    msg << "NDHistogramManager: Booking " << configHistograms_m.size() 
        << " histograms from config";
    logger_m->log(ILogger::Level::Info, msg.str());
  }

  for (const auto &config : configHistograms_m) {
    // Create histInfo object
    histInfo info(config.name.c_str(), config.variable.c_str(), 
                  config.label.c_str(), config.weight.c_str(),
                  config.bins, config.lowerBound, config.upperBound);

    // Create selectionInfo objects
    selectionInfo channelInfo(config.channelVariable, config.channelBins,
                              config.channelLowerBound, config.channelUpperBound,
                              config.channelRegions);

    selectionInfo controlRegionInfo(config.controlRegionVariable, config.controlRegionBins,
                                    config.controlRegionLowerBound, config.controlRegionUpperBound,
                                    config.controlRegionRegions);

    selectionInfo sampleCategoryInfo(config.sampleCategoryVariable, config.sampleCategoryBins,
                                     config.sampleCategoryLowerBound, config.sampleCategoryUpperBound,
                                     config.sampleCategoryRegions);

    // Book the histogram
    BookSingleHistogram(info, std::move(sampleCategoryInfo), 
                       std::move(controlRegionInfo), std::move(channelInfo),
                       config.suffix);
  }

  if (logger_m) {
    logger_m->log(ILogger::Level::Info, "NDHistogramManager: Successfully booked all config histograms");
  }
}
void NDHistogramManager::initialize() {
  std::cout << "NDHistogramManager: initialized with "
            << configHistograms_m.size() << " config histogram(s)."
            << std::endl;
}

void NDHistogramManager::reportMetadata() {
  if (!logger_m) return;
  logger_m->log(ILogger::Level::Info,
                "NDHistogramManager: " +
                std::to_string(configHistograms_m.size()) +
                " config histogram(s) defined.");
}
