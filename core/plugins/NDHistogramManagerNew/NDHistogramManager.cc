#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <api/IPluggableManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <TFile.h>
#include <TH1F.h>
#include <THnSparse.h>
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

    
    if (hasSystematic) {
        std::vector<std::string> systematicVariations;
        for (const auto& syst : allSystematics) {
            if (systematicManager_m->getVariablesForSystematic(syst).count(variable) != 0) {
                systematicVariations.push_back(variable + "_" + syst);
            } else {
                systematicVariations.push_back(variable);
            }
        }
        if (std::find(df.GetColumnNames().begin(), df.GetColumnNames().end(), variable + "_systVector") == df.GetColumnNames().end()) {
            dataManager_m->DefineVector(variable + "_systVector", systematicVariations, "Float_t", *systematicManager_m);
        }
        varVector.push_back(variable + "_systVector");
    } else {
        if (hasMultiFill) {
            varVector.push_back(variable);
        } else {
            if (std::find(df.GetColumnNames().begin(), df.GetColumnNames().end(), variable + "_RVec") == df.GetColumnNames().end()) {
                dataManager_m->DefineVector(variable + "_RVec", {variable}, "Float_t", *systematicManager_m);
            }
            varVector.push_back(variable + "_RVec");
        }
    }
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
  fillInfo.nbins = {channelInfo.bins(), controlRegionInfo.bins(), sampleCategoryInfo.bins(), info.bins()};
  fillInfo.xmin = {channelInfo.lowerBound(), controlRegionInfo.lowerBound(), sampleCategoryInfo.lowerBound(), info.lowerBound()};
  fillInfo.xmax = {channelInfo.upperBound(), controlRegionInfo.upperBound(), sampleCategoryInfo.upperBound(), info.upperBound()};

  // define zero vector if needed
  std::vector<std::string> dfVariableList = df.GetColumnNames();

  if(std::find(dfVariableList.begin(), dfVariableList.end(), "zero__") == dfVariableList.end()) {
    dataManager_m->Define("zero__", [](const std::string &channel) { return 0; }, {}, *systematicManager_m);
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

  // For now, we don't support multi-fill for systematic variations
  fillInfo.systematic_hasMultiFill = false;
  
  const std::vector<std::string> systList = systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);

  const Int_t nSystematics = systList.size();
  
  std::vector<std::string> varVector;

  // We're going to fill for each systematic variation so we can reuse a lot of the same input vectors between the different histograms
  // But we will set the weights to 0 for the non-systematic variations
  std::vector<std::string> usedSystematics(systList.begin(), systList.end());

  // Use helper for each axis
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, channelInfo.variable(), fillInfo.channel_hasSystematic, fillInfo.channel_hasMultiFill, usedSystematics, varVector);
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, controlRegionInfo.variable(), fillInfo.controlRegion_hasSystematic, fillInfo.controlRegion_hasMultiFill, usedSystematics, varVector);
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, sampleCategoryInfo.variable(), fillInfo.sampleCategory_hasSystematic, fillInfo.sampleCategory_hasMultiFill, usedSystematics, varVector);
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, "SystematicCounter", fillInfo.hasSystematic, false, usedSystematics, varVector);
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, info.variable(), fillInfo.hasSystematic, fillInfo.hasMultiFill, usedSystematics, varVector);
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, info.weight(), fillInfo.weight_hasSystematic, fillInfo.weight_hasMultiFill, usedSystematics, varVector, true);

  THnMulti tempModel(fillInfo);
  histos_m.push_back(df.Book<ROOT::VecOps::RVec<Float_t>>(
        std::move(tempModel), varVector));
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