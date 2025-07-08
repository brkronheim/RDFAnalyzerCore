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
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <util.h>
#include <plots.h>
#include <string>
#include <vector>

/**
 * @brief Construct a new NDHistogramManager object
 */
NDHistogramManager::NDHistogramManager(IConfigurationProvider const& configProvider) {}

/**
 * @brief Book N-dimensional histograms
 * @param infos Vector of histogram info objects
 * @param selection Vector of selection info objects
 * @param suffix Suffix to append to histogram names
 * @param allRegionNames Vector of region name vectors
 */
void NDHistogramManager::BookSingleHistogram(
    histInfo &info, // histogram info (name, bins, lowerBound, upperBound)
    std::vector<selectionInfo> &selection, // selection info (variable, bins, lowerBound, upperBound)
    // last dimension of the histogram is the systematic variation
    std::string suffix, // suffix to append to histogram names
    std::vector<std::vector<std::string>> &allRegionNames) { // all region names

  if(dataManager_m == nullptr) {
    throw std::runtime_error("NDHistogramManager::BookSingleHistogram: DataManager not set");
  }

  ROOT::RDF::RNode df = dataManager_m->getDataFrame();

  // extract the binning information from the common selections
  std::vector<int> binVectorBase;
  std::vector<double> lowerBoundVectorBase;
  std::vector<double> upperBoundVectorBase;
  std::vector<std::string> varVectorBase;

  for (auto const &selectionInfo : selection) {
    binVectorBase.push_back(selectionInfo.bins());
    lowerBoundVectorBase.push_back(selectionInfo.lowerBound());
    upperBoundVectorBase.push_back(selectionInfo.upperBound());
    varVectorBase.push_back(selectionInfo.variable());
  }

  // add the binning information from the specific histogram (the last dimension)
  std::string newName = info.name() + "." + suffix;
  std::vector<int> binVector(binVectorBase);
  std::vector<double> lowerBoundVector(lowerBoundVectorBase);
  std::vector<double> upperBoundVector(upperBoundVectorBase);
  std::vector<std::string> varVector(varVectorBase);
  binVector.push_back(info.bins());
  lowerBoundVector.push_back(info.lowerBound());
  upperBoundVector.push_back(info.upperBound());
  varVector.push_back(info.variable());
  varVector.push_back(info.weight());

  Int_t numFills = 1;
  // determine any systematic variations on the histogram
  std::vector<std::string> systVector = varVector;
  for (const auto &syst : allRegionNames[allRegionNames.size() - 1]) {
    std::string systBase(syst);
    if (syst.find("Up") != std::string::npos) {
      systBase = systBase.substr(0, syst.find("Up"));
    }
    if (syst.find("Down") != std::string::npos) {
      systBase = systBase.substr(0, syst.find("Down"));
    }
    if (syst == "Nominal") {
      continue;
    }
      
    // get the variables for the systematic variation
    const auto &varSet = systematicManager_m->getVariablesForSystematic(systBase);
    
    // get the affected variables for the systematic variation
    Int_t affectedVariables = 0;
    std::vector<std::string> newVec;
    for (const auto &branch : varVector) {
      if (varSet.count(branch) != 0) {
        affectedVariables += 1;
        newVec.push_back(branch + "_" + syst);
      } else {
        newVec.push_back(branch);
      }
    }
    
    // if there are affected variables, add them to the systVector (the systematic counter branch will 
    // always be affected, so we need more than 1 affected variable))
    if (affectedVariables > 1) {
      systVector.insert(systVector.end(), newVec.begin(), newVec.end());
      numFills++;
    }
      
  }

  std::string branchName = info.name() + "_" + suffix + "inputDoubleVector";
  dataManager_m->DefineVector(branchName, systVector, "Double_t", *systematicManager_m);
    
  // Get the updated dataframe after DefineVector
  ROOT::RDF::RNode updatedDf = dataManager_m->getDataFrame();
    
  THnMulti tempModel(updatedDf.GetNSlots(), newName.c_str(), newName.c_str(),
                       selection.size() + 1, numFills, binVector,
                       lowerBoundVector, upperBoundVector);
  histos_m.push_back(updatedDf.Book<ROOT::VecOps::RVec<Double_t>>(
        std::move(tempModel), {branchName}));
}



/**
 * @brief Get the vector of histogram result pointers
 * @return Reference to the vector of RResultPtr<THnSparseD>
 */
std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseD>> &
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