#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <NDHistogramManager.h>
#include <TFile.h>
#include <TH1F.h>
#include <THnSparse.h>
#include <cmath>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <util.h>

/**
 * @brief Construct a new NDHistogramManager object
 * @param dataFrameProvider Reference to the dataframe provider
 * @param configProvider Reference to the configuration provider
 */
NDHistogramManager::NDHistogramManager(IDataFrameProvider &dataFrameProvider,
                                       IConfigurationProvider &configProvider)
    : dataFrameProvider_m(dataFrameProvider), configProvider_m(configProvider) {}

/**
 * @brief Book N-dimensional histograms
 * @param infos Vector of histogram info objects
 * @param selection Vector of selection info objects
 * @param suffix Suffix to append to histogram names
 * @param allRegionNames Vector of region name vectors
 */
void NDHistogramManager::BookND(
    std::vector<histInfo> &infos, std::vector<selectionInfo> &selection,
    const std::string &suffix,
    std::vector<std::vector<std::string>> &allRegionNames) {

  if (allRegionNames.empty()) {
    throw std::invalid_argument("NDHistogramManager::BookND: allRegionNames must not be empty");
  }

  ROOT::RDF::RNode df = dataFrameProvider_m.getDataFrame();

  for (const auto &info : infos) {
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
    std::vector<std::string> systVector(varVector);
    for (const auto &syst : allRegionNames[allRegionNames.size() - 1]) {
      std::string systBase = syst;
      if (syst.find("Up") != std::string::npos) {
        systBase = systBase.substr(0, syst.find("Up"));
      }
      if (syst.find("Down") != std::string::npos) {
        systBase = systBase.substr(0, syst.find("Down"));
      }
      if (syst == "Nominal") {
        continue;
      }
      // TODO: This needs to be updated to work with the interface
      // For now, we'll skip systematic handling
      /*
      const auto &varSet =
          dataFrameProvider_m.getSystematicManager().getVariablesForSystematic(
              systBase);
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
      if (affectedVariables > 1) {
        systVector.insert(systVector.end(), newVec.begin(), newVec.end());
        numFills++;
      }
      */
    }

    std::string branchName = info.name() + "_" + suffix + "inputDoubleVector";
    dataFrameProvider_m.DefineVector(branchName, systVector, "Double_t");
    df = dataFrameProvider_m.getDataFrame();
    THnMulti tempModel(df.GetNSlots(), newName.c_str(), newName.c_str(),
                       selection.size() + 1, numFills, binVector,
                       lowerBoundVector, upperBoundVector);
    histos_m.push_back(df.Book<ROOT::VecOps::RVec<Double_t>>(
        std::move(tempModel), {branchName}));
  }
}

/**
 * @brief Save all booked histograms
 * @param fullHistList Vector of vectors of histogram info
 * @param allRegionNames Vector of region name vectors
 */
void NDHistogramManager::SaveHists(
    std::vector<std::vector<histInfo>> &fullHistList,
    std::vector<std::vector<std::string>> &allRegionNames) {
  std::string fileName = configProvider_m.get("saveFile");
  std::vector<std::string> allNames;
  std::vector<std::string> allVariables;
  std::vector<std::string> allLabels;
  std::vector<int> allBins;
  std::vector<float> allLowerBounds;
  std::vector<float> allUpperBounds;
  // Get vectors of the hist names, variables, lables, bins, and bounds
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

  // Open file
  TFile saveFile(fileName.c_str(), "RECREATE");

  // Save hists under hists
  // std::cout << df_m.Count().GetValue() << " Events processed" << std::endl;
  // // Trigger execution const Int_t histNumber = histos_m.size(); const
  // Int_t axisNumber = allRegionNames.size()+1;
  std::vector<Int_t> commonAxisSize = {};

  for (const auto &regionNameList : allRegionNames) {
    commonAxisSize.push_back(regionNameList.size());
  }

  int histIndex = 0;
  std::unordered_map<std::string, TH1F> histMap;
  std::unordered_set<std::string> dirSet;
  for (auto &histo_m : histos_m) {
    auto hist = histo_m.GetPtr();
    // const Int_t finalAxisSize = allBins[histIndex];
    const Int_t currentHistogramSize = hist->GetNbins();
    std::vector<Int_t> indices(commonAxisSize.size() + 1);
    for (int i = 0; i < currentHistogramSize; i++) {
      Float_t content = hist->GetBinContent(i, indices.data());
      if (content == 0) {
        continue;
      }
      Float_t error = hist->GetBinError2(i);
      /*std::cout << "Bin: " << indices << ": " << content << ", " << error <<
      ": "; for(int j = 0; j< commonAxisSize.size()+1; j++){ std::cout <<
      indices[j] << ", ";
      }
      std::cout << std::endl;*/

      std::string dirName = "";
      // std::cout << "histName: " << histName
      const Int_t size = commonAxisSize.size() - 2;
      for (int i = 0; i < size; i++) {
        dirName += allRegionNames[i][indices[i] - 1] + "/";
        // std::cout << "histName: " << histName;
      }

      dirName += allRegionNames[commonAxisSize.size() - 2]
                               [indices[commonAxisSize.size() - 2] - 1];
      std::string histName = allVariables[histIndex];
      // bool isNominal=false;
      if (allRegionNames[commonAxisSize.size() - 1]
                        [indices[commonAxisSize.size() - 1] - 1] == "Nominal") {
        // isNominal= true;
      } else {
        histName += "_" +
                    allRegionNames[commonAxisSize.size() - 1]
                                  [indices[commonAxisSize.size() - 1] - 1];
      }
      if (histMap.count(dirName + "/" + histName) == 0) {
        histMap[dirName + "/" + histName] =
            TH1F(histName.c_str(),
                 (allVariables[histIndex] + ";" + allVariables[histIndex] +
                  ";Counts")
                     .c_str(),
                 allBins[histIndex], allLowerBounds[histIndex],
                 allUpperBounds[histIndex]);
        dirSet.emplace(dirName);
      }
      histMap[dirName + "/" + histName].SetBinContent(
          indices[commonAxisSize.size()], content);
      histMap[dirName + "/" + histName].SetBinError(
          indices[commonAxisSize.size()], sqrt(error));
    }

    histIndex++;
  }

  saveFile.cd();
  for (const auto &dirName : dirSet) {
    std::string newDir(dirName);
    newDir[dirName.find('/')] = '_';
    saveFile.mkdir(newDir.c_str());
  }
  std::unordered_map<std::string,
                     std::map<std::string, std::pair<Float_t, Float_t>>>
      systNormalizations;
  // store the nominal and sytematic normalization for each control region
  for (const auto &pair : histMap) {
    if (pair.first.find("Systematic") ==
        std::string::npos) { // Want the sytematic histogram
      continue;
    }
    std::string dirName = pair.first.substr(0, pair.first.find_last_of("/"));
    auto regionSplit = configProvider_m.splitString(pair.first, "/");
    std::string region =
        regionSplit[0] + "_" +
        regionSplit[2]; //  dirName.substr(0,dirName.find("/"));
    std::string histName =
        regionSplit[regionSplit.size() -
                    1]; // pair.first.substr(pair.first.find_last_of("/")+1);
    std::string nominalName = histName;
    std::string systName = "nominal";
    if (histName.find("Up") != std::string::npos ||
        histName.find("Down") != std::string::npos) {
      systName = nominalName.substr(nominalName.find_last_of("_") + 1);
      nominalName = nominalName.substr(0, nominalName.find_last_of("_"));
    }
    std::cout << nominalName << ", " << systName << std::endl;
    nominalName = dirName + "/" + nominalName;
    auto nominalHist = histMap[nominalName];
    Float_t nominalIntegral = nominalHist.Integral();
    Float_t systIntegral = pair.second.Integral();
    if (systNormalizations[region].count(systName) == 0) {
      systNormalizations[region][systName].first = nominalIntegral;
      systNormalizations[region][systName].second = systIntegral;
    } else {
      systNormalizations[region][systName].first += nominalIntegral;
      systNormalizations[region][systName].second += systIntegral;
    }
  }

  for (const auto &pair : histMap) {
    std::string dirName = pair.first.substr(0, pair.first.find_last_of("/"));
    auto regionSplit = configProvider_m.splitString(pair.first, "/");
    std::string region =
        regionSplit[0] + "_" +
        regionSplit[2]; //  dirName.substr(0,dirName.find("/"));
    std::string histName =
        regionSplit[regionSplit.size() -
                    1]; // pair.first.substr(pair.first.find_last_of("/")+1);
    std::string nominalName = histName;
    std::string systName = "nominal";
    if (histName.find("Up") != std::string::npos ||
        histName.find("Down") != std::string::npos) {
      systName = nominalName.substr(nominalName.find_last_of("_") + 1);
      nominalName = nominalName.substr(0, nominalName.find_last_of("_"));
    }
    nominalName = dirName + "/" + nominalName;
    std::cout << histName << ", " << systName << std::endl;
    std::string newDir(dirName);
    newDir[dirName.find('/')] = '_';
    saveFile.cd(newDir.c_str());
    pair.second.Write();
    //(pair.second*(systNormalizations[region][systName].first/systNormalizations[region][systName].second)).Write();
    //// Need to group this normalization the way combine wants it
    saveFile.cd();
  }
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