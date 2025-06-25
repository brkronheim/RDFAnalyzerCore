/*
 * @file util.cc
 * @brief Utility functions for configuration, file handling, and ROOT data
 * structures.
 *
 * This file provides helper functions for reading configuration files, scanning
 * directories, splitting strings, and setting up ROOT data structures such as
 * TChain and RDataFrame.
 */
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>

#include <dirent.h>

#include <functions.h>
#include <plots.h>
#include <util.h>
#include <configParser.h>



/**
* @brief Get list of tree names from config
* @param configMap Configuration map to search
* @return Vector of tree names (defaults to {"Events"} if not found)
*/
static std::vector<std::string>
getTreeList(const std::unordered_map<std::string, std::string> &configMap) {
    return ConfigParser::getList(configMap, "treeList", {"Events"});
}

/**
    * @brief Get list of file paths from config
    * @param configMap Configuration map to search
    * @return Vector of file paths (empty if not found)
    */
static std::vector<std::string>
getFileList(const std::unordered_map<std::string, std::string> &configMap) {
    return ConfigParser::getList(configMap, "fileList");
}

/**
    * @brief Get directory path from config
    * @param configMap Configuration map to search
    * @return Directory path string (empty if not found)
    */
static std::string
getDirectory(const std::unordered_map<std::string, std::string> &configMap) {
    if (configMap.find("directory") != configMap.end()) {
        return configMap.at("directory");
    }
    return "";
}


/**
 * @brief Check if a file matches the given glob patterns
 * @param name Base name of the file
 * @param globs List of glob patterns to match (include)
 * @param antiglobs List of glob patterns to exclude
 * @param fullName Full path of the file
 * @return true if file matches include patterns and not exclude patterns
 */
static bool matchesGlobs(const std::string &name,
                        const std::vector<std::string> &globs,
                        const std::vector<std::string> &antiglobs,
                        const std::string &fullName) {
    bool found = false;
    for (const auto &glob : globs) {
        if (name.find(glob) != std::string::npos) {
            found = true;
            break;
        }
    }
    if (found) { // only check if match found
        for (const auto &glob : antiglobs) {
            if (fullName.find(glob) != std::string::npos) {
                found = false;
                break;
            }
        }
        if (found && !name.empty() && name.at(0) == '.') {
            found = false;
        }
    }
    return found;
}

/**
 * @brief Recursively scan a directory for ROOT files matching glob patterns
 * @param chain TChain to add matching files to
 * @param directory Directory to scan
 * @param globs List of glob patterns to match (include)
 * @param antiglobs List of glob patterns to exclude
 * @param base Whether this is the base (top-level) directory
 * @return Number of files found and added to the chain
 */
static int scanDirectory(TChain &chain, const std::string &directory,
                        const std::vector<std::string> &globs,
                        const std::vector<std::string> &antiglobs, bool base) {
    int filesFound = 0;
    DIR *dr = opendir(directory.c_str());
    if (dr == nullptr && base) {
        std::cerr << "Error: No directory found. Returning" << std::endl;
        return filesFound;
    }
    struct dirent *en;
    while ((en = readdir(dr)) != NULL) {
        std::string name = en->d_name;
        std::string fullName = directory + "/" + name;
        if (name.find(".root") != std::string::npos) {
            if (matchesGlobs(name, globs, antiglobs, fullName)) {
                chain.Add(fullName.c_str());
                filesFound++;
            }
        } else if (!name.empty() && name.find(".") == std::string::npos) {
            filesFound += scanDirectory(chain, directory + "/" + name + "/", globs,
                                    antiglobs, false);
        }
    }
    closedir(dr);
    return filesFound;
}

/**
 * @brief Setup ROOT thread configuration based on config map
 * @param configMap Configuration map containing thread settings
 * 
 * If threads > 1, enables that many threads.
 * If threads = 1, runs single-threaded.
 * If threads < 1 or not specified, enables maximum number of threads.
 */
static void setupROOTThreads(
    const std::unordered_map<std::string, std::string> &configMap) {
    int threads = -1;
    if (configMap.find("threads") != configMap.end()) {
        threads = std::stoi(configMap.at("threads"));
    }
    if (threads > 1) {
        ROOT::EnableImplicitMT(threads);
        std::cout << "Running with " << threads << " threads" << std::endl;
    } else if (threads == 1) {
        std::cout << "Running with 1 thread" << std::endl;
    } else {
        ROOT::EnableImplicitMT();
        std::cout << "Running with maximum number of threads" << std::endl;
    }
}

/**
 * @brief Scan a directory for ROOT files matching globs and add them to a TChain
 * 
 * This function is not recursive by default. It only checks one directory level
 * unless base is false.
 *
 * @param chain TChain to add files to
 * @param directory Directory to scan
 * @param globs List of substrings to match (include)
 * @param antiglobs List of substrings to exclude
 * @param base If true, only scan the top-level directory
 * @return Number of files found and added
 */
int scan(TChain &chain, const std::string &directory,
         const std::vector<std::string> &globs,
         const std::vector<std::string> &antiglobs, bool base) {
    std::cout << "Checking " << directory << std::endl;
    int filesFound = scanDirectory(chain, directory, globs, antiglobs, base);
    if (filesFound == 0 && base) {
        std::cerr << "Error: No files found. Returning" << std::endl;
    }
    return filesFound;
}








/**
 * @brief Create TChain objects from configuration
 * 
 * Creates and configures TChain objects based on the configuration map.
 * Handles both file list and directory-based input methods.
 *
 * @param configMap Configuration map containing input settings
 * @return Vector of unique_ptr to configured TChain objects
 */
std::vector<std::unique_ptr<TChain>>
makeTChain(std::unordered_map<std::string, std::string> &configMap) {

    // setup ROOT threads now before any dataframes are created
    setupROOTThreads(configMap);

    std::vector<std::unique_ptr<TChain>> tchainVector;
    auto treeListVec = getTreeList(configMap);
    for (const auto &tree : treeListVec) {
        tchainVector.emplace_back(new TChain(tree.c_str()));
    }

    std::vector<std::string> globs =
        ConfigParser::getList(configMap, "globs", {".root"});
    std::vector<std::string> antiGlobs =
        ConfigParser::getList(configMap, "antiglobs", {"FAIL"});

    int fileNum = 0;
    auto fileListVec = getFileList(configMap);
    if (!fileListVec.empty()) {
        fileNum = fileListVec.size();
        for (const auto &file : fileListVec) {
            std::cout << "Adding file " << file << std::endl;
            for (auto &chain : tchainVector) {
                chain->Add(file.c_str());
            }
        }
    } else {
        std::string directory = getDirectory(configMap);
        if (!directory.empty()) {
            for (auto &chain : tchainVector) {
                fileNum = scan(*(tchainVector[0].get()), directory, globs, antiGlobs);
            }
        } else {
            std::cerr
                << "Error!!!!! No input directory provided. Please include one "
                   "in the config file, for example with fileList=pathToFile.root"
                << std::endl;
        }
    }

    for (int i = 1; i < tchainVector.size(); i++) {
        tchainVector[0]->AddFriend(tchainVector[i].get());
    }
    std::cout << fileNum << " files found" << std::endl;
    return tchainVector;
}



/**
 * @brief Save content of a DataFrame to a ROOT file
 * 
 * Saves specified branches from a DataFrame to a ROOT file, handling
 * systematic variations of variables.
 *
 * @param df DataFrame to save
 * @param configMap Configuration map containing save settings
 * @param variableToSystematicMap Map of variables to their systematic variations
 * @return Updated DataFrame after saving
 */
ROOT::RDF::RNode
saveDF(ROOT::RDF::RNode &df,
       const std::unordered_map<std::string, std::string> &configMap,
       const std::unordered_map<std::string, std::unordered_set<std::string>>
           &variableToSystematicMap) {
    std::string saveConfig;
    std::string saveFile;
    std::string saveTree;
    // Read saveConfig, saveFile, and saveTree from the config. It needs to exist
    if (configMap.find("saveConfig") != configMap.end()) {
        saveConfig = configMap.at("saveConfig");
    } else {
        std::cerr << "Error: No saveConfig provided. Please include one in the "
                    "config file."
                 << std::endl;
        return (df);
    }

    if (configMap.find("saveFile") != configMap.end()) {
        saveFile = configMap.at("saveFile");
    } else {
        std::cerr
            << "Error: No saveFile provided. Please include one in the config file."
            << std::endl;
        return (df);
    }

    if (configMap.find("saveTree") != configMap.end()) {
        saveTree = configMap.at("saveTree");
    } else {
        std::cerr
            << "Error: No saveTree provided. Please include one in the config file."
            << std::endl;
        return (df);
    }

    // Get the list of branches to save
    std::vector<std::string> saveVectorInit = ConfigParser::parseVectorConfig(saveConfig);
    std::vector<std::string> saveVector;
    for (auto val : saveVectorInit) {
        val = val.substr(0, val.find(" ")); // drop everything after the space
        if (val.size() > 0) {
            saveVector.push_back(val);
        }
    }

    const unsigned int vecSize = saveVector.size();
    for (unsigned int i = 0; i < vecSize; i++) {
        if (variableToSystematicMap.find(saveVector[i]) !=
            variableToSystematicMap.end()) {
            for (auto &syst : variableToSystematicMap.at(saveVector[i])) {
                saveVector.push_back(saveVector[i] + "_" + syst + "Up");
                saveVector.push_back(saveVector[i] + "_" + syst + "Down");
            }
        }
    }
    // Save the output
    std::cout << "Executing Snapshot" << std::endl;
    std::cout << "Tree: " << saveTree << std::endl;
    std::cout << "SaveFile: " << saveFile << std::endl;
    df.Snapshot(saveTree, saveFile, saveVector);
    std::cout << "Done Saving" << std::endl;
    return (df);
}

/**
 * @brief Save multi-dimensional histograms and their metadata
 * 
 * Saves histograms and their associated metadata (names, variables, labels,
 * binning information) to a ROOT file. Also saves region information.
 *
 * @param fullHistList List of histogram information objects
 * @param hists Histogram holder object containing the actual histograms
 * @param allRegionNames List of region names for the histograms
 * @param fileName Output file name
 */
void save(std::vector<std::vector<histInfo>> &fullHistList, histHolder &hists,
          std::vector<std::vector<std::string>> &allRegionNames,
          std::string fileName) {
    std::vector<std::string> allNames;
    std::vector<std::string> allVariables;
    std::vector<std::string> allLabels;
    std::vector<int> allBins;
    std::vector<float> allLowerBounds;
    std::vector<float> allUpperBounds;
    // Get vectors of the hist names, variables, lables, bins, and bounds
    for (auto const &histList : fullHistList)
        for (auto const &info : histList) {
            allNames.push_back(info.name());
            allVariables.push_back(info.variable());
            allLabels.push_back(info.label());
            allBins.push_back(info.bins());
            allLowerBounds.push_back(info.lowerBound());
            allUpperBounds.push_back(info.upperBound());
        }

    // Open file
    TFile saveFile(fileName.c_str(), "RECREATE");

    // Save hists under hists
    saveFile.mkdir("Hists");
    saveFile.cd("Hists");
    hists.save();
    saveFile.cd();

    // Save hist metadata under histData
    TTree histData("histData", "histData");

    // Make branches
    Int_t name_len;
    Int_t var_len;
    Int_t label_len;
    Char_t name[500];
    Char_t var[500];
    Char_t label[500];

    Int_t bins;
    Float_t lowerBounds;
    Float_t upperBounds;

    histData.Branch("name_len", &name_len, "name_len/I");
    histData.Branch("var_len", &var_len, "var_len/I");
    histData.Branch("label_len", &label_len, "label_len/I");

    histData.Branch("name", name, "name[name_len]/C");
    histData.Branch("var", var, "var[var_len]/C");
    histData.Branch("label", label, "label[label_len]/C");

    histData.Branch("bins", &bins, "bins/I");
    histData.Branch("lowerBounds", &lowerBounds, "lowerBounds/F");
    histData.Branch("upperBounds", &upperBounds, "upperBounds/F");

    // Save the raw data, including null terminators
    for (long unsigned int x = 0; x < allNames.size(); x++) {
        name_len = allNames[x].size();
        var_len = allVariables[x].size();
        label_len = allLabels[x].size();
        char *name_raw = allNames[x].data();
        for (int y = 0; y < name_len; y++) {
            name[y] = name_raw[y];
        }
        char *var_raw = allVariables[x].data();
        for (int y = 0; y < var_len; y++) {
            var[y] = var_raw[y];
        }
        char *label_raw = allLabels[x].data();
        for (int y = 0; y < label_len; y++) {
            label[y] = label_raw[y];
        }
        bins = allBins[x];
        lowerBounds = allLowerBounds[x];
        upperBounds = allUpperBounds[x];
        histData.Fill();
    }

    // Save region metadata under regionData
    TTree regionData("regionData", "regionData");
    Int_t region_len;
    char region[500];
    regionData.Branch("region_len", &region_len, "region_len/I");
    regionData.Branch("region", region, "region[region_len]/C");

    for (auto const &regionList : allRegionNames) {
        for (auto const &regionString : regionList) {
            region_len = regionString.size();
            for (int y = 0; y < region_len; y++) {
                region[y] = regionString.data()[y];
            }
            regionData.Fill();
        }
    }

    histData.Write();
    regionData.Write();
}


