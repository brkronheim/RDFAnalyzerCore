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

// TODO: idealy these will be absorbed into the manager classes

/**
 * @brief Get list of tree names from config
 * @param configProvider Configuration provider to search
 * @return Vector of tree names (defaults to {"Events"} if not found)
 */
static std::vector<std::string>
getTreeList(const IConfigurationProvider &configProvider) {
  return configProvider.getList("treeList", {"Events"});
}

/**
 * @brief Get list of file paths from config
 * @param configProvider Configuration provider to search
 * @return Vector of file paths (empty if not found)
 */
static std::vector<std::string>
getFileList(const IConfigurationProvider &configProvider) {
  return configProvider.getList("fileList");
}

/**
 * @brief Get directory path from config
 * @param configProvider Configuration provider to search
 * @return Directory path string (empty if not found)
 */
static std::string getDirectory(const IConfigurationProvider &configProvider) {
  if (configProvider.getConfigMap().find("directory") !=
      configProvider.getConfigMap().end()) {
    return configProvider.getConfigMap().at("directory");
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
    throw std::runtime_error("Error: No directory found for scanning.");
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
 * @param configProvider Configuration provider containing thread settings
 *
 * If threads > 1, enables that many threads.
 * If threads = 1, runs single-threaded.
 * If threads < 1 or not specified, enables maximum number of threads.
 */
static void setupROOTThreads(const IConfigurationProvider &configProvider) {
  int threads = -1;
  if (configProvider.getConfigMap().find("threads") !=
      configProvider.getConfigMap().end()) {
    threads = std::stoi(configProvider.getConfigMap().at("threads"));
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
 * @brief Scan a directory for ROOT files matching globs and add them to a
 * TChain
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
    throw std::runtime_error("Error: No files found for TChain.");
  }
  return filesFound;
}

/**
 * @brief Create TChain objects from configuration
 *
 * Creates and configures TChain objects based on the configuration map.
 * Handles both file list and directory-based input methods.
 *
 * @param configProvider Configuration provider containing input settings
 * @return Vector of unique_ptr to configured TChain objects
 */
std::vector<std::unique_ptr<TChain>>
makeTChain(const IConfigurationProvider &configProvider) {



  // setup ROOT threads now before any dataframes are created
  setupROOTThreads(configProvider);

  std::vector<std::unique_ptr<TChain>> tchainVector;
  auto treeListVec = getTreeList(configProvider);
  for (const auto &tree : treeListVec) {
    tchainVector.emplace_back(new TChain(tree.c_str()));
  }

  std::vector<std::string> globs = configProvider.getList("globs", {".root"});
  std::vector<std::string> antiGlobs =
      configProvider.getList("antiglobs", {"FAIL"});

  int fileNum = 0;
  auto fileListVec = getFileList(configProvider);
  if (!fileListVec.empty()) {
    fileNum = fileListVec.size();
    for (const auto &file : fileListVec) {
      std::cout << "Adding file " << file << std::endl;
      for (auto &chain : tchainVector) {
        chain->Add(file.c_str());
      }
    }
  } else {
    std::string directory = getDirectory(configProvider);
    if (!directory.empty()) {
      for (auto &chain : tchainVector) {
        fileNum = scan(*(tchainVector[0].get()), directory, globs, antiGlobs);
      }
    } else {
      throw std::runtime_error(
          "Error: No input directory provided. Please include one in the "
          "config file, for example with fileList=pathToFile.root");
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
 * @param configProvider Configuration provider containing save settings
 * @param dataFrameProvider DataFrame provider for systematic information
 * @param systematicManager Systematic manager for systematic variations
 * @return Updated DataFrame after saving
 */
ROOT::RDF::RNode saveDF(ROOT::RDF::RNode &df,
                        const IConfigurationProvider &configProvider,
                        const IDataFrameProvider &dataFrameProvider,
                        const ISystematicManager *systematicManager) {
  std::string saveConfig;
  std::string saveFile;
  std::string saveTree;
  // Read saveConfig, saveFile, and saveTree from the config. It needs to exist
  if (configProvider.getConfigMap().find("saveConfig") !=
      configProvider.getConfigMap().end()) {
    saveConfig = configProvider.getConfigMap().at("saveConfig");
  } else {
    throw std::runtime_error("Error: No saveConfig provided. Please include "
                             "one in the config file.");
    return (df);
  }

  if (configProvider.getConfigMap().find("saveFile") !=
      configProvider.getConfigMap().end()) {
    saveFile = configProvider.getConfigMap().at("saveFile");
  } else {
    throw std::runtime_error(
        "Error: No saveFile provided. Please include one in the config file.");
    return (df);
  }

  if (configProvider.getConfigMap().find("saveTree") !=
      configProvider.getConfigMap().end()) {
    saveTree = configProvider.getConfigMap().at("saveTree");
  } else {
    throw std::runtime_error(
        "Error: No saveTree provided. Please include one in the config file.");
    return (df);
  }

  // Get the list of branches to save
  std::vector<std::string> saveVectorInit =
      configProvider.parseVectorConfig(saveConfig);
  std::vector<std::string> saveVector;
  for (auto val : saveVectorInit) {
    val = val.substr(0, val.find(" ")); // drop everything after the space
    if (val.size() > 0) {
      saveVector.push_back(val);
    }
  }

  // Add systematic variations for each branch if systematicManager is provided
  if (systematicManager) {
    const unsigned int vecSize = saveVector.size();
    for (unsigned int i = 0; i < vecSize; i++) {
      const auto &systs = systematicManager->getSystematicsForVariable(saveVector[i]);
      for (const auto &syst : systs) {
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
