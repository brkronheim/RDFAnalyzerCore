/*
 * @file util.cc
 * @brief Utility functions for configuration, file handling, and ROOT data
 * structures.
 *
 * This file provides helper functions for reading configuration files, scanning
 * directories, splitting strings, and setting up ROOT data structures such as
 * TChain and RDataFrame.
 */
#include <cstdlib>
#include <dlfcn.h>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>
#include <TROOT.h>

#include <dirent.h>

#include <yaml-cpp/yaml.h>

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
 * @brief Validate ROOT runtime environment consistency
 *
 * Ensures the loaded ROOT libraries are consistent with ROOTSYS when ROOTSYS
 * is set, avoiding mixed ROOT installations at runtime.
 */
static void validateRootEnvironment() {
  Dl_info info;
  if (dladdr(reinterpret_cast<void*>(&TROOT::Class), &info) != 0 && info.dli_fname) {
    const std::string libCorePath(info.dli_fname);
    const char *rootSysEnv = std::getenv("ROOTSYS");
    if (rootSysEnv == nullptr || std::string(rootSysEnv).empty()) {
      return;
    }
    const std::string rootSysStr(rootSysEnv);
    if (libCorePath.rfind(rootSysStr, 0) != 0) {
      throw std::runtime_error(
          "Detected ROOT library from '" + libCorePath + "' but ROOTSYS='" + rootSysStr +
          "'. This indicates mixed ROOT installations. Source env.sh and rebuild from a clean build directory.");
    }
  }
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
  if (dr == nullptr) {
    // Directory does not exist or cannot be opened; return 0 files found.
    // Do not throw here so that higher-level logic can decide how to proceed
    // (for example unit tests that operate without input files).
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
  const auto &configMap = configProvider.getConfigMap();
  auto it = configMap.find("threads");
  if (it != configMap.end()) {
    const std::string value = it->second;
    if (value == "auto" || value == "max") {
      threads = 0;
    } else {
      try {
        threads = std::stoi(value);
      } catch (const std::exception &) {
        std::cout << "Invalid threads value '" << value << "', defaulting to auto" << std::endl;
        threads = 0;
      }
    }
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
    std::cout << "Warning: No files found for TChain in directory " << directory << std::endl;
    // Proceed without throwing so tests and non-file-based workflows can continue
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

  validateRootEnvironment();



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
 * @brief Parse a friend-tree YAML configuration file into a list of specs.
 *
 * Expected YAML structure:
 * @code{.yaml}
 * friends:
 *   - alias: calib
 *     treeName: Events          # optional, defaults to "Events"
 *     fileList:                 # explicit file list (local or XRootD)
 *       - /path/to/calib.root
 *       - root://server//path/to/remote.root
 *     indexBranches:            # optional; enables index-based event matching
 *       - run
 *       - luminosityBlock
 *   - alias: taggers
 *     treeName: BTagging
 *     directory: /path/to/dir  # scan a directory instead of explicit files
 *     globs: [.root]
 *     antiglobs: [output.root]
 * @endcode
 *
 * @param configFile Path to the YAML configuration file.
 * @return Vector of parsed FriendTreeSpec objects.
 * @throws std::runtime_error on YAML parse errors.
 */
std::vector<FriendTreeSpec>
parseFriendTreeConfig(const std::string &configFile) {
  std::vector<FriendTreeSpec> specs;

  YAML::Node root;
  try {
    root = YAML::LoadFile(configFile);
  } catch (const YAML::Exception &e) {
    throw std::runtime_error("Error parsing friend tree config '" + configFile +
                             "': " + e.what());
  }

  auto friendsNode = root["friends"];
  if (!friendsNode || !friendsNode.IsSequence()) {
    return specs;
  }

  for (const auto &entry : friendsNode) {
    if (!entry.IsMap()) {
      continue;
    }
    if (!entry["alias"]) {
      std::cerr << "Warning: friend tree entry missing required 'alias' field; skipping."
                << std::endl;
      continue;
    }

    FriendTreeSpec spec;
    spec.alias = entry["alias"].as<std::string>();

    if (entry["treeName"]) {
      spec.treeName = entry["treeName"].as<std::string>();
    }

    if (entry["fileList"] && entry["fileList"].IsSequence()) {
      for (const auto &f : entry["fileList"]) {
        spec.files.push_back(f.as<std::string>());
      }
    }

    if (entry["directory"]) {
      spec.directory = entry["directory"].as<std::string>();
    }

    if (entry["globs"] && entry["globs"].IsSequence()) {
      spec.globs.clear();
      for (const auto &g : entry["globs"]) {
        spec.globs.push_back(g.as<std::string>());
      }
    }

    if (entry["antiglobs"] && entry["antiglobs"].IsSequence()) {
      spec.antiglobs.clear();
      for (const auto &g : entry["antiglobs"]) {
        spec.antiglobs.push_back(g.as<std::string>());
      }
    }

    if (entry["indexBranches"] && entry["indexBranches"].IsSequence()) {
      for (const auto &b : entry["indexBranches"]) {
        spec.indexBranches.push_back(b.as<std::string>());
      }
    }

    specs.push_back(std::move(spec));
  }

  return specs;
}

