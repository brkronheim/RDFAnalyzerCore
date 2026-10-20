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
#include <algorithm>
#include <cctype>
#include <cmath>
#include <dlfcn.h>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>
#include <TEnv.h>
#include <TROOT.h>
#include <TTreeCache.h>
#include <InputCache.h>
#ifdef RDF_HAS_XRD_ADAPTOR
#include <XrdSourceManager.h>
#endif

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
 * ROOT's implicit-MT setting is process-global. If threads > 1, enables that
 * many threads. If threads = 1, disables the process-wide pool.
 * If threads < 1 or not specified, enables maximum number of threads.
 */
static void setupROOTThreads(const IConfigurationProvider &configProvider) {
  int threads = -1;
  const auto &configMap = configProvider.getConfigMap();
  auto it = configMap.find("threads");
  if (it != configMap.end() && !it->second.empty()) {
    const std::string value = it->second;
    const auto first = value.find_first_not_of(" \t\n\r\f\v");
    const auto last = value.find_last_not_of(" \t\n\r\f\v");
    const std::string trimmed = first == std::string::npos
                                    ? std::string{}
                                    : value.substr(first, last - first + 1);

    std::string normalized = trimmed;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char character) {
                     return static_cast<char>(std::tolower(character));
                   });

    if (normalized == "auto" || normalized == "max") {
      threads = 0;
    } else if (!trimmed.empty()) {
      try {
        size_t consumed = 0;
        threads = std::stoi(trimmed, &consumed);
        if (consumed != trimmed.size()) {
          throw std::invalid_argument("trailing characters");
        }
      } catch (const std::exception &) {
        throw std::runtime_error(
            "Invalid 'threads' value '" + trimmed +
            "' (expected an integer, 'auto', or 'max').");
      }
    }
  }

  if (threads > 1) {
    ROOT::EnableImplicitMT(threads);
    std::cout << "Running with " << threads << " threads" << std::endl;
  } else if (threads == 1) {
    ROOT::DisableImplicitMT();
    std::cout << "Running with 1 thread" << std::endl;
  } else {
    ROOT::EnableImplicitMT();
    std::cout << "Running with maximum number of threads" << std::endl;
  }
}

// ROOT_TTREECACHE_SIZE is a multiplier of ROOT's estimated cluster size, not
// a byte count. Keep an explicit size separate so its units remain clear.
static std::optional<Long64_t>
setupROOTInputCache(const IConfigurationProvider &configProvider) {
  const auto &config = configProvider.getConfigMap();
  auto setting = [&config](const char *key) -> std::string {
    auto it = config.find(key);
    if (it == config.end()) return {};
    const auto first = it->second.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return {};
    const auto last = it->second.find_last_not_of(" \t\n\r");
    return it->second.substr(first, last - first + 1);
  };
  auto integer = [](const std::string &value, const char *key, long long max) {
    try {
      size_t end = 0;
      const auto result = std::stoll(value, &end);
      if (end == value.size() && result >= 0 && result <= max) return result;
    } catch (const std::exception &) {}
    throw std::runtime_error("Invalid '" + std::string(key) + "' value '" +
                             value + "' (expected an integer from 0 to " +
                             std::to_string(max) + ").");
  };

  const auto fixedMB = setting("treeCacheSizeMB");
  const auto namedFactor = setting("rootTTreeCacheSize");
  const auto envFactor = setting("ROOT_TTREECACHE_SIZE");
  if (!namedFactor.empty() && !envFactor.empty()) {
    throw std::runtime_error("Set only one of rootTTreeCacheSize and "
                             "ROOT_TTREECACHE_SIZE.");
  }
  const auto factor = namedFactor.empty() ? envFactor : namedFactor;
  if (!fixedMB.empty() && !factor.empty()) {
    throw std::runtime_error("Set either treeCacheSizeMB or the "
                             "ROOT_TTREECACHE_SIZE factor, not both.");
  }

  std::optional<Long64_t> fixedBytes;
  if (!fixedMB.empty()) {
    fixedBytes = integer(fixedMB, "treeCacheSizeMB", 512) * 1024 * 1024;
    std::cout << "ROOT tree cache size: " << fixedMB << " MiB" << std::endl;
  }
  if (!factor.empty()) {
    try {
      size_t end = 0;
      const auto value = std::stod(factor, &end);
      if (end != factor.size() || !std::isfinite(value) || value <= 0 ||
          value > 16) throw std::invalid_argument("out of range");
    } catch (const std::exception &) {
      throw std::runtime_error("Invalid ROOT_TTREECACHE_SIZE factor '" + factor +
                               "' (expected a factor greater than 0 and at most 16).");
    }
    if (setenv("ROOT_TTREECACHE_SIZE", factor.c_str(), 1) != 0)
      throw std::runtime_error("Could not set ROOT_TTREECACHE_SIZE.");
    std::cout << "ROOT tree cache size factor: " << factor << std::endl;
  }

  const auto learnEntries = setting("treeCacheLearnEntries");
  if (!learnEntries.empty()) {
    const auto count = integer(learnEntries, "treeCacheLearnEntries", 100000);
    if (count == 0)
      throw std::runtime_error("treeCacheLearnEntries must be at least 1.");
    TTreeCache::SetLearnEntries(static_cast<Int_t>(count));
    std::cout << "ROOT tree cache learning entries: " << count << std::endl;
  }

  const auto prefill = setting("treeCachePrefill");
  if (!prefill.empty()) {
    integer(prefill, "treeCachePrefill", 1);
    if (setenv("ROOT_TTREECACHE_PREFILL", prefill.c_str(), 1) != 0)
      throw std::runtime_error("Could not set ROOT_TTREECACHE_PREFILL.");
    std::cout << "ROOT tree cache prefill: " << prefill << std::endl;
  }

  const auto async = setting("treeCacheAsyncPrefetch");
  if (!async.empty()) {
    integer(async, "treeCacheAsyncPrefetch", 1);
    gEnv->SetValue("TFile.AsyncReading", async.c_str());
    std::cout << "ROOT tree cache asynchronous prefetch: " << async
              << std::endl;
  }
  return fixedBytes;
}

static void setupROOTInputBackend(const IConfigurationProvider &configProvider) {
  const auto &config = configProvider.getConfigMap();
  auto setting = [&config](const char *key) -> std::string {
    const auto it = config.find(key);
    if (it == config.end()) return {};
    const auto first = it->second.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return {};
    const auto last = it->second.find_last_not_of(" \t\n\r");
    return it->second.substr(first, last - first + 1);
  };
  const auto backend = setting("rootInputBackend");
  const auto nextFilePrefetch = setting("treeCachePrefetchNextFile");
  const bool wantsNextFilePrefetch =
      nextFilePrefetch == "1" || nextFilePrefetch == "true" ||
      nextFilePrefetch == "True";
  if (backend.empty() || backend == "root") {
    if (wantsNextFilePrefetch)
      throw std::runtime_error(
          "treeCachePrefetchNextFile requires rootInputBackend=xrd-adaptor.");
    return;
  }
  if (backend != "xrd-adaptor")
    throw std::runtime_error("Unknown rootInputBackend '" + backend +
                             "' (expected 'root' or 'xrd-adaptor').");
#ifndef RDF_HAS_XRD_ADAPTOR
  throw std::runtime_error(
      "rootInputBackend=xrd-adaptor requires a build configured with "
      "-DRDF_ENABLE_XRD_ADAPTOR=ON and the XrdCl development package.");
#else
  auto integer = [&setting](const char *key, unsigned fallback,
                            unsigned maximum) {
    const auto value = setting(key);
    if (value.empty()) return fallback;
    try {
      std::size_t end = 0;
      const auto parsed = std::stoul(value, &end);
      if (end == value.size() && parsed <= maximum)
        return static_cast<unsigned>(parsed);
    } catch (const std::exception &) {
    }
    throw std::runtime_error("Invalid '" + std::string(key) + "' value '" +
                             value + "'.");
  };
  const auto probeSeconds = integer("xrdSourceProbeIntervalSeconds", 5, 300);
  const auto cacheMB = integer("xrdPrefetchCacheMB", 256, 131072);
  const auto preferredSiteBand = integer("xrdPreferredSiteBandPercent", 20, 100);
  const auto explorationShare = integer("xrdExplorationSharePercent", 10, 50);
  const auto maxReadSources = integer("xrdMaxReadSources", 2, 2);
  if (maxReadSources == 0)
    throw std::runtime_error(
        "Invalid 'xrdMaxReadSources' value '0' (expected 1 or 2).");
  const auto readTimeoutSeconds = integer("xrdReadTimeoutSeconds", 45, 300);
  if (readTimeoutSeconds == 0)
    throw std::runtime_error(
        "Invalid 'xrdReadTimeoutSeconds' value '0' (expected 1 to 300).");
  rdf::xrd::configureXrdAdaptor(
      probeSeconds, static_cast<std::size_t>(cacheMB) * 1024 * 1024,
      preferredSiteBand, explorationShare, maxReadSources,
      readTimeoutSeconds);
  rdf::xrd::registerXrdAdaptor();
  std::cout << "ROOT input backend: XrdAdaptor (source probe interval="
            << probeSeconds << " s, handoff cache=" << cacheMB
            << " MiB, preferred-site band=" << preferredSiteBand
            << "% with 10% hysteresis, exploration sample budget="
            << explorationShare << "% max 2 MiB/read, max full-read sites="
            << maxReadSources << ", read/request timeout="
            << readTimeoutSeconds << " s, exploration site enabled="
            << (explorationShare > 0 ? "yes" : "no") << ")\n";
#endif
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

  // Register an optional TFile backend before TChain::Add opens URLs.
  setupROOTInputBackend(configProvider);

  // Apply I/O controls before TChain::Add opens remote files. An explicit
  // cache size is stored by TChain and carried to each file it opens.
  const auto fixedCacheBytes = setupROOTInputCache(configProvider);

  // Permanently suppress non-fatal ROOT warnings (e.g. "no dictionary for
  // class ..." when opening NanoAOD files with residual CMSSW objects).
  // These warnings are emitted during TChain::Add(), RDataFrame construction,
  // and lazy evaluation, so the suppression must outlive the makeTChain scope.
  gErrorIgnoreLevel = kError;

  std::vector<std::unique_ptr<TChain>> tchainVector;
  auto treeListVec = getTreeList(configProvider);
  for (const auto &tree : treeListVec) {
    // Only the primary sequential input uses the optimized policy. Friends
    // may use indexed/non-sequential entry access and keep ROOT's defaults.
    tchainVector.emplace_back(
        makeInputCacheChain(tree, configProvider, tchainVector.empty()));
    if (fixedCacheBytes && tchainVector.back()->SetCacheSize(*fixedCacheBytes) != 0)
      throw std::runtime_error("Could not set TChain cache size.");
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
