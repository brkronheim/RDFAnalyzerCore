/**
 * @file util.h
 * @brief Utility function declarations for configuration, file handling, and
 * ROOT data structures.
 *
 * This header declares helper functions for reading configuration files,
 * scanning directories, splitting strings, and setting up ROOT data structures
 * such as TChain and RDataFrame.
 */
#ifndef UTIL_H_INCLUDED
#define UTIL_H_INCLUDED

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>

#include <api/IConfigurationProvider.h>

#include <plots.h>

/**
 * @brief Specification for a single friend tree or sidecar input.
 *
 * Used by DataManager::registerFriendTrees() to attach additional columns from
 * separate ROOT files to the main TChain before the RDataFrame is created.
 * Supports both explicit file lists (local paths or XRootD URLs) and
 * directory-based file discovery with glob/antiglob patterns.
 *
 * Example YAML entry (inside a ``friends:`` sequence):
 * @code{.yaml}
 *   - alias: calib
 *     treeName: Events
 *     fileList:
 *       - /path/to/calib.root
 *       - root://server.cern.ch//path/to/remote.root
 *     indexBranches: [run, luminosityBlock, event]
 * @endcode
 */
struct FriendTreeSpec {
    /// Alias used to access friend branches (e.g. "calib" gives "calib.pt").
    std::string alias;
    /// Name of the TTree inside the friend ROOT file(s). Defaults to "Events".
    std::string treeName{"Events"};
    /// Explicit list of ROOT file paths or XRootD URLs.
    std::vector<std::string> files;
    /// Directory to scan for ROOT files (alternative to explicit files list).
    std::string directory;
    /// File name patterns to include when scanning directory (default: {".root"}).
    std::vector<std::string> globs{".root"};
    /// File name patterns to exclude when scanning directory (default: {"FAIL"}).
    std::vector<std::string> antiglobs{"FAIL"};
    /**
     * @brief Branch names used as event identifiers for index-based matching.
     *
     * When non-empty, a TTreeIndex is built using the first two branches as the
     * major and minor index keys (ROOT BuildIndex convention). This enables
     * correct event matching even when file ordering differs between the main
     * tree and the friend tree. Common values: {"run", "luminosityBlock"} or
     * {"run", "event"}. Leave empty for position-based (sequential) matching.
     */
    std::vector<std::string> indexBranches;
};

/**
 * @brief Parse a friend-tree YAML configuration file into a list of specs.
 *
 * The YAML file must contain a top-level ``friends:`` sequence. Each element
 * must have an ``alias`` field and either a ``fileList`` sequence or a
 * ``directory`` string. Optional keys: ``treeName``, ``globs``,
 * ``antiglobs``, ``indexBranches``.
 *
 * @param configFile Path to the YAML configuration file.
 * @return Vector of parsed FriendTreeSpec objects (empty if file is missing
 *         or contains no ``friends`` key).
 * @throws std::runtime_error if the file cannot be parsed as valid YAML.
 */
std::vector<FriendTreeSpec>
parseFriendTreeConfig(const std::string &configFile);

/**
 * @brief Scan a directory for ROOT files matching globs and add them to a
 * TChain.
 * @param chain TChain to add files to
 * @param directory Directory to scan
 * @param globs List of substrings to match (include)
 * @param antiglobs List of substrings to exclude
 * @param base If true, only scan the top-level directory
 * @return Number of files found and added
 */
int scan(TChain &chain, const std::string &directory,
         const std::vector<std::string> &globs,
         const std::vector<std::string> &antiglobs, bool base = true);

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
makeTChain(const IConfigurationProvider &configProvider);

void save(std::vector<std::vector<histInfo>> &fullHistList,
          const histHolder &hists,
          const std::vector<std::vector<std::string>> &allRegionNames,
          const std::string &fileName);

#endif
