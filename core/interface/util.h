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

#include <plots.h>

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
 * @param configMap Configuration map containing input settings
 * @return Vector of unique_ptr to configured TChain objects
 */
std::vector<std::unique_ptr<TChain>>
makeTChain(std::unordered_map<std::string, std::string> &configMap);

void save(std::vector<std::vector<histInfo>> &fullHistList,
          const histHolder &hists,
          const std::vector<std::vector<std::string>> &allRegionNames,
          const std::string &fileName);


ROOT::RDF::RNode
saveDF(ROOT::RDF::RNode &df,
       const std::unordered_map<std::string, std::string> &configMap,
       const std::unordered_map<std::string, std::unordered_set<std::string>>
           &variableToSystematicMap);











#endif
