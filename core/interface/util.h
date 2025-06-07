#ifndef UTIL_H_INCLUDED
#define UTIL_H_INCLUDED


#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <TChain.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDFHelpers.hxx>



#include <plots.h>

int scan(TChain &chain, const std::string &directory, const std::vector<std::string> &globs, const std::vector<std::string> &antiglobs, bool base=true);

void save(std::vector<std::vector<histInfo>> &fullHistList, const histHolder &hists, const std::vector<std::vector<std::string>> &allRegionNames, const std::string &fileName);

std::unordered_map<std::string, std::string> readConfig(const std::string &configFile);

std::vector<std::string> splitString(const std::string &input, const std::string &delimiter);

std::unordered_map<std::string, std::string> processConfig(const std::string &configFile);

std::vector<std::string> configToVector(const std::string &configFile);

std::unique_ptr<TChain> makeTChain(const std::unordered_map<std::string, std::string> &configMap);

ROOT::RDF::RNode saveDF(const ROOT::RDF::RNode &df, const std::unordered_map<std::string, std::string> &configMap, 
    const std::unordered_map<std::string, std::unordered_set<std::string>> &variableToSystematicMap);

ROOT::RDF::RNode addConstants(const ROOT::RDF::RNode &df, const std::unordered_map<std::string, std::string> &configMap);

# endif

