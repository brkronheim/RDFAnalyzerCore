/**
 * @file configParser.h
 * @brief Utility function declarations for configuration file parsing and processing.
 *
 * This header declares helper functions for reading, parsing, and processing configuration files and entries for the analysis framework.
 */
#ifndef CONFIGPARSER_H_INCLUDED
#define CONFIGPARSER_H_INCLUDED

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>

#include <plots.h>

namespace ConfigParser {
    /**
    * @brief Process the top-level configuration file and setup ROOT environment
    * 
    * Reads the configuration file, sets up ROOT error handling,
    * and configures multi-threading based on the config.
    *
    * @param configFile Path to the configuration file
    * @return Map of processed configuration key-value pairs
    */
    std::unordered_map<std::string, std::string>
    processTopLevelConfig(const std::string &configFile);


    /**
    * @brief Read and parse a pair based configuration file
    * 
    * Reads a file line by line, parsing each line into key-value pairs.
    * Comments (starting with #) are stripped, and empty lines are ignored.
    *
    * It is assumed that all lines have a single key-value pair.
    *
    * @param configFile Path to the configuration file
    * @return Map of configuration key-value pairs
    */
    std::unordered_map<std::string, std::string>
    parsePairBasedConfig(const std::string &configFile);

    /**
    * @brief Parse a configuration map for entries with required keys
    * 
    * Extracts entries from a configuration map that contain all required keys.
    * Each entry is expected to be a newline-separated list of key-value pairs.
    *
    * @param configMap Source configuration map
    * @param key Key in configMap containing the entries to parse
    * @param requiredEntryKeys List of keys that must be present in each entry
    * @return Vector of maps, each containing the key-value pairs from valid entries
    */
    std::vector<std::unordered_map<std::string, std::string>>
    parseMultiKeyConfig(const std::unordered_map<std::string, std::string> &configMap,
                std::string_view key,
                const std::vector<std::string> &requiredEntryKeys);

    /**
    * @brief Parse a configuration vector from a configuration map
    * 
    * Extracts a newline-separated list of strings from a configuration map.
    *
    * @param configMap Source configuration map
    * @param key Key to look up in the map
    * @return Vector of strings split from the configuration value
    */
    std::vector<std::string>
    extractVectorEntry(const std::unordered_map<std::string, std::string> &configMap,
                    std::string_view key);

    /**
    * @brief Convert a configuration file to a vector of strings
    * 
    * Reads a configuration file line by line, stripping comments and
    * returning a vector of non-empty lines.
    *
    * @param configFile Path to the configuration file
    * @return Vector of non-empty lines from the configuration file
    */  
    std::vector<std::string> parseVectorConfig(const std::string &configFile);

    /**
    * @brief Split a string into a vector of strings using a delimiter
    * 
    * Splits the input string on the delimiter and trims whitespace from each part.
    * Empty parts after trimming are omitted from the result.
    *
    * @param input String to split
    * @param delimiter String to split on
    * @return Vector of non-empty trimmed substrings
    */
    std::vector<std::string> splitString(std::string_view input,
                                        std::string_view delimiter);


    /**
    * @brief Get a list of strings from a config map with optional defaults
    * @param configMap Configuration map to search
    * @param key Key to look up in the map
    * @param defaultValue Default value if key not found
    * @param delimiter String delimiter to split values
    * @return Vector of strings from the config, or default value if not found
    */
    std::vector<std::string> getList(const std::unordered_map<std::string, std::string> &configMap,
            const std::string &key,
            const std::vector<std::string> &defaultValue = {},
            const std::string &delimiter = ",");

}


#endif
