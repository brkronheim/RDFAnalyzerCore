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


namespace ConfigParser {

    /**
    * @brief Strip comments from a line of text
    * @param line Input line
    * @return Line with comments (starting with #) removed
    */
    static std::string stripComment(const std::string &line) {
        return line.substr(0, line.find("#"));
    }


    /**
    * @brief Trim whitespace from both ends of a string view
    * @param s String view to trim
    * @return Trimmed string view, empty if string is all whitespace
    */
    static std::string_view trim(std::string_view s) {
        size_t first = s.find_first_not_of(" \t\n\r");
        size_t last = s.find_last_not_of(" \t\n\r");
        if (first == std::string_view::npos || last == std::string_view::npos) {
            return {};
        }
        return s.substr(first, last - first + 1);
    }


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
                                        std::string_view delimiter) {
        std::vector<std::string> splitStringVector;
        size_t pos = 0, prev = 0;
        while ((pos = input.find(delimiter, prev)) != std::string_view::npos) {
            auto token = trim(input.substr(prev, pos - prev));
            if (!token.empty()) {
                splitStringVector.emplace_back(token);
            }
            prev = pos + delimiter.size();
        }
        auto token = trim(input.substr(prev));
        if (!token.empty()) {
            splitStringVector.emplace_back(token);
        }
        return splitStringVector;
    }

    /**
    * @brief Split a line into a key-value pair on the first '=' character, optionally stripping comments
    * @param line Line to parse
    * @param stripComments Whether to remove comments (starting with #)
    * @return Pair of strings containing key and value, empty strings if parsing fails
    */
    static std::pair<std::string, std::string>
    parsePair(const std::string &line, bool stripComments) {
        std::string processedLine =
            stripComments ? stripComment(line) : line;
        auto splitIndex = processedLine.find("=");
        if (splitIndex == std::string::npos){
            return {"", ""};
        }
        auto key = std::string(trim(processedLine.substr(0, splitIndex)));
        auto value = std::string(trim(processedLine.substr(splitIndex + 1)));
        return {key, value};
    }

    /**
    * @brief Parse a config entry into a map of key-value pairs
    * @param entry Entry string to parse (space-separated key=value pairs)
    * @return Map of parsed key-value pairs
    */
    static std::unordered_map<std::string, std::string>
    parseEntry(const std::string &entry) {
        std::unordered_map<std::string, std::string> entryKeys;
        auto splitEntry = ConfigParser::splitString(entry, " "); // split on spaces to isolate key-value pairs
        for (auto &pair : splitEntry) {
            auto [key, value] = parsePair(pair, false); // get key and value from pair
            if (!key.empty() && !value.empty()) {
                entryKeys[key] = value;
            }
        }
        return entryKeys;
    }


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
    parsePairBasedConfig(const std::string &configFile) {
        std::unordered_map<std::string, std::string> configMap;
        std::ifstream file(configFile);
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                auto [key, value] = parsePair(line, true);
                if (!key.empty() && !value.empty()) {
                    configMap.emplace(key, value);
                }
            }
            file.close();
        } else {
            std::cerr << "Error: Configuration file " << configFile
                    << " could not be opened." << std::endl;
        }
        return configMap;
    }

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
    processTopLevelConfig(const std::string &configFile) {
        gROOT->ProcessLine("gErrorIgnoreLevel = 2001;");
        auto configMap = parsePairBasedConfig(configFile);
        //setupROOTThreads(configMap);
        return configMap;
    }

    


    

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
                const std::vector<std::string> &requiredEntryKeys) {
        std::vector<std::unordered_map<std::string, std::string>> parsedConfig;
        auto config = splitString(
            configMap.count(std::string(key)) ? configMap.at(std::string(key)) : "",
            "\n");
        for (auto &entry : config) {
            auto entryKeys = parseEntry(entry);
            bool allEntryKeysFound = true;
            for (const auto &entryKey : requiredEntryKeys) {
                if (entryKeys.find(entryKey) == entryKeys.end()) {
                    allEntryKeysFound = false;
                    break;
                }
            }
            if (allEntryKeysFound) {
                parsedConfig.push_back(entryKeys);
            }
        }
        return parsedConfig;
    }

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
                    std::string_view key) {
        std::vector<std::string> parsedConfig;
        auto it = configMap.find(std::string(key));
        if (it != configMap.end()) {
            parsedConfig = splitString(it->second, "\n");
        }
        return parsedConfig;
    }

    /**
    * @brief Convert a configuration file to a vector of strings
    * 
    * Reads a configuration file line by line, stripping comments and
    * returning a vector of non-empty lines.
    *
    * @param configFile Path to the configuration file
    * @return Vector of non-empty lines from the configuration file
    */  
    std::vector<std::string> parseVectorConfig(const std::string &configFile){
        std::vector<std::string> configVector;

        // iterate over the config file
        std::ifstream file(configFile);
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                line = line.substr(0,line.find("#")); // drop everything after the comment
                configVector.push_back(line);
            }
            file.close();
        } else {
            std::cerr << "Error: Configuration file " << configFile << " could not be opened." << std::endl;
        }

        return(configVector);
    }


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
            const std::vector<std::string> &defaultValue,
            const std::string &delimiter) {
        if (configMap.find(key) != configMap.end()) {
            return ConfigParser::splitString(configMap.at(key), delimiter);
        }
        return defaultValue;
    }


}    


