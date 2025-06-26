#include <ConfigurationManager.h>
#include <fstream>
#include <iostream>

/**
 * @brief Construct a new ConfigurationManager object
 * @param configFile Path to the configuration file
 */
ConfigurationManager::ConfigurationManager(const std::string &configFile) {
  processTopLevelConfig(configFile);
}

/**
 * @brief Get the configuration map
 * @return Reference to the configuration map
 */
const std::unordered_map<std::string, std::string> &
ConfigurationManager::getConfigMap() const {
  return configMap_m;
}

/**
 * @brief Get a configuration value by key
 * @param key Configuration key
 * @return Configuration value
 */
std::string ConfigurationManager::get(const std::string &key) const {
  auto it = configMap_m.find(key);
  if (it != configMap_m.end()) {
    return it->second;
  }
  return "";
}

/**
 * @brief Set a configuration value by key
 * @param key Configuration key
 * @param value Value to set
 */
void ConfigurationManager::set(const std::string &key,
                               const std::string &value) {
  configMap_m[key] = value;
}

/**
 * @brief Strip comments from a line
 * @param line Input line
 * @return Line with comments removed
 */
std::string ConfigurationManager::stripComment(const std::string &line) const {
  return line.substr(0, line.find("#"));
}

/**
 * @brief Trim whitespace from a string_view
 * @param s Input string_view
 * @return Trimmed string_view
 */
std::string_view ConfigurationManager::trim(std::string_view s) const {
  size_t first = s.find_first_not_of(" \t\n\r");
  size_t last = s.find_last_not_of(" \t\n\r");
  if (first == std::string_view::npos || last == std::string_view::npos) {
    return {};
  }
  return s.substr(first, last - first + 1);
}

/**
 * @brief Split a string_view by a delimiter
 * @param input Input string_view
 * @param delimiter Delimiter string_view
 * @return Vector of split strings
 */
std::vector<std::string>
ConfigurationManager::splitString(std::string_view input,
                                  std::string_view delimiter) const {
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
 * @brief Parse a key-value pair from a line
 * @param line Input line
 * @param stripComments Whether to strip comments
 * @return Pair of key and value strings
 */
std::pair<std::string, std::string>
ConfigurationManager::parsePair(const std::string &line,
                                bool stripComments) const {
  std::string processedLine = stripComments ? stripComment(line) : line;
  auto splitIndex = processedLine.find("=");
  if (splitIndex == std::string::npos) {
    return {"", ""};
  }
  auto key = std::string(trim(processedLine.substr(0, splitIndex)));
  auto value = std::string(trim(processedLine.substr(splitIndex + 1)));
  return {key, value};
}

/**
 * @brief Parse an entry into a map of keys and values
 * @param entry Input entry string
 * @return Map of entry keys and values
 */
std::unordered_map<std::string, std::string>
ConfigurationManager::parseEntry(const std::string &entry) const {
  std::unordered_map<std::string, std::string> entryKeys;
  auto splitEntry = splitString(entry, " ");
  for (auto &pair : splitEntry) {
    auto [key, value] = parsePair(pair, false);
    if (!key.empty() && !value.empty()) {
      entryKeys[key] = value;
    }
  }
  return entryKeys;
}

/**
 * @brief Process the top-level configuration file
 * @param configFile Path to the configuration file
 */
void ConfigurationManager::processTopLevelConfig(
    const std::string &configFile) {
  configMap_m = parsePairBasedConfig(configFile);
}

/**
 * @brief Parse a pair-based configuration file
 * @param configFile Path to the configuration file
 * @return Map of configuration keys and values
 */
std::unordered_map<std::string, std::string>
ConfigurationManager::parsePairBasedConfig(
    const std::string &configFile) const {
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
 * @brief Parse a multi-key configuration entry
 * @param key Key to look up
 * @param requiredEntryKeys Vector of required entry keys
 * @return Vector of maps of entry keys and values
 */
std::vector<std::unordered_map<std::string, std::string>>
ConfigurationManager::parseMultiKeyConfig(
    std::string_view key,
    const std::vector<std::string> &requiredEntryKeys) const {
  std::vector<std::unordered_map<std::string, std::string>> parsedConfig;
  auto config = splitString(configMap_m.count(std::string(key))
                                ? configMap_m.at(std::string(key))
                                : "",
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
 * @brief Extract a vector entry from the configuration map
 * @param key Key to look up
 * @return Vector of strings from the entry
 */
std::vector<std::string>
ConfigurationManager::extractVectorEntry(std::string_view key) const {
  std::vector<std::string> parsedConfig;
  auto it = configMap_m.find(std::string(key));
  if (it != configMap_m.end()) {
    parsedConfig = splitString(it->second, "\n");
  }
  return parsedConfig;
}

/**
 * @brief Parse a vector-based configuration file
 * @param configFile Path to the configuration file
 * @return Vector of configuration lines
 */
std::vector<std::string>
ConfigurationManager::parseVectorConfig(const std::string &configFile) const {
  std::vector<std::string> configVector;
  std::ifstream file(configFile);
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      line = line.substr(0, line.find("#"));
      configVector.push_back(line);
    }
    file.close();
  } else {
    std::cerr << "Error: Configuration file " << configFile
              << " could not be opened." << std::endl;
  }
  return configVector;
}

/**
 * @brief Get a list from the configuration map, split by delimiter
 * @param key Key to look up
 * @param defaultValue Default value if key not found
 * @param delimiter Delimiter string
 * @return Vector of split strings
 */
std::vector<std::string>
ConfigurationManager::getList(const std::string &key,
                              const std::vector<std::string> &defaultValue,
                              const std::string &delimiter) const {
  if (configMap_m.find(key) != configMap_m.end()) {
    return splitString(configMap_m.at(key), delimiter);
  }
  return defaultValue;
}