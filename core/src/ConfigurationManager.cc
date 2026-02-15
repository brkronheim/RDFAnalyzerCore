#include <ConfigurationManager.h>
#include <TextConfigAdapter.h>
#include <YamlConfigAdapter.h>
#include <stdexcept>
#include <filesystem>

namespace {
  // Helper function for C++17 compatibility (ends_with is C++20)
  bool endsWith(const std::string& str, const std::string& suffix) {
    if (suffix.size() > str.size()) return false;
    return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
  }
}

/**
 * @brief Construct a new ConfigurationManager object
 * @param configFile Path to the configuration file
 */
ConfigurationManager::ConfigurationManager(const std::string &configFile) {
  // Auto-detect format based on file extension
  if (endsWith(configFile, ".yaml") || endsWith(configFile, ".yml")) {
    adapter_m = std::make_shared<YamlConfigAdapter>();
  } else {
    adapter_m = std::make_shared<TextConfigAdapter>();
  }
  processTopLevelConfig(configFile);
}

ConfigurationManager::ConfigurationManager(const std::string &configFile,
                                           std::shared_ptr<IConfigAdapter> adapter)
    : adapter_m(std::move(adapter)) {
  if (!adapter_m) {
    adapter_m = std::make_shared<TextConfigAdapter>();
  }
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
  if (get(key) != "") {
    throw std::runtime_error(
        "Error: Key " + key +
        " already exists. Do not use set to overwrite existing keys.");
  } else {
    configMap_m[key] = value;
  }
}

/**
 * @brief Strip comments from a line
 * @param line Input line
 * @return Line with comments removed
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
/**
 * @brief Process the top-level configuration file
 * @param configFile Path to the configuration file
 */
void ConfigurationManager::processTopLevelConfig(
    const std::string &configFile) {
  // Store the directory of the config file for resolving relative paths
  std::filesystem::path configPath(configFile);
  if (configPath.has_parent_path()) {
    configBasePath_m = configPath.parent_path().string();
  } else {
    configBasePath_m = ".";
  }
  
  configMap_m = adapter_m->parsePairBasedConfig(configFile);
}

/**
 * @brief Resolve a config file path relative to the base config directory
 * @param path Path from config file (may be relative or absolute)
 * @return Resolved absolute path
 */
std::string ConfigurationManager::resolveConfigPath(const std::string &path) const {
  if (path.empty()) {
    return path;
  }
  
  std::filesystem::path filePath(path);
  
  // If it's already absolute, return as-is
  if (filePath.is_absolute()) {
    return path;
  }
  
  // Otherwise, resolve relative to the config base path
  std::filesystem::path resolvedPath = std::filesystem::path(configBasePath_m) / filePath;
  return resolvedPath.string();
}

/**
 * @brief Parse a pair-based configuration file
 * @param configFile Path to the configuration file
 * @return Map of configuration keys and values
 */
std::unordered_map<std::string, std::string>
ConfigurationManager::parsePairBasedConfig(
    const std::string &configFile) const {
  return adapter_m->parsePairBasedConfig(resolveConfigPath(configFile));
}

/**
 * @brief Parse a multi-key configuration from a file
 * @param configFile Path to the configuration file to parse
 * @param requiredEntryKeys Vector of required entry keys
 * @return Vector of maps of entry keys and values
 */
std::vector<std::unordered_map<std::string, std::string>>
ConfigurationManager::parseMultiKeyConfig(
    const std::string &configFile,
    const std::vector<std::string> &requiredEntryKeys) const {
  return adapter_m->parseMultiKeyConfig(resolveConfigPath(configFile), requiredEntryKeys);
}

/**
 * @brief Parse a vector-based configuration file
 * @param configFile Path to the configuration file
 * @return Vector of configuration lines
 */
std::vector<std::string>
ConfigurationManager::parseVectorConfig(const std::string &configFile) const {
  return adapter_m->parseVectorConfig(resolveConfigPath(configFile));
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