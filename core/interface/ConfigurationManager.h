#ifndef CONFIGURATIONMANAGER_H_INCLUDED
#define CONFIGURATIONMANAGER_H_INCLUDED

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

/**
 * @class ConfigurationManager
 * @brief Handles parsing and storing configuration values.
 *
 * Suggested methods: get(key), set(key, value), loadConfig(file), etc.
 */
class ConfigurationManager {
public:
  /**
   * @brief Construct a new ConfigurationManager object
   * @param configFile Path to the configuration file
   */
  ConfigurationManager(const std::string &configFile);

  /**
   * @brief Get the configuration map
   * @return Reference to the configuration map
   */
  const std::unordered_map<std::string, std::string> &getConfigMap() const;

  /**
   * @brief Get a configuration value by key
   * @param key Configuration key
   * @return Configuration value
   */
  std::string get(const std::string &key) const;

  /**
   * @brief Set a configuration value by key
   * @param key Configuration key
   * @param value Value to set
   */
  void set(const std::string &key, const std::string &value);

  /**
   * @brief Parse a multi-key configuration
   * @param key Configuration key
   * @param requiredEntryKeys Required entry keys
   * @return Vector of configuration maps
   */
  std::vector<std::unordered_map<std::string, std::string>>
  parseMultiKeyConfig(std::string_view key,
                      const std::vector<std::string> &requiredEntryKeys) const;

  /**
   * @brief Parse a pair-based configuration
   * @param configFile Path to the configuration file
   * @return Map of configuration values
   */
  std::unordered_map<std::string, std::string>
  parsePairBasedConfig(const std::string &configFile) const;

  /**
   * @brief Extract a vector entry from a configuration key
   * @param key Configuration key
   * @return Vector of configuration values
   */
  std::vector<std::string> extractVectorEntry(std::string_view key) const;

  /**
   * @brief Parse a vector configuration
   * @param configFile Path to the configuration file
   * @return Vector of configuration values
   */
  std::vector<std::string>
  parseVectorConfig(const std::string &configFile) const;

  /**
   * @brief Get a list of configuration values
   * @param key Configuration key
   * @param defaultValue Default value
   * @param delimiter Delimiter
   * @return Vector of configuration values
   */
  std::vector<std::string>
  getList(const std::string &key,
          const std::vector<std::string> &defaultValue = {},
          const std::string &delimiter = ",") const;

  /**
   * @brief Split a string into a vector
   * @param input Input string
   * @param delimiter Delimiter
   * @return Vector of strings
   */
  std::vector<std::string> splitString(std::string_view input,
                                       std::string_view delimiter) const;

private:
  std::unordered_map<std::string, std::string> configMap_m;

  /**
   * @brief Strip comments from a string
   * @param line Input string
   * @return String with comments stripped
   */
  std::string stripComment(const std::string &line) const;

  /**
   * @brief Trim whitespace from a string
   * @param s Input string
   * @return String with whitespace trimmed
   */
  std::string_view trim(std::string_view s) const;

  /**
   * @brief Parse a pair from a string
   * @param line Input string
   * @param stripComments Whether to strip comments
   * @return Pair of strings
   */
  std::pair<std::string, std::string> parsePair(const std::string &line,
                                                bool stripComments) const;

  /**
   * @brief Parse an entry from a string
   * @param entry Input string
   * @return Map of configuration values
   */
  std::unordered_map<std::string, std::string>
  parseEntry(const std::string &entry) const;

  /**
   * @brief Process the top-level configuration
   * @param configFile Path to the configuration file
   */
  void processTopLevelConfig(const std::string &configFile);
};

#endif // CONFIGURATIONMANAGER_H_INCLUDED