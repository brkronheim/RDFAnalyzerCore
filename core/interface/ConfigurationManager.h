#ifndef CONFIGURATIONMANAGER_H_INCLUDED
#define CONFIGURATIONMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

/**
 * @class ConfigurationManager
 * @brief Handles parsing and storing configuration values.
 *
 * Implements IConfigurationProvider interface for better dependency injection.
 */
class ConfigurationManager : public IConfigurationProvider {
public:
  /**
   * @brief Construct a new ConfigurationManager object
   * @param configFile Path to the configuration file
   *
   * Tested in test/testConfigurationManager.cc
   */
  ConfigurationManager(const std::string &configFile);

  /**
   * @brief Get the configuration map
   * @return Reference to the configuration map
   *
   * Tested in SetAndGetWorks in test/testConfigurationManager.cc.
   */
  const std::unordered_map<std::string, std::string> &getConfigMap() const override;

  /**
   * @brief Get a configuration value by key
   * @param key Configuration key
   * @return Configuration value
   *
   * Tested in GetReturnsCorrectValue in test/testConfigurationManager.cc.
   */
  std::string get(const std::string &key) const override;

  /**
   * @brief Set a configuration value by key
   * @param key Configuration key
   * @param value Value to set
   *
   * Tested in SetAndGetWorks in test/testConfigurationManager.cc.
   */
  void set(const std::string &key, const std::string &value) override;

  /**
   * @brief Parse a multi-key configuration from a file
   * @param configFile Path to the configuration file to parse
   * @param requiredEntryKeys Required entry keys
   * @return Vector of configuration maps
   */
  std::vector<std::unordered_map<std::string, std::string>>
  parseMultiKeyConfig(const std::string &configFile,
                      const std::vector<std::string> &requiredEntryKeys) const override;

  /**
   * @brief Parse a pair-based configuration
   * @param configFile Path to the configuration file
   * @return Map of configuration values
   *
   * Tested when constructor is called in test/testConfigurationManager.cc.
   */
  std::unordered_map<std::string, std::string>
  parsePairBasedConfig(const std::string &configFile) const override;

  /**
   * @brief Parse a vector configuration
   * @param configFile Path to the configuration file
   * @return Vector of configuration values
   *
   * Tested in ParseVectorConfigWorks in test/testConfigurationManager.cc.
   */
  std::vector<std::string>
  parseVectorConfig(const std::string &configFile) const override;

  /**
   * @brief Get a list of configuration values
   * @param key Configuration key
   * @param defaultValue Default value
   * @param delimiter Delimiter
   * @return Vector of configuration values
   *
   * Tested in GetListWorks in test/testConfigurationManager.cc.
   */
  std::vector<std::string>
  getList(const std::string &key,
          const std::vector<std::string> &defaultValue = {},
          const std::string &delimiter = ",") const override;

  /**
   * @brief Split a string into a vector
   * @param input Input string
   * @param delimiter Delimiter
   * @return Vector of strings
   *
   * Tested in SplitStringWorks in test/testConfigurationManager.cc.
   */
  std::vector<std::string> splitString(std::string_view input,
                                       std::string_view delimiter) const override;

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
   *
   */
  void processTopLevelConfig(const std::string &configFile);
};

#endif // CONFIGURATIONMANAGER_H_INCLUDED