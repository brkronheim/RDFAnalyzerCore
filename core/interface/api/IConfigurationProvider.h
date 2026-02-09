#ifndef ICONFIGURATIONPROVIDER_H_INCLUDED
#define ICONFIGURATIONPROVIDER_H_INCLUDED

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

/**
 * @brief Interface for configuration providers to enable dependency injection
 * 
 * This interface abstracts configuration access, allowing for better testing
 * and more flexible dependency injection patterns.
 */
class IConfigurationProvider {
public:
    virtual ~IConfigurationProvider() = default;
    
    /**
     * @brief Get a configuration value by key
     * @param key Configuration key
     * @return Configuration value
     */
    virtual std::string get(const std::string &key) const = 0;
    
    /**
     * @brief Set a configuration value by key
     * @param key Configuration key
     * @param value Value to set
     */
    virtual void set(const std::string &key, const std::string &value) = 0;
    
    /**
     * @brief Get the configuration map
     * @return Reference to the configuration map
     */
    virtual const std::unordered_map<std::string, std::string> &getConfigMap() const = 0;
    
    /**
     * @brief Parse a multi-key configuration from a file
     * @param configFile Path to the configuration file to parse
     * @param requiredEntryKeys Required entry keys
     * @return Vector of configuration maps
     */
    virtual std::vector<std::unordered_map<std::string, std::string>>
    parseMultiKeyConfig(const std::string &configFile,
                        const std::vector<std::string> &requiredEntryKeys) const = 0;
    
    /**
     * @brief Parse a pair-based configuration
     * @param configFile Path to the configuration file
     * @return Map of configuration values
     */
    virtual std::unordered_map<std::string, std::string>
    parsePairBasedConfig(const std::string &configFile) const = 0;
    
    /**
     * @brief Parse a vector configuration
     * @param configFile Path to the configuration file
     * @return Vector of configuration values
     */
    virtual std::vector<std::string>
    parseVectorConfig(const std::string &configFile) const = 0;
    
    /**
     * @brief Get a list of configuration values
     * @param key Configuration key
     * @param defaultValue Default value
     * @param delimiter Delimiter
     * @return Vector of configuration values
     */
    virtual std::vector<std::string>
    getList(const std::string &key,
            const std::vector<std::string> &defaultValue = {},
            const std::string &delimiter = ",") const = 0;
    
    /**
     * @brief Split a string into a vector
     * @param input Input string
     * @param delimiter Delimiter
     * @return Vector of strings
     */
    virtual std::vector<std::string> splitString(std::string_view input,
                                                 std::string_view delimiter) const = 0;
};

#endif // ICONFIGURATIONPROVIDER_H_INCLUDED 