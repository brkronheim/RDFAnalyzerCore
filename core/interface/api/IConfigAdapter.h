#ifndef ICONFIGADAPTER_H_INCLUDED
#define ICONFIGADAPTER_H_INCLUDED

#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Interface for configuration parsing adapters.
 */
class IConfigAdapter {
public:
  virtual ~IConfigAdapter() = default;

  virtual std::unordered_map<std::string, std::string>
  parsePairBasedConfig(const std::string &configFile) const = 0;

  virtual std::vector<std::unordered_map<std::string, std::string>>
  parseMultiKeyConfig(const std::string &configFile,
                      const std::vector<std::string> &requiredEntryKeys) const = 0;

  virtual std::vector<std::string>
  parseVectorConfig(const std::string &configFile) const = 0;
};

#endif // ICONFIGADAPTER_H_INCLUDED
