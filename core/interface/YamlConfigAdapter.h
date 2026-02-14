#ifndef YAMLCONFIGADAPTER_H_INCLUDED
#define YAMLCONFIGADAPTER_H_INCLUDED

#include "api/IConfigAdapter.h"
#include <string_view>

/**
 * @brief YAML-based configuration adapter.
 */
class YamlConfigAdapter : public IConfigAdapter {
public:
  std::unordered_map<std::string, std::string>
  parsePairBasedConfig(const std::string &configFile) const override;

  std::vector<std::unordered_map<std::string, std::string>>
  parseMultiKeyConfig(const std::string &configFile,
                      const std::vector<std::string> &requiredEntryKeys) const override;

  std::vector<std::string>
  parseVectorConfig(const std::string &configFile) const override;

private:
  std::string_view trim(std::string_view s) const;
};

#endif // YAMLCONFIGADAPTER_H_INCLUDED
