#ifndef TEXTCONFIGADAPTER_H_INCLUDED
#define TEXTCONFIGADAPTER_H_INCLUDED

#include "api/IConfigAdapter.h"
#include <string_view>

/**
 * @brief Default text-based configuration adapter (current behavior).
 */
class TextConfigAdapter : public IConfigAdapter {
public:
  std::unordered_map<std::string, std::string>
  parsePairBasedConfig(const std::string &configFile) const override;

  std::vector<std::unordered_map<std::string, std::string>>
  parseMultiKeyConfig(const std::string &configFile,
                      const std::vector<std::string> &requiredEntryKeys) const override;

  std::vector<std::string>
  parseVectorConfig(const std::string &configFile) const override;

private:
  std::string stripComment(const std::string &line) const;
  std::string_view trim(std::string_view s) const;
  std::pair<std::string, std::string> parsePair(const std::string &line,
                                                bool stripComments) const;
  std::vector<std::string> splitString(std::string_view input,
                                       std::string_view delimiter) const;
};

#endif // TEXTCONFIGADAPTER_H_INCLUDED
