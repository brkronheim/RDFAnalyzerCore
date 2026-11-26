#pragma once

#include <cctype>
#include <stdexcept>
#include <string>

namespace rdf_config_detail {

inline bool parseBooleanConfig(const std::string& key, const std::string& value, bool fallback) {
  if (value.empty()) return fallback;
  std::string normalized;
  normalized.reserve(value.size());
  for (const unsigned char ch : value)
    normalized.push_back(static_cast<char>(std::tolower(ch)));
  if (normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on")
    return true;
  if (normalized == "0" || normalized == "false" || normalized == "no" || normalized == "off")
    return false;
  throw std::invalid_argument("invalid boolean value '" + value + "' for configuration key '" + key + "'");
}

} // namespace rdf_config_detail
