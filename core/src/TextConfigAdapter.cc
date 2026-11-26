#include <TextConfigAdapter.h>
#include "ConfigDocument.h"
#include <fstream>
#include <stdexcept>

std::string TextConfigAdapter::stripComment(const std::string &line) const {
  return line.substr(0, line.find("#"));
}

std::string_view TextConfigAdapter::trim(std::string_view s) const {
  size_t first = s.find_first_not_of(" \t\n\r");
  size_t last = s.find_last_not_of(" \t\n\r");
  if (first == std::string_view::npos || last == std::string_view::npos) {
    return {};
  }
  return s.substr(first, last - first + 1);
}

std::pair<std::string, std::string>
TextConfigAdapter::parsePair(const std::string &line, bool stripComments) const {
  std::string processedLine = stripComments ? stripComment(line) : line;
  auto splitIndex = processedLine.find("=");
  if (splitIndex == std::string::npos) {
    return {"", ""};
  }
  auto key = std::string(trim(processedLine.substr(0, splitIndex)));
  auto value = std::string(trim(processedLine.substr(splitIndex + 1)));
  return {key, value};
}

std::vector<std::string>
TextConfigAdapter::splitString(std::string_view input,
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

namespace {
std::ifstream openLegacy(const std::string &file) {
  std::ifstream input(file);
  if (!input) throw std::runtime_error("Error: Configuration file '" + file + "' could not be opened.");
  return input;
}
}

rdf_config_detail::ConfigMap TextConfigAdapter::parsePairBasedConfig(const std::string &file) const {
  auto input = openLegacy(file);
  YAML::Node document(YAML::NodeType::Map);
  std::string line;
  while (std::getline(input, line)) {
    auto [key, value] = parsePair(line, true);
    if (!key.empty()) document.force_insert(key, value);
  }
  return rdf_config_detail::mapping(document, file);
}

std::vector<rdf_config_detail::ConfigMap> TextConfigAdapter::parseMultiKeyConfig(
    const std::string &file, const std::vector<std::string> &required) const {
  auto input = openLegacy(file);
  YAML::Node document(YAML::NodeType::Sequence);
  std::string line;
  while (std::getline(input, line)) {
    line = stripComment(line);
    if (trim(line).empty()) continue;
    YAML::Node entry(YAML::NodeType::Map);
    for (const auto &field : splitString(line, " ")) {
      auto [key, value] = parsePair(field, false);
      if (!key.empty() && !value.empty()) entry.force_insert(key, value);
    }
    document.push_back(entry);
  }
  return rdf_config_detail::records(document, file, required);
}

std::vector<std::string> TextConfigAdapter::parseVectorConfig(const std::string &file) const {
  auto input = openLegacy(file);
  YAML::Node document(YAML::NodeType::Sequence);
  std::string line;
  while (std::getline(input, line)) document.push_back(stripComment(line));
  return rdf_config_detail::strings(document, file);
}
