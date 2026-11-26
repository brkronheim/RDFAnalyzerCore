#include <YamlConfigAdapter.h>
#include "ConfigDocument.h"
#include <algorithm>
#include <stdexcept>

namespace {
std::string location(const std::string &source, const YAML::Node &node) {
  const auto mark = node.Mark();
  return mark.is_null() ? source : source + ":" + std::to_string(mark.line + 1);
}

std::string serialize(const YAML::Node &value, const std::string &key, const std::string &source) {
  if (value.IsNull()) return "";
  if (value.IsScalar()) return value.Scalar();
  if (value.IsSequence() && rdf_config_detail::isSourceList(key)) {
    // These native settings are also consumed as comma-separated strings by worker tools.
    std::string csv;
    for (const auto &item : value) {
      if (!item.IsScalar() || item.Scalar().find(',') != std::string::npos)
        throw std::runtime_error(location(source, item) + ": " + key +
                                 " requires scalar items without embedded commas");
      if (item.Scalar().empty()) continue;
      if (!csv.empty()) csv += ',';
      csv += item.Scalar();
    }
    return csv;
  }
  YAML::Emitter emitter;
  if (value.IsSequence() &&
      std::all_of(value.begin(), value.end(), [](const auto &item) { return item.IsScalar(); })) {
    // Preserve items containing commas, quotes or YAML punctuation for getList().
    emitter << YAML::Flow << YAML::BeginSeq;
    for (const auto &item : value) emitter << item.Scalar();
    emitter << YAML::EndSeq;
  } else {
    emitter << YAML::Flow << value;
  }
  if (!emitter.good()) throw std::runtime_error(source + ": " + emitter.GetLastError());
  return emitter.c_str();
}

std::string_view trimmed(std::string_view text) {
  const auto first = text.find_first_not_of(" \t\n\r");
  if (first == std::string_view::npos) return {};
  return text.substr(first, text.find_last_not_of(" \t\n\r") - first + 1);
}
}

namespace rdf_config_detail {
YAML::Node loadDocument(const std::string &source) {
  try {
    auto docs = YAML::LoadAllFromFile(source);
    if (docs.size() != 1)
      throw std::runtime_error(source + ": expected exactly one YAML document");
    return docs.front();
  } catch (const YAML::Exception &error) {
    throw std::runtime_error(source + ": " + error.what());
  }
}

ConfigMap mapping(const YAML::Node &node, const std::string &source) {
  if (!node.IsMap()) throw std::runtime_error(location(source, node) + ": expected a YAML mapping");
  ConfigMap values;
  for (const auto &kv : node) {
    if (!kv.first.IsScalar() || kv.first.Scalar().empty())
      throw std::runtime_error(location(source, kv.first) + ": expected a nonempty scalar key");
    const auto &key = kv.first.Scalar();
    if (!values.emplace(key, serialize(kv.second, key, source)).second)
      throw std::runtime_error(location(source, kv.first) + ": duplicate key '" + key + "'");
  }
  return values;
}

std::vector<ConfigMap> records(const YAML::Node &node, const std::string &source,
                               const std::vector<std::string> &required) {
  if (!node.IsSequence()) throw std::runtime_error(location(source, node) + ": expected a sequence of mappings");
  std::vector<ConfigMap> result;
  for (const auto &entry : node) {
    auto values = mapping(entry, source);
    // Callers select record types in heterogeneous plugin configuration files by key.
    if (std::all_of(required.begin(), required.end(), [&](const auto &key) { return values.count(key); }))
      result.emplace_back(std::move(values));
  }
  return result;
}

std::vector<std::string> strings(const YAML::Node &node, const std::string &source) {
  if (!node.IsSequence()) throw std::runtime_error(location(source, node) + ": expected a sequence of strings");
  std::vector<std::string> result;
  for (const auto &item : node) {
    if (!item.IsScalar()) throw std::runtime_error(location(source, item) + ": expected a scalar list item");
    auto value = trimmed(item.Scalar());
    if (!value.empty()) result.emplace_back(value);
  }
  return result;
}
}

std::string_view YamlConfigAdapter::trim(std::string_view value) const {
  return trimmed(value);
}

rdf_config_detail::ConfigMap YamlConfigAdapter::parsePairBasedConfig(const std::string &file) const {
  return rdf_config_detail::mapping(rdf_config_detail::loadDocument(file), file);
}

std::vector<rdf_config_detail::ConfigMap> YamlConfigAdapter::parseMultiKeyConfig(
    const std::string &file, const std::vector<std::string> &required) const {
  return rdf_config_detail::records(rdf_config_detail::loadDocument(file), file, required);
}

std::vector<std::string> YamlConfigAdapter::parseVectorConfig(const std::string &file) const {
  return rdf_config_detail::strings(rdf_config_detail::loadDocument(file), file);
}
