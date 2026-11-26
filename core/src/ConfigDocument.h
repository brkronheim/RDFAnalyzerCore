// Internal normalized configuration document used by both built-in adapters.
#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <yaml-cpp/yaml.h>

namespace rdf_config_detail {
using ConfigMap = std::unordered_map<std::string, std::string>;
inline bool isSourceList(const std::string &key) {
  return key == "fileList" || key == "treeList" || key == "globs" || key == "antiglobs";
}
YAML::Node loadDocument(const std::string &source);
ConfigMap mapping(const YAML::Node &node, const std::string &source);
std::vector<ConfigMap> records(const YAML::Node &node, const std::string &source,
                               const std::vector<std::string> &required);
std::vector<std::string> strings(const YAML::Node &node, const std::string &source);
}
