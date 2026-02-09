#ifdef HAVE_YAML_CPP

#include <YamlConfigAdapter.h>
#include <yaml-cpp/yaml.h>
#include <fstream>
#include <stdexcept>

std::string_view YamlConfigAdapter::trim(std::string_view s) const {
  size_t first = s.find_first_not_of(" \t\n\r");
  size_t last = s.find_last_not_of(" \t\n\r");
  if (first == std::string_view::npos || last == std::string_view::npos) {
    return {};
  }
  return s.substr(first, last - first + 1);
}

std::unordered_map<std::string, std::string>
YamlConfigAdapter::parsePairBasedConfig(const std::string &configFile) const {
  std::unordered_map<std::string, std::string> configMap;
  
  try {
    YAML::Node config = YAML::LoadFile(configFile);
    
    if (!config.IsMap()) {
      throw std::runtime_error(
          "Error: Configuration file " + configFile +
          " is not a valid YAML map.");
    }
    
    for (const auto& kv : config) {
      std::string key = kv.first.as<std::string>();
      std::string value = kv.second.as<std::string>();
      
      if (configMap.find(key) != configMap.end()) {
        throw std::runtime_error(
            "Error: Key " + key + " already exists in config " + configFile +
            ". Do not use the same key twice in the same config.");
      }
      configMap.emplace(key, value);
    }
  } catch (const YAML::Exception& e) {
    throw std::runtime_error("Error parsing YAML file " + configFile + ": " + e.what());
  }
  
  return configMap;
}

std::vector<std::unordered_map<std::string, std::string>>
YamlConfigAdapter::parseMultiKeyConfig(
    const std::string &configFile,
    const std::vector<std::string> &requiredEntryKeys) const {
  std::vector<std::unordered_map<std::string, std::string>> parsedConfig;
  
  try {
    YAML::Node config = YAML::LoadFile(configFile);
    
    if (!config.IsSequence()) {
      throw std::runtime_error(
          "Error: Configuration file " + configFile +
          " is not a valid YAML sequence for multi-key config.");
    }
    
    for (const auto& entry : config) {
      if (!entry.IsMap()) {
        continue;
      }
      
      std::unordered_map<std::string, std::string> entryKeys;
      for (const auto& kv : entry) {
        std::string key = kv.first.as<std::string>();
        std::string value = kv.second.as<std::string>();
        
        if (entryKeys.find(key) != entryKeys.end()) {
          throw std::runtime_error(
              "Error: Key " + key + " already exists in entry in config " +
              configFile +
              ". Do not use the same key twice in the same entry.");
        }
        entryKeys[key] = value;
      }
      
      // Check if all required keys are present
      bool allEntryKeysFound = true;
      for (const auto &entryKey : requiredEntryKeys) {
        if (entryKeys.find(entryKey) == entryKeys.end()) {
          allEntryKeysFound = false;
          break;
        }
      }
      
      if (allEntryKeysFound) {
        parsedConfig.push_back(entryKeys);
      }
    }
  } catch (const YAML::Exception& e) {
    throw std::runtime_error("Error parsing YAML file " + configFile + ": " + e.what());
  }
  
  return parsedConfig;
}

std::vector<std::string>
YamlConfigAdapter::parseVectorConfig(const std::string &configFile) const {
  std::vector<std::string> configVector;
  
  try {
    YAML::Node config = YAML::LoadFile(configFile);
    
    if (!config.IsSequence()) {
      throw std::runtime_error(
          "Error: Configuration file " + configFile +
          " is not a valid YAML sequence for vector config.");
    }
    
    for (const auto& item : config) {
      std::string value = item.as<std::string>();
      std::string_view trimmed = trim(value);
      if (!trimmed.empty()) {
        configVector.emplace_back(trimmed);
      }
    }
  } catch (const YAML::Exception& e) {
    throw std::runtime_error("Error parsing YAML file " + configFile + ": " + e.what());
  }
  
  return configVector;
}

#endif // HAVE_YAML_CPP
