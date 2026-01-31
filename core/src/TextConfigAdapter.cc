#include <TextConfigAdapter.h>
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

std::unordered_map<std::string, std::string>
TextConfigAdapter::parsePairBasedConfig(const std::string &configFile) const {
  std::unordered_map<std::string, std::string> configMap;
  std::ifstream file(configFile);
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      auto [key, value] = parsePair(line, true);
      if (!key.empty()) {
        if (configMap.find(key) != configMap.end()) {
          throw std::runtime_error(
              "Error: Key " + key + " already exists in config " + configFile +
              ". Do not use the same key twice in the same config.");
        }
        configMap.emplace(key, value);
      }
    }
    file.close();
  } else {
    throw std::runtime_error("Error: Configuration file " + configFile +
                             " could not be opened.");
  }
  return configMap;
}

std::vector<std::unordered_map<std::string, std::string>>
TextConfigAdapter::parseMultiKeyConfig(
    const std::string &configFile,
    const std::vector<std::string> &requiredEntryKeys) const {
  std::vector<std::unordered_map<std::string, std::string>> parsedConfig;

  std::ifstream file(configFile);
  if (!file.is_open()) {
    throw std::runtime_error("Error: Configuration file " + configFile +
                             " could not be opened.");
    return parsedConfig;
  }

  std::string line;
  while (std::getline(file, line)) {
    line = trim(line.substr(0, line.find("#")));
    if (line.empty()) {
      continue;
    }

    std::unordered_map<std::string, std::string> entryKeys;
    auto splitEntry = splitString(line, " ");
    for (auto &pair : splitEntry) {
      auto [key, value] = parsePair(pair, false);
      if (!key.empty() && !value.empty()) {
        if (entryKeys.find(key) != entryKeys.end()) {
          throw std::runtime_error(
              "Error: Key " + key + " already exists in entry " + line +
              " in config " + configFile +
              ". Do not use the same key twice in the same entry.");
        }
        entryKeys[key] = value;
      }
    }

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

  file.close();
  return parsedConfig;
}

std::vector<std::string>
TextConfigAdapter::parseVectorConfig(const std::string &configFile) const {
  std::vector<std::string> configVector;
  std::ifstream file(configFile);
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      line = trim(line.substr(0, line.find("#")));
      if (!line.empty()) {
        configVector.push_back(line);
      }
    }
    file.close();
  } else {
    throw std::runtime_error("Error: Configuration file " + configFile +
                             " could not be opened.");
  }
  return configVector;
}
