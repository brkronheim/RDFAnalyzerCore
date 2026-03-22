#include <GoldenJsonManager.h>
#include <api/ILogger.h>

#include <cctype>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>

// ---------------------------------------------------------------------------
// Minimal JSON parser for the CMS golden JSON format.
//
// Expected input structure:
//   { "355100": [[1, 100], [150, 200]], "355101": [[1, 50]], ... }
//
// The parser only needs to handle:
//   - JSON objects   {}
//   - JSON arrays    []
//   - Quoted strings (run numbers)
//   - Unsigned integers (lumi section boundaries)
// ---------------------------------------------------------------------------

namespace {

class GoldenJsonParser {
public:
  explicit GoldenJsonParser(const std::string &text) : s_(text), pos_(0) {}

  /// Parse the top-level JSON object and return run -> lumi-range list.
  std::unordered_map<unsigned int,
                     std::vector<std::pair<unsigned int, unsigned int>>>
  parse() {
    std::unordered_map<unsigned int,
                       std::vector<std::pair<unsigned int, unsigned int>>>
        result;
    skip();
    expect('{');
    skip();
    while (pos_ < s_.size() && s_[pos_] != '}') {
      std::string runStr = parseString();
      unsigned int run = static_cast<unsigned int>(std::stoul(runStr));
      skip();
      expect(':');
      skip();
      result[run] = parseRanges();
      skip();
      if (pos_ < s_.size() && s_[pos_] == ',') {
        ++pos_;
        skip();
      }
    }
    return result;
  }

private:
  const std::string &s_;
  std::size_t pos_;

  void skip() {
    while (pos_ < s_.size() && std::isspace(static_cast<unsigned char>(s_[pos_])))
      ++pos_;
  }

  void expect(char c) {
    if (pos_ >= s_.size() || s_[pos_] != c) {
      throw std::runtime_error(
          std::string("GoldenJsonManager: expected '") + c +
          "' at position " + std::to_string(pos_));
    }
    ++pos_;
  }

  std::string parseString() {
    expect('"');
    std::string result;
    while (pos_ < s_.size() && s_[pos_] != '"') {
      result += s_[pos_++];
    }
    expect('"');
    return result;
  }

  unsigned int parseUInt() {
    skip();
    if (pos_ >= s_.size() || !std::isdigit(static_cast<unsigned char>(s_[pos_]))) {
      throw std::runtime_error(
          "GoldenJsonManager: expected digit at position " +
          std::to_string(pos_));
    }
    std::string digits;
    while (pos_ < s_.size() &&
           std::isdigit(static_cast<unsigned char>(s_[pos_]))) {
      digits += s_[pos_++];
    }
    return static_cast<unsigned int>(std::stoul(digits));
  }

  std::pair<unsigned int, unsigned int> parseRange() {
    skip();
    expect('[');
    skip();
    unsigned int start = parseUInt();
    skip();
    expect(',');
    skip();
    unsigned int end = parseUInt();
    skip();
    expect(']');
    return {start, end};
  }

  std::vector<std::pair<unsigned int, unsigned int>> parseRanges() {
    skip();
    expect('[');
    skip();
    std::vector<std::pair<unsigned int, unsigned int>> ranges;
    while (pos_ < s_.size() && s_[pos_] != ']') {
      ranges.push_back(parseRange());
      skip();
      if (pos_ < s_.size() && s_[pos_] == ',') {
        ++pos_;
        skip();
      }
    }
    expect(']');
    return ranges;
  }
};

} // anonymous namespace

// ---------------------------------------------------------------------------
// GoldenJsonManager implementation
// ---------------------------------------------------------------------------

void GoldenJsonManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
}

void GoldenJsonManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("GoldenJsonManager: ConfigManager not set");
  }

  const std::string configKey = "goldenJsonConfig";
  std::string configFile;
  try {
    std::cout << "GoldenJsonManager: loading golden JSON files from config key '"
              << configKey << "'..." << std::endl;
    configFile = configManager_m->get(configKey);
  } catch (...) {
    throw std::runtime_error(
        "GoldenJsonManager: config key '" + configKey +
        "' not found. Add 'goldenJsonConfig=<path>' to your configuration.");
  }

  std::cout << "GoldenJsonManager: loading golden JSON files from '" << configFile
            << "'..." << std::endl;
  const auto jsonFiles = configManager_m->parseVectorConfig(configFile);
  if (jsonFiles.empty()) {
    throw std::runtime_error(
        "GoldenJsonManager: no JSON files listed in '" + configFile + "'");
  }

  for (const auto &f : jsonFiles) {
    loadJsonFile(f);
  }
}

void GoldenJsonManager::loadJsonFile(const std::string &filename) {
  std::ifstream file(filename);
  if (!file.is_open()) {
    throw std::runtime_error(
        "GoldenJsonManager: cannot open golden JSON file: " + filename);
  }

  const std::string content((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());

  GoldenJsonParser parser(content);
  auto parsed = parser.parse();

  // Merge into the existing map (supports multiple files / eras).
  for (auto &[run, ranges] : parsed) {
    auto &dest = validLumis_m[run];
    dest.insert(dest.end(), ranges.begin(), ranges.end());
  }
}

bool GoldenJsonManager::isValid(unsigned int run, unsigned int lumi) const {
  auto it = validLumis_m.find(run);
  if (it == validLumis_m.end()) {
    return false;
  }
  for (const auto &range : it->second) {
    if (lumi >= range.first && lumi <= range.second) {
      return true;
    }
  }
  return false;
}

void GoldenJsonManager::applyGoldenJson() {
  if (!dataManager_m || !configManager_m) {
    throw std::runtime_error(
        "GoldenJsonManager: DataManager or ConfigManager not set");
  }

  std::string sampleType;
  std::cout << "GoldenJsonManager: checking sample type from config key 'dtype'..."
            << std::endl;
  sampleType = configManager_m->get("dtype");
  if (sampleType.empty()) {
    std::cout << "GoldenJsonManager: falling back to config key 'type'..."
              << std::endl;
    sampleType = configManager_m->get("type");
  }
  if (sampleType.empty()) {
    throw std::runtime_error(
        "GoldenJsonManager: config keys 'dtype' and 'type' not found");
  }

  if (sampleType != "data") {
    return;
  }

  // Share the validity map via shared_ptr to avoid a full copy into the lambda.
  auto validLumisPtr = std::make_shared<
      std::unordered_map<unsigned int,
                         std::vector<std::pair<unsigned int, unsigned int>>>>(
      validLumis_m);

  dataManager_m->Filter(
      [validLumisPtr](unsigned int run, unsigned int lumi) -> bool {
        auto it = validLumisPtr->find(run);
        if (it == validLumisPtr->end()) {
          return false;
        }
        for (const auto &range : it->second) {
          if (lumi >= range.first && lumi <= range.second) {
            return true;
          }
        }
        return false;
      },
      {"run", "luminosityBlock"});
}

void GoldenJsonManager::initialize() {
  std::cout << "GoldenJsonManager: initialized with " << validLumis_m.size()
            << " certified run(s)." << std::endl;
}

void GoldenJsonManager::reportMetadata() {
  if (!logger_m) return;
  logger_m->log(ILogger::Level::Info,
                "GoldenJsonManager: " + std::to_string(validLumis_m.size()) +
                " certified run(s) loaded.");
}
