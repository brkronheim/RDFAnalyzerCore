#include <CommonSettings.h>
#include <algorithm>
#include <iostream>
#include <map>
#include <stdexcept>

using rdf::config::CommonSettings;
using rdf::config::CompressionAlgorithm;
using rdf::config::NativeOptions;

namespace {
void require(bool condition, const char *message) {
  if (!condition) throw std::runtime_error(message);
}

template<class F> void rejects(F function) {
  try { function(); }
  catch (const std::invalid_argument &) { return; }
  throw std::runtime_error("Expected invalid_argument");
}

void printOptions(const NativeOptions &options) {
  std::cout << '{';
  bool first = true;
  for (const auto &[key, value] : std::map<std::string, std::string>(options.begin(), options.end())) {
    if (!first) std::cout << ',';
    first = false;
    std::cout << rdf::validation::detail::jsonString(key) << ':'
              << rdf::validation::detail::jsonString(value);
  }
  std::cout << "}\n";
}

CommonSettings example(const std::string &base) {
  CommonSettings settings;
  settings.input.cacheEnabled = true;
  settings.input.cacheSizeMB = 32;
  settings.input.cacheStatistics = false;
  settings.input.cacheVerbosity = 2;
  settings.input.floatConstants = base + "/cuts.txt";
  settings.input.integerConstants = base + "/categories.txt";
  settings.input.aliases = base + "/aliases.txt";
  settings.input.optionalBranches = base + "/optional.txt";
  settings.input.friends = base + "/friends.txt";
  settings.skim.tree = "Selected";
  settings.skim.metadataFile = base + "/metadata.root";
  settings.skim.systematics = false;
  settings.skim.compressionAlgorithm = CompressionAlgorithm::Zstd;
  settings.skim.compressionLevel = 7;
  return settings;
}

void selfTest() {
  CommonSettings empty;
  require(empty.nativeOptions().empty(), "Unset settings changed defaults");
  NativeOptions advanced{{"thirdParty.recipe", "opaque: a | b"}, {"treeCacheStats", "true"}};
  require(empty.nativeOptions(advanced) == advanced, "Advanced options were changed");
  auto original = advanced;
  empty.input.cacheStatistics = false;
  auto report = empty.validate(advanced);
  require(!report.passed() && report.issues.front().code == "settings.conflict", "Missing typed conflict");
  rejects([&] { empty.nativeOptions(advanced); });
  require(advanced == original, "Failure mutated caller configuration");

  CommonSettings settings;
  settings.input.cacheSizeMB = 32;
  require(!settings.validate().passed(), "Cache size without enabled policy accepted");
  settings.input.cacheEnabled = false;
  require(!settings.validate().passed(), "Cache size with disabled policy accepted");
  settings.input.cacheEnabled = true;
  for (int size : {0, 32, 512}) {
    settings.input.cacheSizeMB = size;
    require(settings.nativeOptions().at("treeCacheSizeMB") == std::to_string(size), "Cache size boundary lost");
  }
  for (int size : {-1, 513}) {
    settings.input.cacheSizeMB = size;
    rejects([&] { settings.nativeOptions(); });
  }
  settings.input.cacheSizeMB = 32;
  for (const char *key : {"rootTTreeCacheSize", "ROOT_TTREECACHE_SIZE", "treeCacheSizeMB"})
    rejects([&] { settings.nativeOptions({{key, "32"}}); });
  for (int verbosity : {-1, 3}) {
    settings.input.cacheVerbosity = verbosity;
    rejects([&] { settings.nativeOptions(); });
  }
  settings.input.cacheVerbosity.reset();
  settings.input.cacheStatistics = false;
  require(settings.nativeOptions().at("treeCacheStats") == "false", "Explicit false was omitted");
  settings.input.aliases = "relative/native/path.txt";
  require(settings.nativeOptions().at("aliasConfig") == "relative/native/path.txt", "Native path policy changed");
  for (const auto &path : {std::string(""), std::string(" \t"), std::string("bad\npath"),
                           std::string("bad\0path", 8), std::string("root://host/config")}) {
    settings.input.aliases = path;
    rejects([&] { settings.nativeOptions(); });
  }
  settings.input.aliases.reset();
  for (int level : {-1, 10}) {
    settings.skim.compressionLevel = level;
    rejects([&] { settings.nativeOptions(); });
  }
  settings.skim.compressionLevel = 0;
  require(settings.nativeOptions().at("skimCompressionLevel") == "0", "Explicit zero was omitted");
  for (auto [algorithm, name] : {std::pair{CompressionAlgorithm::Zstd, "zstd"},
                                {CompressionAlgorithm::Lzma, "lzma"},
                                {CompressionAlgorithm::Zlib, "zlib"},
                                {CompressionAlgorithm::Lz4, "lz4"}}) {
    settings.skim.compressionAlgorithm = algorithm;
    require(settings.nativeOptions().at("skimCompressionAlgorithm") == name, "Compression mapping differs");
  }
  settings.skim.compressionAlgorithm = static_cast<CompressionAlgorithm>(99);
  rejects([&] { settings.nativeOptions(); });
  rdf::validation::Report valid;
  valid.throwIfInvalid();
  auto copy = valid;
  copy.addIssue("example", "bad value", "settings.example");
  require(valid.passed() && !copy.passed(), "Report has unexpected shared state");
  rejects([&] { copy.throwIfInvalid(); });
}
} // namespace

int main(int argc, char **argv) {
  try {
    const std::string mode = argc > 1 ? argv[1] : "self-test";
    if (mode == "self-test") { selfTest(); return 0; }
    if (mode == "options") {
      printOptions(example(argc > 2 ? argv[2] : "/analysis").nativeOptions({{"extension.custom", "keep | this: value"}}));
    } else if (mode == "empty") {
      printOptions(CommonSettings{}.nativeOptions({{"treeCacheStats", "0"}, {"extension.custom", "keep"}}));
    } else if (mode == "invalid") {
      CommonSettings settings; settings.input.cacheSizeMB = 513;
      std::cout << settings.validate().toJson() << '\n';
    } else if (mode == "report") {
      rdf::validation::Report report;
      report.level = "assets"; report.supportedLevels.push_back("assets");
      std::string message = "bad \"file\"\\path\n\t";
      message.push_back('\0'); message += u8" μ";
      report.addIssue("asset.missing", message, "input_settings.aliases");
      std::cout << report.toJson() << '\n';
    } else if (mode == "passed") {
      std::cout << rdf::validation::Report{}.toJson() << '\n';
    } else if (mode == "describe") {
      std::cout << rdf::config::commonSettingsDescriptionJson() << '\n';
    } else if (mode == "help") {
      std::cout << rdf::config::commonSettingsHelp();
    } else {
      throw std::invalid_argument("Unknown fixture mode");
    }
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n'; return 1;
  }
}
