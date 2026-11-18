#ifndef RDF_COMMON_SETTINGS_H
#define RDF_COMMON_SETTINGS_H

#include <ValidationReport.h>
#include <CommonSettingsSchema.h>
#include <optional>
#include <string>
#include <unordered_map>

namespace rdf::config {

using NativeOptions = std::unordered_map<std::string, std::string>;

/** Unset members emit no native option and preserve native/advanced defaults. */
struct InputSettings {
  std::optional<bool> cacheEnabled;
  std::optional<int> cacheSizeMB;
  std::optional<bool> cacheStatistics;
  std::optional<int> cacheVerbosity;
  std::optional<std::string> floatConstants;
  std::optional<std::string> integerConstants;
  std::optional<std::string> aliases;
  std::optional<std::string> optionalBranches;
  std::optional<std::string> friends;
};

enum class CompressionAlgorithm { Zstd, Lzma, Zlib, Lz4 };

struct SkimSettings {
  std::optional<std::string> tree;
  std::optional<std::string> metadataFile;
  std::optional<bool> systematics;
  std::optional<CompressionAlgorithm> compressionAlgorithm;
  std::optional<int> compressionLevel;
};

/** Optional setup facade; it does not change Analyzer or configuration providers.
 * Paths retain the existing native provider's resolution policy. Validation is
 * configuration-only: no input/schema/payload discovery or filesystem access.
 */
struct CommonSettings {
  InputSettings input;
  SkimSettings skim;

  validation::Report validate(const NativeOptions &advanced = {}) const {
    return prepare(advanced).report;
  }

  /** Return a validated map for the existing IConfigurationProvider::set API.
   * The caller installs it before constructing DataManager/Analyzer. Unknown
   * advanced keys remain available; an explicitly typed key cannot collide.
   * Neither this object nor advanced is modified, including on validation error.
   */
  NativeOptions nativeOptions(const NativeOptions &advanced = {}) const {
    auto prepared = prepare(advanced);
    prepared.report.throwIfInvalid();
    return std::move(prepared.options);
  }

private:
  struct Prepared {
    validation::Report report;
    NativeOptions options;
  };

  Prepared prepare(const NativeOptions &advanced) const {
    Prepared result;
    auto &report = result.report;
    auto &options = result.options;
    options = advanced;
    auto emit = [&](const std::string &key, std::string value, const std::string &location) {
      if (advanced.count(key))
        report.addIssue("settings.conflict", "Advanced option conflicts with typed setting: " + key, location);
      else
        options.emplace(key, std::move(value));
    };
    auto boolean = [&](const auto &value, SettingId id) {
      const auto &spec = commonSettingSpec(id);
      if (value) emit(spec.nativeKey, *value ? "true" : "false", spec.location);
    };
    auto integer = [&](const auto &value, SettingId id) {
      if (!value) return;
      const auto &spec = commonSettingSpec(id);
      if (*value < spec.minimum || *value > spec.maximum)
        report.addIssue("settings.range", "Expected " + std::to_string(spec.minimum) + " through " + std::to_string(spec.maximum), spec.location);
      emit(spec.nativeKey, std::to_string(*value), spec.location);
    };
    auto text = [&](const auto &value, SettingId id) {
      if (!value) return;
      const auto &spec = commonSettingSpec(id);
      if (value->find_first_not_of(" \t\n\r\f\v") == std::string::npos ||
          value->find_first_of(std::string("\0\n\r", 3)) != std::string::npos)
        report.addIssue("settings.value", "Expected a nonempty string without NUL/newlines", spec.location);
      if (spec.localPath && value->find("://") != std::string::npos)
        report.addIssue("settings.path", "Expected a local path", spec.location);
      emit(spec.nativeKey, *value, spec.location);
    };
    boolean(input.cacheEnabled, SettingId::CacheEnabled);
    integer(input.cacheSizeMB, SettingId::CacheSizeMB);
    boolean(input.cacheStatistics, SettingId::CacheStatistics);
    integer(input.cacheVerbosity, SettingId::CacheVerbosity);
    text(input.floatConstants, SettingId::FloatConstants);
    text(input.integerConstants, SettingId::IntegerConstants);
    text(input.aliases, SettingId::Aliases);
    text(input.optionalBranches, SettingId::OptionalBranches);
    text(input.friends, SettingId::Friends);
    if (input.cacheSizeMB) {
      if (input.cacheEnabled != std::optional<bool>(true))
        report.addIssue("settings.dependency", "Requires cache_enabled=true; otherwise native code ignores this size",
                        "input_settings.cache_size_mb");
      for (const auto *key : {"rootTTreeCacheSize", "ROOT_TTREECACHE_SIZE"})
        if (advanced.count(key))
          report.addIssue("settings.conflict", std::string("Fixed cache size conflicts with factor-based option: ") + key,
                          "input_settings.cache_size_mb");
    }
    text(skim.tree, SettingId::SkimTree);
    text(skim.metadataFile, SettingId::MetadataFile);
    boolean(skim.systematics, SettingId::SkimSystematics);
    if (skim.compressionAlgorithm) {
      const auto &spec = commonSettingSpec(SettingId::CompressionAlgorithm);
      const auto index = static_cast<std::size_t>(*skim.compressionAlgorithm);
      if (index < skimCompressionAlgorithms().size()) emit(spec.nativeKey, skimCompressionAlgorithms()[index], spec.location);
      else report.addIssue("settings.value", "Unknown compression algorithm", spec.location);
    }
    integer(skim.compressionLevel, SettingId::CompressionLevel);
    return result;
  }
};

} // namespace rdf::config
#endif
