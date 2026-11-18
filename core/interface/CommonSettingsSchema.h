#ifndef RDF_COMMON_SETTINGS_SCHEMA_H
#define RDF_COMMON_SETTINGS_SCHEMA_H

#include <ValidationReport.h>
#include <array>
#include <string>

namespace rdf::config {

enum class SettingId {
  CacheEnabled, CacheSizeMB, CacheStatistics, CacheVerbosity, FloatConstants,
  IntegerConstants, Aliases, OptionalBranches, Friends, SkimTree, MetadataFile,
  SkimSystematics, CompressionAlgorithm, CompressionLevel
};

enum class SettingType { Boolean, Integer, String };

struct SettingSpec {
  const char *location;
  const char *nativeKey;
  SettingType type;
  const char *description;
  int minimum = 0;
  int maximum = 0;
  bool localPath = false;
  bool inputAsset = false;
};

/** The same keys/ranges drive native lowering, validation and explicit help. */
inline const std::array<SettingSpec, 14> &commonSettingSpecs() {
  static constexpr std::array<SettingSpec, 14> specs{{
      {"input_settings.cache_enabled", "treeCacheEnabled", SettingType::Boolean, "Enable the existing input cache policy."},
      {"input_settings.cache_size_mb", "treeCacheSizeMB", SettingType::Integer, "Requested cache MiB; requires cache_enabled=true.", 0, 512},
      {"input_settings.cache_statistics", "treeCacheStats", SettingType::Boolean, "Report input cache statistics."},
      {"input_settings.cache_verbosity", "treeCacheVerbosity", SettingType::Integer, "Input cache diagnostic verbosity.", 0, 2},
      {"input_settings.float_constants", "floatConfig", SettingType::String, "Native floating-point constants config.", 0, 0, true, true},
      {"input_settings.integer_constants", "intConfig", SettingType::String, "Native integer constants config.", 0, 0, true, true},
      {"input_settings.aliases", "aliasConfig", SettingType::String, "Native input-column aliases config.", 0, 0, true, true},
      {"input_settings.optional_branches", "optionalBranchesConfig", SettingType::String, "Native optional-branch config.", 0, 0, true, true},
      {"input_settings.friends", "friendConfig", SettingType::String, "Native friend config; referenced trees are not recursively checked.", 0, 0, true, true},
      {"skim_settings.tree", "saveTree", SettingType::String, "Skim output tree name."},
      {"skim_settings.metadata_file", "metaFile", SettingType::String, "Metadata output path.", 0, 0, true},
      {"skim_settings.systematics", "skimSystematics", SettingType::Boolean, "Include available systematic skim columns; independent of histogram variations."},
      {"skim_settings.compression_algorithm", "skimCompressionAlgorithm", SettingType::String, "Skim compression algorithm: zstd, lzma, zlib or lz4."},
      {"skim_settings.compression_level", "skimCompressionLevel", SettingType::Integer, "Skim compression level; zero disables compression.", 0, 9}
  }};
  return specs;
}

inline const SettingSpec &commonSettingSpec(SettingId id) {
  return commonSettingSpecs().at(static_cast<std::size_t>(id));
}

inline const char *settingTypeName(SettingType type) {
  switch (type) {
  case SettingType::Boolean: return "boolean";
  case SettingType::Integer: return "integer";
  case SettingType::String: return "string";
  }
  return "unknown";
}

inline const std::array<const char *, 4> &skimCompressionAlgorithms() {
  static constexpr std::array<const char *, 4> values{{"zstd", "lzma", "zlib", "lz4"}};
  return values;
}

/** Explicit metadata only, in the existing workflow-description envelope. */
inline std::string commonSettingsDescriptionJson() {
  using validation::detail::jsonString;
  std::string out = "{\"schema\":\"rdf-workflow-description-v1\",\"backend\":\"settings-provider\","
      "\"capabilities\":[\"configuration-validation\"],\"details\":{\"name\":\"native-common-settings\","
      "\"supported_levels\":[\"configuration\"],\"path_policy\":\"native-owned; retained verbatim\","
      "\"unknown_native_options\":\"preserved\",\"schema\":{";
  bool first = true;
  for (const auto &spec : commonSettingSpecs()) {
    if (!first) out += ',';
    first = false;
    out += jsonString(spec.location) + ":{\"native_key\":" + jsonString(spec.nativeKey);
    out += ",\"type\":" + jsonString(settingTypeName(spec.type));
    out += ",\"description\":" + jsonString(spec.description);
    out += ",\"default\":null,\"nullable\":true,\"unset_behavior\":\"omit native key; retain legacy option or framework default\"";
    if (spec.type == SettingType::Integer)
      out += ",\"minimum\":" + std::to_string(spec.minimum) + ",\"maximum\":" + std::to_string(spec.maximum);
    if (spec.localPath) out += ",\"path\":true";
    if (spec.inputAsset) out += ",\"asset\":true";
    if (&spec == &commonSettingSpec(SettingId::CompressionAlgorithm)) {
      out += ",\"choices\":[";
      for (std::size_t i = 0; i < skimCompressionAlgorithms().size(); ++i) {
        if (i) out += ',';
        out += jsonString(skimCompressionAlgorithms()[i]);
      }
      out += ']';
    }
    out += '}';
  }
  return out + "}}}";
}

inline std::string commonSettingsHelp() {
  std::string out = "Common native settings (all optional; unset preserves native defaults).\n"
                    "Advanced native options remain available; paths retain native resolution.\n";
  for (const auto &spec : commonSettingSpecs()) {
    out += std::string(spec.location) + " -> " + spec.nativeKey + " (" + settingTypeName(spec.type);
    if (spec.type == SettingType::Integer)
      out += ", " + std::to_string(spec.minimum) + ".." + std::to_string(spec.maximum);
    out += "): " + std::string(spec.description) + '\n';
  }
  return out;
}

} // namespace rdf::config
#endif
