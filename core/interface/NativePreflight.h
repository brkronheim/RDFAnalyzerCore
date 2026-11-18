#ifndef RDF_NATIVE_PREFLIGHT_H
#define RDF_NATIVE_PREFLIGHT_H

#include <ValidationReport.h>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace rdf::validation {

enum class AssetKind { File, Directory };

/** Explicit local assets only; no implicit configuration/payload discovery. */
struct AssetRequirement {
  std::string path;
  std::string location;
  AssetKind kind = AssetKind::File;
  std::optional<std::uintmax_t> expectedBytes;
};

/** Check only supplied assets. Relative paths use the caller's explicit base
 * (or the process working directory). Follows ordinary filesystem symlinks;
 * neither opens ROOT files nor parses payloads. Success is a point-in-time
 * availability check, not a checksum, staging or later-read guarantee.
 */
inline Report checkAssets(const std::vector<AssetRequirement> &requirements,
                          const std::filesystem::path &base = {}) {
  Report report;
  report.level = "assets";
  report.supportedLevels = {"assets"};
  for (std::size_t index = 0; index < requirements.size(); ++index) {
    const auto &asset = requirements[index];
    const auto location = asset.location.empty() ? "assets[" + std::to_string(index) + "]" : asset.location;
    const auto &text = asset.path;
    if (text.empty() || text.find_first_of(std::string("\0\n\r", 3)) != std::string::npos ||
        text.find("://") != std::string::npos) {
      report.addIssue("asset.path", "Expected a nonempty local path without NUL/newlines", location);
      continue;
    }
    if (asset.kind != AssetKind::File && asset.kind != AssetKind::Directory) {
      report.addIssue("asset.kind", "Unknown asset kind", location);
      continue;
    }
    if (asset.kind == AssetKind::Directory && asset.expectedBytes) {
      report.addIssue("asset.size", "Expected byte counts apply only to regular files", location);
      continue;
    }
    const auto raw = std::filesystem::path(text);
    const auto path = raw.is_absolute() ? raw : base / raw;
    std::error_code error;
    const auto status = std::filesystem::status(path, error);
    if (error || !std::filesystem::exists(status)) {
      report.addIssue("asset.unavailable", "Cannot access " + path.string() +
                      (error ? ": " + error.message() : ": does not exist"), location);
      continue;
    }
    const bool correctKind = asset.kind == AssetKind::File ? std::filesystem::is_regular_file(status)
                                                           : std::filesystem::is_directory(status);
    if (!correctKind) {
      report.addIssue("asset.kind", "Unexpected filesystem object type: " + path.string(), location);
      continue;
    }
    if (asset.kind == AssetKind::File) {
      // The regular-file guard avoids opening devices, directories or FIFOs.
      std::ifstream stream(path, std::ios::binary);
      if (!stream.is_open())
        report.addIssue("asset.unreadable", "Cannot open file for reading: " + path.string(), location);
      if (asset.expectedBytes) {
        const auto bytes = std::filesystem::file_size(path, error);
        if (error)
          report.addIssue("asset.size", "Cannot read file size: " + path.string(), location);
        else if (bytes != *asset.expectedBytes)
          report.addIssue("asset.size", "Expected " + std::to_string(*asset.expectedBytes) +
                          " bytes, observed " + std::to_string(bytes) + ": " + path.string(), location);
      }
    }
  }
  return report;
}

/** Names/types are supplied by the caller, e.g. from a chosen RDF input node. */
struct ColumnInfo {
  std::string name;
  std::string type;
};

struct ColumnRequirement {
  std::string name;
  std::optional<std::string> type;
  std::string location;
};

/** Compare supplied schema metadata only. Types compare exactly; ROOT typedef
 * spelling/normalization, per-file coverage and optional-column fallback policy
 * remain caller-owned. Never creates a graph, compiles or starts an event loop.
 */
inline Report checkSchema(const std::vector<ColumnRequirement> &requirements,
                          const std::vector<ColumnInfo> &available) {
  Report report;
  report.level = "schema";
  report.supportedLevels = {"schema"};
  std::unordered_map<std::string, std::string> columns;
  auto validText = [](const std::string &value) {
    return value.find_first_not_of(" \t\n\r\f\v") != std::string::npos &&
           value.find_first_of(std::string("\0\n\r", 3)) == std::string::npos;
  };
  for (std::size_t index = 0; index < available.size(); ++index) {
    const auto &column = available[index];
    const auto location = "available[" + std::to_string(index) + "]";
    if (!validText(column.name)) {
      report.addIssue("schema.name", "Expected a nonempty column name without NUL/newlines", location);
      continue;
    }
    if (!columns.emplace(column.name, column.type).second)
      report.addIssue("schema.duplicate", "Duplicate available column: " + column.name, location);
  }
  std::unordered_set<std::string> names;
  for (std::size_t index = 0; index < requirements.size(); ++index) {
    const auto &column = requirements[index];
    const auto location = column.location.empty() ? "required[" + std::to_string(index) + "]" : column.location;
    if (!validText(column.name) || (column.type && !validText(*column.type))) {
      report.addIssue("schema.value", "Expected a column name and optional nonempty exact type", location);
      continue;
    }
    if (!names.insert(column.name).second) {
      report.addIssue("schema.duplicate", "Duplicate required column: " + column.name, location);
      continue;
    }
    const auto found = columns.find(column.name);
    if (found == columns.end())
      report.addIssue("schema.missing", "Required column is missing: " + column.name, location);
    else if (column.type && *column.type != found->second)
      report.addIssue("schema.type", "Expected " + *column.type + ", observed " +
                      (found->second.empty() ? "unknown type" : found->second) + ": " + column.name, location);
  }
  return report;
}

} // namespace rdf::validation
#endif
