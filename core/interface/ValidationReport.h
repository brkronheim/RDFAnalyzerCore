#ifndef RDF_VALIDATION_REPORT_H
#define RDF_VALIDATION_REPORT_H

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace rdf::validation {

/** Setup-only diagnostic values, compatible with Python rdf-validation-v1. */
struct Issue {
  std::string code;
  std::string message;
  std::string location;
};

namespace detail {
inline std::string jsonString(const std::string &value) {
  static constexpr char hex[] = "0123456789abcdef";
  std::string out = "\"";
  for (unsigned char c : value) {
    switch (c) {
    case '"': out += "\\\""; break;
    case '\\': out += "\\\\"; break;
    case '\n': out += "\\n"; break;
    case '\r': out += "\\r"; break;
    case '\t': out += "\\t"; break;
    default:
      if (c < 0x20) {
        out += "\\u00";
        out += hex[c >> 4];
        out += hex[c & 15];
      } else {
        out += static_cast<char>(c);
      }
    }
  }
  return out + '"';
}
} // namespace detail

struct Report {
  std::string level = "configuration";
  std::vector<std::string> supportedLevels{"configuration"};
  std::vector<Issue> issues;

  bool passed() const noexcept { return issues.empty(); }

  void addIssue(std::string code, std::string message, std::string location = {}) {
    issues.push_back({std::move(code), std::move(message), std::move(location)});
  }

  void throwIfInvalid() const {
    if (passed()) return;
    std::string message;
    for (const auto &issue : issues) {
      if (!message.empty()) message += "; ";
      if (!issue.location.empty()) message += issue.location + ": ";
      message += issue.message;
    }
    throw std::invalid_argument(message);
  }

  /** Explicit serialization; never opens inputs, compiles or runs an analysis. */
  std::string toJson() const {
    using detail::jsonString;
    std::string out = "{\"schema\":\"rdf-validation-v1\",\"level\":" + jsonString(level);
    out += ",\"supported_levels\":[";
    for (std::size_t i = 0; i < supportedLevels.size(); ++i) {
      if (i) out += ',';
      out += jsonString(supportedLevels[i]);
    }
    out += "],\"passed\":";
    out += passed() ? "true" : "false";
    out += ",\"issues\":[";
    for (std::size_t i = 0; i < issues.size(); ++i) {
      if (i) out += ',';
      const auto &issue = issues[i];
      out += "{\"code\":" + jsonString(issue.code) + ",\"message\":" + jsonString(issue.message)
          + ",\"location\":" + jsonString(issue.location) + '}';
    }
    return out + "]}";
  }
};

} // namespace rdf::validation
#endif
