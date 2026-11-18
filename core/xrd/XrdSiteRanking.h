#ifndef RDFANALYZER_XRD_SITE_RANKING_H
#define RDFANALYZER_XRD_SITE_RANKING_H

#include <algorithm>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <istream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace rdf::xrd::detail {

// A dependency-free transport snapshot. Counts are support, never injected into
// live counters. Unknown origins and expired rows deliberately have no prior.
class SiteRanking {
public:
  struct Prior { double qualityMs; std::uint64_t jobs; std::int64_t updated; };

  static std::string normalized(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return value;
  }

  static SiteRanking read(std::istream &stream) {
    SiteRanking result;
    std::string line;
    if (!std::getline(stream, line) || line != "# rdf-xrd-site-rankings-v1")
      throw std::runtime_error("unsupported XRootD site ranking schema");
    unsigned lines = 0;
    while (std::getline(stream, line)) {
      if (++lines > 50000 || line.size() > 4096)
        throw std::runtime_error("XRootD site ranking exceeds size limit");
      if (line.empty() || line.front() == '#') continue;
      std::istringstream fields(line);
      std::string kind, a, b, extra;
      fields >> kind >> a >> b;
      a = normalized(a); b = normalized(b);
      if (kind == "alias") {
        if (a.empty() || b.empty() || (fields >> extra) ||
            !result.aliases_.emplace(a, b).second)
          throw std::runtime_error("invalid or duplicate ranking alias");
      } else if (kind == "site") {
        Prior prior{};
        if (!(fields >> prior.qualityMs >> prior.jobs >> prior.updated) ||
            (fields >> extra) || a.empty() || b.empty() ||
            !std::isfinite(prior.qualityMs) || prior.qualityMs < 1 ||
            prior.qualityMs > 1e9 || !prior.jobs || prior.jobs > 1000000000 || prior.updated <= 0 ||
            !result.priors_.emplace(std::make_pair(a, b), prior).second)
          throw std::runtime_error("invalid or duplicate ranking site");
      } else if (kind == "endpoint") {
        if (a.empty() || b.empty() || (fields >> extra) ||
            !result.endpoints_.emplace(a, b).second)
          throw std::runtime_error("invalid or duplicate ranking endpoint");
      } else {
        throw std::runtime_error("unknown ranking record type");
      }
    }
    return result;
  }

  std::string origin(const std::string &name) const {
    const auto key = normalized(name);
    const auto found = aliases_.find(key);
    return found == aliases_.end() ? key : found->second;
  }

  std::string siteForEndpoint(const std::string &host) const {
    const auto found = endpoints_.find(normalized(host));
    return found == endpoints_.end() ? std::string{} : found->second;
  }

  std::optional<double> quality(const std::string &originName,
                              const std::string &site, std::int64_t now,
                              std::int64_t maxAgeSeconds) const {
    const auto found = priors_.find({origin(originName), normalized(site)});
    if (found == priors_.end() || maxAgeSeconds <= 0) return {};
    const auto age = now - found->second.updated;
    if (age < -300 || age > maxAgeSeconds) return {};
    // Gradually weaken an old preference toward the unmeasured default.
    const double freshness = 1.0 - static_cast<double>(std::max<std::int64_t>(0, age)) / maxAgeSeconds;
    return freshness * found->second.qualityMs + (1.0 - freshness) * 260.0;
  }

  std::size_t size() const { return priors_.size(); }

private:
  std::map<std::string, std::string> aliases_, endpoints_;
  std::map<std::pair<std::string, std::string>, Prior> priors_;
};

// A prior influences only the cold start. Three live observations replace it;
// live values never cause historical samples to be re-exported as measurements.
inline double initialSiteQuality(std::optional<double> prior, double live,
                                 std::uint64_t liveSamples) {
  if (!prior || liveSamples >= 3) return live;
  return (*prior + live * liveSamples) / (1.0 + liveSamples);
}

struct SiteRank {
  double qualityMs = 260;
  bool preferred = false;
  bool enoughMeasurements = false;
  bool hasPrior = false;
};

inline bool siteRankBefore(const SiteRank &a, const SiteRank &b) {
  if (a.preferred != b.preferred) return a.preferred;
  if (a.enoughMeasurements != b.enoughMeasurements) return a.enoughMeasurements;
  if (a.enoughMeasurements && a.qualityMs != b.qualityMs) return a.qualityMs < b.qualityMs;
  return (a.hasPrior || b.hasPrior) && a.qualityMs < b.qualityMs;
}

} // namespace rdf::xrd::detail
#endif
