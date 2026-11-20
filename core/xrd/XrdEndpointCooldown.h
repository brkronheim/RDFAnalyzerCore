#ifndef RDFANALYZER_XRD_ENDPOINT_COOLDOWN_H
#define RDFANALYZER_XRD_ENDPOINT_COOLDOWN_H

#include "XrdOpenTrace.h"
#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace rdf::xrd::detail {

// Endpoint identity excludes the file, query, credentials and site name. Track
// both an advertised front door and its resolved backend when they differ.
inline std::string cooldownKey(const std::string &address) {
  if (address.empty()) return {};
  XrdCl::URL url(address.find("://") == std::string::npos ? "root://" + address + "//" : address);
  if (!url.IsValid() || (url.GetProtocol() != "root" && url.GetProtocol() != "roots")) return {};
  auto key = OpenTrace::endpoint(url);
  std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c) { return std::tolower(c); });
  return key;
}

class EndpointCooldown {
public:
  using Clock = std::chrono::steady_clock;
  explicit EndpointCooldown(unsigned seconds = 60) : duration_(seconds) {}
  bool active(const std::string &endpoint, Clock::time_point now = Clock::now()) const {
    const auto key = cooldownKey(endpoint);
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = until_.find(key);
    return found != until_.end() && found->second > now;
  }
  std::vector<std::string> beaten(const std::vector<std::string> &losers,
      const std::vector<std::string> &winners, Clock::time_point now = Clock::now()) {
    std::set<std::string> winning;
    for (const auto &endpoint : winners) {
      const auto key = cooldownKey(endpoint);
      if (!key.empty()) winning.insert(key);
    }
    std::vector<std::string> changed;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto i=until_.begin(); i!=until_.end();)
      if (i->second <= now || winning.count(i->first)) i=until_.erase(i); else ++i;
    if (!duration_.count()) return changed;
    for (const auto &endpoint : losers) {
      const auto key = cooldownKey(endpoint);
      if (key.empty() || winning.count(key)) continue;
      const auto found = until_.find(key);
      if (found != until_.end() && found->second > now) continue; // Do not extend a late/duplicate loss.
      if (until_.size() >= 512) {
        const auto oldest = std::min_element(until_.begin(), until_.end(), [](const auto &a, const auto &b) { return a.second < b.second; });
        until_.erase(oldest);
      }
      until_[key] = now + duration_;
      changed.push_back(key);
    }
    return changed;
  }
  std::vector<std::string> hosts(Clock::time_point now = Clock::now()) const {
    std::set<std::string> result;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &item : until_)
      if (item.second > now) result.insert(XrdCl::URL("root://" + item.first + "//").GetHostName());
    return {result.begin(),result.end()};
  }
  unsigned seconds() const { return duration_.count(); }
private:
  const std::chrono::seconds duration_;
  mutable std::mutex mutex_;
  std::map<std::string,Clock::time_point> until_;
};

inline EndpointCooldown &endpointCooldown() {
  static auto *value = [] {
    unsigned seconds = 60;
    if (const auto *env = std::getenv("RDF_XRD_ENDPOINT_COOLDOWN_SECONDS")) {
      char *end = nullptr;
      const auto parsed = std::strtoul(env, &end, 10);
      if (*env && *env != '-' && end && !*end) seconds = std::min<unsigned long>(parsed, 3600);
    }
    return new EndpointCooldown(seconds);
  }();
  return *value;
}

// Preserve rank order; retain one cooled option only if every usable option
// is cooling down. Other cooled options remain in the caller's catalogue.
template <typename T, typename IsCooling>
std::vector<T> cooldownChoices(const std::vector<T> &candidates, IsCooling cooling) {
  std::vector<T> result;
  for (const auto &candidate : candidates) if (!cooling(candidate)) result.push_back(candidate);
  if (result.empty() && !candidates.empty()) result.push_back(candidates.front());
  return result;
}

inline std::vector<std::string> recordEndpointWinner(const std::vector<std::string> &losers,
    const std::vector<std::string> &winners, const std::function<void(const std::string &)> &log = {}) {
  auto &cooldown = endpointCooldown();
  auto changed = cooldown.beaten(losers,winners);
  if (log) for (const auto &key : changed)
    log("[XrdAdaptor] endpoint-cooldown server=" + key + " reason=beaten seconds=" + std::to_string(cooldown.seconds()));
  return changed;
}

} // namespace rdf::xrd::detail
#endif
