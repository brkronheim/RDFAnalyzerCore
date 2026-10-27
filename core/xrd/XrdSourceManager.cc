#include "XrdSourceManager.h"
#include "XrdOpenRetry.h"

#include <XrdCl/XrdClBuffer.hh>
#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClURL.hh>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace rdf::xrd {
namespace {
constexpr std::uint32_t kReadChunkBytes = 512 * 1024;
constexpr std::size_t kMaxChunksPerRequest = 1000;
constexpr std::size_t kMaxSourcesPerFile = 8;
constexpr std::uint64_t kMaxExplorationBytesPerRead = 2ULL * 1024 * 1024;
// Permit one active-file and one lookahead-file full read to share a site.
constexpr unsigned kMaxFullReadsPerSite = 2;
std::atomic<std::uint64_t> nextManagerId{1};

std::mutex &outputMutex() {
  static std::mutex value;
  return value;
}

bool isRemoteCondorJob() {
  static const bool remote = std::getenv("CONDOR_PROC") != nullptr ||
                             std::getenv("CONDOR_CLUSTER") != nullptr ||
                             std::getenv("_CONDOR_SCRATCH_DIR") != nullptr;
  return remote;
}

enum class LogLevel { Info, Error };

void logLine(const std::string &line, LogLevel level = LogLevel::Info) {
  if (isRemoteCondorJob() && level != LogLevel::Error) return;
  std::lock_guard<std::mutex> lock(outputMutex());
  std::cout << line << '\n';
}

struct GlobalSettings {
  std::mutex mutex;
  unsigned probeIntervalSeconds = 5;
  unsigned readTimeoutSeconds = 120;
  std::size_t prefetchCacheBytes = 256ULL * 1024 * 1024;
  unsigned preferredSiteBandPercent = 20;
  unsigned explorationSharePercent = 10;
  unsigned maxReadSources = 2;
};

GlobalSettings &settings() {
  static GlobalSettings value;
  return value;
}

unsigned explorationSharePercent() {
  auto &global = settings();
  std::lock_guard<std::mutex> lock(global.mutex);
  return global.explorationSharePercent;
}

unsigned maxReadSources() {
  auto &global = settings();
  std::lock_guard<std::mutex> lock(global.mutex);
  return global.maxReadSources;
}

unsigned preferredSiteBandPercent() {
  auto &global = settings();
  std::lock_guard<std::mutex> lock(global.mutex);
  return global.preferredSiteBandPercent;
}

struct CacheEntry {
  std::shared_ptr<std::vector<char>> data;
  std::uint64_t use = 0;
};

std::mutex &registryMutex() {
  static std::mutex value;
  return value;
}

std::map<std::string, std::weak_ptr<void>> &cacheRegistry() {
  static std::map<std::string, std::weak_ptr<void>> value;
  return value;
}

class VectorReadWaiter final : public XrdCl::ResponseHandler {
public:
  explicit VectorReadWaiter(std::uint64_t expected) : expected_(expected) {}

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *response) override {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    std::uint64_t bytes = 0;
    if (ownedResponse) {
      XrdCl::VectorReadInfo *info = nullptr;
      ownedResponse->Get(info);
      if (info) {
        bytes = info->GetSize();
        ownedResponse->Set(static_cast<int *>(nullptr));
        delete info;
      }
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ok_ = ownedStatus && ownedStatus->IsOK() && bytes == expected_;
      done_ = true;
    }
    condition_.notify_one();
  }

  void wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this] { return done_; });
  }

  bool ok() const { return ok_; }

private:
  const std::uint64_t expected_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  bool done_ = false;
  bool ok_ = false;
};

std::string hostnameOnly(const std::string &server) {
  const auto colon = server.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 == server.size())
    return server;
  if (std::all_of(server.begin() + static_cast<std::ptrdiff_t>(colon + 1),
                  server.end(), [](unsigned char c) { return c >= '0' && c <= '9'; }))
    return server.substr(0, colon);
  return server;
}

std::string appendOpaque(const std::string &url, const std::string &opaque) {
  const auto fragment = url.find('#');
  const auto base = url.substr(0, fragment);
  const auto tail = fragment == std::string::npos ? std::string{} : url.substr(fragment);
  const auto separator = base.find('?') == std::string::npos ? '?' : '&';
  return base + separator + opaque + tail;
}

std::string fileIdentity(const std::string &url) {
  const auto path = XrdCl::URL(url).GetPath();
  return path.empty() ? url : path;
}

std::string lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return value;
}

std::string fallbackSiteName(const std::string &server) {
  const auto host = lowercase(hostnameOnly(server));
  if (host.empty()) return "unknown-site";
  const auto dot = host.find('.');
  return dot == std::string::npos ? "host:" + host
                                  : "dns:" + host.substr(dot + 1);
}

struct ConfigQueryState {
  std::mutex mutex;
  std::condition_variable condition;
  bool done = false;
  std::unique_ptr<XrdCl::XRootDStatus> status;
  std::unique_ptr<XrdCl::Buffer> response;
};

class ConfigQueryHandler final : public XrdCl::ResponseHandler {
public:
  explicit ConfigQueryHandler(const std::string &url)
      : fileSystem_(XrdCl::URL(url)) {}

  static std::string querySite(const std::string &url) {
    auto handler = std::make_unique<ConfigQueryHandler>(url);
    auto state = std::make_shared<ConfigQueryState>();
    handler->state_ = state;
    XrdCl::Buffer argument;
    argument.FromString("sitename");
    const auto status = handler->fileSystem_.Query(
        XrdCl::QueryCode::Config, argument, handler.get());
    if (!status.IsOK()) return {};

    // Wait on our own condition variable. XrdCl's synchronous Query would
    // wait for a callback thread and can deadlock when called inside one.
    handler.release();
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->condition.wait_for(lock, std::chrono::seconds(1),
                                   [&state] { return state->done; }) ||
        !state->status || !state->status->IsOK() || !state->response)
      return {};
    return state->response->ToString();
  }

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *response) override {
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    auto state = state_.lock();
    delete this;
    if (!state) {
      delete status;
      return;
    }

    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (status && status->IsOK() && ownedResponse) {
        XrdCl::Buffer *buffer = nullptr;
        ownedResponse->Get(buffer);
        ownedResponse->Set(static_cast<int *>(nullptr));
        state->response.reset(buffer);
      }
      state->status.reset(status);
      state->done = true;
    }
    state->condition.notify_all();
  }

private:
  std::weak_ptr<ConfigQueryState> state_;
  XrdCl::FileSystem fileSystem_;
};

std::string querySiteName(const std::string &lastUrl) {
  if (lastUrl.empty()) return {};
  auto site = ConfigQueryHandler::querySite(lastUrl);
  while (!site.empty() &&
         (std::isspace(static_cast<unsigned char>(site.back())) ||
          site.back() == '\0'))
    site.pop_back();
  auto first = std::find_if_not(site.begin(), site.end(), [](unsigned char c) {
    return std::isspace(c) || c == '\0';
  });
  site.erase(site.begin(), first);
  if (site.empty() || lowercase(site) == "sitename") return {};
  return lowercase(site);
}

class SiteQualityMonitor {
public:
  struct QualitySnapshot {
    double qualityMs = 260;
    bool preferred = false;
    bool enoughMeasurements = false;
  };

  struct Decision {
    double sampleMs = 0;
    double qualityMs = 260;
    unsigned poorFileCount = 0;
    bool newlyPoorFile = false;
    bool blacklisted = false;
    bool newlyBlacklisted = false;
    std::string reason;
  };

  static SiteQualityMonitor &instance() {
    // Keep the monitor alive through process shutdown so the atexit summary
    // cannot race static destruction in a ROOT/XrdCl teardown path.
    static SiteQualityMonitor *monitor = [] {
      auto *value = new SiteQualityMonitor;
      std::atexit([] { SiteQualityMonitor::instance().printSummary(); });
      return value;
    }();
    return *monitor;
  }

  void configurePreferredBand(unsigned percent) {
    std::lock_guard<std::mutex> lock(mutex_);
    preferredBandPercent_ = std::min(percent, 100U);
    recomputePreferredLocked();
  }

  std::string identify(XrdCl::File &file, const std::string &server) {
    const auto serverKey = lowercase(hostnameOnly(server));
    if (serverKey.empty()) {
      std::lock_guard<std::mutex> lock(mutex_);
      sites_["unknown-site"];
      return "unknown-site";
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found = siteByServer_.find(serverKey);
      if (found != siteByServer_.end()) return found->second;
    }

    std::string lastUrl;
    file.GetProperty("LastURL", lastUrl);
    auto site = querySiteName(lastUrl);
    if (site.empty()) site = fallbackSiteName(server);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto result = siteByServer_.emplace(serverKey, std::move(site));
      sites_[result.first->second];
      return result.first->second;
    }
  }

  bool isBlacklisted(const std::string &site) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = sites_.find(site);
    return found != sites_.end() && found->second.blacklisted;
  }

  bool isPoorForFile(const std::string &site, const std::string &file) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = sites_.find(site);
    return found != sites_.end() && found->second.poorFiles.count(file) != 0;
  }

  bool hasEnoughMeasurements(const std::string &site) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = sites_.find(site);
    return found != sites_.end() && enoughMeasurements(found->second);
  }

  bool beginExplorationProbe(const std::string &site) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fullReadsInFlight_.count(site)) return false;
    return explorationProbesInFlight_.insert(site).second;
  }

  void endExplorationProbe(const std::string &site) {
    std::lock_guard<std::mutex> lock(mutex_);
    explorationProbesInFlight_.erase(site);
  }

  bool beginFullRead(const std::string &site) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &inFlight = fullReadsInFlight_[site];
    if (inFlight >= kMaxFullReadsPerSite) return false;
    ++inFlight;
    return true;
  }

  void endFullRead(const std::string &site) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = fullReadsInFlight_.find(site);
    if (found == fullReadsInFlight_.end()) return;
    if (found->second <= 1)
      fullReadsInFlight_.erase(found);
    else
      --found->second;
  }

  bool isPreferred(const std::string &site) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = sites_.find(site);
    return found != sites_.end() && found->second.preferred;
  }

  QualitySnapshot snapshot(const std::string &site) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = sites_.find(site);
    if (found == sites_.end()) return {};
    return {found->second.quality.value(), found->second.preferred,
            enoughMeasurements(found->second)};
  }

  Decision observe(const std::string &site, const std::string &file,
                   bool success, std::uint64_t bytes, double seconds) {
    // Scale large vectors to a 256 KiB equivalent, but keep the actual elapsed
    // response time for smaller reads. Extrapolating a 300-byte metadata read
    // to 256 KiB turns normal network latency into a fictitious multi-minute
    // quality sample.
    const double sampleMs = success && bytes > 0
                                ? std::max(1.0, seconds * 1000.0 *
                                                  std::min(1.0, kReferenceBytes /
                                                                    static_cast<double>(bytes)))
                                : kPoorQualityMs;
    std::lock_guard<std::mutex> lock(mutex_);
    auto &state = sites_[site];
    Decision decision;
    decision.sampleMs = sampleMs;
    ++state.qualitySamples;
    if (!success) {
      ++state.readErrors;
    } else if (bytes > 0) {
      ++state.successfulSamples;
      state.successfulBytes += bytes;
    }
    decision.qualityMs = state.quality.observe(sampleMs);
    if (!success) {
      markPoorLocked(state, file, "read failure", decision);
    } else if (decision.qualityMs > kPoorQualityMs) {
      markPoorLocked(state, file, "quality above 5130 ms", decision);
    }
    recomputePreferredLocked();
    decision.poorFileCount = state.poorFiles.size();
    decision.blacklisted = state.blacklisted;
    return decision;
  }

  Decision markRelativePoor(const std::string &site, const std::string &file,
                            double peerQualityMs) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &state = sites_[site];
    Decision decision;
    decision.qualityMs = state.quality.value();
    decision.sampleMs = decision.qualityMs;
    std::ostringstream reason;
    reason << "quality " << decision.qualityMs << " ms is over 4x peer "
           << peerQualityMs << " ms";
    markPoorLocked(state, file, reason.str(), decision);
    recomputePreferredLocked();
    decision.poorFileCount = state.poorFiles.size();
    decision.blacklisted = state.blacklisted;
    return decision;
  }

  void printSummary() const {
    struct Row {
      std::string site;
      double qualityMs = 0;
      std::uint64_t qualitySamples = 0;
      std::uint64_t successfulSamples = 0;
      std::uint64_t successfulBytes = 0;
      std::uint64_t readErrors = 0;
      std::size_t poorFiles = 0;
      bool preferred = false;
      bool blacklisted = false;
    };
    std::vector<Row> rows;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      rows.reserve(sites_.size());
      for (const auto &entry : sites_) {
        const auto &state = entry.second;
        rows.push_back({entry.first,
                        state.quality.value(),
                        state.qualitySamples,
                        state.successfulSamples,
                        state.successfulBytes,
                        state.readErrors,
                        state.poorFiles.size(),
                        state.preferred,
                        state.blacklisted});
      }
    }
    std::sort(rows.begin(), rows.end(), [](const Row &a, const Row &b) {
      const bool aMeasured = a.qualitySamples != 0;
      const bool bMeasured = b.qualitySamples != 0;
      if (aMeasured != bMeasured) return aMeasured;
      if (aMeasured && a.qualityMs != b.qualityMs)
        return a.qualityMs < b.qualityMs;
      return a.site < b.site;
    });

    std::cout << "[XrdAdaptor] site quality summary (lower quality_ms is better; "
                 "errors are failed read requests)\n";
    if (rows.empty()) {
      std::cout << "[XrdAdaptor]   no sites observed\n";
      return;
    }
    std::cout << "[XrdAdaptor]   rank site quality_ms preferred blacklisted "
                 "read_errors poor_files samples successful_bytes\n";
    for (std::size_t i = 0; i < rows.size(); ++i) {
      const auto &row = rows[i];
      std::cout << "[XrdAdaptor]   " << std::setw(4) << i + 1 << ' '
                << row.site << ' ';
      if (row.qualitySamples)
        std::cout << std::fixed << std::setprecision(1) << row.qualityMs;
      else
        std::cout << "n/a";
      std::cout << ' ' << (row.preferred ? "yes" : "no") << ' '
                << (row.blacklisted ? "yes" : "no") << ' '
                << row.readErrors << ' ' << row.poorFiles << ' '
                << row.qualitySamples << ' ' << row.successfulBytes << '\n';
    }
  }

private:
  static constexpr double kReferenceBytes = 256.0 * 1024.0;
  static constexpr double kPoorQualityMs = 5130.0;
  static constexpr std::uint64_t kMinimumSuccessfulSamples = 3;
  static constexpr std::uint64_t kMinimumSuccessfulBytes =
      3 * 256ULL * 1024ULL;

  struct RollingQuality {
    std::array<double, 4> history{{-1, -1, -1, -1}};
    std::chrono::steady_clock::time_point intervalStart{};
    double intervalSum = 0;
    std::uint64_t intervalSamples = 0;

    double observe(double sampleMs) {
      const auto now = std::chrono::steady_clock::now();
      if (intervalStart == std::chrono::steady_clock::time_point{})
        intervalStart = now;
      while (now - intervalStart >= std::chrono::minutes(1)) {
        for (std::size_t i = history.size() - 1; i > 0; --i)
          history[i] = history[i - 1];
        history[0] = intervalSamples
                         ? intervalSum / static_cast<double>(intervalSamples)
                         : -1;
        intervalStart += std::chrono::minutes(1);
        intervalSum = 0;
        intervalSamples = 0;
      }
      intervalSum += sampleMs;
      ++intervalSamples;
      return value();
    }

    double value() const {
      constexpr std::array<unsigned, 5> weights{{16, 8, 4, 2, 1}};
      double weighted = 0;
      unsigned denominator = 0;
      if (intervalSamples) {
        weighted += weights[0] * intervalSum /
                    static_cast<double>(intervalSamples);
        denominator += weights[0];
      }
      for (std::size_t i = 0; i < history.size(); ++i) {
        if (history[i] < 0) continue;
        weighted += weights[i + 1] * history[i];
        denominator += weights[i + 1];
      }
      return denominator ? weighted / denominator : 260.0;
    }
  };

  struct SiteState {
    RollingQuality quality;
    std::unordered_set<std::string> poorFiles;
    std::uint64_t qualitySamples = 0;
    std::uint64_t successfulSamples = 0;
    std::uint64_t successfulBytes = 0;
    std::uint64_t readErrors = 0;
    bool preferred = false;
    bool blacklisted = false;
  };

  static bool enoughMeasurements(const SiteState &state) {
    return state.successfulSamples >= kMinimumSuccessfulSamples &&
           state.successfulBytes >= kMinimumSuccessfulBytes;
  }

  void recomputePreferredLocked() {
    double best = std::numeric_limits<double>::infinity();
    for (const auto &entry : sites_) {
      const auto &state = entry.second;
      if (state.blacklisted || !enoughMeasurements(state)) continue;
      const auto quality = state.quality.value();
      if (quality <= kPoorQualityMs) best = std::min(best, quality);
    }
    if (!std::isfinite(best)) {
      for (auto &entry : sites_) entry.second.preferred = false;
      return;
    }
    for (auto &entry : sites_) {
      auto &state = entry.second;
      if (state.blacklisted || !enoughMeasurements(state) ||
          state.quality.value() > kPoorQualityMs) {
        state.preferred = false;
        continue;
      }
      const unsigned band = preferredBandPercent_ + (state.preferred ? 10U : 0U);
      const double cutoff = best * (1.0 + static_cast<double>(band) / 100.0);
      state.preferred = state.quality.value() <= cutoff;
    }
  }

  void markPoorLocked(SiteState &state, const std::string &file,
                      const std::string &reason, Decision &decision) {
    decision.reason = reason;
    decision.newlyPoorFile = state.poorFiles.insert(file).second;
    if (!state.blacklisted && state.poorFiles.size() >= 2) {
      state.blacklisted = true;
      decision.newlyBlacklisted = true;
    }
  }

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::string> siteByServer_;
  std::unordered_map<std::string, SiteState> sites_;
  std::unordered_set<std::string> explorationProbesInFlight_;
  std::unordered_map<std::string, unsigned> fullReadsInFlight_;
  unsigned preferredBandPercent_ = 20;
};

void logSiteDecision(const std::string &site, const std::string &file,
                     const SiteQualityMonitor::Decision &decision) {
  if (!decision.newlyPoorFile) return;
  std::ostringstream qualityLog;
  qualityLog << "[XrdAdaptor] site quality warning site=" << site
             << " file=" << file << " quality_ms=" << decision.qualityMs
             << " sample_ms=" << decision.sampleMs
             << " bad_files=" << decision.poorFileCount << "/2 reason="
             << decision.reason;
  logLine(qualityLog.str());
  if (decision.newlyBlacklisted) {
    logLine("[XrdAdaptor] site blacklisted for this process site=" + site +
            " after poor performance on multiple files");
  }
}

class DelayedClose final : public XrdCl::ResponseHandler {
public:
  explicit DelayedClose(std::shared_ptr<XrdCl::File> file)
      : file_(std::move(file)) {
    if (!file_ || !file_->IsOpen() || !file_->Close(this).IsOK()) delete this;
  }

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *) override {
    delete status;
    delete this;
  }

  void HandleResponseWithHosts(XrdCl::XRootDStatus *status,
                               XrdCl::AnyObject *,
                               XrdCl::HostList *hosts) override {
    delete status;
    delete hosts;
    delete this;
  }

private:
  std::shared_ptr<XrdCl::File> file_;
};

void closeAsync(std::shared_ptr<XrdCl::File> file) {
  if (file) new DelayedClose(std::move(file));
}

class ReadRaceState {
public:
  static constexpr std::size_t kNoWinner =
      std::numeric_limits<std::size_t>::max();

  bool complete(std::size_t attempt, bool success, const std::string &site,
                const std::string &file, const std::string &url,
                double qualityMs) {
    bool won = false;
    std::string poorSite;
    double peerQuality = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      results_[attempt] = success;
      if (success && winner_ == kNoWinner) {
        winner_ = attempt;
        won = true;
      }
      if (success) {
        for (const auto &peer : qualitySamples_) {
          if (peer.site == site) continue;
          if (qualityMs > 260. && qualityMs > 4. * peer.qualityMs) {
            poorSite = site;
            peerQuality = peer.qualityMs;
          } else if (peer.qualityMs > 260. &&
                     peer.qualityMs > 4. * qualityMs) {
            poorSite = peer.site;
            peerQuality = qualityMs;
          }
        }
        qualitySamples_.push_back({site, qualityMs});
      }
    }
    if (!poorSite.empty()) {
      const auto decision = SiteQualityMonitor::instance().markRelativePoor(
          poorSite, file, peerQuality);
      logSiteDecision(poorSite, url, decision);
    }
    condition_.notify_all();
    return won;
  }

  std::size_t winner() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return winner_;
  }

  bool finished(std::size_t attempt) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return results_.count(attempt) != 0;
  }

  bool succeeded(std::size_t attempt) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto result = results_.find(attempt);
    return result != results_.end() && result->second;
  }

  void waitForChange(std::chrono::milliseconds duration) {
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait_for(lock, duration);
  }

private:
  struct QualitySample {
    std::string site;
    double qualityMs = 260;
  };

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::unordered_map<std::size_t, bool> results_;
  std::vector<QualitySample> qualitySamples_;
  std::size_t winner_ = kNoWinner;
};

class VectorReadCallback final : public XrdCl::ResponseHandler {
public:
  using Completion = std::function<void(bool, std::uint64_t)>;

  VectorReadCallback(std::uint64_t expected, Completion completion)
      : expected_(expected), completion_(std::move(completion)) {}

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *response) override {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    std::uint64_t bytes = 0;
    if (ownedResponse) {
      XrdCl::VectorReadInfo *info = nullptr;
      ownedResponse->Get(info);
      if (info) {
        bytes = info->GetSize();
        ownedResponse->Set(static_cast<int *>(nullptr));
        delete info;
      }
    }
    const bool success = ownedStatus && ownedStatus->IsOK() &&
                         bytes == expected_;
    completion_(success, bytes);
    delete this;
  }

private:
  const std::uint64_t expected_;
  Completion completion_;
};
} // namespace

struct XrdSourceManager::Source {
  ~Source() { closeAsync(std::move(file)); }

  std::shared_ptr<XrdCl::File> file;
  std::string server;
  std::string site;
  std::string exclude;
  std::atomic<bool> failed{false};
};

struct XrdSourceManager::PendingRead {
  std::shared_ptr<Source> source;
  std::vector<std::unique_ptr<VectorReadWaiter>> waiters;
  std::vector<std::shared_ptr<std::vector<char>>> ownedBuffers;
  std::shared_ptr<ReadRaceState> race;
  std::chrono::steady_clock::time_point started;
  std::size_t sourceIndex = 0;
  std::uint64_t bytes = 0;
  bool allQueued = true;
  std::string slotKey;
  std::uint64_t expectedBytes = 0;
  std::size_t remainingBatches = 0;
  bool batchSuccess = true;
  std::uint64_t batchBytes = 0;
  bool completed = false;
  std::mutex completionMutex;

  void completeBatch(bool success, std::uint64_t completedBytes) {
    bool finishNow = false;
    bool finalSuccess = false;
    std::uint64_t totalBytes = 0;
    double elapsedSeconds = 0;
    {
      std::lock_guard<std::mutex> lock(completionMutex);
      if (completed || remainingBatches == 0) return;
      batchSuccess = batchSuccess && success;
      batchBytes += completedBytes;
      --remainingBatches;
      if (remainingBatches == 0) {
        completed = true;
        finishNow = true;
        totalBytes = batchBytes;
        finalSuccess = batchSuccess && totalBytes == expectedBytes;
        elapsedSeconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - started)
                             .count();
      }
    }
    if (!finishNow) return;

    if (!finalSuccess) source->failed = true;
    auto &monitor = SiteQualityMonitor::instance();
    const auto health = monitor.observe(source->site, fileKey, finalSuccess,
                                        finalSuccess ? totalBytes : expectedBytes,
                                        elapsedSeconds);
    logSiteDecision(source->site, url, health);
    const bool won = race->complete(sourceIndex, finalSuccess, source->site,
                                    fileKey, url, health.qualityMs);
    std::ostringstream result;
    result << std::fixed << std::setprecision(3)
           << "[XrdAdaptor] full read result handle=" << managerId
           << " role=" << role << " site=" << source->site << " server="
           << (source->server.empty() ? "<unknown>" : source->server)
           << " status=" << (finalSuccess ? "ok" : "failed")
           << " winner=" << (won ? "yes" : "no")
           << " bytes=" << totalBytes << " elapsed_s=" << elapsedSeconds
           << " quality_ms=" << health.qualityMs << " data_used="
           << (won ? "yes" : "no");
    logLine(result.str(), finalSuccess ? LogLevel::Info : LogLevel::Error);
    monitor.endFullRead(slotKey);
  }

  std::string fileKey;
  std::string url;
  std::string role;
  std::uint64_t managerId = 0;
};

class XrdSourceManager::ExplorationReadHandler final
    : public XrdCl::ResponseHandler {
public:
  ExplorationReadHandler(std::shared_ptr<Source> source, std::string file,
                         std::string url, std::string peerSite,
                         std::uint64_t managerId, std::uint64_t expectedBytes,
                         std::chrono::steady_clock::time_point started,
                         std::vector<std::shared_ptr<std::vector<char>>> buffers)
      : source_(std::move(source)), file_(std::move(file)), url_(std::move(url)),
        peerSite_(std::move(peerSite)), managerId_(managerId),
        expectedBytes_(expectedBytes), started_(started),
        buffers_(std::move(buffers)) {}

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *response) override {
    finish(status, response);
  }

private:
  void finish(XrdCl::XRootDStatus *status, XrdCl::AnyObject *response) {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    std::uint64_t bytes = 0;
    if (ownedResponse) {
      XrdCl::VectorReadInfo *info = nullptr;
      ownedResponse->Get(info);
      if (info) {
        bytes = info->GetSize();
        ownedResponse->Set(static_cast<int *>(nullptr));
        delete info;
      }
    }

    const bool success = ownedStatus && ownedStatus->IsOK() &&
                         bytes == expectedBytes_;
    const double elapsedSeconds = std::chrono::duration<double>(
                                      std::chrono::steady_clock::now() - started_)
                                      .count();
    auto &monitor = SiteQualityMonitor::instance();
    const auto health = monitor.observe(source_->site, file_, success,
                                        success ? bytes : expectedBytes_,
                                        elapsedSeconds);
    if (!success) source_->failed = true;
    logSiteDecision(source_->site, url_, health);
    if (success) {
      const auto peer = monitor.snapshot(peerSite_);
      if (peer.enoughMeasurements && health.qualityMs > 260. &&
          health.qualityMs > 4. * peer.qualityMs) {
        const auto relative = monitor.markRelativePoor(
            source_->site, file_, peer.qualityMs);
        logSiteDecision(source_->site, url_, relative);
      }
    }

    std::ostringstream result;
    result << std::fixed << std::setprecision(3)
           << "[XrdAdaptor] exploration sample handle=" << managerId_
           << " site=" << source_->site << " server="
           << (source_->server.empty() ? "<unknown>" : source_->server)
           << " status=" << (success ? "ok" : "failed")
           << " bytes=" << bytes << " elapsed_s=" << elapsedSeconds
           << " quality_ms=" << health.qualityMs << " blocking=no";
    logLine(result.str(), success ? LogLevel::Info : LogLevel::Error);
    monitor.endExplorationProbe(source_->site);
    delete this;
  }

  std::shared_ptr<Source> source_;
  std::string file_;
  std::string url_;
  std::string peerSite_;
  std::uint64_t managerId_ = 0;
  std::uint64_t expectedBytes_ = 0;
  std::chrono::steady_clock::time_point started_;
  std::vector<std::shared_ptr<std::vector<char>>> buffers_;
};

class XrdSourceManager::ProbeHandler final : public XrdCl::ResponseHandler {
public:
  ProbeHandler(std::weak_ptr<XrdSourceManager> manager,
               std::shared_ptr<XrdCl::File> file)
      : manager_(std::move(manager)), file_(std::move(file)) {}

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *response) override {
    finish(status, response, nullptr);
  }

  void HandleResponseWithHosts(XrdCl::XRootDStatus *status,
                               XrdCl::AnyObject *response,
                               XrdCl::HostList *hosts) override {
    std::unique_ptr<XrdCl::HostList> ownedHosts(hosts);
    finish(status, response, nullptr);
  }

private:
  void finish(XrdCl::XRootDStatus *status, XrdCl::AnyObject *response,
              XrdCl::HostList *) {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    bool adopted = false;
    if (auto manager = manager_.lock())
      adopted = manager->completeProbe(file_, ownedStatus.get());
    if (!adopted) closeAsync(std::move(file_));
    delete this;
  }

  std::weak_ptr<XrdSourceManager> manager_;
  std::shared_ptr<XrdCl::File> file_;
};

struct XrdSourceManager::PrefetchCache {
  explicit PrefetchCache(std::size_t maxBytes) : limit(maxBytes) {}

  bool copy(std::uint64_t offset, std::uint32_t length, char *destination) {
    std::lock_guard<std::mutex> lock(mutex);
    std::uint64_t cursor = offset;
    const std::uint64_t end = offset + length;
    auto it = entries.upper_bound(cursor);
    std::vector<std::pair<decltype(entries.begin()), std::uint64_t>> spans;
    while (cursor < end) {
      if (it == entries.begin()) return false;
      auto candidate = std::prev(it);
      const auto start = candidate->first;
      const auto spanEnd = start + candidate->second.data->size();
      if (cursor < start || cursor >= spanEnd) return false;
      spans.emplace_back(candidate, cursor - start);
      cursor = std::min(end, spanEnd);
      it = entries.lower_bound(cursor);
    }
    cursor = offset;
    for (const auto &span : spans) {
      auto entry = span.first;
      const auto beginInEntry = span.second;
      const auto count = std::min<std::uint64_t>(end - cursor,
                                                 entry->second.data->size() - beginInEntry);
      std::memcpy(destination + (cursor - offset), entry->second.data->data() + beginInEntry,
                  static_cast<std::size_t>(count));
      entry->second.use = ++clock;
      cursor += count;
    }
    return cursor == end;
  }

  void insert(std::uint64_t offset, std::shared_ptr<std::vector<char>> data) {
    if (!data || data->empty() || data->size() > limit) return;
    std::lock_guard<std::mutex> lock(mutex);
    auto found = entries.find(offset);
    if (found != entries.end()) {
      usedBytes -= found->second.data->size();
      entries.erase(found);
    }
    while (usedBytes + data->size() > limit && !entries.empty()) {
      auto oldest = std::min_element(entries.begin(), entries.end(), [](const auto &a, const auto &b) {
        return a.second.use < b.second.use;
      });
      usedBytes -= oldest->second.data->size();
      entries.erase(oldest);
    }
    usedBytes += data->size();
    entries.emplace(offset, CacheEntry{std::move(data), ++clock});
  }

  std::size_t limit;
  std::mutex mutex;
  std::map<std::uint64_t, CacheEntry> entries;
  std::size_t usedBytes = 0;
  std::uint64_t clock = 0;
};

namespace {
std::shared_ptr<XrdSourceManager::PrefetchCache> acquireCache(
    const std::string &url, std::size_t maxBytes) {
  std::lock_guard<std::mutex> lock(registryMutex());
  for (auto item = cacheRegistry().begin(); item != cacheRegistry().end();) {
    if (item->first != url && item->second.expired())
      item = cacheRegistry().erase(item);
    else
      ++item;
  }
  auto &slot = cacheRegistry()[url];
  if (auto existing = slot.lock())
    return std::static_pointer_cast<XrdSourceManager::PrefetchCache>(existing);
  auto cache = std::make_shared<XrdSourceManager::PrefetchCache>(maxBytes);
  slot = cache;
  return cache;
}
} // namespace

XrdSourceManager::XrdSourceManager(std::string url,
                                   unsigned probeIntervalSeconds,
                                   std::size_t prefetchCacheBytes,
                                   unsigned readTimeoutSeconds)
    : url_(std::move(url)), fileKey_(fileIdentity(url_)),
      probeIntervalSeconds_(probeIntervalSeconds),
      readTimeoutSeconds_(std::clamp(readTimeoutSeconds, 1U, 300U)),
      nextProbeAt_(std::chrono::steady_clock::now()),
      cache_(acquireCache(url_, prefetchCacheBytes)) {
  SiteQualityMonitor::instance().configurePreferredBand(
      preferredSiteBandPercent());
  instanceId_ = nextManagerId.fetch_add(1);
  auto primary = std::make_shared<Source>();
  const auto timeout = static_cast<std::uint16_t>(readTimeoutSeconds_);
  std::string lastError;
  const bool opened = detail::openWithRetry(
      url_,
      [&](const std::string &candidateUrl, std::size_t attempt) {
        // A failed XrdCl handle must not be reused on the next attempt.
        auto file = std::make_shared<XrdCl::File>();
        const auto status = file->Open(candidateUrl, XrdCl::OpenFlags::Read,
                                       XrdCl::Access::None, timeout);
        if (!status.IsOK()) {
          lastError = "XrdCl could not open '" + candidateUrl + "': " + status.ToStr();
        } else {
          XrdCl::StatInfo *statInfo = nullptr;
          const auto statStatus = file->Stat(false, statInfo, timeout);
          std::unique_ptr<XrdCl::StatInfo> ownedStat(statInfo);
          if (statStatus.IsOK() && ownedStat) {
            fileSize_ = ownedStat->GetSize();
            primary->file = std::move(file);
            // Continue replica discovery through the endpoint that worked.
            url_ = candidateUrl;
            return true;
          }
          lastError = "XrdCl could not stat '" + candidateUrl + "': " + statStatus.ToStr();
        }
        logLine("[XrdAdaptor] initial open attempt " + std::to_string(attempt) +
                    "/3 failed: " + lastError, LogLevel::Error);
        return false;
      },
      [](std::chrono::seconds delay) { std::this_thread::sleep_for(delay); });
  if (!opened) throw std::runtime_error(lastError + " (after 3 attempts)");

  primary->file->GetProperty("DataServer", primary->server);
  if (primary->server.empty()) {
    std::string lastUrl;
    if (primary->file->GetProperty("LastURL", lastUrl))
      primary->server = XrdCl::URL(lastUrl).GetHostId();
  }
  primary->exclude = hostnameOnly(primary->server);
  if (!primary->exclude.empty()) triedHosts_.push_back(primary->exclude);
  primary->site = SiteQualityMonitor::instance().identify(*primary->file,
                                                          primary->server);
  if (SiteQualityMonitor::instance().isBlacklisted(primary->site)) {
    logLine("[XrdAdaptor] quarantining primary source on blacklisted site handle=" +
            std::to_string(instanceId_) + " site=" + primary->site +
            " server=" +
            (primary->server.empty() ? "<unknown>" : primary->server));
  }
  sources_.push_back(std::move(primary));
  const auto &openedPrimary = sources_.front();
  logLine("[XrdAdaptor] source handle=" + std::to_string(instanceId_) +
          " role=primary file=" + url_ + " site=" + openedPrimary->site +
          " server=" +
          (openedPrimary->server.empty() ? "<unknown>" : openedPrimary->server));
}

XrdSourceManager::~XrdSourceManager() { stop(); }

void XrdSourceManager::startDiscovery() {
  if (probeIntervalSeconds_ == 0) return;
  const auto explorationShare = explorationSharePercent();
  const auto desiredSites = std::min(
      3U, maxReadSources() + (explorationShare > 0 ? 1U : 0U));
  std::vector<std::string> tried;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    sources_.erase(std::remove_if(sources_.begin(), sources_.end(),
                                  [](const auto &source) {
                                    return source->failed.load();
                                  }),
                   sources_.end());
    std::unordered_set<std::string> healthySites;
    for (const auto &source : sources_) {
      if (source->failed.load() ||
          SiteQualityMonitor::instance().isBlacklisted(source->site) ||
          SiteQualityMonitor::instance().isPoorForFile(source->site, fileKey_))
        continue;
      healthySites.insert(source->site);
    }
    if (stopped_ || probeInFlight_ || healthySites.size() >= desiredSites ||
        sources_.size() >= kMaxSourcesPerFile ||
        std::chrono::steady_clock::now() < nextProbeAt_ || triedHosts_.empty())
      return;
    probeInFlight_ = true;
    tried = triedHosts_;
  }

  std::string opaque = "tried=";
  for (std::size_t i = 0; i < tried.size(); ++i) {
    if (i) opaque += ',';
    opaque += tried[i];
  }
  opaque += "&triedrc=resel";
  std::ostringstream probeLog;
  probeLog << "[XrdAdaptor] replica probe handle=" << instanceId_
           << " file=" << url_ << " excluding_hosts=[";
  for (std::size_t i = 0; i < tried.size(); ++i) {
    if (i) probeLog << ',';
    probeLog << tried[i];
  }
  probeLog << ']';
  logLine(probeLog.str());
  auto file = std::make_shared<XrdCl::File>();
  auto *handler = new ProbeHandler(weak_from_this(), file);
  const auto candidateUrl = appendOpaque(url_, opaque);
  const auto status = file->Open(candidateUrl, XrdCl::OpenFlags::Read,
                                 XrdCl::Access::None, handler,
                                 static_cast<std::uint16_t>(readTimeoutSeconds_));
  if (!status.IsOK()) {
    logLine("[XrdAdaptor] replica probe request failed handle=" +
            std::to_string(instanceId_) + " file=" + url_ +
            " status=" + status.ToStr(),
            LogLevel::Error);
    delete handler;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      probeInFlight_ = false;
      nextProbeAt_ = std::chrono::steady_clock::now() +
                     std::chrono::seconds(probeIntervalSeconds_);
    }
    condition_.notify_all();
  }
}

void XrdSourceManager::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
    sources_.clear();
  }
  condition_.notify_all();
}

std::vector<std::shared_ptr<XrdSourceManager::Source>>
XrdSourceManager::sources(bool includeBlacklistedFallbacks) const {
  const auto explorationShare = explorationSharePercent();
  const auto fullReadLimit = maxReadSources();
  const auto sourceLimit =
      std::min(3U, fullReadLimit + (explorationShare > 0 ? 1U : 0U));
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::shared_ptr<Source>> healthySources;
  std::vector<std::shared_ptr<Source>> poorFallbacks;
  std::vector<std::shared_ptr<Source>> blacklistedFallbacks;
  for (const auto &source : sources_) {
    if (source->failed.load()) continue;
    if (SiteQualityMonitor::instance().isBlacklisted(source->site)) {
      blacklistedFallbacks.push_back(source);
      continue;
    }
    if (SiteQualityMonitor::instance().isPoorForFile(source->site, fileKey_))
      poorFallbacks.push_back(source);
    else
      healthySources.push_back(source);
  }
  auto &monitor = SiteQualityMonitor::instance();
  auto choose = [&monitor, sourceLimit, fullReadLimit](
                    std::vector<std::shared_ptr<Source>> candidates) {
    struct RankedSource {
      std::shared_ptr<Source> source;
      SiteQualityMonitor::QualitySnapshot quality;
    };
    std::vector<std::shared_ptr<Source>> selected;
    if (candidates.empty()) return selected;
    std::vector<RankedSource> ranked;
    ranked.reserve(candidates.size());
    for (auto &candidate : candidates)
      ranked.push_back({candidate, monitor.snapshot(candidate->site)});
    std::stable_sort(ranked.begin(), ranked.end(),
                     [](const RankedSource &a, const RankedSource &b) {
                       if (a.quality.preferred != b.quality.preferred)
                         return a.quality.preferred;
                       if (a.quality.enoughMeasurements !=
                           b.quality.enoughMeasurements)
                         return a.quality.enoughMeasurements;
                       if (a.quality.enoughMeasurements &&
                           a.quality.qualityMs != b.quality.qualityMs)
                         return a.quality.qualityMs < b.quality.qualityMs;
                       return false;
                     });

    const auto distinctSite = [](const Source &source) {
      if (!source.site.empty() && source.site != "unknown-site")
        return source.site;
      const auto host = hostnameOnly(source.server);
      return host.empty() ? source.site : host;
    };

    // A known preferred site is the first option; otherwise favor the best
    // measured source and retain the redirector order for unmeasured sites.
    auto primary = std::find_if(ranked.begin(), ranked.end(),
                                [](const auto &candidate) {
                                  return candidate.quality.preferred;
                                });
    if (primary == ranked.end())
      primary = std::find_if(ranked.begin(), ranked.end(),
                             [](const auto &candidate) {
                               return candidate.quality.enoughMeasurements;
                             });
    if (primary == ranked.end()) primary = ranked.begin();
    selected.push_back(primary->source);

    const auto appendDistinct = [&](const RankedSource &candidate) {
      if (selected.size() >= sourceLimit) return false;
      const auto key = distinctSite(*candidate.source);
      const bool alreadySelected = std::any_of(
          selected.begin(), selected.end(), [&](const auto &source) {
            return distinctSite(*source) == key;
          });
      if (alreadySelected) return false;
      selected.push_back(candidate.source);
      return true;
    };

    // Fill configured full-read slots in quality order. Any additional slot is
    // reserved for an unmeasured site when one is available to explore.
    while (selected.size() < fullReadLimit) {
      bool appended = false;
      for (const auto &candidate : ranked) {
        if (appendDistinct(candidate)) {
          appended = true;
          break;
        }
      }
      if (!appended) break;
    }
    if (selected.size() < sourceLimit) {
      const auto explorer = std::find_if(
          ranked.begin(), ranked.end(), [&](const auto &candidate) {
            if (candidate.quality.enoughMeasurements) return false;
            const auto key = distinctSite(*candidate.source);
            return std::none_of(selected.begin(), selected.end(),
                                [&](const auto &source) {
                                  return distinctSite(*source) == key;
                                });
          });
      if (explorer != ranked.end()) appendDistinct(*explorer);
    }
    for (const auto &candidate : ranked) appendDistinct(candidate);
    return selected;
  };

  if (!healthySources.empty()) return choose(std::move(healthySources));
  if (!poorFallbacks.empty()) return choose(std::move(poorFallbacks));
  return includeBlacklistedFallbacks ? choose(std::move(blacklistedFallbacks))
                                     : std::vector<std::shared_ptr<Source>>{};
}

bool XrdSourceManager::completeProbe(
    const std::shared_ptr<XrdCl::File> &file,
    const XrdCl::XRootDStatus *status) {
  std::string server;
  if (status && status->IsOK()) {
    file->GetProperty("DataServer", server);
    if (server.empty()) {
      std::string lastUrl;
      if (file->GetProperty("LastURL", lastUrl))
        server = XrdCl::URL(lastUrl).GetHostId();
    }
  }
  const auto exclude = hostnameOnly(server);
  std::string site;
  bool globallyBlacklisted = false;
  bool poorForThisFile = false;
  if (status && status->IsOK() && !server.empty()) {
    site = SiteQualityMonitor::instance().identify(*file, server);
    globallyBlacklisted =
        SiteQualityMonitor::instance().isBlacklisted(site);
    poorForThisFile =
        SiteQualityMonitor::instance().isPoorForFile(site, fileKey_);
  }

  bool added = false;
  bool alreadyActive = false;
  bool blacklistedFallbackAvailable = false;
  bool sourceLimitReached = false;
  bool repeatedHost = false;
  bool stopped = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    probeInFlight_ = false;
    const auto probedAt = std::chrono::steady_clock::now();
    nextProbeAt_ = probedAt + std::chrono::seconds(probeIntervalSeconds_);
    stopped = stopped_;
    if (!stopped && status && status->IsOK() && !server.empty()) {
      if (!exclude.empty()) {
        repeatedHost = std::find(triedHosts_.begin(), triedHosts_.end(), exclude) !=
                       triedHosts_.end();
        if (!repeatedHost) triedHosts_.push_back(exclude);
      }
      const auto duplicate = std::find_if(sources_.begin(), sources_.end(), [&](const auto &source) {
        return source->server == server ||
               (!exclude.empty() && source->exclude == exclude);
      });
      if (duplicate == sources_.end()) {
        if (globallyBlacklisted || !poorForThisFile) {
          if (sources_.size() < kMaxSourcesPerFile) {
            auto alternate = std::make_shared<Source>();
            alternate->file = file;
            alternate->server = server;
            alternate->site = site;
            alternate->exclude = exclude;
            sources_.push_back(std::move(alternate));
            added = true;
            blacklistedFallbackAvailable = globallyBlacklisted;
          } else {
            sourceLimitReached = true;
          }
        }
      } else {
        alreadyActive = true;
        blacklistedFallbackAvailable = globallyBlacklisted;
      }
      // Redirectors can occasionally return a host that is already in the
      // tried list. Avoid reopening the same endpoint every probe interval.
      if (repeatedHost) {
        nextProbeAt_ = std::max(nextProbeAt_, probedAt +
                                                   std::chrono::seconds(30));
      } else if (added) {
        const auto explorationShare = explorationSharePercent();
        const auto desiredSites = std::min(
            3U, maxReadSources() + (explorationShare > 0 ? 1U : 0U));
        std::unordered_set<std::string> healthySites;
        for (const auto &source : sources_) {
          if (source->failed.load() ||
              SiteQualityMonitor::instance().isBlacklisted(source->site) ||
              SiteQualityMonitor::instance().isPoorForFile(source->site,
                                                           fileKey_))
            continue;
          healthySites.insert(source->site);
        }
        if (healthySites.size() < desiredSites) nextProbeAt_ = probedAt;
      }
    }
  }
  std::ostringstream resultLog;
  resultLog << "[XrdAdaptor] replica probe result handle=" << instanceId_
            << " file=" << url_;
  if (!status || !status->IsOK())
    resultLog << " status=failed detail="
              << (status ? status->ToStr() : "<no status>");
  else if (server.empty())
    resultLog << " status=opened server=<unknown> action=discarded";
  else
    resultLog << " status=opened server=" << server << " site=" << site
              << " action="
              << (globallyBlacklisted
                      ? (blacklistedFallbackAvailable
                             ? "site-blacklisted-fallback"
                             : "site-blacklisted")
                      : (poorForThisFile
                             ? "poor-site-for-this-file"
                             : (added ? "added"
                                      : (alreadyActive ? "already-active"
                                                       : "discarded"))));
  if (repeatedHost) resultLog << " repeated_host=1 retry_backoff_s=30";
  if (sourceLimitReached) resultLog << " source_limit=" << kMaxSourcesPerFile;
  if (stopped) resultLog << " manager=stopped";
  logLine(resultLog.str(),
          (!status || !status->IsOK()) ? LogLevel::Error : LogLevel::Info);
  condition_.notify_all();
  return added;
}

bool XrdSourceManager::copyPrefetched(std::uint64_t offset,
                                      std::uint32_t length,
                                      char *destination) {
  return cache_->copy(offset, length, destination);
}

void XrdSourceManager::savePrefetched(
    std::uint64_t offset, std::shared_ptr<std::vector<char>> bytes) {
  cache_->insert(offset, std::move(bytes));
}

std::unique_ptr<XrdSourceManager::PendingRead> XrdSourceManager::submit(
    const std::shared_ptr<Source> &source,
    const std::vector<ReadRange> &ranges, std::size_t sourceIndex) {
  auto pending = std::make_unique<PendingRead>();
  pending->source = source;
  pending->sourceIndex = sourceIndex;
  pending->started = std::chrono::steady_clock::now();
  for (std::size_t first = 0; first < ranges.size(); first += kMaxChunksPerRequest) {
    const auto last = std::min(ranges.size(), first + kMaxChunksPerRequest);
    XrdCl::ChunkList chunks;
    chunks.reserve(last - first);
    std::uint64_t expected = 0;
    for (std::size_t i = first; i < last; ++i) {
      chunks.emplace_back(ranges[i].offset, ranges[i].length, ranges[i].buffer);
      expected += ranges[i].length;
    }
    auto waiter = std::make_unique<VectorReadWaiter>(expected);
    const auto status = source->file->VectorRead(
        chunks, nullptr, waiter.get(),
        static_cast<std::uint16_t>(readTimeoutSeconds_));
    if (!status.IsOK()) {
      source->failed = true;
      pending->allQueued = false;
      // Other batches may already be outstanding; keep their handlers alive
      // until every accepted request has delivered its callback.
      continue;
    }
    pending->waiters.push_back(std::move(waiter));
    pending->bytes += expected;
  }
  return pending;
}

bool XrdSourceManager::finish(PendingRead &pending, std::uint64_t &bytes,
                              double &seconds) {
  bool success = pending.allQueued;
  for (auto &waiter : pending.waiters) {
    waiter->wait();
    success = success && waiter->ok();
  }
  bytes = pending.bytes;
  seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                          pending.started)
                .count();
  if (!success) pending.source->failed = true;
  return success;
}

bool XrdSourceManager::issue(const std::shared_ptr<Source> &source,
                             const std::vector<ReadRange> &ranges,
                             std::uint64_t &bytes, double &seconds) {
  auto pending = submit(source, ranges, 0);
  return finish(*pending, bytes, seconds);
}

void XrdSourceManager::scheduleExplorationProbe(
    const std::shared_ptr<Source> &source, const std::string &peerSite,
    const std::vector<ReadRange> &ranges) {
  if (!source || ranges.empty()) return;
  auto &monitor = SiteQualityMonitor::instance();
  if (!monitor.beginExplorationProbe(source->site)) {
    logLine("[XrdAdaptor] exploration sample skipped handle=" +
            std::to_string(instanceId_) + " site=" + source->site +
            " reason=site-sample-in-flight");
    return;
  }

  XrdCl::ChunkList chunks;
  std::vector<std::shared_ptr<std::vector<char>>> buffers;
  buffers.reserve(ranges.size());
  std::uint64_t expectedBytes = 0;
  for (const auto &range : ranges) {
    auto buffer = std::make_shared<std::vector<char>>(range.length);
    chunks.emplace_back(range.offset, range.length, buffer->data());
    expectedBytes += range.length;
    buffers.push_back(std::move(buffer));
  }

  const auto started = std::chrono::steady_clock::now();
  auto *handler = new ExplorationReadHandler(
      source, fileKey_, url_, peerSite, instanceId_, expectedBytes, started,
      std::move(buffers));
  const auto status = source->file->VectorRead(
      chunks, nullptr, handler, static_cast<std::uint16_t>(readTimeoutSeconds_));
  if (!status.IsOK()) {
    monitor.endExplorationProbe(source->site);
    logLine("[XrdAdaptor] exploration sample could not be queued handle=" +
            std::to_string(instanceId_) + " site=" + source->site +
            " status=" + status.ToStr() + " blocking=no",
            LogLevel::Error);
    delete handler;
  } else {
    logLine("[XrdAdaptor] exploration sample queued handle=" +
            std::to_string(instanceId_) + " site=" + source->site +
            " bytes=" + std::to_string(expectedBytes) + " blocking=no");
  }
}

bool XrdSourceManager::readv(const std::vector<ReadRange> &ranges,
                             bool prefetch) {
  const auto readStarted = std::chrono::steady_clock::now();
  const auto requestSequence = readvSequence_.fetch_add(1) + 1;
  const auto demandSequence =
      prefetch ? 0 : demandReadSequence_.fetch_add(1) + 1;
  struct Piece {
    ReadRange range;
    std::shared_ptr<std::vector<char>> owned;
  };

  std::vector<Piece> pieces;
  std::uint64_t requestedBytes = 0;
  std::uint64_t handoffBytes = 0;
  std::uint64_t handoffStoredBytes = 0;
  for (const auto &range : ranges) {
    if (range.length == 0) continue;
    requestedBytes += range.length;
    if (!prefetch) {
      if (!range.buffer) return false;
      if (copyPrefetched(range.offset, range.length, range.buffer)) {
        handoffBytes += range.length;
        continue;
      }
    }
    std::uint32_t consumed = 0;
    while (consumed < range.length) {
      const auto part = std::min(kReadChunkBytes, range.length - consumed);
      Piece piece;
      piece.range.offset = range.offset + consumed;
      piece.range.length = part;
      if (prefetch) {
        piece.owned = std::make_shared<std::vector<char>>(part);
        piece.range.buffer = piece.owned->data();
      } else {
        piece.range.buffer = range.buffer + consumed;
      }
      pieces.push_back(std::move(piece));
      consumed += part;
    }
  }

  const auto logReadTiming = [&](const char *status,
                                 const std::vector<std::shared_ptr<Source>> &active) {
    const double elapsedMs = std::chrono::duration<double, std::milli>(
                                 std::chrono::steady_clock::now() - readStarted)
                                 .count();
    // Report every async-prefetch request, plus the first demand read, handoff
    // hits, and slow demand reads. This keeps routine fast reads quiet while
    // exposing the requests most likely to contribute to reader stalls.
    if (!prefetch && demandSequence != 1 && handoffBytes == 0 && elapsedMs < 100.)
      return;
    std::ostringstream timingLog;
    timingLog << std::fixed << std::setprecision(3)
              << "[XrdAdaptor] readv timing handle=" << instanceId_
              << " sequence=" << requestSequence
              << " demand_sequence=" << demandSequence
              << " file=" << url_
              << " mode=" << (prefetch ? "prefetch" : "read")
              << " status=" << status
              << " requested_bytes=" << requestedBytes
              << " handoff_hit_bytes=" << handoffBytes
              << " handoff_stored_bytes=" << handoffStoredBytes
              << " bytes_not_from_handoff=" << (requestedBytes - handoffBytes)
              << " request_timeout_s=" << readTimeoutSeconds_
              << " elapsed_ms=" << elapsedMs
              << " sources=[";
    for (std::size_t i = 0; i < active.size(); ++i) {
      if (i) timingLog << ';';
      timingLog << active[i]->site << ':' << active[i]->server;
    }
    timingLog << ']';
    const bool failed = std::string(status) == "no-source" ||
                        std::string(status) == "retry-failed" ||
                        std::string(status) == "all-replicas-failed" ||
                        std::string(status) == "race-winner-unavailable";
    logLine(timingLog.str(), failed ? LogLevel::Error : LogLevel::Info);
  };

  if (pieces.empty()) {
    if (handoffBytes > 0) {
      std::ostringstream cacheLog;
      cacheLog << "[XrdAdaptor] handoff cache satisfied request handle="
               << instanceId_ << " file=" << url_ << " bytes="
               << handoffBytes << " (no replica request)";
      logLine(cacheLog.str());
    }
    const std::vector<std::shared_ptr<Source>> noSources;
    logReadTiming("handoff", noSources);
    return true;
  }

  startDiscovery();
  auto active = sources();
  if (active.empty()) {
    if (probeIntervalSeconds_ != 0) {
      const auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::seconds(30);
      while (active.empty() && std::chrono::steady_clock::now() < deadline) {
        startDiscovery();
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopped_) return false;
        auto wakeAt = deadline;
        if (!probeInFlight_) wakeAt = std::min(wakeAt, nextProbeAt_);
        condition_.wait_until(lock, wakeAt);
        lock.unlock();
        active = sources();
      }
    }
    // CMSSW keeps a sole source active even after a poor-quality decision. If
    // every discovered source is globally blacklisted, preserve the same
    // last-resort behavior instead of failing ROOT's initial metadata read.
    if (active.empty()) active = sources(true);
  }
  if (active.empty()) {
    logReadTiming("no-source", active);
    return false;
  }
  const auto fullReadSiteLimit = maxReadSources();
  const auto candidateSiteLimit =
      std::min(3U, fullReadSiteLimit + (explorationSharePercent() > 0 ? 1U : 0U));
  const auto explorationShare = explorationSharePercent();
  auto &siteMonitor = SiteQualityMonitor::instance();
  std::uint64_t networkBytes = 0;
  for (const auto &piece : pieces) networkBytes += piece.range.length;
  const auto explorationBudget =
      std::min(networkBytes * explorationShare / 100,
               kMaxExplorationBytesPerRead);
  const auto sourceKey = [](const std::shared_ptr<Source> &source) {
    if (!source->site.empty() && source->site != "unknown-site")
      return source->site;
    const auto host = hostnameOnly(source->server);
    return host.empty() ? source->server : host;
  };
  const auto race = std::make_shared<ReadRaceState>();
  std::vector<std::shared_ptr<PendingRead>> fullAttempts;
  std::unordered_set<std::string> attemptedSites;
  std::vector<std::shared_ptr<Source>> readSources;
  std::vector<std::shared_ptr<Source>> explorationSources;
  bool explorationScheduled = false;

  const auto launchFullRead = [&](const std::shared_ptr<Source> &source) {
    if (!source || source->failed.load()) return false;
    const auto key = sourceKey(source);
    if (key.empty() || attemptedSites.count(key)) return false;
    if (!siteMonitor.beginFullRead(key)) {
      bool reportLimit = false;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        reportLimit = std::find(reportedFullReadLimitSites_.begin(),
                                reportedFullReadLimitSites_.end(), key) ==
                      reportedFullReadLimitSites_.end();
        if (reportLimit) reportedFullReadLimitSites_.push_back(key);
      }
      if (reportLimit)
        logLine("[XrdAdaptor] full read skipped handle=" +
                std::to_string(instanceId_) + " site=" + source->site +
                " server=" + source->server +
                " reason=site-full-read-limit cap=" +
                std::to_string(kMaxFullReadsPerSite));
      return false;
    }

    auto attempt = std::make_shared<PendingRead>();
    attempt->source = source;
    attempt->race = race;
    attempt->sourceIndex = fullAttempts.size();
    attempt->started = std::chrono::steady_clock::now();
    attempt->slotKey = key;
    attempt->expectedBytes = networkBytes;
    attempt->remainingBatches =
        (pieces.size() + kMaxChunksPerRequest - 1) / kMaxChunksPerRequest;
    attempt->fileKey = fileKey_;
    attempt->url = url_;
    attempt->managerId = instanceId_;
    attempt->role = fullAttempts.empty()
                        ? "full-read-1"
                        : (fullAttempts.size() == 1 ? "full-read-2"
                                                    : "full-read-replacement");
    attempt->ownedBuffers.reserve(pieces.size());
    for (const auto &piece : pieces)
      attempt->ownedBuffers.push_back(
          std::make_shared<std::vector<char>>(piece.range.length));
    attemptedSites.insert(key);
    readSources.push_back(source);
    fullAttempts.push_back(attempt);

    std::ostringstream queued;
    queued << "[XrdAdaptor] full read queued handle=" << instanceId_
           << " role=" << attempt->role << " site=" << source->site
           << " server=" << source->server << " bytes=" << networkBytes
           << " async=1 data_buffer=private";
    logLine(queued.str());

    for (std::size_t first = 0; first < pieces.size();
         first += kMaxChunksPerRequest) {
      const auto last = std::min(pieces.size(), first + kMaxChunksPerRequest);
      XrdCl::ChunkList chunks;
      chunks.reserve(last - first);
      std::uint64_t batchBytes = 0;
      for (std::size_t i = first; i < last; ++i) {
        chunks.emplace_back(pieces[i].range.offset, pieces[i].range.length,
                            attempt->ownedBuffers[i]->data());
        batchBytes += pieces[i].range.length;
      }
      auto *handler = new VectorReadCallback(
          batchBytes, [attempt](bool success, std::uint64_t bytes) {
            attempt->completeBatch(success, bytes);
          });
      const auto status = source->file->VectorRead(
          chunks, nullptr, handler,
          static_cast<std::uint16_t>(readTimeoutSeconds_));
      if (!status.IsOK()) {
        delete handler;
        attempt->completeBatch(false, 0);
      }
    }
    return true;
  };

  const auto fullReadCount = [&] {
    return static_cast<std::size_t>(std::count_if(
        fullAttempts.begin(), fullAttempts.end(), [&](const auto &attempt) {
          return !race->finished(attempt->sourceIndex);
        }));
  };
  const auto fullReadAssignedCount = [&] {
    return static_cast<std::size_t>(std::count_if(
        fullAttempts.begin(), fullAttempts.end(), [&](const auto &attempt) {
          return !race->finished(attempt->sourceIndex) ||
                 race->succeeded(attempt->sourceIndex);
        }));
  };

  const auto scheduleExploration = [&](
      const std::vector<std::shared_ptr<Source>> &candidates) {
    if (explorationScheduled || explorationShare == 0 ||
        candidateSiteLimit <= fullReadSiteLimit ||
        fullReadAssignedCount() < fullReadSiteLimit)
      return;
    const auto peer = std::find_if(
        fullAttempts.begin(), fullAttempts.end(), [&](const auto &attempt) {
          return !attempt->source->failed.load() &&
                 (!race->finished(attempt->sourceIndex) ||
                  race->succeeded(attempt->sourceIndex));
        });
    if (peer == fullAttempts.end()) return;
    const auto peerSite = (*peer)->source->site;
    for (const auto &candidate : candidates) {
      if (!candidate || attemptedSites.count(sourceKey(candidate)) ||
          sourceKey(candidate) == sourceKey(fullAttempts.front()->source) ||
          siteMonitor.isBlacklisted(candidate->site) ||
          siteMonitor.isPoorForFile(candidate->site, fileKey_) ||
          siteMonitor.hasEnoughMeasurements(candidate->site))
        continue;
      std::vector<ReadRange> sampleRanges;
      std::uint64_t sampleBytes = 0;
      for (const auto &piece : pieces) {
        if (sampleBytes + piece.range.length > explorationBudget) continue;
        sampleRanges.push_back(piece.range);
        sampleBytes += piece.range.length;
        if (sampleBytes >= explorationBudget) break;
      }
      if (!sampleRanges.empty()) {
        explorationSources.push_back(candidate);
        scheduleExplorationProbe(candidate, peerSite, sampleRanges);
      }
      explorationScheduled = true;
      return;
    }
  };

  // Start both currently-known full sites before waiting for either response.
  // If only one site is known, keep its request asynchronous while discovery
  // runs so a second site can join the race before the primary finishes.
  for (const auto &source : active) {
    if (fullAttempts.size() >= fullReadSiteLimit) break;
    launchFullRead(source);
  }
  scheduleExploration(active);

  const auto discoveryDeadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(30);
  while (race->winner() == ReadRaceState::kNoWinner) {
    active = sources();
    if (active.empty() && std::chrono::steady_clock::now() >= discoveryDeadline)
      active = sources(true);

    // Keep at most the configured number of site-level full requests in flight. A failed attempt
    // frees a slot for the next quality-ranked replica; a slow losing request
    // keeps its slot until its timing callback completes.
    for (const auto &source : active) {
      if (fullReadCount() >= fullReadSiteLimit) break;
      launchFullRead(source);
    }
    scheduleExploration(active);
    if (race->winner() != ReadRaceState::kNoWinner) break;

    if (stopped_) break;
    if (active.empty() && std::chrono::steady_clock::now() >= discoveryDeadline &&
        fullAttempts.empty())
      break;

    if (fullReadCount() == 0 && active.empty() &&
        (probeIntervalSeconds_ == 0 ||
         std::chrono::steady_clock::now() >= discoveryDeadline))
      break;

    startDiscovery();
    race->waitForChange(std::chrono::milliseconds(50));
  }

  // If the first full copy wins just before discovery exposes its alternate,
  // still give that newly available site a full copy for timing data. The
  // already completed winner remains the only data copied back to ROOT.
  if (race->winner() != ReadRaceState::kNoWinner) {
    active = sources();
    for (const auto &source : active) {
      if (fullReadAssignedCount() >= fullReadSiteLimit) break;
      launchFullRead(source);
    }
    scheduleExploration(active);
  }

  auto winningAttempt = race->winner();
  if (winningAttempt == ReadRaceState::kNoWinner) {
    std::vector<std::shared_ptr<Source>> fallbacks = sources();
    if (fallbacks.empty()) fallbacks = sources(true);
    auto fallback = std::find_if(
        fallbacks.begin(), fallbacks.end(), [&](const auto &source) {
          return source && !attemptedSites.count(sourceKey(source)) &&
                 !source->failed.load();
        });
    if (fallback != fallbacks.end()) {
      std::vector<ReadRange> retryRanges;
      retryRanges.reserve(pieces.size());
      for (const auto &piece : pieces) retryRanges.push_back(piece.range);
      logLine("[XrdAdaptor] full read retry handle=" +
              std::to_string(instanceId_) + " site=" + (*fallback)->site +
              " server=" + (*fallback)->server + " bytes=" +
              std::to_string(networkBytes));
      std::uint64_t retryBytes = 0;
      double retrySeconds = 0;
      const auto retryOk = issue(*fallback, retryRanges, retryBytes,
                                 retrySeconds);
      const auto retryHealth = siteMonitor.observe(
          (*fallback)->site, fileKey_, retryOk, retryBytes, retrySeconds);
      logSiteDecision((*fallback)->site, url_, retryHealth);
      std::ostringstream retryResult;
      retryResult << "[XrdAdaptor] full read retry result handle="
                  << instanceId_ << " site=" << (*fallback)->site
                  << " server=" << (*fallback)->server << " status="
                  << (retryOk ? "ok" : "failed") << " bytes=" << retryBytes
                  << " elapsed_s=" << retrySeconds;
      logLine(retryResult.str(), retryOk ? LogLevel::Info : LogLevel::Error);
      if (!retryOk) {
        logReadTiming("retry-failed", readSources);
        return false;
      }
      readSources.push_back(*fallback);
      winningAttempt = ReadRaceState::kNoWinner;
    } else {
      logReadTiming("all-replicas-failed", readSources);
      return false;
    }
  }

  if (winningAttempt != ReadRaceState::kNoWinner) {
    const auto winner = std::find_if(
        fullAttempts.begin(), fullAttempts.end(), [&](const auto &attempt) {
          return attempt->sourceIndex == winningAttempt;
        });
    if (winner == fullAttempts.end() ||
        !race->succeeded(winningAttempt)) {
      logReadTiming("race-winner-unavailable", readSources);
      return false;
    }
    for (std::size_t i = 0; i < pieces.size(); ++i)
      std::memcpy(pieces[i].range.buffer, (*winner)->ownedBuffers[i]->data(),
                  pieces[i].range.length);
  }

  if (prefetch) {
    // ROOT's sequential reader consumes cache requests in their submission
    // order. Mark earlier requests as newer in the bounded LRU so a warmup
    // larger than the handoff budget preserves the first data it will read.
    for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
      savePrefetched(piece->range.offset, piece->owned);
      if (piece->range.length <= cache_->limit)
        handoffStoredBytes += piece->range.length;
    }
  } else if (cache_->limit > 0) {
    // ROOT's next-file warmup can fetch its TTreeCache cluster through normal
    // ReadBuffers calls (with a destination buffer), not ReadBufferAsync.
    // Retain those successful demand reads too, so the active TFile handle can
    // consume the bytes fetched by the warm handle at the file boundary.
    for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
      if (piece->range.length > cache_->limit) continue;
      auto bytes = std::make_shared<std::vector<char>>(piece->range.length);
      std::memcpy(bytes->data(), piece->range.buffer, piece->range.length);
      savePrefetched(piece->range.offset, std::move(bytes));
      handoffStoredBytes += piece->range.length;
    }
  }
  logReadTiming("ok", readSources);
  return true;
}

void configureXrdAdaptor(unsigned probeIntervalSeconds,
                         std::size_t prefetchCacheBytes,
                         unsigned preferredBandPercent,
                         unsigned explorationShare,
                         unsigned maxReadSourceCount,
                         unsigned readTimeoutSeconds) {
  auto &global = settings();
  {
    std::lock_guard<std::mutex> lock(global.mutex);
    global.probeIntervalSeconds = probeIntervalSeconds;
    global.readTimeoutSeconds = std::clamp(readTimeoutSeconds, 1U, 300U);
    global.prefetchCacheBytes = prefetchCacheBytes;
    global.preferredSiteBandPercent = std::min(preferredBandPercent, 100U);
    global.explorationSharePercent = std::min(explorationShare, 50U);
    global.maxReadSources = std::clamp(maxReadSourceCount, 1U, 2U);
  }
  SiteQualityMonitor::instance().configurePreferredBand(
      std::min(preferredBandPercent, 100U));
}

std::shared_ptr<XrdSourceManager> makeXrdSourceManager(const std::string &url) {
  unsigned interval = 5;
  unsigned readTimeout = 120;
  std::size_t cacheBytes = 256ULL * 1024 * 1024;
  {
    auto &global = settings();
    std::lock_guard<std::mutex> lock(global.mutex);
    interval = global.probeIntervalSeconds;
    readTimeout = global.readTimeoutSeconds;
    cacheBytes = global.prefetchCacheBytes;
  }
  auto manager = std::make_shared<XrdSourceManager>(url, interval, cacheBytes,
                                                    readTimeout);
  manager->startDiscovery();
  return manager;
}

} // namespace rdf::xrd
