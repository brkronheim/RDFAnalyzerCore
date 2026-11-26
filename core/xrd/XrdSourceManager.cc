#include "XrdSourceManager.h"
#include "XrdOpenRetry.h"
#include "XrdReadMemory.h"
#include "XrdReadPipeline.h"
#include "XrdSiteRanking.h"
#include "XrdOpenTrace.h"
#include "XrdInitialOpen.h"

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
#include <future>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <unistd.h>

namespace rdf::xrd {
namespace {
constexpr std::uint32_t kReadChunkBytes = 512 * 1024;
constexpr std::size_t kMaxChunksPerRequest = 1000;
constexpr std::size_t kMaxSourcesPerFile = 8;
constexpr std::size_t kMaxPrefetchEntries = 4096;
constexpr std::uint64_t kMaxExplorationBytesPerRead = 2ULL * 1024 * 1024;
// Permit one active-file and one lookahead-file full read to share a site.
constexpr unsigned kMaxFullReadsPerSite = 2;
// Small concurrent windows can consume few payload bytes but still multiply
// transport work across many files. Reserve this limit only for stall rescues.
constexpr unsigned kMaxAdaptiveHedgesPerProcess = 2;
constexpr double kQualityReferenceBytes = 256. * 1024.;
constexpr double kAdaptiveStallMultiplier = 3.;
constexpr unsigned kColdStallMs = 1000;
constexpr unsigned kMinimumStallMs = 250;
constexpr unsigned kMaximumStallMs = 4000;
constexpr auto kPendingReadPoll = std::chrono::milliseconds(50);
constexpr auto kSlotPoll = std::chrono::milliseconds(5);
std::atomic<unsigned> liveAdaptiveRaces{0};
std::atomic<std::uint64_t> nextManagerId{1};
thread_local bool backgroundRead = false;
std::atomic<unsigned> pipelineDepth{2};
std::atomic<unsigned> hedgeDelayMs{50};
std::atomic<bool> readDiagnostics{false};
std::atomic<XrdReadPolicy> readPolicy{XrdReadPolicy::Adaptive};

// The quality estimator uses milliseconds per 256 KiB for large reads and
// response latency for smaller ones. Keep headroom for ordinary jitter and
// start a rescue within half of even the shortest XrdCl request timeout.
std::chrono::milliseconds backupDelay(XrdReadPolicy policy, unsigned floorMs,
    const detail::SiteRank &site, std::uint64_t bytes, unsigned timeoutSeconds) {
  if (policy == XrdReadPolicy::Legacy) return std::chrono::milliseconds(floorMs);
  const double units = std::max(1., static_cast<double>(bytes) / kQualityReferenceBytes);
  const double estimate = (site.enoughMeasurements || site.hasPrior)
      ? kAdaptiveStallMultiplier * site.qualityMs * units : kColdStallMs;
  const double minimum = std::max(kMinimumStallMs, floorMs);
  const double maximum = std::min(kMaximumStallMs, timeoutSeconds * 1000 / 2);
  return std::chrono::milliseconds(static_cast<unsigned>(
      std::clamp(estimate, static_cast<double>(std::min(minimum, maximum)), maximum)));
}

struct ReadTraffic {
  std::atomic<std::uint64_t> primaryOpens{0}, replicaOpens{0};
  std::atomic<std::uint64_t> fullRequests{0}, hedgeRequests{0}, replacements{0};
  std::atomic<std::uint64_t> hedgeWins{0}, replacementWins{0};
  std::atomic<std::uint64_t> outstanding{0}, peakOutstanding{0};
  std::atomic<std::uint64_t> requestedFullBytes{0}, requestedHedgeBytes{0};
  std::atomic<std::uint64_t> foregroundCalls{0}, foregroundMicroseconds{0};
  std::atomic<std::uint64_t> winningBytes{0}, losingBytes{0};
  std::atomic<std::uint64_t> sampleBytes{0}, errorBytes{0}, handoffBytes{0};

  void beginFull(std::uint64_t bytes, bool hedge) {
    requestedFullBytes += bytes;
    if (hedge) requestedHedgeBytes += bytes;
    ++fullRequests;
    const auto active = ++outstanding;
    auto peak = peakOutstanding.load();
    while (peak < active && !peakOutstanding.compare_exchange_weak(peak, active)) {}
  }

  void finishFull(bool success, bool won, std::uint64_t bytes) {
    if (!success) errorBytes += bytes;
    else if (won) winningBytes += bytes;
    else losingBytes += bytes;
    --outstanding;
  }
};

ReadTraffic &readTraffic() {
  static auto *traffic = new ReadTraffic;
  return *traffic;
}

detail::ReadMemoryBudget &readMemoryBudget() {
  // XrdCl callbacks may still own reservations during static teardown.
  static auto *budget = new detail::ReadMemoryBudget;
  return *budget;
}

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

enum class LogLevel { Info, Always, Error };

void logLine(const std::string &line, LogLevel level = LogLevel::Info) {
  if (isRemoteCondorJob() && !readDiagnostics && level == LogLevel::Info) return;
  std::lock_guard<std::mutex> lock(outputMutex());
  std::cout << line << '\n';
  if (level != LogLevel::Info) std::cout.flush();
}

struct GlobalSettings {
  std::mutex mutex;
  unsigned probeIntervalSeconds = 5;
  unsigned readTimeoutSeconds = 120;
  unsigned openTimeoutSeconds = 30;
  std::uint64_t generation = 0;
  std::size_t prefetchCacheBytes = 32ULL * 1024 * 1024;
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
  return readPolicy.load() == XrdReadPolicy::Legacy ? global.explorationSharePercent : 0U;
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

std::mutex &registryMutex() {
  static std::mutex value;
  return value;
}

std::map<std::string, std::weak_ptr<void>> &cacheRegistry() {
  static std::map<std::string, std::weak_ptr<void>> value;
  return value;
}
} // namespace

namespace detail {
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
      completedBytes_ = bytes;
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
  std::uint64_t completedBytes() const { return completedBytes_; }

private:
  const std::uint64_t expected_;
  std::uint64_t completedBytes_ = 0;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  bool done_ = false;
  bool ok_ = false;
};
} // namespace detail

namespace {
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
        // Match the caller's one-second wait; do not retain query handlers
        // and FileSystems for the much longer default request timeout.
        XrdCl::QueryCode::Config, argument, handler.get(), 1);
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
  using QualitySnapshot = detail::SiteRank;

  struct Decision {
    double sampleMs = 0;
    double qualityMs = 260;
    unsigned poorFileCount = 0;
    bool newlyPoorFile = false;
    bool blacklisted = false;
    bool newlyBlacklisted = false;
    std::string reason;
  };

  static SiteQualityMonitor &instance(bool initialize = true) {
    // Keep the monitor alive through process shutdown so the atexit summary
    // cannot race static destruction in a ROOT/XrdCl teardown path.
    static SiteQualityMonitor *monitor = [] {
      auto *value = new SiteQualityMonitor;
      std::atexit([] { SiteQualityMonitor::instance().printSummary(); });
      return value;
    }();
    if (initialize) monitor->ensureRanking();
    return *monitor;
  }

  void configurePreferredBand(unsigned percent) {
    std::lock_guard<std::mutex> lock(mutex_);
    preferredBandPercent_ = std::min(percent, 100U);
    recomputePreferredLocked();
  }

  void ensureRanking() {
    std::call_once(rankingOnce_, [this] { loadRanking({}, {}, 30); });
  }

  void configureRanking(std::string path, std::string origin, unsigned maxAgeDays) {
    bool first = false;
    std::call_once(rankingOnce_, [&] { loadRanking(path, origin, maxAgeDays); first = true; });
    if (!first) loadRanking(path, origin, maxAgeDays);
  }

  void loadRanking(std::string path, std::string origin, unsigned maxAgeDays) {
    if (path == "auto") path.clear();
    if (path.empty()) {
      const auto *value = std::getenv("RDF_XRD_SITE_RANKING_FILE");
      if (value) path = value;
    }
    char hostname[256]{};
    gethostname(hostname, sizeof(hostname) - 1);
    if (origin.empty()) {
      const auto *value = std::getenv("RDF_XRD_SITE_ORIGIN");
      origin = value && *value ? value : hostname;
    }
    detail::SiteRanking ranking;
    if (!path.empty() && path != "none") {
      std::ifstream stream(path);
      if (!stream) throw std::runtime_error("Cannot read XRootD site rankings: " + path);
      ranking = detail::SiteRanking::read(stream);
    }
    const auto resolved = ranking.origin(origin);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ranking_ = std::move(ranking);
      origin_ = resolved;
      maxRankingAgeSeconds_ = static_cast<std::int64_t>(maxAgeDays) * 86400;
    }
    logLine("[XrdAdaptor] context origin=" + resolved + " execution_host=" + hostname +
            " epoch=" + std::to_string(std::time(nullptr)) + " ranking_file=" +
            (path.empty() ? "none" : path), LogLevel::Always);
  }

  std::string origin() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return origin_;
  }

  std::string endpointSite(const std::string &host) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto known = siteByServer_.find(lowercase(host));
    if (known != siteByServer_.end()) return known->second;
    const auto saved = ranking_.siteForEndpoint(host);
    return saved.empty() ? fallbackSiteName(host) : saved;
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
      // This is a lookup optimization, not quality/blacklist history.
      // Re-query evicted hosts rather than retaining every checked server.
      if (siteByServer_.size() >= 256 && !siteByServer_.count(serverKey))
        siteByServer_.erase(siteByServer_.begin());
      const auto result = siteByServer_.emplace(serverKey, std::move(site));
      const auto inserted = sites_.try_emplace(result.first->second).second;
      if (inserted) {
        const auto prior = ranking_.quality(origin_, result.first->second,
                                              std::time(nullptr), maxRankingAgeSeconds_);
        if (prior) logLine("[XrdAdaptor] site-prior origin=" + origin_ +
                          " site=" + result.first->second + " quality_ms=" +
                          std::to_string(*prior) + " weight=1", LogLevel::Always);
      }
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

  bool beginFullRead(const std::string &site, bool background = false) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &inFlight = fullReadsInFlight_[site];
    if (inFlight >= (background ? 1U : kMaxFullReadsPerSite)) return false;
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
    const auto prior = ranking_.quality(origin_, site, std::time(nullptr), maxRankingAgeSeconds_);
    if (found == sites_.end()) return {prior.value_or(260), false, false, prior.has_value()};
    const auto &state = found->second;
    return {detail::initialSiteQuality(prior, state.quality.value(), state.qualitySamples),
            state.preferred, enoughMeasurements(state), prior.has_value()};
  }

  Decision observe(const std::string &site, const std::string &file,
                   bool success, std::uint64_t bytes, double seconds) {
    // Scale large vectors to a 256 KiB equivalent, but keep the actual elapsed
    // response time for smaller reads. Extrapolating a 300-byte metadata read
    // to 256 KiB turns normal network latency into a fictitious multi-minute
    // quality sample.
    const double sampleMs = success && bytes > 0
                                ? std::max(1.0, seconds * 1000.0 *
                                                  std::min(1.0, kQualityReferenceBytes /
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
    const auto &traffic = readTraffic();
    const auto won = traffic.winningBytes.load(), lost = traffic.losingBytes.load();
    const auto failed = traffic.errorBytes.load();
    std::ostringstream metrics;
    metrics << "[XrdAdaptor] read-traffic process=1 primary_opens=" << traffic.primaryOpens
            << " replica_opens=" << traffic.replicaOpens << " full_requests=" << traffic.fullRequests
            << " stall_hedges=" << traffic.hedgeRequests << " hedge_wins=" << traffic.hedgeWins
            << " replacements=" << traffic.replacements << " replacement_wins=" << traffic.replacementWins
            << " full_outstanding=" << traffic.outstanding << " peak_full_outstanding=" << traffic.peakOutstanding
            << " full_requested_bytes=" << traffic.requestedFullBytes
            << " hedge_requested_bytes=" << traffic.requestedHedgeBytes
            << " full_completed_bytes=" << (won + lost + failed) << " winning_bytes=" << won
            << " losing_bytes=" << lost << " failed_response_bytes=" << failed
            << " foreground_read_calls=" << traffic.foregroundCalls
            << " foreground_read_us=" << traffic.foregroundMicroseconds
            << " exploration_bytes=" << traffic.sampleBytes << " handoff_hit_bytes=" << traffic.handoffBytes;
    logLine(metrics.str(), LogLevel::Always);
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
      if (row.qualitySamples) {
        logLine("[XrdAdaptor] site-stat origin=" + origin() + " site=" + row.site +
                " quality_ms=" + std::to_string(row.qualityMs) +
                " samples=" + std::to_string(row.qualitySamples) +
                " read_errors=" + std::to_string(row.readErrors) +
                " successful_bytes=" + std::to_string(row.successfulBytes) +
                " epoch=" + std::to_string(std::time(nullptr)), LogLevel::Always);
      }
    }
  }

private:
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
    // Two distinct paths establish a process-wide blacklist. Further paths
    // cannot affect selection and need not be retained for the process lifetime.
    decision.newlyPoorFile =
        !state.blacklisted && state.poorFiles.insert(file).second;
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
  detail::SiteRanking ranking_;
  std::once_flag rankingOnce_;
  std::string origin_;
  std::int64_t maxRankingAgeSeconds_ = 30 * 86400;
};

detail::OpenTrace openTrace(const std::string &url, std::uint64_t handle,
                            std::uint64_t attempt, const std::string &role) {
  auto &monitor = SiteQualityMonitor::instance();
  return {monitor.origin(), fileIdentity(url), url, role, handle, attempt,
          [](const std::string &line) { logLine(line, LogLevel::Always); },
          [](const std::string &host) { return SiteQualityMonitor::instance().endpointSite(host); }};
}

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
                      XrdCl::AnyObject *response) override {
    delete status;
    delete response;
    delete this;
  }

  void HandleResponseWithHosts(XrdCl::XRootDStatus *status,
                               XrdCl::AnyObject *response,
                               XrdCl::HostList *hosts) override {
    delete status;
    delete response;
    delete hosts;
    delete this;
  }

private:
  std::shared_ptr<XrdCl::File> file_;
};

void closeAsync(std::shared_ptr<XrdCl::File> file) {
  if (file) new DelayedClose(std::move(file));
}
} // namespace

namespace detail {
class ReadRaceState {
public:
  static constexpr std::size_t kNoWinner =
      std::numeric_limits<std::size_t>::max();

  ~ReadRaceState() {
    if (holdsAdaptiveSlot_) --liveAdaptiveRaces;
  }

  // Only the scheduling thread admits new attempts. Every pending callback
  // owns this state until completion, including a slow primary beaten by its
  // backup. Thus the cross-file limit also covers late losing transfers.
  bool admitAdaptiveHedge() {
    if (holdsAdaptiveSlot_) return true;
    auto live = liveAdaptiveRaces.load();
    while (live < kMaxAdaptiveHedgesPerProcess) {
      if (liveAdaptiveRaces.compare_exchange_weak(live, live + 1)) {
        holdsAdaptiveSlot_ = true;
        return true;
      }
    }
    return false;
  }

  bool registerEndpoints(std::size_t attempt, const std::vector<std::string> &endpoints) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (winner_ != kNoWinner) return false;
    endpoints_[attempt] = endpoints;
    return true;
  }

  bool complete(std::size_t attempt, bool success, const std::string &site,
                const std::string &file, const std::string &url,
                double qualityMs) {
    bool won = false;
    std::string poorSite;
    double peerQuality = 0;
    std::vector<std::string> beaten, winnerEndpoints;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      results_[attempt] = success;
      if (success && winner_ == kNoWinner) {
        winner_ = attempt;
        won = true;
        for (const auto &entry : endpoints_) {
          if (entry.first == attempt) winnerEndpoints = entry.second;
          else beaten.insert(beaten.end(), entry.second.begin(), entry.second.end());
        }
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
    if (won) detail::recordEndpointWinner(beaten, winnerEndpoints,
        [](const std::string &line) { logLine(line, LogLevel::Always); });
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

  std::size_t completions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return results_.size();
  }

  void waitForChange(std::chrono::milliseconds duration) {
    waitForChange(duration, completions());
  }

  void waitForChange(std::chrono::milliseconds duration, std::size_t completed) {
    std::unique_lock<std::mutex> lock(mutex_);
    // A callback can win between the caller's winner check and this wait.
    // Preserve that notification instead of sleeping an extra 5--50 ms/window.
    condition_.wait_for(lock, duration, [this, completed] {
      return winner_ != kNoWinner || results_.size() != completed;
    });
  }

private:
  struct QualitySample {
    std::string site;
    double qualityMs = 260;
  };

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::unordered_map<std::size_t, bool> results_;
  std::map<std::size_t, std::vector<std::string>> endpoints_;
  std::vector<QualitySample> qualitySamples_;
  std::size_t winner_ = kNoWinner;
  bool holdsAdaptiveSlot_ = false;
};
} // namespace detail

namespace {
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
  std::string requestedUrl;
  bool cooling() const {
    return detail::endpointCooldown().active(server) || detail::endpointCooldown().active(requestedUrl);
  }
  std::deque<std::string> locatedReplicas;
  std::atomic<bool> failed{false};
};

namespace {
class FullReadSlot {
public:
  FullReadSlot(const std::string &site, bool background) : site_(site) {
    if (!SiteQualityMonitor::instance().beginFullRead(site_, background)) site_.clear();
  }
  FullReadSlot(const FullReadSlot &) = delete;
  FullReadSlot &operator=(const FullReadSlot &) = delete;
  FullReadSlot(FullReadSlot &&other) noexcept { site_.swap(other.site_); }
  ~FullReadSlot() { if (!site_.empty()) SiteQualityMonitor::instance().endFullRead(site_); }
  explicit operator bool() const { return !site_.empty(); }
private:
  std::string site_;
};
} // namespace

struct XrdSourceManager::PendingRead {
  std::shared_ptr<Source> source;
  std::vector<std::unique_ptr<detail::VectorReadWaiter>> waiters;
  std::unique_ptr<detail::ReadMemoryBudget::Reservation> memory;
  std::vector<std::shared_ptr<std::vector<char>>> ownedBuffers;
  std::shared_ptr<detail::ReadRaceState> race;
  std::chrono::steady_clock::time_point started;
  std::size_t sourceIndex = 0;
  bool hedgeAttempt = false;
  bool allQueued = true;
  std::optional<FullReadSlot> slot;
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
    auto& traffic = readTraffic();
    if (won && hedgeAttempt) ++traffic.hedgeWins;
    else if (won && sourceIndex > 0) ++traffic.replacementWins;
    traffic.finishFull(finalSuccess, won, totalBytes);
    if (!won) {
      // Every batch has completed, so XrdCl no longer writes these buffers.
      // Keep the small timing/result record, not a full failed/losing copy.
      ownedBuffers.clear();
      memory.reset();
    }
    std::ostringstream result;
    result << std::fixed << std::setprecision(3)
           << "[XrdAdaptor] full read result handle=" << managerId
           << " window=" << windowId << " role=" << role << " site=" << source->site << " server="
           << (source->server.empty() ? "<unknown>" : source->server)
           << " status=" << (finalSuccess ? "ok" : "failed")
           << " winner=" << (won ? "yes" : "no")
           << " bytes=" << totalBytes << " elapsed_s=" << elapsedSeconds
           << " quality_ms=" << health.qualityMs << " data_used="
           << (won ? "yes" : "no");
    logLine(result.str(), finalSuccess ? LogLevel::Info : LogLevel::Error);
    slot.reset();
  }

  std::string fileKey;
  std::string url;
  std::string role;
  std::uint64_t managerId = 0;
  std::uint64_t windowId = 0;
};

class XrdSourceManager::ExplorationReadHandler final
    : public XrdCl::ResponseHandler {
public:
  ExplorationReadHandler(std::shared_ptr<Source> source, std::string file,
                         std::string url, std::string peerSite,
                         std::uint64_t managerId, std::uint64_t expectedBytes,
                         std::chrono::steady_clock::time_point started,
                         std::vector<std::shared_ptr<std::vector<char>>> buffers,
                         std::unique_ptr<detail::ReadMemoryBudget::Reservation> memory)
      : source_(std::move(source)), file_(std::move(file)), url_(std::move(url)),
        peerSite_(std::move(peerSite)), managerId_(managerId),
        expectedBytes_(expectedBytes), started_(started),
        memory_(std::move(memory)), buffers_(std::move(buffers)) {}

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

    readTraffic().sampleBytes += bytes;
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
  std::unique_ptr<detail::ReadMemoryBudget::Reservation> memory_;
  std::vector<std::shared_ptr<std::vector<char>>> buffers_;
};

class XrdSourceManager::ProbeHandler final : public XrdCl::ResponseHandler {
public:
  ProbeHandler(std::weak_ptr<XrdSourceManager> manager,
               std::shared_ptr<XrdCl::File> file, detail::OpenTrace trace,
               detail::InitialOperation operation)
      : manager_(std::move(manager)), file_(std::move(file)), trace_(std::move(trace)),
        operation_(std::move(operation)) {}

  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject *response) override {
    finish(status, response, nullptr);
  }

  void HandleResponseWithHosts(XrdCl::XRootDStatus *status,
                               XrdCl::AnyObject *response,
                               XrdCl::HostList *hosts) override {
    std::unique_ptr<XrdCl::HostList> ownedHosts(hosts);
    finish(status, response, hosts);
  }

private:
  void finish(XrdCl::XRootDStatus *status, XrdCl::AnyObject *response,
              XrdCl::HostList *hosts) {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    if (status) trace_.result(*file_, *status, hosts);
    bool adopted = false;
    if (auto manager = manager_.lock())
      adopted = manager->completeProbe(file_, ownedStatus.get());
    if (!adopted) closeAsync(std::move(file_));
    delete this;
  }

  std::weak_ptr<XrdSourceManager> manager_;
  std::shared_ptr<XrdCl::File> file_;
  detail::OpenTrace trace_;
  detail::InitialOperation operation_;
};

struct XrdSourceManager::PrefetchCache {
  struct CacheEntry {
    std::shared_ptr<std::vector<char>> data;
    std::uint64_t use = 0;
  };

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
      it = entries.upper_bound(cursor);
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

  std::uint64_t reserveUses(std::size_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    clock += count;
    return clock;
  }

  void insert(std::uint64_t offset, std::shared_ptr<std::vector<char>> data,
              std::uint64_t use = 0) {
    if (!data || data->empty() || data->size() > limit) return;
    std::lock_guard<std::mutex> lock(mutex);
    if (!use) use = ++clock;
    auto found = entries.find(offset);
    if (found != entries.end()) {
      if (found->second.use > use) return;
      usedBytes -= found->second.data->size();
      entries.erase(found);
    }
    usedBytes += data->size();
    entries.emplace(offset, CacheEntry{std::move(data), use});
    // Tiny metadata reads must not turn a payload-byte cap into millions of
    // map nodes and separately allocated vector/control blocks.
    while ((usedBytes > limit ||
            entries.size() > kMaxPrefetchEntries) && !entries.empty()) {
      auto oldest = std::min_element(entries.begin(), entries.end(), [](const auto &a, const auto &b) {
        return a.second.use < b.second.use;
      });
      usedBytes -= oldest->second.data->size();
      entries.erase(oldest);
    }
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

BackgroundReadScope::BackgroundReadScope(bool background) : previous_(backgroundRead) { backgroundRead = previous_ || background; }
bool isBackgroundRead() { return backgroundRead; }
BackgroundReadScope::~BackgroundReadScope() { backgroundRead = previous_; }
void configureXrdReadScheduling(unsigned depth, unsigned delay, bool diagnostics) {
  configureXrdReadScheduling(depth, delay, diagnostics, XrdReadPolicy::Legacy);
}
void configureXrdReadScheduling(unsigned depth, unsigned delay, bool diagnostics, XrdReadPolicy policy) {
  readPolicy = policy;
  pipelineDepth = std::clamp(depth, 1U, 4U);
  hedgeDelayMs = std::min(delay, 1000U);
  readDiagnostics = diagnostics;
}

XrdSourceManager::XrdSourceManager(std::string url, unsigned probeIntervalSeconds,
                                   std::size_t prefetchCacheBytes,
                                   unsigned readTimeoutSeconds)
    : XrdSourceManager(std::move(url), probeIntervalSeconds, prefetchCacheBytes,
                       readTimeoutSeconds, 30) {}

XrdSourceManager::XrdSourceManager(std::string url,
                                   unsigned probeIntervalSeconds,
                                   std::size_t prefetchCacheBytes,
                                   unsigned readTimeoutSeconds,
                                   unsigned openTimeoutSeconds)
    : url_(std::move(url)), fileKey_(fileIdentity(url_)),
      probeIntervalSeconds_(probeIntervalSeconds),
      readTimeoutSeconds_(std::clamp(readTimeoutSeconds, 1U, 300U)),
      openTimeoutSeconds_(std::clamp(openTimeoutSeconds, 1U, 300U)),
      nextProbeAt_(std::chrono::steady_clock::now()),
      cache_(acquireCache(url_, prefetchCacheBytes)) {
  SiteQualityMonitor::instance().configurePreferredBand(
      preferredSiteBandPercent());
  instanceId_ = nextManagerId.fetch_add(1);
  auto primary = std::make_shared<Source>();
  const auto timeout = static_cast<std::uint16_t>(openTimeoutSeconds_);
  std::string lastError;
  // Library-only controls also work in already prepared worker executables.
  const auto envSeconds = [](const char *name, unsigned fallback, unsigned cap) {
    const char *value = std::getenv(name);
    if (!value || !*value) return fallback;
    char *end = nullptr;
    const auto parsed = std::strtoul(value, &end, 10);
    return end && !*end && *value != '-' ? static_cast<unsigned>(std::min<unsigned long>(parsed, cap)) : fallback;
  };
  const unsigned locateSeconds = envSeconds("RDF_XRD_LOCATE_TIMEOUT_SECONDS", 3, 10);
  const unsigned candidateSeconds = std::max(1U, std::min(openTimeoutSeconds_,
      envSeconds("RDF_XRD_INITIAL_OPEN_TIMEOUT_SECONDS", 8, 30)));
  const XrdCl::URL initialUrl(url_);
  if (locateSeconds && (initialUrl.GetProtocol() == "root" || initialUrl.GetProtocol() == "roots")) {
    auto &monitor = SiteQualityMonitor::instance();
    const auto started = std::chrono::steady_clock::now();
    logLine("[XrdAdaptor] locate-start origin=" + monitor.origin() + " handle=" +
        std::to_string(instanceId_) + " file=" + fileKey_ + " deadline_s=" +
        std::to_string(locateSeconds) + " mode=nowait", LogLevel::Always);
    const auto endpoints = detail::locateInitial(url_, locateSeconds,
        [&](const std::vector<std::string> &urls) {
          std::set<std::string> good;
          for (const auto &candidate : urls) {
            const auto site = monitor.endpointSite(XrdCl::URL(candidate).GetHostName());
            const auto rank = monitor.snapshot(site);
            if (!monitor.isBlacklisted(site) && (rank.hasPrior || rank.enoughMeasurements) && rank.qualityMs < 260)
              good.insert(site);
          }
          return good.size() >= 2;
        }, [&](unsigned count, unsigned failures, bool partial) {
          logLine("[XrdAdaptor] locate-result origin=" + monitor.origin() + " handle=" +
              std::to_string(instanceId_) + " file=" + fileKey_ + " endpoints=" + std::to_string(count) +
              " failed_queries=" + std::to_string(failures) + " partial=" + (partial ? "yes" : "no") +
              " elapsed_s=" + std::to_string(std::chrono::duration<double>(std::chrono::steady_clock::now()-started).count()), LogLevel::Always);
        }, [](const std::string &line) { logLine(line, LogLevel::Always); });
    std::vector<detail::InitialCandidate> candidates;
    for (const auto &candidate : endpoints) {
      const auto site = monitor.endpointSite(XrdCl::URL(candidate).GetHostName());
      if (!monitor.isBlacklisted(site) && !monitor.isPoorForFile(site, fileKey_))
        candidates.push_back({candidate, site, monitor.snapshot(site)});
    }
    candidates = detail::rankInitial(std::move(candidates));
    auto result = detail::openInitialCandidates(candidates, candidateSeconds,
        [&](const std::string &candidate, unsigned attempt) {
          return openTrace(candidate, instanceId_, attempt, "ranked-primary");
        });
    if (result.file) {
      primary->file = std::move(result.file);
      primary->requestedUrl = result.candidate;
      fileSize_ = result.size;
    }
    // Keep the catalogue on the private transport, preserving the public ABI.
    // Subsequent replica discovery tries these ranked sites before global routing.
    for (const auto &candidate : candidates)
      if (candidate.url != result.candidate) primary->locatedReplicas.push_back(candidate.url);
    logLine("[XrdAdaptor] initial-selection handle=" + std::to_string(instanceId_) +
        " file=" + fileKey_ + " mode=" + (primary->file ? "ranked" : "global-fallback") +
        " candidates=" + std::to_string(candidates.size()), LogLevel::Always);
  }
  const bool opened = primary->file || detail::openWithRetry(
      url_,
      [&](const std::string &candidateUrl, std::size_t attempt) {
        // A failed XrdCl handle must not be reused on the next attempt.
        auto file = std::make_shared<XrdCl::File>();
        const auto status = detail::tracedOpen(*file,
            openTrace(candidateUrl, instanceId_, attempt, "primary"), timeout);
        if (!status.IsOK()) {
          lastError = "XrdCl could not open '" + fileIdentity(candidateUrl) + "': " + status.ToStr();
        } else {
          XrdCl::StatInfo *statInfo = nullptr;
          std::string resolvedUrl;
          file->GetProperty("LastURL", resolvedUrl);
          auto statTrace = openTrace(resolvedUrl.empty() ? candidateUrl : resolvedUrl,
                                      instanceId_, attempt, "primary");
          statTrace.file = fileIdentity(candidateUrl);
          statTrace.operation = "stat";
          statTrace.begin();
          const auto statStatus = file->Stat(false, statInfo, timeout);
          statTrace.result(*file, statStatus, nullptr);
          std::unique_ptr<XrdCl::StatInfo> ownedStat(statInfo);
          if (statStatus.IsOK() && ownedStat) {
            fileSize_ = ownedStat->GetSize();
            primary->file = std::move(file);
            // Continue replica discovery through the endpoint that worked.
            url_ = candidateUrl;
            return true;
          }
          lastError = "XrdCl could not stat '" + fileIdentity(candidateUrl) + "': " + statStatus.ToStr();
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
  ++readTraffic().primaryOpens;
  sources_.push_back(std::move(primary));
  const auto &openedPrimary = sources_.front();
  logLine("[XrdAdaptor] source handle=" + std::to_string(instanceId_) +
          " role=primary file=" + fileKey_ + " site=" + openedPrimary->site +
          " server=" +
          detail::OpenTrace::endpoint(XrdCl::URL("root://" + openedPrimary->server + "//")), LogLevel::Always);
}

XrdSourceManager::~XrdSourceManager() { stop(); }

void XrdSourceManager::startDiscovery() {
  if (probeIntervalSeconds_ == 0) return;
  const auto explorationShare = explorationSharePercent();
  const auto desiredSites = std::min(
      3U, maxReadSources() + (explorationShare > 0 ? 1U : 0U));
  std::vector<std::string> tried;
  std::string locatedCandidate;
  std::optional<detail::InitialOperation> operation;
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
    // Standby opens share the metadata budget with initial lookup/open.
    // Leave capacity for demand when several files open at the same time.
    operation.emplace(healthySites.empty() ? detail::kMaxMetadataOperations :
                                            detail::kStandbyMetadataLimit);
    if (!operation->acquired) {
      nextProbeAt_ = std::chrono::steady_clock::now() + kPendingReadPoll;
      return;
    }
    probeInFlight_ = true;
    tried = triedHosts_;
    bool healthyAvailable = !healthySites.empty();
    for (const auto &source : sources_) {
      auto &catalogue = source->locatedReplicas;
      for (auto i=catalogue.begin(); i!=catalogue.end();) {
        const auto host = hostnameOnly(XrdCl::URL(*i).GetHostId());
        if (std::find(tried.begin(), tried.end(), host) != tried.end()) { i=catalogue.erase(i); continue; }
        if (!detail::endpointCooldown().active(*i)) {
          locatedCandidate = *i; catalogue.erase(i); break;
        }
        ++i;
      }
      if (!locatedCandidate.empty()) break;
    }
    if (locatedCandidate.empty() && !healthyAvailable) {
      for (const auto &source : sources_) if (!source->locatedReplicas.empty()) {
        locatedCandidate = source->locatedReplicas.front(); source->locatedReplicas.pop_front(); break;
      }
    }
    // Suppress repeated global redirects to cooled hosts only while a usable
    // source exists. With no usable alternative retain ordinary recovery.
    if (healthyAvailable) {
      for (const auto &host : detail::endpointCooldown().hosts())
        if (std::find(tried.begin(),tried.end(),host)==tried.end()) tried.push_back(host);
    }
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
  const auto candidateUrl = locatedCandidate.empty() ? appendOpaque(url_, opaque) : locatedCandidate;
  auto trace = openTrace(candidateUrl, instanceId_, ++probeSequence_, "replica");
  trace.begin();
  auto *handler = new ProbeHandler(weak_from_this(), file, trace, std::move(*operation));
  const auto status = file->Open(candidateUrl, XrdCl::OpenFlags::Read,
                                 XrdCl::Access::None, handler,
                                 static_cast<std::uint16_t>(openTimeoutSeconds_));
  if (!status.IsOK()) {
    trace.result(*file, status, nullptr);
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
  // Closed ROOT file objects may remain alive. Release their cache ownership
  // now; an active peer or in-progress window keeps its own shared reference.
  std::atomic_store(&cache_, std::shared_ptr<PrefetchCache>{});
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
                       return detail::siteRankBefore(a.quality, b.quality);
                     });

    ranked = detail::cooldownChoices(ranked, [](const RankedSource &candidate) { return candidate.source->cooling(); });

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

  if (status && status->IsOK()) ++readTraffic().replicaOpens;
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
            << " file=" << fileKey_;
  if (!status || !status->IsOK())
    resultLog << " status=failed detail="
              << (status ? status->ToStr() : "<no status>");
  else if (server.empty())
    resultLog << " status=opened server=<unknown> action=discarded";
  else
    resultLog << " status=opened server="
              << detail::OpenTrace::endpoint(XrdCl::URL("root://" + server + "//")) << " site=" << site
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
          (!status || !status->IsOK()) ? LogLevel::Error : LogLevel::Always);
  condition_.notify_all();
  return added;
}

bool XrdSourceManager::copyPrefetched(std::uint64_t offset,
                                      std::uint32_t length,
                                      char *destination) {
  const auto cache = std::atomic_load(&cache_);
  return cache && cache->copy(offset, length, destination);
}

void XrdSourceManager::savePrefetched(
    std::uint64_t offset, std::shared_ptr<std::vector<char>> bytes, std::uint64_t use) {
  if (const auto cache = std::atomic_load(&cache_))
    cache->insert(offset, std::move(bytes), use);
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
    auto waiter = std::make_unique<detail::VectorReadWaiter>(expected);
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
  }
  return pending;
}

bool XrdSourceManager::finish(PendingRead &pending, std::uint64_t &bytes,
                              double &seconds) {
  bool success = pending.allQueued;
  bytes = 0;
  for (auto &waiter : pending.waiters) {
    waiter->wait();
    success = success && waiter->ok();
    bytes += waiter->completedBytes();
  }
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

  std::size_t sampleBytes = 0;
  for (const auto &range : ranges) sampleBytes += range.length;
  auto memory = readMemoryBudget().tryReserve(sampleBytes, true);
  if (!memory) {
    monitor.endExplorationProbe(source->site);
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
      std::move(buffers), std::move(memory));
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

bool XrdSourceManager::readv(const std::vector<ReadRange> &ranges, bool prefetch) {
  if (stopped_) return false;
  // Bound private copies even for very large ROOT cache/cluster requests.
  // Visit windows backwards so the first ranges remain newest in the handoff
  // LRU, including warmup reads arriving with a destination buffer.
  for (const auto &range : ranges) {
    if (range.length && ((!prefetch && !range.buffer) ||
        range.offset > std::numeric_limits<std::uint64_t>::max() - range.length))
      return false;
  }
  std::vector<std::vector<ReadRange>> windows;
  std::vector<ReadRange> window;
  std::size_t bytes = 0;
  const auto flush = [&] {
    if (stopped_) return false;
    if (window.empty()) return true;
    std::reverse(window.begin(), window.end());
    windows.push_back(std::move(window));
    window = {};
    bytes = 0;
    return true;
  };
  for (auto range = ranges.rbegin(); range != ranges.rend(); ++range) {
    std::uint32_t remaining = range->length;
    while (remaining) {
      const auto count = static_cast<std::uint32_t>(std::min<std::size_t>(
          remaining, detail::ReadMemoryBudget::kWindow - bytes));
      remaining -= count;
      window.push_back({range->offset + remaining, count,
                        prefetch ? nullptr : range->buffer + remaining});
      bytes += count;
      if ((bytes == detail::ReadMemoryBudget::kWindow ||
           window.size() == kMaxChunksPerRequest) && !flush())
        return false;
    }
  }
  if (!flush()) return false;
  const auto cache = std::atomic_load(&cache_);
  if (!cache) return false;
  // Reserve LRU ordering before launching: completion order must not evict
  // the earliest ranges, which ROOT consumes first, from a bounded handoff.
  constexpr auto stride = kMaxChunksPerRequest + detail::ReadMemoryBudget::kWindow / kReadChunkBytes + 1;
  const auto base = cache->reserveUses(windows.size() * stride) - windows.size() * stride;
  const bool background = backgroundRead;
  // One timing per synchronous logical call, spanning all pipelined windows.
  // Parallel callers overlap; this sum cannot be subtracted from worker wall.
  const bool foreground = !prefetch && !background;
  const auto started = foreground ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
  const bool success = detail::runReadWindows(windows, pipelineDepth.load(),
      [&](const auto &part, std::size_t i) {
        return readvWindow(part, prefetch, background,
                           base + i * stride + 1);
      });
  if (foreground) {
    auto &traffic = readTraffic();
    ++traffic.foregroundCalls;
    traffic.foregroundMicroseconds += std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - started).count();
  }
  return success;
}

bool XrdSourceManager::readvWindow(const std::vector<ReadRange> &ranges,
                                   bool prefetch, bool background, std::uint64_t cacheUse) {
  const auto cache = std::atomic_load(&cache_);
  if (!cache || stopped_) return false;
  const auto readStarted = std::chrono::steady_clock::now();
  const auto requestSequence = readvSequence_.fetch_add(1) + 1;
  const auto demandSequence =
      prefetch ? 0 : demandReadSequence_.fetch_add(1) + 1;
  struct Piece {
    ReadRange range;
    std::shared_ptr<std::vector<char>> owned;
  };

  std::unique_ptr<detail::ReadMemoryBudget::Reservation> retryMemory;
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
        // The winning attempt supplies storage directly to the handoff cache.
        piece.range.buffer = nullptr;
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

  readTraffic().handoffBytes += handoffBytes;
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
  const auto policy = readPolicy.load();
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
  const auto race = std::make_shared<detail::ReadRaceState>();
  std::vector<std::shared_ptr<PendingRead>> fullAttempts;
  std::unordered_set<std::string> attemptedSites;
  std::vector<std::shared_ptr<Source>> readSources;
  std::vector<std::shared_ptr<Source>> explorationSources;
  bool explorationScheduled = false;

  std::uint64_t slotDeferrals = 0, budgetDeferrals = 0, hedgeDeferrals = 0;
  auto nextBackupAt = std::chrono::steady_clock::time_point::max();
  const auto launchFullRead = [&](const std::shared_ptr<Source> &source, bool hedge) {
    if (race->winner() != detail::ReadRaceState::kNoWinner) return false;
    if (!source || source->failed.load()) return false;
    // A different pipeline window can cool an endpoint after sources() took
    // its snapshot. Recheck immediately before issuing a speculative copy.
    if (source->cooling() && std::any_of(active.begin(), active.end(), [](const auto &other) {
          return other && !other->failed.load() && !other->cooling();
        })) return false;
    const auto key = sourceKey(source);
    if (key.empty() || attemptedSites.count(key)) return false;
    // Hard-error replacement serves demand and must be able to use its reserved slot.
    const bool speculative = background ||
        (policy == XrdReadPolicy::Legacy ? !fullAttempts.empty() : hedge);
    FullReadSlot slot(key, speculative);
    if (!slot) {
      ++slotDeferrals;
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

    auto memory = readMemoryBudget().tryReserve(networkBytes, speculative);
    if (!memory) {
      ++budgetDeferrals;
      return false;
    }
    if (hedge && policy == XrdReadPolicy::Adaptive && !race->admitAdaptiveHedge()) {
      ++hedgeDeferrals;
      return false;
    }
    auto attempt = std::make_shared<PendingRead>();
    attempt->memory = std::move(memory);
    attempt->source = source;
    attempt->race = race;
    attempt->sourceIndex = fullAttempts.size();
    attempt->hedgeAttempt = hedge;
    attempt->started = std::chrono::steady_clock::now();
    attempt->slot.emplace(std::move(slot));
    attempt->expectedBytes = networkBytes;
    attempt->remainingBatches =
        (pieces.size() + kMaxChunksPerRequest - 1) / kMaxChunksPerRequest;
    attempt->fileKey = fileKey_;
    attempt->url = url_;
    attempt->managerId = instanceId_;
    attempt->windowId = requestSequence;
    attempt->role = fullAttempts.empty()
                        ? "full-read-1"
                        : (fullAttempts.size() == 1 ? "full-read-2"
                                                    : "full-read-replacement");
    if (!race->registerEndpoints(attempt->sourceIndex, {source->server, source->requestedUrl})) {
      return false;
    }
    attempt->ownedBuffers.reserve(pieces.size());
    for (const auto &piece : pieces)
      attempt->ownedBuffers.push_back(
          std::make_shared<std::vector<char>>(piece.range.length));
    attemptedSites.insert(key);
    readSources.push_back(source);
    auto &traffic = readTraffic();
    if (hedge) ++traffic.hedgeRequests;
    else if (!fullAttempts.empty()) ++traffic.replacements;
    traffic.beginFull(networkBytes, hedge);
    const auto delay = backupDelay(policy, hedgeDelayMs.load(),
        policy == XrdReadPolicy::Legacy ? detail::SiteRank{} : siteMonitor.snapshot(source->site),
        networkBytes, readTimeoutSeconds_);
    nextBackupAt = attempt->started + delay;
    fullAttempts.push_back(attempt);

    std::ostringstream queued;
    queued << "[XrdAdaptor] full read queued handle=" << instanceId_
           << " window=" << requestSequence << " role=" << attempt->role << " site=" << source->site
           << " server=" << source->server << " bytes=" << networkBytes
           << " policy=" << (policy == XrdReadPolicy::Adaptive ? "adaptive" : "legacy")
           << " hedge=" << hedge << " backup_delay_ms=" << delay.count()
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
        if (sampleBytes >= explorationBudget ||
            sampleRanges.size() >= kMaxChunksPerRequest) break;
      }
      if (!sampleRanges.empty()) {
        explorationSources.push_back(candidate);
        scheduleExplorationProbe(candidate, peerSite, sampleRanges);
      }
      explorationScheduled = true;
      return;
    }
  };

  // Start the best site first; a backup joins after the hedge delay (zero
  // retains immediate racing), or immediately following a read failure.
  // If only one site is known, keep its request asynchronous while discovery
  // runs so a second site can join the race before the primary finishes.
  for (const auto &source : active) {
    const auto pending = fullReadCount();
    if (pending >= fullReadSiteLimit ||
        (pending && std::chrono::steady_clock::now() < nextBackupAt)) break;
    launchFullRead(source, pending > 0);
  }
  scheduleExploration(active);

  const auto discoveryDeadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(30);
  while (race->winner() == detail::ReadRaceState::kNoWinner) {
    const auto observedCompletions = race->completions();
    active = sources();
    if (active.empty() && std::chrono::steady_clock::now() >= discoveryDeadline)
      active = sources(true);

    // Keep at most the configured number of site-level full requests in flight. A failed attempt
    // frees a slot for the next quality-ranked replica; a slow losing request
    // keeps its slot until its timing callback completes.
    for (const auto &source : active) {
      const auto pending = fullReadCount();
      if (pending >= fullReadSiteLimit ||
          (pending && std::chrono::steady_clock::now() < nextBackupAt)) break;
      launchFullRead(source, pending > 0);
    }
    scheduleExploration(active);
    if (race->winner() != detail::ReadRaceState::kNoWinner) break;

    if (stopped_) return false;
    if (active.empty() && std::chrono::steady_clock::now() >= discoveryDeadline &&
        fullAttempts.empty())
      break;

    if (fullReadCount() == 0 &&
        (probeIntervalSeconds_ == 0 || std::chrono::steady_clock::now() >= discoveryDeadline) &&
        std::none_of(active.begin(), active.end(), [&](const auto &source) {
          return !attemptedSites.count(sourceKey(source));
        })) break;

    startDiscovery();
    // A different reader releasing a site/budget slot cannot signal this
    // race. Avoid adding a 50 ms stall to every small window in that case.
    const auto wait = fullReadCount() == 0 && !active.empty() ? kSlotPoll : kPendingReadPoll;
    race->waitForChange(wait, observedCompletions);
  }

  // Once the read has won, do not launch another full copy solely for timing.
  // Keep bounded exploration samples; already in-flight losing reads drain
  // normally and continue to supply quality measurements.
  if (race->winner() != detail::ReadRaceState::kNoWinner) {
    active = sources();
    scheduleExploration(active);
  }

  auto winningAttempt = race->winner();
  if (winningAttempt == detail::ReadRaceState::kNoWinner) {
    std::vector<std::shared_ptr<Source>> fallbacks = sources();
    if (fallbacks.empty()) fallbacks = sources(true);
    auto fallback = std::find_if(
        fallbacks.begin(), fallbacks.end(), [&](const auto &source) {
          return source && !attemptedSites.count(sourceKey(source)) &&
                 !source->failed.load();
        });
    if (fallback != fallbacks.end()) {
      // A last-resort endpoint can appear just as discovery times out. Give
      // its synchronous replacement the same site and payload reservations
      // as an ordinary attempt, including lower priority for future files.
      const auto site = sourceKey(*fallback);
      while (!siteMonitor.beginFullRead(site, background)) {
        if (stopped_) return false;
        race->waitForChange(kSlotPoll);
      }
      struct SiteRelease {
        SiteQualityMonitor &monitor;
        const std::string &site;
        ~SiteRelease() { monitor.endFullRead(site); }
      } releaseSite{siteMonitor, site};
      while (!(retryMemory = readMemoryBudget().tryReserve(networkBytes, background))) {
        if (stopped_) return false;
        race->waitForChange(kSlotPoll);
      }
      std::vector<ReadRange> retryRanges;
      retryRanges.reserve(pieces.size());
      for (auto &piece : pieces) {
        piece.owned = std::make_shared<std::vector<char>>(piece.range.length);
        retryRanges.push_back({piece.range.offset, piece.range.length, piece.owned->data()});
      }
      logLine("[XrdAdaptor] full read retry handle=" +
              std::to_string(instanceId_) + " site=" + (*fallback)->site +
              " server=" + (*fallback)->server + " bytes=" +
              std::to_string(networkBytes));
      std::uint64_t retryBytes = 0;
      double retrySeconds = 0;
      auto &traffic = readTraffic();
      ++traffic.replacements;
      traffic.beginFull(networkBytes, false);
      const auto retryOk = issue(*fallback, retryRanges, retryBytes, retrySeconds);
      if (retryOk) ++traffic.replacementWins;
      traffic.finishFull(retryOk, retryOk, retryBytes);
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
      if (!prefetch) {
        for (const auto &piece : pieces)
          std::memcpy(piece.range.buffer, piece.owned->data(), piece.range.length);
      }
      readSources.push_back(*fallback);
    } else {
      logReadTiming("all-replicas-failed", readSources);
      return false;
    }
  }

  if (winningAttempt != detail::ReadRaceState::kNoWinner) {
    const auto winner = std::find_if(
        fullAttempts.begin(), fullAttempts.end(), [&](const auto &attempt) {
          return attempt->sourceIndex == winningAttempt;
        });
    if (winner == fullAttempts.end() ||
        !race->succeeded(winningAttempt)) {
      logReadTiming("race-winner-unavailable", readSources);
      return false;
    }
    for (std::size_t i = 0; i < pieces.size(); ++i) {
      pieces[i].owned = (*winner)->ownedBuffers[i];
      if (!prefetch)
        std::memcpy(pieces[i].range.buffer, pieces[i].owned->data(),
                    pieces[i].range.length);
    }
  }

  if (prefetch) {
    // ROOT's sequential reader consumes cache requests in their submission
    // order. Mark earlier requests as newer in the bounded LRU so a warmup
    // larger than the handoff budget preserves the first data it will read.
    for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
      savePrefetched(piece->range.offset, piece->owned,
                     cacheUse + static_cast<std::uint64_t>(std::distance(pieces.rbegin(), piece)));
      if (piece->range.length <= cache->limit)
        handoffStoredBytes += piece->range.length;
    }
  } else if (cache->limit > 0) {
    // ROOT's next-file warmup can fetch its TTreeCache cluster through normal
    // ReadBuffers calls (with a destination buffer), not ReadBufferAsync.
    // Retain those successful demand reads too, so the active TFile handle can
    // consume the bytes fetched by the warm handle at the file boundary.
    for (auto piece = pieces.rbegin(); piece != pieces.rend(); ++piece) {
      if (piece->range.length > cache->limit) continue;
      // The winner is immutable once all its callbacks have completed.
      // Share that allocation instead of copying ROOT's destination again.
      savePrefetched(piece->range.offset, piece->owned,
                     cacheUse + static_cast<std::uint64_t>(std::distance(pieces.rbegin(), piece)));
      handoffStoredBytes += piece->range.length;
    }
  }
  if (slotDeferrals || budgetDeferrals || hedgeDeferrals) {
    std::ostringstream contention;
    contention << "[XrdAdaptor] read scheduling handle=" << instanceId_
               << " background=" << background << " slot_deferrals=" << slotDeferrals
               << " budget_deferrals=" << budgetDeferrals
               << " hedge_limit_deferrals=" << hedgeDeferrals;
    logLine(contention.str());
  }
  logReadTiming("ok", readSources);
  return true;
}

void configureXrdSiteRanking(const std::string &file, const std::string &origin,
                             unsigned maxAgeDays) {
  SiteQualityMonitor::instance(false).configureRanking(file, origin, maxAgeDays);
}

void configureXrdAdaptor(unsigned probeIntervalSeconds,
                         std::size_t prefetchCacheBytes,
                         unsigned preferredBandPercent,
                         unsigned explorationShare,
                         unsigned maxReadSourceCount,
                         unsigned readTimeoutSeconds) {
  configureXrdAdaptor(probeIntervalSeconds, prefetchCacheBytes, preferredBandPercent,
                      explorationShare, maxReadSourceCount, readTimeoutSeconds, 30);
}

void configureXrdAdaptor(unsigned probeIntervalSeconds,
                         std::size_t prefetchCacheBytes,
                         unsigned preferredBandPercent,
                         unsigned explorationShare,
                         unsigned maxReadSourceCount,
                         unsigned readTimeoutSeconds,
                         unsigned openTimeoutSeconds) {
  auto &global = settings();
  {
    std::lock_guard<std::mutex> lock(global.mutex);
    global.probeIntervalSeconds = probeIntervalSeconds;
    global.readTimeoutSeconds = std::clamp(readTimeoutSeconds, 1U, 300U);
    global.openTimeoutSeconds = std::clamp(openTimeoutSeconds, 1U, 300U);
    ++global.generation;
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
  unsigned openTimeout = 30;
  std::size_t cacheBytes = 32ULL * 1024 * 1024;
  {
    auto &global = settings();
    std::lock_guard<std::mutex> lock(global.mutex);
    interval = global.probeIntervalSeconds;
    readTimeout = global.readTimeoutSeconds;
    openTimeout = global.openTimeoutSeconds;
    cacheBytes = global.prefetchCacheBytes;
  }
  auto manager = std::make_shared<XrdSourceManager>(url, interval, cacheBytes,
                                                    readTimeout, openTimeout);
  manager->startDiscovery();
  return manager;
}

std::shared_ptr<XrdSourceManager> acquireXrdSourceManager(const std::string &url) {
  struct Entry {
    std::weak_ptr<XrdSourceManager> active;
    std::shared_future<std::shared_ptr<XrdSourceManager>> pending;
  };
  static std::mutex poolMutex;
  static std::map<std::pair<std::string, std::uint64_t>, std::shared_ptr<Entry>> pool;
  unsigned interval, readTimeout, openTimeout;
  std::size_t cacheBytes;
  std::uint64_t generation;
  {
    auto &global = settings();
    std::lock_guard<std::mutex> lock(global.mutex);
    interval = global.probeIntervalSeconds;
    readTimeout = global.readTimeoutSeconds;
    openTimeout = global.openTimeoutSeconds;
    cacheBytes = global.prefetchCacheBytes;
    generation = global.generation;
  }
  const auto key = std::make_pair(url, generation);
  std::shared_ptr<Entry> entry;
  std::shared_future<std::shared_ptr<XrdSourceManager>> pending;
  std::unique_ptr<std::promise<std::shared_ptr<XrdSourceManager>>> creator;
  {
    std::lock_guard<std::mutex> lock(poolMutex);
    for (auto it = pool.begin(); it != pool.end();) {
      if (it->second->active.expired() && !it->second->pending.valid())
        it = pool.erase(it);
      else
        ++it;
    }
    auto &slot = pool[key];
    if (!slot) slot = std::make_shared<Entry>();
    entry = slot;
    if (auto active = entry->active.lock()) {
      if (!active->isStopped()) return active;
    }
    if (!entry->pending.valid()) {
      creator = std::make_unique<std::promise<std::shared_ptr<XrdSourceManager>>>();
      entry->pending = creator->get_future().share();
    }
    pending = entry->pending;
  }
  // Never hold the registry mutex across network I/O or while joining another
  // open. Different URLs must remain independent, including active/lookahead.
  if (!creator) return pending.get();
  try {
    auto manager = std::make_shared<XrdSourceManager>(
        url, interval, cacheBytes, readTimeout, openTimeout);
    manager->startDiscovery();
    {
      std::lock_guard<std::mutex> lock(poolMutex);
      entry->active = manager;
      entry->pending = {};
    }
    creator->set_value(manager);
    return manager;
  } catch (...) {
    {
      std::lock_guard<std::mutex> lock(poolMutex);
      pool.erase(key);
    }
    creator->set_exception(std::current_exception());
    throw;
  }
}

} // namespace rdf::xrd
