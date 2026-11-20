#ifndef RDFANALYZER_XRD_SOURCE_MANAGER_H
#define RDFANALYZER_XRD_SOURCE_MANAGER_H

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace XrdCl {
class File;
class XRootDStatus;
}

namespace rdf::xrd {

// Marks future-file warmup, including its ordinary destination-buffer reads.
// ROOT's active-file asynchronous prefetch retains demand priority.
class BackgroundReadScope {
public:
  explicit BackgroundReadScope(bool background = true);
  ~BackgroundReadScope();
  BackgroundReadScope(const BackgroundReadScope &) = delete;
  BackgroundReadScope &operator=(const BackgroundReadScope &) = delete;
private:
  bool previous_;
};
bool isBackgroundRead();
void configureXrdReadScheduling(unsigned pipelineDepth, unsigned hedgeDelayMs,
                                bool diagnostics = false);
// Empty file/origin use RDF_XRD_SITE_RANKING_FILE / RDF_XRD_SITE_ORIGIN.
// "none" disables priors. Unknown origins retain ordinary live selection.
void configureXrdSiteRanking(const std::string &file = {},
                             const std::string &origin = {},
                             unsigned maxAgeDays = 30);

struct ReadRange {
  std::uint64_t offset = 0;
  std::uint32_t length = 0;
  char *buffer = nullptr;
};

class XrdSourceManager : public std::enable_shared_from_this<XrdSourceManager> {
public:
  struct PrefetchCache;
  class ProbeHandler;
  class ExplorationReadHandler;

  XrdSourceManager(std::string url, unsigned probeIntervalSeconds,
                   std::size_t prefetchCacheBytes,
                   unsigned readTimeoutSeconds = 120);
  XrdSourceManager(std::string url, unsigned probeIntervalSeconds,
                   std::size_t prefetchCacheBytes, unsigned readTimeoutSeconds,
                   unsigned openTimeoutSeconds);
  ~XrdSourceManager();

  XrdSourceManager(const XrdSourceManager &) = delete;
  XrdSourceManager &operator=(const XrdSourceManager &) = delete;

  bool readv(const std::vector<ReadRange> &ranges, bool prefetch);
  std::uint64_t size() const { return fileSize_; }
  void startDiscovery();
  void stop();
  bool isStopped() const { return stopped_.load(); }
  const std::string &url() const { return url_; }

private:
  struct Source;
  struct PendingRead;

  bool readvWindow(const std::vector<ReadRange> &ranges, bool prefetch,
                   bool background, std::uint64_t cacheUse);

  bool completeProbe(const std::shared_ptr<XrdCl::File> &file,
                     const XrdCl::XRootDStatus *status);
  std::unique_ptr<PendingRead>
  submit(const std::shared_ptr<Source> &source,
         const std::vector<ReadRange> &ranges, std::size_t sourceIndex);
  bool finish(PendingRead &pending, std::uint64_t &bytes, double &seconds);
  bool issue(const std::shared_ptr<Source> &source,
             const std::vector<ReadRange> &ranges, std::uint64_t &bytes,
             double &seconds);
  void scheduleExplorationProbe(const std::shared_ptr<Source> &source,
                                const std::string &peerSite,
                                const std::vector<ReadRange> &ranges);
  bool copyPrefetched(std::uint64_t offset, std::uint32_t length,
                      char *destination);
  void savePrefetched(std::uint64_t offset,
                      std::shared_ptr<std::vector<char>> bytes, std::uint64_t use = 0);
  std::vector<std::shared_ptr<Source>>
  sources(bool includeBlacklistedFallbacks = false) const;

  std::string url_;
  std::string fileKey_;
  std::uint64_t fileSize_ = 0;
  unsigned probeIntervalSeconds_ = 5;
  unsigned readTimeoutSeconds_ = 120;
  unsigned openTimeoutSeconds_ = 30;
  std::chrono::steady_clock::time_point nextProbeAt_;
  std::shared_ptr<PrefetchCache> cache_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<std::shared_ptr<Source>> sources_;
  std::vector<std::string> triedHosts_;
  std::vector<std::string> reportedFullReadLimitSites_;
  bool probeInFlight_ = false;
  std::atomic<bool> stopped_{false};
  std::uint64_t instanceId_ = 0;
  std::atomic<std::uint64_t> readvSequence_{0};
  std::atomic<std::uint64_t> probeSequence_{0};
  std::atomic<std::uint64_t> demandReadSequence_{0};
};

void configureXrdAdaptor(unsigned probeIntervalSeconds,
                         std::size_t prefetchCacheBytes,
                         unsigned preferredSiteBandPercent = 20,
                         unsigned explorationSharePercent = 10,
                         unsigned maxReadSources = 2,
                         unsigned readTimeoutSeconds = 120);
void configureXrdAdaptor(unsigned probeIntervalSeconds,
                         std::size_t prefetchCacheBytes,
                         unsigned preferredSiteBandPercent,
                         unsigned explorationSharePercent,
                         unsigned maxReadSources,
                         unsigned readTimeoutSeconds,
                         unsigned openTimeoutSeconds);
std::shared_ptr<XrdSourceManager> makeXrdSourceManager(const std::string &url);
// Share only live read-only transport state, never ROOT file cursors. Concurrent
// opens of the same URL join one in-flight open; unused entries are weak.
std::shared_ptr<XrdSourceManager> acquireXrdSourceManager(const std::string &url);
void registerXrdAdaptor();

} // namespace rdf::xrd

#endif
