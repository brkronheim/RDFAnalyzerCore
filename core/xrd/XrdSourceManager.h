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
                   std::size_t prefetchCacheBytes);
  ~XrdSourceManager();

  XrdSourceManager(const XrdSourceManager &) = delete;
  XrdSourceManager &operator=(const XrdSourceManager &) = delete;

  bool readv(const std::vector<ReadRange> &ranges, bool prefetch);
  std::uint64_t size() const { return fileSize_; }
  void startDiscovery();
  void stop();
  const std::string &url() const { return url_; }

private:
  struct Source;
  struct PendingRead;

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
                      std::shared_ptr<std::vector<char>> bytes);
  std::vector<std::shared_ptr<Source>>
  sources(bool includeBlacklistedFallbacks = false) const;

  std::string url_;
  std::string fileKey_;
  std::uint64_t fileSize_ = 0;
  unsigned probeIntervalSeconds_ = 5;
  std::chrono::steady_clock::time_point nextProbeAt_;
  std::shared_ptr<PrefetchCache> cache_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<std::shared_ptr<Source>> sources_;
  std::vector<std::string> triedHosts_;
  std::vector<std::string> reportedFullReadLimitSites_;
  bool probeInFlight_ = false;
  bool stopped_ = false;
  std::uint64_t instanceId_ = 0;
  std::atomic<std::uint64_t> readvSequence_{0};
  std::atomic<std::uint64_t> demandReadSequence_{0};
};

void configureXrdAdaptor(unsigned probeIntervalSeconds,
                         std::size_t prefetchCacheBytes,
                         unsigned preferredSiteBandPercent = 20,
                         unsigned explorationSharePercent = 10,
                         unsigned maxReadSources = 2);
std::shared_ptr<XrdSourceManager> makeXrdSourceManager(const std::string &url);
void registerXrdAdaptor();

} // namespace rdf::xrd

#endif
