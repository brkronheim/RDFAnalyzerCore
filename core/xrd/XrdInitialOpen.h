#ifndef RDFANALYZER_XRD_INITIAL_OPEN_H
#define RDFANALYZER_XRD_INITIAL_OPEN_H

#include "XrdOpenTrace.h"
#include "XrdSiteRanking.h"
#include "XrdEndpointCooldown.h"
#include <XrdCl/XrdClFileSystem.hh>
#include <atomic>
#include <algorithm>
#include <deque>
#include <set>
#include <vector>

namespace rdf::xrd::detail {

// Requests own their transports until callback completion, even if the caller
// has already used a winner or reached its deadline. Bound those late owners
// across active files and lookahead, not just within an individual lookup.
inline std::atomic<unsigned> initialOperations{0};
struct InitialOperation {
  bool acquired = false;
  InitialOperation() {
    auto n = initialOperations.load();
    while (n < 32) {
      if (initialOperations.compare_exchange_weak(n, n + 1)) { acquired = true; break; }
    }
  }
  ~InitialOperation() { if (acquired) --initialOperations; }
};

inline std::string locationUrl(const std::string &original, const std::string &address) {
  XrdCl::URL result(original);
  XrdCl::URL endpoint(result.GetProtocol() + "://" + address + "//");
  if (!endpoint.IsValid() || endpoint.GetHostName().empty()) return {};
  result.SetHostName(endpoint.GetHostName());
  result.SetPort(endpoint.GetPort());
  return result.GetURL(); // Retain path, authentication parameters and protocol.
}

struct InitialLocations {
  std::mutex mutex;
  std::condition_variable changed;
  std::deque<std::string> managers;
  std::set<std::string> seenManagers, seenServers;
  std::vector<std::string> servers;
  unsigned pending = 0, failures = 0;
  bool stopped = false;
  std::function<void(const std::string &)> log;
  explicit InitialLocations(const std::string &url) { managers.push_back(url); seenManagers.insert(url); }

  // Called with mutex held. Pending/offline advertisements are not readable
  // replicas; do not trigger staging or merge an entire site's server pool.
  void add(const std::string &original, const XrdCl::LocationInfo &locations) {
    for (auto i = locations.Begin(); i != locations.End(); ++i) {
      const auto url = locationUrl(original, i->GetAddress());
      if (url.empty()) continue;
      if (i->GetType() == XrdCl::LocationInfo::ServerOnline && seenServers.size() < 256 && seenServers.insert(url).second)
        servers.push_back(url);
      else if (i->IsManager() && seenManagers.size() < 32 && seenManagers.insert(url).second)
        managers.push_back(url);
    }
  }
};

class InitialLocateHandler final : public XrdCl::ResponseHandler {
public:
  InitialLocateHandler(std::shared_ptr<InitialLocations> state, std::string url)
      : state_(std::move(state)), url_(std::move(url)), fs_(XrdCl::URL(url_)) {}
  bool available() const { return operation_.acquired; }
  XrdCl::XRootDStatus start(unsigned timeout) {
    // Accept partial advertisements: cold metadata can otherwise request a
    // five-second wait, which XrdCl immediately rejects with our shorter budget.
    // No Refresh: repeated workers should benefit from federation caches.
    const auto flags = static_cast<XrdCl::OpenFlags::Flags>(kXR_prefname | kXR_compress | kXR_nowait);
    return fs_.Locate(XrdCl::URL(url_).GetPathWithParams(), flags, this, timeout);
  }
  void HandleResponse(XrdCl::XRootDStatus *status, XrdCl::AnyObject *response) override {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (status && status->IsOK() && response) {
        XrdCl::LocationInfo *locations = nullptr;
        response->Get(locations);
        if (locations && !state_->stopped) state_->add(url_, *locations);
      } else {
        ++state_->failures;
        if (state_->log) state_->log("[XrdAdaptor] locate-query-result server=" + OpenTrace::endpoint(XrdCl::URL(url_)) +
            " status=failed detail=" + (status ? status->ToStr() : "missing-status"));
      }
      --state_->pending;
      state_->changed.notify_all();
    }
    delete this;
  }
private:
  InitialOperation operation_;
  std::shared_ptr<InitialLocations> state_;
  std::string url_;
  XrdCl::FileSystem fs_;
};

inline std::vector<std::string> locateInitial(const std::string &url, unsigned timeout,
    const std::function<bool(const std::vector<std::string> &)> &enough,
    const std::function<void(unsigned, unsigned, bool)> &report,
    const std::function<void(const std::string &)> &log = {}) {
  auto state = std::make_shared<InitialLocations>(url);
  state->log = log;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout);
  auto settle = deadline;
  std::unique_lock<std::mutex> lock(state->mutex);
  while (std::chrono::steady_clock::now() < deadline) {
    while (state->pending < 4 && !state->managers.empty()) {
      const auto next = state->managers.front(); state->managers.pop_front();
      auto *handler = new InitialLocateHandler(state, next);
      if (!handler->available()) { delete handler; ++state->failures; continue; }
      ++state->pending;
      lock.unlock();
      const auto status = handler->start(timeout);
      if (!status.IsOK()) handler->HandleResponse(new XrdCl::XRootDStatus(status), nullptr);
      lock.lock();
    }
    if (!state->pending && state->managers.empty()) break;
    if (enough(state->servers)) {
      if (settle == deadline) settle = std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(150));
      if (std::chrono::steady_clock::now() >= settle) break;
    }
    state->changed.wait_until(lock, std::min(deadline, settle));
  }
  state->stopped = true;
  const auto result = state->servers;
  const auto failures = state->failures;
  // NoWait cannot establish that a manager advertised every available replica.
  const bool partial = true;
  lock.unlock();
  report(result.size(), failures, partial);
  return result;
}

class InitialClose final : public XrdCl::ResponseHandler {
public:
  explicit InitialClose(std::shared_ptr<XrdCl::File> file) : file_(std::move(file)) {
    if (!file_->IsOpen() || !file_->Close(this).IsOK()) delete this;
  }
  void HandleResponse(XrdCl::XRootDStatus *s, XrdCl::AnyObject *r) override { delete s; delete r; delete this; }
private:
  std::shared_ptr<XrdCl::File> file_;
};

struct InitialOpenResult { std::shared_ptr<XrdCl::File> file; std::uint64_t size = 0; std::string candidate; };
struct InitialAttempt {
  InitialOperation operation;
  std::shared_ptr<XrdCl::File> file = std::make_shared<XrdCl::File>();
  OpenTrace trace;
  std::string original;
  unsigned timeout;
  InitialAttempt(OpenTrace trace, unsigned timeout) : trace(std::move(trace)), original(this->trace.candidate), timeout(timeout) {}
};
struct InitialOpenState {
  std::mutex mutex;
  std::condition_variable changed;
  InitialOpenResult winner;
  std::deque<std::shared_ptr<InitialAttempt>> readyToStat;
  unsigned pending = 0;
  std::map<std::string,std::string> attemptedEndpoints;
  std::function<void(const std::string &)> log;
  bool stopped = false;
  void complete(std::shared_ptr<XrdCl::File> file, bool ok, std::uint64_t size, const std::string &candidate = {}) {
    bool keep = false;
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (ok && !stopped && !winner.file) {
        winner = {file, size, candidate}; keep = true;
        std::vector<std::string> losers;
        for (const auto &endpoint : attemptedEndpoints)
          if (endpoint.first != candidate) { losers.push_back(endpoint.first); losers.push_back(endpoint.second); }
        recordEndpointWinner(losers, {candidate, attemptedEndpoints[candidate]}, log);
      }
      --pending;
      changed.notify_all();
    }
    if (!keep && file) new InitialClose(std::move(file));
  }
};

// Dispatch Stat from the caller, never recursively from an Open callback:
// XrdCl can hold the File state lock while delivering that callback.
class InitialOpenHandler final : public XrdCl::ResponseHandler {
public:
  InitialOpenHandler(std::shared_ptr<InitialOpenState> state, std::shared_ptr<InitialAttempt> attempt, bool stat = false)
      : state_(std::move(state)), attempt_(std::move(attempt)), stat_(stat) {}
  void start() {
    auto &a = *attempt_;
    a.trace.begin();
    const auto status = stat_ ? a.file->Stat(false, this, a.timeout) :
        a.file->Open(a.trace.candidate, XrdCl::OpenFlags::Read, XrdCl::Access::None, this, a.timeout);
    if (!status.IsOK()) HandleResponse(new XrdCl::XRootDStatus(status), nullptr);
  }
  void HandleResponse(XrdCl::XRootDStatus *s, XrdCl::AnyObject *r) override { HandleResponseWithHosts(s, r, nullptr); }
  void HandleResponseWithHosts(XrdCl::XRootDStatus *s, XrdCl::AnyObject *r, XrdCl::HostList *h) override {
    std::unique_ptr<XrdCl::XRootDStatus> status(s);
    std::unique_ptr<XrdCl::AnyObject> response(r);
    std::unique_ptr<XrdCl::HostList> hosts(h);
    const auto result = s ? *s : XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInternal);
    attempt_->trace.result(*attempt_->file, result, h, !stat_);
    if (!stat_ && result.IsOK()) {
      bool queued = false;
      {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if (!state_->stopped && !state_->winner.file) {
          state_->readyToStat.push_back(attempt_);
          state_->changed.notify_all();
          queued = true;
        }
      }
      if (queued) { delete this; return; }
    }
    XrdCl::StatInfo *info = nullptr;
    if (stat_ && r) r->Get(info);
    state_->complete(attempt_->file, stat_ && result.IsOK() && info,
                     info ? info->GetSize() : 0, attempt_->original);
    delete this;
  }
private:
  std::shared_ptr<InitialOpenState> state_;
  std::shared_ptr<InitialAttempt> attempt_;
  bool stat_;
};

struct InitialCandidate { std::string url, site; SiteRank rank; };
inline std::vector<InitialCandidate> rankInitial(std::vector<InitialCandidate> candidates) {
  std::set<std::string> cooled;
  for (const auto &candidate : candidates) if (endpointCooldown().active(candidate.url)) cooled.insert(candidate.url);
  std::stable_sort(candidates.begin(), candidates.end(), [&cooled](const auto &a, const auto &b) {
    const auto ac = cooled.count(a.url), bc = cooled.count(b.url);
    return ac != bc ? !ac : siteRankBefore(a.rank, b.rank);
  });
  std::set<std::string> sites;
  std::vector<InitialCandidate> result;
  for (auto &candidate : candidates)
    if (sites.insert(candidate.site).second) result.push_back(std::move(candidate));
  if (result.size() > 4) result.resize(4);
  return result;
}

inline InitialOpenResult openInitialCandidates(const std::vector<InitialCandidate> &candidates,
    unsigned timeout, const std::function<OpenTrace(const std::string &, unsigned)> &trace,
    const std::function<void(std::shared_ptr<InitialOpenState>, const InitialCandidate &, unsigned)> &launch = {}) {
  auto state = std::make_shared<InitialOpenState>();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout);
  auto hedgeAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  std::vector<bool> attempted(candidates.size(), false);
  const auto nextCandidate = [&] {
    for (std::size_t i=0; i<candidates.size(); ++i)
      if (!attempted[i] && !endpointCooldown().active(candidates[i].url)) return i;
    if (!state->pending) for (std::size_t i=0; i<candidates.size(); ++i) if (!attempted[i]) return i;
    return candidates.size();
  };
  std::unique_lock<std::mutex> lock(state->mutex);
  while (!state->winner.file && std::chrono::steady_clock::now() < deadline) {
    while (!state->readyToStat.empty() && !state->winner.file) {
      auto attempt = state->readyToStat.front(); state->readyToStat.pop_front();
      std::string resolved;
      if (attempt->file->GetProperty("LastURL", resolved) && !resolved.empty()) attempt->trace.candidate = resolved;
      state->attemptedEndpoints[attempt->original] = resolved;
      attempt->trace.operation = "stat";
      attempt->trace.started = std::chrono::steady_clock::now();
      auto *handler = new InitialOpenHandler(state, std::move(attempt), true);
      lock.unlock(); handler->start(); lock.lock();
    }
    while (!state->winner.file && state->pending < 2 &&
           (!state->pending || std::chrono::steady_clock::now() >= hedgeAt)) {
      const auto index = nextCandidate();
      if (index == candidates.size()) break;
      attempted[index] = true;
      if (launch) {
        state->attemptedEndpoints[candidates[index].url] = {};
        ++state->pending;
        lock.unlock(); launch(state, candidates[index], index + 1); lock.lock();
        continue;
      }
      auto attempt = std::make_shared<InitialAttempt>(trace(candidates[index].url, index + 1), timeout);
      state->log = attempt->trace.log;
      if (!attempt->operation.acquired) continue;
      state->attemptedEndpoints[candidates[index].url] = {};
      auto *handler = new InitialOpenHandler(state, std::move(attempt));
      ++state->pending;
      lock.unlock(); handler->start(); lock.lock();
    }
    if (state->winner.file || (!state->pending && nextCandidate() == candidates.size())) break;
    if (!state->readyToStat.empty()) continue;
    state->changed.wait_until(lock, state->pending < 2 && nextCandidate() < candidates.size() ? std::min(deadline, hedgeAt) : deadline);
  }
  state->stopped = true;
  auto result = std::move(state->winner);
  auto ready = std::move(state->readyToStat);
  lock.unlock();
  for (auto &attempt : ready) state->complete(attempt->file, false, 0);
  return result; // Late callbacks close losers without blocking this caller.
}

} // namespace rdf::xrd::detail
#endif
