#include <InputCache.h>
#include <ROOT/RDataFrame.hxx>
#include <TChainElement.h>
#include <TFile.h>
#include <TDirectory.h>
#include <TFileCacheRead.h>
#include <TBranch.h>
#include <TObjArray.h>
#include <TTree.h>
#include <TROOT.h>
#include <TTreeCache.h>
#include <TFilePrefetch.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <future>

namespace {
std::string setting(const IConfigurationProvider &config, const char *key) {
  const auto &map = config.getConfigMap();
  const auto it = map.find(key);
  if (it == map.end()) return {};
  const auto first = it->second.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  return it->second.substr(first, it->second.find_last_not_of(" \t\r\n") - first + 1);
}
bool flag(const IConfigurationProvider &config, const char *key, bool fallback) {
  const auto value = setting(config, key);
  if (value.empty()) return fallback;
  if (value == "1" || value == "true" || value == "True") return true;
  if (value == "0" || value == "false" || value == "False") return false;
  throw std::runtime_error(std::string(key) + " must be 0/1 or false/true");
}
Long64_t number(const IConfigurationProvider &config, const char *key,
                Long64_t fallback, Long64_t maximum) {
  const auto value = setting(config, key);
  if (value.empty()) return fallback;
  try {
    size_t end = 0;
    const auto n = std::stoll(value, &end);
    if (end == value.size() && n >= 0 && n <= maximum) return n;
  } catch (const std::exception &) {}
  throw std::runtime_error(std::string("Invalid ") + key + ": " + value);
}
// Public API present in ROOT 6.26--6.36. Older builds without this API keep
// ordinary synchronous caching; no ROOT internals or worker pool required.
template <class Cache>
auto prefetch(Cache &cache, bool enabled, int)
    -> decltype(cache.SetEnablePrefetching(enabled), bool()) {
  cache.SetEnablePrefetching(enabled);
  return cache.IsEnablePrefetching() == enabled;
}
template <class Cache> bool prefetch(Cache &, bool enabled, long) { return !enabled; }

const char *asyncOpenStatusName(TFile::EAsyncOpenStatus status) {
  switch (status) {
  case TFile::kAOSNotAsync: return "not-async";
  case TFile::kAOSFailure: return "failure";
  case TFile::kAOSInProgress: return "in-progress";
  case TFile::kAOSSuccess: return "success";
  }
  return "unknown";
}
}

std::unique_ptr<TChain> makeInputCacheChain(
    const std::string &name, const IConfigurationProvider &config,
    bool allowOptimization) {
  const bool stats = flag(config, "treeCacheStats", true);
  bool optimize = allowOptimization && flag(config, "treeCacheEnabled", false);
  bool asyncOpenNextFile =
      allowOptimization && flag(config, "treeCacheAsyncOpenNext", false);
  bool prefetchNextFile =
      allowOptimization && flag(config, "treeCachePrefetchNextFile", false);
  if (ROOT::IsImplicitMTEnabled()) {
    if (optimize)
      std::cerr << "[InputCache] fallback: IMT is active; retaining ROOT cache policy\n";
    if (asyncOpenNextFile)
      std::cerr << "[InputCache] fallback: IMT is active; disabling next-file asynchronous open\n";
    if (prefetchNextFile)
      std::cerr << "[InputCache] fallback: IMT is active; disabling next-file data prefetch\n";
    optimize = false;
    asyncOpenNextFile = false;
    prefetchNextFile = false;
    if (stats)
      std::cout << "[InputCache] IMT: per-worker cache statistics are unavailable on "
                   "the supplied chain; reports cover the master chain only. "
                   "Process-wide ROOT read counters include worker I/O.\n";
  }
  if (!stats && !optimize && !asyncOpenNextFile && !prefetchNextFile)
    return std::make_unique<TChain>(name.c_str());
  auto chain = std::make_unique<InputCacheChain>(name, config, optimize,
                                                asyncOpenNextFile);
  return chain;
}

InputCacheChain::InputCacheChain(const std::string &name)
    : TChain(name.c_str()), optimize_(false) {}

InputCacheChain::InputCacheChain(const std::string &name,
                               const IConfigurationProvider &config, bool optimize)
    : InputCacheChain(name, config, optimize,
                      !ROOT::IsImplicitMTEnabled() &&
                          flag(config, "treeCacheAsyncOpenNext", false)) {}

InputCacheChain::InputCacheChain(const std::string &name,
                               const IConfigurationProvider &config, bool optimize,
                               bool asyncOpenNextFile)
    : TChain(name.c_str()),
      asyncPrefetch_(flag(config, "treeCacheAsyncPrefetch", true)),
      asyncOpenNextFile_(asyncOpenNextFile),
      prefetchNextFile_(!ROOT::IsImplicitMTEnabled() &&
                        flag(config, "treeCachePrefetchNextFile", false)),
      explicitBranches_(flag(config, "treeCacheExplicitBranches", true)),
      statistics_(flag(config, "treeCacheStats", true)),
      optimize_(optimize),
      verbosity_(number(config, "treeCacheVerbosity", 0, 2)) {
  warmCacheLimitBytes_ = number(config, "xrdPrefetchCacheMB", 256, 131072) *
                         1024 * 1024;
  if (prefetchNextFile_) ROOT::EnableThreadSafety();
  if (prefetchNextFile_) asyncOpenNextFile_ = false;
  if (!setting(config, "firstEntry").empty() && !setting(config, "lastEntry").empty()) {
    first_ = number(config, "firstEntry", 0, TTree::kMaxEntries);
    last_ = number(config, "lastEntry", 0, TTree::kMaxEntries);
    if (last_ <= first_) { first_ = 0; last_ = -1; }
  }
  if (!optimize_) {
    if (statistics_ || verbosity_)
      std::cout << "[InputCache] statistics=" << statistics_
                << ", optimization disabled; retaining ROOT cache policy\n";
    if (asyncOpenNextFile_)
      std::cout << "[InputCache] next-file asynchronous open enabled\n";
    return;
  }
  // Keep the existing factor-based configuration available; never reinterpret
  // ROOT_TTREECACHE_SIZE as bytes or silently override a configured factor.
  if (setting(config, "rootTTreeCacheSize").empty() &&
      setting(config, "ROOT_TTREECACHE_SIZE").empty()) {
    const auto size = number(config, "treeCacheSizeMB", 128, 512);
    if (SetCacheSize(size * 1024 * 1024) != 0)
      std::cerr << "[InputCache] fallback: could not set cache size\n";
  }
  if (!explicitBranches_) fallbackReason_ = "explicit branch policy disabled";
  if (asyncOpenNextFile_)
    std::cout << "[InputCache] next-file asynchronous open enabled\n";
  if (verbosity_)
    std::cout << "[InputCache] enabled, requested bytes=" << GetCacheSize()
              << ", async prefetch=" << asyncPrefetch_
              << ", dependencies=ROOT TTreeReader proxies (resolved at loop startup)\n";
}

InputCacheChain::~InputCacheChain() {
  if (statistics_) reportCache("chain-destruction");
}

Long64_t InputCacheChain::LoadTree(Long64_t entry) {
  const auto callbackStarted = std::chrono::steady_clock::now();
  const int previousTree = GetTreeNumber();
  auto *previousTreeObject = GetTree();
  bool crossingBoundary = false;
  if (previousTreeObject && previousTree >= 0) {
    const auto offset = GetTreeOffset()[previousTree];
    crossingBoundary = entry < offset ||
                       entry >= offset + previousTreeObject->GetEntries();
  }

  int expectedTree = -1;
  std::string expectedUrl;
  if (crossingBoundary && previousTree >= 0) {
    expectedTree = previousTree + 1;
    auto *files = GetListOfFiles();
    if (files && expectedTree < files->GetEntriesFast()) {
      auto *element = dynamic_cast<TChainElement *>(files->At(expectedTree));
      if (element && element->GetTitle()) expectedUrl = element->GetTitle();
    }
  }

  struct WarmState {
    bool found = false;
    bool ready = false;
    double ageSeconds = -1.;
  };
  const auto warmStateFor = [this](const std::string &url) {
    WarmState state;
    if (url.empty()) return state;
    const auto now = std::chrono::steady_clock::now();
    for (const auto &task : warmTasks_) {
      if (task.url != url) continue;
      state.found = true;
      state.ready = task.file.wait_for(std::chrono::seconds(0)) ==
                    std::future_status::ready;
      state.ageSeconds = std::chrono::duration<double>(now - task.scheduledAt).count();
      break;
    }
    return state;
  };
  const auto warmBefore = warmStateFor(expectedUrl);
  const auto openStatusBefore = expectedUrl.empty()
                                    ? TFile::kAOSNotAsync
                                    : TFile::GetAsyncOpenStatus(expectedUrl.c_str());

  double reportMs = 0.;
  // Report before ROOT closes the old file. Do not replace the notify chain:
  // TTreeReader uses it to refresh proxies and friends on file transitions.
  if (statistics_ && previousTreeObject && previousTree >= 0) {
    if (crossingBoundary) {
      const auto reportStarted = std::chrono::steady_clock::now();
      reportCache("file-boundary-or-end");
      reportMs = std::chrono::duration<double, std::milli>(
                     std::chrono::steady_clock::now() - reportStarted)
                     .count();
    }
  }
  const auto loadStarted = std::chrono::steady_clock::now();
  const auto result = TChain::LoadTree(entry);
  const double rootLoadMs = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - loadStarted)
                                .count();
  const auto warmAfterLoad = warmStateFor(expectedUrl);
  const auto openStatusAfterLoad = expectedUrl.empty()
                                       ? TFile::kAOSNotAsync
                                       : TFile::GetAsyncOpenStatus(expectedUrl.c_str());
  double configureMs = 0.;
  double warmReleaseMs = 0.;
  double scheduleMs = 0.;
  if (result >= 0 && GetTreeNumber() != configuredTree_) {
    configuredTree_ = GetTreeNumber();
    fileStarted_ = std::chrono::steady_clock::now();
    lastReport_.clear();
    const auto configureStarted = std::chrono::steady_clock::now();
    if (optimize_) configureCurrentTree();
    configureMs = std::chrono::duration<double, std::milli>(
                      std::chrono::steady_clock::now() - configureStarted)
                      .count();
    if (pendingAsyncOpenTree_ == configuredTree_)
      pendingAsyncOpenTree_ = -1;
    auto *currentFile = GetCurrentFile();
    const auto releaseStarted = std::chrono::steady_clock::now();
    releaseWarmFileFor(currentFile ? currentFile->GetName() : std::string{});
    warmReleaseMs = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - releaseStarted)
                        .count();
    const auto scheduleStarted = std::chrono::steady_clock::now();
    scheduleNextFileOpen();
    // The first lookahead is queued when ROOT finishes learning the proxy
    // branches. Repeat it after each file switch so every current file warms
    // the next one, once that branch set is known to be complete.
    if (prefetchNextFile_ && dependenciesResolved_)
      scheduleNextFilePrefetch();
    scheduleMs = std::chrono::duration<double, std::milli>(
                     std::chrono::steady_clock::now() - scheduleStarted)
                     .count();
  }

  if (crossingBoundary &&
      (statistics_ || asyncOpenNextFile_ || prefetchNextFile_)) {
    const auto *currentFile = GetCurrentFile();
    const auto callbackMs = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - callbackStarted)
                                .count();
    std::ostringstream transitionLog;
    transitionLog << std::fixed << std::setprecision(3)
                  << "[InputCache] file-transition from_index=" << previousTree
                  << " expected_to_index=" << expectedTree
                  << " actual_to_index=" << GetTreeNumber()
                  << " requested_entry=" << entry
                  << " result=" << result
                  << " target=" << (expectedUrl.empty() ? "<unknown>" : expectedUrl)
                  << " opened=" << (currentFile ? currentFile->GetName() : "<none>")
                  << " warm_task_found=" << warmBefore.found
                  << " warm_task_ready_before=" << warmBefore.ready
                  << " warm_task_ready_after_load=" << warmAfterLoad.ready
                  << " warm_task_age_s=" << warmBefore.ageSeconds
                  << " root_async_open_before=" << asyncOpenStatusName(openStatusBefore)
                  << " root_async_open_after_load=" << asyncOpenStatusName(openStatusAfterLoad)
                  << " cache_report_ms=" << reportMs
                  << " root_loadtree_ms=" << rootLoadMs
                  << " configure_tree_ms=" << configureMs
                  << " warm_release_ms=" << warmReleaseMs
                  << " schedule_next_ms=" << scheduleMs
                  << " callback_total_ms=" << callbackMs;
    std::cout << transitionLog.str() << '\n';
  }
  return result;
}

void InputCacheChain::scheduleNextFileOpen() {
  if (!asyncOpenNextFile_ || prefetchNextFile_ || GetTreeNumber() < 0) return;

  const int nextTree = GetTreeNumber() + 1;
  auto *files = GetListOfFiles();
  if (!files || nextTree >= files->GetEntriesFast()) return;
  if (pendingAsyncOpenTree_ == nextTree) return;
  // Keep at most one ROOT async-open request outstanding. Sequential RDF
  // loops consume it when TChain::LoadTree opens this URL on the next switch.
  if (pendingAsyncOpenTree_ >= 0) return;

  auto *element = dynamic_cast<TChainElement *>(files->At(nextTree));
  if (!element || !element->GetTitle() || !*element->GetTitle()) return;
  const std::string url = element->GetTitle();
  const auto schemeEnd = url.find("://");
  const auto protocol = url.substr(0, schemeEnd);
  if (schemeEnd == std::string::npos ||
      (protocol.rfind("root", 0) != 0 && protocol.rfind("xroot", 0) != 0)) {
    if (verbosity_)
      std::cout << "[InputCache] async next-file open skipped: unsupported URL "
                << url << '\n';
    return;
  }

  // TChain's default mode opens files with "READ". TFile::Open(url) will
  // adopt this matching request when LoadTree reaches the next element.
  if (TFile::GetAsyncOpenStatus(url.c_str()) != TFile::kAOSNotAsync) {
    pendingAsyncOpenTree_ = nextTree;
    if (verbosity_)
      std::cout << "[InputCache] reusing pending async open file_index="
                << nextTree << " file=" << url << '\n';
    return;
  }

  auto *handle = TFile::AsyncOpen(url.c_str(), "READ");
  if (!handle) {
    if (verbosity_)
      std::cerr << "[InputCache] fallback: could not queue async open for "
                << url << '\n';
    return;
  }

  pendingAsyncOpenTree_ = nextTree;
  const auto status = TFile::GetAsyncOpenStatus(handle);
  if (verbosity_)
    std::cout << "[InputCache] queued next-file async open file_index="
              << nextTree << " file=" << url
              << " status=" << status << '\n';
}

void InputCacheChain::configureCurrentTree() {
  auto *tree = GetTree();
  auto *file = GetCurrentFile();
  if (!tree || !file) return;
  auto *cache = tree->GetReadCache(file, true);
  if (!cache) {
    if (verbosity_) std::cout << "[InputCache] fallback: no cache (size zero or unavailable)\n";
    return;
  }
  if (!prefetch(*cache, asyncPrefetch_, 0))
    std::cerr << "[InputCache] fallback: asynchronous prefetch unavailable\n";
  // The RDF Range node does not set the TTreeReader range. Translate the
  // configured global interval into this file's coordinates on every switch.
  // Do not apply the main tree's interval to indexed friends.
  if (last_ >= 0) {
    const auto offset = GetTreeOffset()[GetTreeNumber()];
    const auto entries = tree->GetEntries();
    tree->SetCacheEntryRange(std::clamp(first_ - offset, 0LL, entries),
                            std::clamp(last_ - offset, 0LL, entries));
  }
  if (!fallbackReason_.empty()) cache->StartLearningPhase();
  if (verbosity_ > 1) reportCache();
}

Int_t InputCacheChain::AddBranchToCache(const char *name, bool subbranches) {
  const auto result = TChain::AddBranchToCache(name, subbranches);
  if (optimize_ && result < 0)
    markDependenciesIncomplete(std::string("branch registration failed: ") + (name ? name : "<null>"));
  return result;
}
Int_t InputCacheChain::AddBranchToCache(TBranch *branch, bool subbranches) {
  const auto result = TChain::AddBranchToCache(branch, subbranches);
  if (optimize_ && result < 0) markDependenciesIncomplete("branch metadata registration failed");
  return result;
}

void InputCacheChain::markDependenciesIncomplete(const std::string &reason) {
  if (!optimize_) return;
  fallbackReason_ = reason.empty() ? "undeclared input dependencies" : reason;
  dependenciesResolved_ = false;
  if (GetTree() && GetCurrentFile()) {
    if (auto *cache = GetTree()->GetReadCache(GetCurrentFile(), false))
      cache->StartLearningPhase();
  }
  std::cerr << "[InputCache] retaining learning: " << fallbackReason_ << '\n';
}

Int_t InputCacheChain::StopCacheLearningPhase() {
  // TTreeReader reaches this after all proxies (including count leaves and
  // split branches) have been created. Raw RNode actions are covered too.
  if (!fallbackReason_.empty()) {
    if (verbosity_) std::cout << "[InputCache] retaining learning: " << fallbackReason_ << '\n';
    return 0;
  }
  const auto result = TChain::StopCacheLearningPhase();
  dependenciesResolved_ = result == 0;
  if (dependenciesResolved_ && prefetchNextFile_)
    scheduleNextFilePrefetch();
  if (verbosity_) reportCache();
  return result;
}

void InputCacheChain::releaseWarmFileFor(const std::string &url) {
  if (!prefetchNextFile_ || url.empty()) return;
  for (auto task = warmTasks_.begin(); task != warmTasks_.end();) {
    if (task->url == url) task->consumed = true;
    if (!task->consumed ||
        task->file.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
      ++task;
      continue;
    }
    try {
      task->file.get().reset();
    } catch (const std::exception &error) {
      std::cerr << "[InputCache] next-file prefetch failed for " << url
                << ": " << error.what() << '\n';
    }
    task = warmTasks_.erase(task);
  }
}

void InputCacheChain::scheduleNextFilePrefetch() {
  if (!prefetchNextFile_ || GetTreeNumber() < 0 || !GetTree() ||
      !GetCurrentFile() || warmCacheLimitBytes_ <= 0)
    return;

  for (auto task = warmTasks_.begin(); task != warmTasks_.end();) {
    if (!task->consumed ||
        task->file.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
      ++task;
      continue;
    }
    try {
      task->file.get().reset();
    } catch (const std::exception &error) {
      std::cerr << "[InputCache] next-file prefetch failed for " << task->url
                << ": " << error.what() << '\n';
    }
    task = warmTasks_.erase(task);
  }

  auto *currentCache = GetTree()->GetReadCache(GetCurrentFile(), false);
  if (!currentCache || !currentCache->GetCachedBranches()) return;
  std::vector<std::string> branches;
  auto *cachedBranches = currentCache->GetCachedBranches();
  for (Int_t i = 0; i < cachedBranches->GetEntriesFast(); ++i) {
    auto *branch = dynamic_cast<TBranch *>(cachedBranches->At(i));
    if (branch && branch->GetName() && *branch->GetName())
      branches.emplace_back(branch->GetName());
  }
  if (branches.empty()) return;

  const int nextTree = GetTreeNumber() + 1;
  auto *files = GetListOfFiles();
  if (!files || nextTree >= files->GetEntriesFast()) return;
  auto *element = dynamic_cast<TChainElement *>(files->At(nextTree));
  if (!element || !element->GetTitle() || !*element->GetTitle()) return;
  const std::string url = element->GetTitle();
  if (std::any_of(warmTasks_.begin(), warmTasks_.end(), [&](const auto &task) {
        return task.url == url;
      }))
    return;
  if (warmTasks_.size() >= 2) {
    if (verbosity_)
      std::cout << "[InputCache] next-file warmup skipped: two lookahead files "
                   "are still pending\n";
    return;
  }

  const std::string treeName = GetTree()->GetName();
  const Long64_t cacheBytes = std::min(GetCacheSize(), warmCacheLimitBytes_);
  std::cout << "[InputCache] warming next file's first cached cluster: " << url
            << " (" << branches.size() << " branches)\n";
  WarmTask task;
  task.url = url;
  task.scheduledAt = std::chrono::steady_clock::now();
  task.file = std::async(
      std::launch::async,
      [url, treeName, branches = std::move(branches), cacheBytes,
       scheduledAt = task.scheduledAt]()
          -> std::shared_ptr<TFile> {
        const auto elapsedMs = [](std::chrono::steady_clock::time_point start,
                                  std::chrono::steady_clock::time_point end) {
          return std::chrono::duration<double, std::milli>(end - start).count();
        };
        const auto openStarted = std::chrono::steady_clock::now();
        TDirectory::TContext directoryContext;
        std::unique_ptr<TFile> opened(TFile::Open(url.c_str(), "READ"));
        const auto openedAt = std::chrono::steady_clock::now();
        if (!opened) {
          std::cerr << "[InputCache] next-file warmup could not open " << url
                    << " reason=TFile::Open returned null open_ms="
                    << elapsedMs(openStarted, openedAt)
                    << " total_ms=" << elapsedMs(scheduledAt, openedAt) << '\n';
          return {};
        }
        if (opened->IsZombie()) {
          std::cerr << "[InputCache] next-file warmup could not open " << url
                    << " reason=zombie backend=" << opened->ClassName()
                    << " errno=" << opened->GetErrno()
                    << " open_ms=" << elapsedMs(openStarted, openedAt)
                    << " total_ms=" << elapsedMs(scheduledAt, openedAt) << '\n';
          return {};
        }
        // Keep the warmed TFile independent so this thread can read its first
        // cached cluster. Queue ROOT's matching file open after that first
        // handle is acquired; TChain can adopt it at the next file boundary
        // while it consumes the bytes held in the URL-scoped Xrd handoff cache.
        const auto asyncQueueStarted = std::chrono::steady_clock::now();
        auto *asyncHandle = TFile::AsyncOpen(url.c_str(), "READ");
        const auto asyncStatus = asyncHandle
                                     ? TFile::GetAsyncOpenStatus(asyncHandle)
                                     : TFile::kAOSFailure;
        const auto asyncQueuedAt = std::chrono::steady_clock::now();
        if (!asyncHandle)
          std::cerr << "[InputCache] next-file warmup could not queue ROOT open for "
                    << url << '\n';
        const auto treeSetupStarted = std::chrono::steady_clock::now();
        auto *tree = dynamic_cast<TTree *>(opened->Get(treeName.c_str()));
        if (!tree || tree->GetEntries() <= 0) {
          std::cerr << "[InputCache] next-file warmup failed file=" << url
                    << " reason=missing-or-empty-tree tree=" << treeName
                    << " open_ms=" << elapsedMs(openStarted, openedAt)
                    << " async_open_queue_ms="
                    << elapsedMs(asyncQueueStarted, asyncQueuedAt)
                    << " async_open_status=" << asyncOpenStatusName(asyncStatus)
                    << " total_ms=" << elapsedMs(scheduledAt,
                                                 std::chrono::steady_clock::now())
                    << '\n';
          return {};
        }
        tree->SetBranchStatus("*", false);
        for (const auto &name : branches) {
          if (tree->GetBranch(name.c_str())) {
            tree->SetBranchStatus(name.c_str(), true);
            tree->AddBranchToCache(name.c_str(), true);
          }
        }
        if (cacheBytes > 0 && tree->SetCacheSize(cacheBytes) != 0)
          std::cerr << "[InputCache] next-file warmup could not set ROOT cache "
                       "size for " << url << '\n';
        auto *cache = tree->GetReadCache(opened.get(), true);
        if (!cache) {
          std::cerr << "[InputCache] next-file warmup failed file=" << url
                    << " reason=no-read-cache total_ms="
                    << elapsedMs(scheduledAt, std::chrono::steady_clock::now())
                    << '\n';
          return {};
        }
        cache->SetEnablePrefetching(true);
        tree->StopCacheLearningPhase();
        tree->SetCacheEntryRange(0, tree->GetEntries());
        const auto treeSetupAt = std::chrono::steady_clock::now();
        const auto entryReadStarted = std::chrono::steady_clock::now();
        if (tree->GetEntry(0) < 0) {
          std::cerr << "[InputCache] next-file warmup could not read entry 0 "
                    << "from " << url
                    << " open_ms=" << elapsedMs(openStarted, openedAt)
                    << " async_open_queue_ms="
                    << elapsedMs(asyncQueueStarted, asyncQueuedAt)
                    << " tree_setup_ms=" << elapsedMs(treeSetupStarted, treeSetupAt)
                    << " get_entry0_ms="
                    << elapsedMs(entryReadStarted, std::chrono::steady_clock::now())
                    << " total_ms=" << elapsedMs(scheduledAt,
                                                 std::chrono::steady_clock::now())
                    << '\n';
          return {};
        }
        const auto entryReadAt = std::chrono::steady_clock::now();
        const auto prefetchDrainStarted = std::chrono::steady_clock::now();
        cache->WaitFinishPrefetch();
        const auto completedAt = std::chrono::steady_clock::now();
        std::ostringstream warmupLog;
        warmupLog << "[InputCache] next-file warmup complete file=" << url
                  << " status=ready"
                  << " open_ms=" << elapsedMs(openStarted, openedAt)
                  << " async_open_queue_ms="
                  << elapsedMs(asyncQueueStarted, asyncQueuedAt)
                  << " async_open_status=" << asyncOpenStatusName(asyncStatus)
                  << " tree_setup_ms=" << elapsedMs(treeSetupStarted, treeSetupAt)
                  << " get_entry0_ms=" << elapsedMs(entryReadStarted, entryReadAt)
                  << " prefetch_drain_ms="
                  << elapsedMs(prefetchDrainStarted, completedAt)
                  << " total_ms=" << elapsedMs(scheduledAt, completedAt)
                  << " warm_file_bytes=" << opened->GetBytesRead()
                  << " warm_file_read_calls=" << opened->GetReadCalls()
                  << " warm_cache_bytes=" << cache->GetNtot()
                  << " prefetched_blocks=" << cache->GetPrefetchedBlocks();
        std::cout << warmupLog.str() << '\n';
        return std::shared_ptr<TFile>(opened.release());
      });
  warmTasks_.push_back(std::move(task));
}

void InputCacheChain::reportCache(const char *reason) const {
  auto *tree = GetTree();
  auto *file = GetCurrentFile();
  if (!tree || !file) return;
  auto *cache = tree->GetReadCache(file, false);
  // Do not create a cache just to report it. File counters remain useful
  // when caching is disabled or the graph consumes no physical columns.
  auto *prefetcher = cache ? cache->GetPrefetchObj() : nullptr;
  const double prefetchWaitSeconds =
      prefetcher ? prefetcher->GetWaitTime() / 1.e+6 : 0.;
  std::ostringstream signature;
  signature << file->GetName() << ':' << GetTreeNumber() << ':'
            << file->GetBytesRead() << ':' << file->GetReadCalls() << ':'
            << (cache ? cache->GetBytesRead() : 0) << ':'
            << (cache ? cache->GetNoCacheReadCalls() : 0) << ':'
            << (cache ? cache->GetPrefetchedBlocks() : 0) << ':'
            << prefetchWaitSeconds;
  if (lastReport_ == signature.str()) return;
  lastReport_ = signature.str();
  const double prefetchWaitDeltaSeconds =
      prefetcher == lastPrefetcher_ &&
              prefetchWaitSeconds >= lastPrefetchWaitSeconds_
          ? prefetchWaitSeconds - lastPrefetchWaitSeconds_
          : prefetchWaitSeconds;
  lastPrefetcher_ = prefetcher;
  lastPrefetchWaitSeconds_ = prefetchWaitSeconds;
  const double seconds = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - fileStarted_).count();
  std::cout << "[InputCache] reading statistics reason=" << reason
            << " tree=" << GetName() << " file_index=" << GetTreeNumber()
            << " file=" << file->GetName()
            << " backend=" << file->ClassName()
            << " tree_entries=" << tree->GetEntries()
            << " last_loaded_entry=" << tree->GetReadEntry() << '\n'
            << "[InputCache] file_bytes_read=" << file->GetBytesRead()
            << " file_read_calls=" << file->GetReadCalls()
            << " file_readahead_extra_bytes=" << file->GetBytesReadExtra()
            << " observed_file_wall_s=" << seconds
            << " read_calls_per_wall_s=" << (seconds > 0 ? file->GetReadCalls() / seconds : 0)
            << " read_MiB_per_wall_s=" << (seconds > 0 ? file->GetBytesRead() / (1048576. * seconds) : 0)
            << " average_read_bytes=" << (file->GetReadCalls() > 0 ?
                  double(file->GetBytesRead()) / file->GetReadCalls() : 0) << '\n'
            << "[InputCache] process_ROOT_bytes_read=" << TFile::GetFileBytesRead()
            << " process_ROOT_read_calls=" << TFile::GetFileReadCalls()
            << " (all ROOT files, including other readers; cumulative)\n";
  if (!cache) {
    std::cout << "[InputCache] cache unavailable or disabled; cache counters unavailable\n";
    return;
  }
  const auto *branches = cache->GetCachedBranches();
  std::cout << "[InputCache] file=" << file->GetName()
            << " bytes=" << cache->GetBufferSize()
            << " async=" << cache->IsEnablePrefetching()
            << " legacy_async_reading=" << cache->IsAsyncReading()
            << " async_open_next_file=" << asyncOpenNextFile_
            << " learning=" << cache->IsLearning()
            << " registered=" << (branches ? branches->GetEntries() : 0)
            << " dependencies=" << (!fallbackReason_.empty() ? "uncertain" :
                                     dependenciesResolved_ ? "complete (ROOT readers)" : "pending ROOT readers")
            << " fallback=" << (fallbackReason_.empty() ? "none" : fallbackReason_)
            << " configured-global-range=";
  if (last_ >= 0) {
    const auto offset = GetTreeOffset()[GetTreeNumber()];
    const auto entries = tree->GetEntries();
    std::cout << '[' << first_ << ',' << last_ << ") local-range=["
              << std::clamp(first_ - offset, 0LL, entries) << ','
              << std::clamp(last_ - offset, 0LL, entries) << ")\n";
  } else {
    std::cout << "unspecified (ROOT default)\n";
  }
  if (verbosity_ > 1 && branches)
    for (auto *branch : *branches) std::cout << "[InputCache] branch=" << branch->GetName() << '\n';
  if (statistics_) {
    // ROOT's public report includes efficiency, cached/uncached transactions
    // and bytes. These are cache snapshots, not additive per-file counters.
    std::cout << "[InputCache] cache-lifetime snapshot (not additive per file):"
              << " cached_bytes=" << cache->GetBytesRead()
              << " cached_read_calls=" << cache->GetReadCalls()
              << " uncached_bytes=" << cache->GetNoCacheBytesRead()
              << " uncached_read_calls=" << cache->GetNoCacheReadCalls()
              << " readahead_extra_bytes=" << cache->GetBytesReadExtra()
              << " efficiency=" << cache->GetEfficiency()
              << " relative_efficiency=" << cache->GetEfficiencyRel()
              << " miss_cache_efficiency=" << cache->GetMissEfficiency()
              << " relative_miss_cache_efficiency=" << cache->GetMissEfficiencyRel()
              << " prefetch_wait_lifetime_s=" << prefetchWaitSeconds
              << " prefetch_wait_since_report_s=" << prefetchWaitDeltaSeconds
              << " optimize_misses=" << cache->GetOptimizeMisses()
              << " cache_enabled=" << cache->IsEnabled()
              << " learn_entries=" << cache->GetLearnEntries()
              << " blocks_in_buffer=" << cache->GetNseek()
              << " buffer_payload_bytes=" << cache->GetNtot() << '\n';
    cache->Print();
    std::cout << "[InputCache] file bytes read=" << file->GetBytesRead()
              << " read calls=" << file->GetReadCalls()
              << " prefetched blocks=" << cache->GetPrefetchedBlocks() << '\n';
  }
}
