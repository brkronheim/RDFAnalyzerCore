// Compile the source manager here to exercise its internal cache and site
// bookkeeping without adding test-only API to the reader shared library.
#include "XrdSourceManager.cc"
#include <gtest/gtest.h>
#include <thread>
#include <fstream>
#include <future>
#include <unistd.h>

namespace rdf::xrd {
TEST(XrdReadMemory, AdaptiveRescueUsesSiteLatencyWindowSizeAndDeadline) {
  const detail::SiteRank cold{}, warm{40., true, true, false}, prior{100., false, false, true};
  using namespace std::chrono_literals;
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 50, cold, 32, 120), 1000ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 50, warm, 256 * 1024, 120), 250ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 50, prior, 256 * 1024, 120), 300ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 50, warm, 1024 * 1024, 120), 480ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 750, warm, 32, 120), 750ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 1000, prior, 8 * 1024 * 1024, 1), 500ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Adaptive, 50, prior, 8 * 1024 * 1024, 120), 4000ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Legacy, 50, warm, 8 * 1024 * 1024, 1), 50ms);
  EXPECT_EQ(backupDelay(XrdReadPolicy::Legacy, 0, cold, 32, 120), 0ms);
}

TEST(XrdReadMemory, AdaptiveHedgeCapIncludesLatePrimariesAcrossFiles) {
  std::vector<std::shared_ptr<detail::ReadRaceState>> windows;
  for (unsigned i = 0; i < 3; ++i) windows.push_back(std::make_shared<detail::ReadRaceState>());
  ASSERT_TRUE(windows[0]->admitAdaptiveHedge());
  ASSERT_TRUE(windows[0]->admitAdaptiveHedge());
  ASSERT_TRUE(windows[1]->admitAdaptiveHedge());
  EXPECT_FALSE(windows[2]->admitAdaptiveHedge());
  auto pendingPrimary = windows[0];
  windows[0].reset();
  EXPECT_FALSE(windows[2]->admitAdaptiveHedge());
  pendingPrimary.reset();
  EXPECT_TRUE(windows[2]->admitAdaptiveHedge());
}

TEST(XrdReadMemory, ConcurrentFileRacesHoldHedgePermitsUntilCallbacksDrain) {
  constexpr unsigned files = 12;
  std::vector<std::shared_ptr<detail::ReadRaceState>> windows(files);
  std::vector<std::future<bool>> readers;
  std::promise<void> release;
  auto start = release.get_future().share();
  for (auto &window : windows) {
    window = std::make_shared<detail::ReadRaceState>();
    readers.push_back(std::async(std::launch::async, [window, start] {
      start.wait();
      return window->admitAdaptiveHedge();
    }));
  }
  release.set_value();
  unsigned admitted = 0;
  for (auto &reader : readers) if (reader.get()) ++admitted;
  EXPECT_EQ(admitted, kMaxAdaptiveHedgesPerProcess);
  EXPECT_EQ(liveAdaptiveRaces.load(), admitted);
  windows.clear();
  EXPECT_EQ(liveAdaptiveRaces.load(), 0U);
}

TEST(XrdReadMemory, FailedSubmissionReturnsForegroundAndBackgroundSiteSlots) {
  const std::string site("slot-scope-test");
  try {
    FullReadSlot reserved(site, true);
    ASSERT_TRUE(reserved);
    EXPECT_FALSE(static_cast<bool>(FullReadSlot(site, true)));
    // A failed speculative submission can coexist with real demand.
    FullReadSlot demand(site, false);
    ASSERT_TRUE(demand);
    throw std::bad_alloc();
  } catch (const std::bad_alloc &) {
  }
  EXPECT_TRUE(static_cast<bool>(FullReadSlot(site, true)));
}

TEST(XrdInitialOpen, LateStandbyCallbackClosesTransportWhenManagerHasGone) {
  char path[] = "/tmp/rdf-xrd-standby-close-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  auto remove = std::shared_ptr<const char>(path, [](const char *name) { unlink(name); });
  const std::string url = "file://" + std::string(path);
  auto file = std::make_shared<XrdCl::File>();
  auto status = file->Open(url, XrdCl::OpenFlags::Read);
  ASSERT_TRUE(status.IsOK());
  std::weak_ptr<XrdCl::File> transport = file;
  detail::InitialOperation operation(detail::kStandbyMetadataLimit);
  ASSERT_TRUE(operation.acquired);
  auto *callback = new XrdSourceManager::ProbeHandler({}, std::move(file),
      openTrace(url, 1, 1, "replica"), std::move(operation));
  callback->HandleResponse(new XrdCl::XRootDStatus(status), nullptr);
  EXPECT_EQ(detail::initialOperations.load(), 0U);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!transport.expired() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_TRUE(transport.expired());
}

TEST(XrdInitialOpen, StandbyOpensLeaveMetadataCapacityForRecovery) {
  std::vector<std::unique_ptr<detail::InitialOperation>> callbacks;
  for (unsigned n = 0; n < detail::kStandbyMetadataLimit; ++n) {
    auto token = std::make_unique<detail::InitialOperation>(detail::kStandbyMetadataLimit);
    ASSERT_TRUE(token->acquired);
    callbacks.push_back(std::move(token));
  }
  EXPECT_FALSE(detail::InitialOperation(detail::kStandbyMetadataLimit).acquired);
  detail::InitialOperation recovery;
  ASSERT_TRUE(recovery.acquired);
  callbacks.clear();
  EXPECT_TRUE(detail::InitialOperation(detail::kStandbyMetadataLimit).acquired);
}

TEST(XrdReadMemory, FailedReadBetweenSchedulingAndWaitWakesImmediately) {
  detail::ReadRaceState race;
  const auto observed = race.completions();
  ASSERT_FALSE(race.complete(0, false, "local", "/failure-test", "file:///test", 10.));
  const auto start = std::chrono::steady_clock::now();
  race.waitForChange(std::chrono::seconds(1), observed);
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(250));
}

TEST(XrdReadMemory, TrafficDistinguishesSuccessfulCopiesFromShortResponses) {
  ReadTraffic traffic;
  traffic.beginFull(512, false);
  traffic.beginFull(512, true);
  traffic.finishFull(true, true, 512);
  traffic.finishFull(true, false, 512);
  traffic.beginFull(512, false);
  traffic.finishFull(false, false, 20);
  EXPECT_EQ(traffic.fullRequests.load(), 3u);
  EXPECT_EQ(traffic.outstanding.load(), 0u);
  EXPECT_EQ(traffic.peakOutstanding.load(), 2u);
  EXPECT_EQ(traffic.winningBytes.load(), 512u);
  EXPECT_EQ(traffic.losingBytes.load(), 512u);
  EXPECT_EQ(traffic.errorBytes.load(), 20u);
  EXPECT_EQ(traffic.requestedFullBytes.load(), 1536u);
  EXPECT_EQ(traffic.requestedHedgeBytes.load(), 512u);
}

TEST(XrdReadMemory, SerialPipelineUsesCallingThread) {
  const auto caller = std::this_thread::get_id();
  std::vector<int> windows{0, 1, 2};
  unsigned completed = 0;
  EXPECT_TRUE(detail::runReadWindows(windows, 1, [&](int, std::size_t) {
    EXPECT_EQ(std::this_thread::get_id(), caller);
    ++completed;
    return true;
  }));
  EXPECT_EQ(completed, windows.size());
}

TEST(XrdReadMemory, PipelineOverlapsWindowsAndDrainsOnFailure) {
  std::vector<int> windows{0, 1, 2, 3};
  std::atomic<unsigned> active{0}, peak{0}, completed{0};
  std::promise<void> overlapped;
  auto gate = overlapped.get_future().share();
  const auto read = [&](int window, std::size_t) {
    const auto n = ++active;
    auto old = peak.load();
    while (old < n && !peak.compare_exchange_weak(old, n)) {}
    if (n == 2) overlapped.set_value();
    EXPECT_EQ(gate.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    --active;
    ++completed;
    return window != 0;
  };
  EXPECT_FALSE(detail::runReadWindows(windows, 2, read));
  EXPECT_EQ(peak, 2U);
  EXPECT_EQ(active, 0U);
  EXPECT_EQ(completed, 2U);
}

TEST(XrdReadMemory, PipelineDrainsOtherWritersBeforeRethrowing) {
  std::vector<int> windows{0, 1};
  std::atomic<bool> written{false};
  EXPECT_THROW(detail::runReadWindows(windows, 2, [&](int i, std::size_t) {
    if (!i) throw std::runtime_error("read failed");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    written = true;
    return true;
  }), std::runtime_error);
  EXPECT_TRUE(written);
}

TEST(XrdReadMemory, BackgroundReadsLeaveDemandCapacityAndSiteSlot) {
  detail::ReadMemoryBudget budget;
  auto speculative = budget.tryReserve(48 * 1024 * 1024, true);
  ASSERT_TRUE(speculative);
  EXPECT_FALSE(budget.tryReserve(1, true));
  auto demand = budget.tryReserve(16 * 1024 * 1024);
  EXPECT_TRUE(demand);
  auto &monitor = SiteQualityMonitor::instance();
  const std::string site = "priority-reservation-test";
  EXPECT_TRUE(monitor.beginFullRead(site, true));
  EXPECT_FALSE(monitor.beginFullRead(site, true));
  EXPECT_TRUE(monitor.beginFullRead(site, false));
  EXPECT_FALSE(monitor.beginFullRead(site, false));
  monitor.endFullRead(site);
  monitor.endFullRead(site);
  EXPECT_FALSE(backgroundRead);
  { BackgroundReadScope scope;
    EXPECT_TRUE(backgroundRead);
    EXPECT_FALSE(std::async(std::launch::async, [] { return backgroundRead; }).get());
    { BackgroundReadScope nested; EXPECT_TRUE(backgroundRead); }
    EXPECT_TRUE(backgroundRead);
  }
  EXPECT_FALSE(backgroundRead);
}

TEST(XrdReadMemory, OutOfOrderWindowsPreserveEarliestHandoffData) {
  XrdSourceManager::PrefetchCache cache(8);
  const auto stamp = cache.reserveUses(4);
  cache.insert(0, std::make_shared<std::vector<char>>(4, 'a'), stamp);
  cache.insert(4, std::make_shared<std::vector<char>>(4, 'b'), stamp-1);
  cache.insert(8, std::make_shared<std::vector<char>>(4, 'c'), stamp-2);
  char bytes[8];
  ASSERT_TRUE(cache.copy(0, 8, bytes));
  EXPECT_EQ(std::string(bytes, 8), "aaaabbbb");
  EXPECT_EQ(cache.usedBytes, 8U);
}

TEST(XrdReadMemory, CompletedReadDoesNotLoseNotificationBeforeWait) {
  detail::ReadRaceState race;
  ASSERT_TRUE(race.complete(0, true, "local", "/notification-test", "file:///test", 1.));
  const auto start = std::chrono::steady_clock::now();
  race.waitForChange(std::chrono::seconds(1));
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(250));
}

TEST(XrdReadMemory, ConcurrentFileOpensShareTransportUntilLastOwnerReleasesIt) {
  char path[] = "/tmp/rdf-xrd-shared-test-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(write(fd, "shared transport", 16), 16);
  close(fd);
  auto cleanup = std::shared_ptr<const char>(path, [](const char *name) { unlink(name); });
  const auto url = "file://" + std::string(path);
  configureXrdAdaptor(0, 1024, 20, 0, 1, 5);
  const auto before = nextManagerId.load();
  std::vector<std::future<std::shared_ptr<XrdSourceManager>>> opens;
  for (unsigned i = 0; i < 8; ++i)
    opens.push_back(std::async(std::launch::async, [url] { return acquireXrdSourceManager(url); }));
  auto warm = opens.front().get();
  for (unsigned i = 1; i < opens.size(); ++i) {
    auto peer = opens[i].get();
    EXPECT_EQ(peer, warm);
  }
  EXPECT_EQ(nextManagerId.load(), before + 1);
  auto active = acquireXrdSourceManager(url);
  std::weak_ptr<XrdSourceManager> lifetime = warm;
  warm.reset();
  char bytes[16] = {};
  ASSERT_TRUE(active->readv({{0, 16, bytes}}, false));
  EXPECT_EQ(std::string(bytes, 16), "shared transport");
  EXPECT_FALSE(lifetime.expired());
  active.reset();
  EXPECT_TRUE(lifetime.expired());
  auto reopened = acquireXrdSourceManager(url);
  EXPECT_EQ(nextManagerId.load(), before + 2);
  reopened->stop();
  EXPECT_NE(acquireXrdSourceManager(url), reopened);
}

TEST(XrdReadMemory, FailedPooledOpenCanBeRetriedAndReconfigurationDoesNotReuseIt) {
  char path[] = "/tmp/rdf-xrd-retry-pool-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  unlink(path);
  const auto url = "file://" + std::string(path);
  configureXrdAdaptor(0, 1024, 20, 0, 1, 5);
  EXPECT_THROW(acquireXrdSourceManager(url), std::runtime_error);
  { std::ofstream file(path); file << "available"; }
  auto cleanup = std::shared_ptr<const char>(path, [](const char *name) { unlink(name); });
  auto first = acquireXrdSourceManager(url);
  configureXrdAdaptor(0, 2048, 20, 0, 1, 5);
  auto second = acquireXrdSourceManager(url);
  EXPECT_NE(first, second);
}

TEST(XrdReadMemory, BudgetIsSharedAndReleasedAfterLastOwner) {
  detail::ReadMemoryBudget budget;
  std::vector<std::unique_ptr<detail::ReadMemoryBudget::Reservation>> pending;
  for (unsigned i = 0; i < 8; ++i) {
    auto slot = budget.tryReserve(detail::ReadMemoryBudget::kWindow);
    ASSERT_TRUE(slot);
    pending.push_back(std::move(slot));
  }
  EXPECT_FALSE(budget.tryReserve(1));
  // Simulate a late losing callback releasing its copy on another thread.
  std::thread callback([slot = std::move(pending.back())]() mutable { slot.reset(); });
  callback.join();
  auto replacement = budget.tryReserve(detail::ReadMemoryBudget::kWindow);
  EXPECT_TRUE(replacement);
  EXPECT_FALSE(budget.tryReserve(1));
  pending.clear();
  replacement.reset();
  EXPECT_TRUE(budget.tryReserve(detail::ReadMemoryBudget::kLimit));
  EXPECT_FALSE(budget.tryReserve(detail::ReadMemoryBudget::kLimit + 1));
}

TEST(XrdReadMemory, PoorFileHistorySaturatesWithoutForgettingBlacklist) {
  auto &monitor = SiteQualityMonitor::instance();
  const std::string site = "memory-test-site";
  auto decision = monitor.observe(site, "/first", false, 0, 0);
  EXPECT_EQ(decision.poorFileCount, 1U);
  EXPECT_FALSE(decision.blacklisted);
  decision = monitor.observe(site, "/first", false, 0, 0);
  EXPECT_EQ(decision.poorFileCount, 1U);
  EXPECT_FALSE(decision.newlyPoorFile);
  decision = monitor.observe(site, "/second", false, 0, 0);
  EXPECT_TRUE(decision.newlyBlacklisted);
  for (unsigned i = 0; i < 10000; ++i) {
    decision = monitor.observe(site, "/later/" + std::to_string(i), false, 0, 0);
    ASSERT_EQ(decision.poorFileCount, 2U);
    ASSERT_TRUE(decision.blacklisted);
    ASSERT_FALSE(decision.newlyPoorFile);
  }
}

TEST(XrdReadMemory, CacheSharesWinnerAndEvictsWithinLimit) {
  XrdSourceManager::PrefetchCache cache(8);
  auto first = std::make_shared<std::vector<char>>(4, 'a');
  std::weak_ptr<std::vector<char>> lifetime = first;
  cache.insert(0, first);
  first.reset();
  EXPECT_FALSE(lifetime.expired());
  cache.insert(4, std::make_shared<std::vector<char>>(4, 'b'));
  char data[8] = {};
  ASSERT_TRUE(cache.copy(0, 8, data));
  EXPECT_EQ(std::string(data, 8), "aaaabbbb");
  cache.insert(8, std::make_shared<std::vector<char>>(4, 'c'));
  EXPECT_TRUE(lifetime.expired());
  EXPECT_LE(cache.usedBytes, cache.limit);
  EXPECT_FALSE(cache.copy(0, 8, data));
  EXPECT_EQ(std::string(data, 8), "aaaabbbb");
  cache.insert(4, std::make_shared<std::vector<char>>(4, 'd'));
  ASSERT_TRUE(cache.copy(4, 8, data));
  EXPECT_EQ(std::string(data, 8), "ddddcccc");
}

TEST(XrdReadMemory, TinyReadsDoNotAccumulateUnboundedCacheMetadata) {
  XrdSourceManager::PrefetchCache cache(1024 * 1024);
  for (std::uint64_t i = 0; i < 5000; ++i)
    cache.insert(i, std::make_shared<std::vector<char>>(1, 'x'));
  EXPECT_EQ(cache.entries.size(), kMaxPrefetchEntries);
  EXPECT_EQ(cache.usedBytes, kMaxPrefetchEntries);
  char byte = 0;
  EXPECT_FALSE(cache.copy(0, 1, &byte));
  EXPECT_TRUE(cache.copy(4999, 1, &byte));
  EXPECT_EQ(byte, 'x');
}

// Exercise real XrdCl callbacks without a server or grid credentials.
TEST(XrdReadMemory, LocalTransportWindowedDemandAndPrefetch) {
  char path[] = "/tmp/rdf-xrd-memory-test-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  auto cleanup = std::shared_ptr<const char>(path, [](const char *name) { unlink(name); });
  {
    std::ofstream output(path, std::ios::binary);
    std::vector<char> block(251 * 4096);
    for (std::size_t i = 0; i < block.size(); ++i) block[i] = i % 251;
    for (unsigned i = 0; i < 26; ++i) output.write(block.data(), block.size());
    ASSERT_TRUE(output.good());
  }
  const std::string url = "file://" + std::string(path);
  configureXrdAdaptor(0, 16 * 1024 * 1024, 20, 0, 1, 5);
  auto reader = makeXrdSourceManager(url);
  constexpr std::size_t length = 24 * 1024 * 1024 + 137;
  std::vector<char> bytes(length);
  ASSERT_TRUE(reader->readv({{173, static_cast<std::uint32_t>(length), bytes.data()}}, false));
  for (std::size_t i = 0; i < length; ++i)
    ASSERT_EQ(static_cast<unsigned char>(bytes[i]), (i + 173) % 251) << i;
  // Warmup demand reads, as well as explicit prefetches, retain their first
  // window when the total request is larger than the handoff cache.
  auto cache = acquireCache(url, 16 * 1024 * 1024);
  ASSERT_TRUE(cache->copy(173, 8 * 1024 * 1024, bytes.data()));
  ASSERT_TRUE(reader->readv({{17, static_cast<std::uint32_t>(length), nullptr}}, true));
  ASSERT_TRUE(cache->copy(17, 8 * 1024 * 1024, bytes.data()));
  auto handoff = makeXrdSourceManager(url);
  std::fill(bytes.begin(), bytes.end(), 0);
  ASSERT_TRUE(handoff->readv({{17, static_cast<std::uint32_t>(length), bytes.data()}}, false));
  for (std::size_t i = 0; i < length; ++i)
    ASSERT_EQ(static_cast<unsigned char>(bytes[i]), (i + 17) % 251) << i;
  char tiny[11];
  ASSERT_TRUE(reader->readv({{4097, 3, tiny}, {127, 0, nullptr}, {31, 8, tiny + 3}}, false));
  for (unsigned i = 0; i < 3; ++i) EXPECT_EQ(static_cast<unsigned char>(tiny[i]), (4097 + i) % 251);
  for (unsigned i = 0; i < 8; ++i) EXPECT_EQ(static_cast<unsigned char>(tiny[i + 3]), (31 + i) % 251);
  EXPECT_FALSE(reader->readv({{0, 1, nullptr}}, false));
  EXPECT_FALSE(reader->readv({{std::numeric_limits<std::uint64_t>::max(), 2, tiny}}, false));

  std::unique_ptr<detail::ReadMemoryBudget::Reservation> reservation;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!(reservation = readMemoryBudget().tryReserve(detail::ReadMemoryBudget::kLimit)) &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_TRUE(reservation);
  auto waitingReader = std::async(std::launch::async, [&] {
    char target[11];
    return handoff->readv({{25 * 1024 * 1024, 11, target}}, false);
  });
  EXPECT_EQ(waitingReader.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
  reservation.reset();
  EXPECT_TRUE(waitingReader.get());
  reader->stop();
  EXPECT_FALSE(reader->readv({{0, 1, tiny}}, false));
  EXPECT_TRUE(handoff->readv({{0, 1, tiny}}, false));

  const auto closedUrl = url + "?closed=1";
  auto closedReader = makeXrdSourceManager(closedUrl);
  EXPECT_TRUE(closedReader->readv({{0, 1, tiny}}, false));
  std::weak_ptr<XrdSourceManager::PrefetchCache> closedCache =
      acquireCache(closedUrl, 16 * 1024 * 1024);
  EXPECT_FALSE(closedCache.expired());
  closedReader->stop();
  EXPECT_TRUE(closedCache.expired());
}
} // namespace rdf::xrd

namespace rdf::xrd {
TEST(XrdInitialOpen, RankUsesHistoryAndLiveScoresAndDiversifiesSites) {
  std::vector<detail::InitialCandidate> candidates{
      {"root://unknown//f", "unknown", {}},
      {"root://slow//f", "slow", {500, false, false, true}},
      {"root://fast-a//f", "fast", {20, false, false, true}},
      {"root://fast-b//f", "fast", {20, false, false, true}},
      {"root://live//f", "live", {40, true, true, true}}};
  auto ranked = detail::rankInitial(std::move(candidates));
  ASSERT_EQ(ranked.size(), 4U);
  EXPECT_EQ(ranked[0].site, "live");
  EXPECT_EQ(ranked[1].site, "fast");
  EXPECT_EQ(ranked[2].site, "unknown");
  EXPECT_EQ(ranked[3].site, "slow");
}

TEST(XrdInitialOpen, LocateDeduplicatesAndSkipsOfflineReplicasPreservingUrlParameters) {
  detail::InitialLocations state("roots://user@redirector//store/f?authz=token");
  XrdCl::LocationInfo locations;
  using L = XrdCl::LocationInfo;
  locations.Add(L::Location("server:1095", L::ServerOnline, L::Read));
  locations.Add(L::Location("server:1095", L::ServerOnline, L::Read));
  locations.Add(L::Location("offline:1094", L::ServerPending, L::Read));
  locations.Add(L::Location("manager:1094", L::ManagerOnline, L::Read));
  state.add("roots://user@redirector//store/f?authz=token", locations);
  ASSERT_EQ(state.servers.size(), 1U);
  const XrdCl::URL endpoint(state.servers[0]);
  EXPECT_EQ(endpoint.GetHostName(), "server");
  EXPECT_EQ(endpoint.GetProtocol(), "roots");
  EXPECT_EQ(endpoint.GetUserName(), "user");
  EXPECT_EQ(endpoint.GetPort(), 1095);
  EXPECT_EQ(endpoint.GetPathWithParams(), "/store/f?authz=token");
  EXPECT_EQ(state.managers.size(), 2U);
  for (unsigned i = 0; i < 600; ++i) {
    XrdCl::LocationInfo extra;
    extra.Add(L::Location("node" + std::to_string(i) + ":1094", L::ServerOnline, L::Read));
    extra.Add(L::Location("manager" + std::to_string(i) + ":1094", L::ManagerOnline, L::Read));
    state.add("root://redirector//f", extra);
  }
  EXPECT_EQ(state.servers.size(), 256U);
  EXPECT_EQ(state.seenManagers.size(), 32U);
}

TEST(XrdInitialOpen, BackupWinsWithoutWaitingForStalledFirstOpenOrLateCallback) {
  std::shared_ptr<detail::InitialOpenState> late;
  const auto before = std::chrono::steady_clock::now();
  auto result = detail::openInitialCandidates({{"first", "a", {}}, {"backup", "b", {}}}, 1, {},
      [&](auto state, const auto &candidate, unsigned attempt) {
        if (attempt == 1) late = state;
        else state->complete(std::make_shared<XrdCl::File>(), true, 123, candidate.url);
      });
  ASSERT_TRUE(result.file);
  EXPECT_EQ(result.candidate, "backup");
  EXPECT_EQ(result.size, 123U);
  EXPECT_LT(std::chrono::steady_clock::now()-before, std::chrono::milliseconds(700));
  ASSERT_TRUE(late);
  EXPECT_TRUE(late->stopped);
  late->complete(std::make_shared<XrdCl::File>(), true, 456, "first");
  EXPECT_FALSE(late->winner.file);
  EXPECT_EQ(late->pending, 0U);
}

TEST(XrdInitialOpen, CallerDeadlineAndProcessBudgetBoundUnresponsiveTransports) {
  std::shared_ptr<detail::InitialOpenState> late;
  unsigned launches = 0;
  const auto before = std::chrono::steady_clock::now();
  auto result = detail::openInitialCandidates({{"a", "a", {}}, {"b", "b", {}}, {"c", "c", {}}}, 1, {},
      [&](auto state, const auto &, unsigned) { late = state; ++launches; });
  EXPECT_FALSE(result.file);
  EXPECT_EQ(launches, 2U);
  EXPECT_LT(std::chrono::steady_clock::now()-before, std::chrono::seconds(2));
  late->complete({}, false, 0); late->complete({}, false, 0);
  std::vector<std::unique_ptr<detail::InitialOperation>> owned;
  for (unsigned i=0; i<32; ++i) {
    owned.push_back(std::make_unique<detail::InitialOperation>());
    EXPECT_TRUE(owned.back()->acquired);
  }
  EXPECT_FALSE(detail::InitialOperation().acquired);
  owned.clear();
  EXPECT_EQ(detail::initialOperations.load(), 0U);
}

TEST(XrdInitialOpen, RealLocalCallbacksStatWinnerAndRecoverFromMissingCandidate) {
  char path[] = "/tmp/rdf-xrd-initial-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(write(fd, "payload", 7), 7);
  close(fd);
  auto cleanup = std::shared_ptr<const char>(path, [](const char *p) { unlink(p); });
  const std::string url = "file://" + std::string(path);
  auto result = detail::openInitialCandidates({{"file:///rdf-no-such-initial-file", "missing", {}}, {url, "local", {}}}, 2,
      [](const auto &candidate, unsigned attempt) { return openTrace(candidate, 991, attempt, "ranked-primary"); });
  ASSERT_TRUE(result.file);
  EXPECT_EQ(result.size, 7U);
  EXPECT_EQ(result.candidate, url);
  closeAsync(std::move(result.file));
}
} // namespace rdf::xrd

namespace rdf::xrd {
TEST(XrdEndpointCooldown, EndpointIdentityExpiryRecoveryAndBoundedHistory) {
  using C = detail::EndpointCooldown;
  C cooldown(10);
  const auto now = C::Clock::now();
  const std::string loser = "root://user:password@STORAGE.example:1095//file?authz=secret";
  EXPECT_EQ(detail::cooldownKey(loser), "storage.example:1095");
  cooldown.beaten({loser}, {"winner.example:1094"}, now);
  EXPECT_TRUE(cooldown.active("root://storage.example:1095//different-file",now));
  EXPECT_FALSE(cooldown.active("storage.example:1094",now));
  EXPECT_FALSE(cooldown.active("other.example:1095",now));
  // A late loser callback does not prolong its original cooldown.
  cooldown.beaten({loser},{"winner.example:1094"},now+std::chrono::seconds(9));
  EXPECT_FALSE(cooldown.active(loser,now+std::chrono::seconds(10)));
  cooldown.beaten({loser},{"winner.example:1094"},now+std::chrono::seconds(11));
  cooldown.beaten({"winner.example:1094"},{loser},now+std::chrono::seconds(12));
  EXPECT_FALSE(cooldown.active(loser,now+std::chrono::seconds(12)));
  EXPECT_TRUE(cooldown.active("winner.example:1094",now+std::chrono::seconds(12)));
  for (unsigned i=0;i<700;++i) cooldown.beaten({"node"+std::to_string(i)+":1094"},{},now);
  EXPECT_EQ(cooldown.hosts(now).size(),512U);
  EXPECT_TRUE(cooldown.hosts(now+std::chrono::seconds(30)).empty());
  C disabled(0);
  disabled.beaten({loser},{},now);
  EXPECT_FALSE(disabled.active(loser,now));
}

TEST(XrdEndpointCooldown, HealthyAlternativesWinButSoleCooledEndpointRemainsUsable) {
  detail::EndpointCooldown cooldown;
  cooldown.beaten({"cold.example:1094"},{"healthy.example:1094"});
  const auto cooling = [&](const std::string &url) { return cooldown.active(url); };
  auto selected = detail::cooldownChoices(std::vector<std::string>{"cold.example:1094","healthy.example:1094"},cooling);
  ASSERT_EQ(selected.size(),1U);
  EXPECT_EQ(selected.front(),"healthy.example:1094");
  selected = detail::cooldownChoices(std::vector<std::string>{"cold.example:1094"},cooling);
  ASSERT_EQ(selected.size(),1U);
  EXPECT_EQ(selected.front(),"cold.example:1094");
}

TEST(XrdEndpointCooldown, OpenLoserIsSkippedImmediatelyThenRetriedWhenAlternativesFail) {
  const std::string cold="root://cooldown-open-loser.example//file";
  const std::string healthy="root://cooldown-open-winner.example//file";
  const std::string backend="root://cooldown-open-backend.example:1095//file";
  std::shared_ptr<detail::InitialOpenState> late;
  const std::vector<detail::InitialCandidate> candidates{{cold,"cold",{}},{healthy,"healthy",{}}};
  auto first=detail::openInitialCandidates(candidates,1,{},[&](auto state,const auto &c,unsigned i) {
    if (i==1) {
      late=state;
      std::lock_guard<std::mutex> lock(state->mutex);
      state->attemptedEndpoints[cold]=backend;
    } else state->complete(std::make_shared<XrdCl::File>(),true,7,c.url);
  });
  ASSERT_TRUE(first.file);
  EXPECT_TRUE(detail::endpointCooldown().active(cold));
  EXPECT_TRUE(detail::endpointCooldown().active(backend));
  std::vector<std::string> launched;
  auto second=detail::openInitialCandidates(candidates,1,{},[&](auto state,const auto &c,unsigned) {
    launched.push_back(c.url);
    state->complete(std::make_shared<XrdCl::File>(),true,7,c.url);
  });
  ASSERT_EQ(launched.size(),1U);
  EXPECT_EQ(launched[0],healthy);
  // The healthy connection now fails: cooldown cannot cause an availability failure.
  launched.clear();
  auto recovered=detail::openInitialCandidates(candidates,1,{},[&](auto state,const auto &c,unsigned) {
    launched.push_back(c.url);
    state->complete(c.url==cold ? std::make_shared<XrdCl::File>() : nullptr,c.url==cold,7,c.url);
  });
  ASSERT_TRUE(recovered.file);
  ASSERT_EQ(launched.size(),2U);
  EXPECT_EQ(launched[0],healthy);
  EXPECT_EQ(launched[1],cold);
  EXPECT_FALSE(detail::endpointCooldown().active(cold));
  late->complete({},false,0,cold);
}

TEST(XrdEndpointCooldown, FullReadWinnerCoolsPendingLoserAndSoleSourceCanRecover) {
  const std::string slow="cooldown-read-slow.example:1094";
  const std::string fast="cooldown-read-fast.example:1094";
  detail::ReadRaceState race;
  ASSERT_TRUE(race.registerEndpoints(0,{slow}));
  ASSERT_TRUE(race.registerEndpoints(1,{fast}));
  EXPECT_TRUE(race.complete(1,true,"fast","/f","root://file//f",10));
  EXPECT_TRUE(detail::endpointCooldown().active(slow)); // Before its callback.
  EXPECT_FALSE(race.registerEndpoints(2,{"late-copy.example:1094"}));
  EXPECT_FALSE(race.complete(0,true,"slow","/f","root://file//f",100));
  detail::ReadRaceState only;
  ASSERT_TRUE(only.registerEndpoints(0,{slow}));
  EXPECT_TRUE(only.complete(0,true,"slow","/f","root://file//f",10));
  EXPECT_FALSE(detail::endpointCooldown().active(slow));
}
} // namespace rdf::xrd
