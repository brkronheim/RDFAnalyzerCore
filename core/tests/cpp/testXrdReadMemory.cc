// Compile the source manager here to exercise its internal cache and site
// bookkeeping without adding test-only API to the reader shared library.
#include "XrdSourceManager.cc"
#include <gtest/gtest.h>
#include <thread>
#include <fstream>
#include <future>
#include <unistd.h>

namespace rdf::xrd {
TEST(XrdReadMemory, CompletedReadDoesNotLoseNotificationBeforeWait) {
  ReadRaceState race;
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
