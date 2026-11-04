#include "XrdOpenRetry.h"
#include <gtest/gtest.h>

using rdf::xrd::detail::initialOpenUrls;
using rdf::xrd::detail::openWithRetry;

TEST(XrdOpenRetry, RetriesPrivateAndNonCMSPathsOnlyAtOriginalEndpoint) {
  for (const auto *url : {"root://private//store/user/test/file.root",
                          "root://private//data/file.root"}) {
    EXPECT_EQ(initialOpenUrls(url), (std::vector<std::string>{url, url, url}));
  }
}

TEST(XrdOpenRetry, StopsAfterSuccessWithoutWaitingAgain) {
  std::vector<std::string> attempted;
  std::vector<long> delays;
  const bool ok = openWithRetry("root://cms-xrd-global.cern.ch//store/data/test.root",
      [&](const std::string &url, std::size_t) {
        attempted.push_back(url);
        return attempted.size() == 2;
      }, [&](std::chrono::seconds delay) { delays.push_back(delay.count()); });
  EXPECT_TRUE(ok);
  ASSERT_EQ(attempted.size(), 2U);
#ifdef RDF_XRD_OPEN_POLICY_HEADER
  EXPECT_NE(attempted[0], attempted[1]);
#else
  EXPECT_EQ(attempted[0], attempted[1]);
#endif
  EXPECT_EQ(delays, (std::vector<long>{1}));
}

TEST(XrdOpenRetry, ExhaustsThreeAttemptsWithBoundedBackoff) {
  unsigned attempts = 0;
  std::vector<long> delays;
  EXPECT_FALSE(openWithRetry("root://private//file.root",
      [&](const std::string &, std::size_t attempt) {
        EXPECT_EQ(attempt, ++attempts);
        return false;
      }, [&](std::chrono::seconds delay) { delays.push_back(delay.count()); }));
  EXPECT_EQ(attempts, 3U);
  EXPECT_EQ(delays, (std::vector<long>{1, 2}));
}
