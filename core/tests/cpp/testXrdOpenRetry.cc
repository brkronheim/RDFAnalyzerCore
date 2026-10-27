#include "XrdOpenRetry.h"
#include <gtest/gtest.h>

using rdf::xrd::detail::initialOpenUrls;
using rdf::xrd::detail::openWithRetry;

TEST(XrdOpenRetry, UsesAlternateCMSRedirectorsForSameFile) {
  const auto urls = initialOpenUrls("root://cms-xrd-global.cern.ch//store/mc/test.root?token=value");
  ASSERT_EQ(urls.size(), 3U);
  EXPECT_EQ(urls.front(), "root://cms-xrd-global.cern.ch//store/mc/test.root?token=value");
  EXPECT_EQ(XrdCl::URL(urls[1]).GetHostName(), "cmsxrootd.fnal.gov");
  EXPECT_EQ(XrdCl::URL(urls[2]).GetHostName(), "xrootd-cms.infn.it");
  for (const auto &url : urls) {
    EXPECT_EQ(XrdCl::URL(url).GetPath(), "/store/mc/test.root");
    EXPECT_EQ(XrdCl::URL(url).GetParams().at("token"), "value");
  }
}

TEST(XrdOpenRetry, RemovesSiteRoutingPrefixForFallback) {
  const auto urls = initialOpenUrls("root://xrootd-cms.infn.it//store/test/xrootd/T2_DE_DESY/store/mc/test.root?tried=bad&triedrc=resel");
  EXPECT_NE(urls.front().find("T2_DE_DESY"), std::string::npos);
  for (std::size_t i = 1; i < urls.size(); ++i) {
    EXPECT_EQ(XrdCl::URL(urls[i]).GetPath(), "/store/mc/test.root");
    EXPECT_EQ(XrdCl::URL(urls[i]).GetParams().count("tried"), 0U);
    EXPECT_EQ(XrdCl::URL(urls[i]).GetParams().count("triedrc"), 0U);
  }
}

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
  EXPECT_NE(attempted[0], attempted[1]);
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
