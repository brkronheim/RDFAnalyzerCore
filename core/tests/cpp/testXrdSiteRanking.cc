#include "XrdSiteRanking.h"
#include "XrdOpenTrace.h"
#include "XrdSourceManager.h"
#include <ConfigurationManager.h>
#include <util.h>
#include <TChain.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <unistd.h>

using namespace rdf::xrd::detail;

TEST(XrdSiteRanking, OriginAliasesExpireAndNeverCrossSites) {
  std::istringstream input("# rdf-xrd-site-rankings-v1\n"
      "alias worker-a cluster-a\nendpoint storage.example t2_example\n"
      "site cluster-a t2_example 40 20 100000\n"
      "site cluster-b t2_example 200 10 100000\n");
  const auto ranking = SiteRanking::read(input);
  EXPECT_DOUBLE_EQ(*ranking.quality("WORKER-A", "T2_EXAMPLE", 100000, 1000), 40);
  EXPECT_DOUBLE_EQ(*ranking.quality("cluster-b", "t2_example", 100000, 1000), 200);
  EXPECT_FALSE(ranking.quality("cluster-c", "t2_example", 100000, 1000));
  EXPECT_FALSE(ranking.quality("cluster-a", "t2_example", 101001, 1000));
  EXPECT_FALSE(ranking.quality("cluster-a", "t2_example", 99000, 1000));
  EXPECT_DOUBLE_EQ(*ranking.quality("cluster-a", "t2_example", 100500, 1000), 150);
  EXPECT_EQ(ranking.siteForEndpoint("STORAGE.EXAMPLE"), "t2_example");
}

TEST(XrdSiteRanking, RejectsMalformedSnapshotsInsteadOfPartiallyLoading) {
  for (const auto &body : {"site a b nan 5 100", "site a b 20 -1 100", "site a b 20 1 -100",
                          "site a b 20 1 100\nsite a b 30 1 100", "alias worker a\nalias worker b"}) {
    std::istringstream input(std::string("# rdf-xrd-site-rankings-v1\n") + body);
    EXPECT_THROW(SiteRanking::read(input), std::runtime_error) << body;
  }
}

TEST(XrdSiteRanking, ColdOrderingUsesPriorButLiveMeasurementsWin) {
  SiteRank slow{500, false, false, true}, fast{40, false, false, true}, unknown;
  EXPECT_TRUE(siteRankBefore(fast, slow));
  EXPECT_TRUE(siteRankBefore(fast, unknown));
  EXPECT_FALSE(siteRankBefore(unknown, fast));
  EXPECT_TRUE(siteRankBefore(unknown, slow));
  EXPECT_TRUE(siteRankBefore(SiteRank{80, false, true, false}, fast));
  EXPECT_DOUBLE_EQ(initialSiteQuality(40, 260, 0), 40);
  EXPECT_DOUBLE_EQ(initialSiteQuality(40, 100, 1), 70);
  EXPECT_DOUBLE_EQ(initialSiteQuality(40, 100, 3), 100);
  EXPECT_DOUBLE_EQ(initialSiteQuality({}, 100, 0), 100);
}

TEST(XrdOpenTrace, LogsBeginAndFailedStorageHostWithoutCallingItTheRedirector) {
  std::vector<std::string> lines;
  OpenTrace trace{"origin", "/store/test.root", "root://user:password@redirector//store/test.root?authz=secret", "primary", 3, 1,
      [&](const std::string &line) { lines.push_back(line); },
      [](const std::string &host) { return "dns:" + host; }};
  trace.begin();
  ASSERT_EQ(lines.size(), 1U);
  EXPECT_NE(lines[0].find("open-start"), std::string::npos);
  EXPECT_NE(lines[0].find("server=redirector:1094"), std::string::npos);
  XrdCl::HostList hosts;
  hosts.emplace_back(XrdCl::URL("root://redirector//")); hosts.back().flags = kXR_isManager;
  hosts.emplace_back(XrdCl::URL("root://user:password@bad-storage.example:1095//")); hosts.back().flags = kXR_isServer;
  XrdCl::File unopened;
  trace.result(unopened, XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errOperationExpired), &hosts);
  EXPECT_NE(lines.back().find("server=bad-storage.example:1095"), std::string::npos);
  EXPECT_NE(lines.back().find("endpoint_role=storage status=failed"), std::string::npos);
  for (const auto &line : lines) {
    EXPECT_EQ(line.find("secret"), std::string::npos);
    EXPECT_EQ(line.find("password"), std::string::npos);
  }
}

TEST(XrdOpenTrace, LocalFailedOpenProducesMatchedStartAndResult) {
  std::vector<std::string> lines;
  XrdCl::File file;
  OpenTrace trace{"origin", "/rdf-test-no-such-file", "file:///rdf-test-no-such-file", "primary", 1, 1,
      [&](const std::string &line) { lines.push_back(line); },
      [](const std::string &) { return "unknown-site"; }};
  const auto status = tracedOpen(file, trace, 1);
  EXPECT_FALSE(status.IsOK());
  ASSERT_GE(lines.size(), 2U);
  EXPECT_NE(lines.front().find("open-start"), std::string::npos);
  EXPECT_NE(lines.back().find("status=failed"), std::string::npos);
}

TEST(XrdSiteRanking, AnalysisConfigurationLoadsSnapshotBeforeInputDiscovery) {
  char directory[] = "/tmp/rdf-xrd-ranking-config-XXXXXX";
  ASSERT_NE(mkdtemp(directory), nullptr);
  const std::filesystem::path dir(directory);
  auto cleanup = std::shared_ptr<const char>(directory, [](const char *p) { std::filesystem::remove_all(p); });
  const auto snapshot = dir / "sites.tsv";
  const auto config = dir / "analysis.txt";
  {
    std::ofstream stream(config);
    stream << "threads=1\ntreeList=Events\ndirectory=" << (dir / "no-input").string()
           << "\nrootInputBackend=xrd-adaptor\nxrdSiteOrigin=configuration-origin\n"
           << "xrdSiteRankingMaxAgeDays=0\nxrdSiteRankingFile=" << snapshot.string() << '\n';
  }
  ConfigurationManager missing(config.string());
  EXPECT_THROW(makeTChain(missing), std::runtime_error);
  {
    std::ofstream stream(snapshot);
    stream << "# rdf-xrd-site-rankings-v1\nsite configuration-origin site 40 5 100000\n";
  }
  ConfigurationManager valid(config.string());
  testing::internal::CaptureStdout();
  auto chains = makeTChain(valid);
  const auto output = testing::internal::GetCapturedStdout();
  ASSERT_EQ(chains.size(), 1U);
  EXPECT_NE(output.find("context origin=configuration-origin"), std::string::npos);
  EXPECT_NE(output.find(snapshot.string()), std::string::npos);
  rdf::xrd::configureXrdSiteRanking("none");
}
