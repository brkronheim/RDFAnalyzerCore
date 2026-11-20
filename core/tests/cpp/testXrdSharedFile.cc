#include <XrdTFile.h>
#include <TFile.h>
#include <TObjString.h>
#include <TROOT.h>
#include <gtest/gtest.h>
#include <future>
#include <unistd.h>

TEST(XrdSharedFile, ClosingWarmFileKeepsActiveFileReadableAndCursorsIndependent) {
  ROOT::EnableThreadSafety();
  char path[] = "/tmp/rdf-xrd-shared-root-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  auto cleanup = std::shared_ptr<const char>(path, [](const char *p) { unlink(p); });
  {
    TFile file(path, "RECREATE");
    TObjString("expected ROOT object").Write("payload");
  }
  const auto url = "file://" + std::string(path);
  rdf::xrd::configureXrdAdaptor(0, 1024 * 1024, 20, 0, 1, 5);
  auto warm = std::make_unique<RDFXrdFile>(url.c_str(), "READ", "", 1, 0);
  ASSERT_FALSE(warm->IsZombie());
  RDFXrdFile active(url.c_str(), "READ", "", 1, 0);
  ASSERT_FALSE(active.IsZombie());
  char expected[4], actual[4];
  ASSERT_FALSE(warm->ReadBuffer(expected, 0, 4));
  ASSERT_FALSE(active.ReadBuffer(actual, 0, 4));
  EXPECT_EQ(std::string(expected, 4), std::string(actual, 4));
  char expectedNext[4], unrelated[4];
  ASSERT_FALSE(warm->ReadBuffer(expectedNext, 4, 4));
  ASSERT_FALSE(warm->ReadBuffer(unrelated, 32, 4));
  ASSERT_FALSE(active.ReadBuffer(actual, 4));
  EXPECT_EQ(std::string(expectedNext, 4), std::string(actual, 4));
  warm->Close();
  EXPECT_FALSE(warm->IsOpen());
  EXPECT_EQ(warm->GetSize(), 0);
  EXPECT_TRUE(active.IsOpen());
  auto *object = dynamic_cast<TObjString *>(active.Get("payload"));
  ASSERT_NE(object, nullptr);
  EXPECT_EQ(object->GetString(), "expected ROOT object");
  active.Close();
  EXPECT_FALSE(active.IsOpen());
  RDFXrdFile reopened(url.c_str(), "READ", "", 1, 0);
  ASSERT_NE(reopened.Get("payload"), nullptr);
}
