/**
 * @file testRootOutputSink.cc
 * @brief Unit tests for the RootOutputSink class, including glob pattern support
 * @date 2025
 */

#include <gtest/gtest.h>

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <RootOutputSink.h>
#include <SystematicManager.h>

#include <TFile.h>
#include <TTree.h>

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace {

/// Write a minimal saveConfig file listing the given patterns (one per line)
void writeSaveConfigFile(const std::string& path,
                         const std::vector<std::string>& patterns) {
  std::ofstream out(path);
  for (const auto& p : patterns) {
    out << p << "\n";
  }
}

/// Write a minimal analysis config file
void writeAnalysisConfig(const std::string& path,
                         const std::string& saveFile,
                         const std::string& saveConfigFile) {
  std::ofstream out(path);
  out << "saveTree=Events\n";
  out << "saveFile=" << saveFile << "\n";
  out << "saveConfig=" << saveConfigFile << "\n";
}

} // namespace

class RootOutputSinkTest : public ::testing::Test {
protected:
  std::string cfgPath;
  std::string saveConfigPath;
  std::string outputPath;

  void SetUp() override {
    const std::string base = std::string(TEST_SOURCE_DIR) + "/aux/root_output_sink_test_";
    cfgPath        = base + "config.txt";
    saveConfigPath = base + "saveconfig.txt";
    outputPath     = base + "output.root";
    cleanup();
  }

  void TearDown() override { cleanup(); }

  void cleanup() {
    std::remove(cfgPath.c_str());
    std::remove(saveConfigPath.c_str());
    std::remove(outputPath.c_str());
  }

  /// Build an in-memory DataManager with columns Electron_pt, Electron_eta, Muon_pt
  std::unique_ptr<DataManager> makeDataManager() {
    auto dm = std::make_unique<DataManager>(5);
    SystematicManager sm;
    dm->Define("Electron_pt",  []() { return 30.0f; }, {}, sm);
    dm->Define("Electron_eta", []() { return 1.5f;  }, {}, sm);
    dm->Define("Muon_pt",      []() { return 20.0f; }, {}, sm);
    return dm;
  }
};

/// Exact column names (no glob) must still be written – backward compatibility
TEST_F(RootOutputSinkTest, ExactColumnNamesAreWritten) {
  writeSaveConfigFile(saveConfigPath, {"Electron_pt", "Muon_pt"});
  writeAnalysisConfig(cfgPath, outputPath, saveConfigPath);

  ConfigurationManager config(cfgPath);
  auto dm = makeDataManager();
  SystematicManager sm;

  RootOutputSink sink;
  auto df = dm->getDataFrame();
  ASSERT_NO_THROW(
      sink.writeDataFrame(df, config, dm.get(), &sm, OutputChannel::Skim));

  TFile f(outputPath.c_str(), "READ");
  ASSERT_FALSE(f.IsZombie());
  auto* tree = dynamic_cast<TTree*>(f.Get("Events"));
  ASSERT_NE(tree, nullptr);
  EXPECT_NE(tree->GetBranch("Electron_pt"),  nullptr);
  EXPECT_NE(tree->GetBranch("Muon_pt"),      nullptr);
  EXPECT_EQ(tree->GetBranch("Electron_eta"), nullptr); // not requested
  f.Close();
}

/// A glob pattern like Electron_* must expand to all matching columns
TEST_F(RootOutputSinkTest, GlobPatternExpandsMatchingColumns) {
  writeSaveConfigFile(saveConfigPath, {"Electron_*"});
  writeAnalysisConfig(cfgPath, outputPath, saveConfigPath);

  ConfigurationManager config(cfgPath);
  auto dm = makeDataManager();
  SystematicManager sm;

  RootOutputSink sink;
  auto df = dm->getDataFrame();
  ASSERT_NO_THROW(
      sink.writeDataFrame(df, config, dm.get(), &sm, OutputChannel::Skim));

  TFile f(outputPath.c_str(), "READ");
  ASSERT_FALSE(f.IsZombie());
  auto* tree = dynamic_cast<TTree*>(f.Get("Events"));
  ASSERT_NE(tree, nullptr);
  EXPECT_NE(tree->GetBranch("Electron_pt"),  nullptr);
  EXPECT_NE(tree->GetBranch("Electron_eta"), nullptr);
  EXPECT_EQ(tree->GetBranch("Muon_pt"),      nullptr); // not matched
  f.Close();
}

/// A saveConfig with both exact names and glob patterns works correctly
TEST_F(RootOutputSinkTest, MixedExactAndGlobPatterns) {
  writeSaveConfigFile(saveConfigPath, {"Muon_pt", "Electron_*"});
  writeAnalysisConfig(cfgPath, outputPath, saveConfigPath);

  ConfigurationManager config(cfgPath);
  auto dm = makeDataManager();
  SystematicManager sm;

  RootOutputSink sink;
  auto df = dm->getDataFrame();
  ASSERT_NO_THROW(
      sink.writeDataFrame(df, config, dm.get(), &sm, OutputChannel::Skim));

  TFile f(outputPath.c_str(), "READ");
  ASSERT_FALSE(f.IsZombie());
  auto* tree = dynamic_cast<TTree*>(f.Get("Events"));
  ASSERT_NE(tree, nullptr);
  EXPECT_NE(tree->GetBranch("Electron_pt"),  nullptr);
  EXPECT_NE(tree->GetBranch("Electron_eta"), nullptr);
  EXPECT_NE(tree->GetBranch("Muon_pt"),      nullptr);
  f.Close();
}

/// A glob pattern that matches nothing does not throw
TEST_F(RootOutputSinkTest, GlobPatternWithNoMatchDoesNotThrow) {
  writeSaveConfigFile(saveConfigPath, {"Jet_*"});
  writeAnalysisConfig(cfgPath, outputPath, saveConfigPath);

  ConfigurationManager config(cfgPath);
  auto dm = makeDataManager();
  SystematicManager sm;

  RootOutputSink sink;
  auto df = dm->getDataFrame();
  // When the glob matches nothing, columns is empty so all columns are snapshotted.
  ASSERT_NO_THROW(
      sink.writeDataFrame(df, config, dm.get(), &sm, OutputChannel::Skim));
}
