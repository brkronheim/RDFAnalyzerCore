#include "test_workspace.h"
#include "stxs/Manager.h"
#include "StxsColumns.h"
#include <gtest/gtest.h>
#include <TFile.h>
#include <TH1D.h>
#include <filesystem>
#include <fstream>

namespace {
class StxsTest : public ::testing::Test {
protected:
  std::filesystem::path dir;
  void SetUp() override {
    dir = testWorkspacePath() / "stxs-generic-test";
    std::filesystem::create_directories(dir);
  }
  void TearDown() override { std::filesystem::remove_all(dir); }
};

TEST_F(StxsTest, LoadYamlBinAndMigrationTables) {
  auto path = dir / "bins.yaml";
  std::ofstream(path) << "- analysis_bin: 3\n  label: signal region\n  fine_categories: [71, 72]\n";
  auto config = stxs::loadConfig(path.string(), dir.string(), "coarse", "fine", true);
  EXPECT_EQ(config.analysisBinLabels.at(3), "signal region");
  EXPECT_EQ(config.fineCategoryToAnalysisBin.at(72), 3);
  auto migration = dir / "migration.yaml";
  std::ofstream(migration) << "- {fine_category: 71, pt_bin: 1, jet_pt_bin: 2, scale_bin: 3, acceptance_fallback: true}\n";
  stxs::loadMigrationBinConfig(migration.string(), config);
  EXPECT_TRUE(config.migrationBins.at(71).useAcceptanceFallback);
  std::ofstream(migration) << "- {fine_category: 71, pt_bin: 1}\n";
  EXPECT_THROW(stxs::loadMigrationBinConfig(migration.string(), config), std::runtime_error);
  EXPECT_EQ(config.migrationBins.at(71).jetPtBin, 2);
}

TEST_F(StxsTest, GenericCategoryMappingAndConfiguredFallback) {
  {
    TFile file((dir / "migration.root").c_str(), "RECREATE");
    TH1D delta("delta", "", 4, 0, 4);
    delta.SetBinContent(2, 0.2);
    delta.Write();
    TH1D scale("scale", "", 4, 0, 4);
    scale.SetBinContent(3, 2.0);
    scale.Write();
  }
  stxs::Config cfg;
  cfg.enabled = true;
  cfg.auxDirectory = dir.string();
  // Arbitrary IDs outside the former analysis-specific category range.
  cfg.migrationBins[8107] = {2, 1, 3, false};
  cfg.migrationBins[9201] = {2, 1, 3, true};
  stxs::MigrationSpec spec;
  spec.productionModeId = 77;
  spec.delta75File = "migration.root";
  spec.delta75Histogram = "delta";
  spec.scaleUpFile = "migration.root";
  spec.scaleUpHistogram = "scale";
  cfg.migrationSpecs.push_back(spec);
  stxs::Manager manager(cfg);
  auto weights = manager.migrationWeights(77, 8107, 1.7f, 0.6f);
  EXPECT_NEAR(weights.delta75Up, 1.2f, 1e-6);
  EXPECT_NEAR(weights.delta75Down, 0.8f, 1e-6);
  EXPECT_NEAR(weights.fineAcceptanceUp, 0.5f, 1e-6);
  auto fallback = manager.migrationWeights(77, 9201, 1.7f, 0.6f);
  EXPECT_FLOAT_EQ(fallback.fineAcceptanceUp, 1.7f);
  EXPECT_FLOAT_EQ(fallback.fineAcceptanceDown, 0.6f);
  EXPECT_FLOAT_EQ(manager.migrationWeights(77, 123, 1, 1).delta75Up, 1.0f);
  EXPECT_FLOAT_EQ(manager.migrationWeights(999, 8107, 1, 1).delta75Up, 1.0f);
  cfg.migrationBins.clear();
  EXPECT_THROW(stxs::Manager missing(cfg), std::runtime_error);
}

TEST_F(StxsTest, MigrationMappingParserRejectsInvalidInput) {
  auto path = dir / "bins.txt";
  { std::ofstream out(path); out << "# arbitrary category\n8107 2 3 4 1 # fallback\n"; }
  stxs::Config cfg;
  stxs::loadMigrationBinConfig(path.string(), cfg);
  ASSERT_EQ(cfg.migrationBins.size(), 1u);
  EXPECT_EQ(cfg.migrationBins.at(8107).jetPtBin, 3);
  EXPECT_TRUE(cfg.migrationBins.at(8107).useAcceptanceFallback);
  { std::ofstream out(path); out << "8107 2 3 4 0\n8107 1 2 3 0\n"; }
  EXPECT_THROW(stxs::loadMigrationBinConfig(path.string(), cfg), std::runtime_error);
  { std::ofstream out(path); out << "8107 0 3 4 0\n"; }
  EXPECT_THROW(stxs::loadMigrationBinConfig(path.string(), cfg), std::runtime_error);
  { std::ofstream out(path); out << "8107 2 3 4 2\n"; }
  EXPECT_THROW(stxs::loadMigrationBinConfig(path.string(), cfg), std::runtime_error);
}

TEST_F(StxsTest, AnalysisBinsAndTemplateBinsDoNotRequireCms) {
  auto path = dir / "analysis.txt";
  { std::ofstream out(path); out << "1 first 8107,9201\n2 second 1729\n"; }
  auto cfg = stxs::loadConfig(path.string(), dir.string(), "coarse", "fine", true);
  stxs::Manager manager(cfg);
  EXPECT_EQ(manager.analysisBin(9201), 1);
  EXPECT_EQ(manager.analysisBin(1729), 2);
  EXPECT_EQ(manager.analysisBin(123), 0);
  EXPECT_EQ(stxs::stxsTemplateBin(false, 2), 1);
  EXPECT_EQ(stxs::stxsTemplateBin(true, 2), 3);
  EXPECT_EQ(stxs::StxsConfig{}.decayChannelBranch, "analysis_channel");
}
}
