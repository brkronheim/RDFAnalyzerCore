#include <gtest/gtest.h>

#include <PlottingUtility.h>

#include <RtypesCore.h>
#include <TFile.h>
#include <TH1D.h>
#include <TROOT.h>

#include <cstdio>
#include <filesystem>
#include <string>

namespace {

void writeTestMetaFile(const std::string& metaPath) {
  TFile file(metaPath.c_str(), "RECREATE");
  ASSERT_FALSE(file.IsZombie()) << "Failed to create test meta file at: " << metaPath;

  auto* dirA = file.mkdir("procA");
  auto* dirB = file.mkdir("procB");
  auto* dirData = file.mkdir("data");
  ASSERT_NE(dirA, nullptr);
  ASSERT_NE(dirB, nullptr);
  ASSERT_NE(dirData, nullptr);

  TH1D normA("counter_weightSum_procA", "counter_weightSum_procA", 1, 0.0, 1.0);
  normA.SetBinContent(1, 10.0);
  normA.Write();

  TH1D normB("counter_weightSum_procB", "counter_weightSum_procB", 1, 0.0, 1.0);
  normB.SetBinContent(1, 20.0);
  normB.Write();

  dirA->cd();
  TH1D histA("pt", "pt", 2, 0.0, 2.0);
  histA.SetBinContent(1, 10.0);
  histA.SetBinContent(2, 10.0);
  histA.Write();

  dirB->cd();
  TH1D histB("pt", "pt", 2, 0.0, 2.0);
  histB.SetBinContent(1, 10.0);
  histB.SetBinContent(2, 20.0);
  histB.Write();

  dirData->cd();
  TH1D data("pt", "pt", 2, 0.0, 2.0);
  data.SetBinContent(1, 4.0);
  data.SetBinContent(2, 4.0);
  data.Write();

  file.Close();
}

PlotRequest createTestPlotRequest(const std::string& metaPath, const std::string& outputPath, bool logY) {
  PlotRequest request;
  request.metaFile = metaPath;
  request.outputFile = outputPath;
  request.title = "Plot";
  request.xAxisTitle = "pt";
  request.logY = logY;
  request.drawRatio = true;
  request.processes = {
      {"procA", "pt", "Process A", kRed + 1, 2.0, "counter_weightSum_procA", false},
      {"procB", "pt", "Process B", kBlue + 1, 4.0, "counter_weightSum_procB", false},
      {"data", "pt", "Data", kBlack, 1.0, "", true}};
  return request;
}

} // namespace

TEST(PlottingUtilityTest, CreatesLinearAndLogStackPlotsWithRatio) {
  gROOT->SetBatch(true);

  const std::string baseDir = std::string(TEST_SOURCE_DIR) + "/aux";
  const std::string metaPath = baseDir + "/test_plotting_meta.root";
  const std::string linearPath = baseDir + "/test_plotting_linear.png";
  const std::string logPath = baseDir + "/test_plotting_log.png";

  std::remove(metaPath.c_str());
  std::remove(linearPath.c_str());
  std::remove(logPath.c_str());

  writeTestMetaFile(metaPath);

  PlottingUtility utility;
  const auto results =
      utility.makeStackPlots({createTestPlotRequest(metaPath, linearPath, false),
                              createTestPlotRequest(metaPath, logPath, true)},
                             true);

  ASSERT_EQ(results.size(), 2u);
  EXPECT_TRUE(results[0].success) << results[0].message;
  EXPECT_TRUE(results[1].success) << results[1].message;
  EXPECT_DOUBLE_EQ(results[0].mcIntegral, 8.0);
  EXPECT_DOUBLE_EQ(results[0].dataIntegral, 8.0);
  EXPECT_DOUBLE_EQ(results[1].mcIntegral, 8.0);
  EXPECT_DOUBLE_EQ(results[1].dataIntegral, 8.0);

  EXPECT_TRUE(std::filesystem::exists(linearPath));
  EXPECT_TRUE(std::filesystem::exists(logPath));
  EXPECT_GT(std::filesystem::file_size(linearPath), 0u);
  EXPECT_GT(std::filesystem::file_size(logPath), 0u);

  TH1D numerator("numerator", "numerator", 2, 0.0, 2.0);
  numerator.SetBinContent(1, 2.0);
  numerator.SetBinContent(2, 4.0);
  TH1D denominator("denominator", "denominator", 2, 0.0, 2.0);
  denominator.SetBinContent(1, 1.0);
  denominator.SetBinContent(2, 2.0);

  auto ratio = PlottingUtility::computeRatioHistogram(numerator, denominator);
  ASSERT_NE(ratio, nullptr);
  EXPECT_DOUBLE_EQ(ratio->GetBinContent(1), 2.0);
  EXPECT_DOUBLE_EQ(ratio->GetBinContent(2), 2.0);

  TH1D zeroDenominator("zeroDenominator", "zeroDenominator", 2, 0.0, 2.0);
  auto zeroRatio = PlottingUtility::computeRatioHistogram(numerator, zeroDenominator, "zero_ratio");
  ASSERT_NE(zeroRatio, nullptr);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinContent(1), 0.0);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinContent(2), 0.0);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinError(1), 0.0);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinError(2), 0.0);

  std::remove(metaPath.c_str());
  std::remove(linearPath.c_str());
  std::remove(logPath.c_str());
}
