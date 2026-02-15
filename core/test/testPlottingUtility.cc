#include <gtest/gtest.h>

#include <PlottingUtility.h>

#include <RtypesCore.h>
#include <TFile.h>
#include <TH1D.h>
#include <TROOT.h>
#include <TCanvas.h>
#include <TLegend.h>
#include <TLine.h>

#include <cstdio>
#include <filesystem>
#include <string>
#include <cstdlib>
#include <algorithm>
#include <cctype>

namespace {

// Skip message for tests when image library is not available
constexpr const char* kImageLibrarySkipMessage = 
    "Skipping PNG file check: ROOT image library (libASImage) not available. "
    "Install libgif or giflib package to enable full image testing.";

// Check if ROOT's image library (TASImage) is available for saving PNG files
bool isImageLibraryAvailable() {
  static bool checked = false;
  static bool available = false;
  
  if (!checked) {
    // Try to load the image library
    TCanvas testCanvas("test", "test", 1, 1);
    auto tempDir = std::filesystem::temp_directory_path();
    std::string testPath = (tempDir / ("test_image_lib_" + std::to_string(getpid()) + ".png")).string();
    testCanvas.SaveAs(testPath.c_str());
    available = std::filesystem::exists(testPath);
    if (available) {
      std::remove(testPath.c_str());
    }
    checked = true;
  }
  
  return available;
}

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

// Helper: by default tests KEEP generated PNGs so you can inspect them locally.
// - To copy generated PNGs to another directory set env var PLOTTING_TEST_OUTPUT_DIR
// - To force removal after tests set REMOVE_TEST_PLOTS=1 in the environment
static bool shouldRemovePlotsAfterTest() {
  const char* v = std::getenv("REMOVE_TEST_PLOTS");
  if (!v) return false; // default: keep plots
  std::string s(v);
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
  return s == "1" || s == "true" || s == "yes";
}

static void copyPlotIfRequested(const std::string& src) {
  const char* out = std::getenv("PLOTTING_TEST_OUTPUT_DIR");
  if (!out || std::strlen(out) == 0) return;
  try {
    std::filesystem::path destDir(out);
    std::filesystem::create_directories(destDir);
    const auto dst = destDir / std::filesystem::path(src).filename();
    std::filesystem::copy_file(src, dst, std::filesystem::copy_options::overwrite_existing);
  } catch (const std::exception& e) {
    ADD_FAILURE() << "Failed to copy plot '" << src << "' to '" << (out ? out : "") << "': " << e.what();
  }
}

} // namespace

TEST(PlottingUtilityTest, CreatesLinearAndLogStackPlotsWithRatio) {
  gROOT->SetBatch(true);

  const std::string baseDir = std::string(TEST_SOURCE_DIR) + "/aux";
  const std::string metaPath = baseDir + "/test_plotting_meta.root";
  const std::string linearPath = baseDir + "/test_plotting_linear.png";
  const std::string logPath = baseDir + "/test_plotting_log.png";
  const std::string ratioPath = baseDir + "/test_plotting_ratio.png";

  std::remove(metaPath.c_str());
  std::remove(linearPath.c_str());
  std::remove(logPath.c_str());
  std::remove(ratioPath.c_str());

  writeTestMetaFile(metaPath);

  PlottingUtility utility;
  const auto results =
      utility.makeStackPlots({createTestPlotRequest(metaPath, linearPath, false),
                              createTestPlotRequest(metaPath, logPath, true)},
                             true);

  ASSERT_EQ(results.size(), 2u);
  EXPECT_TRUE(results[0].success) << results[0].message;
  EXPECT_TRUE(results[1].success) << results[1].message;
  EXPECT_DOUBLE_EQ(results[0].mcIntegral, 10.0);
  EXPECT_DOUBLE_EQ(results[0].dataIntegral, 8.0);
  EXPECT_DOUBLE_EQ(results[1].mcIntegral, 10.0);
  EXPECT_DOUBLE_EQ(results[1].dataIntegral, 8.0);

  // Only check file existence/size if image library is available
  if (isImageLibraryAvailable()) {
    EXPECT_TRUE(std::filesystem::exists(linearPath));
    EXPECT_TRUE(std::filesystem::exists(logPath));
    EXPECT_GT(std::filesystem::file_size(linearPath), 0u);
    EXPECT_GT(std::filesystem::file_size(logPath), 0u);
  } else {
    GTEST_SKIP() << kImageLibrarySkipMessage;
  }

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

  // save a multi-panel ratio + pull example (top: numerator/denominator, middle: ratio, bottom: pull)
  {
    gROOT->SetBatch(true);
    TCanvas cAll("cAll", "ratio_all", 700, 900);

    // top pad (main histograms)
    TPad topPad("top", "top", 0.0, 0.45, 1.0, 1.0);
    topPad.SetBottomMargin(0.02);
    topPad.SetLeftMargin(0.12);
    topPad.SetRightMargin(0.05);
    topPad.Draw();
    topPad.cd();

    numerator.SetLineColor(kOrange+1);
    numerator.SetLineWidth(1);
    numerator.SetMarkerStyle(20);
    numerator.Draw("HIST E");
    denominator.SetLineColor(kBlue+1);
    denominator.SetLineWidth(1);
    denominator.Draw("HIST E same");

    TLegend legTop(0.65, 0.72, 0.88, 0.88);
    legTop.AddEntry(&numerator, "numerator", "l");
    legTop.AddEntry(&denominator, "denominator", "l");
    legTop.Draw();

    // middle pad (ratio)
    cAll.cd();
    TPad ratioPad("rpad", "rpad", 0.0, 0.25, 1.0, 0.45);
    ratioPad.SetTopMargin(0.03);
    ratioPad.SetBottomMargin(0.02);
    ratioPad.SetLeftMargin(0.12);
    ratioPad.SetRightMargin(0.05);
    ratioPad.Draw();
    ratioPad.cd();

    ratio->SetStats(false);
    ratio->GetYaxis()->SetTitle("Data/MC");
    ratio->SetMinimum(0.0);
    ratio->SetMaximum(2.0);
    ratio->Draw("E1");

    TLine oneLine(ratio->GetXaxis()->GetXmin(), 1.0, ratio->GetXaxis()->GetXmax(), 1.0);
    oneLine.SetLineStyle(2);
    oneLine.Draw("same");

    // bottom pad (pull)
    cAll.cd();
    TPad pullPad("ppad", "ppad", 0.0, 0.0, 1.0, 0.25);
    pullPad.SetTopMargin(0.02);
    pullPad.SetBottomMargin(0.30);
    pullPad.SetLeftMargin(0.12);
    pullPad.SetRightMargin(0.05);
    pullPad.Draw();
    pullPad.cd();

    TH1D pullHist("pull_single", "pull_single", 2, 0.0, 2.0);
    for (int b = 1; b <= pullHist.GetNbinsX(); ++b) {
      const double num = numerator.GetBinContent(b);
      const double den = denominator.GetBinContent(b);
      const double numErr = numerator.GetBinError(b);
      const double denErr = denominator.GetBinError(b);
      const double denom = std::sqrt(numErr * numErr + denErr * denErr);
      const double pull = (std::abs(denom) < 1e-12) ? 0.0 : (num - den) / denom;
      pullHist.SetBinContent(b, pull);
    }
    pullHist.SetFillColor(kGray+1);
    pullHist.Draw("bar");

    cAll.SaveAs(ratioPath.c_str());
  }
  EXPECT_TRUE(std::filesystem::exists(ratioPath));
  EXPECT_GT(std::filesystem::file_size(ratioPath), 0u);

  TH1D zeroDenominator("zeroDenominator", "zeroDenominator", 2, 0.0, 2.0);
  auto zeroRatio = PlottingUtility::computeRatioHistogram(numerator, zeroDenominator, "zero_ratio");
  ASSERT_NE(zeroRatio, nullptr);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinContent(1), 0.0);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinContent(2), 0.0);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinError(1), 0.0);
  EXPECT_DOUBLE_EQ(zeroRatio->GetBinError(2), 0.0);

  // always remove the meta file (test data); keep PNG examples by default
  std::remove(metaPath.c_str());

  // copy to alternate directory if requested, and remove only if explicitly enabled
  copyPlotIfRequested(linearPath);
  copyPlotIfRequested(logPath);
  copyPlotIfRequested(ratioPath);
  if (shouldRemovePlotsAfterTest()) {
    std::remove(linearPath.c_str());
    std::remove(logPath.c_str());
    std::remove(ratioPath.c_str());
  }
}

TEST(PlottingUtilityTest, ComputesDetailedRatioSummary) {
  gROOT->SetBatch(true);

  TH1D lo("lo", "lo", 3, 0.0, 3.0);
  TH1D nlo("nlo", "nlo", 3, 0.0, 3.0);
  TH1D cov("cov", "cov", 3, 0.0, 3.0);
  TH1D syst("syst", "syst", 3, 0.0, 3.0);

  lo.SetBinContent(1, 12.0);
  lo.SetBinError(1, 3.0);
  nlo.SetBinContent(1, 10.0);
  nlo.SetBinError(1, 2.0);
  cov.SetBinContent(1, 1.0);
  syst.SetBinContent(1, 1.5);

  lo.SetBinContent(2, 0.0);
  lo.SetBinError(2, 0.0);
  nlo.SetBinContent(2, 5.0);
  nlo.SetBinError(2, 1.0);

  auto summary = PlottingUtility::computeRatioSummary(lo, nlo, &cov, &syst);
  ASSERT_EQ(summary.ratio.size(), 3u);
  ASSERT_EQ(summary.error.size(), 3u);
  ASSERT_EQ(summary.pull.size(), 3u);
  EXPECT_NEAR(summary.ratio[0], 1.2, 1e-12);
  EXPECT_GT(summary.error[0], 0.0);
  EXPECT_GT(summary.pull[0], 0.0);
  EXPECT_DOUBLE_EQ(summary.ratio[1], 0.0);
  EXPECT_DOUBLE_EQ(summary.error[1], 0.0);

  // save summary histograms for visual inspection
  const std::string baseDir = std::string(TEST_SOURCE_DIR) + "/aux";
  const std::string summaryCombinedPath = baseDir + "/test_ratio_summary.png"; // reused filename, now multi-panel
  std::remove(summaryCombinedPath.c_str());

  const int nbins = static_cast<int>(summary.ratio.size());
  TH1D ratioHist("summary_ratio", "summary_ratio", nbins, 0.0, static_cast<double>(nbins));
  TH1D errorHist("summary_error", "summary_error", nbins, 0.0, static_cast<double>(nbins));
  TH1D pullHist("summary_pull", "summary_pull", nbins, 0.0, static_cast<double>(nbins));
  for (int i = 0; i < nbins; ++i) {
    ratioHist.SetBinContent(i + 1, summary.ratio[i]);
    errorHist.SetBinContent(i + 1, summary.error[i]);
    pullHist.SetBinContent(i + 1, summary.pull[i]);
  }

  // multi-panel canvas: top=ratio, middle=error, bottom=pull
  {
    TCanvas cAll("summaryAll", "summaryAll", 700, 900);

    TPad top("t", "t", 0.0, 0.66, 1.0, 1.0);
    top.SetBottomMargin(0.02);
    top.SetLeftMargin(0.12);
    top.SetRightMargin(0.05);
    top.Draw();
    top.cd();
    ratioHist.SetStats(false);
    ratioHist.GetYaxis()->SetTitle("Ratio");
    ratioHist.Draw("bar");

    cAll.cd();
    TPad mid("m", "m", 0.0, 0.33, 1.0, 0.66);
    mid.SetTopMargin(0.03);
    mid.SetBottomMargin(0.02);
    mid.SetLeftMargin(0.12);
    mid.SetRightMargin(0.05);
    mid.Draw();
    mid.cd();
    errorHist.SetStats(false);
    errorHist.GetYaxis()->SetTitle("Error");
    errorHist.Draw("bar");

    cAll.cd();
    TPad bot("b", "b", 0.0, 0.0, 1.0, 0.33);
    bot.SetTopMargin(0.03);
    bot.SetBottomMargin(0.30);
    bot.SetLeftMargin(0.12);
    bot.SetRightMargin(0.05);
    bot.Draw();
    bot.cd();
    pullHist.SetStats(false);
    pullHist.GetYaxis()->SetTitle("Pull");
    pullHist.Draw("bar");

    cAll.SaveAs(summaryCombinedPath.c_str());
  }

  // Only check file existence if image library is available
  if (isImageLibraryAvailable()) {
    EXPECT_TRUE(std::filesystem::exists(summaryCombinedPath));
  } else {
    GTEST_SKIP() << kImageLibrarySkipMessage;
  }

  // copy/cleanup behavior (keep by default)
  copyPlotIfRequested(summaryCombinedPath);
  if (shouldRemovePlotsAfterTest()) {
    std::remove(summaryCombinedPath.c_str());
  }
}

TEST(PlottingUtilityTest, ComputesPCAEnvelope) {
  TH1D nominal("nominal", "nominal", 2, 0.0, 2.0);
  nominal.SetBinContent(1, 10.0);
  nominal.SetBinContent(2, 20.0);

  TH1D var1("var1", "var1", 2, 0.0, 2.0);
  TH1D var2("var2", "var2", 2, 0.0, 2.0);
  TH1D var3("var3", "var3", 2, 0.0, 2.0);
  var1.SetBinContent(1, 9.0);
  var1.SetBinContent(2, 19.0);
  var2.SetBinContent(1, 10.0);
  var2.SetBinContent(2, 21.0);
  var3.SetBinContent(1, 11.0);
  var3.SetBinContent(2, 20.0);

  std::vector<const TH1D*> variations = {&var1, &var2, &var3};
  auto pca = PlottingUtility::computePCAEnvelope(nominal, variations, "unit");
  ASSERT_NE(pca.mean, nullptr);
  ASSERT_NE(pca.up, nullptr);
  ASSERT_NE(pca.down, nullptr);
  ASSERT_EQ(pca.explainedVariance.size(), 2u);

  EXPECT_NEAR(pca.mean->GetBinContent(1), 10.0, 1e-12);
  EXPECT_NEAR(pca.mean->GetBinContent(2), 20.0, 1e-12);
  EXPECT_GE(pca.up->GetBinContent(1), pca.mean->GetBinContent(1));
  EXPECT_LE(pca.down->GetBinContent(1), pca.mean->GetBinContent(1));

  // save PCA envelope plot for visual inspection
  {
    gROOT->SetBatch(true);
    const std::string baseDir = std::string(TEST_SOURCE_DIR) + "/aux";
    const std::string pcaPath = baseDir + "/test_pca_envelope.png";
    std::remove(pcaPath.c_str());

    TCanvas cPCA("cPCA", "PCA", 700, 500);
    pca.mean->SetLineColor(kBlack);
    pca.up->SetLineColor(kRed);
    pca.down->SetLineColor(kBlue);
    pca.up->SetFillColorAlpha(kRed, 0.25);
    pca.down->SetFillColorAlpha(kBlue, 0.15);

    pca.mean->SetStats(false);
    pca.mean->Draw("hist");
    pca.up->Draw("same E2");
    pca.down->Draw("same E2");

    TLegend leg(0.6, 0.7, 0.9, 0.9);
    leg.AddEntry(pca.mean.get(), "mean", "l");
    leg.AddEntry(pca.up.get(), "+uncert", "f");
    leg.AddEntry(pca.down.get(), "-uncert", "f");
    leg.Draw();

    // create a 3-panel PCA plot (top: mean+envelope, middle: mean/nominal, bottom: pull)
    const std::string pcaPanelPath = baseDir + "/test_pca_envelope.png";
    std::remove(pcaPanelPath.c_str());

    TCanvas cPCAall("cPCAall", "PCA all", 800, 1000);

    TPad tpad("tpad", "tpad", 0.0, 0.56, 1.0, 1.0);
    tpad.SetBottomMargin(0.02);
    tpad.SetLeftMargin(0.12);
    tpad.SetRightMargin(0.05);
    tpad.Draw();
    tpad.cd();

    // draw mean and envelope band
    auto band = std::unique_ptr<TH1D>(static_cast<TH1D*>(pca.mean->Clone("pca_band")));
    for (int b = 1; b <= band->GetNbinsX(); ++b) {
      const double err = std::abs(pca.up->GetBinContent(b) - pca.mean->GetBinContent(b));
      band->SetBinError(b, err);
      band->SetBinContent(b, pca.mean->GetBinContent(b));
    }
    band->SetDirectory(nullptr);
    band->SetFillColor(kGray+2);
    band->SetFillStyle(3354);
    band->Draw("E2");
    pca.mean->SetLineColor(kBlack);
    pca.mean->Draw("hist same");
    pca.up->SetLineColor(kRed);
    pca.down->SetLineColor(kBlue);
    pca.up->SetLineStyle(2);
    pca.down->SetLineStyle(2);
    pca.up->Draw("hist same");
    pca.down->Draw("hist same");

    cPCAall.cd();
    TPad rpad("rpad", "rpad", 0.0, 0.28, 1.0, 0.56);
    rpad.SetTopMargin(0.03);
    rpad.SetBottomMargin(0.02);
    rpad.SetLeftMargin(0.12);
    rpad.SetRightMargin(0.05);
    rpad.Draw();
    rpad.cd();

    // mean / nominal ratio
    auto ratioPCA = PlottingUtility::computeRatioHistogram(*pca.mean, nominal, "pca_ratio");
    if (ratioPCA) {
      ratioPCA->SetDirectory(nullptr);
      ratioPCA->SetMinimum(0.59);
      ratioPCA->SetMaximum(1.41);
      ratioPCA->Draw("E1");
      TLine one(ratioPCA->GetXaxis()->GetXmin(), 1.0, ratioPCA->GetXaxis()->GetXmax(), 1.0);
      one.SetLineStyle(2);
      one.Draw("same");
    }

    cPCAall.cd();
    TPad ppad("ppad", "ppad", 0.0, 0.0, 1.0, 0.28);
    ppad.SetTopMargin(0.03);
    ppad.SetBottomMargin(0.30);
    ppad.SetLeftMargin(0.12);
    ppad.SetRightMargin(0.05);
    ppad.Draw();
    ppad.cd();

    // pull between mean and nominal
    auto pullH = std::unique_ptr<TH1D>(static_cast<TH1D*>(pca.mean->Clone("pca_pull")));
    for (int b = 1; b <= pullH->GetNbinsX(); ++b) {
      const double m = pca.mean->GetBinContent(b);
      const double mErr = std::abs(pca.up->GetBinContent(b) - m);
      const double nom = nominal.GetBinContent(b);
      const double nomErr = nominal.GetBinError(b);
      const double denom = std::sqrt(mErr * mErr + nomErr * nomErr);
      const double pull = (std::abs(denom) < 1e-12) ? 0.0 : (m - nom) / denom;
      pullH->SetBinContent(b, pull);
      pullH->SetBinError(b, 0.0);
    }
    pullH->SetFillColor(kGray+1);
    pullH->Draw("bar");

    cPCAall.SaveAs(pcaPanelPath.c_str());
    
    // Only check file existence if image library is available
    if (isImageLibraryAvailable()) {
      EXPECT_TRUE(std::filesystem::exists(pcaPanelPath));
    } else {
      GTEST_SKIP() << kImageLibrarySkipMessage;
    }

    // copy PCA plot if requested; keep file by default unless REMOVE_TEST_PLOTS=1
    copyPlotIfRequested(pcaPanelPath);
    if (shouldRemovePlotsAfterTest()) {
      std::remove(pcaPanelPath.c_str());
    }
  }
}
