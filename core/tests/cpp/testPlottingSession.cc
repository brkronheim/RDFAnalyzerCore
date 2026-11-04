#include "../../src/PlottingInternal.h"
#include <PlottingUtility.h>
#include <TDirectory.h>
#include <TFile.h>
#include <TH1D.h>
#include <THnSparse.h>
#include <TObjString.h>
#include <TROOT.h>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <unistd.h>

namespace {
std::string output(const std::string &name) {
  auto directory = std::filesystem::temp_directory_path() /
                   ("rdf_plot_session_" + std::to_string(getpid()));
  std::filesystem::create_directories(directory);
  return (directory / name).string();
}
PlotProcessConfig process(std::vector<double> values, bool data = false) {
  PlotProcessConfig p;
  p.legendLabel = data ? "Data" : "Background";
  p.values = std::move(values);
  p.variances = {4, 9, 16};
  p.isData = data;
  return p;
}
PlotRequest toy(const std::string &name = "test.svg") {
  PlotRequest r;
  r.outputFile = output(name);
  r.binEdges = {0, 1, 3, 6};
  r.title = "PolishedTestTitle";
  r.xAxisTitle = "Observable [GeV]";
  r.yAxisTitle = "Events / bin";
  r.luminosityLabel = "100 fb^{-1} (13.6 TeV)";
  r.processes = {process({10, 20, 30}), process({12, 18, 30}, true)};
  return r;
}
void writeFile(const std::string &path, double norm = 10) {
  TDirectory::TContext context(nullptr);
  TFile file(path.c_str(), "RECREATE");
  double edges[]{0, 1, 3, 6};
  TH1D hist("observable", "", 3, edges);
  hist.SetDirectory(nullptr);
  for (int b = 1; b <= 3; ++b) {
    hist.SetBinContent(b, 10 * b);
    hist.SetBinError(b, b);
  }
  hist.SetBinContent(0, 2);
  hist.SetBinError(0, 3);
  hist.SetBinContent(4, 3);
  hist.SetBinError(4, 4);
  hist.Write();
  TH1D counter("counter", "", 1, 0, 1);
  counter.SetDirectory(nullptr);
  counter.SetBinContent(1, norm);
  counter.Write();
}
void writeSparse(const std::string &path) {
  TDirectory::TContext context(nullptr);
  TFile file(path.c_str(), "RECREATE");
  int bins[]{2, 2, 1, 3, 3};
  double low[]{0, 0, 0, 0, 0}, high[]{2, 2, 1, 3, 6}, edges[]{0, 1, 3, 6};
  THnSparseD hist("source", "", 5, bins, low, high);
  hist.SetBinEdges(4, edges);
  hist.Sumw2();
  // child Zmumu nominal and down; Up is present globally but empty in child.
  double coordinates[]{1.5, 1.5, .5, .5, .5};
  hist.Fill(coordinates, 10);
  coordinates[3] = 2.5;
  hist.Fill(coordinates, 10);
  coordinates[3] = .5;
  coordinates[4] = 2.;
  hist.Fill(coordinates, 2);
  hist.Fill(coordinates, -2);
  coordinates[1] = .5;
  coordinates[3] = 1.5;
  hist.Fill(coordinates, 1000);
  auto *dir = file.mkdir("nd_histograms_v1");
  dir->cd();
  hist.Write();
  file.cd();
  TObjString schema(
      R"({"schema_version":1,"directory":"nd_histograms_v1","histograms":[{"name":"observable","key":"source","axes":[{"role":"channel","bins":2,"labels":["Zee","Zmumu"],"edges":[0,1,2]},{"role":"control","bins":2,"labels":["parent","child"],"edges":[0,1,2]},{"role":"sample","bins":1,"labels":["Default"],"edges":[0,1]},{"role":"systematic","bins":3,"labels":["Nominal","JESUp","JESDown"],"edges":[0,1,2,3]},{"role":"observable","bins":3,"labels":[],"edges":[0,1,3,6]}]}]})");
  schema.Write("__rdf_histogram_schema_v1");
}
} // namespace

TEST(PlottingSessionTest,
     AggregatesDataGroupsVariancesAndBlindsBeforeReporting) {
  auto request = toy();
  request.processes.push_back(process({1, 2, 3}, true));
  request.processes.push_back(process({5, 10, 15}));
  request.processes[0].group = "background";
  request.processes.back().group = "background";
  request.processes.back().scale = 2;
  request.blindBins = {false, true, false};
  request.density = true;
  rdfplot::Reader reader(1, 16 * 1024 * 1024);
  rdfplot::Prepared p;
  reader.prepare(request, p);
  EXPECT_EQ(p.count, 1u);
  EXPECT_DOUBLE_EQ(p.result.mcIntegral, 120);
  EXPECT_DOUBLE_EQ(p.result.dataIntegral, 46);
  EXPECT_DOUBLE_EQ(p.result.mcVariance, 145);
  EXPECT_DOUBLE_EQ(p.data[1], 10);
  EXPECT_DOUBLE_EQ(p.variance[1], 45. / 4);
  EXPECT_DOUBLE_EQ(p.series[0].values[2], 20);
}

TEST(PlottingSessionTest, CorrelatesNuisancesBeforeEnveloping) {
  auto request = toy();
  request.processes.pop_back();
  auto second = process({10, 20, 30});
  request.processes[0].variations = {{"shared", {12, 24, 36}, {8, 16, 24}}};
  second.variations = {{"shared", {8, 16, 24}, {12, 24, 36}},
                       {"independent", {11, 22, 33}, {9, 18, 27}}};
  request.processes.push_back(second);
  rdfplot::Reader reader(1, 16 * 1024 * 1024);
  rdfplot::Prepared p;
  reader.prepare(request, p);
  EXPECT_EQ(p.systHigh, (std::vector<double>{1, 2, 3}));
  EXPECT_EQ(p.systLow, p.systHigh);
}

TEST(PlottingSessionTest, SumsFileNormalizationAndFoldsFlowWithSquaredErrors) {
  const auto file1 = output("input1.root"), file2 = output("input2.root");
  writeFile(file1, 10);
  writeFile(file2, 30);
  auto request = toy();
  request.processes.clear();
  PlotProcessConfig config;
  config.files = {file1, file2};
  config.histogramName = "observable";
  config.normalizationHistogram = "counter";
  config.scale = 40;
  request.flow = PlotFlow::Fold;
  request.processes = {config};
  rdfplot::Reader reader(2, 16 * 1024 * 1024);
  rdfplot::Prepared p;
  reader.prepare(request, p);
  EXPECT_EQ(p.total, (std::vector<double>{24, 40, 66}));
  EXPECT_EQ(p.variance, (std::vector<double>{20, 8, 50}));
  EXPECT_EQ(reader.opens, 2u);
  reader.prepare(request, p);
  EXPECT_EQ(reader.opens, 2u);
}

TEST(PlottingSessionTest,
     SparseSelectionsPreserveCancellationAndEmptyMigrations) {
  auto request = toy();
  request.processes.clear();
  const auto path = output("sparse.root");
  writeSparse(path);
  PlotProcessConfig config;
  config.files = {path};
  config.histogramName = "observable";
  config.readSystematics = true;
  config.selections = {{"channel", {"Zmumu"}}, {"control", {"child"}}};
  request.processes = {config};
  rdfplot::Reader reader(1, 16 * 1024 * 1024);
  rdfplot::Prepared p;
  reader.prepare(request, p);
  EXPECT_EQ(p.total, (std::vector<double>{10, 0, 0}));
  EXPECT_EQ(p.variance, (std::vector<double>{100, 8, 0}));
  EXPECT_EQ(p.systLow, (std::vector<double>{10, 0, 0}));
  EXPECT_EQ(p.systHigh, (std::vector<double>{0, 0, 0}));
  request.processes[0].selections["control"] = {"unknown"};
  EXPECT_THROW(reader.prepare(request, p), std::invalid_argument);
}

TEST(PlottingSessionTest, PersistentDrawablesSurviveMixedPlotsAndExports) {
  gROOT->SetBatch(true);
  PlottingUtility::Session session;
  auto request = toy("persistent.svg");
  request.additionalOutputs = {output("persistent.pdf"),
                               output("persistent.png")};
  auto result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  const auto warm = session.stats();
  for (int i = 0; i < 4; ++i) {
    request.logY = i % 2;
    result = session.render(request);
    ASSERT_TRUE(result.success) << result.message;
  }
  const auto after = session.stats();
  EXPECT_EQ(after.workspaces, 1u);
  EXPECT_EQ(after.drawableObjects, warm.drawableObjects);
  EXPECT_EQ(after.bufferGrowths, warm.bufferGrowths);
  EXPECT_EQ(result.outputs.size(), 3u);
  std::ifstream file(request.outputFile);
  std::string svg((std::istreambuf_iterator<char>(file)), {});
  EXPECT_NE(svg.find("PolishedTestTitle"), std::string::npos);
  EXPECT_NE(svg.find("Pull"), std::string::npos);
  EXPECT_NE(svg.find("Data / MC"), std::string::npos);
  request.panels = {{PlotPanel::Significance},
                    {PlotPanel::SignalBackground},
                    {PlotPanel::Ratio}};
  request.processes[0].isSignal = true;
  request.processes[0].displayScale = 2;
  result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  EXPECT_EQ(session.stats().workspaces, 2u);
  EXPECT_DOUBLE_EQ(result.mcIntegral, 60);
  EXPECT_DOUBLE_EQ(result.dataIntegral, 60);
}

TEST(PlottingSessionTest, RejectsInvalidInputsAndRecoversWithoutLeakingState) {
  PlottingUtility::Session session;
  auto request = toy();
  request.processes[0].variances[0] = -1;
  EXPECT_FALSE(session.render(request).success);
  request = toy();
  request.binEdges = {0, 1, 1, 6};
  EXPECT_FALSE(session.render(request).success);
  request = toy();
  request.blindBins = {true};
  EXPECT_FALSE(session.render(request).success);
  request = toy();
  request.outputFile = output("invalid.xyz");
  EXPECT_FALSE(session.render(request).success);
  request = toy();
  request.style.height = 100;
  EXPECT_FALSE(session.render(request).success);
  request = toy();
  ASSERT_TRUE(session.render(request).success);
  request.processes.erase(request.processes.begin());
  ASSERT_TRUE(session.render(request).success);
  request = toy();
  request.processes.pop_back();
  ASSERT_TRUE(session.render(request).success);
  request = toy();
  request.binLabels = {"low", "middle", "high"};
  ASSERT_TRUE(session.render(request).success);
  request = toy();
  ASSERT_TRUE(session.render(request).success);
  EXPECT_THROW(
      std::async(std::launch::async, [&] { return session.render(request); })
          .get(),
      std::invalid_argument);
}

TEST(PlottingSessionTest,
     ParallelPreparationHasDeterministicResultsAndBoundedFileReuse) {
  auto request = toy();
  const auto path = output("parallel.root");
  writeFile(path);
  request.processes.clear();
  PlotProcessConfig config;
  config.files = {path};
  config.histogramName = "observable";
  request.processes = {config};
  PlotBatchOptions options;
  options.workers = 2;
  options.maxOpenFiles = 2;
  PlottingUtility::Session parallel(options), serial;
  std::vector<PlotRequest> requests(6, request);
  for (std::size_t i = 0; i < requests.size(); ++i)
    requests[i].outputFile = output("parallel" + std::to_string(i) + ".svg");
  auto a = parallel.renderBatch(requests), b = serial.renderBatch(requests);
  for (std::size_t i = 0; i < a.size(); ++i) {
    ASSERT_TRUE(a[i].success) << a[i].message;
    EXPECT_EQ(a[i].mcIntegral, b[i].mcIntegral);
    EXPECT_EQ(a[i].mcVariance, b[i].mcVariance);
  }
  EXPECT_EQ(parallel.stats().fileOpens, 2u);
  EXPECT_EQ(serial.stats().fileOpens, 1u);
}

TEST(PlottingSessionTest, DrawsSignedHeatmapsAndResetsPaletteAndAxes) {
  PlottingUtility::Session session;
  PlotRequest request;
  request.kind = PlotKind::Heatmap;
  request.binEdges = {0, 1, 2};
  request.yBinEdges = {0, 1, 2, 3};
  request.matrix = {10, -3, 0, 5, -1, 8};
  request.title = "Signed response";
  request.xAxisTitle = "Reco bin";
  request.yAxisTitle = "Truth bin";
  request.yBinLabels = {"Z+light jets", "Z+b jets", "Top"};
  request.outputFile = output("response.png");
  request.horizontalSeparators = {1};
  auto result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  auto warm = session.stats();
  result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  EXPECT_EQ(session.stats().drawableObjects, warm.drawableObjects);
  request.logZ = true;
  result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  request.matrix.assign(6, 0);
  EXPECT_FALSE(session.render(request).success);
  auto stack = toy("after_heatmap.svg");
  ASSERT_TRUE(session.render(stack).success);
}

TEST(PlottingSessionTest,
     RatioRetainsUncertaintyAtZeroNumeratorAndValidatesEdges) {
  TH1D a("zero_num", "", 2, 0, 2), b("denom", "", 2, 0, 2);
  a.SetDirectory(nullptr);
  b.SetDirectory(nullptr);
  a.SetBinError(1, 3);
  b.SetBinContent(1, 2);
  auto result = PlottingUtility::computeRatioSummary(a, b);
  EXPECT_DOUBLE_EQ(result.error[0], 1.5);
  b.GetXaxis()->SetLimits(0, 4);
  EXPECT_THROW(PlottingUtility::computeRatioSummary(a, b),
               std::invalid_argument);
}

TEST(PlottingSessionTest, SharedParserHandlesExtendedJsonAndVersionedYaml) {
  auto requests = PlottingUtility::parseRequests(
      R"({"schema_version":1,"plots":[{"kind":"overlay","panels":["ratio",{"type":"pull","autoRange":false,"minimum":-3,"maximum":3}],"style":{"width":1200},"processes":[{"color":"#4477AA","values":[1,2],"variances":[1,2]}]}]})");
  ASSERT_EQ(requests.size(), 1u);
  EXPECT_EQ(requests[0].kind, PlotKind::Overlay);
  EXPECT_EQ(requests[0].panels.size(), 2u);
  EXPECT_EQ(requests[0].style.width, 1200);
  EXPECT_EQ(requests[0].processes[0].colorHex, "#4477AA");
  EXPECT_THROW(PlottingUtility::parseRequests("schema_version: 7\nplots: []"),
               std::invalid_argument);
  EXPECT_THROW(PlottingUtility::parseRequests("[{kind: invented}]"),
               std::invalid_argument);
}

TEST(PlottingSessionTest,
     OverlayComparisonUsesPhysicalNormalizationAndValidatesCovariance) {
  auto request = toy("overlay.svg");
  request.kind = PlotKind::Overlay;
  request.processes[1].isData = false;
  request.processes[1].displayScale = 5;
  request.panels = {{PlotPanel::Ratio}, {PlotPanel::Pull}};
  PlottingUtility::Session session;
  auto result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  EXPECT_DOUBLE_EQ(result.mcIntegral, 120);
  request.processes[1].referenceCovariance = {1e9, 0, 0};
  EXPECT_FALSE(session.render(request).success);
  request.processes[1].referenceCovariance = {0, 0, 0};
  ASSERT_TRUE(session.render(request).success);
  request.referenceProcess = 9;
  EXPECT_FALSE(session.render(request).success);
}

TEST(PlottingSessionTest, MemoryBudgetAndStrictNormalizationFailExplicitly) {
  auto request = toy();
  const auto path = output("normalization.root");
  writeFile(path, 0);
  request.processes.clear();
  PlotProcessConfig config;
  config.files = {path};
  config.histogramName = "observable";
  config.normalizationHistogram = "counter";
  request.processes = {config};
  PlottingUtility::Session session;
  EXPECT_FALSE(session.render(request).success);
  request.strictNormalization = false;
  auto result = session.render(request);
  ASSERT_TRUE(result.success) << result.message;
  EXPECT_FALSE(result.warnings.empty());
  request = toy();
  request.binEdges.resize(10001);
  std::iota(request.binEdges.begin(), request.binEdges.end(), 0.);
  for (auto &p : request.processes) {
    p.values.assign(10000, 1);
    p.variances.assign(10000, 1);
  }
  PlotBatchOptions options;
  options.maxPreparedBytes = 1024 * 1024;
  PlottingUtility::Session small(options);
  result = small.render(request);
  EXPECT_FALSE(result.success);
  EXPECT_NE(result.message.find("budget"), std::string::npos);
}

TEST(PlottingSessionTest, RasterAfterCanvasResizeMatchesFreshSession) {
  auto request = toy("reused_resize.png");
  request.binLabels = {"low category", "middle category", "high category"};
  PlottingUtility::Session reused;
  auto previous = toy("previous.svg");
  previous.logY = true;
  ASSERT_TRUE(reused.render(previous).success);
  ASSERT_TRUE(reused.render(request).success);
  std::ifstream reusedFile(request.outputFile, std::ios::binary);
  std::string actual((std::istreambuf_iterator<char>(reusedFile)), {});
  request.outputFile = output("fresh_resize.png");
  PlottingUtility::Session fresh;
  ASSERT_TRUE(fresh.render(request).success);
  std::ifstream freshFile(request.outputFile, std::ios::binary);
  std::string expected((std::istreambuf_iterator<char>(freshFile)), {});
  EXPECT_EQ(actual, expected);

  // Switching back must also restore the full numeric range, not the first
  // categorical bin's edges. Compare curves as well as histogram frames.
  request.binLabels.clear();
  request.kind = PlotKind::Overlay;
  request.processes[1].isData = false;
  request.outputFile = output("reused_numeric.png");
  ASSERT_TRUE(reused.render(request).success);
  std::ifstream numericFile(request.outputFile, std::ios::binary);
  actual.assign(std::istreambuf_iterator<char>(numericFile), {});
  request.outputFile = output("fresh_numeric.png");
  PlottingUtility::Session freshNumeric;
  ASSERT_TRUE(freshNumeric.render(request).success);
  std::ifstream referenceFile(request.outputFile, std::ios::binary);
  expected.assign(std::istreambuf_iterator<char>(referenceFile), {});
  EXPECT_EQ(actual, expected);
}

TEST(PlottingSessionTest,
     CollaborationHeadersAndExternalLegendRemainConfigurable) {
  PlottingUtility::Session session;
  auto request = toy("headers.svg");
  request.experimentExtra = "Simulation";
  for (auto experiment :
       {PlotExperiment::CMS, PlotExperiment::ATLAS, PlotExperiment::LHCb,
        PlotExperiment::ALICE, PlotExperiment::Custom}) {
    request.experiment = experiment;
    request.experimentLabel =
        experiment == PlotExperiment::Custom ? "Custom experiment" : "";
    auto result = session.render(request);
    ASSERT_TRUE(result.success) << result.message;
    std::ifstream file(request.outputFile);
    std::string svg((std::istreambuf_iterator<char>(file)), {});
    EXPECT_NE(svg.find("Simulation"), std::string::npos);
  }
  request.style.legendInside = false;
  ASSERT_TRUE(session.render(request).success);
}

TEST(PlottingSessionTest, ExplicitAlignedRebinPreservesYieldsAndVariance) {
  const auto path = output("rebin.root");
  writeFile(path);
  auto request = toy();
  request.binEdges = {0, 3, 6};
  request.processes.clear();
  PlotProcessConfig config;
  config.files = {path};
  config.histogramName = "observable";
  request.processes = {config};
  rdfplot::Reader reader(1, 16 * 1024 * 1024);
  rdfplot::Prepared p;
  EXPECT_THROW(reader.prepare(request, p), std::invalid_argument);
  request.rebin = true;
  reader.prepare(request, p);
  EXPECT_EQ(p.total, (std::vector<double>{30, 30}));
  EXPECT_EQ(p.variance, (std::vector<double>{5, 9}));
  request.binEdges = {0, 2, 6};
  EXPECT_THROW(reader.prepare(request, p), std::invalid_argument);
}
