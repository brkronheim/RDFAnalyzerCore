#include <CutflowManager.h>
#include <RegionManager.h>
#include <NullOutputSink.h>
#include <TFile.h>
#include <TH1D.h>
#include <TH2D.h>
#include <TObject.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <iostream>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// Helper: pass a boolean column value as a filter predicate.
// ---------------------------------------------------------------------------
namespace {
bool passBoolCut(bool pass) { return pass; }
} // anonymous namespace

// ---------------------------------------------------------------------------
// CutflowManager implementation
// ---------------------------------------------------------------------------

void CutflowManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  logger_m = &ctx.logger;
  metaSink_m = &ctx.metaSink;
}

void CutflowManager::bindToRegionManager(RegionManager *rm) {
  regionManager_m = rm;
}

void CutflowManager::initialize() {
  if (!regionManager_m) return;
  // Validate that the bound RegionManager has at least been set up.
  // (Regions may not yet be declared here; real validation occurs in execute.)
  if (logger_m) {
    logger_m->log(ILogger::Level::Info,
                  "CutflowManager: bound to RegionManager with " +
                      std::to_string(regionManager_m->getRegionNames().size()) +
                      " declared region(s).");
  }
}

void CutflowManager::addCut(const std::string &name,
                             const std::string &boolColumn) {
  if (!dataManager_m) {
    throw std::runtime_error(
        "CutflowManager::addCut(): context not set. "
        "Call setContext() before addCut().");
  }

  // Capture the current DF node *before* this cut's filter is applied.
  // All boolean columns for every registered cut must already be defined.
  ROOT::RDF::RNode dfBefore = dataManager_m->getDataFrame();
  cuts_m.push_back({name, boolColumn, dfBefore});

  // Apply the filter on the main analysis dataframe.
  dataManager_m->Filter(passBoolCut, {boolColumn});
}

void CutflowManager::execute() {
  if (cuts_m.empty()) return;

  // The "base" node is captured before the first cut's filter was applied.
  // It must carry all boolean columns used by every registered cut so that
  // N-1 chains can be constructed from it.
  ROOT::RDF::RNode baseDf = cuts_m[0].dfNode;

  // Book total event count (before any registered cut).
  totalCountResult_m = baseDf.Count();

  // Sequential cutflow:
  //   cuts_m[j].dfNode has cuts 0..j-1 applied as filters;
  //   applying cut j's column as a filter yields the count after cuts 0..j.
  cutflowCountResults_m.clear();
  for (const auto &cut : cuts_m) {
    // Copy to a non-const local: RNode::Filter() is not const-qualified.
    ROOT::RDF::RNode dfNode = cut.dfNode;
    auto filtered = dfNode.Filter(passBoolCut, {cut.column});
    cutflowCountResults_m.push_back(filtered.Count());
  }

  // N-1 counts:
  //   For cut i, apply every cut except i starting from the base node.
  nMinusOneCountResults_m.clear();
  for (std::size_t i = 0; i < cuts_m.size(); ++i) {
    ROOT::RDF::RNode nMinus1Df = baseDf;
    for (std::size_t j = 0; j < cuts_m.size(); ++j) {
      if (j != i) {
        nMinus1Df = nMinus1Df.Filter(passBoolCut, {cuts_m[j].column});
      }
    }
    nMinusOneCountResults_m.push_back(nMinus1Df.Count());
  }

  // -------------------------------------------------------------------------
  // Per-region count bookings (all on the same computation graph → single pass)
  // -------------------------------------------------------------------------
  if (!regionManager_m) return;

  regionPending_m.clear();
  for (const auto &regionName : regionManager_m->getRegionNames()) {
    ROOT::RDF::RNode regionDf = regionManager_m->getRegionDataFrame(regionName);

    RegionCutflowPending pending;

    // Total events in this region (before any cut).
    pending.totalCount = regionDf.Count();

    // Sequential cutflow on the region DF.
    // Apply each cut in turn, starting from the region's (already filtered) DF.
    ROOT::RDF::RNode accumulated = regionDf;
    for (const auto &cut : cuts_m) {
      accumulated = accumulated.Filter(passBoolCut, {cut.column});
      pending.cutflowCounts.push_back(accumulated.Count());
    }

    // N-1 counts on the region DF.
    for (std::size_t i = 0; i < cuts_m.size(); ++i) {
      ROOT::RDF::RNode nMinus1 = regionDf;
      for (std::size_t j = 0; j < cuts_m.size(); ++j) {
        if (j != i) {
          nMinus1 = nMinus1.Filter(passBoolCut, {cuts_m[j].column});
        }
      }
      pending.nMinusOneCounts.push_back(nMinus1.Count());
    }

    regionPending_m.emplace(regionName, std::move(pending));
  }
}

void CutflowManager::finalize() {
  if (cuts_m.empty()) return;

  // Retrieve all lazy results (event loop must have completed by now).
  totalEventCount_m = totalCountResult_m.GetValue();

  cutflowCounts_m.clear();
  for (std::size_t i = 0; i < cuts_m.size(); ++i) {
    cutflowCounts_m.emplace_back(cuts_m[i].name,
                                 cutflowCountResults_m[i].GetValue());
  }

  nMinusOneCounts_m.clear();
  for (std::size_t i = 0; i < cuts_m.size(); ++i) {
    nMinusOneCounts_m.emplace_back(cuts_m[i].name,
                                   nMinusOneCountResults_m[i].GetValue());
  }

  // Retrieve per-region results.
  regionResults_m.clear();
  if (regionManager_m) {
    for (const auto &regionName : regionManager_m->getRegionNames()) {
      auto &pending = regionPending_m.at(regionName);
      RegionCutflowResult result;
      result.totalCount = pending.totalCount.GetValue();
      for (std::size_t i = 0; i < cuts_m.size(); ++i) {
        result.cutflowCounts.emplace_back(cuts_m[i].name,
                                          pending.cutflowCounts[i].GetValue());
        result.nMinusOneCounts.emplace_back(
            cuts_m[i].name, pending.nMinusOneCounts[i].GetValue());
      }
      regionResults_m.emplace(regionName, std::move(result));
    }
  }

  // Write histograms to the meta output ROOT file.
  if (dynamic_cast<NullOutputSink *>(metaSink_m) != nullptr) return;

  std::string fileName =
      metaSink_m->resolveOutputFile(*configManager_m, OutputChannel::Meta);
  if (fileName.empty()) return;

  TFile outFile(fileName.c_str(), "UPDATE");
  if (outFile.IsZombie()) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Error,
                    "CutflowManager: failed to open meta output file: " +
                        fileName);
    }
    return;
  }

  const int nCuts = static_cast<int>(cuts_m.size());

  // Sequential cutflow histogram: bins are "total" + one per cut.
  {
    TH1D hist("cutflow", "Cutflow;Cut;Events", nCuts + 1, -0.5,
              static_cast<double>(nCuts) + 0.5);
    hist.GetXaxis()->SetBinLabel(1, "total");
    hist.SetBinContent(1, static_cast<double>(totalEventCount_m));
    for (int b = 0; b < nCuts; ++b) {
      hist.GetXaxis()->SetBinLabel(b + 2,
                                   cutflowCounts_m[b].first.c_str());
      hist.SetBinContent(b + 2,
                         static_cast<double>(cutflowCounts_m[b].second));
    }
    hist.SetDirectory(&outFile);
    hist.Write("cutflow", TObject::kOverwrite);
  }

  // N-1 histogram: one bin per cut.
  {
    TH1D hist("cutflow_nminus1", "N-1 Cutflow;Cut removed;Events", nCuts,
              -0.5, static_cast<double>(nCuts) - 0.5);
    for (int b = 0; b < nCuts; ++b) {
      hist.GetXaxis()->SetBinLabel(b + 1,
                                   nMinusOneCounts_m[b].first.c_str());
      hist.SetBinContent(b + 1,
                         static_cast<double>(nMinusOneCounts_m[b].second));
    }
    hist.SetDirectory(&outFile);
    hist.Write("cutflow_nminus1", TObject::kOverwrite);
  }

  // -------------------------------------------------------------------------
  // Region-aware output: a single TH2D (regions × cuts).
  // Using one large 2-D histogram keeps the output compact and avoids
  // proliferating small individual histograms per region.
  // -------------------------------------------------------------------------
  if (regionManager_m && !regionManager_m->getRegionNames().empty()) {
    const auto &regionNames = regionManager_m->getRegionNames();
    const int nRegions = static_cast<int>(regionNames.size());

    // Axes: X = region (total + one per region), Y = cut step (total + cuts).
    // Bin layout:
    //   X-axis: bin 1 = "global", bins 2..nRegions+1 = declared regions
    //   Y-axis: bin 1 = "total",  bins 2..nCuts+1     = cuts in order
    TH2D hist2d("cutflow_regions",
                "Cutflow per Region;Region;Cut",
                nRegions + 1, -0.5, static_cast<double>(nRegions) + 0.5,
                nCuts + 1,    -0.5, static_cast<double>(nCuts) + 0.5);

    hist2d.GetXaxis()->SetBinLabel(1, "global");
    for (int r = 0; r < nRegions; ++r) {
      hist2d.GetXaxis()->SetBinLabel(r + 2, regionNames[r].c_str());
    }
    hist2d.GetYaxis()->SetBinLabel(1, "total");
    for (int b = 0; b < nCuts; ++b) {
      hist2d.GetYaxis()->SetBinLabel(b + 2, cuts_m[b].name.c_str());
    }

    // Global column (bin 1 on X-axis).
    hist2d.SetBinContent(1, 1, static_cast<double>(totalEventCount_m));
    for (int b = 0; b < nCuts; ++b) {
      hist2d.SetBinContent(1, b + 2,
                           static_cast<double>(cutflowCounts_m[b].second));
    }

    // Per-region columns.
    for (int r = 0; r < nRegions; ++r) {
      const auto &res = regionResults_m.at(regionNames[r]);
      hist2d.SetBinContent(r + 2, 1, static_cast<double>(res.totalCount));
      for (int b = 0; b < nCuts; ++b) {
        hist2d.SetBinContent(
            r + 2, b + 2,
            static_cast<double>(res.cutflowCounts[b].second));
      }
    }

    hist2d.SetDirectory(&outFile);
    hist2d.Write("cutflow_regions", TObject::kOverwrite);
  }

  outFile.Close();
}

void CutflowManager::reportMetadata() {
  if (cuts_m.empty() || !logger_m) return;

  std::ostringstream ss;
  ss << "CutflowManager: sequential cutflow\n";
  ss << "  total: " << totalEventCount_m << "\n";
  for (const auto &[name, count] : cutflowCounts_m) {
    ss << "  after " << name << ": " << count << "\n";
  }
  logger_m->log(ILogger::Level::Info, ss.str());

  std::ostringstream ss2;
  ss2 << "CutflowManager: N-1 table\n";
  for (const auto &[name, count] : nMinusOneCounts_m) {
    ss2 << "  N-1 (" << name << " removed): " << count << "\n";
  }
  logger_m->log(ILogger::Level::Info, ss2.str());

  if (!regionManager_m || regionManager_m->getRegionNames().empty()) return;

  std::ostringstream ss3;
  ss3 << "CutflowManager: per-region sequential cutflow\n";
  for (const auto &regionName : regionManager_m->getRegionNames()) {
    const auto &res = regionResults_m.at(regionName);
    ss3 << "  [" << regionName << "] total: " << res.totalCount << "\n";
    for (const auto &[name, count] : res.cutflowCounts) {
      ss3 << "  [" << regionName << "] after " << name << ": " << count << "\n";
    }
  }
  logger_m->log(ILogger::Level::Info, ss3.str());
}

const std::vector<std::pair<std::string, ULong64_t>> &
CutflowManager::getCutflowCounts() const {
  return cutflowCounts_m;
}

const std::vector<std::pair<std::string, ULong64_t>> &
CutflowManager::getNMinusOneCounts() const {
  return nMinusOneCounts_m;
}

const std::vector<std::pair<std::string, ULong64_t>> &
CutflowManager::getRegionCutflowCounts(const std::string &regionName) const {
  auto it = regionResults_m.find(regionName);
  if (it == regionResults_m.end()) {
    throw std::runtime_error(
        "CutflowManager::getRegionCutflowCounts(): region '" + regionName +
        "' not found. Ensure bindToRegionManager() was called and the "
        "analysis has been run.");
  }
  return it->second.cutflowCounts;
}

const std::vector<std::pair<std::string, ULong64_t>> &
CutflowManager::getRegionNMinusOneCounts(const std::string &regionName) const {
  auto it = regionResults_m.find(regionName);
  if (it == regionResults_m.end()) {
    throw std::runtime_error(
        "CutflowManager::getRegionNMinusOneCounts(): region '" + regionName +
        "' not found. Ensure bindToRegionManager() was called and the "
        "analysis has been run.");
  }
  return it->second.nMinusOneCounts;
}

ULong64_t
CutflowManager::getRegionTotalCount(const std::string &regionName) const {
  auto it = regionResults_m.find(regionName);
  if (it == regionResults_m.end()) {
    throw std::runtime_error(
        "CutflowManager::getRegionTotalCount(): region '" + regionName +
        "' not found. Ensure bindToRegionManager() was called and the "
        "analysis has been run.");
  }
  return it->second.totalCount;
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries()
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string>
CutflowManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  entries["num_cuts"] = std::to_string(cuts_m.size());

  if (!cuts_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < cuts_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << cuts_m[i].name << ':' << cuts_m[i].column;
    }
    entries["cuts"] = ss.str();
  }

  return entries;
}

// ---------------------------------------------------------------------------
// Plugin helper function
// ---------------------------------------------------------------------------
#include <analyzer.h>

std::shared_ptr<CutflowManager> makeCutflowManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<CutflowManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
