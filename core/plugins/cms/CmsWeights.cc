//==============================================================================
// CmsWeights — Generic CMS event weight pattern definitions
//
// Implements configurable weight patterns for common CMS corrections.
// Uses WorkingPointRecipe for fixed-WP b-tag/c-tag weight computation.
//==============================================================================

#include "CmsWeights.h"

#include "CorrectionManager.h"
#include "TaggerWorkingPointManager.h"
#include "WeightManager.h"
#include "WorkingPointRecipe.h"

#include "analyzer.h"

#include <correction.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace cms {

// ===========================================================================
// Internal helpers
// ===========================================================================
namespace {

using CorrectionRef = correction::Correction::Ref;

const CorrectionRef &loadCorrectionByName(const std::string &filePath,
                                          const std::string &name) {
  static std::unordered_map<std::string, CorrectionRef> cache;
  const std::string key = filePath + "::" + name;
  const auto cached = cache.find(key);
  if (cached != cache.end()) return cached->second;
  auto set = correction::CorrectionSet::from_file(filePath);
  return cache.emplace(key, set->at(name)).first->second;
}

/// Check whether a correction of the given name exists in the JSON file.
/// Avoids throwing when the correction is absent (unlike at()).
bool hasCorrectionInFile(const std::string &filePath,
                         const std::string &name) {
  if (filePath.empty() || name.empty()) return false;
  try {
    auto set = correction::CorrectionSet::from_file(filePath);
    set->at(name);
    return true;
  } catch (const std::out_of_range &) {
    return false;
  } catch (...) {
    return false;
  }
}

std::string jetTaggerBase(const std::string &jetTagger) {
  if (jetTagger.find("DeepFlav") != std::string::npos) return "deepJet";
  if (jetTagger.find("RobustParT") != std::string::npos) return "robustParticleTransformer";
  if (jetTagger.find("UParT") != std::string::npos) return "UParTAK4";
  return "particleNet";
}

bool fileExists(const std::string &path) {
  if (path.empty()) return false;
  if (path.rfind("root://", 0) == 0 || path.rfind("/eos/", 0) == 0) return true;
  return std::filesystem::exists(path);
}

void fillDefaultSourceNames(CmsWeightConfig &cfg) {
  if (cfg.btagSourceNames.empty()) cfg.btagSourceNames = defaultBTagSourceNames();
  if (cfg.ctagLightSourceNames.empty()) cfg.ctagLightSourceNames = defaultCTagLightSourceNames();
  if (cfg.ctagWcSourceNames.empty()) cfg.ctagWcSourceNames = defaultCTagWcSourceNames();
  if (cfg.ctagTnpSourceNames.empty()) cfg.ctagTnpSourceNames = defaultCTagTnpSourceNames();
}

} // namespace

// ===========================================================================
// Correction evaluation helpers
// ===========================================================================

float evalCorr3(const std::string &filePath, const std::string &name,
                float absEta, float pt, const std::string &variation) {
  const auto &corr = loadCorrectionByName(filePath, name);
  return static_cast<float>(
      corr->evaluate({static_cast<double>(absEta), static_cast<double>(pt),
                      variation}));
}

// ===========================================================================
// Exported helpers (declared in CmsWeights.h)
// ===========================================================================

std::string triggerTagForYear(const std::string &canonicalYear) {
  if (canonicalYear == "2022_preEE") return "22";
  if (canonicalYear == "2022_postEE") return "22EE";
  if (canonicalYear == "2023_preBPix") return "23";
  if (canonicalYear == "2023_postBPix") return "23post";
  if (canonicalYear == "2024") return "24";
  return "";
}

std::string ctagCorrectionName(const std::string &taggerBase,
                                const std::string &canonicalYear,
                                const std::string &suffix) {
  // Run 2: legacy naming uses deepJet_wp / deepCSV_wp
  const bool isRun2 = (canonicalYear == "2016_PreVFP" ||
                       canonicalYear == "2016_PostVFP" ||
                       canonicalYear == "2017" ||
                       canonicalYear == "2018");
  if (isRun2) {
    return "deepJet_" + suffix;
  }
  // Run 3: modern _light/_wc/_tnp naming
  return taggerBase + "_" + suffix;
}

float productOrOne(const std::vector<float> &weights) {
  float product = 1.0f;
  for (const auto w : weights) product *= w;
  return product;
}

float safeWeightRatio(float numerator, float denominator) {
  if (!std::isfinite(numerator) || !std::isfinite(denominator) ||
      std::fabs(denominator) < 1.0e-6f) return 1.0f;
  return numerator / denominator;
}

std::vector<float> mergeBTagScaleFactors(
    const std::vector<int> &flavour,
    const std::vector<float> &heavy,
    const std::vector<float> &light) {
  std::vector<float> result(flavour.size(), 1.0f);
  for (size_t i = 0; i < flavour.size(); ++i) {
    result[i] = (flavour[i] == 4 || flavour[i] == 5)
                    ? (i < heavy.size() ? heavy[i] : 1.0f)
                    : (i < light.size() ? light[i] : 1.0f);
  }
  return result;
}

std::vector<float> mergeCTagScaleFactors(
    const std::vector<int> &flavour,
    const std::vector<float> &light,
    const std::vector<float> &wc,
    const std::vector<float> &tnp) {
  std::vector<float> result(flavour.size(), 1.0f);
  for (size_t i = 0; i < flavour.size(); ++i) {
    if (flavour[i] == 4) result[i] = i < wc.size() ? wc[i] : 1.0f;
    else if (flavour[i] == 5) result[i] = i < tnp.size() ? tnp[i] : 1.0f;
    else result[i] = i < light.size() ? light[i] : 1.0f;
  }
  return result;
}

std::vector<float> selectElectronRecoScaleFactors(
    const std::vector<float> &pt,
    const std::vector<float> &below20,
    const std::vector<float> &between20And75,
    const std::vector<float> &above75) {
  std::vector<float> result(pt.size(), 1.0f);
  for (size_t i = 0; i < pt.size(); ++i) {
    if (pt[i] < 20.0f) result[i] = i < below20.size() ? below20[i] : 1.0f;
    else if (pt[i] <= 75.0f) result[i] = i < between20And75.size() ? between20And75[i] : 1.0f;
    else result[i] = i < above75.size() ? above75[i] : 1.0f;
  }
  return result;
}

// ===========================================================================
// Pileup weights
// ===========================================================================

void defineCmsPileupWeights(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  const auto &ys = *cfg.yearSettings;
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");

  corrMgr->registerCorrection(
      "cms_pileup_weight", ys.pileupFile, ys.pileupCorrection,
      {"Pileup_nTrueInt"});
  corrMgr->applyCorrection("cms_pileup_weight", {"nominal"}, {}, "pileup_weight_nominal");
  corrMgr->applyCorrection("cms_pileup_weight", {"up"}, {}, "pileup_weight_up");
  corrMgr->applyCorrection("cms_pileup_weight", {"down"}, {}, "pileup_weight_down");
}

// ===========================================================================
// Muon scale factors
// ===========================================================================

void defineCmsMuonScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  const auto &ys = *cfg.yearSettings;
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");

  // Muon ID — always from POG muon_Z.json
  corrMgr->registerCorrection(
      "cms_muon_id_sf", ys.muonFile, ys.muonCorrection,
      {"SelectedMuon_absEta", "SelectedMuon_pt"});
  corrMgr->applyCorrectionVec("cms_muon_id_sf", {"nominal"}, {}, "muon_id_sf_nominal_vec");
  corrMgr->applyCorrectionVec("cms_muon_id_sf", {"systup"}, {}, "muon_id_sf_systup_vec");
  corrMgr->applyCorrectionVec("cms_muon_id_sf", {"systdown"}, {}, "muon_id_sf_systdown_vec");

  // Muon isolation — POG TightPFIso by default, with custom TightMiniIso
  // override when cfg.triggerSfRoot points to analysis-specific JSONs.
  const std::string isoRoot =
      cfg.triggerSfRoot.empty() ? "" : cfg.triggerSfRoot;
  const std::string yearTag = triggerTagForYear(ys.canonicalYear);
  const bool useCustomIso = !yearTag.empty() && !isoRoot.empty();

  if (useCustomIso) {
    const std::string isoFile =
        isoRoot + "/NUM_TightMiniIso_DEN_TightID_" + yearTag + "_schemaV2.json";
    if (fileExists(isoFile)) {
      constexpr float kPtMin = 15.0f;
      an.Define("muon_iso_sf_nominal_vec",
                [isoFile](const std::vector<float> &abseta,
                          const std::vector<float> &pt) {
                  std::vector<float> r(pt.size(), 1.0f);
                  for (size_t i = 0; i < pt.size(); ++i)
                    if (pt[i] >= kPtMin)
                      r[i] = evalCorr3(isoFile, "NUM_TightMiniIso_DEN_TightID",
                                       abseta[i], pt[i], "nominal");
                  return r;
                }, {"SelectedMuon_absEta", "SelectedMuon_pt"})
          ->Define("muon_iso_sf_systup_vec",
                [isoFile](const std::vector<float> &abseta,
                          const std::vector<float> &pt) {
                  std::vector<float> r(pt.size(), 1.0f);
                  for (size_t i = 0; i < pt.size(); ++i)
                    if (pt[i] >= kPtMin)
                      r[i] = evalCorr3(isoFile, "NUM_TightMiniIso_DEN_TightID",
                                       abseta[i], pt[i], "systup");
                  return r;
                }, {"SelectedMuon_absEta", "SelectedMuon_pt"})
          ->Define("muon_iso_sf_systdown_vec",
                [isoFile](const std::vector<float> &abseta,
                          const std::vector<float> &pt) {
                  std::vector<float> r(pt.size(), 1.0f);
                  for (size_t i = 0; i < pt.size(); ++i)
                    if (pt[i] >= kPtMin)
                      r[i] = evalCorr3(isoFile, "NUM_TightMiniIso_DEN_TightID",
                                       abseta[i], pt[i], "systdown");
                  return r;
                }, {"SelectedMuon_absEta", "SelectedMuon_pt"});
      return;
    }
  }

  // POG fallback: TightPFIso from muon_Z.json
  corrMgr->registerCorrection(
      "cms_muon_iso_sf", ys.muonFile, "NUM_TightPFIso_DEN_MediumID",
      {"SelectedMuon_absEta", "SelectedMuon_pt"});
  corrMgr->applyCorrectionVec("cms_muon_iso_sf", {"nominal"}, {}, "muon_iso_sf_nominal_vec");
  corrMgr->applyCorrectionVec("cms_muon_iso_sf", {"systup"}, {}, "muon_iso_sf_systup_vec");
  corrMgr->applyCorrectionVec("cms_muon_iso_sf", {"systdown"}, {}, "muon_iso_sf_systdown_vec");
}

// ===========================================================================
// Electron scale factors
// ===========================================================================

void defineCmsElectronScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  const auto &ys = *cfg.yearSettings;
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");

  // Electron ID
  corrMgr->registerCorrection(
      "cms_electron_id_sf", ys.electronFile, ys.electronCorrection,
      {"SelectedElectron_scEta", "SelectedElectron_pt"});
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sf", ys.electronWorkingPoint}, {},
      "electron_id_sf_nominal_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfup", ys.electronWorkingPoint}, {},
      "electron_id_sf_up_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfdown", ys.electronWorkingPoint}, {},
      "electron_id_sf_down_vec");

  // Electron reconstruction (per-pT bin)
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sf", "RecoBelow20"}, {},
      "electron_reco_sf_below20_nominal_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sf", "Reco20to75"}, {},
      "electron_reco_sf_20to75_nominal_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sf", "RecoAbove75"}, {},
      "electron_reco_sf_above75_nominal_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfup", "RecoBelow20"}, {},
      "electron_reco_sf_below20_up_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfup", "Reco20to75"}, {},
      "electron_reco_sf_20to75_up_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfup", "RecoAbove75"}, {},
      "electron_reco_sf_above75_up_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfdown", "RecoBelow20"}, {},
      "electron_reco_sf_below20_down_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfdown", "Reco20to75"}, {},
      "electron_reco_sf_20to75_down_vec");
  corrMgr->applyCorrectionVec("cms_electron_id_sf",
      {ys.electronYearToken, "sfdown", "RecoAbove75"}, {},
      "electron_reco_sf_above75_down_vec");

  // Merge per-pT bin reco SFs
  an.Define("electron_reco_sf_nominal_vec", selectElectronRecoScaleFactors,
            {"SelectedElectron_pt", "electron_reco_sf_below20_nominal_vec",
             "electron_reco_sf_20to75_nominal_vec",
             "electron_reco_sf_above75_nominal_vec"})
      ->Define("electron_reco_sf_up_vec", selectElectronRecoScaleFactors,
               {"SelectedElectron_pt", "electron_reco_sf_below20_up_vec",
                "electron_reco_sf_20to75_up_vec",
                "electron_reco_sf_above75_up_vec"})
      ->Define("electron_reco_sf_down_vec", selectElectronRecoScaleFactors,
               {"SelectedElectron_pt", "electron_reco_sf_below20_down_vec",
                "electron_reco_sf_20to75_down_vec",
                "electron_reco_sf_above75_down_vec"});
}

// ===========================================================================
// b-tag scale factors (fixed-WP method via WorkingPointRecipe)
// ===========================================================================

void defineCmsBTagScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  fillDefaultSourceNames(const_cast<CmsWeightConfig&>(cfg));

  const auto &ys = *cfg.yearSettings;
  const std::string taggerBase = jetTaggerBase(cfg.jetTagger);
  const bool hasEfficiency = fileExists(cfg.btagEfficiencyFile) &&
                             !cfg.btagEfficiencyCorrection.empty();
  const bool hasSF = fileExists(ys.btvBTagFile);
  const bool apply = cfg.applyBTagWeights && hasEfficiency && hasSF;

  std::vector<std::string> btagCols{
      "btag_weight_nominal", "btag_weight_up", "btag_weight_down",
      "btag_weight_jes_up", "btag_weight_jes_down"};
  for (const auto &src : cfg.btagSourceNames) {
    if (src == "jes") continue;
    btagCols.push_back("btag_weight_" + src + "_up");
    btagCols.push_back("btag_weight_" + src + "_down");
  }

  if (!apply) {
    rdfanalysis::weights::defineConstantColumns(an, btagCols, 1.0f);
    return;
  }

  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
  auto btagMgr = an.getPlugin<TaggerWorkingPointManager>("btagManager");

  // Register corrections
  corrMgr->registerCorrection(
      "cms_btag_heavy_wp", ys.btvBTagFile, taggerBase + "_comb",
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_btag_light_wp", ys.btvBTagFile, taggerBase + "_light",
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_btag_efficiency", cfg.btagEfficiencyFile,
      cfg.btagEfficiencyCorrection,
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});

  const std::vector<std::string> workingPoints{"L", "M", "T", "XT", "XXT"};

  // MC efficiency columns per WP
  const auto effCols = rdfanalysis::weights::createWorkingPointColumns(
      workingPoints, "cms_btag_eff",
      [&](const std::string &wp, const std::string &outCol) {
        corrMgr->applyCorrectionVec("cms_btag_efficiency", {wp}, {}, outCol);
      });

  // Helper: define one b-tag weight variation using WorkingPointRecipe
  auto defineVar = [&](const std::string &label,
                       const std::string &heavyVar,
                       const std::string &lightVar,
                       const std::string &outCol) {
    rdfanalysis::weights::defineFixedWorkingPointVariation(
        *btagMgr, workingPoints, effCols, outCol,
        [&](const std::string &wp) {
          const std::string heavy = "cms_btag_heavy_" + wp + "_" + label;
          const std::string light = "cms_btag_light_" + wp + "_" + label;
          const std::string merged = "cms_btag_" + wp + "_" + label;
          corrMgr->applyCorrectionVec("cms_btag_heavy_wp", {heavyVar, wp}, {}, heavy);
          corrMgr->applyCorrectionVec("cms_btag_light_wp", {lightVar, wp}, {}, light);
          an.Define(merged, mergeBTagScaleFactors,
                    {"Jet_hadronFlavour", heavy, light});
          return merged;
        });
  };

  defineVar("central", "central", "central", "btag_weight_nominal");
  defineVar("up", "up", "up", "btag_weight_up");
  defineVar("down", "down", "down", "btag_weight_down");
  defineVar("jes_up", "up_jes", "up_jes", "btag_weight_jes_up");
  defineVar("jes_down", "down_jes", "down_jes", "btag_weight_jes_down");
  for (const auto &src : cfg.btagSourceNames) {
    if (src == "jes") continue;
    defineVar(src + "_up", "up_" + src, "up_" + src,
              "btag_weight_" + src + "_up");
    defineVar(src + "_down", "down_" + src, "down_" + src,
              "btag_weight_" + src + "_down");
  }
}

// ===========================================================================
// c-tag scale factors (fixed-WP method via WorkingPointRecipe)
// ===========================================================================

void defineCmsCTagScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  fillDefaultSourceNames(const_cast<CmsWeightConfig&>(cfg));

  const auto &ys = *cfg.yearSettings;
  const std::string taggerBase = jetTaggerBase(cfg.jetTagger);
  const bool hasEfficiency = fileExists(cfg.ctagEfficiencyFile) &&
                             !cfg.ctagEfficiencyCorrection.empty();
  const bool hasSF = fileExists(ys.btvCTagFile);
  const bool apply = cfg.applyCTagWeights && hasEfficiency && hasSF;

  std::vector<std::string> ctagCols{
      "ctag_weight_nominal", "ctag_weight_up", "ctag_weight_down",
      "ctag_weight_jes_up", "ctag_weight_jes_down",
      "ctag_weight_jer_up", "ctag_weight_jer_down"};
  for (const auto &src : cfg.ctagLightSourceNames) {
    ctagCols.push_back("ctag_weight_" + src + "_up");
    ctagCols.push_back("ctag_weight_" + src + "_down");
  }
  for (const auto &src : cfg.ctagWcSourceNames) {
    ctagCols.push_back("ctag_weight_" + src + "_up");
    ctagCols.push_back("ctag_weight_" + src + "_down");
  }
  for (const auto &src : cfg.ctagTnpSourceNames) {
    if (src == "jes" || src == "jer") continue;
    ctagCols.push_back("ctag_weight_" + src + "_up");
    ctagCols.push_back("ctag_weight_" + src + "_down");
  }

  if (!apply) {
    rdfanalysis::weights::defineConstantColumns(an, ctagCols, 1.0f);
    return;
  }

  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
  auto ctagMgr = an.getPlugin<TaggerWorkingPointManager>("ctagManager");

  // Resolve c-tag correction names per year (Run 2 vs Run 3 naming)
  const std::string ctagLightName = ctagCorrectionName(taggerBase, ys.canonicalYear, "light");
  const std::string ctagWcName = ctagCorrectionName(taggerBase, ys.canonicalYear, "wc");
  const std::string ctagTnpName = ctagCorrectionName(taggerBase, ys.canonicalYear, "tnp");

  // Verify that the required c-tag corrections exist in the BTV file.
  // Some years (e.g. 2024) may not have the light/wc/tnp entries.
  // For Run 2, also try the deepJet_wp / deepCSV_wp naming if the modern
  // naming is not found.
  bool hasLightCorr = hasCorrectionInFile(ys.btvCTagFile, ctagLightName);
  bool hasWcCorr = hasCorrectionInFile(ys.btvCTagFile, ctagWcName);
  bool hasTnpCorr = hasCorrectionInFile(ys.btvCTagFile, ctagTnpName);

  // Fallback: for Run 2, try the legacy deepJet_wp naming if modern not found
  if ((!hasLightCorr || !hasWcCorr || !hasTnpCorr) &&
      (ys.canonicalYear == "2016_PreVFP" ||
       ys.canonicalYear == "2016_PostVFP" ||
       ys.canonicalYear == "2017" ||
       ys.canonicalYear == "2018")) {
    if (!hasLightCorr) hasLightCorr = hasCorrectionInFile(ys.btvCTagFile, "deepJet_wp");
    if (!hasWcCorr) hasWcCorr = hasCorrectionInFile(ys.btvCTagFile, "deepJet_wp");
    if (!hasTnpCorr) hasTnpCorr = hasCorrectionInFile(ys.btvCTagFile, "deepJet_wp");
  }

  if (!hasLightCorr || !hasWcCorr || !hasTnpCorr) {
    if (ys.canonicalYear == "2024") {
      std::cerr << "INFO: CmsWeights::defineCmsCTagScaleFactors — 2024 BTV "
                << "ctagging.json.gz lacks light/wc/tnp corrections. "
                << "c-tag weights set to unity.\n";
    }
    rdfanalysis::weights::defineConstantColumns(an, ctagCols, 1.0f);
    return;
  }

  // Register corrections with resolved names
  corrMgr->registerCorrection(
      "cms_ctag_light_wp", ys.btvCTagFile, ctagLightName,
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_ctag_wc_wp", ys.btvCTagFile, ctagWcName,
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_ctag_tnp_wp", ys.btvCTagFile, ctagTnpName,
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_ctag_efficiency", cfg.ctagEfficiencyFile,
      cfg.ctagEfficiencyCorrection,
      {"Jet_hadronFlavour", "Jet_abseta", "Jet_pt_corr_nominal"});

  const std::vector<std::string> workingPoints{"L", "M", "T"};

  // MC efficiency columns per WP
  const auto effCols = rdfanalysis::weights::createWorkingPointColumns(
      workingPoints, "cms_ctag_eff",
      [&](const std::string &wp, const std::string &outCol) {
        corrMgr->applyCorrectionVec("cms_ctag_efficiency", {wp}, {}, outCol);
      });

  // Helper: define one c-tag weight variation using WorkingPointRecipe
  auto defineVar = [&](const std::string &label,
                       const std::string &lightVar,
                       const std::string &wcVar,
                       const std::string &tnpVar,
                       const std::string &outCol) {
    rdfanalysis::weights::defineFixedWorkingPointVariation(
        *ctagMgr, workingPoints, effCols, outCol,
        [&](const std::string &wp) {
          const std::string light = "cms_ctag_light_" + wp + "_" + label;
          const std::string wc = "cms_ctag_wc_" + wp + "_" + label;
          const std::string tnp = "cms_ctag_tnp_" + wp + "_" + label;
          const std::string merged = "cms_ctag_" + wp + "_" + label;
          corrMgr->applyCorrectionVec("cms_ctag_light_wp", {lightVar, wp}, {}, light);
          corrMgr->applyCorrectionVec("cms_ctag_wc_wp", {wcVar, wp}, {}, wc);
          corrMgr->applyCorrectionVec("cms_ctag_tnp_wp", {tnpVar, wp}, {}, tnp);
          an.Define(merged, mergeCTagScaleFactors,
                    {"Jet_hadronFlavour", light, wc, tnp});
          return merged;
        });
  };

  defineVar("central", "central", "central", "central", "ctag_weight_nominal");
  defineVar("up", "up", "up", "up", "ctag_weight_up");
  defineVar("down", "down", "down", "down", "ctag_weight_down");
  defineVar("jes_up", "central", "central", "up_jes", "ctag_weight_jes_up");
  defineVar("jes_down", "central", "central", "down_jes", "ctag_weight_jes_down");
  defineVar("jer_up", "central", "central", "up_jer", "ctag_weight_jer_up");
  defineVar("jer_down", "central", "central", "down_jer", "ctag_weight_jer_down");
  for (const auto &src : cfg.ctagLightSourceNames) {
    defineVar(src + "_up", "up_" + src, "central", "central",
              "ctag_weight_" + src + "_up");
    defineVar(src + "_down", "down_" + src, "central", "central",
              "ctag_weight_" + src + "_down");
  }
  for (const auto &src : cfg.ctagWcSourceNames) {
    defineVar(src + "_up", "central", "up_" + src, "central",
              "ctag_weight_" + src + "_up");
    defineVar(src + "_down", "central", "down_" + src, "central",
              "ctag_weight_" + src + "_down");
  }
  for (const auto &src : cfg.ctagTnpSourceNames) {
    if (src == "jes" || src == "jer") continue;
    defineVar(src + "_up", "central", "central", "up_" + src,
              "ctag_weight_" + src + "_up");
    defineVar(src + "_down", "central", "central", "down_" + src,
              "ctag_weight_" + src + "_down");
  }
}

// ===========================================================================
// Trigger scale factors — POG-only by default
// ===========================================================================

void defineCmsTriggerScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC) {
    an.Define("trig_sf_nominal", []() { return 1.0f; }, {});
    an.Define("trig_sf_up", []() { return 1.0f; }, {});
    an.Define("trig_sf_down", []() { return 1.0f; }, {});
    return;
  }

  const bool hasOverride = !cfg.triggerSfRoot.empty();
  if (hasOverride && !cfg.usePogTriggerSFs) {
    // Analysis-specific trigger SFs — analysis code must define them.
    // Default to unity so the pipeline runs without crashing.
    an.Define("trig_sf_nominal", []() { return 1.0f; }, {});
    an.Define("trig_sf_up", []() { return 1.0f; }, {});
    an.Define("trig_sf_down", []() { return 1.0f; }, {});
    return;
  }

  if (cfg.usePogTriggerSFs && cfg.yearSettings) {
    // POG trigger SFs from muon/electron JSON files
    const auto &ys = *cfg.yearSettings;
    auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");

    // Muon trigger SF (from muon_Z.json)
    corrMgr->registerCorrection(
        "cms_muon_trig_sf", ys.muonFile,
        "NUM_MediumID_DEN_TrackerMuons",
        {"SelectedMuon_absEta", "SelectedMuon_pt"});
    corrMgr->applyCorrectionVec("cms_muon_trig_sf", {"nominal"}, {},
                                "muon_trig_sf_nominal_vec");
    corrMgr->applyCorrectionVec("cms_muon_trig_sf", {"systup"}, {},
                                "muon_trig_sf_up_vec");
    corrMgr->applyCorrectionVec("cms_muon_trig_sf", {"systdown"}, {},
                                "muon_trig_sf_down_vec");

    // Electron trigger SF (from electron.json)
    if (ys.electronCorrection == "Electron-ID-SF") {
      corrMgr->registerCorrection(
          "cms_electron_trig_sf", ys.electronFile, ys.electronCorrection,
          {"SelectedElectron_scEta", "SelectedElectron_pt"});
      corrMgr->applyCorrectionVec("cms_electron_trig_sf",
          {ys.electronYearToken, "sf", "trigger"}, {},
          "electron_trig_sf_nominal_vec");
      corrMgr->applyCorrectionVec("cms_electron_trig_sf",
          {ys.electronYearToken, "sfup", "trigger"}, {},
          "electron_trig_sf_up_vec");
      corrMgr->applyCorrectionVec("cms_electron_trig_sf",
          {ys.electronYearToken, "sfdown", "trigger"}, {},
          "electron_trig_sf_down_vec");
    }

    // Per-vector products for trigger SFs
    an.Define("muon_trig_weight_nominal", productOrOne, {"muon_trig_sf_nominal_vec"})
        ->Define("muon_trig_weight_up", productOrOne, {"muon_trig_sf_up_vec"})
        ->Define("muon_trig_weight_down", productOrOne, {"muon_trig_sf_down_vec"})
        ->Define("electron_trig_weight_nominal", productOrOne, {"electron_trig_sf_nominal_vec"})
        ->Define("electron_trig_weight_up", productOrOne, {"electron_trig_sf_up_vec"})
        ->Define("electron_trig_weight_down", productOrOne, {"electron_trig_sf_down_vec"});

    // Combined trigger SF (muon × electron — analysis may redefine)
    an.Define("trig_sf_nominal",
              [](float mu, float el) { return mu * el; },
              {"muon_trig_weight_nominal", "electron_trig_weight_nominal"})
        ->Define("trig_sf_up",
                 [](float mu, float el) { return mu * el; },
                 {"muon_trig_weight_up", "electron_trig_weight_up"})
        ->Define("trig_sf_down",
                 [](float mu, float el) { return mu * el; },
                 {"muon_trig_weight_down", "electron_trig_weight_down"});
  } else {
    an.Define("trig_sf_nominal", []() { return 1.0f; }, {});
    an.Define("trig_sf_up", []() { return 1.0f; }, {});
    an.Define("trig_sf_down", []() { return 1.0f; }, {});
  }
}

// ===========================================================================
// L1 prefiring weights
// ===========================================================================

void defineCmsL1PrefireWeights(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC) {
    an.Define("l1_prefire_weight_nominal", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_up", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_down", []() { return 1.0f; }, {});
    return;
  }

  if (!cfg.yearSettings) {
    an.Define("l1_prefire_weight_nominal", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_up", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_down", []() { return 1.0f; }, {});
    return;
  }

  const auto &ys = *cfg.yearSettings;

  // L1 prefiring only affects 2016 and 2017 eras
  // For 2018+ (Run 3), L1 prefiring was fixed and weights are unity
  const bool isPrefireEra =
      (ys.canonicalYear == "2016_PreVFP" ||
       ys.canonicalYear == "2016_PostVFP" ||
       ys.canonicalYear == "2017");

  if (!isPrefireEra) {
    an.Define("l1_prefire_weight_nominal", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_up", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_down", []() { return 1.0f; }, {});
    return;
  }

  // Check that the L1PreFiringWeight branches exist
  const auto cols = an.GetColumnNames();
  const bool hasL1Nom = std::find(cols.begin(), cols.end(),
                                   "L1PreFiringWeight_Nom") != cols.end();

  if (!hasL1Nom) {
    std::cerr << "INFO: CmsWeights::defineCmsL1PrefireWeights — "
              << "L1PreFiringWeight_Nom branch not found in NanoAOD. "
              << "L1 prefiring weights set to unity.\n";
    an.Define("l1_prefire_weight_nominal", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_up", []() { return 1.0f; }, {})
        ->Define("l1_prefire_weight_down", []() { return 1.0f; }, {});
    return;
  }

  const bool hasL1Up = std::find(cols.begin(), cols.end(),
                                  "L1PreFiringWeight_Up") != cols.end();
  const bool hasL1Dn = std::find(cols.begin(), cols.end(),
                                  "L1PreFiringWeight_Dn") != cols.end();

  if (hasL1Up && hasL1Dn) {
    an.Define("l1_prefire_weight_nominal",
              [](float nom) { return std::isfinite(nom) ? nom : 1.0f; },
              {"L1PreFiringWeight_Nom"})
        ->Define("l1_prefire_weight_up",
                 [](float up) { return std::isfinite(up) ? up : 1.0f; },
                 {"L1PreFiringWeight_Up"})
        ->Define("l1_prefire_weight_down",
                 [](float dn) { return std::isfinite(dn) ? dn : 1.0f; },
                 {"L1PreFiringWeight_Dn"});
  } else {
    // Only nominal available — use symmetric up/down = nom^2 / nom variation
    an.Define("l1_prefire_weight_nominal",
              [](float nom) { return std::isfinite(nom) ? nom : 1.0f; },
              {"L1PreFiringWeight_Nom"})
        ->Define("l1_prefire_weight_up",
                 [](float nom) {
                   const float safe = std::isfinite(nom) ? nom : 1.0f;
                   return safe * safe;  // simplified up variation
                 },
                 {"L1PreFiringWeight_Nom"})
        ->Define("l1_prefire_weight_down",
                 [](float nom) {
                   const float safe = std::isfinite(nom) ? nom : 1.0f;
                   return 2.0f * safe - safe * safe;  // simplified down variation
                 },
                 {"L1PreFiringWeight_Nom"});
  }
}

// ===========================================================================
// Per-vector SF product columns
// ===========================================================================

void defineCmsVecProductColumns(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC) return;

  // Muon ID per-object -> event-level product
  an.Define("muon_id_weight_nominal", productOrOne, {"muon_id_sf_nominal_vec"})
      ->Define("muon_id_weight_systup", productOrOne, {"muon_id_sf_systup_vec"})
      ->Define("muon_id_weight_systdown", productOrOne, {"muon_id_sf_systdown_vec"});

  // Muon ISO
  an.Define("muon_iso_weight_nominal", productOrOne, {"muon_iso_sf_nominal_vec"})
      ->Define("muon_iso_weight_systup", productOrOne, {"muon_iso_sf_systup_vec"})
      ->Define("muon_iso_weight_systdown", productOrOne, {"muon_iso_sf_systdown_vec"});

  // Electron ID
  an.Define("electron_id_weight_nominal", productOrOne, {"electron_id_sf_nominal_vec"})
      ->Define("electron_id_weight_up", productOrOne, {"electron_id_sf_up_vec"})
      ->Define("electron_id_weight_down", productOrOne, {"electron_id_sf_down_vec"});

  // Electron reco
  an.Define("electron_reco_weight_nominal", productOrOne, {"electron_reco_sf_nominal_vec"})
      ->Define("electron_reco_weight_up", productOrOne, {"electron_reco_sf_up_vec"})
      ->Define("electron_reco_weight_down", productOrOne, {"electron_reco_sf_down_vec"});
}

// ===========================================================================
// Nominal weight composition
// ===========================================================================

void defineCmsNominalWeight(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC) {
    an.Define("weight_nominal", []() { return 1.0f; }, {});
    return;
  }

  auto wm = an.getPlugin<WeightManager>("weightManager");

  // Register scale factor components
  wm->addScaleFactor("genWeight", cfg.yearSettings ? "genWeight_nominal" : "genWeight");
  wm->addScaleFactor("pileup", "pileup_weight_nominal");
  wm->addScaleFactor("l1_prefire", "l1_prefire_weight_nominal");
  wm->addScaleFactor("muon_id", "muon_id_weight_nominal");
  wm->addScaleFactor("muon_iso", "muon_iso_weight_nominal");
  wm->addScaleFactor("electron_id", "electron_id_weight_nominal");
  wm->addScaleFactor("electron_reco", "electron_reco_weight_nominal");
  wm->addScaleFactor("trigger", "trig_sf_nominal");

  // Systematic weight variations
  wm->addWeightVariation("pileup", "pileup_weight_up", "pileup_weight_down");
  wm->addWeightVariation("l1_prefire", "l1_prefire_weight_up",
                          "l1_prefire_weight_down");
  wm->addWeightVariation("muon_id", "muon_id_weight_systup",
                          "muon_id_weight_systdown");
  wm->addWeightVariation("muon_iso", "muon_iso_weight_systup",
                          "muon_iso_weight_systdown");
  wm->addWeightVariation("electron_id", "electron_id_weight_up",
                          "electron_id_weight_down");
  wm->addWeightVariation("electron_reco", "electron_reco_weight_up",
                          "electron_reco_weight_down");
  wm->addWeightVariation("trigger", "trig_sf_up", "trig_sf_down");

  // b-tag variations
  wm->addWeightVariation("btag", "btag_weight_up", "btag_weight_down");
  wm->addWeightVariation("btag_jes", "btag_weight_jes_up",
                          "btag_weight_jes_down");
  for (const auto &src : cfg.btagSourceNames) {
    if (src == "jes") continue;
    wm->addWeightVariation("btag_" + src, "btag_weight_" + src + "_up",
                            "btag_weight_" + src + "_down");
  }

  // c-tag variations
  wm->addWeightVariation("ctag", "ctag_weight_up", "ctag_weight_down");
  wm->addWeightVariation("ctag_jes", "ctag_weight_jes_up",
                          "ctag_weight_jes_down");
  wm->addWeightVariation("ctag_jer", "ctag_weight_jer_up",
                          "ctag_weight_jer_down");
  for (const auto &src : cfg.ctagLightSourceNames) {
    wm->addWeightVariation("ctag_" + src, "ctag_weight_" + src + "_up",
                            "ctag_weight_" + src + "_down");
  }
  for (const auto &src : cfg.ctagWcSourceNames) {
    wm->addWeightVariation("ctag_" + src, "ctag_weight_" + src + "_up",
                            "ctag_weight_" + src + "_down");
  }
  for (const auto &src : cfg.ctagTnpSourceNames) {
    if (src == "jes" || src == "jer") continue;
    wm->addWeightVariation("ctag_" + src, "ctag_weight_" + src + "_up",
                            "ctag_weight_" + src + "_down");
  }

  wm->defineNominalWeight("weight_nominal");
}

// ===========================================================================
// Full standard weights setup
// ===========================================================================

void setupCmsStandardWeights(Analyzer &an, const CmsWeightConfig &cfg) {
  fillDefaultSourceNames(const_cast<CmsWeightConfig&>(cfg));

  // Configure tagger working-point manager branches from year settings
  if (cfg.yearSettings) {
    const auto &ys = *cfg.yearSettings;

    // b-tag: object columns + tagger branch + input collection + WPs
    setupBtagTaggerManager(an, ys, cfg.btagWorkingPoints);

    // c-tag: object columns + tagger branches + input collection + WPs
    setupCtagTaggerManager(an, ys, cfg.ctagWorkingPoints);
  }

  // Define all weight components in order
  defineCmsPileupWeights(an, cfg);
  defineCmsL1PrefireWeights(an, cfg);
  defineCmsMuonScaleFactors(an, cfg);
  defineCmsElectronScaleFactors(an, cfg);
  defineCmsBTagScaleFactors(an, cfg);
  defineCmsCTagScaleFactors(an, cfg);
  defineCmsTriggerScaleFactors(an, cfg);
  defineCmsVecProductColumns(an, cfg);
  defineCmsNominalWeight(an, cfg);
}

} // namespace cms
