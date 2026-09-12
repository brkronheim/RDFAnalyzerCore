//==============================================================================
// CmsWeights — Generic CMS event weight pattern definitions
//
// Implements configurable weight patterns for common CMS corrections.
// Uses WorkingPointRecipe for fixed-WP b-tag/c-tag weight computation.
//==============================================================================

#include "CmsWeights.h"

#include "CorrectionManager.h"
#include "PhysicsObjectCollection.h"
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

void defineUnityVectorColumns(Analyzer &an,
                              const std::vector<std::string> &columns) {
  for (const auto &column : columns) {
    an.Define(column, []() { return ROOT::VecOps::RVec<float>{}; }, {});
  }
}

ROOT::VecOps::RVec<Float_t> boundedAbsEta(
    const ROOT::VecOps::RVec<Float_t> &eta, Float_t upperBound) {
  const Float_t safeUpperBound = std::nextafter(upperBound, 0.0f);
  ROOT::VecOps::RVec<Float_t> result(eta.size(), 0.0f);
  for (std::size_t index = 0; index < eta.size(); ++index) {
    const Float_t value = std::fabs(eta[index]);
    result[index] = std::isfinite(value)
                        ? std::clamp(value, 0.0f, safeUpperBound)
                        : 0.0f;
  }
  return result;
}

ROOT::VecOps::RVec<Float_t> boundedEta(
    const ROOT::VecOps::RVec<Float_t> &eta, Float_t upperBound) {
  const Float_t safeUpperBound = std::nextafter(upperBound, 0.0f);
  ROOT::VecOps::RVec<Float_t> result(eta.size(), 0.0f);
  for (std::size_t index = 0; index < eta.size(); ++index) {
    result[index] = std::isfinite(eta[index])
                        ? std::clamp(eta[index], -safeUpperBound,
                                     safeUpperBound)
                        : 0.0f;
  }
  return result;
}

ROOT::VecOps::RVec<Float_t> boundedBTagPt(
    const ROOT::VecOps::RVec<Float_t> &pt) {
  constexpr Float_t kMinPt = 20.0f;
  const Float_t safeMaxPt = std::nextafter(1000.0f, 0.0f);
  ROOT::VecOps::RVec<Float_t> result(pt.size(), kMinPt);
  for (std::size_t index = 0; index < pt.size(); ++index) {
    result[index] = std::isfinite(pt[index])
                        ? std::clamp(pt[index], kMinPt, safeMaxPt)
                        : kMinPt;
  }
  return result;
}

Float_t productSelectedJetScaleFactors(
    const PhysicsObjectCollection &jets,
    const ROOT::VecOps::RVec<Float_t> &scaleFactors) {
  Float_t weight = 1.0f;
  for (std::size_t position = 0; position < jets.size(); ++position) {
    const Int_t index = jets.index(position);
    if (index >= 0 && static_cast<std::size_t>(index) < scaleFactors.size()) {
      weight *= scaleFactors[static_cast<std::size_t>(index)];
    }
  }
  return weight;
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

float productOrOneRVec(const ROOT::VecOps::RVec<float> &weights) {
  float product = 1.0f;
  for (const auto w : weights) product *= w;
  return product;
}

float safeWeightRatio(float numerator, float denominator) {
  if (!std::isfinite(numerator) || !std::isfinite(denominator) ||
      std::fabs(denominator) < 1.0e-6f) return 1.0f;
  return numerator / denominator;
}

ROOT::VecOps::RVec<float> mergeBTagScaleFactors(
    const ROOT::VecOps::RVec<int> &flavour,
    const ROOT::VecOps::RVec<float> &heavy,
    const ROOT::VecOps::RVec<float> &light) {
  ROOT::VecOps::RVec<float> result(flavour.size(), 1.0f);
  for (size_t i = 0; i < flavour.size(); ++i) {
    result[i] = (flavour[i] == 4 || flavour[i] == 5)
                    ? (i < heavy.size() ? heavy[i] : 1.0f)
                    : (i < light.size() ? light[i] : 1.0f);
  }
  return result;
}

ROOT::VecOps::RVec<float> mergeCTagScaleFactors(
    const ROOT::VecOps::RVec<int> &flavour,
    const ROOT::VecOps::RVec<float> &light,
    const ROOT::VecOps::RVec<float> &wc,
    const ROOT::VecOps::RVec<float> &tnp) {
  ROOT::VecOps::RVec<float> result(flavour.size(), 1.0f);
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
  if (!cfg.isMC || !cfg.yearSettings || !cfg.applyPileupWeights) {
    rdfanalysis::weights::defineConstantColumns(
        an, {"pileup_weight_nominal", "pileup_weight_up",
             "pileup_weight_down"},
        1.0f);
    return;
  }
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
  if (!cfg.isMC || !cfg.yearSettings || !cfg.applyMuonScaleFactors) {
    defineUnityVectorColumns(
        an, {"muon_id_sf_nominal_vec", "muon_id_sf_systup_vec",
             "muon_id_sf_systdown_vec", "muon_iso_sf_nominal_vec",
             "muon_iso_sf_systup_vec", "muon_iso_sf_systdown_vec"});
    return;
  }
  const auto &ys = *cfg.yearSettings;
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");

  an.Define("SelectedMuon_absEta_sf",
            [](const ROOT::VecOps::RVec<Float_t> &eta) {
              return boundedAbsEta(eta, 2.4f);
            },
            {"SelectedMuon_absEta"});
  an.Define("SelectedMuon_eta_sf",
            [](const ROOT::VecOps::RVec<Float_t> &eta) {
              return boundedEta(eta, 2.4f);
            },
            {"SelectedMuon_eta"});

  // Muon ID — always from POG muon_Z.json
  corrMgr->registerCorrection(
      "cms_muon_id_sf", ys.muonFile, ys.muonCorrection,
      {"SelectedMuon_eta_sf", "SelectedMuon_pt"});
  corrMgr->applyCorrectionVec("cms_muon_id_sf", {"nominal"}, {}, "muon_id_sf_nominal_vec");
  corrMgr->applyCorrectionVec("cms_muon_id_sf", {"systup"}, {}, "muon_id_sf_systup_vec");
  corrMgr->applyCorrectionVec("cms_muon_id_sf", {"systdown"}, {}, "muon_id_sf_systdown_vec");

  // Muon isolation — POG TightPFIso by default, with an optional POG
  // correction-name override or custom correctionlib payload.
  const std::string isoRoot =
      cfg.triggerSfRoot.empty() ? "" : cfg.triggerSfRoot;
  const std::string yearTag = triggerTagForYear(ys.canonicalYear);
  const bool useCustomIso = !cfg.muonIsoSfFile.empty() ||
                            (!yearTag.empty() && !isoRoot.empty());

  if (useCustomIso) {
    const std::string isoFile = cfg.muonIsoSfFile.empty()
        ? isoRoot + "/NUM_TightMiniIso_DEN_TightID_" + yearTag +
              "_schemaV2.json"
        : cfg.muonIsoSfFile;
    if (!fileExists(isoFile)) {
      throw std::runtime_error(
          "CmsWeights::defineCmsMuonScaleFactors: custom muon isolation "
          "payload was requested but is missing: '" + isoFile + "'.");
    }
    const std::string isoCorrection = cfg.muonIsoCorrection.empty()
        ? "NUM_TightMiniIso_DEN_TightID" : cfg.muonIsoCorrection;

    constexpr float kPtMin = 15.0f;
    an.Define("muon_iso_sf_nominal_vec",
              [isoFile, isoCorrection](const std::vector<float> &abseta,
                        const std::vector<float> &pt) {
                std::vector<float> r(pt.size(), 1.0f);
                for (size_t i = 0; i < pt.size(); ++i)
                  if (pt[i] >= kPtMin)
                    r[i] = evalCorr3(isoFile, isoCorrection,
                                     abseta[i], pt[i], "nominal");
                return r;
              }, {"SelectedMuon_absEta_sf", "SelectedMuon_pt"})
        ->Define("muon_iso_sf_systup_vec",
              [isoFile, isoCorrection](const std::vector<float> &abseta,
                        const std::vector<float> &pt) {
                std::vector<float> r(pt.size(), 1.0f);
                for (size_t i = 0; i < pt.size(); ++i)
                  if (pt[i] >= kPtMin)
                    r[i] = evalCorr3(isoFile, isoCorrection,
                                     abseta[i], pt[i], "systup");
                return r;
              }, {"SelectedMuon_absEta_sf", "SelectedMuon_pt"})
        ->Define("muon_iso_sf_systdown_vec",
              [isoFile, isoCorrection](const std::vector<float> &abseta,
                        const std::vector<float> &pt) {
                std::vector<float> r(pt.size(), 1.0f);
                for (size_t i = 0; i < pt.size(); ++i)
                  if (pt[i] >= kPtMin)
                    r[i] = evalCorr3(isoFile, isoCorrection,
                                     abseta[i], pt[i], "systdown");
                return r;
              }, {"SelectedMuon_absEta_sf", "SelectedMuon_pt"});
    return;
  }

  // Standard POG isolation SF from muon_Z.json. Its eta input is signed,
  // like the medium-ID correction above.
  const std::string isoCorrection = cfg.muonIsoCorrection.empty()
      ? "NUM_TightPFIso_DEN_MediumID" : cfg.muonIsoCorrection;
  corrMgr->registerCorrection(
      "cms_muon_iso_sf", ys.muonFile, isoCorrection,
      {"SelectedMuon_eta_sf", "SelectedMuon_pt"});
  corrMgr->applyCorrectionVec("cms_muon_iso_sf", {"nominal"}, {}, "muon_iso_sf_nominal_vec");
  corrMgr->applyCorrectionVec("cms_muon_iso_sf", {"systup"}, {}, "muon_iso_sf_systup_vec");
  corrMgr->applyCorrectionVec("cms_muon_iso_sf", {"systdown"}, {}, "muon_iso_sf_systdown_vec");
}

// ===========================================================================
// Electron scale factors
// ===========================================================================

void defineCmsElectronScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings || !cfg.applyElectronScaleFactors) {
    defineUnityVectorColumns(
        an, {"electron_id_sf_nominal_vec", "electron_id_sf_up_vec",
             "electron_id_sf_down_vec", "electron_reco_sf_below20_nominal_vec",
             "electron_reco_sf_20to75_nominal_vec",
             "electron_reco_sf_above75_nominal_vec",
             "electron_reco_sf_below20_up_vec",
             "electron_reco_sf_20to75_up_vec",
             "electron_reco_sf_above75_up_vec",
             "electron_reco_sf_below20_down_vec",
             "electron_reco_sf_20to75_down_vec",
             "electron_reco_sf_above75_down_vec",
             "electron_reco_sf_nominal_vec", "electron_reco_sf_up_vec",
             "electron_reco_sf_down_vec"});
    return;
  }
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
// b-tag scale factors (fixed-WP or continuous-shape method)
// ===========================================================================

void defineCmsBTagScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  fillDefaultSourceNames(const_cast<CmsWeightConfig&>(cfg));

  const auto &ys = *cfg.yearSettings;
  const std::string taggerBase = jetTaggerBase(cfg.jetTagger);

  std::vector<std::string> btagCols{
      "btag_weight_nominal", "btag_weight_up", "btag_weight_down"};
  const bool hasJesSource =
      std::find(cfg.btagSourceNames.begin(), cfg.btagSourceNames.end(),
                "jes") != cfg.btagSourceNames.end();
  if (cfg.btagMethod == BTagMethod::Shape || hasJesSource) {
    btagCols.push_back("btag_weight_jes_up");
    btagCols.push_back("btag_weight_jes_down");
  }
  for (const auto &src : cfg.btagSourceNames) {
    if (src == "jes") continue;
    btagCols.push_back("btag_weight_" + src + "_up");
    btagCols.push_back("btag_weight_" + src + "_down");
  }

  if (cfg.btagMethod == BTagMethod::Shape) {
    if (!cfg.applyBTagScaleFactors) {
      rdfanalysis::weights::defineConstantColumns(an, btagCols, 1.0f);
      return;
    }

    const std::string shapeName = cfg.btagShapeCorrection.empty()
                                       ? taggerBase + "_shape"
                                       : cfg.btagShapeCorrection;
    const std::string shapeFile = cfg.btagShapeFile.empty()
                                      ? ys.btvBTagFile
                                      : cfg.btagShapeFile;
    const bool hasShape = fileExists(shapeFile) &&
                          hasCorrectionInFile(shapeFile, shapeName);
    if (!hasShape) {
      throw std::runtime_error(
          "CmsWeights::defineCmsBTagScaleFactors: continuous b-tag "
          "correction '" + shapeName + "' was not found in '" + shapeFile +
          "'. Provide btagShapeFile/btagShapeCorrection or disable shape "
          "scale factors.");
    }

    auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
    corrMgr->registerCorrection(
        "cms_btag_shape", shapeFile, shapeName,
        {"Jet_hadronFlavour_int", "Jet_abseta", "Jet_pt_corr_nominal",
       ys.btagBranch});

    const auto defineShapeVariation =
        [&](const std::string &variation, const std::string &outputColumn) {
          const std::string scaleFactorColumn =
              "cms_btag_shape_" + variation;
          corrMgr->applyCorrectionVec("cms_btag_shape", {variation}, {},
                                      scaleFactorColumn);
          an.Define(outputColumn, productSelectedJetScaleFactors,
                    {"TaggerJets", scaleFactorColumn});
        };

    defineShapeVariation("central", "btag_weight_nominal");
    defineShapeVariation("up", "btag_weight_up");
    defineShapeVariation("down", "btag_weight_down");
    defineShapeVariation("up_jes", "btag_weight_jes_up");
    defineShapeVariation("down_jes", "btag_weight_jes_down");
    for (const auto &source : cfg.btagSourceNames) {
      if (source == "jes") continue;
      defineShapeVariation("up_" + source,
                           "btag_weight_" + source + "_up");
      defineShapeVariation("down_" + source,
                           "btag_weight_" + source + "_down");
    }
    return;
  }

  const bool hasEfficiency = fileExists(cfg.btagEfficiencyFile) &&
                             !cfg.btagEfficiencyCorrection.empty() &&
                             hasCorrectionInFile(cfg.btagEfficiencyFile,
                                                 cfg.btagEfficiencyCorrection);
  const bool hasSF = fileExists(ys.btvBTagFile);
  if (cfg.applyBTagWeights && (!hasEfficiency || !hasSF)) {
    throw std::runtime_error(
        "CmsWeights::defineCmsBTagScaleFactors: fixed-WP b-tag weights were "
        "requested, but the efficiency correction or BTV scale-factor file "
        "is missing. Efficiency file='" + cfg.btagEfficiencyFile +
        "', correction='" + cfg.btagEfficiencyCorrection + "', SF file='" +
        ys.btvBTagFile + "'.");
  }
  const bool apply = cfg.applyBTagWeights;

  if (!apply) {
    rdfanalysis::weights::defineConstantColumns(an, btagCols, 1.0f);
    return;
  }

  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
  auto btagMgr = an.getPlugin<TaggerWorkingPointManager>("btagManager");

  an.Define("Jet_abseta_btag",
            [](const ROOT::VecOps::RVec<Float_t> &eta) {
              return boundedAbsEta(eta, 2.5f);
            },
            {"Jet_eta"});
  an.Define("Jet_pt_corr_nominal_btag", boundedBTagPt,
            {"Jet_pt_corr_nominal"});
  an.Define("Jet_abseta_btag_eff",
            [](const ROOT::VecOps::RVec<Float_t> &eta) {
              return boundedAbsEta(eta, 2.4f);
            },
            {"Jet_eta"});
  an.Define("Jet_pt_corr_nominal_btag_eff", boundedBTagPt,
            {"Jet_pt_corr_nominal"});

  // Register corrections
  corrMgr->registerCorrection(
      "cms_btag_heavy_wp", ys.btvBTagFile, taggerBase + "_comb",
      {"Jet_hadronFlavour_int", "Jet_abseta_btag",
       "Jet_pt_corr_nominal_btag"});
  corrMgr->registerCorrection(
      "cms_btag_light_wp", ys.btvBTagFile, taggerBase + "_light",
      {"Jet_hadronFlavour_int", "Jet_abseta_btag",
       "Jet_pt_corr_nominal_btag"});
  corrMgr->registerCorrection(
      "cms_btag_efficiency", cfg.btagEfficiencyFile,
      cfg.btagEfficiencyCorrection,
      {"Jet_hadronFlavour_int", "Jet_abseta_btag_eff",
       "Jet_pt_corr_nominal_btag_eff"});

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
                    {"Jet_hadronFlavour_int", heavy, light});
          return merged;
        });
  };

  defineVar("central", "central", "central", "btag_weight_nominal");
  defineVar("up", "up", "up", "btag_weight_up");
  defineVar("down", "down", "down", "btag_weight_down");
  if (hasJesSource) {
    defineVar("jes_up", "up_jes", "up_jes", "btag_weight_jes_up");
    defineVar("jes_down", "down_jes", "down_jes", "btag_weight_jes_down");
  }
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
      {"Jet_hadronFlavour_int", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_ctag_wc_wp", ys.btvCTagFile, ctagWcName,
      {"Jet_hadronFlavour_int", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_ctag_tnp_wp", ys.btvCTagFile, ctagTnpName,
      {"Jet_hadronFlavour_int", "Jet_abseta", "Jet_pt_corr_nominal"});
  corrMgr->registerCorrection(
      "cms_ctag_efficiency", cfg.ctagEfficiencyFile,
      cfg.ctagEfficiencyCorrection,
      {"Jet_hadronFlavour_int", "Jet_abseta", "Jet_pt_corr_nominal"});

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
                    {"Jet_hadronFlavour_int", light, wc, tnp});
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
  if (!cfg.isMC || !cfg.applyTriggerScaleFactors) {
    an.Define("trig_sf_nominal", []() { return 1.0f; }, {});
    an.Define("trig_sf_up", []() { return 1.0f; }, {});
    an.Define("trig_sf_down", []() { return 1.0f; }, {});
    return;
  }

  const bool hasDimuonTriggerFiles =
      !cfg.dimuonTriggerMu17File.empty() || !cfg.dimuonTriggerMu8File.empty();
  if (hasDimuonTriggerFiles) {
    if (cfg.dimuonTriggerMu17File.empty() ||
        cfg.dimuonTriggerMu8File.empty()) {
      throw std::runtime_error(
          "CmsWeights::defineCmsTriggerScaleFactors: both Mu17 and Mu8 "
          "dimuon trigger payloads must be configured.");
    }
    if (!fileExists(cfg.dimuonTriggerMu17File) ||
        !fileExists(cfg.dimuonTriggerMu8File)) {
      throw std::runtime_error(
          "CmsWeights::defineCmsTriggerScaleFactors: a configured dimuon "
          "trigger payload is missing. Mu17='" + cfg.dimuonTriggerMu17File +
          "', Mu8='" + cfg.dimuonTriggerMu8File + "'.");
    }

    auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
    corrMgr->registerCorrection(
        "cms_dimuon_trigger_mu17", cfg.dimuonTriggerMu17File,
        cfg.dimuonTriggerMu17Correction,
        {"SelectedMuonLead_absEta", "Muon1_pt"});
    corrMgr->registerCorrection(
        "cms_dimuon_trigger_mu8", cfg.dimuonTriggerMu8File,
        cfg.dimuonTriggerMu8Correction,
        {"SelectedMuonSublead_absEta", "Muon2_pt"});

    const auto applyLegVariations = [&](const std::string &correction,
                                        const std::string &prefix) {
      corrMgr->applyCorrection(correction, {"nominal"}, {},
                               prefix + "_nominal");
      corrMgr->applyCorrection(correction, {"systup"}, {}, prefix + "_up");
      corrMgr->applyCorrection(correction, {"systdown"}, {}, prefix + "_down");
    };
    applyLegVariations("cms_dimuon_trigger_mu17", "dimuon_trigger_mu17");
    applyLegVariations("cms_dimuon_trigger_mu8", "dimuon_trigger_mu8");

    const auto defineEventVariation = [&](const std::string &variation) {
      const std::string mu17 = "dimuon_trigger_mu17_" + variation;
      const std::string mu8 = "dimuon_trigger_mu8_" + variation;
      const std::string output = "trig_sf_" + variation;
      an.Define(
          output,
          [](Int_t nGoodMuons, Float_t leadPt, Float_t subleadPt,
             Float_t mu17Sf, Float_t mu8Sf) {
            if (nGoodMuons != 2) return 1.0f;
            const Float_t leadFactor = leadPt >= 17.0f ? mu17Sf : 1.0f;
            const Float_t subleadFactor = subleadPt >= 15.0f ? mu8Sf : 1.0f;
            return leadFactor * subleadFactor;
          },
          {"nGoodMuons", "Muon1_pt", "Muon2_pt", mu17, mu8});
    };
    defineEventVariation("nominal");
    defineEventVariation("up");
    defineEventVariation("down");
    return;
  }

  if (cfg.triggerSfFile.empty() || cfg.triggerSfCorrection.empty() ||
      !fileExists(cfg.triggerSfFile)) {
    std::cerr << "WARNING: CmsWeights::defineCmsTriggerScaleFactors — "
                 "no path-specific trigger SF payload is configured; "
                 "trigger weights are unity.\n";
    an.Define("trig_sf_nominal", []() { return 1.0f; }, {});
    an.Define("trig_sf_up", []() { return 1.0f; }, {});
    an.Define("trig_sf_down", []() { return 1.0f; }, {});
    return;
  }

  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
  corrMgr->registerCorrection("cms_trigger_sf", cfg.triggerSfFile,
                              cfg.triggerSfCorrection,
                              {"SelectedMuon_absEta", "SelectedMuon_pt"});
  corrMgr->applyCorrectionVec("cms_trigger_sf", {"nominal"}, {},
                              "trigger_sf_nominal_vec");
  corrMgr->applyCorrectionVec("cms_trigger_sf", {"systup"}, {},
                              "trigger_sf_up_vec");
  corrMgr->applyCorrectionVec("cms_trigger_sf", {"systdown"}, {},
                              "trigger_sf_down_vec");
    an.Define("trig_sf_nominal", productOrOneRVec, {"trigger_sf_nominal_vec"})
      ->Define("trig_sf_up", productOrOneRVec, {"trigger_sf_up_vec"})
      ->Define("trig_sf_down", productOrOneRVec, {"trigger_sf_down_vec"});
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
    an.Define("muon_id_weight_nominal", productOrOneRVec, {"muon_id_sf_nominal_vec"})
      ->Define("muon_id_weight_systup", productOrOneRVec, {"muon_id_sf_systup_vec"})
      ->Define("muon_id_weight_systdown", productOrOneRVec, {"muon_id_sf_systdown_vec"});

  // Muon ISO
    an.Define("muon_iso_weight_nominal", productOrOneRVec, {"muon_iso_sf_nominal_vec"})
      ->Define("muon_iso_weight_systup", productOrOneRVec, {"muon_iso_sf_systup_vec"})
      ->Define("muon_iso_weight_systdown", productOrOneRVec, {"muon_iso_sf_systdown_vec"});

  // Electron ID
    an.Define("electron_id_weight_nominal", productOrOneRVec, {"electron_id_sf_nominal_vec"})
      ->Define("electron_id_weight_up", productOrOneRVec, {"electron_id_sf_up_vec"})
      ->Define("electron_id_weight_down", productOrOneRVec, {"electron_id_sf_down_vec"});

  // Electron reco
    an.Define("electron_reco_weight_nominal", productOrOneRVec, {"electron_reco_sf_nominal_vec"})
      ->Define("electron_reco_weight_up", productOrOneRVec, {"electron_reco_sf_up_vec"})
      ->Define("electron_reco_weight_down", productOrOneRVec, {"electron_reco_sf_down_vec"});
}

// ===========================================================================
// Nominal weight composition
// ===========================================================================

void defineCmsNominalWeight(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC) {
    an.Define("weight_nominal", []() { return 1.0; }, {});
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
  wm->addScaleFactor("btag", "btag_weight_nominal");
  wm->addScaleFactor("ctag", "ctag_weight_nominal");

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
  const bool hasJesSource =
      std::find(cfg.btagSourceNames.begin(), cfg.btagSourceNames.end(),
                "jes") != cfg.btagSourceNames.end();
  if (cfg.btagMethod == BTagMethod::Shape || hasJesSource) {
    wm->addWeightVariation("btag_jes", "btag_weight_jes_up",
                            "btag_weight_jes_down");
  }
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

void setupCmsStandardWeights(Analyzer &an, CmsWeightConfig cfg) {
  fillDefaultSourceNames(cfg);

  // Configure tagger working-point manager branches from year settings
  if (cfg.yearSettings) {
    const auto &ys = *cfg.yearSettings;

    // b-tag: object columns + tagger branch + input collection + WPs
    setupBtagTaggerManager(an, ys, cfg.btagWorkingPoints);

    // c-tag setup is only needed when c-tag weights are enabled.
    if (cfg.applyCTagWeights) {
      setupCtagTaggerManager(an, ys, cfg.ctagWorkingPoints);
    }
  }

  // Define all weight components in order
  defineCmsPileupWeights(an, cfg);
  defineCmsL1PrefireWeights(an, cfg);
  defineCmsMuonScaleFactors(an, cfg);
  defineCmsElectronScaleFactors(an, cfg);
  defineCmsBTagScaleFactors(an, cfg);
  defineCmsCTagScaleFactors(an, cfg);
  if (cfg.btagMethod == BTagMethod::FixedWP && cfg.applyBTagWeights) {
    if (auto btagMgr = an.findPlugin<TaggerWorkingPointManager>("btagManager")) {
      btagMgr->execute();
    }
  }
  if (cfg.ctagMethod == BTagMethod::FixedWP && cfg.applyCTagWeights) {
    if (auto ctagMgr = an.findPlugin<TaggerWorkingPointManager>("ctagManager")) {
      ctagMgr->execute();
    }
  }
  defineCmsTriggerScaleFactors(an, cfg);
  defineCmsVecProductColumns(an, cfg);
  defineCmsNominalWeight(an, cfg);
}

} // namespace cms
