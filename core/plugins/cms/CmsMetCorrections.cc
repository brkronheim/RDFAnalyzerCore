#include "CmsMetCorrections.h"
#include "CmsCorrections.h"
#include "CorrectionManager.h"
#include "JetEnergyScaleManager.h"
#include "analyzer.h"
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace cms {
namespace {
using Vec = ROOT::VecOps::RVec<float>;
void requireColumns(Analyzer &an, const std::vector<std::string> &columns) {
  for (const auto &column : columns)
    if (!an.HasColumn(column))
      throw std::runtime_error("CmsMetCorrections: missing required column " + column);
}
void sameSize(std::size_t n, std::initializer_list<std::size_t> sizes) {
  for (const auto size : sizes)
    if (size != n) throw std::runtime_error("CmsMetCorrections: misaligned object arrays");
}
}

Vec detail::puppiType1JetSum(
    const Vec &rawPt, const Vec &correctedPt, const Vec &eta, const Vec &phi,
    const Vec &muonSubtrFactor, const Vec &muonSubtrDeltaEta,
    const Vec &muonSubtrDeltaPhi, const Vec &emFraction,
    float ptThreshold, float maxEmFraction, float maxAbsEta) {
  sameSize(rawPt.size(), {correctedPt.size(), eta.size(), phi.size(),
      muonSubtrFactor.size(), muonSubtrDeltaEta.size(), muonSubtrDeltaPhi.size(),
      emFraction.size()});
  Vec sum{0.f, 0.f};
  for (std::size_t i = 0; i < rawPt.size(); ++i) {
    if (!std::isfinite(rawPt[i]) || !std::isfinite(correctedPt[i]) ||
        !std::isfinite(eta[i]) || !std::isfinite(phi[i]) ||
        !std::isfinite(muonSubtrFactor[i]) ||
        !std::isfinite(muonSubtrDeltaEta[i]) ||
        !std::isfinite(muonSubtrDeltaPhi[i]) || !std::isfinite(emFraction[i]))
      throw std::runtime_error("CmsMetCorrections: non-finite jet input");
    if (rawPt[i] <= 0.f || emFraction[i] > maxEmFraction ||
        std::abs(eta[i] + muonSubtrDeltaEta[i]) >= maxAbsEta) continue;
    const float hadronicFraction = 1.f - muonSubtrFactor[i];
    const float correctedHadronicPt = correctedPt[i] * hadronicFraction;
    if (correctedHadronicPt <= ptThreshold) continue;
    // PUPPI has no L1 offset subtraction. Use the same full-jet JEC/JER
    // factor on the muon-subtracted four-vector as the PAT MET producer.
    const float delta = (correctedPt[i] - rawPt[i]) * hadronicFraction;
    const float direction = phi[i] + muonSubtrDeltaPhi[i];
    sum[0] += delta * std::cos(direction);
    sum[1] += delta * std::sin(direction);
  }
  return sum;
}

Vec detail::leptonMetShift(const Vec &originalPt, const Vec &correctedPt,
                           const Vec &phi, const ROOT::VecOps::RVec<bool> &mask) {
  sameSize(originalPt.size(), {correctedPt.size(), phi.size(), mask.size()});
  Vec shift{0.f, 0.f};
  for (std::size_t i = 0; i < originalPt.size(); ++i) {
    if (!mask[i]) continue;
    if (!std::isfinite(originalPt[i]) || !std::isfinite(correctedPt[i]) ||
        !std::isfinite(phi[i]))
      throw std::runtime_error("CmsMetCorrections: non-finite lepton input");
    const float delta = correctedPt[i] - originalPt[i];
    shift[0] += delta * std::cos(phi[i]);
    shift[1] += delta * std::sin(phi[i]);
  }
  return shift;
}

void defineCmsRebuiltPuppiMet(Analyzer &an, const CmsCorrectionConfig &cfg) {
  if (!cfg.yearSettings || !cfg.applyJetCorrections ||
      !cfg.jetRawPtColumnOverride.empty())
    throw std::runtime_error("CmsMetCorrections: raw Puppi MET rebuild requires standard AK4 PUPPI corrections");
  const auto &ys = *cfg.yearSettings;
  if (ys.canonicalYear != "2024" ||
      ys.jecCompoundCorrection.find("AK4PFPuppi") == std::string::npos ||
      ys.jecCompoundCorrection.find("Regression") != std::string::npos)
    throw std::runtime_error("CmsMetCorrections: raw rebuild currently supports 2024 standard AK4 PUPPI only");
  const bool isData = an.getConfigurationProvider().get("dtype") == "data";
  auto corrections = an.getPlugin<CorrectionManager>("correctionManager");
  auto low = JetEnergyScaleManager::create(an, "metLowJetEnergyScaleManager");
  requireColumns(an, {"RawPuppiMET_pt", "RawPuppiMET_phi", "Jet_pt_raw",
      "Jet_muonSubtrFactor", "Jet_muonSubtrDeltaEta", "Jet_muonSubtrDeltaPhi",
      "Jet_chEmEF", "Jet_neEmEF", "CorrT1METJet_rawPt", "CorrT1METJet_eta",
      "CorrT1METJet_phi", "CorrT1METJet_area", "CorrT1METJet_EmEF",
      "CorrT1METJet_muonSubtrFactor", "CorrT1METJet_muonSubtrDeltaEta",
      "CorrT1METJet_muonSubtrDeltaPhi"});
  low->setJetColumns("CorrT1METJet_rawPt", "CorrT1METJet_eta", "CorrT1METJet_phi", "");
  low->setRawPtColumn("CorrT1METJet_rawPt");
  const auto jec = corrections->getCompoundCorrection(cfg.configKeyPrefix + "_jec_nominal");
  std::vector<std::string> schema;
  for (const auto &input : jec->inputs()) schema.push_back(input.name());
  const auto lowJec = cfg.configKeyPrefix + "_metlow_jec_nominal";
  const auto lowInputs = detail::jecCompoundInputColumns(
      schema, "CorrT1METJet_rawPt", ys.rhoBranch, "CorrT1METJet");
  // CorrectionManager's packed-input columns are named after the correction
  // alias. Keep the low-jet alias distinct from the main Jet collection.
  corrections->registerCorrection(lowJec, ys.jmeFile, jec->name(), lowInputs);
  low->applyCorrectionlib(*corrections, lowJec, {},
      "CorrT1METJet_rawPt", "CorrT1METJet_pt_corr_jec", false, "", "", lowInputs);
  low->execute();
  if (!isData) {
    // Extra low-pT jets have no stored gen index. Match geometrically and
    // require the same resolution window as the main smearing algorithm.
    requireColumns(an, {"GenJet_pt", "GenJet_eta", "GenJet_phi"});
    const auto resolution = corrections->getCorrection(cfg.configKeyPrefix + "_jer_resolution");
    an.Define("CorrT1METJet_genMatchedPt",
        [resolution](const Vec &pt, const Vec &eta, const Vec &phi,
                     const Vec &genPt, const Vec &genEta, const Vec &genPhi, float rho) {
          Vec matched(pt.size(), -1.f);
          for (std::size_t i = 0; i < pt.size(); ++i) {
            double best = .2 * .2;
            if (!(pt[i] > 0.f)) continue;
            const double res = resolution->evaluate({double(eta[i]), double(pt[i]), double(rho)});
            for (std::size_t j = 0; j < genPt.size(); ++j) {
              const double dp = std::remainder(double(phi[i]) - genPhi[j], 2. * M_PI);
              const double de = double(eta[i]) - genEta[j];
              const double dr2 = dp * dp + de * de;
              if (dr2 < best && std::abs(pt[i] - genPt[j]) < 3. * pt[i] * res) {
                best = dr2;
                matched[i] = genPt[j];
              }
            }
          }
          return matched;
        }, {"CorrT1METJet_pt_corr_jec", "CorrT1METJet_eta", "CorrT1METJet_phi",
            "GenJet_pt", "GenJet_eta", "GenJet_phi", ys.rhoBranch});
    low->setJERSmearingColumns("CorrT1METJet_genMatchedPt", ys.rhoBranch, "event");
    const std::vector<std::string> resolutionInputs{
        "CorrT1METJet_eta", "CorrT1METJet_pt_corr_jec", ys.rhoBranch};
    const std::vector<std::string> sfInputs{"CorrT1METJet_eta", "CorrT1METJet_pt_corr_jec"};
    const auto schedule = [&](const std::string &direction) {
      const std::string uncertainty = direction.empty() ? "" : cfg.configKeyPrefix + "_jer_scale_factor_uncertainty";
      low->applyJERSmearing(*corrections, cfg.configKeyPrefix + "_jer_resolution",
          cfg.configKeyPrefix + "_jer_scale_factor", "CorrT1METJet_pt_corr_jec",
          direction.empty() ? "CorrT1METJet_pt_corr_nominal" : "CorrT1METJet_pt_jer_" + direction,
          "nom", false, "", "", resolutionInputs, sfInputs, uncertainty,
          sfInputs, direction == "up" ? 1.f : -1.f);
    };
    schedule("");
    if (cfg.applyJerSystematics && an.getSystematicManager().isEnabled()) {
      schedule("up"); schedule("down");
      an.getSystematicManager().registerVariationColumns("CorrT1METJet_pt_corr_nominal",
          "jer", "CorrT1METJet_pt_jer_up", "CorrT1METJet_pt_jer_down");
    }
    low->execute();
  } else {
    an.Define("CorrT1METJet_pt_corr_nominal", [](const Vec &v) { return v; }, {"CorrT1METJet_pt_corr_jec"});
  }
  if (cfg.applyJesSystematics && an.getSystematicManager().isEnabled()) {
    const auto sources = cfg.jesMode == CmsCorrectionConfig::Total
        ? std::vector<std::string>{"Total"}
        : cfg.jesMode == CmsCorrectionConfig::Full
            ? CmsYearRegistry::defaultJesFullSources(ys.canonicalYear)
            : CmsYearRegistry::defaultJesReducedSources(ys.canonicalYear);
    for (const auto &source : sources) {
      const auto label = source == "Total" ? "total" : source;
      const auto up = "CorrT1METJet_pt_jes_" + label + "_up";
      const auto down = "CorrT1METJet_pt_jes_" + label + "_down";
      const std::vector<std::string> inputs{"CorrT1METJet_eta", "CorrT1METJet_pt_corr_nominal"};
      const auto lowJes = cfg.configKeyPrefix + "_metlow_jes_" + source;
      const auto jes = corrections->getCorrection(cfg.configKeyPrefix + "_jes_" + source);
      corrections->registerCorrection(lowJes, ys.jmeFile, jes->name(), inputs);
      for (const auto &direction : {std::pair<std::string, float>{up, 1.f}, {down, -1.f}})
        low->applyCorrectionlib(*corrections, lowJes,
            {}, "CorrT1METJet_pt_corr_nominal", direction.first, false, "", "", inputs, true, direction.second);
      an.getSystematicManager().registerVariationColumns("CorrT1METJet_pt_corr_nominal", label, up, down);
    }
    low->execute();
  }
  an.Define("cms_met_jet_em_fraction", [](const Vec &ch, const Vec &ne) { return ch + ne; },
      {"Jet_chEmEF", "Jet_neEmEF"});
  const auto sum = [](const Vec &raw, const Vec &pt, const Vec &eta, const Vec &phi,
                      const Vec &mu, const Vec &de, const Vec &dp, const Vec &em) {
    return detail::puppiType1JetSum(raw, pt, eta, phi, mu, de, dp, em);
  };
  an.Define("cms_met_jet_type1_sum", sum,
      {"Jet_pt_raw", "Jet_pt_corr_nominal", "Jet_eta", "Jet_phi", "Jet_muonSubtrFactor",
       "Jet_muonSubtrDeltaEta", "Jet_muonSubtrDeltaPhi", "cms_met_jet_em_fraction"});
  an.Define("cms_met_lowjet_type1_sum", sum,
      {"CorrT1METJet_rawPt", "CorrT1METJet_pt_corr_nominal", "CorrT1METJet_eta",
       "CorrT1METJet_phi", "CorrT1METJet_muonSubtrFactor", "CorrT1METJet_muonSubtrDeltaEta",
       "CorrT1METJet_muonSubtrDeltaPhi", "CorrT1METJet_EmEF"});
  for (const auto &family : {std::pair<std::string, std::string>{"Muon", cfg.metMuonMaskColumn},
                             {"Electron", cfg.metElectronMaskColumn}}) {
    const auto output = "cms_met_" + family.first + "_shift";
    if (family.second.empty()) an.Define(output, []() { return Vec{0.f, 0.f}; }, {});
    else an.Define(output, detail::leptonMetShift,
        {family.first + "_pt", family.first + "_pt_corr_nominal", family.first + "_phi", family.second});
  }
  an.Define("cms_met_unclustered_shift", []() { return Vec{0.f, 0.f}; }, {});
  if (an.getSystematicManager().isEnabled()) {
    requireColumns(an, {"PuppiMET_pt", "PuppiMET_phi", "PuppiMET_ptUnclusteredUp",
        "PuppiMET_phiUnclusteredUp", "PuppiMET_ptUnclusteredDown", "PuppiMET_phiUnclusteredDown"});
    for (const auto &direction : {std::string("Up"), std::string("Down")})
      an.Define("cms_met_unclustered_shift_" + direction,
          [](float pt, float phi, float variedPt, float variedPhi) {
            return Vec{variedPt * std::cos(variedPhi) - pt * std::cos(phi),
                       variedPt * std::sin(variedPhi) - pt * std::sin(phi)};
          }, {"PuppiMET_pt", "PuppiMET_phi", "PuppiMET_ptUnclustered" + direction,
              "PuppiMET_phiUnclustered" + direction});
    an.getSystematicManager().registerVariationColumns("cms_met_unclustered_shift",
        "UnclusteredMET", "cms_met_unclustered_shift_Up", "cms_met_unclustered_shift_Down");
  }
  an.Define("cms_met_corrected_xy",
      [](float pt, float phi, const Vec &jets, const Vec &lowJets,
         const Vec &muons, const Vec &electrons, const Vec &unclustered) {
        return Vec{pt * std::cos(phi) - jets[0] - lowJets[0] - muons[0] - electrons[0] + unclustered[0],
                   pt * std::sin(phi) - jets[1] - lowJets[1] - muons[1] - electrons[1] + unclustered[1]};
      }, {"RawPuppiMET_pt", "RawPuppiMET_phi", "cms_met_jet_type1_sum",
          "cms_met_lowjet_type1_sum", "cms_met_Muon_shift", "cms_met_Electron_shift",
          "cms_met_unclustered_shift"});
  an.Define("PuppiMET_pt_corr_nominal", [](const Vec &xy) { return std::hypot(xy[0], xy[1]); }, {"cms_met_corrected_xy"});
  an.Define("PuppiMET_phi_corr_nominal", [](const Vec &xy) { return std::atan2(xy[1], xy[0]); }, {"cms_met_corrected_xy"});
}
}
