// All common implementation (applyCorrection, applyResolutionSmearing,
// propagateMET, PhysicsObjectCollection integration, execute(), etc.)
// lives in ObjectEnergyManagerBase.cc.  This translation unit provides
// the Rochester-specific extensions and emits the vtable for this class.

#include <MuonRochesterManager.h>
#include <analyzer.h>
#include <api/ILogger.h>
#include <cmath>
#include <cstdint>
#include <vector>
#include <sstream>
#include <stdexcept>

namespace {

uint64_t splitmix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

double erfinvApprox(double x) {
  const double w = -std::log((1.0 - x) * (1.0 + x));
  double p;
  if (w < 5.0) {
    const double t = w - 2.5;
    p = 2.81022636e-08;
    p = 3.43273939e-07 + p * t;
    p = -3.5233877e-06 + p * t;
    p = -4.39150654e-06 + p * t;
    p = 0.00021858087 + p * t;
    p = -0.00125372503 + p * t;
    p = -0.00417768164 + p * t;
    p = 0.246640727 + p * t;
    p = 1.50140941 + p * t;
  } else {
    const double t = std::sqrt(w) - 3.0;
    p = -0.000200214257;
    p = 0.000100950558 + p * t;
    p = 0.00134934322 + p * t;
    p = -0.00367342844 + p * t;
    p = 0.00573950773 + p * t;
    p = -0.0076224613 + p * t;
    p = 0.00943887047 + p * t;
    p = 1.00167406 + p * t;
    p = 2.83297682 + p * t;
  }
  return p * x;
}

double crystalBallRandom(double mean, double sigma, double alpha, double n,
                         uint64_t seed) {
  const double fa = std::abs(alpha);
  const double ex = std::exp(-fa * fa / 2.0);
  const double c1 = n / fa / (n - 1.0) * ex;
  const double d1 = 2.0 * std::sqrt(M_PI / 2.0) * std::erf(fa / std::sqrt(2.0));
  const double c = (d1 + 2.0 * c1) / c1;
  const double d = (d1 + 2.0 * c1) / 2.0;
  const double norm = 1.0 / sigma / (d1 + 2.0 * c1);
  const double k = 1.0 / (n - 1.0);
  const double normA = norm * std::pow(n / fa, n) * ex;
  const double normSigma = norm * sigma;
  const double normC = normSigma * c1;
  const double f = 1.0 - fa * fa / n;
  const double g = sigma * n / fa;
  const double cdfMinusAlpha = [&]() {
    const double x = mean - alpha * sigma;
    const double delta = (x - mean) / sigma;
    return normC / std::pow(f - sigma * delta / g, n - 1.0);
  }();
  const double cdfPlusAlpha = [&]() {
    const double x = mean + alpha * sigma;
    const double delta = (x - mean) / sigma;
    return normSigma * (d - std::sqrt(M_PI / 2.0) *
                                std::erf(-delta / std::sqrt(2.0)));
  }();
  const double u = std::max(
      static_cast<double>((splitmix64(seed) >> 11U)) /
          static_cast<double>(1ULL << 53U),
      1e-12);

  if (u < cdfMinusAlpha) {
    return mean + g * (f - std::pow(normC / u, k));
  }
  if (u > cdfPlusAlpha) {
    return mean - g * (f - std::pow(c - u / normC, -k));
  }
  const double argument = (d - u / normSigma) / std::sqrt(M_PI / 2.0);
  return mean - std::sqrt(2.0) * sigma * erfinvApprox(argument);
}

double applyMuonPtBoundaryFilter(double correctedPt, double originalPt) {
  if (!std::isfinite(correctedPt) || originalPt < 26.0 || originalPt > 200.0)
    return originalPt;
  return correctedPt;
}

} // namespace

// ---------------------------------------------------------------------------
// Rochester-specific input columns
// ---------------------------------------------------------------------------

void MuonRochesterManager::setRochesterInputColumns(
    const std::string &chargeColumn, const std::string &genPtColumn,
    const std::string &nLayersColumn, const std::string &u1Column,
    const std::string &u2Column) {
  if (chargeColumn.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::setRochesterInputColumns: "
        "chargeColumn must not be empty");
  chargeColumn_m  = chargeColumn;
  genPtColumn_m   = genPtColumn;
  nLayersColumn_m = nLayersColumn;
  u1Column_m      = u1Column;
  u2Column_m      = u2Column;
}

void MuonRochesterManager::setScaleResolutionEventColumns(
    const std::string &lumiColumn, const std::string &eventColumn) {
  if (lumiColumn.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::setScaleResolutionEventColumns: lumiColumn must not be empty");
  if (eventColumn.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::setScaleResolutionEventColumns: eventColumn must not be empty");
  lumiColumn_m = lumiColumn;
  eventColumn_m = eventColumn;
}

// ---------------------------------------------------------------------------
// Internal helper
// ---------------------------------------------------------------------------

std::vector<std::string>
MuonRochesterManager::buildRochesterInputColumns(
    const std::string &ptColumn) const {
  if (chargeColumn_m.empty())
    throw std::runtime_error(
        "MuonRochesterManager::buildRochesterInputColumns: "
        "call setRochesterInputColumns() before applying Rochester corrections");
  if (getEtaColumn().empty())
    throw std::runtime_error(
        "MuonRochesterManager::buildRochesterInputColumns: "
        "call setObjectColumns() before applying Rochester corrections");

  // Standard CMS Rochester correctionlib numeric input order:
  //   charge, eta, phi, pt, genPt, nTrackerLayers, u1, u2
  return {chargeColumn_m, getEtaColumn(), getPhiColumn(),
          ptColumn, genPtColumn_m, nLayersColumn_m, u1Column_m, u2Column_m};
}

// ---------------------------------------------------------------------------
// Convenience Rochester correction methods
// ---------------------------------------------------------------------------

void MuonRochesterManager::applyRochesterCorrection(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &inputPtColumn, const std::string &outputPtColumn) {
  applyCorrectionlib(cm, correctionName, {"nom"},
                     inputPtColumn, outputPtColumn,
                     /*applyToMass=*/false, "", "",
                     buildRochesterInputColumns(inputPtColumn));
}

void MuonRochesterManager::applyRochesterSystematicSet(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &setName, const std::string &inputPtColumn,
    const std::string &outputPtPrefix) {
  applySystematicSet(cm, correctionName, setName,
                     inputPtColumn, outputPtPrefix,
                     /*applyToMass=*/false,
                     buildRochesterInputColumns(inputPtColumn));
}

void MuonRochesterManager::applyScaleAndResolution(
    const std::string &jsonFile, bool isData, const std::string &inputPtColumn,
    const std::string &outputPtColumn, const std::string &scaleVariation,
    const std::string &resolutionVariation) {
  if (jsonFile.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::applyScaleAndResolution: jsonFile must not be empty");
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::applyScaleAndResolution: inputPtColumn must not be empty");
  if (outputPtColumn.empty())
    throw std::invalid_argument(
        "MuonRochesterManager::applyScaleAndResolution: outputPtColumn must not be empty");
  if (chargeColumn_m.empty() || nLayersColumn_m.empty() || lumiColumn_m.empty() ||
      eventColumn_m.empty() || getEtaColumn().empty() || getPhiColumn().empty()) {
    throw std::runtime_error(
        "MuonRochesterManager::applyScaleAndResolution: configure object, Rochester, and event columns before scheduling corrections");
  }

  ScaleResolutionStep step;
  step.correctionSet = std::shared_ptr<correction::CorrectionSet>(
      correction::CorrectionSet::from_file(jsonFile).release());
  step.isData = isData;
  step.inputPtColumn = inputPtColumn;
  step.outputPtColumn = outputPtColumn;
  step.scaleVariation = scaleVariation;
  step.resolutionVariation = resolutionVariation;
  scaleResolutionSteps_m.push_back(std::move(step));
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

const std::string &MuonRochesterManager::getChargeColumn()  const { return chargeColumn_m;  }
const std::string &MuonRochesterManager::getGenPtColumn()   const { return genPtColumn_m;   }
const std::string &MuonRochesterManager::getNLayersColumn() const { return nLayersColumn_m; }
const std::string &MuonRochesterManager::getU1Column()      const { return u1Column_m;      }
const std::string &MuonRochesterManager::getU2Column()      const { return u2Column_m;      }
const std::string &MuonRochesterManager::getLumiColumn()    const { return lumiColumn_m;    }
const std::string &MuonRochesterManager::getEventColumn()   const { return eventColumn_m;   }

void MuonRochesterManager::execute() {
  ObjectEnergyManagerBase::execute();

  for (const auto &step : scaleResolutionSteps_m) {
    const auto *cset = step.correctionSet.get();
    const auto aData = cset->at("a_data");
    const auto mData = cset->at("m_data");
    const auto aMc = cset->at("a_mc");
    const auto mMc = cset->at("m_mc");
    const auto kData = cset->at("k_data");
    const auto kMc = cset->at("k_mc");
    const auto cbParams = cset->at("cb_params");
    const auto polyParams = cset->at("poly_params");

    ROOT::RDF::RNode df = getCurrentDataFrame();
    auto newDf = df.Define(
        step.outputPtColumn,
        [aData, mData, aMc, mMc, kData, kMc, cbParams, polyParams,
         isData = step.isData, scaleVariation = step.scaleVariation,
         resolutionVariation = step.resolutionVariation](
            const ROOT::VecOps::RVec<Float_t> &pt,
            const ROOT::VecOps::RVec<Float_t> &eta,
            const ROOT::VecOps::RVec<Float_t> &phi,
            const ROOT::VecOps::RVec<Float_t> &charge,
            const ROOT::VecOps::RVec<Float_t> &nLayers,
            UInt_t lumi,
            ULong64_t event)
            -> ROOT::VecOps::RVec<Float_t> {
          ROOT::VecOps::RVec<Float_t> output(pt.size(), 0.0f);
          for (std::size_t i = 0; i < pt.size(); ++i) {
            const double ptIn = pt[i];
            const double etaIn = eta[i];
            const double phiIn = phi[i];
            const double chargeIn = charge[i];
            const double layers = nLayers[i];

            const auto &aCorr = isData ? aData : aMc;
            const auto &mCorr = isData ? mData : mMc;
            const double aNom = aCorr->evaluate({etaIn, phiIn, std::string("nom")});
            const double mNom = mCorr->evaluate({etaIn, phiIn, std::string("nom")});
            double ptScaled = 1.0 / (mNom / ptIn + chargeIn * aNom);
            ptScaled = applyMuonPtBoundaryFilter(ptScaled, ptIn);

            double ptCorrected = ptScaled;
            if (!isData) {
              const double mean = cbParams->evaluate({std::abs(etaIn), layers, 0});
              const double sigma = cbParams->evaluate({std::abs(etaIn), layers, 1});
              const double n = cbParams->evaluate({std::abs(etaIn), layers, 2});
              const double alpha = cbParams->evaluate({std::abs(etaIn), layers, 3});
              const double poly0 = polyParams->evaluate({std::abs(etaIn), layers, 0});
              const double poly1 = polyParams->evaluate({std::abs(etaIn), layers, 1});
              const double poly2 = polyParams->evaluate({std::abs(etaIn), layers, 2});
              const double stddev = std::max(poly0 + poly1 * ptScaled + poly2 * ptScaled * ptScaled, 0.0);
              const double kDataNom = kData->evaluate({std::abs(etaIn), std::string("nom")});
              const double kMcNom = kMc->evaluate({std::abs(etaIn), std::string("nom")});
              const double kNom = kMcNom < kDataNom
                                      ? std::sqrt(kDataNom * kDataNom - kMcNom * kMcNom)
                                      : 0.0;
              const uint64_t phiBits = static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int64_t>((phiIn / M_PI) * ((1 << 30) - 1)) & 0xFFF));
              const uint64_t seed = splitmix64(static_cast<uint64_t>(event) ^
                                              (static_cast<uint64_t>(lumi) << 32U) ^ phiBits);
              const double rndm = crystalBallRandom(mean, sigma, alpha, n, seed);
              ptCorrected = ptScaled * (1.0 + kNom * stddev * rndm);
              ptCorrected = applyMuonPtBoundaryFilter(ptCorrected, ptScaled);
              const double ratio = ptScaled != 0.0 ? ptCorrected / ptScaled : 1.0;
              if (!std::isfinite(ratio) || ratio > 2.0 || ratio < 0.1 || ptCorrected < 0.0)
                ptCorrected = ptScaled;

              if (resolutionVariation == "up" || resolutionVariation == "down") {
                const double kUnc = kMc->evaluate({std::abs(etaIn), std::string("stat")});
                if (kNom > 0.0 && ptScaled > 0.0) {
                  const double stdTimesCb = (ptCorrected / ptScaled - 1.0) / kNom;
                  const double shiftedK =
                      resolutionVariation == "up" ? (kNom + kUnc) : (kNom - kUnc);
                  double ptVar = ptScaled * (1.0 + shiftedK * stdTimesCb);
                  const double varRatio = ptScaled != 0.0 ? ptVar / ptScaled : 1.0;
                  if (!std::isfinite(varRatio) || varRatio > 2.0 || varRatio < 0.1 || ptVar < 0.0)
                    ptVar = ptScaled;
                  ptCorrected = ptVar;
                }
              }

              if (scaleVariation == "up" || scaleVariation == "down") {
                const double statA = aMc->evaluate({etaIn, phiIn, std::string("stat")});
                const double statM = mMc->evaluate({etaIn, phiIn, std::string("stat")});
                const double rhoStat = mMc->evaluate({etaIn, phiIn, std::string("rho_stat")});
                const double unc = ptCorrected * ptCorrected * std::sqrt(
                    statM * statM / (ptCorrected * ptCorrected) +
                    statA * statA +
                    2.0 * chargeIn * rhoStat * statM / ptCorrected * statA);
                ptCorrected += scaleVariation == "up" ? unc : -unc;
              }
            }

            output[i] = static_cast<Float_t>(ptCorrected);
          }
          return output;
        },
        {step.inputPtColumn, getEtaColumn(), getPhiColumn(), chargeColumn_m,
         nLayersColumn_m, lumiColumn_m, eventColumn_m});
    setCurrentDataFrame(newDf);
  }

  scaleResolutionSteps_m.clear();
}

// ---------------------------------------------------------------------------
// Metadata extension hooks
// ---------------------------------------------------------------------------

void MuonRochesterManager::appendObjectMetadata(std::ostringstream &ss) const {
  if (!chargeColumn_m.empty())
    ss << "  Rochester input columns:"
       << " charge="  << chargeColumn_m
       << " genPt="   << genPtColumn_m
       << " nLayers=" << nLayersColumn_m
       << " u1="      << u1Column_m
       << " u2="      << u2Column_m << "\n";
  if (!lumiColumn_m.empty())
    ss << "  Scale-resolution event columns:"
       << " lumi=" << lumiColumn_m
       << " event=" << eventColumn_m << "\n";
}

void MuonRochesterManager::appendObjectProvenanceEntries(
    std::unordered_map<std::string, std::string> &entries) const {
  if (!chargeColumn_m.empty()) {
    entries["muon_charge_column"]  = chargeColumn_m;
    entries["muon_gen_pt_column"]  = genPtColumn_m;
    entries["muon_nlayers_column"] = nLayersColumn_m;
    entries["muon_u1_column"]      = u1Column_m;
    entries["muon_u2_column"]      = u2Column_m;
  }
  if (!lumiColumn_m.empty()) {
    entries["muon_lumi_column"] = lumiColumn_m;
    entries["muon_event_column"] = eventColumn_m;
  }
}

std::shared_ptr<MuonRochesterManager> MuonRochesterManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<MuonRochesterManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
