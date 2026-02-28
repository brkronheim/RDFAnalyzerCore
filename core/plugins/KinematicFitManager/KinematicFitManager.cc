#include <KinematicFit.h>
#include <KinematicFitGPU.h>
#include <KinematicFitManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <algorithm>
#include <cmath>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// ── private helpers (file-local) ──────────────────────────────────────────────

namespace {

/// Parse a string to double, throwing a clear message on failure.
double parseDouble(const std::string &s, const std::string &key) {
  try {
    return std::stod(s);
  } catch (...) {
    throw std::runtime_error(
        "KinematicFitManager: cannot parse '" + s + "' as double for key '" +
        key + "'");
  }
}

/// Parse a string to int, throwing a clear message on failure.
int parseInt(const std::string &s, const std::string &key) {
  try {
    return std::stoi(s);
  } catch (...) {
    throw std::runtime_error(
        "KinematicFitManager: cannot parse '" + s + "' as int for key '" +
        key + "'");
  }
}

/// Split @p s by a single-character delimiter, skipping empty tokens.
std::vector<std::string> splitBy(const std::string &s, char delim) {
  std::vector<std::string> parts;
  std::istringstream ss(s);
  std::string tok;
  while (std::getline(ss, tok, delim)) {
    if (!tok.empty()) parts.push_back(tok);
  }
  return parts;
}

/**
 * @brief Parse one particle specification.
 *
 * Accepted formats:
 *  - 6 fields  "label:ptCol:etaCol:phiCol:massCol:type"          (scalar or recoil)
 *  - 7 fields  "label:ptCol:etaCol:phiCol:massCol:type:index"    (collection index)
 *
 * For type == "recoil" the ptCol/etaCol/phiCol/massCol are RVec collection
 * columns whose entries are summed per event.
 * For type == "met" set etaCol to "_" to auto-define a η = 0 column.
 * For numeric massCol (e.g. "0", "0.106") a constant column is created.
 * For collection-indexed particles, index is the zero-based position in the
 * RVec; missing jets (index ≥ collection size) resolve to 0.
 */
KinFitParticleConfig parseParticleSpec(const std::string &spec) {
  const auto parts = splitBy(spec, ':');
  if (parts.size() != 6 && parts.size() != 7) {
    throw std::runtime_error(
        "KinematicFitManager: invalid particle spec '" + spec +
        "' – expected label:ptCol:etaCol:phiCol:massCol:type "
        "or label:ptCol:etaCol:phiCol:massCol:type:index");
  }
  KinFitParticleConfig p;
  p.name    = parts[0];
  p.ptCol   = parts[1];
  p.etaCol  = parts[2];
  p.phiCol  = parts[3];
  p.massCol = parts[4];
  p.type    = parts[5];
  p.collectionIndex = (parts.size() == 7) ? parseInt(parts[6], "collectionIndex") : -1;

  if (p.collectionIndex >= 0 && p.type == "recoil") {
    throw std::runtime_error(
        "KinematicFitManager: particle '" + p.name +
        "' has type 'recoil' but also specifies a collection index – "
        "recoil particles always sum the entire collection.");
  }
  return p;
}

/**
 * @brief Parse one constraint specification.
 *
 * Accepted formats:
 *  - "idx1+idx2:targetMass"              — two-body hard mass constraint
 *  - "idx1+idx2:targetMass:massSigma"    — two-body soft mass constraint
 *                                          (Gaussian penalty with width massSigma)
 *  - "idx1+idx2+idx3:targetMass"         — three-body hard mass constraint
 *                                          (e.g. top: m(b, l, ν) = 173.3 GeV)
 *  - "idx1+idx2+idx3:targetMass:massSigma" — three-body soft mass constraint
 *  - "pt:idx:targetPt"                   — pT constraint on a single particle
 *                                          (set targetPt=0 for no-MET events)
 *
 * For soft mass constraints massSigma is the resonance width in GeV;
 * typical values: Z = 2.495, W = 2.085, top = 1.4, Higgs ≈ 0.004.
 * Indices are zero-based into the fit's particle list.
 */
KinFitConstraintConfig parseConstraintSpec(const std::string &spec) {
  // ── pT constraint: "pt:idx:targetPt" ──────────────────────────────────
  if (spec.size() >= 3 && spec[0] == 'p' && spec[1] == 't' && spec[2] == ':') {
    const auto parts = splitBy(spec.substr(3), ':');
    if (parts.size() != 2) {
      throw std::runtime_error(
          "KinematicFitManager: invalid pT constraint spec '" + spec +
          "' – expected pt:idx:targetPt");
    }
    KinFitConstraintConfig con;
    con.type        = KinFitConstraintConfig::Type::PT;
    con.idx1        = parseInt(parts[0], "ptConstraintIdx");
    con.targetValue = parseDouble(parts[1], "ptConstraintTarget");
    return con;
  }

  // ── Mass constraint: "idx1+idx2:mass[:sigma]" or "idx1+idx2+idx3:mass[:sigma]"
  const auto colonParts = splitBy(spec, ':');
  if (colonParts.size() != 2 && colonParts.size() != 3) {
    throw std::runtime_error(
        "KinematicFitManager: invalid constraint spec '" + spec +
        "' – expected idx1+idx2:mass[:massSigma], "
        "idx1+idx2+idx3:mass[:massSigma], or pt:idx:targetPt");
  }
  const auto idxParts = splitBy(colonParts[0], '+');
  if (idxParts.size() != 2 && idxParts.size() != 3) {
    throw std::runtime_error(
        "KinematicFitManager: invalid constraint indices in '" + spec +
        "' – expected two or three indices separated by '+'");
  }
  KinFitConstraintConfig con;
  con.type        = KinFitConstraintConfig::Type::MASS;
  con.idx1        = parseInt(idxParts[0], "constraintIdx1");
  con.idx2        = parseInt(idxParts[1], "constraintIdx2");
  con.idx3        = (idxParts.size() == 3) ? parseInt(idxParts[2], "constraintIdx3") : -1;
  con.targetValue = parseDouble(colonParts[1], "constraintMass");
  con.massSigma   = (colonParts.size() == 3)
                      ? parseDouble(colonParts[2], "constraintMassSigma")
                      : 0.0;
  return con;
}

// ── recoil 4-vector computation helpers ──────────────────────────────────────
//
// These functions sum a collection of jets (given as four RVec<Float_t>
// columns) into a single effective 4-vector.  They are used to create helper
// scalar columns that the main fit lambda reads, so the fit lambda always
// sees ordinary scalar (Float_t) inputs regardless of the source.
//
// If the collection is empty, all functions return 0 so the recoil particle
// has zero 4-momentum and does not affect the kinematic constraints.

using RVecF = ROOT::VecOps::RVec<Float_t>;

/// Cap for the pseudorapidity of the summed recoil vector when θ → 0.
/// Corresponds to θ ≈ 1e-13 rad, well beyond CMS/ATLAS tracker acceptance.
static constexpr float kMaxForwardRecoilEta = 25.0f;

static Float_t recoilPt(const RVecF &pt, const RVecF &phi) {
  double px = 0, py = 0;
  for (std::size_t k = 0; k < pt.size(); ++k) {
    px += static_cast<double>(pt[k]) * std::cos(static_cast<double>(phi[k]));
    py += static_cast<double>(pt[k]) * std::sin(static_cast<double>(phi[k]));
  }
  return static_cast<Float_t>(std::sqrt(px * px + py * py));
}

static Float_t recoilEta(const RVecF &pt, const RVecF &eta,
                         const RVecF &phi) {
  double px = 0, py = 0, pz = 0;
  for (std::size_t k = 0; k < pt.size(); ++k) {
    const double p = static_cast<double>(pt[k]);
    const double e = static_cast<double>(eta[k]);
    const double f = static_cast<double>(phi[k]);
    px += p * std::cos(f);
    py += p * std::sin(f);
    pz += p * std::sinh(e);
  }
  const double ptSum = std::sqrt(px * px + py * py);
  if (ptSum < 1e-9 && std::abs(pz) < 1e-9) return 0.0f;
  const double theta = std::atan2(ptSum, pz);
  const double tanHalf = std::tan(theta / 2.0);
  if (tanHalf < 1e-30) return kMaxForwardRecoilEta; // protect against θ ≈ 0
  return static_cast<Float_t>(-std::log(tanHalf));
}

static Float_t recoilPhi(const RVecF &pt, const RVecF &phi) {
  double px = 0, py = 0;
  for (std::size_t k = 0; k < pt.size(); ++k) {
    px += static_cast<double>(pt[k]) * std::cos(static_cast<double>(phi[k]));
    py += static_cast<double>(pt[k]) * std::sin(static_cast<double>(phi[k]));
  }
  return (px == 0.0 && py == 0.0)
             ? 0.0f
             : static_cast<Float_t>(std::atan2(py, px));
}

static Float_t recoilMass(const RVecF &pt, const RVecF &eta,
                          const RVecF &phi, const RVecF &mass) {
  double E = 0, px = 0, py = 0, pz = 0;
  for (std::size_t k = 0; k < pt.size(); ++k) {
    const double p  = static_cast<double>(pt[k]);
    const double e  = static_cast<double>(eta[k]);
    const double f  = static_cast<double>(phi[k]);
    const double m  = static_cast<double>(mass[k]);
    const double ch = std::cosh(e);
    E  += std::sqrt(p * p * ch * ch + m * m);
    px += p * std::cos(f);
    py += p * std::sin(f);
    pz += p * std::sinh(e);
  }
  const double m2 = E * E - px * px - py * py - pz * pz;
  return static_cast<Float_t>(m2 > 0.0 ? std::sqrt(m2) : 0.0);
}

} // anonymous namespace

// ── constructor ───────────────────────────────────────────────────────────────

KinematicFitManager::KinematicFitManager(
    const IConfigurationProvider &configProvider) {
  registerFits(configProvider);
}

// ── public interface ──────────────────────────────────────────────────────────

/**
 * @brief Apply a single kinematic fit to the current RDataFrame.
 *
 * For each particle the method resolves the scalar column names that the fit
 * lambda will read:
 *
 *  - **Normal scalar** (collectionIndex == -1, type != "recoil"):
 *    The column names are used directly.
 *  - **Collection-indexed** (collectionIndex >= 0):
 *    Helper scalar columns are defined that extract element @e index from the
 *    RVec collection.  If the collection is shorter than @e index, 0 is
 *    returned so the particle is effectively absent.
 *  - **Recoil** (type == "recoil"):
 *    Helper scalar columns are defined that compute (pT, η, φ, m) of the
 *    vector sum of all jets in the collection.  This captures the full
 *    hadronic recoil regardless of how many extra jets are present.
 *
 * The fit itself sees ordinary (pT, η, φ, m) scalar inputs for every particle
 * regardless of source.  Thread safety is guaranteed because a fresh
 * KinematicFit object is created inside the per-event lambda.
 *
 * Output layout (2 + 3*N floats, N = number of particles):
 *  [0]          chi2
 *  [1]          converged (1 = yes, 0 = no)
 *  [2+3i+0..2]  pT, eta, phi of fitted particle i
 *
 * Output RDataFrame columns:
 *  {name}_chi2,  {name}_converged,
 *  {name}_{label}_pt_fitted, {name}_{label}_eta_fitted, {name}_{label}_phi_fitted
 */
void KinematicFitManager::applyFit(const std::string &fitName) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error(
        "KinematicFitManager: DataManager or SystematicManager not set");
  }

  const KinFitConfig cfg      = getFitConfig(fitName); // copy for lambda capture
  const int          nParticles = static_cast<int>(cfg.particles.size());

  // ── resolve effective scalar column names ─────────────────────────────────
  // For normal particles these are just the config column names.
  // For collection-indexed / recoil particles we define helper columns first.
  std::vector<std::string> ptCols(nParticles), etaCols(nParticles),
                            phiCols(nParticles), massCols(nParticles);

  for (int i = 0; i < nParticles; ++i) {
    const auto &p   = cfg.particles[i];
    const std::string pfx = fitName + "_" + p.name + "_kinfit";

    if (p.type == "recoil") {
      // ── Recoil: define helper columns summing the jet collection ──────────
      // The collection may have zero entries (no extra jets); in that case
      // the helpers return 0 and the recoil particle has zero 4-momentum.
      const std::string tmpPt   = pfx + "_pt";
      const std::string tmpEta  = pfx + "_eta";
      const std::string tmpPhi  = pfx + "_phi";
      const std::string tmpMass = pfx + "_mass";

      dataManager_m->Define(tmpPt,
          [](const RVecF &pt, const RVecF &phi) { return recoilPt(pt, phi); },
          {p.ptCol, p.phiCol}, *systematicManager_m);

      dataManager_m->Define(tmpEta,
          [](const RVecF &pt, const RVecF &eta, const RVecF &phi) {
            return recoilEta(pt, eta, phi);
          },
          {p.ptCol, p.etaCol, p.phiCol}, *systematicManager_m);

      dataManager_m->Define(tmpPhi,
          [](const RVecF &pt, const RVecF &phi) { return recoilPhi(pt, phi); },
          {p.ptCol, p.phiCol}, *systematicManager_m);

      dataManager_m->Define(tmpMass,
          [](const RVecF &pt, const RVecF &eta, const RVecF &phi,
             const RVecF &mass) { return recoilMass(pt, eta, phi, mass); },
          {p.ptCol, p.etaCol, p.phiCol, p.massCol}, *systematicManager_m);

      ptCols[i]   = tmpPt;
      etaCols[i]  = tmpEta;
      phiCols[i]  = tmpPhi;
      massCols[i] = tmpMass;

    } else if (p.collectionIndex >= 0) {
      // ── Collection-indexed: extract element N from an RVec column ─────────
      // If the collection is shorter than the requested index, return 0 so
      // the particle is effectively absent from the event.
      const int idx = p.collectionIndex;

      const std::string tmpPt   = pfx + "_pt";
      const std::string tmpEta  = pfx + "_eta";
      const std::string tmpPhi  = pfx + "_phi";

      dataManager_m->Define(tmpPt,
          [idx](const RVecF &v) -> Float_t {
            return (idx < static_cast<int>(v.size())) ? v[idx] : 0.0f;
          },
          {p.ptCol}, *systematicManager_m);

      dataManager_m->Define(tmpEta,
          [idx](const RVecF &v) -> Float_t {
            return (idx < static_cast<int>(v.size())) ? v[idx] : 0.0f;
          },
          {p.etaCol}, *systematicManager_m);

      dataManager_m->Define(tmpPhi,
          [idx](const RVecF &v) -> Float_t {
            return (idx < static_cast<int>(v.size())) ? v[idx] : 0.0f;
          },
          {p.phiCol}, *systematicManager_m);

      ptCols[i]  = tmpPt;
      etaCols[i] = tmpEta;
      phiCols[i] = tmpPhi;

      // Mass: may be a collection column, a literal, or "_"/""
      bool  isMassLiteral  = false;
      float massLiteralVal = 0.0f;
      if (!p.massCol.empty() && p.massCol != "_") {
        try { massLiteralVal = std::stof(p.massCol); isMassLiteral = true; }
        catch (const std::invalid_argument &) {} // not a numeric literal
        catch (const std::out_of_range &)     {} // out of float range
      } else {
        isMassLiteral = true;
      }

      if (isMassLiteral) {
        const std::string tmpMass = pfx + "_mass";
        const float massVal = massLiteralVal;
        dataManager_m->Define(tmpMass,
            [massVal](ULong64_t) -> Float_t { return massVal; },
            {"rdfentry_"}, *systematicManager_m);
        massCols[i] = tmpMass;
      } else {
        // massCol is an RVec collection → extract element at collectionIndex
        const std::string tmpMass = pfx + "_mass";
        dataManager_m->Define(tmpMass,
            [idx](const RVecF &v) -> Float_t {
              return (idx < static_cast<int>(v.size())) ? v[idx] : 0.0f;
            },
            {p.massCol}, *systematicManager_m);
        massCols[i] = tmpMass;
      }

    } else {
      // ── Normal scalar columns (current behaviour) ─────────────────────────
      ptCols[i] = p.ptCol;

      // eta: absent (MET) → auto-define constant 0
      if (p.etaCol.empty() || p.etaCol == "_") {
        const std::string tmpEta = pfx + "_eta";
        dataManager_m->Define(tmpEta,
            [](ULong64_t) -> Float_t { return 0.0f; },
            {"rdfentry_"}, *systematicManager_m);
        etaCols[i] = tmpEta;
      } else {
        etaCols[i] = p.etaCol;
      }

      phiCols[i] = p.phiCol;

      // mass: literal or column
      bool  isMassLiteral  = false;
      float massLiteralVal = 0.0f;
      if (!p.massCol.empty() && p.massCol != "_") {
        try { massLiteralVal = std::stof(p.massCol); isMassLiteral = true; }
        catch (const std::invalid_argument &) {} // not a numeric literal
        catch (const std::out_of_range &)     {} // out of float range
      } else {
        isMassLiteral = true;
      }

      if (isMassLiteral) {
        const std::string tmpMass = pfx + "_mass";
        const float massVal = massLiteralVal;
        dataManager_m->Define(tmpMass,
            [massVal](ULong64_t) -> Float_t { return massVal; },
            {"rdfentry_"}, *systematicManager_m);
        massCols[i] = tmpMass;
      } else {
        massCols[i] = p.massCol;
      }
    }
  }

  // ── pack all scalar inputs into one RVec<Float_t> ─────────────────────────
  std::vector<std::string> inputCols;
  inputCols.reserve(4 * nParticles);
  for (int i = 0; i < nParticles; ++i) {
    inputCols.push_back(ptCols[i]);
    inputCols.push_back(etaCols[i]);
    inputCols.push_back(phiCols[i]);
    inputCols.push_back(massCols[i]);
  }

  const std::string inputsCol = fitName + "_inputs";
  dataManager_m->DefineVector(inputsCol, inputCols, "Float_t",
                               *systematicManager_m);

  // ── resolve the run-variable column ───────────────────────────────────────
  // If no runVar was configured, define a constant-true helper column so the
  // fit lambda always receives a bool regardless.
  const std::string &storedRunVar = getRunVar(fitName);
  std::string runVarCol;
  if (storedRunVar.empty()) {
    runVarCol = fitName + "_alwaysRun";
    dataManager_m->Define(runVarCol,
        [](ULong64_t) -> bool { return true; },
        {"rdfentry_"}, *systematicManager_m);
  } else {
    runVarCol = storedRunVar;
  }

  // ── build per-particle sigma arrays (same for every event) ────────────────
  // Pre-computed once here and captured by both the CPU and GPU lambdas,
  // eliminating the per-event particle-type dispatch.
  std::vector<float> sigmas(static_cast<size_t>(nParticles) * 3);
  for (int i = 0; i < nParticles; ++i) {
    const auto &p = cfg.particles[i];
    float sigPt, sigEta, sigPhi;
    if (p.type == "lepton") {
      sigPt  = static_cast<float>(cfg.leptonPtResolution);
      sigEta = static_cast<float>(cfg.leptonEtaResolution);
      sigPhi = static_cast<float>(cfg.leptonPhiResolution);
    } else if (p.type == "met") {
      sigPt  = static_cast<float>(cfg.metPtResolution);
      sigEta = static_cast<float>(cfg.metEtaResolution);
      sigPhi = static_cast<float>(cfg.metPhiResolution);
    } else if (p.type == "recoil") {
      sigPt  = static_cast<float>(cfg.recoilPtResolution);
      sigEta = static_cast<float>(cfg.recoilEtaResolution);
      sigPhi = static_cast<float>(cfg.recoilPhiResolution);
    } else { // "jet" or unrecognised
      sigPt  = static_cast<float>(cfg.jetPtResolution);
      sigEta = static_cast<float>(cfg.jetEtaResolution);
      sigPhi = static_cast<float>(cfg.jetPhiResolution);
    }
    sigmas[static_cast<size_t>(i) * 3 + 0] = sigPt;
    sigmas[static_cast<size_t>(i) * 3 + 1] = sigEta;
    sigmas[static_cast<size_t>(i) * 3 + 2] = sigPhi;
  }

  // ── build flat constraint arrays captured by the GPU lambda ───────────────
  const int nConstraints = static_cast<int>(cfg.constraints.size());
  std::vector<int>   conTypes (static_cast<size_t>(nConstraints));
  std::vector<int>   conIdx1  (static_cast<size_t>(nConstraints));
  std::vector<int>   conIdx2  (static_cast<size_t>(nConstraints));
  std::vector<int>   conIdx3  (static_cast<size_t>(nConstraints));
  std::vector<float> conTarget(static_cast<size_t>(nConstraints));
  std::vector<float> conSigma (static_cast<size_t>(nConstraints));
  for (int c = 0; c < nConstraints; ++c) {
    const auto &con = cfg.constraints[static_cast<size_t>(c)];
    conTypes [static_cast<size_t>(c)] = (con.type == KinFitConstraintConfig::Type::PT)
                                            ? 2
                                            : (con.idx3 >= 0 ? 1 : 0);
    conIdx1  [static_cast<size_t>(c)] = con.idx1;
    conIdx2  [static_cast<size_t>(c)] = con.idx2;
    conIdx3  [static_cast<size_t>(c)] = con.idx3;
    conTarget[static_cast<size_t>(c)] = static_cast<float>(con.targetValue);
    conSigma [static_cast<size_t>(c)] = static_cast<float>(con.massSigma);
  }

  // ── choose CPU or GPU per-event fit lambda ────────────────────────────────
  const std::string resultsCol = fitName + "_results";

#ifdef USE_CUDA
  if (cfg.useGPU) {
    // Validate GPU limits before defining the column so the error is reported
    // at configuration time rather than during the event loop.
    if (nParticles > kGPUMaxParticles) {
      throw std::runtime_error(
          "KinematicFitManager: fit '" + fitName +
          "' requests useGPU=true but nParticles (" +
          std::to_string(nParticles) + ") exceeds kGPUMaxParticles (" +
          std::to_string(kGPUMaxParticles) + ")");
    }
    if (nConstraints > kGPUMaxConstraints) {
      throw std::runtime_error(
          "KinematicFitManager: fit '" + fitName +
          "' requests useGPU=true but nConstraints (" +
          std::to_string(nConstraints) + ") exceeds kGPUMaxConstraints (" +
          std::to_string(kGPUMaxConstraints) + ")");
    }

    // Upload static data (resolutions and constraints) to the GPU once.
    // The context is shared across all events via shared_ptr, so device
    // memory for static data is allocated only once per fit definition.
    // Per-event input/output device buffers are managed by kinFitRunGPU()
    // using thread-local storage that grows on demand and is never freed
    // until thread exit — no per-event cudaMalloc/cudaFree overhead.
    auto ctx = std::make_shared<CudaKinFitContext>(
        sigmas.data(), nParticles,
        conTypes.data(), conIdx1.data(), conIdx2.data(), conIdx3.data(),
        conTarget.data(), conSigma.data(), nConstraints);

    auto fitLambdaGPU =
        [ctx, nParticles, maxIter = cfg.maxIterations,
         tol = static_cast<float>(cfg.convergenceTolerance)](
            ROOT::VecOps::RVec<Float_t> &inputs, bool runVar)
        -> ROOT::VecOps::RVec<Float_t> {
      if (!runVar) {
        return ROOT::VecOps::RVec<Float_t>(2 + 3 * nParticles, -1.0f);
      }
      const int outSize = 2 + nParticles * 3;
      ROOT::VecOps::RVec<Float_t> out(outSize);
      kinFitRunGPU(*ctx, inputs.data(), /*nEvents=*/1, maxIter, tol, out.data());
      return out;
    };

    dataManager_m->Define(resultsCol, fitLambdaGPU, {inputsCol, runVarCol},
                           *systematicManager_m);
  } else
#else
  if (cfg.useGPU) {
    throw std::runtime_error(
        "KinematicFitManager: fit '" + fitName +
        "' requests useGPU=true but this build was compiled without CUDA "
        "support.  Rebuild the project with -DUSE_CUDA=ON.");
  }
#endif
  { // CPU path (also the else-branch of the USE_CUDA block above)
    // ── per-event fit lambda ────────────────────────────────────────────────
    // Output layout: [chi2, converged, pT_0, eta_0, phi_0, pT_1, eta_1, phi_1, ...]
    // When runVar is false the fit is skipped and all outputs are -1.
    // The precomputed sigmas vector is captured to avoid re-reading cfg per event.
    auto fitLambda =
        [cfg, nParticles, sigmas](ROOT::VecOps::RVec<Float_t> &inputs, bool runVar)
        -> ROOT::VecOps::RVec<Float_t> {
      if (!runVar) {
        // Sentinel: chi2 = -1, converged = -1 (→ false), fitted momenta = -1
        return ROOT::VecOps::RVec<Float_t>(2 + 3 * nParticles, -1.0f);
      }
      KinematicFit fitter;

      for (int i = 0; i < nParticles; ++i) {
        const double pt   = static_cast<double>(inputs[i * 4 + 0]);
        const double eta  = static_cast<double>(inputs[i * 4 + 1]);
        const double phi  = static_cast<double>(inputs[i * 4 + 2]);
        const double mass = static_cast<double>(inputs[i * 4 + 3]);
        const double sigPt  = static_cast<double>(sigmas[static_cast<size_t>(i) * 3 + 0]);
        const double sigEta = static_cast<double>(sigmas[static_cast<size_t>(i) * 3 + 1]);
        const double sigPhi = static_cast<double>(sigmas[static_cast<size_t>(i) * 3 + 2]);
        fitter.addParticle({pt, eta, phi, mass, sigPt, sigEta, sigPhi});
      }

      for (const auto &con : cfg.constraints) {
        if (con.type == KinFitConstraintConfig::Type::PT) {
          fitter.addPtConstraint(con.idx1, con.targetValue);
        } else if (con.idx3 >= 0) {
          fitter.addThreeBodyMassConstraint(con.idx1, con.idx2, con.idx3,
                                            con.targetValue, con.massSigma);
        } else {
          fitter.addMassConstraint(con.idx1, con.idx2, con.targetValue,
                                   con.massSigma);
        }
      }

      const KinFitResult res = fitter.fit(cfg.maxIterations,
                                          cfg.convergenceTolerance);

      ROOT::VecOps::RVec<Float_t> out(2 + 3 * nParticles);
      out[0] = static_cast<Float_t>(res.chi2);
      out[1] = static_cast<Float_t>(res.converged ? 1.0f : 0.0f);
      for (int i = 0; i < nParticles; ++i) {
        out[2 + i * 3 + 0] = static_cast<Float_t>(res.fittedParticles[i].pt);
        out[2 + i * 3 + 1] = static_cast<Float_t>(res.fittedParticles[i].eta);
        out[2 + i * 3 + 2] = static_cast<Float_t>(res.fittedParticles[i].phi);
      }
      return out;
    };

    dataManager_m->Define(resultsCol, fitLambda, {inputsCol, runVarCol},
                           *systematicManager_m);
  }

  // ── slice individual output columns ──────────────────────────────────────
  dataManager_m->Define(fitName + "_chi2",
      [](const RVecF &v) -> Float_t { return v[0]; },
      {resultsCol}, *systematicManager_m);

  dataManager_m->Define(fitName + "_converged",
      [](const RVecF &v) -> bool { return v[1] > 0.5f; },
      {resultsCol}, *systematicManager_m);

  for (int i = 0; i < nParticles; ++i) {
    const std::string &pName = cfg.particles[i].name;

    dataManager_m->Define(fitName + "_" + pName + "_pt_fitted",
        [i](const RVecF &v) -> Float_t { return v[2 + i * 3 + 0]; },
        {resultsCol}, *systematicManager_m);

    dataManager_m->Define(fitName + "_" + pName + "_eta_fitted",
        [i](const RVecF &v) -> Float_t { return v[2 + i * 3 + 1]; },
        {resultsCol}, *systematicManager_m);

    dataManager_m->Define(fitName + "_" + pName + "_phi_fitted",
        [i](const RVecF &v) -> Float_t { return v[2 + i * 3 + 2]; },
        {resultsCol}, *systematicManager_m);
  }
}

/**
 * @brief Apply all configured kinematic fits to the current RDataFrame.
 */
void KinematicFitManager::applyAllFits() {
  for (const auto &name : getAllFitNames()) {
    applyFit(name);
  }
}

/**
 * @brief Get the run-variable column name for a named fit.
 *
 * Returns an empty string when no runVar was specified in the config, which
 * applyFit() treats as "always run" (a constant-true helper column is
 * auto-generated in that case).
 */
const std::string &
KinematicFitManager::getRunVar(const std::string &fitName) const {
  auto it = kinfit_runVars_m.find(fitName);
  if (it != kinfit_runVars_m.end()) {
    return it->second;
  }
  throw std::runtime_error("KinematicFitManager: fit not found: " + fitName);
}

/**
 * @brief Get the KinFitConfig for a named fit.
 */
const KinFitConfig &
KinematicFitManager::getFitConfig(const std::string &fitName) const {
  return getObject(fitName);
}

/**
 * @brief Get the names of all configured fits.
 */
std::vector<std::string> KinematicFitManager::getAllFitNames() const {
  std::vector<std::string> names;
  names.reserve(objects_m.size());
  for (const auto &pair : objects_m) {
    names.push_back(pair.first);
  }
  return names;
}

// ── setupFromConfigFile ───────────────────────────────────────────────────────

void KinematicFitManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error(
        "KinematicFitManager: ConfigManager not set");
  }
  registerFits(*configManager_m);
}

// ── private: parse configuration ─────────────────────────────────────────────

/**
 * @brief Parse the kinematic fit configuration and register fit instances.
 *
 * Required keys per config-file line:
 *   name, particles, constraints, maxIterations, convergenceTolerance
 *
 * Optional keys (fall back to struct defaults if absent):
 *   leptonPtResolution, leptonEtaResolution, leptonPhiResolution,
 *   jetPtResolution,    jetEtaResolution,    jetPhiResolution,
 *   metPtResolution,    metEtaResolution,    metPhiResolution,
 *   recoilPtResolution, recoilEtaResolution, recoilPhiResolution
 */
void KinematicFitManager::registerFits(
    const IConfigurationProvider &configProvider) {
  const auto fitConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("kinematicFitConfig"),
      {"name", "particles", "constraints",
       "maxIterations", "convergenceTolerance"});

  for (const auto &entry : fitConfig) {
    KinFitConfig cfg;

    // ── particles ──────────────────────────────────────────────────────────
    for (const auto &spec : splitBy(entry.at("particles"), ',')) {
      cfg.particles.push_back(parseParticleSpec(spec));
    }
    if (cfg.particles.empty()) {
      throw std::runtime_error(
          "KinematicFitManager: fit '" + entry.at("name") +
          "' has no particles");
    }

    // ── constraints ────────────────────────────────────────────────────────
    for (const auto &spec : splitBy(entry.at("constraints"), ',')) {
      const auto con = parseConstraintSpec(spec);
      const int n = static_cast<int>(cfg.particles.size());
      // Validate all referenced indices are in range
      auto checkIdx = [&](int idx, const char *role) {
        if (idx < 0 || idx >= n) {
          throw std::runtime_error(
              "KinematicFitManager: " + std::string(role) +
              " particle index " + std::to_string(idx) +
              " out of range in '" + spec +
              "' for fit '" + entry.at("name") + "'");
        }
      };
      checkIdx(con.idx1, "constraint");
      if (con.type == KinFitConstraintConfig::Type::MASS) {
        checkIdx(con.idx2, "constraint");
        if (con.idx3 >= 0) checkIdx(con.idx3, "constraint");
      }
      cfg.constraints.push_back(con);
    }

    // ── convergence ────────────────────────────────────────────────────────
    cfg.maxIterations =
        parseInt(entry.at("maxIterations"), "maxIterations");
    cfg.convergenceTolerance =
        parseDouble(entry.at("convergenceTolerance"), "convergenceTolerance");

    // ── optional resolution parameters (fall back to struct defaults) ──────
    auto getOpt = [&](const std::string &key, double def) -> double {
      auto it = entry.find(key);
      return (it != entry.end()) ? parseDouble(it->second, key) : def;
    };
    cfg.leptonPtResolution  = getOpt("leptonPtResolution",  cfg.leptonPtResolution);
    cfg.leptonEtaResolution = getOpt("leptonEtaResolution", cfg.leptonEtaResolution);
    cfg.leptonPhiResolution = getOpt("leptonPhiResolution", cfg.leptonPhiResolution);
    cfg.jetPtResolution     = getOpt("jetPtResolution",     cfg.jetPtResolution);
    cfg.jetEtaResolution    = getOpt("jetEtaResolution",    cfg.jetEtaResolution);
    cfg.jetPhiResolution    = getOpt("jetPhiResolution",    cfg.jetPhiResolution);
    cfg.metPtResolution     = getOpt("metPtResolution",     cfg.metPtResolution);
    cfg.metEtaResolution    = getOpt("metEtaResolution",    cfg.metEtaResolution);
    cfg.metPhiResolution    = getOpt("metPhiResolution",    cfg.metPhiResolution);
    cfg.recoilPtResolution  = getOpt("recoilPtResolution",  cfg.recoilPtResolution);
    cfg.recoilEtaResolution = getOpt("recoilEtaResolution", cfg.recoilEtaResolution);
    cfg.recoilPhiResolution = getOpt("recoilPhiResolution", cfg.recoilPhiResolution);

    // ── optional useGPU flag ───────────────────────────────────────────────
    // Accepted values: "true" / "false" / "1" / "0" (case-insensitive).
    // Default is false (CPU execution) so existing configs are unaffected.
    {
      auto it = entry.find("useGPU");
      if (it != entry.end()) {
        std::string val = it->second;
        // Normalize to lowercase for case-insensitive comparison.
        std::transform(val.begin(), val.end(), val.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (val == "true" || val == "1") {
          cfg.useGPU = true;
        } else if (val == "false" || val == "0") {
          cfg.useGPU = false;
        } else {
          throw std::runtime_error(
              "KinematicFitManager: invalid value '" + it->second +
              "' for key 'useGPU' in fit '" + entry.at("name") +
              "' – expected 'true', 'false', '1', or '0'");
        }
      }
    }

    objects_m.emplace(entry.at("name"), std::move(cfg));

    // ── optional runVar ────────────────────────────────────────────────────
    // Store the column name (empty string = always run; applyFit() will
    // auto-define a constant-true helper column in that case).
    auto runVarIt = entry.find("runVar");
    const std::string runVar =
        (runVarIt != entry.end()) ? runVarIt->second : "";
    kinfit_runVars_m.emplace(entry.at("name"), runVar);
  }
}
