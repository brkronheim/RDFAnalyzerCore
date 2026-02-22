#include <KinematicFit.h>
#include <KinematicFitManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <cmath>
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
 *  - "idx1+idx2:targetMass"         — two-body invariant mass constraint
 *  - "idx1+idx2+idx3:targetMass"    — three-body invariant mass constraint
 *                                     (e.g. top: m(b, l, ν) = 173.3 GeV)
 *  - "pt:idx:targetPt"              — pT constraint on a single particle
 *                                     (set targetPt=0 for no-MET events)
 *
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

  // ── Mass constraint: "idx1+idx2:mass" or "idx1+idx2+idx3:mass" ────────
  const auto colonParts = splitBy(spec, ':');
  if (colonParts.size() != 2) {
    throw std::runtime_error(
        "KinematicFitManager: invalid constraint spec '" + spec +
        "' – expected idx1+idx2:mass, idx1+idx2+idx3:mass, or pt:idx:targetPt");
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

  // ── per-event fit lambda ──────────────────────────────────────────────────
  // Output layout: [chi2, converged, pT_0, eta_0, phi_0, pT_1, eta_1, phi_1, ...]
  auto fitLambda =
      [cfg, nParticles](ROOT::VecOps::RVec<Float_t> &inputs)
      -> ROOT::VecOps::RVec<Float_t> {
    KinematicFit fitter;

    for (int i = 0; i < nParticles; ++i) {
      const auto &p     = cfg.particles[i];
      const double pt   = static_cast<double>(inputs[i * 4 + 0]);
      const double eta  = static_cast<double>(inputs[i * 4 + 1]);
      const double phi  = static_cast<double>(inputs[i * 4 + 2]);
      const double mass = static_cast<double>(inputs[i * 4 + 3]);

      double sigPt, sigEta, sigPhi;
      if (p.type == "lepton") {
        sigPt  = cfg.leptonPtResolution;
        sigEta = cfg.leptonEtaResolution;
        sigPhi = cfg.leptonPhiResolution;
      } else if (p.type == "met") {
        sigPt  = cfg.metPtResolution;
        sigEta = cfg.metEtaResolution; // very large → eta free
        sigPhi = cfg.metPhiResolution;
      } else if (p.type == "recoil") {
        // Large uncertainties: the recoil is soft/composite, poorly known.
        // No mass constraint is imposed on this particle; it participates in
        // the fit only to absorb the χ² from the hadronic activity.
        sigPt  = cfg.recoilPtResolution;
        sigEta = cfg.recoilEtaResolution;
        sigPhi = cfg.recoilPhiResolution;
      } else { // "jet" or unrecognised
        sigPt  = cfg.jetPtResolution;
        sigEta = cfg.jetEtaResolution;
        sigPhi = cfg.jetPhiResolution;
      }

      fitter.addParticle({pt, eta, phi, mass, sigPt, sigEta, sigPhi});
    }

    for (const auto &con : cfg.constraints) {
      if (con.type == KinFitConstraintConfig::Type::PT) {
        fitter.addPtConstraint(con.idx1, con.targetValue);
      } else if (con.idx3 >= 0) {
        fitter.addThreeBodyMassConstraint(con.idx1, con.idx2, con.idx3,
                                          con.targetValue);
      } else {
        fitter.addMassConstraint(con.idx1, con.idx2, con.targetValue);
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

  const std::string resultsCol = fitName + "_results";
  dataManager_m->Define(resultsCol, fitLambda, {inputsCol},
                         *systematicManager_m);

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

    objects_m.emplace(entry.at("name"), std::move(cfg));
  }
}
