#include <KinematicFit.h>
#include <KinematicFitManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
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
 * @brief Parse one particle specification: "label:ptCol:etaCol:phiCol:massCol:type".
 *
 * Fields:
 *  - label   : user-defined name (used in output column names)
 *  - ptCol   : column for pT
 *  - etaCol  : column for eta; use "_" or "" for MET (eta fixed at 0)
 *  - phiCol  : column for phi
 *  - massCol : column name OR numeric literal (e.g. "0", "0.106")
 *  - type    : "lepton", "jet", or "met"
 */
KinFitParticleConfig parseParticleSpec(const std::string &spec) {
  const auto parts = splitBy(spec, ':');
  if (parts.size() != 6) {
    throw std::runtime_error(
        "KinematicFitManager: invalid particle spec '" + spec +
        "' – expected label:ptCol:etaCol:phiCol:massCol:type (6 colon-separated fields)");
  }
  KinFitParticleConfig p;
  p.name    = parts[0];
  p.ptCol   = parts[1];
  p.etaCol  = parts[2]; // "_" or "" → MET (no eta column)
  p.phiCol  = parts[3];
  p.massCol = parts[4]; // column name or numeric literal
  p.type    = parts[5]; // "lepton", "jet", or "met"
  return p;
}

/**
 * @brief Parse one constraint specification: "idx1+idx2:targetMass".
 *
 * idx1 and idx2 are zero-based indices into the fit's particle list.
 */
KinFitConstraintConfig parseConstraintSpec(const std::string &spec) {
  const auto colonParts = splitBy(spec, ':');
  if (colonParts.size() != 2) {
    throw std::runtime_error(
        "KinematicFitManager: invalid constraint spec '" + spec +
        "' – expected idx1+idx2:targetMass");
  }
  const auto idxParts = splitBy(colonParts[0], '+');
  if (idxParts.size() != 2) {
    throw std::runtime_error(
        "KinematicFitManager: invalid constraint indices in '" + spec +
        "' – expected two indices separated by '+'");
  }
  KinFitConstraintConfig con;
  con.idx1       = parseInt(idxParts[0], "constraintIdx1");
  con.idx2       = parseInt(idxParts[1], "constraintIdx2");
  con.targetMass = parseDouble(colonParts[1], "constraintMass");
  return con;
}

} // anonymous namespace

// ── constructor ───────────────────────────────────────────────────────────────

/**
 * @brief Construct a KinematicFitManager and load fits from configuration.
 */
KinematicFitManager::KinematicFitManager(
    const IConfigurationProvider &configProvider) {
  registerFits(configProvider);
}

// ── public interface ──────────────────────────────────────────────────────────

/**
 * @brief Apply a single kinematic fit to the current RDataFrame.
 *
 * For each particle in the fit:
 *  - pT, eta, phi, mass are packed into one RVec<Float_t> (4 values/particle).
 *  - For MET (etaCol == "" or "_"), a constant 0 column is defined automatically.
 *  - For numeric massCol literals, a constant column is defined automatically.
 *
 * Output layout (2 + 3*N floats, where N = number of particles):
 *  [0]          chi2
 *  [1]          converged (1=yes, 0=no)
 *  [2+3i+0..2]  pT, eta, phi of fitted particle i
 *
 * Output RDataFrame columns:
 *  {name}_chi2,  {name}_converged,
 *  {name}_{label}_pt_fitted, {name}_{label}_eta_fitted, {name}_{label}_phi_fitted
 *  (one set per particle in the fit)
 */
void KinematicFitManager::applyFit(const std::string &fitName) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error(
        "KinematicFitManager: DataManager or SystematicManager not set");
  }

  const KinFitConfig cfg      = getFitConfig(fitName); // copy for lambda capture
  const int          nParticles = static_cast<int>(cfg.particles.size());

  // ── build input column list (4 values per particle: pT, eta, phi, mass) ──
  std::vector<std::string> inputCols;
  inputCols.reserve(4 * nParticles);

  for (int i = 0; i < nParticles; ++i) {
    const auto &p = cfg.particles[i];

    // pT column (always required)
    inputCols.push_back(p.ptCol);

    // eta column – if absent (MET), define a constant zero column
    if (p.etaCol.empty() || p.etaCol == "_") {
      const std::string tmpEta = fitName + "_" + p.name + "_kinfit_eta";
      dataManager_m->Define(tmpEta,
          [](ULong64_t) -> Float_t { return 0.0f; },
          {"rdfentry_"}, *systematicManager_m);
      inputCols.push_back(tmpEta);
    } else {
      inputCols.push_back(p.etaCol);
    }

    // phi column (always required)
    inputCols.push_back(p.phiCol);

    // mass column – may be a column name or a numeric literal
    bool  isMassLiteral = false;
    float massLiteralVal = 0.0f;
    if (!p.massCol.empty() && p.massCol != "_") {
      try {
        massLiteralVal = std::stof(p.massCol);
        isMassLiteral  = true;
      } catch (...) {
        isMassLiteral  = false;
      }
    } else {
      isMassLiteral = true; // empty / "_" → massless
    }

    if (isMassLiteral) {
      const std::string tmpMass = fitName + "_" + p.name + "_kinfit_mass";
      const float massVal = massLiteralVal;
      dataManager_m->Define(tmpMass,
          [massVal](ULong64_t) -> Float_t { return massVal; },
          {"rdfentry_"}, *systematicManager_m);
      inputCols.push_back(tmpMass);
    } else {
      inputCols.push_back(p.massCol);
    }
  }

  // ── pack all inputs into one RVec<Float_t> ────────────────────────────────
  const std::string inputsCol = fitName + "_inputs";
  dataManager_m->DefineVector(inputsCol, inputCols, "Float_t",
                               *systematicManager_m);

  // ── lambda: run the fit per event, return a packed result vector ──────────
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
        sigEta = cfg.metEtaResolution; // very large → eta effectively free
        sigPhi = cfg.metPhiResolution;
      } else { // "jet" or unrecognised type
        sigPt  = cfg.jetPtResolution;
        sigEta = cfg.jetEtaResolution;
        sigPhi = cfg.jetPhiResolution;
      }

      fitter.addParticle({pt, eta, phi, mass, sigPt, sigEta, sigPhi});
    }

    for (const auto &con : cfg.constraints) {
      fitter.addMassConstraint(con.idx1, con.idx2, con.targetMass);
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
  using RVecF = ROOT::VecOps::RVec<Float_t>;

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
 *   metPtResolution,    metEtaResolution,    metPhiResolution
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
      if (con.idx1 < 0 || con.idx1 >= n || con.idx2 < 0 || con.idx2 >= n) {
        throw std::runtime_error(
            "KinematicFitManager: constraint particle index out of range in '" +
            spec + "' for fit '" + entry.at("name") + "'");
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

    objects_m.emplace(entry.at("name"), std::move(cfg));
  }
}
