#include <KinematicFit.h>
#include <KinematicFitManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <stdexcept>
#include <string>
#include <vector>

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
 * Packs the 16 input 4-momentum columns into a single RVec, runs the fit
 * inside a lambda (creating a new KinematicFit object per event, which is
 * thread-safe), and then slices the result vector into individual output
 * columns.
 *
 * Output columns defined (Float_t unless noted):
 *   {name}_chi2, {name}_converged (Float_t, 0/1),
 *   {name}_l1Pt_fitted, {name}_l1Eta_fitted, {name}_l1Phi_fitted,
 *   {name}_l2Pt_fitted, {name}_l2Eta_fitted, {name}_l2Phi_fitted,
 *   {name}_j1Pt_fitted, {name}_j1Eta_fitted, {name}_j1Phi_fitted,
 *   {name}_j2Pt_fitted, {name}_j2Eta_fitted, {name}_j2Phi_fitted
 */
void KinematicFitManager::applyFit(const std::string &fitName) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error(
        "KinematicFitManager: DataManager or SystematicManager not set");
  }

  const KinFitConfig cfg = getFitConfig(fitName); // copy for lambda capture

  // ── pack the 16 input columns into one RVec<Float_t> ────────────────────
  const std::vector<std::string> inputCols = {
      cfg.lepton1Pt,  cfg.lepton1Eta,  cfg.lepton1Phi,  cfg.lepton1Mass,
      cfg.lepton2Pt,  cfg.lepton2Eta,  cfg.lepton2Phi,  cfg.lepton2Mass,
      cfg.jet1Pt,     cfg.jet1Eta,     cfg.jet1Phi,     cfg.jet1Mass,
      cfg.jet2Pt,     cfg.jet2Eta,     cfg.jet2Phi,     cfg.jet2Mass};

  const std::string inputsCol = fitName + "_inputs";
  dataManager_m->DefineVector(inputsCol, inputCols, "Float_t",
                               *systematicManager_m);

  // ── lambda that performs the fit and returns all results ─────────────────
  // Output layout (14 floats):
  //  [0]  chi2
  //  [1]  converged (0 or 1)
  //  [2..4]  l1 (pT, eta, phi) fitted
  //  [5..7]  l2 (pT, eta, phi) fitted
  //  [8..10] j1 (pT, eta, phi) fitted
  // [11..13] j2 (pT, eta, phi) fitted
  auto fitLambda =
      [cfg](ROOT::VecOps::RVec<Float_t> &inputs)
      -> ROOT::VecOps::RVec<Float_t> {
    KinematicFit fitter;

    // Particles: lepton1, lepton2, jet1, jet2
    fitter.addParticle({static_cast<double>(inputs[0]),
                        static_cast<double>(inputs[1]),
                        static_cast<double>(inputs[2]),
                        static_cast<double>(inputs[3]),
                        cfg.leptonPtResolution,
                        cfg.leptonEtaResolution,
                        cfg.leptonPhiResolution});
    fitter.addParticle({static_cast<double>(inputs[4]),
                        static_cast<double>(inputs[5]),
                        static_cast<double>(inputs[6]),
                        static_cast<double>(inputs[7]),
                        cfg.leptonPtResolution,
                        cfg.leptonEtaResolution,
                        cfg.leptonPhiResolution});
    fitter.addParticle({static_cast<double>(inputs[8]),
                        static_cast<double>(inputs[9]),
                        static_cast<double>(inputs[10]),
                        static_cast<double>(inputs[11]),
                        cfg.jetPtResolution,
                        cfg.jetEtaResolution,
                        cfg.jetPhiResolution});
    fitter.addParticle({static_cast<double>(inputs[12]),
                        static_cast<double>(inputs[13]),
                        static_cast<double>(inputs[14]),
                        static_cast<double>(inputs[15]),
                        cfg.jetPtResolution,
                        cfg.jetEtaResolution,
                        cfg.jetPhiResolution});

    fitter.addMassConstraint(0, 1, cfg.dileptonMassConstraint);
    fitter.addMassConstraint(2, 3, cfg.dijetMassConstraint);

    const KinFitResult res = fitter.fit(cfg.maxIterations,
                                        cfg.convergenceTolerance);

    ROOT::VecOps::RVec<Float_t> out(14);
    out[0]  = static_cast<Float_t>(res.chi2);
    out[1]  = static_cast<Float_t>(res.converged ? 1.0 : 0.0);
    out[2]  = static_cast<Float_t>(res.fittedParticles[0].pt);
    out[3]  = static_cast<Float_t>(res.fittedParticles[0].eta);
    out[4]  = static_cast<Float_t>(res.fittedParticles[0].phi);
    out[5]  = static_cast<Float_t>(res.fittedParticles[1].pt);
    out[6]  = static_cast<Float_t>(res.fittedParticles[1].eta);
    out[7]  = static_cast<Float_t>(res.fittedParticles[1].phi);
    out[8]  = static_cast<Float_t>(res.fittedParticles[2].pt);
    out[9]  = static_cast<Float_t>(res.fittedParticles[2].eta);
    out[10] = static_cast<Float_t>(res.fittedParticles[2].phi);
    out[11] = static_cast<Float_t>(res.fittedParticles[3].pt);
    out[12] = static_cast<Float_t>(res.fittedParticles[3].eta);
    out[13] = static_cast<Float_t>(res.fittedParticles[3].phi);
    return out;
  };

  // Define the combined result vector column
  const std::string resultsCol = fitName + "_results";
  dataManager_m->Define(resultsCol, fitLambda, {inputsCol},
                         *systematicManager_m);

  // ── slice individual output columns ─────────────────────────────────────
  using RVecF = ROOT::VecOps::RVec<Float_t>;

  dataManager_m->Define(fitName + "_chi2",
      [](const RVecF &v) -> Float_t { return v[0]; },
      {resultsCol}, *systematicManager_m);

  dataManager_m->Define(fitName + "_converged",
      [](const RVecF &v) -> bool { return v[1] > 0.5f; },
      {resultsCol}, *systematicManager_m);

  dataManager_m->Define(fitName + "_l1Pt_fitted",
      [](const RVecF &v) -> Float_t { return v[2]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_l1Eta_fitted",
      [](const RVecF &v) -> Float_t { return v[3]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_l1Phi_fitted",
      [](const RVecF &v) -> Float_t { return v[4]; },
      {resultsCol}, *systematicManager_m);

  dataManager_m->Define(fitName + "_l2Pt_fitted",
      [](const RVecF &v) -> Float_t { return v[5]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_l2Eta_fitted",
      [](const RVecF &v) -> Float_t { return v[6]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_l2Phi_fitted",
      [](const RVecF &v) -> Float_t { return v[7]; },
      {resultsCol}, *systematicManager_m);

  dataManager_m->Define(fitName + "_j1Pt_fitted",
      [](const RVecF &v) -> Float_t { return v[8]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_j1Eta_fitted",
      [](const RVecF &v) -> Float_t { return v[9]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_j1Phi_fitted",
      [](const RVecF &v) -> Float_t { return v[10]; },
      {resultsCol}, *systematicManager_m);

  dataManager_m->Define(fitName + "_j2Pt_fitted",
      [](const RVecF &v) -> Float_t { return v[11]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_j2Eta_fitted",
      [](const RVecF &v) -> Float_t { return v[12]; },
      {resultsCol}, *systematicManager_m);
  dataManager_m->Define(fitName + "_j2Phi_fitted",
      [](const RVecF &v) -> Float_t { return v[13]; },
      {resultsCol}, *systematicManager_m);
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

// ── private helpers ───────────────────────────────────────────────────────────

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
} // anonymous namespace

/**
 * @brief Parse the kinematic fit configuration and register fit instances.
 */
void KinematicFitManager::registerFits(
    const IConfigurationProvider &configProvider) {
  const auto fitConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("kinematicFitConfig"),
      {"name",
       "lepton1Pt",   "lepton1Eta",   "lepton1Phi",   "lepton1Mass",
       "lepton2Pt",   "lepton2Eta",   "lepton2Phi",   "lepton2Mass",
       "jet1Pt",      "jet1Eta",      "jet1Phi",      "jet1Mass",
       "jet2Pt",      "jet2Eta",      "jet2Phi",      "jet2Mass",
       "dileptonMassConstraint", "dijetMassConstraint",
       "leptonPtResolution",  "leptonEtaResolution",  "leptonPhiResolution",
       "jetPtResolution",     "jetEtaResolution",     "jetPhiResolution",
       "maxIterations",       "convergenceTolerance"});

  for (const auto &entry : fitConfig) {
    KinFitConfig cfg;

    cfg.lepton1Pt   = entry.at("lepton1Pt");
    cfg.lepton1Eta  = entry.at("lepton1Eta");
    cfg.lepton1Phi  = entry.at("lepton1Phi");
    cfg.lepton1Mass = entry.at("lepton1Mass");

    cfg.lepton2Pt   = entry.at("lepton2Pt");
    cfg.lepton2Eta  = entry.at("lepton2Eta");
    cfg.lepton2Phi  = entry.at("lepton2Phi");
    cfg.lepton2Mass = entry.at("lepton2Mass");

    cfg.jet1Pt   = entry.at("jet1Pt");
    cfg.jet1Eta  = entry.at("jet1Eta");
    cfg.jet1Phi  = entry.at("jet1Phi");
    cfg.jet1Mass = entry.at("jet1Mass");

    cfg.jet2Pt   = entry.at("jet2Pt");
    cfg.jet2Eta  = entry.at("jet2Eta");
    cfg.jet2Phi  = entry.at("jet2Phi");
    cfg.jet2Mass = entry.at("jet2Mass");

    cfg.dileptonMassConstraint =
        parseDouble(entry.at("dileptonMassConstraint"), "dileptonMassConstraint");
    cfg.dijetMassConstraint =
        parseDouble(entry.at("dijetMassConstraint"), "dijetMassConstraint");

    cfg.leptonPtResolution  =
        parseDouble(entry.at("leptonPtResolution"),  "leptonPtResolution");
    cfg.leptonEtaResolution =
        parseDouble(entry.at("leptonEtaResolution"), "leptonEtaResolution");
    cfg.leptonPhiResolution =
        parseDouble(entry.at("leptonPhiResolution"), "leptonPhiResolution");

    cfg.jetPtResolution  =
        parseDouble(entry.at("jetPtResolution"),  "jetPtResolution");
    cfg.jetEtaResolution =
        parseDouble(entry.at("jetEtaResolution"), "jetEtaResolution");
    cfg.jetPhiResolution =
        parseDouble(entry.at("jetPhiResolution"), "jetPhiResolution");

    cfg.maxIterations =
        parseInt(entry.at("maxIterations"), "maxIterations");
    cfg.convergenceTolerance =
        parseDouble(entry.at("convergenceTolerance"), "convergenceTolerance");

    objects_m.emplace(entry.at("name"), cfg);
  }
}
