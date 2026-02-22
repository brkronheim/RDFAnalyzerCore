#ifndef KINEMATICFITMANAGER_H_INCLUDED
#define KINEMATICFITMANAGER_H_INCLUDED

#include <KinematicFit.h>
#include <NamedObjectManager.h>
#include <api/IConfigurationProvider.h>
#include <string>
#include <vector>

/**
 * @brief Configuration for a single kinematic fit instance.
 *
 * Encapsulates the RDataFrame column names for the input 4-momenta of two
 * leptons and two jets, the target invariant masses for the dilepton and dijet
 * systems, the per-particle momentum resolutions, and the fit convergence
 * parameters.
 */
struct KinFitConfig {
  // ── input column names ──────────────────────────────────────────────────
  std::string lepton1Pt;   ///< Column name: lepton-1 pT  [GeV]
  std::string lepton1Eta;  ///< Column name: lepton-1 eta
  std::string lepton1Phi;  ///< Column name: lepton-1 phi [rad]
  std::string lepton1Mass; ///< Column name: lepton-1 mass [GeV]

  std::string lepton2Pt;   ///< Column name: lepton-2 pT  [GeV]
  std::string lepton2Eta;  ///< Column name: lepton-2 eta
  std::string lepton2Phi;  ///< Column name: lepton-2 phi [rad]
  std::string lepton2Mass; ///< Column name: lepton-2 mass [GeV]

  std::string jet1Pt;   ///< Column name: jet-1 pT  [GeV]
  std::string jet1Eta;  ///< Column name: jet-1 eta
  std::string jet1Phi;  ///< Column name: jet-1 phi [rad]
  std::string jet1Mass; ///< Column name: jet-1 mass [GeV]

  std::string jet2Pt;   ///< Column name: jet-2 pT  [GeV]
  std::string jet2Eta;  ///< Column name: jet-2 eta
  std::string jet2Phi;  ///< Column name: jet-2 phi [rad]
  std::string jet2Mass; ///< Column name: jet-2 mass [GeV]

  // ── mass constraints ────────────────────────────────────────────────────
  double dileptonMassConstraint; ///< Target dilepton mass [GeV] (e.g. 91.2 for Z)
  double dijetMassConstraint;    ///< Target dijet mass   [GeV] (e.g. 125.0 for H)

  // ── momentum resolutions ────────────────────────────────────────────────
  double leptonPtResolution;  ///< Fractional lepton pT resolution (sigma_pT/pT)
  double leptonEtaResolution; ///< Absolute lepton eta resolution
  double leptonPhiResolution; ///< Absolute lepton phi resolution [rad]

  double jetPtResolution;  ///< Fractional jet pT resolution (sigma_pT/pT)
  double jetEtaResolution; ///< Absolute jet eta resolution
  double jetPhiResolution; ///< Absolute jet phi resolution [rad]

  // ── convergence parameters ──────────────────────────────────────────────
  int    maxIterations;        ///< Maximum fit iterations
  double convergenceTolerance; ///< Convergence criterion on |delta_chi2|
};

/**
 * @class KinematicFitManager
 * @brief Plugin manager that loads kinematic fit configurations and applies
 *        them as new columns to an RDataFrame.
 *
 * Configuration is read from a text file whose path is given by the
 * "kinematicFitConfig" key in the main analysis config.  Each line of the
 * fit config file describes one fit instance.  Required keys per line:
 *
 *   name, lepton1Pt, lepton1Eta, lepton1Phi, lepton1Mass,
 *   lepton2Pt, lepton2Eta, lepton2Phi, lepton2Mass,
 *   jet1Pt,    jet1Eta,    jet1Phi,    jet1Mass,
 *   jet2Pt,    jet2Eta,    jet2Phi,    jet2Mass,
 *   dileptonMassConstraint, dijetMassConstraint,
 *   leptonPtResolution, leptonEtaResolution, leptonPhiResolution,
 *   jetPtResolution,    jetEtaResolution,    jetPhiResolution,
 *   maxIterations, convergenceTolerance
 *
 * For each configured fit with a given @p name, the following RDataFrame
 * columns are defined:
 *   - @p {name}_chi2        — chi-square of the fit (Float_t)
 *   - @p {name}_converged   — 1 if converged, 0 otherwise (Float_t)
 *   - @p {name}_l1Pt_fitted, @p {name}_l1Eta_fitted, @p {name}_l1Phi_fitted
 *   - @p {name}_l2Pt_fitted, @p {name}_l2Eta_fitted, @p {name}_l2Phi_fitted
 *   - @p {name}_j1Pt_fitted, @p {name}_j1Eta_fitted, @p {name}_j1Phi_fitted
 *   - @p {name}_j2Pt_fitted, @p {name}_j2Eta_fitted, @p {name}_j2Phi_fitted
 *
 * Example config line (dilepton+dijet system, Z+H):
 * @code
 * name=zhFit \
 *   lepton1Pt=lep1_pt lepton1Eta=lep1_eta lepton1Phi=lep1_phi lepton1Mass=lep1_mass \
 *   lepton2Pt=lep2_pt lepton2Eta=lep2_eta lepton2Phi=lep2_phi lepton2Mass=lep2_mass \
 *   jet1Pt=jet1_pt   jet1Eta=jet1_eta   jet1Phi=jet1_phi   jet1Mass=jet1_mass     \
 *   jet2Pt=jet2_pt   jet2Eta=jet2_eta   jet2Phi=jet2_phi   jet2Mass=jet2_mass     \
 *   dileptonMassConstraint=91.2 dijetMassConstraint=125.0 \
 *   leptonPtResolution=0.02 leptonEtaResolution=0.001 leptonPhiResolution=0.001 \
 *   jetPtResolution=0.10    jetEtaResolution=0.05    jetPhiResolution=0.05       \
 *   maxIterations=50 convergenceTolerance=1e-6
 * @endcode
 */
class KinematicFitManager : public NamedObjectManager<KinFitConfig> {
public:
  /**
   * @brief Construct a KinematicFitManager and load fits from configuration.
   * @param configProvider Reference to the analysis configuration provider.
   */
  explicit KinematicFitManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Apply a single kinematic fit to the current RDataFrame.
   *
   * Defines all output columns (chi2, converged flag, fitted momenta) for
   * the fit identified by @p fitName.
   * @param fitName Name of the fit as given in the config file.
   */
  void applyFit(const std::string &fitName);

  /**
   * @brief Apply all configured kinematic fits to the current RDataFrame.
   */
  void applyAllFits();

  /**
   * @brief Get the KinFitConfig for a named fit.
   * @param fitName Name of the fit.
   * @return Const reference to the KinFitConfig.
   */
  const KinFitConfig &getFitConfig(const std::string &fitName) const;

  /**
   * @brief Get the names of all configured fits.
   * @return Vector of fit names.
   */
  std::vector<std::string> getAllFitNames() const;

  std::string type() const override { return "KinematicFitManager"; }

  void setupFromConfigFile() override;

private:
  /**
   * @brief Parse and register fits from the configuration provider.
   * @param configProvider Reference to the configuration provider.
   */
  void registerFits(const IConfigurationProvider &configProvider);
};

#endif // KINEMATICFITMANAGER_H_INCLUDED
