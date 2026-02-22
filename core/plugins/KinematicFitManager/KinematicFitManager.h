#ifndef KINEMATICFITMANAGER_H_INCLUDED
#define KINEMATICFITMANAGER_H_INCLUDED

#include <KinematicFit.h>
#include <NamedObjectManager.h>
#include <api/IConfigurationProvider.h>
#include <string>
#include <vector>

/**
 * @brief Configuration for one particle in the kinematic fit.
 *
 * Specifies the RDataFrame column names for the measured 4-momentum and the
 * particle type (used to look up the momentum resolution).
 *
 * For MET (missing transverse energy), set @p etaCol to an empty string or
 * "_".  Eta will be fixed at 0 with a very large uncertainty so it is
 * effectively unconstrained by the fit.  Set @p massCol to "0" or leave it
 * empty to treat the particle as massless (neutrino).
 *
 * @p massCol may also be a numeric literal (e.g. "0.106") instead of a column
 * name.  In that case, a constant column is defined automatically.
 */
struct KinFitParticleConfig {
  std::string name;     ///< Label used in output column names
  std::string ptCol;    ///< Column name for transverse momentum [GeV]
  std::string etaCol;   ///< Column name for pseudorapidity ("" or "_" for MET)
  std::string phiCol;   ///< Column name for azimuthal angle [rad]
  std::string massCol;  ///< Column name OR numeric literal for mass [GeV]
  std::string type;     ///< Particle type: "lepton", "jet", or "met"
};

/**
 * @brief A single two-body invariant mass constraint.
 */
struct KinFitConstraintConfig {
  int    idx1;       ///< Index of the first  particle in the particles vector
  int    idx2;       ///< Index of the second particle in the particles vector
  double targetMass; ///< Target invariant mass [GeV]
};

/**
 * @brief Complete configuration for one kinematic fit instance.
 */
struct KinFitConfig {
  std::vector<KinFitParticleConfig>   particles;   ///< Ordered list of particles
  std::vector<KinFitConstraintConfig> constraints; ///< Two-body mass constraints

  // ── per-type resolution parameters (with physics-motivated defaults) ──────
  double leptonPtResolution  = 0.02;   ///< Fractional lepton pT resolution
  double leptonEtaResolution = 0.001;  ///< Absolute lepton eta resolution
  double leptonPhiResolution = 0.001;  ///< Absolute lepton phi resolution [rad]

  double jetPtResolution     = 0.10;   ///< Fractional jet pT resolution
  double jetEtaResolution    = 0.05;   ///< Absolute jet eta resolution
  double jetPhiResolution    = 0.05;   ///< Absolute jet phi resolution [rad]

  double metPtResolution     = 0.20;   ///< Fractional MET pT resolution
  /// @brief MET eta resolution.  Set very large so eta is effectively free.
  double metEtaResolution    = 100.0;
  double metPhiResolution    = 0.05;   ///< Absolute MET phi resolution [rad]

  int    maxIterations        = 50;    ///< Maximum linearisation iterations
  double convergenceTolerance = 1e-6;  ///< Convergence criterion on |Δχ²|
};

/**
 * @class KinematicFitManager
 * @brief Plugin manager that loads kinematic fit configurations and applies
 *        them as new columns to an RDataFrame.
 *
 * Configuration is read from a text file whose path is given by the
 * "kinematicFitConfig" key in the main analysis config.  Each line describes
 * one fit instance with the following keys:
 *
 *   Required:
 *     name            – unique identifier for the fit
 *     particles       – comma-separated list of particle specs, each with
 *                       the form  label:ptCol:etaCol:phiCol:massCol:type
 *                         - etaCol: use "_" or "" for MET (eta fixed at 0)
 *                         - massCol: column name OR numeric literal
 *                         - type: "lepton", "jet", or "met"
 *     constraints     – comma-separated two-body mass constraints, each with
 *                       the form  idx1+idx2:targetMass
 *                       (idx1/idx2 are zero-based indices into the particles list)
 *     maxIterations        – maximum number of fit iterations
 *     convergenceTolerance – convergence criterion on |Δχ²|
 *
 *   Optional (falling back to built-in defaults if absent):
 *     leptonPtResolution, leptonEtaResolution, leptonPhiResolution
 *     jetPtResolution,    jetEtaResolution,    jetPhiResolution
 *     metPtResolution,    metEtaResolution,    metPhiResolution
 *
 * For each fit named @p N, the following RDataFrame columns are defined:
 *   - @p N_chi2          — chi-square of the fit (Float_t)
 *   - @p N_converged     — true if the fit converged (bool)
 *   - @p N_{label}_pt_fitted  — fitted pT  for each particle (Float_t)
 *   - @p N_{label}_eta_fitted — fitted eta for each particle (Float_t)
 *   - @p N_{label}_phi_fitted — fitted phi for each particle (Float_t)
 *
 * @par Example config lines
 * @code
 * # Z+H → ll bb  (2 leptons + 2 jets)
 * name=zhFit \
 *   particles=mu1:mu1_pt:mu1_eta:mu1_phi:mu1_mass:lepton,mu2:mu2_pt:mu2_eta:mu2_phi:mu2_mass:lepton,bjet1:bjet1_pt:bjet1_eta:bjet1_phi:bjet1_mass:jet,bjet2:bjet2_pt:bjet2_eta:bjet2_phi:bjet2_mass:jet \
 *   constraints=0+1:91.2,2+3:125.0 \
 *   leptonPtResolution=0.02 leptonEtaResolution=0.001 leptonPhiResolution=0.001 \
 *   jetPtResolution=0.10    jetEtaResolution=0.05    jetPhiResolution=0.05 \
 *   maxIterations=50 convergenceTolerance=1e-6
 *
 * # W → lν  (1 lepton + MET)
 * name=wlvFit \
 *   particles=lep:lep_pt:lep_eta:lep_phi:lep_mass:lepton,nu:met_pt:_:met_phi:0:met \
 *   constraints=0+1:80.4 \
 *   leptonPtResolution=0.02 leptonEtaResolution=0.001 leptonPhiResolution=0.001 \
 *   metPtResolution=0.20 metPhiResolution=0.05 \
 *   maxIterations=50 convergenceTolerance=1e-6
 *
 * # ttbar semi-leptonic (1 lepton + MET + 4 jets, constrain W masses)
 * name=ttbarFit \
 *   particles=lep:lep_pt:lep_eta:lep_phi:lep_mass:lepton,nu:met_pt:_:met_phi:0:met,j1:j1_pt:j1_eta:j1_phi:j1_mass:jet,j2:j2_pt:j2_eta:j2_phi:j2_mass:jet,j3:j3_pt:j3_eta:j3_phi:j3_mass:jet,j4:j4_pt:j4_eta:j4_phi:j4_mass:jet \
 *   constraints=0+1:80.4,2+3:80.4 \
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
   * Defines all output columns (chi2, converged flag, fitted momenta for each
   * particle) for the fit identified by @p fitName.
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
