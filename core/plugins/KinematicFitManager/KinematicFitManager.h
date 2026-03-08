#ifndef KINEMATICFITMANAGER_H_INCLUDED
#define KINEMATICFITMANAGER_H_INCLUDED

#include <KinematicFit.h>
#include <NamedObjectManager.h>
#include <api/IConfigurationProvider.h>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Configuration for one particle in the kinematic fit.
 *
 * Three modes are supported:
 *
 * **1. Scalar columns** (default, collectionIndex == -1)
 *   Each of ptCol/etaCol/phiCol/massCol names a scalar (Float_t) RDataFrame
 *   column. This is the standard mode for pre-selected signal leptons and jets.
 *
 * **2. Collection-indexed** (collectionIndex >= 0)
 *   ptCol/etaCol/phiCol/massCol name `ROOT::VecOps::RVec<Float_t>` collection
 *   columns (e.g. `Jet_pt`, `Jet_eta`). The integer @p collectionIndex selects
 *   which element to use.  If the collection is shorter than the requested
 *   index the value falls back to 0, so a "missing" jet simply decouples from
 *   the fit.  Specify the index as the 7th colon-separated field in the
 *   particle spec string, e.g.:
 *     @code j1:Jet_pt:Jet_eta:Jet_phi:Jet_mass:jet:0 @endcode
 *   This mode is useful when the jet collection is stored as a single RVec
 *   and you want to pick individual jets by position.
 *
 * **3. Recoil** (type == "recoil")
 *   ptCol/etaCol/phiCol/massCol name `RVec<Float_t>` collection columns for
 *   all jets in the event.  The 4-momenta of every jet in the collection are
 *   summed into a single effective particle (the hadronic recoil system).
 *   This particle enters the fit *unconstrained* (no mass constraint is
 *   applied to it) but participates in the χ² minimisation with a large
 *   effective uncertainty.  The intended use is to account for additional
 *   QCD radiation (ISR/FSR jets) whose multiplicity varies event by event:
 *   all extra jets are captured in one recoil 4-vector regardless of how
 *   many there are.  When the collection is empty (no extra jets), the recoil
 *   particle has zero 4-momentum and does not affect the fit.
 *   Config syntax (no index field required):
 *     @code isr:ExtraJet_pt:ExtraJet_eta:ExtraJet_phi:ExtraJet_mass:recoil @endcode
 *
 * **Special column values**
 *   - @p etaCol  == "" or "_"  (MET only): a constant η = 0 column is created
 *     automatically, with a very large σ_η so the MET direction is free in η.
 *   - @p massCol may be a numeric literal (e.g. "0", "0.106") instead of a
 *     column name. A constant column is created automatically.
 */
struct KinFitParticleConfig {
  std::string name;     ///< Label used in output column names
  std::string ptCol;    ///< Column name (scalar or RVec) for pT [GeV]
  std::string etaCol;   ///< Column name for eta; "" or "_" for MET
  std::string phiCol;   ///< Column name (scalar or RVec) for phi [rad]
  std::string massCol;  ///< Column name OR numeric literal for mass [GeV]
  std::string type;     ///< "lepton", "jet", "met", or "recoil"
  int collectionIndex = -1; ///< ≥0: select element from RVec; -1: scalar
};

/**
 * @brief A single kinematic constraint for the fit.
 *
 * Two types of constraint are supported:
 *
 * **MASS** – Invariant mass constraint on two or three particles.
 *   - Two-body (idx3 == -1): m(p_idx1, p_idx2) == targetValue
 *   - Three-body (idx3 >= 0): m(p_idx1, p_idx2, p_idx3) == targetValue
 *     Used for top-quark decays: t → b W → b l ν gives
 *     m(b, l, ν) = 173.3 GeV.
 *
 * **PT** – Transverse-momentum constraint on a single particle (idx1 only).
 *   Constrains pT(p_idx1) == targetValue.  Set targetValue = 0 to remove the
 *   MET contribution in events where no genuine missing energy is expected
 *   (e.g. fully hadronic W → jj decays).
 *
 * **Soft mass constraints via massSigma**
 *
 * By default mass constraints are hard (exact).  Setting @p massSigma > 0
 * converts a MASS constraint into a soft Gaussian-penalty constraint that
 * accounts for the resonance's intrinsic width.  The penalty term added to
 * the W = DVDᵀ matrix diagonal is (2·targetValue·massSigma)², so the fitter
 * allows the reconstructed mass to deviate from @p targetValue by roughly
 * @p massSigma GeV.
 *
 * Recommended values (PDG widths):
 *   - Z boson:   massSigma = 2.495  GeV
 *   - W boson:   massSigma = 2.085  GeV
 *   - top quark: massSigma = 1.4    GeV
 *   - Higgs:     massSigma = 0.004  GeV (~4 MeV)
 *
 * massSigma has no effect on PT constraints.
 *
 * Config syntax:
 *   @code
 *   # Two-body mass constraint (hard)
 *   constraints=0+1:91.2
 *   # Two-body mass constraint with Z width (soft)
 *   constraints=0+1:91.2:2.495
 *   # Three-body mass constraint with top width (soft)
 *   constraints=0+1+2:173.3:1.4
 *   # pT constraint (particle 3 must have pT = 0 GeV)
 *   constraints=pt:3:0.0
 *   @endcode
 */
struct KinFitConstraintConfig {
  enum class Type { MASS, PT };

  Type   type       = Type::MASS; ///< Constraint type
  int    idx1       = -1;  ///< First  particle (mass 2/3-body or pT-only)
  int    idx2       = -1;  ///< Second particle (mass constraints only)
  int    idx3       = -1;  ///< Third  particle (three-body mass only; -1 = two-body)
  double targetValue = 0.0; ///< Target invariant mass [GeV] or target pT [GeV]
  /// @brief Resonance width / mass uncertainty [GeV] for soft mass constraints.
  /// Set > 0 to soften the constraint (see struct documentation).
  /// Has no effect on PT constraints.
  double massSigma  = 0.0;
};

/**
 * @brief Complete configuration for one kinematic fit instance.
 */
struct KinFitConfig {
  std::vector<KinFitParticleConfig>   particles;   ///< Ordered list of particles
  std::vector<KinFitConstraintConfig> constraints; ///< Mass and pT constraints

  // ── per-type resolution parameters (with physics-motivated defaults) ──────
  double leptonPtResolution  = 0.02;   ///< Fractional lepton pT resolution
  double leptonEtaResolution = 0.001;  ///< Absolute lepton eta resolution
  double leptonPhiResolution = 0.001;  ///< Absolute lepton phi resolution [rad]

  double jetPtResolution     = 0.10;   ///< Fractional jet pT resolution
  double jetEtaResolution    = 0.05;   ///< Absolute jet eta resolution
  double jetPhiResolution    = 0.05;   ///< Absolute jet phi resolution [rad]

  double metPtResolution     = 0.20;   ///< Fractional MET pT resolution
  /// @brief MET eta resolution. Very large so η is effectively free.
  double metEtaResolution    = 100.0;
  double metPhiResolution    = 0.05;   ///< Absolute MET phi resolution [rad]

  /// @brief Fractional pT resolution for the hadronic recoil system.
  /// Set large (default 0.30 = 30 %) because the recoil is a composite of
  /// multiple soft jets and its momentum is poorly measured.
  double recoilPtResolution  = 0.30;
  /// @brief Absolute eta resolution for the hadronic recoil.
  double recoilEtaResolution = 0.10;
  /// @brief Absolute phi resolution for the hadronic recoil.
  double recoilPhiResolution = 0.10;

  int    maxIterations        = 50;    ///< Maximum linearisation iterations
  double convergenceTolerance = 1e-6;  ///< Convergence criterion on |Δχ²|

  /// @brief Whether to run this fit on the GPU.
  ///
  /// When @p true the fit is dispatched to the GPU via the CUDA implementation
  /// in KinematicFitGPU.cu.  The project must be compiled with
  /// @c -DUSE_CUDA=ON; if it is not, setting @p useGPU = true throws a
  /// std::runtime_error at applyFit() time with a clear diagnostic message.
  ///
  /// GPU execution processes events in a batch for maximum throughput.  Set
  /// this to @p true for computationally expensive fits (many particles,
  /// tight convergence) on large datasets.
  ///
  /// Config key: @c useGPU=true / @c useGPU=false  (default: false)
  bool useGPU = false;
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
 *     particles       – comma-separated list of particle specs (see below)
 *     constraints     – comma-separated constraints; three formats supported:
 *                       @code
 *                       # Two-body invariant mass (hard): idx1+idx2:targetMass
 *                       0+1:91.2
 *                       # Two-body invariant mass (soft): idx1+idx2:targetMass:massSigma
 *                       # massSigma = resonance width; allows mass to deviate
 *                       # by ~massSigma GeV (Gaussian penalty).
 *                       0+1:91.2:2.495     # Z with PDG width
 *                       # Three-body invariant mass: idx1+idx2+idx3:targetMass[:massSigma]
 *                       # Used for top decay: t → b l ν  (idx 0=b, 1=l, 2=ν)
 *                       0+1+2:173.3:1.4    # top with PDG width
 *                       # pT (size) constraint on one particle: pt:idx:targetPt
 *                       # Set targetPt=0 to suppress MET in hadronic events
 *                       pt:2:0.0
 *                       @endcode
 *                       where indices are zero-based into the particles list.
 *     maxIterations        – maximum number of linearisation iterations
 *     convergenceTolerance – convergence criterion on |Δχ²|
 *
 *   Optional (fall back to built-in defaults if absent):
 *     runVar               – name of a boolean RDataFrame column; when the
 *                           column value is false for an event the fit is
 *                           skipped and all output columns receive -1 (chi2,
 *                           fitted momenta) or false (converged).  Omit this
 *                           key to run the fit on every event (equivalent to
 *                           a constant-true column).
 *     leptonPtResolution, leptonEtaResolution, leptonPhiResolution
 *     jetPtResolution,    jetEtaResolution,    jetPhiResolution
 *     metPtResolution,    metEtaResolution,    metPhiResolution
 *     recoilPtResolution, recoilEtaResolution, recoilPhiResolution
 *
 * **Particle spec format**
 *
 *   Normal (scalar) particle – 6 colon-separated fields:
 *   @code label:ptCol:etaCol:phiCol:massCol:type @endcode
 *
 *   Collection-indexed particle – 7 fields, 7th is a zero-based integer index:
 *   @code label:ptCol:etaCol:phiCol:massCol:type:index @endcode
 *   ptCol/etaCol/phiCol/massCol are RVec<Float_t> columns.  Element @e index
 *   is extracted per event.  If the collection is shorter than @e index,
 *   the value falls back to 0 (the particle is effectively absent).
 *
 *   Recoil particle – 6 fields, type must be "recoil":
 *   @code label:ptCol:etaCol:phiCol:massCol:recoil @endcode
 *   ptCol/etaCol/phiCol/massCol are RVec<Float_t> collections containing
 *   all extra jets.  Their 4-momenta are summed per event into a single
 *   effective particle representing the hadronic recoil.  The number of
 *   jets in the collection may vary event by event; an empty collection
 *   gives a zero 4-vector that does not affect the fit.
 *
 * **Output columns** (per fit named @p N):
 *   - @p N_chi2             — chi-square of the fit (Float_t)
 *   - @p N_converged        — true if the fit converged (bool)
 *   - @p N_{label}_pt_fitted  — fitted pT  for each particle (Float_t)
 *   - @p N_{label}_eta_fitted — fitted eta for each particle (Float_t)
 *   - @p N_{label}_phi_fitted — fitted phi for each particle (Float_t)
 *
 * @par Example config lines
 * @code
 * # Z+H → ll bb  (2 leptons + 2 jets, scalar columns; only run when isZH is true)
 * name=zhFit runVar=isZH \
 *   particles=mu1:mu1_pt:mu1_eta:mu1_phi:mu1_mass:lepton,mu2:mu2_pt:mu2_eta:mu2_phi:mu2_mass:lepton,bjet1:bjet1_pt:bjet1_eta:bjet1_phi:bjet1_mass:jet,bjet2:bjet2_pt:bjet2_eta:bjet2_phi:bjet2_mass:jet \
 *   constraints=0+1:91.2,2+3:125.0 \
 *   leptonPtResolution=0.02 jetPtResolution=0.10 maxIterations=50 convergenceTolerance=1e-6
 *
 * # W → lν  (1 lepton + MET)
 * name=wlvFit \
 *   particles=lep:lep_pt:lep_eta:lep_phi:lep_mass:lepton,nu:met_pt:_:met_phi:0:met \
 *   constraints=0+1:80.4 \
 *   metPtResolution=0.20 maxIterations=50 convergenceTolerance=1e-6
 *
 * # ttbar semi-leptonic (lepton + MET + 2 signal jets + hadronic recoil from extra jets)
 * # ExtraJet_* are RVec<Float_t> columns holding all extra QCD jets in the event.
 * # Their multiplicity is variable; the recoil particle absorbs them all.
 * # Two W mass constraints plus a THREE-BODY top mass constraint on b+l+ν.
 * name=ttbarFit \
 *   particles=lep:lep_pt:lep_eta:lep_phi:lep_mass:lepton,nu:met_pt:_:met_phi:0:met,j1:j1_pt:j1_eta:j1_phi:j1_mass:jet,j2:j2_pt:j2_eta:j2_phi:j2_mass:jet,bjet:bjet_pt:bjet_eta:bjet_phi:bjet_mass:jet,isr:ExtraJet_pt:ExtraJet_eta:ExtraJet_phi:ExtraJet_mass:recoil \
 *   constraints=0+1:80.4,2+3:80.4,0+1+4:173.3 \
 *   recoilPtResolution=0.30 recoilEtaResolution=0.10 recoilPhiResolution=0.10 \
 *   maxIterations=50 convergenceTolerance=1e-6
 *
 * # Z → jj (hadronic, no real MET expected)
 * # Constrain dijet mass to Z and also pin MET pT to 0 (no genuine MET).
 * name=hadZFit \
 *   particles=j1:jet1_pt:jet1_eta:jet1_phi:jet1_mass:jet,j2:jet2_pt:jet2_eta:jet2_phi:jet2_mass:jet,met:met_pt:_:met_phi:0:met \
 *   constraints=0+1:91.2,pt:2:0.0 \
 *   jetPtResolution=0.10 metPtResolution=0.20 maxIterations=50 convergenceTolerance=1e-6
 *
 * # Z+H with jets from a collection (collection-indexed selection)
 * # Jet_* are RVec<Float_t> columns; :0 / :1 pick the leading/sub-leading jet.
 * name=zhCollFit \
 *   particles=mu1:Lep_pt:Lep_eta:Lep_phi:Lep_mass:lepton:0,mu2:Lep_pt:Lep_eta:Lep_phi:Lep_mass:lepton:1,bjet1:Jet_pt:Jet_eta:Jet_phi:Jet_mass:jet:0,bjet2:Jet_pt:Jet_eta:Jet_phi:Jet_mass:jet:1 \
 *   constraints=0+1:91.2,2+3:125.0 \
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
   * @brief Get the run variable (boolean RDataFrame column) for a named fit.
   *
   * Returns the column name stored for @p fitName.  If the fit was configured
   * without a @p runVar key the returned string is empty, which @c applyFit()
   * interprets as "always run".
   *
   * @param fitName Name of the fit as given in the config file.
   * @return The run-variable column name, or an empty string if none was set.
   */
  const std::string &getRunVar(const std::string &fitName) const;

  /**
   * @brief Get the names of all configured fits.
   * @return Vector of fit names.
   */
  std::vector<std::string> getAllFitNames() const;

  std::string type() const override { return "KinematicFitManager"; }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs the number of loaded fits.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports loaded fit names to the logger.
   */
  void reportMetadata() override;

private:
  /**
   * @brief Parse and register fits from the configuration provider.
   * @param configProvider Reference to the configuration provider.
   */
  void registerFits(const IConfigurationProvider &configProvider);

  /// Map from fit name to run-variable column name (empty = always run).
  std::unordered_map<std::string, std::string> kinfit_runVars_m;
};

#endif // KINEMATICFITMANAGER_H_INCLUDED
