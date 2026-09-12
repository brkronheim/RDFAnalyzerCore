#ifndef CMS_WEIGHTS_H_INCLUDED
#define CMS_WEIGHTS_H_INCLUDED

//==============================================================================
// CmsWeights — Generic CMS event weight pattern definitions
//
// Purpose:
//   Provides configurable helper functions for common CMS event-weight
//   patterns.  Each function defines the necessary correction columns on
//   the dataframe and registers them with the CorrectionManager and
//   WeightManager.  Analyses may call individual functions for fine-grained
//   control, or use setupCmsStandardWeights() for the full set.
//
//   All functions respect an isMC flag: for data, weight columns are set
//   to unity (1.0f) and no corrections are loaded.
//
// Weight patterns provided:
//   1. Pileup reweighting               (defineCmsPileupWeights)
//   2. Muon ID + isolation SF           (defineCmsMuonScaleFactors)
//   3. Electron ID + reconstruction SF  (defineCmsElectronScaleFactors)
//   4. b-tag scale factors              (defineCmsBTagScaleFactors)
//   5. c-tag scale factors              (defineCmsCTagScaleFactors)
//   6. Trigger scale factors            (defineCmsTriggerScaleFactors)
//   7. Jet Pileup ID efficiency SFs     (defineCmsJetPuIdScaleFactors)
//   8. Full nominal weight composition  (defineCmsNominalWeight)
//   9. One-shot all of the above        (setupCmsStandardWeights)
//
// Dependencies:
//   - CorrectionManager, WeightManager, TaggerWorkingPointManager
//   - CmsYearConfig (for correction paths)
//==============================================================================

#include "CmsCorrections.h"
#include "CmsYearConfig.h"

#include <ROOT/RVec.hxx>

#include <string>
#include <vector>

// Forward declarations
class Analyzer;
class TaggerWorkingPointManager;

namespace cms {

// ---------------------------------------------------------------------------
// BTagMethod — selects between fixed-WP and shape-based calibration
// ---------------------------------------------------------------------------
enum class BTagMethod {
  FixedWP,  ///< Standard working-point-based method (BTV Method 1a)
  Shape     ///< Continuous discriminant shape correction (iterativeFit)
};

// ---------------------------------------------------------------------------
// ChannelMask — controls which SF categories apply in a given analysis region
// ---------------------------------------------------------------------------
/// Allows per-region control of scale factor application for multi-channel
/// analyses (e.g., muon channel applies muon SFs but not electron SFs).
struct ChannelMask {
  /// All flags default OFF — the user must explicitly opt in per channel.
  bool applyMuonSFs = false;
  bool applyElectronSFs = false;
  bool applyTauSFs = false;
  bool applyPhotonSFs = false;
  bool applyBTagSFs = false;
  bool applyCTagSFs = false;
  bool applyBoostedTagSFs = false;
  bool applyTriggerSFs = false;
  bool applyPileupSFs = false;
  bool applyL1PrefireSFs = false;
  bool applyJetPuIdSFs = false;
  bool applyFatJetSFs = false;
};

// ---------------------------------------------------------------------------
// CmsWeightConfig — controls weight definitions
// ---------------------------------------------------------------------------
struct CmsWeightConfig {
  /// Year settings (correction file paths and names).
  const CmsYearSettings *yearSettings = nullptr;

  /// Whether this is a MC sample (data gets unity weights).
  bool isMC = true;

  /// B-tag / c-tag calibration method (fixed-WP vs shape).
  BTagMethod btagMethod = BTagMethod::FixedWP;
  BTagMethod ctagMethod = BTagMethod::FixedWP;

  /// PU ID working point: "L" (loose), "M" (medium), or "T" (tight).
  /// Default is "M" (medium), which is the most commonly used for Run 2 UL.
  std::string puIdWorkingPoint = "M";

  /// Optional dedicated payload for continuous b-tag corrections.  When empty,
  /// the era b-tag payload is used.
  std::string btagShapeFile;

  /// Override shape correction names (empty = auto-detect from tagger base).
  /// Auto-detected as: <taggerBase> + "_shape" for b-tag, "_cshape" for c-tag.
  /// Only used when btagMethod/ctagMethod == Shape.
  std::string btagShapeCorrection;  ///< e.g., "particleNet_shape" for Run 3
  std::string btagShapeLightCorrection;  ///< e.g., "particleNet_shape" (combined shape)
  std::string ctagShapeCorrection;  ///< e.g., "particleNet_cshape" if available

  /// Jet tagger name (for b-tag/c-tag correction JSON lookup).
  /// Default is year-dependent: see CmsCorrectionConfig::defaultJetTagger().
  std::string jetTagger = "PNet";

  /// B-tag working point efficiency file and correction name.
  std::string btagEfficiencyFile;
  std::string btagEfficiencyCorrection;

  /// C-tag working point efficiency file and correction name.
  std::string ctagEfficiencyFile;
  std::string ctagEfficiencyCorrection;

  /// Root directory used by custom muon isolation payload lookup.
  std::string triggerSfRoot;
  /// Optional explicit TightMiniIso SF payload override.
  std::string muonIsoSfFile;
  /// Optional correction name from the standard muon_Z.json payload.
  /// Defaults to NUM_TightPFIso_DEN_MediumID when no custom iso file is used.
  std::string muonIsoCorrection;
  /// Optional Run-3 dimuon trigger-leg payloads. When both are configured,
  /// the lead muon uses Mu17 and the sublead muon uses Mu8, as in VHccPoCo.
  std::string dimuonTriggerMu17File;
  std::string dimuonTriggerMu8File;
  std::string dimuonTriggerMu17Correction =
      "NUM_Mu17leg_DEN_MediumId_and_LooseMiniIso";
  std::string dimuonTriggerMu8Correction =
      "NUM_Mu8leg_DEN_MediumId_and_LooseMiniIso";
  /// Path-specific trigger SF payload and correction for the selected HLT.
  /// The correction is evaluated for each selected muon and multiplied.
  std::string triggerSfFile;
  std::string triggerSfCorrection;
  /// Retained for configuration compatibility; path-specific trigger settings
  /// control whether trigger scale factors are evaluated.
  bool usePogTriggerSFs = true;

  /// B-tag and c-tag source names for systematic variations.
  std::vector<std::string> btagSourceNames;
  std::vector<std::string> ctagLightSourceNames;
  std::vector<std::string> ctagWcSourceNames;
  std::vector<std::string> ctagTnpSourceNames;

  /// Whether to apply b-tag / c-tag fixed-WP weights.
  bool applyBTagWeights = false;
  bool applyCTagWeights = false;

  // -- Individual weight component enable flags --
  // These allow analyses to skip weight computations for object types they
  // don't use, saving memory and CPU.  All default OFF — you must explicitly
  // opt in to each weight component that your analysis needs.
  bool applyPileupWeights = false;
  bool applyL1PrefireWeights = false;
  bool applyMuonScaleFactors = false;
  bool applyElectronScaleFactors = false;
  bool applyBTagScaleFactors = false;      ///< b-tag SF weight component
  bool applyCTagScaleFactors = false;      ///< c-tag SF weight component
  bool applyTriggerScaleFactors = false;
  bool applyTauScaleFactors = false;
  bool applyPhotonScaleFactors = false;
  bool applyJetPuIdScaleFactors = false;
  bool applyPileupMinbiasXsec = false;  ///< Pileup min-bias xsec uncertainty

  /// Channel-specific SF masks for multi-channel analyses.
  /// When non-empty, creates channel-specific nominal weight columns
  /// (e.g., "weight_nominal_muon", "weight_nominal_electron") that only
  /// include the scale factors enabled in each channel's mask.
  /// When empty (default), creates a single "weight_nominal" with all SFs.
  /// Example:
  ///   cfg.channels["muon"] = {.applyElectronSFs=false, .applyTauSFs=false, ...};
  ///   cfg.channels["electron"] = {.applyMuonSFs=false, .applyTauSFs=false, ...};
  std::unordered_map<std::string, ChannelMask> channels;

  // -- Tagger working points (must be set for b-tag / c-tag SFs) --
  /// B-tag discriminator working-point thresholds.
  /// These are set on the TaggerWorkingPointManager("btagManager") when
  /// setupCmsStandardWeights() runs.  All zero (default) means the caller
  /// has configured them externally.
  /// When all fields are zero, setupCmsStandardWeights() auto-populates
  /// from CmsYearRegistry::defaultBTagWorkingPoints().
  BTagWorkingPoints btagWorkingPoints;

  /// C-tag (CvL × CvB) working-point thresholds.
  /// When all fields are zero, setupCmsStandardWeights() auto-populates
  /// from CmsYearRegistry::defaultCTagWorkingPoints().
  CTagWorkingPoints ctagWorkingPoints;

  /// Prefix for correction-manager registration names.
  /// All correction names registered by CmsWeights functions are prefixed
  /// with this string (e.g. "cms_pileup_weight", "cms_muon_id_sf").
  /// Analyses that need isolated correction sets can change this to their
  /// own prefix (e.g. "vhbb").
  std::string correctionNamePrefix = "cms";

  /// Print a human-readable summary of this configuration to a stream.
  void print(std::ostream &os) const;
};

// ---------------------------------------------------------------------------
// Default source names
// ---------------------------------------------------------------------------

/// Default b-tag systematic source names.
inline std::vector<std::string> defaultBTagSourceNames() {
  return {"hf", "hfstats1", "hfstats2", "lf", "lfstats1", "lfstats2",
          "cferr1", "cferr2", "jes"};
}

/// Default c-tag light source names.
inline std::vector<std::string> defaultCTagLightSourceNames() {
  return {"light_correlated", "light_uncorrelated"};
}

/// Default c-tag W+c source names.
inline std::vector<std::string> defaultCTagWcSourceNames() {
  return {"wc_stat", "wc_syst"};
}

/// Default c-tag TnP source names.
inline std::vector<std::string> defaultCTagTnpSourceNames() {
  return {"bkg", "bdecay", "colorreconnection", "hdamp", "jes", "jer",
          "mescales", "pdf", "pileup", "psscale", "statistic", "topmass",
          "tune"};
}

// ---------------------------------------------------------------------------
// Individual weight pattern functions
// ---------------------------------------------------------------------------

/// Define pileup reweighting columns (nominal + up/down).
void defineCmsPileupWeights(Analyzer &an, const CmsWeightConfig &cfg);

/// Define muon ID and isolation scale factors (nominal + up/down).
void defineCmsMuonScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define electron ID and reconstruction scale factors (nominal + up/down,
/// per-pT bin for reco).
void defineCmsElectronScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define b-tag fixed-WP scale factors (nominal + per-source variations).
/// Requires TaggerWorkingPointManager ("btagManager") to be configured.
void defineCmsBTagScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define c-tag fixed-WP scale factors (nominal + per-source variations).
/// Requires TaggerWorkingPointManager ("ctagManager") to be configured.
void defineCmsCTagScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define path-specific trigger scale factors from cfg.triggerSfFile and
/// cfg.triggerSfCorrection. Missing settings produce unity columns and are
/// reported by the correction audit.
/// Result columns: trig_sf_nominal, trig_sf_up, trig_sf_down.
void defineCmsTriggerScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define tau ID, anti-electron, anti-muon, and energy scale scale factors.
/// Uses the tau.json.gz correction payloads from the TAU POG.
/// Applies DeepTau2018v2p5VSjet (tau ID), VSe (anti-e), VSmu (anti-muon),
/// and tau_energy_scale corrections via CorrectionManager.
/// Only applicable to MC.
void defineCmsTauScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define jet pileup ID (PU ID) efficiency scale factors.
/// Reads the Jet_puId_i column to determine which jets pass the selected
/// working point, then applies SF(abseta, pt, wp, systematic) from the
/// correction JSON. Three working points (L/M/T) are supported.
/// For Run 2 UL (2016–2018), the PU ID SFs are bundled in the JME JSON file.
/// For Run 3, PU ID SFs are not yet available (weights set to unity).
/// Only applicable to MC.
void defineCmsJetPuIdScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define pileup min-bias cross section uncertainty weights.
/// Evaluates additional pileup weight variations corresponding to a ±X% shift
/// in the min-bias cross section. These are required by the CMS recommendation
/// on top of the nominal pileup weights.
/// Only applicable to MC.
void defineCmsPileupMinbiasXsecWeights(Analyzer &an, const CmsWeightConfig &cfg);

/// Define photon ID, CSEV, and pixel veto scale factors.
/// Uses the photon.json.gz correction payloads from the EGM POG
/// (Run 3 only). For Run 2, photon corrections are not available
/// as correctionlib JSONs and will be set to unity.
/// Only applicable to MC.
void defineCmsPhotonScaleFactors(Analyzer &an, const CmsWeightConfig &cfg);

/// Define per-vector SF product columns.
/// For each column matching "<prefix>_sf_<variation>_vec", creates a scalar
/// column "<prefix>_weight_<variation>" as the product over objects.
/// Needed because WeightManager expects scalar weight columns.
void defineCmsVecProductColumns(Analyzer &an, const CmsWeightConfig &cfg);

/// Define L1 prefiring weights.
/// Reads the L1PreFiringWeight_* branches available in NanoAOD v9+.
/// Defines columns: l1_prefire_weight_nominal, l1_prefire_weight_up,
///   l1_prefire_weight_down, and optionally the split ECAL/Muon components.
///
/// For eras without L1 prefiring (Run 3, 2018), weights default to 1.0.
/// Only applicable to MC.
void defineCmsL1PrefireWeights(Analyzer &an, const CmsWeightConfig &cfg);

/// Define the nominal event weight column as the product of all registered
/// scale factors and normalizations.
void defineCmsNominalWeight(Analyzer &an, const CmsWeightConfig &cfg);

// ---------------------------------------------------------------------------
// One-shot setup
// ---------------------------------------------------------------------------

/// Configure all standard CMS weights in one call.
/// Calls defineCmsPileupWeights, defineCmsMuonScaleFactors,
/// defineCmsElectronScaleFactors, defineCmsBTagScaleFactors,
/// defineCmsCTagScaleFactors, defineCmsTriggerScaleFactors,
/// defineCmsVecProductColumns, and defineCmsNominalWeight.
/// Also configures TaggerWorkingPointManager branches from yearSettings.
void setupCmsStandardWeights(Analyzer &an, CmsWeightConfig cfg);

// ---------------------------------------------------------------------------
// Helper utilities (visible to ROOT JIT)
// ---------------------------------------------------------------------------

/// Merge b-tag scale factors for heavy (flavour 4/5) and light (rest) jets.
ROOT::VecOps::RVec<float> mergeBTagScaleFactors(
  const ROOT::VecOps::RVec<int> &flavour,
  const ROOT::VecOps::RVec<float> &heavy,
  const ROOT::VecOps::RVec<float> &light);

/// Merge c-tag scale factors: flavour 4 -> WC SF, flavour 5 -> TnP SF,
/// rest -> light SF.
ROOT::VecOps::RVec<float> mergeCTagScaleFactors(
  const ROOT::VecOps::RVec<int> &flavour,
  const ROOT::VecOps::RVec<float> &light,
  const ROOT::VecOps::RVec<float> &wc,
  const ROOT::VecOps::RVec<float> &tnp);

/// Select electron reconstruction SF per pT bin (<20, 20-75, >75 GeV).
std::vector<float> selectElectronRecoScaleFactors(
    const std::vector<float> &pt,
    const std::vector<float> &below20,
    const std::vector<float> &between20And75,
    const std::vector<float> &above75);

/// Compute the product of a vector of weights with safeguards.
float productOrOne(const std::vector<float> &weights);
float productOrOneRVec(const ROOT::VecOps::RVec<float> &weights);

/// Safe ratio with zero/NaN protection.
float safeWeightRatio(float numerator, float denominator);

/// Evaluate a 3-argument correction (absEta, pt, variation) from correctionlib.

/// Get a short trigger tag for a canonical year (e.g. "2022_preEE" -> "22").
std::string triggerTagForYear(const std::string &canonicalYear);

/// Resolve c-tag correction name per year:
/// Run 3 uses taggerBase + "_light"/"_wc"/"_tnp" naming.
/// Run 2 uses "deepJet_<suffix>" (legacy format).
std::string ctagCorrectionName(const std::string &taggerBase,
                                const std::string &canonicalYear,
                                const std::string &suffix);
float evalCorr3(const std::string &filePath, const std::string &name,
                float absEta, float pt, const std::string &variation);

/// Map canonical year to the trigger-SF year tag string (muon-era format).
std::string triggerTagForYear(const std::string &canonicalYear);

} // namespace cms

#endif // CMS_WEIGHTS_H_INCLUDED
