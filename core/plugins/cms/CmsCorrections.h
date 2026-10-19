#ifndef CMS_CORRECTIONS_H_INCLUDED
#define CMS_CORRECTIONS_H_INCLUDED

//==============================================================================
// CmsCorrections — Generic CMS correction workflow setup
//
// Purpose:
//   Provides generic functions for setting up the CMS correction workflow
//   in an RDFAnalyzerCore analysis pipeline.  This includes:
//
//     1. Creating all standard CMS plugin instances
//        (JetEnergyScaleManager, ElectronEnergyScaleManager,
//         MuonRochesterManager, etc.)
//     2. Configuring the correction workflow with year-specific paths
//        from a CmsYearSettings struct.
//     3. Defining fallback identity columns when corrections are disabled.
//
//   The configuration is driven by a CmsCorrectionConfig struct that
//   specifies which corrections to apply and with what systematic
//   breakdown.  Typical usage is:
//
//   auto settings = registry.load();  // from CmsYearConfig
//   CmsCorrectionConfig corrCfg;
//   corrCfg.yearSettings = &settings;
//   corrCfg.applyJetCorrections = true;
//   corrCfg.jesMode = CmsCorrectionConfig::Full;
//   configureCmsCorrectionWorkflow(analyzer, corrCfg);
//
// Dependencies:
//   - TaggerWorkingPointManager, JetEnergyScaleManager,
//     ElectronEnergyScaleManager, MuonRochesterManager,
//     CorrectedObjectCollectionManagers, TriggerManager
//   - CorrectionManager, WeightManager
//==============================================================================

#include "CmsYearConfig.h"

#include <iostream>
#include <string>
#include <vector>

// Forward declarations
class Analyzer;

namespace cms {

namespace detail {

/// Convert the default MC compound to its DATA counterpart for data samples.
/// Explicit CmsCorrectionConfig compound overrides are used verbatim instead.
std::string jecCompoundForSample(const std::string &defaultCompound, bool isData);

/// Legacy JEC input layout, optionally including phi and a data run number.
std::vector<std::string> jecCompoundInputColumns(bool isData,
                                                 const std::string &rawPtColumn,
                                                 const std::string &rhoBranch,
                                                 const std::string &collectionPrefix = "Jet",
                                                 bool includePhi = false);

/// Map the payload's ordered input names to dataframe columns. Phi and run
/// are included only when requested by the actual compound schema.
std::vector<std::string> jecCompoundInputColumns(
    const std::vector<std::string> &schemaInputs, const std::string &rawPtColumn,
    const std::string &rhoBranch, const std::string &collectionPrefix = "Jet");

} // namespace detail

// ---------------------------------------------------------------------------
// CmsCorrectionConfig — controls which corrections to apply
// ---------------------------------------------------------------------------
struct CmsCorrectionConfig {
  /// Pointer to year settings (must outlive the config).
  const CmsYearSettings *yearSettings = nullptr;

  /// Jet tagger algorithm name, e.g. "PNet", "DeepFlav", "RobustParT".
  /// Used to select the correct b-tag/c-tag discriminator branches.
  /// Default is year-dependent: "PNet" for Run 2 and Run 3 up to 2023,
  /// "UParTAK4" for 2024+.
  std::string jetTagger = "PNet";

  /// Determine the default jet tagger for a given canonical year.
  /// Run 2 UL (2016–2018): "DeepFlav" — BTV v9 payloads only have deepJet_* corrections
  /// 2022–2023:            "PNet" — BTV v12 payloads have particleNet_* corrections
  /// 2024+:                "UParTAK4" — BTV v15 payloads use UParTAK4 naming
  static std::string defaultJetTagger(const std::string &canonicalYear) {
    const bool isRun2 = (canonicalYear == "2016_PreVFP" ||
                         canonicalYear == "2016_PostVFP" ||
                         canonicalYear == "2017" ||
                         canonicalYear == "2018");
    if (isRun2) return "DeepFlav";
    if (canonicalYear == "2024" || canonicalYear == "2025") return "UParTAK4";
    return "PNet";
  }

  /// JES systematic mode.
  ///   Total   — just "Total" (single combined uncertainty)
  ///   Reduced — regrouped set (~10 sources)
  ///   Full    — individual sources (~30 sources)
  enum JesMode { Total, Reduced, Full } jesMode = Reduced;

  // -- Correction enable flags (all default OFF — opt-in) --
  bool applyJetCorrections = false;
  bool applyFatJetCorrections = false;   ///< AK8 fat-jet corrections (JEC/JER)
  bool applyElectronCorrections = false;
  bool applyMuonCorrections = false;
  bool applyBtagCorrections = false;
  bool applyCtagCorrections = false;
  bool applyTauCorrections = false;      ///< Tau energy scale corrections

  // -- Systematic variation flags (used when SystematicManager is enabled) --
  /// Register JES source variations in addition to nominal jet corrections.
  bool applyJesSystematics = true;
  /// Register AK4/AK8 JER up/down variations; nominal smearing is unaffected.
  bool applyJerSystematics = true;
  /// Register electron and muon scale/resolution variations; nominal corrections are unaffected.
  bool applyLeptonScaleSmearSystematics = true;

  // -- Max JES workflow slots (for the JetEnergyScaleManager) --
  size_t maxJesSlots = 12;

  // -- Per-analysis correction name overrides (advanced) --
  std::string jesCompoundCorrectionOverride;  ///< Override JEC compound correction name
  std::string jesTotalCorrectionOverride;     ///< Override JES total correction name

  // -- Corrected collection names --
  std::string correctedJetCollectionName = "CorrectedJets";
  std::string correctedElectronCollectionName = "CorrectedElectrons";
  std::string correctedMuonCollectionName = "CorrectedMuons";

  // -- Optional alternate raw jet inputs --
  // When set, the JEC/JER workflow uses these columns instead of deriving
  // Jet_pt_raw and Jet_mass_raw from the NanoAOD jet collection.
  std::string jetRawPtColumnOverride;
  std::string jetRawMassColumnOverride;

  // -- Prefix for config keys pushed to the Analyzer --
  std::string configKeyPrefix = "cms";

  /// Prefix for correction-manager registration names.
  /// All correction names registered by CmsCorrections functions (JES, JER,
  /// fat-jet JES, etc.) are prefixed with this string.
  std::string correctionNamePrefix = "cms";

  // -- HEM issue handling for 2018 --
  bool applyHemIssueFilter = false;
  /// Fraction of 2018 luminosity affected by HEM issue (run >= 319077).
  /// Used to randomly select matching fraction of MC events.
  double hemIssueLumiFraction = 0.0;

  // -- MET Type-1 corrections --
  /// Apply Type-1 MET corrections using fully calibrated jet energies.
  /// When true and applyJetCorrections is true, propagates the change from
  /// NanoAOD jets to the final nominal JEC/JER-corrected jets via
  /// JetEnergyScaleManager::propagateMET().
  /// Produces PuppiMET_pt_corr_nominal / PuppiMET_phi_corr_nominal.
  bool applyMetType1 = false;

  // -- Tagger working-point thresholds --
  /// B-tag discriminator thresholds for each working point.
  /// When any field is non-zero, setupCmsFrameworkPlugins() automatically
  /// registers all non-zero WPs with the "btagManager" TaggerWorkingPointManager.
  /// WP names: L (loose), M (medium), T (tight), XT (extra-tight), XXT.
  BTagWorkingPoints btagWorkingPoints;

  /// C-tag (CvL × CvB) discriminator thresholds for each working point.
  /// When any field is non-zero, setupCmsFrameworkPlugins() automatically
  /// registers pair-based WPs with the "ctagManager" TaggerWorkingPointManager.
  /// WP names: L (loose), M (medium), T (tight).
  CTagWorkingPoints ctagWorkingPoints;

  /// Print a human-readable summary of this configuration to a stream.
  void print(std::ostream &os) const;
};

// ---------------------------------------------------------------------------
// CmsCorrectionAudit — structured audit record for correction stack status
// ---------------------------------------------------------------------------

/// A single audit entry recording why a correction component was not applied.
struct CmsCorrectionAuditEntry {
  std::string category;    ///< e.g. "pileup", "btag", "tau_tes", "jet_pu_id"
  std::string reason;      ///< e.g. "payload_not_found", "correction_name_missing", "year_unsupported"
  std::string detail;      ///< Human-readable explanation
  std::string remediation; ///< Suggested action (e.g. "override file path", "disable flag")
};

/// Collection of audit entries for the CMS correction stack.
struct CmsCorrectionAudit {
  std::vector<CmsCorrectionAuditEntry> entries;

  /// Add a single audit entry.
  void add(const std::string &category, const std::string &reason,
           const std::string &detail, const std::string &remediation = "");

  /// Print the full audit log to a stream (default: std::cout).
  void print(std::ostream &os = std::cout) const;

  /// Print a compact one-line summary.
  void printSummary(std::ostream &os = std::cout) const;

  /// Return true if no entries have been recorded.
  bool empty() const { return entries.empty(); }

  /// Return true if any entry matches the given category.
  bool hasCategory(const std::string &category) const;

  /// Return true if any entry matches the given reason.
  bool hasReason(const std::string &reason) const;
};

// ---------------------------------------------------------------------------
// Correction stack validation
// ---------------------------------------------------------------------------

/// Check each enabled correction flag against payload availability for the
/// configured year.  Returns an audit with entries for all enabled-but-unavailable
/// components, and prints a summary table.
///
/// @param corrCfg   The correction configuration.
/// @param weightCfg The weight configuration (nullptr = skip weight checks).
/// @return An audit struct with entries for every unavailable component.
struct CmsWeightConfig;
CmsCorrectionAudit validateCmsCorrectionStack(
    const CmsCorrectionConfig &corrCfg,
    const CmsWeightConfig *weightCfg = nullptr);

/// Create and register CMS correction-related framework plugin instances.
/// Must be called once before any other CMS correction functions.
/// Creates: CorrectionManager, WeightManager,
///   GenWeightManager, TaggerWorkingPointManager x2 (btag/ctag) [if enabled],
///   JetEnergyScaleManager [if enabled], ElectronEnergyScaleManager [if enabled],
///   MuonRochesterManager [if enabled], TauEnergyScaleManager,
///   CorrectedJetCollectionManager, CorrectedFatJetCollectionManager,
///   CorrectedElectronCollectionManager, CorrectedMuonCollectionManager,
///   CorrectedTauCollectionManager,
///   GoldenJsonManager, JetVetoMapManager.
///
/// When cfg flags like applyJetCorrections, applyMuonCorrections, etc. are
/// false, the corresponding plugin instances are NOT created.  This saves
/// memory and avoids wasted computation for analyses that don't use certain
/// object types.  Default (no-arg) creates all plugins for backward
/// compatibility.
///
/// Optional plugins not created here (analysis should create if needed):
///   TriggerManager, OnnxManager, KinematicFitManager, NDHistogramManager
///
/// @param an  The Analyzer instance.
/// @param cfg Configuration controlling which plugins to create.
void setupCmsFrameworkPlugins(Analyzer &an,
                               const CmsCorrectionConfig &cfg = CmsCorrectionConfig());

// ---------------------------------------------------------------------------
// Tagger working-point configuration helpers
// ---------------------------------------------------------------------------

/// Register b-tag working points from a BTagWorkingPoints struct.
/// Registers only non-zero thresholds with WP names L, M, T, XT, XXT.
/// Requires the "btagManager" plugin to have been created.
void configureBtagWorkingPoints(Analyzer &an, const BTagWorkingPoints &btWp);

/// Register c-tag working points from a CTagWorkingPoints struct.
/// Each WP is a pair {CvL, CvB}.  Registers only pairs with at least one
/// non-zero threshold.  WP names: L, M, T.
/// Requires the "ctagManager" plugin to have been created.
void configureCtagWorkingPoints(Analyzer &an, const CTagWorkingPoints &ctWp);

/// One-shot b-tag manager setup: object columns + tagger branch + input
/// collection + working-point thresholds.
void setupBtagTaggerManager(Analyzer &an, const CmsYearSettings &ys,
                             const BTagWorkingPoints &btWp);

/// One-shot c-tag manager setup: object columns + tagger branches
/// (CvL + CvB) + input collection + working-point thresholds.
void setupCtagTaggerManager(Analyzer &an, const CmsYearSettings &ys,
                             const CTagWorkingPoints &ctWp);

// ---------------------------------------------------------------------------
// Correction input columns
// ---------------------------------------------------------------------------

/// Define the small set of raw-input columns needed by the correction
/// framework (e.g. Electron_scEta from Electron_eta + Electron_deltaEtaSC,
/// Jet_genMatchedPt from Jet_genJetIdx + GenJet_pt).
/// When cfg flags are false, the corresponding input columns are skipped.
void defineCmsCorrectionInputs(Analyzer &an,
                                const CmsCorrectionConfig &cfg = CmsCorrectionConfig());

// ---------------------------------------------------------------------------
// Correction workflow configuration
// ---------------------------------------------------------------------------

/// Configure the JEC/JER, electron scale/smear, and muon scale/resolution
/// workflows using year-specific paths and correction names.
/// Pushes the appropriate config keys into the Analyzer's configuration
/// provider so that the energy-scale managers find their payloads.
///
/// For Run 2 UL (2016–2018), muon Rochester corrections are applied via
/// RoccoR kFactor files instead of correctionlib JSONs. This function
/// calls configureCmsMuonRoccoR() for those years automatically.
void configureCmsCorrectionWorkflow(Analyzer &an,
                                     const CmsCorrectionConfig &cfg);

// ---------------------------------------------------------------------------
// Run 2 muon Rochester kFactor (RoccoR) integration
// ---------------------------------------------------------------------------

/// Configure and apply Run 2 muon Rochester corrections using the RoccoR
/// kFactor text files. This is used when the year has no correctionlib JSON
/// for muon scale/resolution (Run 2 UL 2016–2018).
///
/// The kFactor file is resolved from the tauFile path pattern:
///   roccor/roccor.Run2.v5.tgz contains RoccoR20{16,17,18}{,a,b}UL.txt
///
/// For now, the function expects the kFactor file to be provided via the
/// CmsYearSettings. The RoccoR-based correction includes scale correction
/// for both data and MC, plus optional resolution smearing for MC.
///
/// @param an             The Analyzer instance.
/// @param cfg            Correction configuration (year settings, flags).
void configureCmsMuonRoccoR(Analyzer &an, const CmsCorrectionConfig &cfg);

// ---------------------------------------------------------------------------
// Correction fallback columns
// ---------------------------------------------------------------------------

/// When corrections are disabled (e.g. for data or reduced ntuples), define
/// pass-through columns so the rest of the pipeline works uniformly.
/// Creates identity aliases for:
///   Jet_pt_corr_nominal, CorrectedJets, PuppiMET_pt/phi_corr_nominal,
///   Jet_pt_jes_{total,source}_{up,down}, Jet_pt_jer_{up,down},
///   Jet_mass_jes_{total,source}_{up,down}, Jet_mass_jer_{up,down},
///   CorrectedElectrons, CorrectedMuons,
///   Electron_pt_scale_{up,down}, Electron_pt_smear_{up,down},
///   Muon_pt_scale_{up,down}, Muon_pt_smear_{up,down}
void defineCmsCorrectionFallbacks(Analyzer &an,
                                   const CmsCorrectionConfig &cfg);

// ---------------------------------------------------------------------------
// HEM issue handling (2018)
// ---------------------------------------------------------------------------

/// Apply the 2018 HEM issue filter (jet veto in damaged region).
/// For data: vetoes events with run >= 319077 where jets lie in the
///   HEM-affected region (-1.57 < phi < -0.87, -2.5 < eta < -1.3
///   and -3.0 < eta < -2.5).
/// For MC: randomly selects a fraction of events (cfg.hemIssueLumiFraction)
///   to apply the same veto using the event number.
///
/// The HEM-affected eta-phi regions are:
///   Region 1: -1.57 < phi < -0.87, -2.5 < eta < -1.3
///   Region 2: -1.57 < phi < -0.87, -3.0 < eta < -2.5
///
/// See: https://twiki.cern.ch/twiki/bin/viewauth/CMS/JME/JetVetoMap#HEM_issue_2018
void applyCmsHemIssueFilter(Analyzer &an, const CmsCorrectionConfig &cfg);

// ---------------------------------------------------------------------------
// JetVetoMap integration
// ---------------------------------------------------------------------------

/// Apply the JetVetoMap filter using the JetVetoMapManager plugin.
/// Loads JetVetoMap JSON files from the CmsYearSettings->jmeFile path
/// (the jetvetomaps.json.gz alongside the JME JSON).
/// The filter vetoes events with jets in flagged detector regions.
/// Applied to both data and MC.
///
/// Requires JetVetoMapManager plugin to have been created (done by
/// setupCmsFrameworkPlugins).  The jetVetoMapConfig key will be set
/// automatically from the JME file location if not already configured.
void applyCmsJetVetoMap(Analyzer &an, const CmsCorrectionConfig &cfg);

// ---------------------------------------------------------------------------
// MET XY-shift correction (Run 2 UL — 2016–2018)
// ---------------------------------------------------------------------------

/// Apply the MET XY-shift correction to correct the phi-dependent MET
/// modulation caused by beamspot position and detector effects.
///
/// The correction applies a per-event shift to MET_x and MET_y based on
/// the number of primary vertices (npv) and the data-taking era.
/// Parameters are hardcoded from the JME POG recommendation
/// (XYMETCorrection_withUL17andUL18andUL16.h).
///
/// For Run 3 (2022+), the XY-shift is negligible and no correction is
/// applied (PuppiMET is the recommendation).  The function is a no-op
/// for those years.
///
/// @param an                The Analyzer instance.
/// @param yearSettings      Year settings (used to determine the year).
/// @param inputMETPtColumn  Input MET pT column name (e.g. "PuppiMET_pt").
/// @param inputMETPhiColumn Input MET phi column name.
/// @param outputMETPtColumn Output corrected MET pT column name.
/// @param outputMETPhiColumn Output corrected MET phi column name.
/// @param isMC              Whether the sample is MC.
void applyCmsMetXYShift(Analyzer &an,
                         const CmsYearSettings &yearSettings,
                         const std::string &inputMETPtColumn,
                         const std::string &inputMETPhiColumn,
                         const std::string &outputMETPtColumn,
                         const std::string &outputMETPhiColumn,
                         bool isMC);

// ---------------------------------------------------------------------------
// MET event-cleaning filters
// ---------------------------------------------------------------------------

/// Apply the standard set of MET event-cleaning (noise) filters.
/// These reject events with detector noise, beam halo, dead cells, etc.
/// The filters are applied to both data and MC, though some flags are
/// data-only (the code handles this automatically).
///
/// The filters applied are:
///   Flag_HBHENoiseFilter, Flag_HBHENoiseIsoFilter,
///   Flag_CSCTightHaloFilter, Flag_goodVertices,
///   Flag_eeBadScFilter, Flag_EcalDeadCellTriggerPrimitiveFilter,
///   Flag_BadPFMuonFilter, Flag_BadChargedCandidateFilter,
///   Flag_ecalBadCalibFilter
///
/// Only filters that exist as columns in the NanoAOD are applied;
/// missing columns are silently skipped.
///
/// @param an  The Analyzer instance.
void applyCmsMetFilters(Analyzer &an);

// ---------------------------------------------------------------------------
// Jet ID filter
// ---------------------------------------------------------------------------

/// Apply the standard CMS jet ID (tight + tightLepVeto) based on the era.
/// Implements the recommendations from the JetID.txt reference:
///
/// NanoAODv9 (Run 2 UL, CHS jets):
///   Uses Jet_jetId flag: 6 = tight + tightLepVeto, 2 = tight only.
///
/// NanoAODv12 (Run 3 2022-2023, Puppi jets):
///   Bug workaround: Jet_jetId & (1<<1) for |eta|<=2.7,
///   plus eta-dependent extra cuts for forward region.
///
/// NanoAODv15 (Run 3 2024+, Puppi jets):
///   Jet_jetId removed. Uses pseudocode based on Jet_neHEF, Jet_neEmEF,
///   Jet_chMultiplicity, Jet_neMultiplicity, Jet_chHEF, Jet_chMultiplicity,
///   Jet_muEF, Jet_chEmEF.
///
/// Creates filter column "jet_id_pass" (bool) that is true for jets passing
/// both tight ID and tightLepVeto.
///
/// @param an             The Analyzer instance.
/// @param yearSettings   Year settings (determines era-specific logic).
/// @param applyTightLepVeto  Whether to apply lepton veto criteria.
void applyCmsJetId(Analyzer &an,
                    const CmsYearSettings &yearSettings,
                    bool applyTightLepVeto = true);

// ---------------------------------------------------------------------------
// GenWeights integration
// ---------------------------------------------------------------------------

/// Automatically configure and execute GenWeightManager.
/// Sets up LHE scale, PDF, and parton shower weight variations.
/// Should be called after the dataframe is set up with genWeight column.
void setupCmsGenWeights(Analyzer &an);

} // namespace cms

#endif // CMS_CORRECTIONS_H_INCLUDED
