#ifndef CMS_YEAR_CONFIG_H_INCLUDED
#define CMS_YEAR_CONFIG_H_INCLUDED

//==============================================================================
// CmsYearConfig — Generic CMS year registry and correction path resolution
//
// Purpose:
//   Provides a configurable year registry for CMS correction payloads.
//   Contains:
//     1. CmsYearSettings — a struct holding all year-specific correction
//        file paths, correction names, and branch names.
//     2. CmsYearRegistry — a class that loads defaults from a YAML file
//        or from hardcoded built-in defaults, normalizes year aliases,
//        resolves CVMFS paths with {version} token substitution, and
//        allows per-field overrides.
//
//   The design follows a layered override pattern:
//     1. Built-in C++ defaults (hardcoded for each year 2016–2024)
//     2. YAML registry file (overrides built-in defaults)
//     3. Programmatic overrides (overrides YAML file)
//     4. Per-run config keys (overrides programmatic overrides)
//
// Usage:
//   CmsYearRegistry registry;
//   registry.setYear("2022_preEE");
//   registry.setCorrectionsBase("/cvmfs/cms-griddata.cern.ch/cat/metadata");
//   auto settings = registry.load();
//   // settings.jmeFile, settings.pileupFile, etc. are resolved.
//
//   // Override individual settings:
//   registry.override("jetTagger", "PNet");
//   auto settings2 = registry.load();  // btagBranch auto-updates
//
// Dependencies:
//   - yaml-cpp (for YAML file loading)
//   - ROOT (for type aliases, not strictly required)
//==============================================================================

#include <yaml-cpp/yaml.h>

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace cms {

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------
class CmsYearRegistry;

// ---------------------------------------------------------------------------
// BTagWorkingPoints — b-tag discriminator thresholds for fixed-WP SFs
// ---------------------------------------------------------------------------
/// Holds caller-supplied b-tag thresholds (L/M/T/XT/XXT). Built-in
/// year/tagger defaults are not currently available.
struct BTagWorkingPoints {
  float loose = 0.0f;
  float medium = 0.0f;
  float tight = 0.0f;
  float xtight = 0.0f;
  float xxtight = 0.0f;
};

// ---------------------------------------------------------------------------
// CTagWorkingPoints — c-tag (CvL × CvB) discriminator thresholds
// ---------------------------------------------------------------------------
/// Holds caller-supplied c-tag thresholds for the CvsL and CvsB
/// discriminators. Built-in year/tagger defaults are not currently available.
struct CTagWorkingPoints {
  float cvlLoose = 0.0f, cvlMedium = 0.0f, cvlTight = 0.0f;
  float cvbLoose = 0.0f, cvbMedium = 0.0f, cvbTight = 0.0f;
};

// ---------------------------------------------------------------------------
// CmsYearSettings — complete set of correction paths/names for one era
// ---------------------------------------------------------------------------
struct CmsYearSettings {
  // -- Identification --
  std::string canonicalYear;       ///< Normalized year label, e.g. "2022_preEE"
  std::string electronYearToken;   ///< Token for electron SF lookups (e.g. "2022preVFP")

  // -- Branch names --
  std::string rhoBranch = "fixedGridRhoFastjetAll"; ///< Rho branch for JEC
  std::string btagBranch = "Jet_btagPNetB";         ///< b-tag discriminator branch
  std::string cvlBranch = "Jet_btagPNetCvL";        ///< c-tag CvL branch
  std::string cvbBranch = "Jet_btagPNetCvB";        ///< c-tag CvB branch

  // -- Pileup --
  std::string pileupFile;           ///< Path to pileup weights JSON
  std::string pileupCorrection;     ///< Correction name inside the PU JSON

  // -- Muon ID/ISO --
  std::string muonFile;             ///< Path to muon SF JSON
  std::string muonCorrection;       ///< Correction name for muon ID

  // -- Electron ID/Reco --
  std::string electronFile;         ///< Path to electron SF JSON
  std::string electronCorrection;   ///< Correction name for electron ID
  std::string electronWorkingPoint; ///< Working point for electron ID

  // -- BTV (b-tag and c-tag) --
  std::string btvBTagFile;          ///< Path to b-tag SF JSON
  std::string btvCTagFile;          ///< Path to c-tag SF JSON

  // -- JME (JEC/JER) --
  std::string jmeFile;              ///< Path to AK4 JME JSON (JEC + JER)
  std::string fatJetJmeFile;        ///< Path to AK8 fat-jet JME JSON (JEC + JER)
  std::string jecCompoundCorrection;   ///< Compound JEC correction name
  std::string jesTotalCorrection;      ///< Total JES correction name
  std::vector<std::string> jesSources; ///< Per-source JES names for full systematics
  std::string jerPtResolutionCorrection;  ///< JER PtResolution correction name
  std::string jerScaleFactorCorrection;   ///< JER ScaleFactor correction name
  std::vector<std::string> jerScaleFactorInputs; ///< Inputs for JER SF (e.g. Jet_eta)

  // -- Electron scale/smearing --
  std::string electronScaleSmearingFile;        ///< Path to electron scale+smear JSON
  std::string electronScaleCorrection;          ///< Scale correction name
  std::string electronSmearingCorrection;       ///< Smearing correction name
  bool electronScaleSmearingEtDependent = false; ///< Whether scale/smear uses Et

  // -- Muon scale/resolution --
  std::string muonScaleResolutionFile; ///< Path to muon scale+resolution JSON (Run 3)
  std::string muonRoccoRFile;          ///< Path to RoccoR kFactor text file (Run 2 UL only)

  // -- Trigger scale factors (optional) --
  std::string triggerSfRoot; ///< Root directory for trigger SF files

  // -- Tau ID/ISO/energy scale --
  std::string tauFile;                    ///< Path to tau SF JSON
  std::string tauEnergyScaleCorrection;   ///< Correction name for tau energy scale (e.g. "tau_energy_scale")
  std::string tauEnergyScaleSmearingCorrection; ///< Correction name for tau energy resolution smearing (if available)

  // -- Photon ID/ISO --
  std::string photonFile;     ///< Path to photon SF JSON (Run 3 only)

  // -- Pileup Jet ID efficiency SFs --
  std::string puIdFile;           ///< Path to PU ID SF JSON (default: fallback to jmeFile)
  std::string puIdCorrection;     ///< Correction name for PU ID SF (default: "pileupJetId_sf")

  // -- Golden JSON --
  std::string goldenJsonFile; ///< Path to golden JSON for certified lumis

  // -- Triggers (common CMS groups) --
  std::vector<std::string> doubleMuonTriggers;
  std::vector<std::string> doubleElectronTriggers;
  std::vector<std::string> singleMuonTriggers;
  std::vector<std::string> singleElectronTriggers;
  std::vector<std::string> metTriggers;
};

// ---------------------------------------------------------------------------
// CmsYearRegistry — loads and resolves year-specific correction paths
// ---------------------------------------------------------------------------
class CmsYearRegistry {
public:
  CmsYearRegistry() = default;
  ~CmsYearRegistry();  // Must be defined in .cc where YamlRegistryState is complete

  // -- Year selection --
  /// Set the canonical or aliased year name (e.g. "2022_preEE", "22pre").
  void setYear(const std::string &year);

  /// Get the normalized canonical year.
  std::string year() const { return canonicalYear_m; }

  // -- Path resolution base --
  /// Set the base path for CVMFS-style correction files.
  /// Default: "/cvmfs/cms-griddata.cern.ch/cat/metadata"
  void setCorrectionsBase(const std::string &base);

  /// Get the corrections base path.
  std::string correctionsBase() const { return correctionsBase_m; }

  /// Set a version override for {version} tokens in paths.
  /// If empty (default), "latest" is used.
  void setVersionOverride(const std::string &version);

  // -- Override API --
  /// Override a specific setting by key name.
  /// Keys correspond to CmsYearSettings field names.
  void override(const std::string &key, const std::string &value);

  /// Override a list setting (comma-separated).
  void overrideList(const std::string &key, const std::vector<std::string> &values);

  /// Clear all overrides.
  void clearOverrides();

  // -- Registry loading --
  /// Load a YAML registry file to augment/replace built-in defaults.
  /// The file should have a "years:" top-level key with year sub-entries.
  void loadRegistryFile(const std::string &yamlPath);

  // -- Build settings --
  /// Build and return the resolved CmsYearSettings for the current year,
  /// incorporating built-in defaults, YAML registry, and programmatic overrides.
  CmsYearSettings load() const;

  /// Build settings, also applying an inline config map (e.g. from Analyzer config).
  CmsYearSettings loadWithOverrides(
      const std::unordered_map<std::string, std::string> &configMap,
      const std::string &configPrefix = "") const;

  // -- Utilities --
  /// Normalize a year alias to its canonical form.
  static std::string normalizeYear(const std::string &year);

  /// Resolve a correction path: {version} substitution, CVMFS base prepending.
  std::string resolvePath(const std::string &rawPath) const;

  /// List all supported years.
  static std::vector<std::string> supportedYears();

  /// Get the default JES source list (reduced set) for a given year.
  static std::vector<std::string> defaultJesReducedSources(const std::string &canonicalYear);

  /// Get the full JES source list for a given year (~30 individual sources).
  static std::vector<std::string> defaultJesFullSources(const std::string &canonicalYear);

  /// Get the default JER per-(eta,pT)-bin uncertainty sources for a given year.
  static std::vector<std::string> defaultJerPerBinSources(const std::string &canonicalYear);

  /// Backward-compatible alias.
  static std::vector<std::string> defaultJesSources(const std::string &canonicalYear) {
    return defaultJesReducedSources(canonicalYear);
  }

  /// Get the default b-tag branch for a tagger algorithm.
  static std::string defaultBTagBranch(const std::string &jetTagger,
                                        const std::string &canonicalYear = "");

  /// Get the default c-tag CvL branch for a tagger algorithm.
  static std::string defaultCvLBranch(const std::string &jetTagger,
                                       const std::string &canonicalYear = "");

  /// Get the default c-tag CvB branch for a tagger algorithm.
  static std::string defaultCvBBranch(const std::string &jetTagger,
                                       const std::string &canonicalYear = "");

  /// Reserved for a future BTV POG working-point table. No implementation is
  /// currently linked; callers must provide thresholds explicitly.
  static BTagWorkingPoints defaultBTagWorkingPoints(
      const std::string &canonicalYear,
      const std::string &jetTagger);

  /// Reserved for a future BTV POG working-point table. No implementation is
  /// currently linked; callers must provide thresholds explicitly.
  static CTagWorkingPoints defaultCTagWorkingPoints(
      const std::string &canonicalYear,
      const std::string &jetTagger);

private:
  // -- Internal state --
  std::string canonicalYear_m;
  std::string correctionsBase_m = "/cvmfs/cms-griddata.cern.ch/cat/metadata";
  std::string versionOverride_m;

  // Overrides applied on top of defaults + YAML
  std::unordered_map<std::string, std::string> scalarOverrides_m;
  std::unordered_map<std::string, std::vector<std::string>> listOverrides_m;

  // Loaded YAML registry nodes (lazily cached)
  struct YamlRegistryState {
    YAML::Node years;
    std::string sourcePath;
  };
  std::unique_ptr<YamlRegistryState> yamlState_m;

  // -- Internal helpers --
  void ensureYearSet() const;
  std::string resolvePathInternal(const std::string &rawPath,
                                   const std::string &correctionsBase,
                                   const std::string &repoRootHint) const;
  void applyYearEntry(CmsYearSettings &settings,
                      const std::string &yamlKey,
                      const std::string &canonicalYear) const;
};

} // namespace cms

#endif // CMS_YEAR_CONFIG_H_INCLUDED
