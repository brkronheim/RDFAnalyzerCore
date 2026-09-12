//==============================================================================
// CmsYearConfig — Generic CMS year registry and correction path resolution
//
// Provides default correction payload paths for all CMS data-taking years
// (2016-2024) and resolves them against a configurable base path.
//
// The defaults are extracted from the VHbb STXS analysis year registry and
// generalized.  Analyses may override any path or name via the override API
// or by providing a YAML registry file.
//==============================================================================

#include "CmsYearConfig.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

// yaml-cpp is used only when a YAML registry file is loaded.
// For built-in defaults, no YAML dependency is required.
#include <yaml-cpp/yaml.h>

namespace cms {

// ===========================================================================
// Internal helpers
// ===========================================================================
namespace {

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

std::string trimCopy(const std::string &value) {
  const auto first = value.find_first_not_of(" \t\n\r");
  if (first == std::string::npos) return "";
  const auto last = value.find_last_not_of(" \t\n\r");
  return value.substr(first, last - first + 1);
}

std::string joinPath(const std::string &base, const std::string &suffix) {
  if (base.empty()) return suffix;
  if (base.back() == '/') return base + suffix;
  return base + "/" + suffix;
}

std::vector<std::string> withHltPrefix(const std::vector<std::string> &triggers) {
  std::vector<std::string> out;
  out.reserve(triggers.size());
  for (const auto &t : triggers) {
    if (t.rfind("HLT_", 0) != 0) {
      out.push_back("HLT_" + t);
    } else {
      out.push_back(t);
    }
  }
  return out;
}

// Check if a file exists (for validation).
bool pathExists(const std::string &path) {
  if (path.empty()) return false;
  // Only check local files; root:// and /eos/ paths are assumed remote.
  if (path.rfind("root://", 0) == 0 || path.rfind("/eos/", 0) == 0) return true;
  return std::filesystem::exists(path);
}

// ---------------------------------------------------------------------------
// Default correction paths per year
// ---------------------------------------------------------------------------

struct BuiltInYearEntry {
  std::string canonicalYear;
  std::string electronYearToken;
  std::string rhoBranch;
  std::string muonCorrection;
  std::string electronCorrection;
  std::string electronWorkingPoint;
  std::string pileupFile;
  std::string pileupCorrection;
  std::string muonFile;
  std::string electronFile;
  std::string btvBTagFile;
  std::string btvCTagFile;
  std::string jmeFile;
  std::string jecCompoundCorrection;
  std::string jesTotalCorrection;
  std::string jerPtResolutionCorrection;
  std::string jerScaleFactorCorrection;
  std::vector<std::string> jerScaleFactorInputs;
  std::string electronScaleSmearingFile;
  std::string electronScaleCorrection;
  std::string electronSmearingCorrection;
  bool electronScaleSmearingEtDependent;
  std::string muonScaleResolutionFile;
  std::string goldenJsonFile;
  std::vector<std::string> doubleMuonTriggers;
  std::vector<std::string> doubleElectronTriggers;
  std::vector<std::string> singleMuonTriggers;
  std::vector<std::string> singleElectronTriggers;
  std::vector<std::string> metTriggers;
};

// clang-format off
const BuiltInYearEntry kBuiltInDefaults[] = {
  // ==========================================================================
  // 2016 Pre-VFP (UltraLegacy)
  // ==========================================================================
  {
    "2016_PreVFP",
    "2016preVFP",
    "fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "UL-Electron-ID-SF",
    "wp80iso",
    "LUM/Run2-2016preVFP-UL-NanoAODv9/latest/puWeights.json.gz",
    "Collisions16_UltraLegacy_goldenJSON",
    "MUO/Run2-2016preVFP-UL-NanoAODv9/latest/muon_Z.json.gz",
    "EGM/Run2-2016preVFP-UL-NanoAODv15/latest/electron.json.gz",
    "BTV/Run2-2016preVFP-UL-NanoAODv9/latest/btagging.json.gz",
    "BTV/Run2-2016preVFP-UL-NanoAODv9/latest/ctagging.json.gz",
    "JME/Run2-2016preVFP-UL-NanoAODv9/latest/jet_jerc.json.gz",
    "Summer19UL16APV_V7_MC_L1L2L3Res_AK4PFchs",
    "Summer19UL16APV_V7_MC_Total_AK4PFchs",
    "Summer20UL16APV_JRV5_MC_PtResolution_AK4PFchs",
    "Summer20UL16APV_JRV5_MC_ScaleFactor_AK4PFchs",
    {"Jet_eta"},
    "",   // electronScaleSmearingFile (not available for Run 2)
    "",   // electronScaleCorrection
    "",   // electronSmearingCorrection
    false,
    "",   // muonScaleResolutionFile (not available for Run 2)
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt",
    // triggers
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL", "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
     "Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL", "Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL_DZ"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ"},
    {"IsoMu24", "IsoTkMu24"},
    {"Ele27_WPTight_Gsf"},
    {"PFMET110_PFMHT110_IDTight", "PFMET120_PFMHT120_IDTight",
     "PFMET170_NoiseCleaned", "PFMET170_BeamHaloCleaned", "PFMET170_HBHECleaned"}
  },
  // ==========================================================================
  // 2016 Post-VFP (UltraLegacy)
  // ==========================================================================
  {
    "2016_PostVFP",
    "2016postVFP",
    "fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "UL-Electron-ID-SF",
    "wp80iso",
    "LUM/Run2-2016postVFP-UL-NanoAODv9/latest/puWeights.json.gz",
    "Collisions16_UltraLegacy_goldenJSON",
    "MUO/Run2-2016postVFP-UL-NanoAODv9/latest/muon_Z.json.gz",
    "EGM/Run2-2016postVFP-UL-NanoAODv15/latest/electron.json.gz",
    "BTV/Run2-2016postVFP-UL-NanoAODv9/latest/btagging.json.gz",
    "BTV/Run2-2016postVFP-UL-NanoAODv9/latest/ctagging.json.gz",
    "JME/Run2-2016postVFP-UL-NanoAODv9/latest/jet_jerc.json.gz",
    "Summer19UL16_V7_MC_L1L2L3Res_AK4PFchs",
    "Summer19UL16_V7_MC_Total_AK4PFchs",
    "Summer20UL16_JRV5_MC_PtResolution_AK4PFchs",
    "Summer20UL16_JRV5_MC_ScaleFactor_AK4PFchs",
    {"Jet_eta"},
    "", "", "", false, "",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL", "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
     "Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL", "Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL_DZ"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ"},
    {"IsoMu24", "IsoTkMu24"},
    {"Ele27_WPTight_Gsf", "Ele27_eta2p1_WPTight_Gsf"},
    {"PFMET110_PFMHT110_IDTight", "PFMET120_PFMHT120_IDTight",
     "PFMET170_NoiseCleaned", "PFMET170_BeamHaloCleaned", "PFMET170_HBHECleaned"}
  },
  // ==========================================================================
  // 2017 (UltraLegacy)
  // ==========================================================================
  {
    "2017",
    "2017",
    "fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "UL-Electron-ID-SF",
    "wp80iso",
    "LUM/Run2-2017-UL-NanoAODv9/latest/puWeights.json.gz",
    "Collisions17_UltraLegacy_goldenJSON",
    "MUO/Run2-2017-UL-NanoAODv9/latest/muon_Z.json.gz",
    "EGM/Run2-2017-UL-NanoAODv15/latest/electron.json.gz",
    "BTV/Run2-2017-UL-NanoAODv9/latest/btagging.json.gz",
    "BTV/Run2-2017-UL-NanoAODv9/latest/ctagging.json.gz",
    "JME/Run2-2017-UL-NanoAODv9/latest/jet_jerc.json.gz",
    "Summer19UL17_V5_MC_L1L2L3Res_AK4PFchs",
    "Summer19UL17_V5_MC_Total_AK4PFchs",
    "Summer19UL17_JRV4_MC_PtResolution_AK4PFchs",
    "Summer19UL17_JRV4_MC_ScaleFactor_AK4PFchs",
    {"Jet_eta"},
    "", "", "", false, "",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL", "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
     "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8", "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu27"},
    {"Ele32_WPTight_Gsf_L1DoubleEG", "Ele28_eta2p1_WPTight_Gsf_HT150"},
    {"PFMET120_PFMHT120_IDTight", "PFMET120_PFMHT120_IDTight_PFHT60"}
  },
  // ==========================================================================
  // 2018 (UltraLegacy)
  // ==========================================================================
  {
    "2018",
    "2018",
    "fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "UL-Electron-ID-SF",
    "wp80iso",
    "LUM/Run2-2018-UL-NanoAODv9/latest/puWeights.json.gz",
    "Collisions18_UltraLegacy_goldenJSON",
    "MUO/Run2-2018-UL-NanoAODv9/latest/muon_Z.json.gz",
    "EGM/Run2-2018-UL-NanoAODv15/latest/electron.json.gz",
    "BTV/Run2-2018-UL-NanoAODv9/latest/btagging.json.gz",
    "BTV/Run2-2018-UL-NanoAODv9/latest/ctagging.json.gz",
    "JME/Run2-2018-UL-NanoAODv9/latest/jet_jerc.json.gz",
    "Summer19UL18_V5_MC_L1L2L3Res_AK4PFchs",
    "Summer19UL18_V5_MC_Total_AK4PFchs",
    "Summer19UL18_JRV3_MC_PtResolution_AK4PFchs",
    "Summer19UL18_JRV3_MC_ScaleFactor_AK4PFchs",
    {"Jet_eta"},
    "", "", "", false, "",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu24"},
    {"Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"},
    {"PFMET120_PFMHT120_IDTight"}
  },
  // ==========================================================================
  // 2022 Pre-EE (Run 3)
  // ==========================================================================
  {
    "2022_preEE",
    "2022_preEE",
    "Rho_fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "Electron-ID-SF",
    "wp80iso",
    "LUM/Run3-22CDSep23-Summer22-NanoAODv12/latest/puWeights.json.gz",
    "Collisions2022_355100_357900_eraBCD_GoldenJson",
    "MUO/Run3-22CDSep23-Summer22-NanoAODv12/latest/muon_Z.json.gz",
    "EGM/Run3-22CDSep23-Summer22-NanoAODv12/latest/electron.json.gz",
    "BTV/Run3-22CDSep23-Summer22-NanoAODv12/latest/btagging.json.gz",
    "BTV/Run3-22CDSep23-Summer22-NanoAODv12/latest/ctagging.json.gz",
    "JME/Run3-22CDSep23-Summer22-NanoAODv12/latest/jet_jerc.json.gz",
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    "Summer22_22Sep2023_V3_MC_Regrouped_Total_AK4PFPuppi",
    "Summer22_22Sep2023_JRV2_MC_PtResolution_AK4PFPuppi",
    "Summer22_22Sep2023_JRV2_MC_ScaleFactor_AK4PFPuppi",
    {"Jet_eta", "Jet_pt_corr_jec"},
    "EGM/Run3-22CDSep23-Summer22-NanoAODv12/latest/electronSS_EtDependent.json.gz",
    "EGMScale",
    "EGMSmearAndSyst",
    true,
    "MUO/Run3-22CDSep23-Summer22-NanoAODv12/latest/muon_scalesmearing.json.gz",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu24"},
    {"Ele30_WPTight_Gsf"},
    {"PFMET120_PFMHT120_IDTight"}
  },
  // ==========================================================================
  // 2022 Post-EE (Run 3)
  // ==========================================================================
  {
    "2022_postEE",
    "2022_postEE",
    "Rho_fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "Electron-ID-SF",
    "wp80iso",
    "LUM/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/puWeights.json.gz",
    "Collisions2022_359022_362760_eraEFG_GoldenJson",
    "MUO/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/muon_Z.json.gz",
    "EGM/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/electron.json.gz",
    "BTV/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/btagging.json.gz",
    "BTV/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/ctagging.json.gz",
    "JME/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/jet_jerc.json.gz",
    "Summer22EE_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    "Summer22EE_22Sep2023_V3_MC_Regrouped_Total_AK4PFPuppi",
    "Summer22EE_22Sep2023_JRV2_MC_PtResolution_AK4PFPuppi",
    "Summer22EE_22Sep2023_JRV2_MC_ScaleFactor_AK4PFPuppi",
    {"Jet_eta", "Jet_pt_corr_jec"},
    "EGM/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/electronSS_EtDependent.json.gz",
    "EGMScale",
    "EGMSmearAndSyst",
    true,
    "MUO/Run3-22EFGSep23-Summer22EE-NanoAODv12/latest/muon_scalesmearing.json.gz",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu24"},
    {"Ele30_WPTight_Gsf"},
    {"PFMET120_PFMHT120_IDTight"}
  },
  // ==========================================================================
  // 2023 Pre-BPix (Run 3)
  // ==========================================================================
  {
    "2023_preBPix",
    "2023_preBPix",
    "Rho_fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "Electron-ID-SF",
    "wp80iso",
    "LUM/Run3-23CSep23-Summer23-NanoAODv12/latest/puWeights.json.gz",
    "Collisions2023_366403_369802_eraBC_GoldenJson",
    "MUO/Run3-23CSep23-Summer23-NanoAODv12/latest/muon_Z.json.gz",
    "EGM/Run3-23CSep23-Summer23-NanoAODv12/latest/electron.json.gz",
    "BTV/Run3-23CSep23-Summer23-NanoAODv12/latest/btagging.json.gz",
    "BTV/Run3-23CSep23-Summer23-NanoAODv12/latest/ctagging.json.gz",
    "JME/Run3-23CSep23-Summer23-NanoAODv12/latest/jet_jerc.json.gz",
    "Summer23Prompt23_V2_MC_L1L2L3Res_AK4PFPuppi",
    "Summer23Prompt23_V2_MC_Regrouped_Total_AK4PFPuppi",
    "Summer23Prompt23_RunCv1234_JRV2_MC_PtResolution_AK4PFPuppi",
    "Summer23Prompt23_RunCv1234_JRV2_MC_ScaleFactor_AK4PFPuppi",
    {"Jet_eta", "Jet_pt_corr_jec"},
    "EGM/Run3-23CSep23-Summer23-NanoAODv12/latest/electronSS_EtDependent.json.gz",
    "EGMScale",
    "EGMSmearAndSyst",
    true,
    "MUO/Run3-23CSep23-Summer23-NanoAODv12/latest/muon_scalesmearing.json.gz",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions23/Cert_Collisions2023_366442_370790_Golden.json",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu24"},
    {"Ele30_WPTight_Gsf"},
    {"PFMET120_PFMHT120_IDTight"}
  },
  // ==========================================================================
  // 2023 Post-BPix (Run 3)
  // ==========================================================================
  {
    "2023_postBPix",
    "2023_postBPix",
    "Rho_fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "Electron-ID-SF",
    "wp80iso",
    "LUM/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/puWeights.json.gz",
    "Collisions2023_369803_370790_eraD_GoldenJson",
    "MUO/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/muon_Z.json.gz",
    "EGM/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/electron.json.gz",
    "BTV/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/btagging.json.gz",
    "BTV/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/ctagging.json.gz",
    "JME/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/jet_jerc.json.gz",
    "Summer23BPixPrompt23_V3_MC_L1L2L3Res_AK4PFPuppi",
    "Summer23BPixPrompt23_V3_MC_Regrouped_Total_AK4PFPuppi",
    "Summer23BPixPrompt23_RunD_JRV2_MC_PtResolution_AK4PFPuppi",
    "Summer23BPixPrompt23_RunD_JRV2_MC_ScaleFactor_AK4PFPuppi",
    {"Jet_eta", "Jet_pt_corr_jec"},
    "EGM/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/electronSS_EtDependent.json.gz",
    "EGMScale",
    "EGMSmearAndSyst",
    true,
    "MUO/Run3-23DSep23-Summer23BPix-NanoAODv12/latest/muon_scalesmearing.json.gz",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions23/Cert_Collisions2023_366442_370790_Golden.json",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu24"},
    {"Ele30_WPTight_Gsf"},
    {"PFMET120_PFMHT120_IDTight"}
  },
  // ==========================================================================
  // 2024 (Run 3)
  // ==========================================================================
  {
    "2024",
    "2024Prompt",
    "Rho_fixedGridRhoFastjetAll",
    "NUM_MediumID_DEN_TrackerMuons",
    "Electron-ID-SF",
    "wp80iso",
    "LUM/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/puWeights_BCDEFGHI.json.gz",
    "Collisions24_BCDEFGHI_goldenJSON",
    "MUO/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/muon_Z.json.gz",
    "EGM/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/electron.json.gz",
    "BTV/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/btagging.json.gz",
    "BTV/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/ctagging.json.gz",
    "JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/jet_jerc.json.gz",
    "Summer24Prompt24_V5_MC_L1L2L3Res_AK4PFPuppi",
    "Summer24Prompt24_V5_MC_Regrouped_Total_AK4PFPuppi",
    "Summer24Prompt24_JRV2_MC_PtResolution_AK4PFPuppi",
    "Summer24Prompt24_JRV2_MC_ScaleFactor_AK4PFPuppi",
    {"Jet_eta", "Jet_pt_corr_jec"},
    "EGM/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/electronSS_EtDependent.json.gz",
    "EGMScale",
    "EGMSmearAndSyst",
    true,
    "MUO/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/muon_scalesmearing.json.gz",
    "/eos/user/c/cmsdqm/www/CAF/certification/Collisions24/Cert_Collisions2024_378981_386951_Golden.json",
    {"Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"},
    {"Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"},
    {"IsoMu24"},
    {"Ele30_WPTight_Gsf"},
    {"PFMET120_PFMHT120_IDTight"}
  },
};
// clang-format on

constexpr size_t kNumBuiltInDefaults =
    sizeof(kBuiltInDefaults) / sizeof(kBuiltInDefaults[0]);

const BuiltInYearEntry *findBuiltIn(const std::string &canonicalYear) {
  for (size_t i = 0; i < kNumBuiltInDefaults; ++i) {
    if (kBuiltInDefaults[i].canonicalYear == canonicalYear) {
      return &kBuiltInDefaults[i];
    }
  }
  return nullptr;
}

} // namespace

// ===========================================================================
// Year alias normalization
// ===========================================================================

std::string CmsYearRegistry::normalizeYear(const std::string &year) {
  static const std::unordered_map<std::string, std::string> aliases{
    // 2016
    {"2016pre", "2016_PreVFP"},
    {"2016_preVFP", "2016_PreVFP"},
    {"2016preVFP", "2016_PreVFP"},
    {"2016_PreVFP", "2016_PreVFP"},
    {"2016post", "2016_PostVFP"},
    {"2016_postVFP", "2016_PostVFP"},
    {"2016postVFP", "2016_PostVFP"},
    {"2016_PostVFP", "2016_PostVFP"},
    {"2016", "2016_PostVFP"},
    // 2017
    {"2017", "2017"},
    // 2018
    {"2018", "2018"},
    // 2022
    {"22pre", "2022_preEE"},
    {"22preEE", "2022_preEE"},
    {"2022preEE", "2022_preEE"},
    {"2022_preEE", "2022_preEE"},
    {"22post", "2022_postEE"},
    {"22postEE", "2022_postEE"},
    {"2022postEE", "2022_postEE"},
    {"2022_postEE", "2022_postEE"},
    {"2022", "2022_postEE"},
    // 2023
    {"23pre", "2023_preBPix"},
    {"23preBPix", "2023_preBPix"},
    {"2023preBPix", "2023_preBPix"},
    {"2023_preBPix", "2023_preBPix"},
    {"23post", "2023_postBPix"},
    {"23postBPix", "2023_postBPix"},
    {"2023postBPix", "2023_postBPix"},
    {"2023_postBPix", "2023_postBPix"},
    {"2023", "2023_postBPix"},
    // 2024
    {"24", "2024"},
    {"2024", "2024"},
  };

  const auto it = aliases.find(year);
  if (it == aliases.end()) {
    throw std::runtime_error("CmsYearConfig: unsupported year alias '" +
                             year + "'. Supported values: 2016_PreVFP, " +
                             "2016_PostVFP, 2017, 2018, 2022_preEE, " +
                             "2022_postEE, 2023_preBPix, 2023_postBPix, 2024.");
  }
  return it->second;
}

// ===========================================================================
// Default JES sources per year
// ===========================================================================

std::vector<std::string> CmsYearRegistry::defaultJesReducedSources(
    const std::string &canonicalYear) {
  // The reduced (regrouped) JES uncertainty set.
  // CAT names map to JECDatabase "Regrouped_*" corrections.
  std::vector<std::string> sources{
      "FlavorQCD",
      "RelativeBal",
      "HF",
      "BBEC1",
      "EC2",
      "Absolute"};

  // Year-tagged sources (correlated within year, uncorrelated across years)
  const auto yearTag = [&]() -> std::string {
    if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP") return "2016";
    if (canonicalYear == "2017") return "2017";
    if (canonicalYear == "2018") return "2018";
    if (canonicalYear == "2022_preEE" || canonicalYear == "2022_postEE") return "2022";
    if (canonicalYear == "2023_preBPix") return "2023";
    if (canonicalYear == "2023_postBPix") return "2023BPix";
    if (canonicalYear == "2024") return "2024";
    return "";
  }();
  if (!yearTag.empty()) {
    sources.push_back("Absolute_" + yearTag);
    sources.push_back("HF_" + yearTag);
    sources.push_back("EC2_" + yearTag);
    sources.push_back("RelativeSample_" + yearTag);
    sources.push_back("BBEC1_" + yearTag);
  }

  return sources;
}

std::vector<std::string> CmsYearRegistry::defaultJesFullSources(
    const std::string &canonicalYear) {
  // Full JES uncertainty set following JME CAT naming (Table 1–style).
  // Untagged sources: correlated across years.
  std::vector<std::string> sources{
      "FlavorQCD",
      "Fragmentation",
      "RelativeBal",
      "RelativeFSR",
      "RelativeJEREC1",
      "RelativeJEREC2",
      "RelativeJERHF",
      "RelativePtBB",
      "RelativePtEC1",
      "RelativePtEC2",
      "RelativePtHF",
      "SinglePionECAL",
      "SinglePionHCAL",
      "PileUpDataMC",
      "PileUpPtBB",
      "PileUpPtEC1",
      "PileUpPtEC2",
      "PileUpPtHF",
      "PileUpPtRef",
  };

  // Year-tagged sources (correlated within year, uncorrelated across years)
  const auto yearTag = [&]() -> std::string {
    if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP") return "2016";
    if (canonicalYear == "2017") return "2017";
    if (canonicalYear == "2018") return "2018";
    if (canonicalYear == "2022_preEE" || canonicalYear == "2022_postEE") return "2022";
    if (canonicalYear == "2023_preBPix") return "2023";
    if (canonicalYear == "2023_postBPix") return "2023BPix";
    if (canonicalYear == "2024") return "2024";
    return "";
  }();
  if (!yearTag.empty()) {
    sources.push_back("AbsoluteMPFBias_" + yearTag);
    sources.push_back("AbsoluteScale_" + yearTag);
    sources.push_back("AbsoluteStat_" + yearTag);
    sources.push_back("RelativeSample_" + yearTag);
    sources.push_back("RelativeStatEC_" + yearTag);
    sources.push_back("RelativeStatFSR_" + yearTag);
    sources.push_back("RelativeStatHF_" + yearTag);
    sources.push_back("TimePtEta_" + yearTag);
  }

  return sources;
}

std::vector<std::string> CmsYearRegistry::defaultJerPerBinSources(
    const std::string &canonicalYear) {
  // JER per-(eta,pt)-bin uncertainty sources (Table 4–style).
  // Named after the bin definition for clarity.
  (void)canonicalYear;  // JER bins are generally stable across years
  return {
      "eta0to1p93_pt0toInf",
      "eta1p93to2p5_pt0toInf",
      "eta2p5to3p0_pt0to50",
      "eta2p5to3p0_pt50toInf",
      "eta3to5_pt0to50",
      "eta3to5_pt50toInf",
  };
}

// ===========================================================================
// Default tagger branches
// ===========================================================================

std::string CmsYearRegistry::defaultBTagBranch(
    const std::string &jetTagger, const std::string &canonicalYear) {
  if (jetTagger.find("DeepFlav") != std::string::npos) {
    return "Jet_btagDeepFlavB";
  }
  if (jetTagger.find("RobustParT") != std::string::npos) {
    return "Jet_btagRobustParTAK4B";
  }
  if (jetTagger.find("UParT") != std::string::npos) {
    return "Jet_btagUParTAK4B";
  }
  // Empty tagger: use year-appropriate default
  if (jetTagger.empty()) {
    if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP" ||
        canonicalYear == "2017" || canonicalYear == "2018") {
      return "Jet_btagDeepFlavB";
    }
    return "Jet_btagPNetB";
  }
  // ParticleNet (PNet) is the default
  if (jetTagger.find("PNet") != std::string::npos) {
    return "Jet_btagPNetB";
  }
  // Fallback: use DeepFlav for Run 2
  if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP" ||
      canonicalYear == "2017" || canonicalYear == "2018") {
    return "Jet_btagDeepFlavB";
  }
  return "Jet_btagPNetB";
}

std::string CmsYearRegistry::defaultCvLBranch(
    const std::string &jetTagger, const std::string &canonicalYear) {
  if (jetTagger.empty()) {
    if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP" ||
        canonicalYear == "2017" || canonicalYear == "2018") {
      return "Jet_btagDeepFlavCvL";
    }
    return "Jet_btagPNetCvL";
  }
  if (jetTagger.find("DeepFlav") != std::string::npos) {
    return "Jet_btagDeepFlavCvL";
  }
  if (jetTagger.find("RobustParT") != std::string::npos) {
    return "Jet_btagRobustParTAK4CvL";
  }
  if (jetTagger.find("UParT") != std::string::npos) {
    return "Jet_btagUParTAK4CvL";
  }
  if (jetTagger.find("PNet") != std::string::npos) {
    return "Jet_btagPNetCvL";
  }
  if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP" ||
      canonicalYear == "2017" || canonicalYear == "2018") {
    return "Jet_btagDeepFlavCvL";
  }
  return "Jet_btagPNetCvL";
}

std::string CmsYearRegistry::defaultCvBBranch(
    const std::string &jetTagger, const std::string &canonicalYear) {
  if (jetTagger.empty()) {
    if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP" ||
        canonicalYear == "2017" || canonicalYear == "2018") {
      return "Jet_btagDeepFlavCvB";
    }
    return "Jet_btagPNetCvB";
  }
  if (jetTagger.find("DeepFlav") != std::string::npos) {
    return "Jet_btagDeepFlavCvB";
  }
  if (jetTagger.find("RobustParT") != std::string::npos) {
    return "Jet_btagRobustParTAK4CvB";
  }
  if (jetTagger.find("UParT") != std::string::npos) {
    return "Jet_btagUParTAK4CvB";
  }
  if (jetTagger.find("PNet") != std::string::npos) {
    return "Jet_btagPNetCvB";
  }
  if (canonicalYear == "2016_PreVFP" || canonicalYear == "2016_PostVFP" ||
      canonicalYear == "2017" || canonicalYear == "2018") {
    return "Jet_btagDeepFlavCvB";
  }
  return "Jet_btagPNetCvB";
}

// ===========================================================================
// Supported years
// ===========================================================================

std::vector<std::string> CmsYearRegistry::supportedYears() {
  return {
    "2016_PreVFP", "2016_PostVFP", "2017", "2018",
    "2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix", "2024"
  };
}

// ===========================================================================
// Registry implementation
// ===========================================================================

CmsYearRegistry::~CmsYearRegistry() = default;

void CmsYearRegistry::setYear(const std::string &year) {
  canonicalYear_m = normalizeYear(year);
}

void CmsYearRegistry::setCorrectionsBase(const std::string &base) {
  correctionsBase_m = base;
}

void CmsYearRegistry::setVersionOverride(const std::string &version) {
  versionOverride_m = version;
}

void CmsYearRegistry::override(const std::string &key, const std::string &value) {
  scalarOverrides_m[key] = value;
}

void CmsYearRegistry::overrideList(const std::string &key,
                                    const std::vector<std::string> &values) {
  listOverrides_m[key] = values;
}

void CmsYearRegistry::clearOverrides() {
  scalarOverrides_m.clear();
  listOverrides_m.clear();
}

void CmsYearRegistry::loadRegistryFile(const std::string &yamlPath) {
  if (yamlPath.empty()) return;

  try {
    auto node = YAML::LoadFile(yamlPath);
    auto yearsNode = node["years"];
    if (!yearsNode) {
      std::cerr << "CmsYearConfig: warning: YAML registry '"
                << yamlPath << "' has no 'years' key.\n";
      return;
    }
    yamlState_m = std::make_unique<YamlRegistryState>();
    yamlState_m->years = yearsNode;
    yamlState_m->sourcePath = yamlPath;
  } catch (const YAML::Exception &e) {
    throw std::runtime_error("CmsYearConfig: failed to load YAML registry '" +
                             yamlPath + "': " + e.what());
  }
}

void CmsYearRegistry::ensureYearSet() const {
  if (canonicalYear_m.empty()) {
    throw std::runtime_error(
        "CmsYearConfig: year not set. Call setYear() before load().");
  }
}

// ===========================================================================
// Path resolution
// ===========================================================================

std::string CmsYearRegistry::resolvePathInternal(
    const std::string &rawPath,
    const std::string &correctionsBase,
    const std::string &repoRootHint) const {
  if (rawPath.empty()) return rawPath;

  std::string resolved = rawPath;

  // Substitute {version} token
  if (resolved.find("{version}") != std::string::npos) {
    const std::string versionToken =
        versionOverride_m.empty() ? "latest" : versionOverride_m;
    const auto pos = resolved.find("{version}");
    resolved.replace(pos, std::string("{version}").size(), versionToken);
  }

  // Absolute paths, root://, and /eos/ paths are used as-is
  if (resolved.rfind("/", 0) == 0 ||
      resolved.rfind("root://", 0) == 0) {
    return resolved;
  }

  // Prepend correctionsBase for relative paths that look like CVMFS paths
  // (e.g. "LUM/Run3-...", "JME/Run3-...")
  if (!correctionsBase.empty() &&
      resolved.find('/') != std::string::npos &&
      resolved.rfind("analyses/", 0) != 0) {
    return joinPath(correctionsBase, resolved);
  }

  // Prepend repoRootHint for analysis-relative paths (e.g. "analyses/...")
  if (!repoRootHint.empty()) {
    return joinPath(repoRootHint, resolved);
  }

  return resolved;
}

std::string CmsYearRegistry::resolvePath(const std::string &rawPath) const {
  return resolvePathInternal(rawPath, correctionsBase_m, "");
}

// ===========================================================================
// YAML entry application
// ===========================================================================

void CmsYearRegistry::applyYearEntry(
    CmsYearSettings &settings,
    const std::string &yamlKey,
    const std::string &canonicalYear) const {
  // This is called when a YAML registry file is loaded.
  // It applies the YAML values over the built-in defaults.
  // Since we don't have the YAML node here (it's stateful), the
  // YAML application happens inside load() where we have access.
}

// ===========================================================================
// Build settings
// ===========================================================================

CmsYearSettings CmsYearRegistry::load() const {
  ensureYearSet();

  const BuiltInYearEntry *builtIn = findBuiltIn(canonicalYear_m);
  if (!builtIn) {
    throw std::runtime_error("CmsYearConfig: no built-in defaults for '" +
                             canonicalYear_m + "'.");
  }

  CmsYearSettings s;
  s.canonicalYear = builtIn->canonicalYear;
  s.electronYearToken = builtIn->electronYearToken;
  s.rhoBranch = builtIn->rhoBranch;
  s.muonCorrection = builtIn->muonCorrection;
  s.electronCorrection = builtIn->electronCorrection;
  s.electronWorkingPoint = builtIn->electronWorkingPoint;
  s.pileupCorrection = builtIn->pileupCorrection;
  s.jecCompoundCorrection = builtIn->jecCompoundCorrection;
  s.jesTotalCorrection = builtIn->jesTotalCorrection;
  s.jerPtResolutionCorrection = builtIn->jerPtResolutionCorrection;
  s.jerScaleFactorCorrection = builtIn->jerScaleFactorCorrection;
  s.jerScaleFactorInputs = builtIn->jerScaleFactorInputs;
  s.electronScaleCorrection = builtIn->electronScaleCorrection;
  s.electronSmearingCorrection = builtIn->electronSmearingCorrection;
  s.electronScaleSmearingEtDependent = builtIn->electronScaleSmearingEtDependent;
  s.goldenJsonFile = builtIn->goldenJsonFile;
  s.doubleMuonTriggers = withHltPrefix(builtIn->doubleMuonTriggers);
  s.doubleElectronTriggers = withHltPrefix(builtIn->doubleElectronTriggers);
  s.singleMuonTriggers = withHltPrefix(builtIn->singleMuonTriggers);
  s.singleElectronTriggers = withHltPrefix(builtIn->singleElectronTriggers);
  s.metTriggers = withHltPrefix(builtIn->metTriggers);

  // Resolve file paths
  s.pileupFile = resolvePathInternal(builtIn->pileupFile, correctionsBase_m, "");
  s.muonFile = resolvePathInternal(builtIn->muonFile, correctionsBase_m, "");
  s.electronFile = resolvePathInternal(builtIn->electronFile, correctionsBase_m, "");
  s.btvBTagFile = resolvePathInternal(builtIn->btvBTagFile, correctionsBase_m, "");
  s.btvCTagFile = resolvePathInternal(builtIn->btvCTagFile, correctionsBase_m, "");
  s.jmeFile = resolvePathInternal(builtIn->jmeFile, correctionsBase_m, "");
  s.electronScaleSmearingFile = resolvePathInternal(
      builtIn->electronScaleSmearingFile, correctionsBase_m, "");
  s.muonScaleResolutionFile = resolvePathInternal(
      builtIn->muonScaleResolutionFile, correctionsBase_m, "");

  // Set JES sources from defaults
  s.jesSources = defaultJesSources(canonicalYear_m);

  // Apply YAML registry overrides if loaded
  if (yamlState_m) {
    const auto yearNode = yamlState_m->years[canonicalYear_m];
    if (yearNode) {
      // Helper to resolve a YAML value as a correction path
      auto resolveYamlPath = [&](const std::string &key) -> std::string {
        const auto node = yearNode[key];
        if (!node) return "";
        return resolvePathInternal(node.as<std::string>(), correctionsBase_m, "");
      };

      auto resolveYamlString = [&](const std::string &key) -> std::string {
        const auto node = yearNode[key];
        if (!node) return "";
        return node.as<std::string>();
      };

      auto resolveYamlStringVector = [&](const std::string &key) -> std::vector<std::string> {
        const auto node = yearNode[key];
        if (!node) return {};
        return node.as<std::vector<std::string>>();
      };

      // Override fields from YAML if present
      if (yearNode["electronYearToken"]) s.electronYearToken = resolveYamlString("electronYearToken");
      if (yearNode["rhoBranch"]) s.rhoBranch = resolveYamlString("rhoBranch");
      if (yearNode["muonCorrection"]) s.muonCorrection = resolveYamlString("muonCorrection");
      if (yearNode["electronCorrection"]) s.electronCorrection = resolveYamlString("electronCorrection");
      if (yearNode["electronWorkingPoint"]) s.electronWorkingPoint = resolveYamlString("electronWorkingPoint");
      if (yearNode["pileupCorrection"]) s.pileupCorrection = resolveYamlString("pileupCorrection");
      if (yearNode["pileupFile"]) s.pileupFile = resolveYamlPath("pileupFile");
      if (yearNode["muonFile"]) s.muonFile = resolveYamlPath("muonFile");
      if (yearNode["electronFile"]) s.electronFile = resolveYamlPath("electronFile");
      if (yearNode["btvBTagFile"]) s.btvBTagFile = resolveYamlPath("btvBTagFile");
      if (yearNode["btvCTagFile"]) s.btvCTagFile = resolveYamlPath("btvCTagFile");
      if (yearNode["jmeFile"]) s.jmeFile = resolveYamlPath("jmeFile");
      if (yearNode["jecCompoundCorrection"]) s.jecCompoundCorrection = resolveYamlString("jecCompoundCorrection");
      if (yearNode["jesTotalCorrection"]) s.jesTotalCorrection = resolveYamlString("jesTotalCorrection");
      if (yearNode["jesSources"]) s.jesSources = resolveYamlStringVector("jesSources");
      if (yearNode["jerPtResolutionCorrection"]) s.jerPtResolutionCorrection = resolveYamlString("jerPtResolutionCorrection");
      if (yearNode["jerScaleFactorCorrection"]) s.jerScaleFactorCorrection = resolveYamlString("jerScaleFactorCorrection");
      if (yearNode["jerScaleFactorInputs"]) s.jerScaleFactorInputs = resolveYamlStringVector("jerScaleFactorInputs");
      if (yearNode["electronScaleSmearingFile"]) s.electronScaleSmearingFile = resolveYamlPath("electronScaleSmearingFile");
      if (yearNode["electronScaleCorrection"]) s.electronScaleCorrection = resolveYamlString("electronScaleCorrection");
      if (yearNode["electronSmearingCorrection"]) s.electronSmearingCorrection = resolveYamlString("electronSmearingCorrection");
      if (yearNode["electronScaleSmearingEtDependent"]) {
        s.electronScaleSmearingEtDependent = yearNode["electronScaleSmearingEtDependent"].as<bool>();
      }
      if (yearNode["muonScaleResolutionFile"]) s.muonScaleResolutionFile = resolveYamlPath("muonScaleResolutionFile");
      if (yearNode["triggerSfRoot"]) {
        s.triggerSfRoot = resolveYamlPath("triggerSfRoot");
      }
      if (yearNode["goldenJsonFile"]) {
        const auto gjPath = yearNode["goldenJsonFile"].as<std::string>();
        s.goldenJsonFile = resolvePathInternal(gjPath, correctionsBase_m, "");
      }
      if (yearNode["doubleMuonTriggers"]) s.doubleMuonTriggers = withHltPrefix(resolveYamlStringVector("doubleMuonTriggers"));
      if (yearNode["doubleElectronTriggers"]) s.doubleElectronTriggers = withHltPrefix(resolveYamlStringVector("doubleElectronTriggers"));
      if (yearNode["singleMuonTriggers"]) s.singleMuonTriggers = withHltPrefix(resolveYamlStringVector("singleMuonTriggers"));
      if (yearNode["singleElectronTriggers"]) s.singleElectronTriggers = withHltPrefix(resolveYamlStringVector("singleElectronTriggers"));
      if (yearNode["metTriggers"]) s.metTriggers = withHltPrefix(resolveYamlStringVector("metTriggers"));
    }
  }

  // Apply programmatic scalar overrides
  for (const auto &[key, value] : scalarOverrides_m) {
    if (key == "rhoBranch") s.rhoBranch = value;
    else if (key == "btagBranch") s.btagBranch = value;
    else if (key == "cvlBranch") s.cvlBranch = value;
    else if (key == "cvbBranch") s.cvbBranch = value;
    else if (key == "electronYearToken") s.electronYearToken = value;
    else if (key == "muonCorrection") s.muonCorrection = value;
    else if (key == "electronCorrection") s.electronCorrection = value;
    else if (key == "electronWorkingPoint") s.electronWorkingPoint = value;
    else if (key == "pileupFile") s.pileupFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "pileupCorrection") s.pileupCorrection = value;
    else if (key == "muonFile") s.muonFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "electronFile") s.electronFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "btvBTagFile") s.btvBTagFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "btvCTagFile") s.btvCTagFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "jmeFile") s.jmeFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "jecCompoundCorrection") s.jecCompoundCorrection = value;
    else if (key == "jesTotalCorrection") s.jesTotalCorrection = value;
    else if (key == "jerPtResolutionCorrection") s.jerPtResolutionCorrection = value;
    else if (key == "jerScaleFactorCorrection") s.jerScaleFactorCorrection = value;
    else if (key == "electronScaleSmearingFile") s.electronScaleSmearingFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "electronScaleCorrection") s.electronScaleCorrection = value;
    else if (key == "electronSmearingCorrection") s.electronSmearingCorrection = value;
    else if (key == "muonScaleResolutionFile") s.muonScaleResolutionFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "triggerSfRoot") s.triggerSfRoot = resolvePathInternal(value, correctionsBase_m, "");
    else if (key == "goldenJsonFile") s.goldenJsonFile = resolvePathInternal(value, correctionsBase_m, "");
  }

  // Apply programmatic list overrides
  for (const auto &[key, values] : listOverrides_m) {
    if (key == "jesSources") s.jesSources = values;
    else if (key == "jerScaleFactorInputs") s.jerScaleFactorInputs = values;
    else if (key == "doubleMuonTriggers") s.doubleMuonTriggers = withHltPrefix(values);
    else if (key == "doubleElectronTriggers") s.doubleElectronTriggers = withHltPrefix(values);
    else if (key == "singleMuonTriggers") s.singleMuonTriggers = withHltPrefix(values);
    else if (key == "singleElectronTriggers") s.singleElectronTriggers = withHltPrefix(values);
    else if (key == "metTriggers") s.metTriggers = withHltPrefix(values);
  }

  return s;
}

CmsYearSettings CmsYearRegistry::loadWithOverrides(
    const std::unordered_map<std::string, std::string> &configMap,
    const std::string &configPrefix) const {
  auto settings = load();

  // Apply inline config map overrides (e.g. from Analyzer config)
  for (const auto &[key, value] : configMap) {
    // Strip prefix if present
    std::string strippedKey = key;
    if (!configPrefix.empty() && key.rfind(configPrefix, 0) == 0) {
      strippedKey = key.substr(configPrefix.size());
      // Strip leading '_' separator between prefix and key name
      if (!strippedKey.empty() && strippedKey[0] == '_') {
        strippedKey = strippedKey.substr(1);
      }
    }

    // Map config keys to CmsYearSettings fields
    if (strippedKey == "rhoBranch") settings.rhoBranch = value;
    else if (strippedKey == "btagBranch") settings.btagBranch = value;
    else if (strippedKey == "cvlBranch") settings.cvlBranch = value;
    else if (strippedKey == "cvbBranch") settings.cvbBranch = value;
    else if (strippedKey == "electronYearToken") settings.electronYearToken = value;
    else if (strippedKey == "muonCorrection") settings.muonCorrection = value;
    else if (strippedKey == "electronCorrection") settings.electronCorrection = value;
    else if (strippedKey == "electronWorkingPoint") settings.electronWorkingPoint = value;
    else if (strippedKey == "pileupFile") settings.pileupFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "pileupCorrection") settings.pileupCorrection = value;
    else if (strippedKey == "muonFile") settings.muonFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "electronFile") settings.electronFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "btvBTagFile") settings.btvBTagFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "btvCTagFile") settings.btvCTagFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "jmeFile") settings.jmeFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "jecCompoundCorrection") settings.jecCompoundCorrection = value;
    else if (strippedKey == "jesTotalCorrection") settings.jesTotalCorrection = value;
    else if (strippedKey == "jerPtResolutionCorrection") settings.jerPtResolutionCorrection = value;
    else if (strippedKey == "jerScaleFactorCorrection") settings.jerScaleFactorCorrection = value;
    else if (strippedKey == "electronScaleSmearingFile") settings.electronScaleSmearingFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "electronScaleCorrection") settings.electronScaleCorrection = value;
    else if (strippedKey == "electronSmearingCorrection") settings.electronSmearingCorrection = value;
    else if (strippedKey == "muonScaleResolutionFile") settings.muonScaleResolutionFile = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "triggerSfRoot") settings.triggerSfRoot = resolvePathInternal(value, correctionsBase_m, "");
    else if (strippedKey == "goldenJsonFile") settings.goldenJsonFile = resolvePathInternal(value, correctionsBase_m, "");
  }

  return settings;
}

} // namespace cms
