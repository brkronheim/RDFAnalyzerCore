/**
 * @file analysis.cc
 * @brief CMS analysis template — NanoAOD single-lepton (W/Z+jets style).
 *
 * This file is a starting-point template for a realistic CMS analysis
 * using the RDFAnalyzerCore framework.  It demonstrates every major
 * building block used in production analyses:
 *
 *  - **Multiple object collections** of the same type at different working
 *    points: loose and tight muons, loose and tight electrons, and jets.
 *    Users can add or rename working-point collections without changing any
 *    framework code.
 *
 *  - **CMS default corrections** applied at the point of object definition:
 *    Rochester momentum corrections for muons, energy-scale/smearing for
 *    electrons, and JEC for jets.  Each correction is applied to the nominal
 *    collection and—automatically via the framework—to all systematic
 *    variations.
 *
 *  - **Multiple analysis regions** via RegionManager: a signal region (SR),
 *    a W+jets control region (W CR), and a tt̄ control region (top CR), all
 *    built on a shared preselection node.
 *
 *  - **Cutflow auditing** via CutflowManager: sequential and N-1 tables.
 *
 *  - **Event weights** via WeightManager: pileup reweighting, muon ID/ISO
 *    scale factors, and electron ID scale factors are all combined into a
 *    single nominal-weight column with automatic propagation of each
 *    component's systematic variation.
 *
 *  - **Config-driven histograms** via NDHistogramManager: no histogram
 *    booking code in analysis.cc.
 *
 *  - **Trigger selection** via TriggerManager: fully config-driven, zero
 *    trigger logic in analysis.cc.
 *
 * ## How to adapt this template
 *
 *  1. Replace the object-selection functions (buildLooseMuons, buildTightMuons,
 *     buildLooseElectrons, buildTightElectrons, buildPreJets) with your own
 *     working-point definitions.
 *  2. Replace the region predicates (isSignalRegion, isWControlRegion,
 *     isTopControlRegion) with your analysis regions.
 *  3. Add or remove Define/Filter calls for your derived quantities.
 *  4. Update histograms.yaml to book the histograms you need.
 *  5. Update corrections.yaml / triggers.yaml / floats.yaml as required.
 *
 * ## Build and run
 *
 *   cmake -S . -B build && cmake --build build -j$(nproc)
 *   cd build/analyses/CMSAnalysisTemplate
 *   ./cms_analysis_template ../../../analyses/CMSAnalysisTemplate/cfg.yaml
 *
 * @see README.md for full documentation.
 * @see docs/ANALYSIS_GUIDE.md for the framework API guide.
 * @see docs/CMS_CORRECTIONS.md for the corrections stack documentation.
 */

#include "analyzer.h"
#include <CutflowManager.h>
#include <NDHistogramManager.h>
#include <PhysicsObjectCollection.h>
#include <RegionManager.h>
#include <TriggerManager.h>
#include <WeightManager.h>
#include <functions.h>

#include <cmath>
#include <iostream>

// ============================================================================
// Type aliases for NanoAOD branch types
// ============================================================================
using FloatVec = ROOT::VecOps::RVec<Float_t>;
using IntVec   = ROOT::VecOps::RVec<Int_t>;
using BoolVec  = ROOT::VecOps::RVec<bool>;

// ============================================================================
// Object collection builders
//
// Each builder takes the raw NanoAOD branches as input and returns a
// PhysicsObjectCollection.  The collection stores 4-vectors and original
// indices for objects that pass the selection mask.
//
// Collection naming convention used in this template:
//   looseMuons    — loose ID + isolation, lower pT threshold
//   tightMuons    — tight ID + tighter isolation (subset of looseMuons)
//   looseElectrons — loose MVA-based ID
//   tightElectrons — tight MVA-based ID (subset of looseElectrons)
//   preJets        — jet quality + kinematics before overlap removal
//   goodJets       — preJets with ΔR overlap removal vs. selected leptons
//
// IMPORTANT: When CMS corrections are active (see corrections.yaml), you
// should pass the *corrected* pt/mass columns to these builders so that the
// object selection is performed on the corrected kinematics.  The column
// names produced by the correction managers are documented in the cfg.yaml.
// ============================================================================

// ----------------------------------------------------------------------------
// Loose muon collection
//
// Criteria (CMS Run 3 NanoAOD defaults — adapt to your analysis):
//   - Loose muon ID (Muon_looseId)
//   - Relative PF isolation (Muon_pfRelIso04_all) < 0.25
//   - pT > 10 GeV
//   - |η| < 2.4
// ----------------------------------------------------------------------------
PhysicsObjectCollection buildLooseMuons(
        const FloatVec &pt,
        const FloatVec &eta,
        const FloatVec &phi,
        const FloatVec &mass,
        const BoolVec  &looseId,
        const FloatVec &iso) {
    ROOT::VecOps::RVec<bool> mask(pt.size(), false);
    for (std::size_t i = 0; i < pt.size(); ++i) {
        mask[i] = looseId[i]
               && iso[i]  < 0.25f
               && pt[i]   > 10.0f
               && std::abs(eta[i]) < 2.4f;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ----------------------------------------------------------------------------
// Tight muon collection
//
// Criteria:
//   - Tight muon ID (Muon_tightId)
//   - Relative PF isolation (Muon_pfRelIso04_all) < 0.15 (tight WP)
//   - pT > 26 GeV  (above the single-muon trigger threshold)
//   - |η| < 2.4
// ----------------------------------------------------------------------------
PhysicsObjectCollection buildTightMuons(
        const FloatVec &pt,
        const FloatVec &eta,
        const FloatVec &phi,
        const FloatVec &mass,
        const BoolVec  &tightId,
        const FloatVec &iso) {
    ROOT::VecOps::RVec<bool> mask(pt.size(), false);
    for (std::size_t i = 0; i < pt.size(); ++i) {
        mask[i] = tightId[i]
               && iso[i]  < 0.15f
               && pt[i]   > 26.0f
               && std::abs(eta[i]) < 2.4f;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ----------------------------------------------------------------------------
// Loose electron collection
//
// Criteria (CMS MVA-based ID for Run 3 NanoAOD — adapt as needed):
//   - Loose cut-based electron ID (Electron_cutBased >= 1)
//   - pT > 10 GeV
//   - |η_SC| < 2.5, excluding barrel-endcap gap (1.444 < |η| < 1.566)
// ----------------------------------------------------------------------------
PhysicsObjectCollection buildLooseElectrons(
        const FloatVec &pt,
        const FloatVec &eta,
        const FloatVec &phi,
        const FloatVec &mass,
        const IntVec   &cutBased,
        const FloatVec &etaSC) {
    ROOT::VecOps::RVec<bool> mask(pt.size(), false);
    for (std::size_t i = 0; i < pt.size(); ++i) {
        float absEtaSC = std::abs(etaSC[i]);
        bool inGap = (absEtaSC > 1.444f && absEtaSC < 1.566f);
        mask[i] = (cutBased[i] >= 1)
               && pt[i] > 10.0f
               && absEtaSC < 2.5f
               && !inGap;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ----------------------------------------------------------------------------
// Tight electron collection
//
// Criteria:
//   - Tight cut-based electron ID (Electron_cutBased >= 4)
//   - pT > 30 GeV  (above the single-electron trigger threshold)
//   - |η_SC| < 2.5, excluding barrel-endcap gap
// ----------------------------------------------------------------------------
PhysicsObjectCollection buildTightElectrons(
        const FloatVec &pt,
        const FloatVec &eta,
        const FloatVec &phi,
        const FloatVec &mass,
        const IntVec   &cutBased,
        const FloatVec &etaSC) {
    ROOT::VecOps::RVec<bool> mask(pt.size(), false);
    for (std::size_t i = 0; i < pt.size(); ++i) {
        float absEtaSC = std::abs(etaSC[i]);
        bool inGap = (absEtaSC > 1.444f && absEtaSC < 1.566f);
        mask[i] = (cutBased[i] >= 4)
               && pt[i] > 30.0f
               && absEtaSC < 2.5f
               && !inGap;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ----------------------------------------------------------------------------
// Pre-selection jet collection
//
// Criteria:
//   - Tight jet ID (Jet_jetId >= 2)
//   - pT > 30 GeV
//   - |η| < 4.7  (forward jets included; tighten to 2.4 for b-tagging)
//
// Overlap removal with selected leptons is applied separately in main().
// ----------------------------------------------------------------------------
PhysicsObjectCollection buildPreJets(
        const IntVec   &jetId,
        const FloatVec &pt,
        const FloatVec &eta,
        const FloatVec &phi,
        const FloatVec &mass) {
    ROOT::VecOps::RVec<bool> mask(pt.size(), false);
    for (std::size_t i = 0; i < pt.size(); ++i) {
        mask[i] = (jetId[i] >= 2)
               && pt[i]  > 30.0f
               && std::abs(eta[i]) < 4.7f;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ============================================================================
// Event-level predicates
//
// Each predicate evaluates a single cut condition and is registered with
// CutflowManager for sequential-cut + N-1 tracking.
//
// Design note: all predicates must be defined as standalone functions (not
// lambdas) so that they can be passed to Analyzer::Define by function pointer.
// ============================================================================

// Exactly one tight muon (W→μν topology)
bool hasExactlyOneTightMuon(const PhysicsObjectCollection &tightMuons) {
    return tightMuons.size() == 1;
}

// Zero additional loose muons (veto second muon for W→μν)
bool hasNoExtraLooseMuon(const PhysicsObjectCollection &looseMuons,
                          const PhysicsObjectCollection &tightMuons) {
    // loose collection includes tight objects; veto if more than one loose muon
    return looseMuons.size() == tightMuons.size();
}

// Zero loose electrons (lepton-flavour veto for W→μν)
bool hasNoLooseElectron(const PhysicsObjectCollection &looseElectrons) {
    return looseElectrons.empty();
}

// At least one good jet (basic jet activity)
bool hasAtLeastOneJet(const PhysicsObjectCollection &goodJets) {
    return !goodJets.empty();
}

// MET > 30 GeV (neutrino from W)
bool hasMETCut(Float_t met) {
    return met > 30.0f;
}

// Transverse mass m_T(μ, MET) > 40 GeV (W-boson transverse mass)
// m_T = sqrt(2 * pT_mu * MET * (1 - cos(Δφ)))
bool hasTransverseMassCut(Float_t mt) {
    return mt > 40.0f;
}

// ============================================================================
// Derived physics quantities
// ============================================================================

// Transverse mass m_T(lepton, MET)
Float_t computeTransverseMass(const PhysicsObjectCollection &tightMuons,
                               Float_t metPt, Float_t metPhi) {
    if (tightMuons.empty()) return -999.f;
    const auto &v = tightMuons.vectors()[0];
    float mu_pt  = static_cast<float>(std::hypot(v.Px(), v.Py()));
    float mu_phi = static_cast<float>(v.Phi());
    float dphi   = mu_phi - metPhi;
    // Wrap Δφ to (−π, π]
    while (dphi >  static_cast<float>(M_PI)) dphi -= 2.f * static_cast<float>(M_PI);
    while (dphi < -static_cast<float>(M_PI)) dphi += 2.f * static_cast<float>(M_PI);
    float mt2 = 2.f * mu_pt * metPt * (1.f - std::cos(dphi));
    return mt2 > 0.f ? std::sqrt(mt2) : 0.f;
}

// Leading muon pT [GeV]
Float_t leadMuPt(const PhysicsObjectCollection &muons) {
    if (muons.empty()) return -999.f;
    const auto &v = muons.vectors()[0];
    return static_cast<Float_t>(std::hypot(v.Px(), v.Py()));
}

// Leading muon η
Float_t leadMuEta(const PhysicsObjectCollection &muons) {
    if (muons.empty()) return -999.f;
    return static_cast<Float_t>(muons.vectors()[0].Eta());
}

// Leading muon φ
Float_t leadMuPhi(const PhysicsObjectCollection &muons) {
    if (muons.empty()) return -999.f;
    return static_cast<Float_t>(muons.vectors()[0].Phi());
}

// Leading jet pT [GeV]
Float_t leadJetPt(const PhysicsObjectCollection &jets) {
    if (jets.empty()) return -999.f;
    const auto &v = jets.vectors()[0];
    return static_cast<Float_t>(std::hypot(v.Px(), v.Py()));
}

// Leading jet η
Float_t leadJetEta(const PhysicsObjectCollection &jets) {
    if (jets.empty()) return -999.f;
    return static_cast<Float_t>(jets.vectors()[0].Eta());
}

// Jet multiplicity
Int_t nGoodJets(const PhysicsObjectCollection &jets) {
    return static_cast<Int_t>(jets.size());
}

// ============================================================================
// Region predicates
//
// These are used as boolean column definitions and passed to
// RegionManager::declareRegion().  They are evaluated *after* the shared
// preselection (trigger + tight lepton + MET cuts) has been applied.
//
// Regions defined in this template:
//   presel   — shared preselection: 1 tight muon, MET > 30 GeV, m_T > 40 GeV
//   signal   — signal region: nJets >= 1, m_T > 60 GeV
//   wCR      — W+jets control region: nJets == 0
//   topCR    — top-quark control region: nJets >= 2
// ============================================================================

bool isSignalRegion(Int_t nJets, Float_t mt) {
    return nJets >= 1 && mt > 60.f;
}

bool isWControlRegion(Int_t nJets) {
    return nJets == 0;
}

bool isTopControlRegion(Int_t nJets) {
    return nJets >= 2;
}

// ============================================================================
// main()
// ============================================================================
int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file.yaml>\n";
        return 1;
    }

    // -----------------------------------------------------------------------
    // 1. Construct the Analyzer from the YAML config file.
    //    All paths in cfg.yaml are resolved relative to the config file's
    //    directory, or as absolute paths.
    // -----------------------------------------------------------------------
    auto an = Analyzer(argv[1]);

    // -----------------------------------------------------------------------
    // 2. Register plugins.
    //
    //    Plugin registration order matters when plugins have implicit
    //    dependencies.  The general safe order is:
    //      NDHistogramManager — reads histogramConfig
    //      CutflowManager     — tracks per-cut sequential + N-1 tables
    //      TriggerManager     — reads triggerConfig; apply before analysis cuts
    //      RegionManager      — hierarchical analysis regions
    //      WeightManager      — nominal and varied event weights
    // -----------------------------------------------------------------------
    auto histMgr    = NDHistogramManager::create(an);
    auto cutflowMgr = CutflowManager::create(an);
    auto trigMgr    = TriggerManager::create(an);
    auto regionMgr  = RegionManager::create(an);
    auto weightMgr  = WeightManager::create(an);

    // -----------------------------------------------------------------------
    // 3. Apply trigger selection.
    //
    //    applyAllTriggers() reads the trigger group matching the 'type' key
    //    in cfg.yaml (0 = data, 1 = MC by convention in this template).
    //    It defines "allTriggersPassVector" and applies a filter on
    //    "pass_applyTrigger".  This happens before analysis cuts so that
    //    the cutflow table starts from triggered events.
    // -----------------------------------------------------------------------
    trigMgr->applyAllTriggers();

    // -----------------------------------------------------------------------
    // 4. Object collections.
    //
    //    All collections are defined on the dataframe before any CutflowManager
    //    cuts are registered, so that the N-1 computation can access them.
    //
    //    NOTE: To use CMS Rochester corrections for muons or energy-scale
    //    corrections for electrons, replace the plain NanoAOD branch names
    //    below with the corrected column names produced by the correction
    //    managers.  See cfg.yaml and README.md for how to enable corrections.
    //
    //    Example with Rochester-corrected pT:
    //      an.Define("looseMuons", buildLooseMuons, {
    //          "Muon_pt_roc",   // <- corrected pT from MuonRochesterManager
    //          "Muon_eta", "Muon_phi", "Muon_mass",
    //          "Muon_looseId", "Muon_pfRelIso04_all"});
    // -----------------------------------------------------------------------

    // Loose muons — used for the second-muon veto
    an.Define("looseMuons", buildLooseMuons,
        {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass",
         "Muon_looseId", "Muon_pfRelIso04_all"});

    // Tight muons — primary signal lepton
    an.Define("tightMuons", buildTightMuons,
        {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass",
         "Muon_tightId", "Muon_pfRelIso04_all"});

    // Loose electrons — used for the electron veto
    an.Define("looseElectrons", buildLooseElectrons,
        {"Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass",
         "Electron_cutBased", "Electron_eta"});

    // Tight electrons — available for an e+jets channel variant
    an.Define("tightElectrons", buildTightElectrons,
        {"Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass",
         "Electron_cutBased", "Electron_eta"});

    // Pre-selection jets (before overlap removal)
    an.Define("preJets", buildPreJets,
        {"Jet_jetId", "Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"});

    // Overlap-cleaned jets: remove jets within ΔR < 0.4 of any tight muon
    an.Define("goodJets",
        [](const PhysicsObjectCollection &jets,
           const PhysicsObjectCollection &muons) {
            return jets.removeOverlap(muons, 0.4f);
        },
        {"preJets", "tightMuons"});

    // Also remove jets overlapping with tight electrons for a combined veto
    an.Define("cleanJets",
        [](const PhysicsObjectCollection &jets,
           const PhysicsObjectCollection &electrons) {
            return jets.removeOverlap(electrons, 0.4f);
        },
        {"goodJets", "tightElectrons"});

    // -----------------------------------------------------------------------
    // 5. Event-level cut booleans.
    //
    //    IMPORTANT: All boolean columns used by CutflowManager::addCut()
    //    must be defined BEFORE the first addCut() call.
    // -----------------------------------------------------------------------

    an.Define("hasOneTightMuon",  hasExactlyOneTightMuon, {"tightMuons"});
    an.Define("hasNoExtraMuon",   hasNoExtraLooseMuon,    {"looseMuons", "tightMuons"});
    an.Define("hasNoElectron",    hasNoLooseElectron,     {"looseElectrons"});
    an.Define("hasOneJet",        hasAtLeastOneJet,       {"cleanJets"});

    // Derived quantities needed for MET and transverse-mass cuts
    an.Define("TransverseMass",   computeTransverseMass,  {"tightMuons", "MET_pt", "MET_phi"});
    an.Define("passMETCut",       hasMETCut,              {"MET_pt"});
    an.Define("passMTCut",        hasTransverseMassCut,   {"TransverseMass"});

    // Jet multiplicity (needed for region definition)
    an.Define("nCleanJets",       nGoodJets,              {"cleanJets"});

    // -----------------------------------------------------------------------
    // 6. Register analysis cuts with CutflowManager.
    //
    //    Each addCut() call captures the current DF state for the N-1
    //    computation, then applies the cut as a filter on the main DF.
    //    The order here defines the sequential cutflow table.
    // -----------------------------------------------------------------------
    cutflowMgr->addCut("OneTightMuon", "hasOneTightMuon");
    cutflowMgr->addCut("NoExtraMuon",  "hasNoExtraMuon");
    cutflowMgr->addCut("NoElectron",   "hasNoElectron");
    cutflowMgr->addCut("METCut",       "passMETCut");
    cutflowMgr->addCut("MTCut",        "passMTCut");

    // -----------------------------------------------------------------------
    // 7. Derived physics quantities (computed after preselection).
    // -----------------------------------------------------------------------

    an.Define("LeadMuPt",  leadMuPt,  {"tightMuons"});
    an.Define("LeadMuEta", leadMuEta, {"tightMuons"});
    an.Define("LeadMuPhi", leadMuPhi, {"tightMuons"});
    an.Define("LeadJetPt", leadJetPt, {"cleanJets"});
    an.Define("LeadJetEta",leadJetEta,{"cleanJets"});

    // Region discriminant booleans
    an.Define("pass_signal", isSignalRegion,     {"nCleanJets", "TransverseMass"});
    an.Define("pass_wCR",    isWControlRegion,   {"nCleanJets"});
    an.Define("pass_topCR",  isTopControlRegion, {"nCleanJets"});

    // -----------------------------------------------------------------------
    // 8. Declare analysis regions.
    //
    //    All region boolean columns must be defined before declareRegion().
    //    The base node is captured at the first declareRegion() call.
    //
    //    Region hierarchy in this template:
    //      signal   (root — events with ≥1 jet and m_T > 60 GeV)
    //      wCR      (root — 0-jet events; W+jets normalisation)
    //      topCR    (root — ≥2 jets; top-quark enriched)
    // -----------------------------------------------------------------------
    regionMgr->declareRegion("signal", "pass_signal");
    regionMgr->declareRegion("wCR",    "pass_wCR");
    regionMgr->declareRegion("topCR",  "pass_topCR");

    // -----------------------------------------------------------------------
    // 9. Event weights (MC only — for data set eventWeight = 1.0 in cfg.yaml).
    //
    //    WeightManager builds the nominal combined weight column as the
    //    product of all registered scale factor columns.  Each addScaleFactor()
    //    call refers to a dataframe column that must already be defined.
    //
    //    For this template we define a constant eventWeight = 1.0 to keep the
    //    template self-contained; in a real MC analysis you would add:
    //      wm->addScaleFactor("pileup",  "puWeight");
    //      wm->addScaleFactor("muonID",  "muonIdSF");
    //      wm->addScaleFactor("muonISO", "muonIsoSF");
    //      wm->addNormalization("xsec_lumi", xsec * lumi / sumWeights);
    // -----------------------------------------------------------------------

    // Constant unit weight — replace with real scale factors for MC
    an.Define("eventWeight", []() -> Float_t { return 1.0f; }, {});

    // Register the eventWeight column as the single scale factor component
    weightMgr->addScaleFactor("nominal", "eventWeight");
    weightMgr->defineNominalWeight("weight_nominal");

    // -----------------------------------------------------------------------
    // 10. Book histograms from histograms.yaml (NDHistogramManager).
    //     Called after all Define/Filter operations.
    // -----------------------------------------------------------------------
    an.bookConfigHistograms();

    // -----------------------------------------------------------------------
    // 11. Execute the RDataFrame event loop and write all outputs.
    //     run() triggers the loop exactly once, writes the skimmed tree (when
    //     enableSkim=1 in cfg.yaml), all histogram files, weight audit tables,
    //     region summaries, and cutflow tables.
    // -----------------------------------------------------------------------
    an.run();

    return 0;
}
