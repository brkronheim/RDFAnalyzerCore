/**
 * @file analysis.cc
 * @brief CMS Run2016G DoubleMuon Z→μμ analysis — CMS Open Data NanoAOD.
 *
 * This example demonstrates a complete end-to-end analysis using the
 * RDFAnalyzerCore framework with CMS Open Data (NanoAOD).  The analysis
 * selects events with exactly two opposite-sign tight muons, reconstructs
 * the dimuon invariant mass to study the Z boson peak, and produces
 * per-jet-multiplicity histograms for a Voigtian + exponential fit via
 * CMS Combine and CombineHarvester (see fit_zpeak.py).
 *
 * ## Framework features demonstrated
 *
 * - **TriggerManager** plugin for config-driven trigger selection.
 * - **PhysicsObjectCollection** for muon and jet object management, including
 *   built-in ΔR overlap removal (removeOverlap) and pair building (makePairs).
 * - **CutflowManager** plugin for sequential cutflow and N-1 tables.
 * - **NDHistogramManager** plugin for config-driven histogram booking.
 * - **YAML configuration** (cfg.yaml and all subconfigs) as the sole format.
 * - **LAW batch submission** via dataset_manifest.yaml (see README.md).
 *
 * ## Data source
 *
 * CMS Open Data record 30522 — publicly accessible, no credentials needed:
 *   /DoubleMuon/Run2016G-UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD
 *   https://opendata.cern.ch/record/30522
 *
 * ## Build & run
 *
 *   cmake -S . -B build && cmake --build build -j$(nproc)
 *   cd build/analyses/CMS_Run2016_DoubleMuon
 *   ./analysis ../../../analyses/CMS_Run2016_DoubleMuon/cfg.yaml
 */

#include "analyzer.h"
#include <CutflowManager.h>
#include <NDHistogramManager.h>
#include <PhysicsObjectCollection.h>
#include <TriggerManager.h>
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
// Muon PhysicsObjectCollection builder
//
// Selects muons passing tight quality criteria:
//   - Tight muon ID (Muon_tightId)
//   - Relative PF isolation (Muon_pfRelIso04_all) < 0.15 (tight WP)
//   - Transverse momentum pT > 20 GeV
//   - Pseudorapidity |η| < 2.4 (tracker acceptance)
//
// PhysicsObjectCollection stores the 4-vector and original index for each
// selected muon, enabling downstream feature extraction (e.g. charge).
// ============================================================================
PhysicsObjectCollection buildMuonCollection(
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
               && pt[i]   > 20.0f
               && std::abs(eta[i]) < 2.4f;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ============================================================================
// Pre-selection jet collection builder (no overlap removal here)
//
// Selects jets passing tight quality requirements:
//   - Tight jet ID (Jet_jetId >= 2)
//   - Transverse momentum pT > 30 GeV
//   - Pseudorapidity |η| < 2.5
//
// Overlap removal with the muon collection is performed separately via
// PhysicsObjectCollection::removeOverlap(), keeping the two steps explicit
// and individually auditable.
// ============================================================================
PhysicsObjectCollection buildPreJetCollection(
        const IntVec   &jetId,
        const FloatVec &pt,
        const FloatVec &eta,
        const FloatVec &phi,
        const FloatVec &mass) {
    ROOT::VecOps::RVec<bool> mask(pt.size(), false);
    for (std::size_t i = 0; i < pt.size(); ++i) {
        mask[i] = (jetId[i] >= 2)
               && pt[i]  > 30.0f
               && std::abs(eta[i]) < 2.5f;
    }
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}

// ============================================================================
// Event-level boolean predicates for CutflowManager
//
// These are evaluated on every event (after the trigger filter), including
// those with fewer than two tight muons, so guards are required when
// accessing indexed objects.
// ============================================================================

// Exactly two tight muons in the event
bool hasExactlyTwoMuons(const PhysicsObjectCollection &muons) {
    return muons.size() == 2;
}

// The two tight muons carry opposite electric charges (Z → μ⁺μ⁻)
bool hasOppositeSign(const PhysicsObjectCollection &muons,
                     const IntVec &charge) {
    if (muons.size() < 2) return false;
    return charge[muons.index(0)] * charge[muons.index(1)] < 0;
}

// Dimuon invariant mass in the Z peak window [70, 110] GeV.
// Guard for size < 2 is required for the N-1 computation.
bool inMassWindow(const PhysicsObjectCollection &muons) {
    if (muons.size() < 2) return false;
    const auto pairs = makePairs(muons);
    const float m = static_cast<float>(pairs[0].p4.M());
    return m > 70.0f && m < 110.0f;
}

// ============================================================================
// Derived physics quantities (computed AFTER all event-level cuts)
//
// At this point every event has exactly two opposite-sign tight muons in
// the Z mass window, so no guards are needed.
// ============================================================================

// Dimuon invariant mass [GeV] using PhysicsObjectCollection::makePairs
Float_t dimuonMass(const PhysicsObjectCollection &muons) {
    return static_cast<Float_t>(makePairs(muons)[0].p4.M());
}

// Leading muon pT [GeV]
Float_t leadMuPt(const PhysicsObjectCollection &muons) {
    const auto &vecs = muons.vectors();
    float pt0 = static_cast<float>(std::hypot(vecs[0].Px(), vecs[0].Py()));
    float pt1 = static_cast<float>(std::hypot(vecs[1].Px(), vecs[1].Py()));
    return std::max(pt0, pt1);
}

// Subleading muon pT [GeV]
Float_t sublMuPt(const PhysicsObjectCollection &muons) {
    const auto &vecs = muons.vectors();
    float pt0 = static_cast<float>(std::hypot(vecs[0].Px(), vecs[0].Py()));
    float pt1 = static_cast<float>(std::hypot(vecs[1].Px(), vecs[1].Py()));
    return std::min(pt0, pt1);
}

// Leading muon pseudorapidity
Float_t leadMuEta(const PhysicsObjectCollection &muons) {
    const auto &vecs = muons.vectors();
    float pt0 = static_cast<float>(std::hypot(vecs[0].Px(), vecs[0].Py()));
    float pt1 = static_cast<float>(std::hypot(vecs[1].Px(), vecs[1].Py()));
    return static_cast<Float_t>(pt0 >= pt1 ? vecs[0].Eta() : vecs[1].Eta());
}

// Subleading muon pseudorapidity
Float_t sublMuEta(const PhysicsObjectCollection &muons) {
    const auto &vecs = muons.vectors();
    float pt0 = static_cast<float>(std::hypot(vecs[0].Px(), vecs[0].Py()));
    float pt1 = static_cast<float>(std::hypot(vecs[1].Px(), vecs[1].Py()));
    return static_cast<Float_t>(pt0 >= pt1 ? vecs[1].Eta() : vecs[0].Eta());
}

// Number of selected jets (after muon overlap removal)
Int_t nGoodJets(const PhysicsObjectCollection &jets) {
    return static_cast<Int_t>(jets.size());
}

// ============================================================================
// Per-jet-bin dimuon mass variables
//
// Returns the dimuon mass only when the event is in the specified jet
// category; otherwise returns the sentinel -999.  Because all histogram
// lowerBound values are 70 GeV, sentinel events are invisible in plots.
//
// This lets the config-driven NDHistogramManager fill per-category
// histograms without additional C++ code per bin.
// ============================================================================
Float_t dimuonMass0j  (Float_t m, Int_t nj) { return nj == 0 ? m : -999.f; }
Float_t dimuonMass1j  (Float_t m, Int_t nj) { return nj == 1 ? m : -999.f; }
Float_t dimuonMass2j  (Float_t m, Int_t nj) { return nj == 2 ? m : -999.f; }
Float_t dimuonMassGe3j(Float_t m, Int_t nj) { return nj >= 3 ? m : -999.f; }

// ============================================================================
// main()
// ============================================================================
int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <config_file.yaml>\n";
        return 1;
    }

    // -----------------------------------------------------------------------
    // 1. Construct the Analyzer from the YAML config file.
    // -----------------------------------------------------------------------
    auto an = Analyzer(argv[1]);

    // -----------------------------------------------------------------------
    // 2. Register plugins.
    //
    //    Plugin registration order:
    //      a) NDHistogramManager – reads histogramConfig from cfg.yaml
    //      b) CutflowManager    – tracks sequential + N-1 cutflow tables
    //      c) TriggerManager    – reads triggerConfig; applies trigger filter
    //         via applyAllTriggers() before analysis cuts are registered.
    // -----------------------------------------------------------------------
    auto histMgr    = makeNDHistogramManager(an);
    auto cutflowMgr = makeCutflowManager(an);
    auto trigMgr    = makeTriggerManager(an);

    // -----------------------------------------------------------------------
    // 3. Apply trigger selection via TriggerManager.
    //
    //    applyAllTriggers() performs three steps internally:
    //      a) Defines "allTriggersPassVector" (RVec<bool> of each HLT path).
    //      b) Defines "pass_applyTrigger" (bool OR across paths).
    //      c) Applies a filter on "pass_applyTrigger" on the main dataframe.
    //
    //    The trigger filter runs BEFORE CutflowManager cuts so that the
    //    cutflow table reflects the analysis selection starting from the
    //    triggered data sample (standard CMS practice).
    // -----------------------------------------------------------------------
    trigMgr->applyAllTriggers();

    // -----------------------------------------------------------------------
    // 4. Define PhysicsObjectCollections and event-level boolean columns.
    //
    //    IMPORTANT: All boolean columns used by CutflowManager::addCut()
    //    must be defined BEFORE the first addCut() call.  CutflowManager
    //    captures the dataframe state at the first addCut() as the base
    //    node for the N-1 computation.
    // -----------------------------------------------------------------------

    // Tight muon collection built from NanoAOD branches
    an.Define("goodMuons", buildMuonCollection,
        {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass",
         "Muon_tightId", "Muon_pfRelIso04_all"});

    // Event-level cut booleans (evaluated with guards for safety)
    an.Define("hasTwoMuons",     hasExactlyTwoMuons, {"goodMuons"});
    an.Define("hasOppositeSign", hasOppositeSign,    {"goodMuons", "Muon_charge"});
    an.Define("inMassWindow",    inMassWindow,       {"goodMuons"});

    // -----------------------------------------------------------------------
    // 5. Register analysis cuts with CutflowManager.
    //
    //    Each addCut() call:
    //      a) Captures the current dataframe state as the N-1 base for this cut.
    //      b) Applies the boolean column as a filter on the main dataframe.
    //
    //    The cutflow starts from "events passing trigger" as the baseline.
    // -----------------------------------------------------------------------
    cutflowMgr->addCut("DimuonPair",   "hasTwoMuons");
    cfm->addCut("OppositeSign", "hasOppositeSign");
    cfm->addCut("MassWindow",   "inMassWindow");

    // -----------------------------------------------------------------------
    // 6. Derived quantities (computed after all cuts; exactly 2 OS muons
    //    in [70, 110] GeV guaranteed at this point).
    // -----------------------------------------------------------------------

    // Dimuon invariant mass [GeV] via PhysicsObjectCollection makePairs()
    an.Define("DimuonMass", dimuonMass, {"goodMuons"});

    // Individual muon kinematics
    an.Define("LeadMuPt",  leadMuPt,  {"goodMuons"});
    an.Define("SublMuPt",  sublMuPt,  {"goodMuons"});
    an.Define("LeadMuEta", leadMuEta, {"goodMuons"});
    an.Define("SublMuEta", sublMuEta, {"goodMuons"});

    // Pre-selection jet collection (quality + kinematic cuts, no overlap removal)
    an.Define("preJets", buildPreJetCollection,
        {"Jet_jetId", "Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"});

    // Overlap-cleaned jet collection via PhysicsObjectCollection::removeOverlap.
    // Jets within ΔR < 0.4 of any selected tight muon are removed.
    an.Define("goodJets",
        [](const PhysicsObjectCollection &jets,
           const PhysicsObjectCollection &muons) {
            return jets.removeOverlap(muons, 0.4f);
        },
        {"preJets", "goodMuons"});

    // Jet multiplicity after overlap removal
    an.Define("nGoodJets", nGoodJets, {"goodJets"});

    // Per-jet-bin dimuon mass variables for config-driven histogramming.
    // Sentinel -999 for events outside the category; invisible in plots.
    an.Define("DimuonMass_0j",   dimuonMass0j,   {"DimuonMass", "nGoodJets"});
    an.Define("DimuonMass_1j",   dimuonMass1j,   {"DimuonMass", "nGoodJets"});
    an.Define("DimuonMass_2j",   dimuonMass2j,   {"DimuonMass", "nGoodJets"});
    an.Define("DimuonMass_ge3j", dimuonMassGe3j, {"DimuonMass", "nGoodJets"});

    // Constant event weight = 1.0 for data (referenced as "weight" in histograms.yaml)
    an.Define("eventWeight", []() -> Float_t { return 1.0f; }, {});

    // -----------------------------------------------------------------------
    // 7. Book histograms from histograms.yaml (NDHistogramManager).
    //    Called after all Define/Filter operations are complete.
    // -----------------------------------------------------------------------
    an.bookConfigHistograms();

    // -----------------------------------------------------------------------
    // 8. Execute the RDataFrame graph and write all outputs.  Use
    //    run() (not save()) so that the histogram manager gets a chance to
    //    write its histograms to the meta file.  `save()` only writes the
    //    skimmed tree, which was the source of the CI failure when histo
    //    verification was performed.
    //      - Skimmed event tree  → saveFile in cfg.yaml
    //      - Histograms          → histograms/ directory of the meta file
    //      - Cutflow tables      → cutflow, cutflow_nminus1 histograms
    // -----------------------------------------------------------------------
    an.run();

    return 0;
}
