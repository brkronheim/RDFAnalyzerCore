# CMS Standard Correction Stack — User Recipe

This document is a quick-start recipe for applying the standard set of CMS
corrections and weights in an RDFAnalyzerCore analysis.  It describes the
exact call order, flags to enable, output columns produced, and extension
patterns for analysis-specific corrections.

For the full implementation status matrix, see
[`core/plugins/cms/README.md`](../core/plugins/cms/README.md).

---

## Quick Start

```cpp
#include "CmsYearConfig.h"
#include "CmsNanoAodCompat.h"
#include "CmsCorrections.h"
#include "CmsWeights.h"

using namespace cms;

void setupCmsStack(Analyzer &an, const std::string &year, bool isMC) {
  // 1. Year registry
  CmsYearRegistry registry;
  registry.setYear(year);
  auto ys = registry.load();

  // 2. NanoAOD compatibility columns
  defineNanoAodCompatibilityColumns(an);

  // 3. Correction workflow config
  CmsCorrectionConfig corrCfg;
  corrCfg.yearSettings = &ys;
  corrCfg.jetTagger = CmsCorrectionConfig::defaultJetTagger(year);
  corrCfg.applyJetCorrections = true;       // JEC/JER for AK4 jets
  corrCfg.applyMuonCorrections = true;      // Muon scale/resolution
  corrCfg.applyElectronCorrections = true;  // Electron scale/smear
  corrCfg.applyTauCorrections = true;       // Tau energy scale (if using taus)

  // 4. Create framework plugins
  setupCmsFrameworkPlugins(an, corrCfg);

  // 5. Correction inputs + workflow
  defineCmsCorrectionInputs(an, corrCfg);
  configureCmsCorrectionWorkflow(an, corrCfg);
  defineCmsCorrectionFallbacks(an, corrCfg);

  // 6. MET filters + jet ID (always recommended)
  applyCmsMetFilters(an);
  if (year == "2018") {
    corrCfg.applyHemIssueFilter = true;
    applyCmsHemIssueFilter(an, corrCfg);
  }
  applyCmsJetVetoMap(an, corrCfg);
  applyCmsJetId(an, ys, true);

  // 7. Event weights (opt-in per component)
  CmsWeightConfig wCfg;
  wCfg.yearSettings = &ys;
  wCfg.isMC = isMC;
  wCfg.jetTagger = corrCfg.jetTagger;
  wCfg.btagEfficiencyFile = "/path/to/btag_eff.json.gz";  // required for b-tag SFs
  wCfg.applyPileupWeights = true;
  wCfg.applyMuonScaleFactors = true;
  wCfg.applyElectronScaleFactors = true;
  wCfg.applyBTagScaleFactors = true;
  wCfg.applyTriggerScaleFactors = true;     // POG generic trigger SFs

  // 8. Configure and compute all weights
  setupCmsStandardWeights(an, wCfg);
  setupCmsGenWeights(an);                   // LHE, PDF, PS variations

  // 9. Validate configuration
  auto audit = validateCmsCorrectionStack(corrCfg, &wCfg);
  audit.printSummary();                     // Prints any issues found
}
```

---

## Required Input Columns

Your NanoAOD or ntuple must provide the following columns for the standard stack.
These are mostly standard NanoAOD branches; `CmsNanoAodCompat` provides aliases
for version-dependent type differences.

| Category       | Required Columns (NanoAOD)                        | Notes |
|----------------|---------------------------------------------------|-------|
| **Event**      | `run`, `event`, `luminosityBlock`                 | For random seeding |
| **MET**        | `PuppiMET_pt`, `PuppiMET_phi`                     | Run 3; `PFMET_*` also supported |
| **Jets (AK4)** | `Jet_pt`, `Jet_eta`, `Jet_phi`, `Jet_mass`       | Raw columns |
|                | `Jet_area`, `Jet_rawFactor`                       | For JEC |
|                | `Jet_genJetIdx`, `GenJet_pt`                      | For JER matching |
|                | `Jet_hadronFlavour`                               | For b-tag/c-tag SFs |
|                | `Jet_btag*` (tagger-dependent)                    | Discriminant |
| **Electrons**  | `Electron_pt`, `Electron_eta`, `Electron_phi`, `Electron_mass` | |
|                | `Electron_deltaEtaSC`                             | For supercluster η |
| **Muons**      | `Muon_pt`, `Muon_eta`, `Muon_phi`, `Muon_mass`   | |
|                | `Muon_charge`, `Muon_nTrackerLayers`              | For Rochester (Run 2) |
| **Taus**       | `SelectedTau_pt`, `SelectedTau_eta`, ...          | After tau selection |
| **Pileup**     | `Pileup_nTrueInt`                                 | For PU weights |
| **L1 Prefire** | `L1PreFiringWeight_Nom`, `_Up`, `_Dn`            | 2016/2017 only |
| **Gen.**       | `genWeight`, `LHEScaleWeight`, `LHEPdfWeight`, `PSWeight` | For gen weights |
| **Filters**    | `Flag_HBHENoiseFilter`, etc.                      | Standard MET flags |

---

## Enable Flags Reference

### `CmsCorrectionConfig` flags

| Flag | Default | Description |
|------|---------|-------------|
| `applyJetCorrections` | false | JEC/JER for AK4 jets |
| `applyFatJetCorrections` | false | JEC/JER for AK8 fat-jets |
| `applyElectronCorrections` | false | Electron energy scale + smearing |
| `applyMuonCorrections` | false | Muon scale/resolution (Run 3 correctionlib or Run 2 RoccoR) |
| `applyTauCorrections` | false | Tau energy scale |
| `applyBtagCorrections` | false | Create b-tag TaggerWorkingPointManager (required for b-tag SFs) |
| `applyCtagCorrections` | false | Create c-tag TaggerWorkingPointManager (required for c-tag SFs) |
| `applyHemIssueFilter` | false | 2018 HEM issue jet veto |
| `applyMetType1` | false | Propagate the final calibrated jet shift to PuppiMET |
| `rebuildPuppiMetFromRaw` | false | Rebuild Run 3 standard AK4 PUPPI Type-1 MET from raw MET and both jet collections; requires `applyMetType1` |
| `metAllowLegacyInputs` | false | Explicit v12 compatibility for unavailable muon-subtracted directions and low-jet EM fraction; see the CMS plugin README |
| `applyUnclusteredMetSystematics` | true | Propagate PUPPI unclustered vector deltas when rebuilding MET with systematics enabled |
| `jesMode` | Reduced | JES systematic granularity (Total/Reduced/Full) |

### `CmsWeightConfig` flags

| Flag | Default | Description |
|------|---------|-------------|
| `applyPileupWeights` | false | Pileup reweighting |
| `applyMuonScaleFactors` | false | Muon ID + isolation SFs |
| `applyElectronScaleFactors` | false | Electron ID + reconstruction SFs |
| `applyBTagScaleFactors` | false | B-tag fixed-WP or shape SFs |
| `applyCTagScaleFactors` | false | C-tag fixed-WP or shape SFs |
| `applyTriggerScaleFactors` | false | POG generic trigger SFs (muon×electron) |
| `applyTauScaleFactors` | false | Tau ID (VSjet), anti-e (VSe), anti-mu (VSmu) SFs |
| `applyPhotonScaleFactors` | false | Photon ID + CSEV SFs (Run 3 only) |
| `applyJetPuIdScaleFactors` | false | Jet pileup ID efficiency SFs (Run 2 only) |
| `applyL1PrefireWeights` | false | L1 prefiring weights (2016/2017) |
| `applyPileupMinbiasXsec` | false | Pileup min-bias cross-section uncertainty |
| `channels` | empty | Multi-channel SF masks for per-channel weights |

---

## Output Columns

### Correction Columns

| Column | Description | Gated by |
|--------|-------------|----------|
| `Jet_pt_corr_nominal` | JEC/JER-corrected AK4 jet pT | `applyJetCorrections` |
| `Jet_mass_corr_nominal` | JEC/JER-corrected AK4 jet mass | `applyJetCorrections` |
| `Jet_pt_jes_*_up/down` | JES systematic variations | `applyJetCorrections` |
| `Jet_pt_jer_up/down` | JER systematic variations | `applyJetCorrections` |
| `PuppiMET_pt_corr_nominal` | Type-1 MET corrected pT | `applyMetType1` |
| `Electron_pt_corr_nominal` | Scale/smear-corrected electron pT | `applyElectronCorrections` |
| `Muon_pt_corr_nominal` | Scale-corrected muon pT | `applyMuonCorrections` |
| `SelectedTau_pt_corr_nominal` | Energy-scale-corrected tau pT | `applyTauCorrections` |
| `CorrectedJets` | PhysicsObjectCollection of AK4 jets | `applyJetCorrections` |
| `CorrectedElectrons` | PhysicsObjectCollection of electrons | `applyElectronCorrections` |
| `CorrectedMuons` | PhysicsObjectCollection of muons | `applyMuonCorrections` |
| `CorrectedTaus` | PhysicsObjectCollection of taus | `applyTauCorrections` |
| `CorrectedFatJets` | PhysicsObjectCollection of AK8 jets | `applyFatJetCorrections` |
| `hem_veto_pass` | HEM filter pass flag | `applyHemIssueFilter` |
| `met_filter_pass` | Combined MET filter flag | always (if columns exist) |
| `jet_id_pass` | Jet tight ID per-jet flag | always |

### Weight Columns

| Column | Description | Gated by |
|--------|-------------|----------|
| `pileup_weight_nominal/up/down` | Pileup weight | `applyPileupWeights` |
| `l1_prefire_weight_nominal/up/down` | L1 prefiring weight | `applyL1PrefireWeights` |
| `muon_id_weight_nominal/systup/systdown` | Muon ID SF | `applyMuonScaleFactors` |
| `muon_iso_weight_nominal/systup/systdown` | Muon isolation SF | `applyMuonScaleFactors` |
| `electron_id_weight_nominal/up/down` | Electron ID SF | `applyElectronScaleFactors` |
| `electron_reco_weight_nominal/up/down` | Electron reco SF | `applyElectronScaleFactors` |
| `trig_sf_nominal/up/down` | Trigger SF (muon×electron product) | `applyTriggerScaleFactors` |
| `btag_weight_nominal/up/down` | B-tag SF | `applyBTagScaleFactors` |
| `btag_weight_<src>_up/down` | Per-source b-tag variations | `applyBTagScaleFactors` |
| `ctag_weight_nominal/up/down` | C-tag SF | `applyCTagScaleFactors` |
| `tau_id_weight_nominal/up/down` | Tau ID SF | `applyTauScaleFactors` |
| `photon_id_weight_nominal/up/down` | Photon ID SF | `applyPhotonScaleFactors` |
| `photon_csev_weight_nominal/up/down` | Photon CSEV SF | `applyPhotonScaleFactors` |
| `pu_id_weight_nominal/up/down` | Jet PU ID SF | `applyJetPuIdScaleFactors` |
| `pileup_mb_xsec_weight_up/down` | PU min-bias xsec | `applyPileupMinbiasXsec` |
| `weight_nominal` | Combined event weight | isMC |
| `weight_nominal_<channel>` | Per-channel weight | `channels` configured |

### Generator Weight Columns

Produced by `setupCmsGenWeights()`.

| Column Pattern | Description |
|----------------|-------------|
| `genWeight_nominal` | Normalized gen weight |
| `genWeight_lhe_<muR_muF>` | 8 LHE scale variations |
| `genWeight_ps_<isr/fsr>_<up/down>` | 4 parton shower variations |
| `genWeight_pdf_<index>` | PDF member variations (103 NNPDF members) |
| `genWeight_pdfAs_up/down` | PDF αₛ variations |

---

## Per-Channel Weight Composition

When `CmsWeightConfig::channels` is configured, `defineCmsNominalWeight()` creates
per-channel nominal and systematically-varied weight columns:

```cpp
CmsWeightConfig wCfg;
wCfg.channels["muon"] = {
  .applyMuonSFs = true,
  .applyElectronSFs = false,
  .applyBTagSFs = true,
  // ... all other flags false
};
wCfg.channels["electron"] = {
  .applyMuonSFs = false,
  .applyElectronSFs = true,
  .applyBTagSFs = true,
  // ...
};
```

Produces:
- `weight_nominal_muon` = genWeight × pileup × muon_id × muon_iso × trigger × btag × ...
- `weight_nominal_electron` = genWeight × pileup × electron_id × electron_reco × trigger × btag × ...
- Per-channel systematic variants: `weight_nominal_muon_pileupUp`, etc.

---

## Validation

Call `validateCmsCorrectionStack()` after configuring your flags to check that
requested payloads are available for the configured year:

```cpp
auto audit = validateCmsCorrectionStack(corrCfg, &wCfg);
audit.print();           // Full report
audit.printSummary();    // One-line summary
if (!audit.empty()) {
  // Handle missing payloads
}
```

The audit records each missing payload with:
- **category**: e.g. `"jec"`, `"tau_tes"`, `"btag"`
- **reason**: e.g. `"payload_not_found"`, `"correction_name_missing"`
- **detail**: Human-readable explanation
- **remediation**: Suggested fix

---

## Analysis-Specific Extensions

### 1. Trigger Matching SFs

The generic POG trigger SFs (muon×electron product) are suitable for inclusive
single-lepton or dilepton analyses.  For path-specific or channel-specific
trigger SFs, provide precomputed columns:

```cpp
// In your analysis code, define trigger SF columns:
an.Define("my_trig_sf_nominal", ...);
an.Define("my_trig_sf_up", ...);
an.Define("my_trig_sf_down", ...);

// Then override in CmsWeightConfig:
wCfg.usePogTriggerSFs = false;  // Skip POG generic SFs
wCfg.triggerSfRoot = "/path/to/analysis/trigger/sf";
```

### 2. Top-pT Reweighting

Not a central CMS payload.  Use a histogram-based helper:

```cpp
// Define top-pT weight using a TH2 histogram
an.Define("top_pt_weight", [](float pt) { return myTopPtWeight(pt); },
          {"GenTop_pt"});
// Then multiply into nominal weight manually or via WeightManager
```

### 3. DY Z-pT Reweighting

Analysis-specific, similar to top-pT:

```cpp
an.Define("dy_zpt_weight", [](float zPt, int genVnloId) { ... },
          {"GenZ_pt", "genVnloId"});
```

### 4. V+jets Stitching

Uses generator-level bin information.  Typically done via `genBinWeight` from
the generator ntuple and multiplied into the nominal weight.

### 5. STXS Reweighting

Use `StxsManager` for STXS cross-section splitting and reweighting.
The STXS stage-1.2 binning is configured via `stxsConfig`.

### 6. Boosted-Tag SFs (Fat Jets)

Not centrally available in CAT CVMFS as of 2026.  When payloads become
available, use the reserved `ChannelMask::applyBoostedTagSFs` flag and
follow the b-tag SF pattern.  See [`core/plugins/cms/README.md`](../core/plugins/cms/README.md)
for the current status.

### 7. Custom Correctionlib Payloads

Use `CorrectionManager` directly to register and evaluate any correctionlib JSON:

```cpp
auto cm = an.getPlugin<CorrectionManager>("correctionManager");
cm->registerCorrection("my_corr", "/path/to/my.json.gz", "MyCorrection",
                       {"Jet_pt", "Jet_eta"});
cm->applyCorrectionVec("my_corr", {"nominal"}, {}, "my_corr_nominal_vec");
```

---

## Central Payload Availability

| Category | 2016-2018 (UL) | 2022-2023 | 2024 | 2025 |
|----------|---------------|-----------|------|------|
| JEC/JER (AK4) | ✅ | ✅ | ✅ | ✅ |
| JEC/JER (AK8) | ✅ | ✅ | ✅ | ✅ |
| Electron scale/smear | ❌ (MiniAOD) | ✅ | ✅ | ✅ |
| Muon scale/resolution | ❌ (RoccoR) | ✅ | ✅ | ✅ |
| Electron ID/Reco SFs | ✅ | ✅ | ✅ | ✅ |
| Muon ID/ISO SFs | ✅ | ✅ | ✅ | ✅ |
| Tau ID SFs | ✅ | ✅ | ✅ | ❌ |
| Tau energy scale | ✅ | ✅ | ✅ | ❌ |
| Photon ID/CSEV SFs | ❌ | ✅ | ✅ | ✅ |
| B-tag SFs (fixed-WP) | ✅ | ✅ | ✅ | ✅ |
| B-tag SFs (shape) | ✅ | ✅ | ❌ | ❌ |
| C-tag SFs (fixed-WP) | ✅ | ✅ | ❌ | ❌ |
| C-tag SFs (shape) | ✅ | ✅ | ❌ | ❌ |
| Pileup weights | ✅ | ✅ | ✅ | ✅ |
| L1 prefiring | ✅ (2016/2017) | ❌ | ❌ | ❌ |
| Jet PU ID SFs | ✅ | ❌ | ❌ | ❌ |
| RoccoR (Run 2 muons) | ✅ (extract from .tgz) | N/A | N/A | N/A |
| Golden JSON | ✅ | ✅ | ✅ | ✅ |

✅ = Available in CAT CVMFS
❌ = Not available (or not applicable)

---

## See Also

- [`core/plugins/cms/README.md`](../core/plugins/cms/README.md) — implementation status matrix
- [`docs/CMS_CORRECTIONS.md`](CMS_CORRECTIONS.md) — correction manager patterns
- [`docs/JET_ENERGY_CORRECTIONS.md`](JET_ENERGY_CORRECTIONS.md) — JEC/JER reference
- [`docs/API_REFERENCE.md`](API_REFERENCE.md) — API reference
- [`docs/CONFIG_REFERENCE.md`](CONFIG_REFERENCE.md) — configuration format
