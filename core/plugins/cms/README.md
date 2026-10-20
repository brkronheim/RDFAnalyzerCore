# CMS Analysis Infrastructure (`core/plugins/cms/`)

This directory provides a **generic, configurable CMS analysis infrastructure** for
building physics analyses with RDFAnalyzerCore.  It is extracted and generalized
from the VHbb STXS analysis in `analyses/VHbbSTXS/`.

## Components

| Component | File(s) | Purpose |
|-----------|---------|---------|
| **CmsYearConfig** | `CmsYearConfig.h/.cc` | Year registry: default correction payload paths for 2016–2024, year alias normalization, CVMFS path resolution with `{version}` token substitution, YAML override support |
| **CmsNanoAodCompat** | `CmsNanoAodCompat.h/.cc` | NanoAOD version compatibility shims: uniform `_u32`, `_int`, `_i` type aliases for columns that change type across NanoAOD versions |
| **CmsCorrections** | `CmsCorrections.h/.cc` | Correction workflow setup: plugin factory, JEC/JER/electron/muon workflow config, fallback identity columns |
| **CmsWeights** | `CmsWeights.h/.cc` | Weight pattern helpers: pileup, muon/electron SFs, b-tag/c-tag SFs, trigger SFs, nominal weight composition |

## Design

### Layered Override Pattern

```
Built-in C++ defaults (hardcoded per year)
    ↓
YAML registry file (optional, overrides built-in)
    ↓
Programmatic overrides (C++ override() calls)
    ↓
Inline config map (from Analyzer YAML config keys)
```

A user can:
1. **Do nothing** — get sensible CMS defaults for their year
2. **Override individual paths** via `CmsYearRegistry.override("pileupFile", "/my/path.json")`
3. **Provide a full YAML registry** via `CmsYearRegistry.loadRegistryFile("my_registry.yaml")`
4. **Override via config keys** in their analysis YAML (e.g. `pileupFile: /my/path.json`)

### Usage Example

```cpp
#include "CmsYearConfig.h"
#include "CmsNanoAodCompat.h"
#include "CmsCorrections.h"
#include "CmsWeights.h"

using namespace cms;

int main(int argc, char **argv) {
    Analyzer an(argv[1]);

    // 1. Setup year registry
    CmsYearRegistry registry;
    registry.setYear("2022_preEE");
    auto yearSettings = registry.load();

    // 2. NanoAOD compatibility
    defineNanoAodCompatibilityColumns(an);

    // 3. Framework plugins (opt in to what you need)
    CmsCorrectionConfig corrCfg;
    corrCfg.yearSettings = &yearSettings;
    corrCfg.jetTagger = "PNet";
    corrCfg.jesMode = CmsCorrectionConfig::Full;
    corrCfg.applyJetCorrections = true;
    corrCfg.applyElectronCorrections = true;
    corrCfg.applyMuonCorrections = true;
    setupCmsFrameworkPlugins(an, corrCfg);

    // 4. Correction workflow
    defineCmsCorrectionInputs(an, corrCfg);
    configureCmsCorrectionWorkflow(an, corrCfg);
    defineCmsCorrectionFallbacks(an, corrCfg);

    // 5. Object selection, etc. (analysis-specific)

    // 6. Weights (opt in to what you need)
    CmsWeightConfig weightCfg;
    weightCfg.yearSettings = &yearSettings;
    weightCfg.isMC = true;
    weightCfg.btagEfficiencyFile = "path/to/btag_eff.json";
    weightCfg.applyPileupWeights = true;
    weightCfg.applyMuonScaleFactors = true;
    weightCfg.applyElectronScaleFactors = true;
    weightCfg.applyBTagScaleFactors = true;
    weightCfg.applyTriggerScaleFactors = true;
    setupCmsStandardWeights(an, weightCfg);

    // 7. Run
    an.run();
}
```

## Electron payload contracts

Electron SF helpers require index-aligned `SelectedElectron_scEta` and
`SelectedElectron_pt` vectors. They inspect the payload schema and additionally
consume `SelectedElectron_phi` when it declares phi, as the 2023 payloads do.
Year, variation, and working-point categories are checked during setup so an
invalid configuration fails before processing events.

| Correction era | Electron SF year token |
| --- | --- |
| 2022 pre-EE | `2022Re-recoBCD` |
| 2022 post-EE | `2022Re-recoE+PromptFG` |
| 2023 pre-BPix | `2023PromptC` |
| 2023 post-BPix | `2023PromptD` |
| 2024 | `2024Prompt` |

Run 2 reconstruction uses `RecoBelow20` and `RecoAbove20`. Run 3 uses
`RecoBelow20`, `Reco20to75`, and `RecoAbove75`; exactly 75 GeV selects
`RecoAbove75`. Existing per-bin output column names remain available in Run 2,
with both columns above 20 GeV evaluated using `RecoAbove20`.

`electronScaleSmearingEtDependent` selects the Et-dependent scale/smear inputs
and categories when true, or the standard `total_correction`, `rho`, and
`err_rho` recipe when false. Override the payload file and correction names
together to match the selected recipe. Standard smearing uncertainty shifts the
width by `err_rho`, with the down width bounded at zero; both recipes reuse the
same Gaussian draw for nominal and varied momenta.

## Quick Start

For a standard CMS analysis, explicitly enable each correction and weight
component that your analysis needs.  Nothing is on by default.

```cpp
#include "CmsYearConfig.h"
#include "CmsCorrections.h"
#include "CmsWeights.h"

using namespace cms;

void setupAnalysis(Analyzer &an, const std::string &year, bool isMC) {
  CmsYearRegistry registry;
  registry.setYear(year);
  auto yearSettings = registry.load();

  // --- Object corrections (opt in to what you need) ---
  CmsCorrectionConfig corrCfg;
  corrCfg.yearSettings = &yearSettings;
  corrCfg.jetTagger = CmsCorrectionConfig::defaultJetTagger(year);
  corrCfg.applyJetCorrections = true;
  corrCfg.applyElectronCorrections = true;   // if using electrons
  corrCfg.applyMuonCorrections = true;       // if using muons
  setupCmsFrameworkPlugins(an, corrCfg);

  defineNanoAodCompatibilityColumns(an);
  defineCmsCorrectionInputs(an, corrCfg);
  configureCmsCorrectionWorkflow(an, corrCfg);
  defineCmsCorrectionFallbacks(an, corrCfg);

  applyCmsMetFilters(an);
  applyCmsMetXYShift(an, yearSettings, "PuppiMET_pt", "PuppiMET_phi",
                     "PuppiMET_pt_corr", "PuppiMET_phi_corr", isMC);
  if (year == "2018") { corrCfg.applyHemIssueFilter = true;
    applyCmsHemIssueFilter(an, corrCfg); }
  applyCmsJetVetoMap(an, corrCfg);
  applyCmsJetId(an, yearSettings, true);

  // --- Event weights (opt in to what you need) ---
  CmsWeightConfig weightCfg;
  weightCfg.yearSettings = &yearSettings;
  weightCfg.isMC = isMC;
  weightCfg.jetTagger = corrCfg.jetTagger;
  weightCfg.applyPileupWeights = true;
  weightCfg.applyMuonScaleFactors = true;     // if using muons
  weightCfg.applyElectronScaleFactors = true; // if using electrons
  weightCfg.applyBTagScaleFactors = true;     // b-tag SFs
  weightCfg.applyCTagScaleFactors = true;     // c-tag SFs (optional)
  weightCfg.applyTriggerScaleFactors = true;  // trigger SFs (if applicable)
  setupCmsStandardWeights(an, weightCfg);
  setupCmsGenWeights(an);
}
```

## Current Status

### Fully Implemented (✅)

- ✅ **Year registry** — built-in defaults for 10 eras (2016_PreVFP through 2025; 2026 aliased to 2025), year alias normalization, CVMFS path resolution with `{version}` token, YAML registry file loading, programmatic and config-map overrides
- ✅ **NanoAOD compatibility columns** — `PV_npvsGood_u32`, `Jet_genJetIdx_int`, `FatJet_genJetIdx_int`, `Jet_jetId_i`, `Jet_puId_i`, `Jet_pt_raw`, `Jet_mass_raw`
- ✅ **Framework plugin setup** — all managers created: `CorrectionManager`, `WeightManager`, `GenWeightManager`, `TaggerWorkingPointManager × 2`, `JetEnergyScaleManager` (AK4 + AK8 FatJet), `ElectronEnergyScaleManager`, `MuonRochesterManager`, `TauEnergyScaleManager` (gated by `applyTauCorrections`), `CorrectedCollectionManagers × 4` (Jets, FatJets, Electrons, Muons, Taus — tau gated by `applyTauCorrections`), `GoldenJsonManager`, `JetVetoMapManager`
- ✅ **Correction input columns** (`Electron_scEta`, `Jet_genMatchedPt`, `FatJet_genMatchedPt`), workflow config with JES mode selection (Total/Reduced/Full), and fallback identity columns
- ✅ **Pileup weights** — via `defineCmsPileupWeights()` with up/down systematics
- ✅ **L1 prefiring weights** — reads NanoAOD `L1PreFiringWeight_*` branches for 2016/2017, unity for later years
- ✅ **Muon ID + isolation SF** — custom TightMiniIso or POG TightPFIso fallback, systup/systdown
- ✅ **Electron ID + reco SF** — Run 2 bins (<20, ≥20 GeV), Run 3 bins (<20, [20,75), ≥75 GeV), up/down
- ✅ **Tau ID SFs** — `defineCmsTauScaleFactors()`: DeepTau2018v2p5 VSjet/VSe/VSmu at Medium WP, nom/up/down. Run 2 UL and Run 3 2022–2024
- ✅ **Photon ID SFs** — `defineCmsPhotonScaleFactors()`: Photon-ID-SF, CSEV, PixVeto at Medium WP. Run 3 2022–2025
- ✅ **b-tag fixed-WP weight computation** — via `WorkingPointRecipe::defineFixedWorkingPointVariation()`, multiple WPs (L/M/T/XT/XXT), full systematic coverage (9 sources + JES)
- ✅ **c-tag fixed-WP weight computation** — light/WC/TnP per flavour, Run 2 legacy naming fallback (`deepJet_wp`), 2+2+13 sources
- ✅ **b-tag shape calibration** — via `btagMethod=Shape`: hf/lf/hfstats1/2/lfstats1/2/cferr1/2 + JES
- ✅ **c-tag shape calibration** — via `ctagMethod=Shape`: 13 sources (incl. Stat decorrelated)
- ✅ **Trigger SFs (POG generic)** — per-object `Muon_triggerSF` / `Electron_triggerSF`, combined as muon×electron product
- ✅ **GenWeightManager** — LHE scale (8 vars), PDF (NNPDF, 103 members), PDF αₛ (up/down via `definePdfAsWeights()`), PS (4 vars) — wired into framework plugins
- ✅ **MET XY-shift corrections** — `applyCmsMetXYShift()` with per-era Run 2 UL parameters embedded from JME POG. Supports PFMET and PuppiMET, era auto-detected from run number
- ✅ **MET filters (event cleaning)** — `applyCmsMetFilters()`: all 9 standard flags (HBHE noise, CSCTightHalo, goodVertices, eeBadSc, ECAL dead cell, bad PF muon/charged candidate, ecal bad calib), AND logic
- ✅ **HEM issue filter** (2018) — jet-veto in HEM-damaged region with lumi-weighted MC selection
- ✅ **JetVetoMap integration** — auto-configures from JME file path
- ✅ **Jet ID (tight)** — `applyCmsJetId()` with era-dependent criteria from bundled `JetID.txt`
- ✅ **Run 2 muon Rochester (RoccoR)** — `MuonRochesterManager` with `applyRoccoRCorrection()`, auto-discovers kFactor files from `roccor_data/`. Bundled `roccor.Run2.v5.tgz`
- ✅ **FatJet (AK8) JEC/JER in standard workflow** — `fatJetEnergyScaleManager` and `CorrectedFatJetCollectionManager` integrated into `configureCmsCorrectionWorkflow()` with programmatic JES uncertainty registration, systematic sources, and JER smearing. Enabled via `CmsCorrectionConfig::applyFatJetCorrections` (default: `true`)
- ✅ **Golden JSON filter** — under `cms/`
- ✅ **TriggerManager** — trigger group management, sample-to-group mapping, config-driven trigger application
- ✅ **ObjectEnergyManagerBase** — Type-1 MET propagation via `propagateMET()`
- ✅ **Year-dependent tagger defaults** — `CmsCorrectionConfig::defaultJetTagger()` returns `"DeepFlav"` (Run 2 UL), `"PNet"` (Run 3 2022–2023), `"UParTAK4"` (2024+). Branch resolution for all taggers
- ✅ **Helper utilities** — `mergeBTagScaleFactors`, `mergeCTagScaleFactors`, `selectElectronRecoScaleFactors`, `productOrOne`, `safeWeightRatio`, `triggerTagForYear`, `ctagCorrectionName`, `jetTaggerBase`
- ✅ **Per-vector SF product columns** — `defineCmsVecProductColumns()` for muon_id, muon_iso, electron_id, electron_reco
- ✅ **Nominal weight composition** — `defineCmsNominalWeight()`: genWeight × pileup × l1_prefire × muon_id/iso × electron_id/reco × trigger × btag × ctag × tau_id × photon_id × pu_id, with full systematic variations
- ✅ **Weight component enable flags** — `CmsWeightConfig` has per-component flags (`applyPileupWeights`, `applyMuonScaleFactors`, `applyElectronScaleFactors`, `applyBTagScaleFactors`, etc.) to skip computation entirely for unused object types. `setupCmsFrameworkPlugins()` and `defineCmsCorrectionInputs()` also accept `CmsCorrectionConfig` to gate manager creation and input columns.
- ✅ **Multi-channel SF masks** — per-channel weight composition (`weight_nominal_muon`, `weight_nominal_electron`, `weight_nominal_tau`). b-tag and c-tag scale factors included in all per-channel weights. `ChannelMask` flags `applyBTagSFs` and `applyCTagSFs` control per-channel activation.
- ✅ **Per-channel systematic variations** — when `cfg.channels` is configured, `defineCmsNominalWeight()` now creates per-channel varied weight columns (`weight_nominal_muon_pileupUp/Down`, etc.) that only include systematics for components active in that channel. Columns are auto-registered with `SystematicManager` for discovery by `NDHistogramManager`.
- ✅ **Pileup min-bias xsec uncertainty** — `defineCmsPileupMinbiasXsecWeights()` evaluates additional pileup weight variations for min-bias cross section uncertainty. Enabled via `CmsWeightConfig::applyPileupMinbiasXsec`.
- ✅ **MET Type-1 corrections** — `configureCmsCorrectionWorkflow()` propagates the change from NanoAOD jets to fully calibrated nominal JEC/JER jet energies, plus JES variations, via `JetEnergyScaleManager::propagateMET()`. Produces `PuppiMET_pt_corr_nominal` / `PuppiMET_phi_corr_nominal`. Enabled via `CmsCorrectionConfig::applyMetType1`.
- ✅ **FatJet raw pT/mass** — `CmsNanoAodCompat` defines `FatJet_pt_raw` and `FatJet_mass_raw` from `FatJet_rawFactor`.
- ✅ **Muon genPt derivation** — `CmsNanoAodCompat` derives `Muon_genPt` from `Muon_genPartIdx` + `GenPart_pt` for Rochester corrections.
- ✅ **Photon CSEV SFs in weight** — `defineCmsPhotonScaleFactors()` now produces `photon_csev_weight_nominal/up/down` and includes them in `defineCmsNominalWeight()` under the `applyPhotonSFs` flag.
- ✅ **Year-specific trigger SF names** — Muon trigger SF uses correct correction names per era (`IsoMu24`/`IsoMu27`/etc.) instead of the ID SF name.
- ✅ **Unit tests** — `CmsInfrastructureTest` is registered with the framework CTest suite and covers the implemented year registry, NanoAOD compatibility, corrections, and weights APIs.
- ✅ **Tau energy scale** — TauEnergyScaleManager and CorrectedTauCollectionManager gated by `applyTauCorrections` flag. Configured in `configureCmsCorrectionWorkflow()` with correction name from `yearSettings->tauEnergyScaleCorrection` (default `"tau_energy_scale"`). Fallback identity columns produced when disabled.
- ✅ **Correction stack validation** — `validateCmsCorrectionStack()` checks each enabled flag against payload availability for the configured year. `CmsCorrectionAudit` struct records category/reason/detail/remediation for every unavailable component. Can be called before the event loop to catch misconfigurations early.

### Partially Implemented (🔶)

- 🔶 **2024+ c-tag corrections**: BTV payloads for 2024+ lack `_light/_wc/_tnp` entries (only `UParTAK4_wp_values`). Falls back to unity weights with info message.
- 🔶 **Electron scale/smear for Run 2**: `electronScaleSmearingFile` is empty for 2016–2018 UL — corrections were embedded in MiniAOD, not available as correctionlib JSONs.
- ✅ **Tau energy scale**: TauEnergyScaleManager and CorrectedTauCollectionManager created in plugin setup (gated by `applyTauCorrections` flag). Tau energy scale correction configured in `configureCmsCorrectionWorkflow()` from `yearSettings->tauEnergyScaleCorrection` (defaults to `"tau_energy_scale"`). CorrectedTaus collection with `SelectedTau_pt_corr_nominal` / `SelectedTau_mass_corr_nominal` columns.
- 🔶 **Boosted jet (AK8) tagging SFs**: GloParT and boosted X→bb/cc scale factors not yet available in CAT CVMFS. Infrastructure for custom boosted SF payloads under development.
- 🔶 **Shape-based b-tag/c-tag for 2024+**: Correction names like `particleNet_shape` exist for Run 2 UL and Run 3 2022–2023, but not for 2024+ (UParTAK4).

## Missing Features & Next Steps

This section lists every correction or feature not yet implemented in the generic module.
Items are ordered by priority.  Features that depend on external resources that cannot be
bundled with the repo (e.g. parameters that must be extracted from CMS POG TWiki pages) are
marked with a note — the preference is to bundle everything possible with the repo (see the
`.txt` metadata files in this directory for examples like `MET-xy.txt`, `JetID.txt`, etc.).

### Legend

| Priority | Meaning |
|----------|---------|
| **P1** | Commonly used in ≥2 physics channels. Blocks realistic analysis. |
| **P2** | Used in one specific channel or for precision measurements. |
| **P3** | Rarely needed or only for very specific analyses. |

### Next-Up Features

#### 1. Shape-based b-tag/c-tag for 2024+ (UParTAK4) 🔶 P2
| | |
|---|---|
| **What's missing** | Continuous `UParTAK4_shape` corrections are not available in the currently installed 2024 BTV payload; it exposes working-point corrections only. |
| **External inputs needed** | A BTV shape payload containing the configured continuous correction, supplied through `btagShapeFile` when it is separate from the era payload. |
| **Difficulty** | Low (once payloads exist) |
| **Approach** | Select `btagMethod=Shape` and fail fast when the requested continuous correction is absent; fixed-WP event counting remains a separate explicit mode. |

#### 2. ~~FatJet (AK8) energy corrections into standard workflow~~ ✅ Completed
| | |
|---|---|
| **Status** | Fully integrated into `configureCmsCorrectionWorkflow()` with JES uncertainty registration, systematic sources, and JER smearing. `applyFatJetCorrections` defaults to `true`. |

#### 3. Boosted jet (X→bb/cc) tagging SFs 🔶 P2
| | |
|---|---|
| **What's missing** | GloParT and boosted X→bb/cc scale factors not in standard CAT BTV payloads. Infrastructure for custom boosted SF payloads under development. |
| **External inputs needed** | Boosted-specific correction payloads from BTV POG (not yet in CAT CVMFS). |
| **Difficulty** | Medium (3–4 days once payloads exist) |
| **Approach** | Add `defineCmsBoostedTagScaleFactors()` following pattern of `defineCmsBTagScaleFactors()` once payloads are available. |

#### 4. ~~Jet pileup ID efficiency SFs~~ ✅ Completed
| | |
|---|---|
| **Status** | Fully implemented in `defineCmsJetPuIdScaleFactors()`. |
| **What it does** | Reads `Jet_puId_i` to determine which jets pass the selected PU ID working point (L/M/T, configurable via `cfg.puIdWorkingPoint`), then evaluates the correction `pileupJetId_sf` from the JME JSON file (or from `ys.puIdFile` if set). The per-jet SF is masked: SF=1 for jets not passing the WP or with pT≥50 GeV. Event weight = product of per-jet masked SFs. Systematic variations: nominal, up, down. |
| **Correction file** | Defaults to `ys.jmeFile` (which is the `jet_jerc.json.gz` from CAT CVMFS, already configured for all eras). Override via `ys.puIdFile` or the `puIdFile` config key. |
| **Correction name** | Default `"pileupJetId_sf"`, override via `ys.puIdCorrection`. |
| **Run 3 support** | PU ID SFs are still WIP for Run 3 (per JME POG). When no payload is found, weights default to unity with an info message. |
| **Configuration** | `cfg.puIdWorkingPoint = "M"` (medium) by default. `ChannelMask::applyJetPuIdSFs` controls per-channel activation. |
| **Bit mapping** | Handles the UL16 bit flip automatically (UL16: loose=bit0, medium=bit1, tight=bit2; UL17/18+: loose=bit2, medium=bit1, tight=bit0). |

#### 5. ~~Tau energy scale in nominal weight~~ ✅ Completed
| | |
|---|---|
| **Status** | `TauEnergyScaleManager` and `CorrectedTauCollectionManager` created in plugin setup (gated by `CmsCorrectionConfig::applyTauCorrections`). Tau energy scale correction configured via `configureCmsCorrectionWorkflow()` using `yearSettings->tauEnergyScaleCorrection` (defaults to `"tau_energy_scale"` from the TAU POG JSON). Produces `SelectedTau_pt_corr_nominal` / `SelectedTau_mass_corr_nominal` columns. Fallback identity columns gated on `!applyTauCorrections`. Tau energy scale SFs are separate from tau ID SFs (handled in `CmsWeights`). |

#### 6. ~~Per-channel systematic variations~~ ✅ Completed
| | |
|---|---|
| **Status** | `defineCmsNominalWeight()` now builds a systematic variation registry and creates per-channel varied weight columns (`weight_nominal_muon_pileupUp/Down`, etc.) for every systematic whose underlying SF component is active in the channel. Columns are registered with `SystematicManager` via `registerVariationColumns()` so that `NDHistogramManager` auto-discovers them. Naming convention `baseVar_systNameUp/Down` matches what `SystematicManager::autoRegisterSystematics()` expects. |

#### 7. `ChannelMask::applyBoostedTagSFs` / `applyFatJetSFs` flags unused 🔶 P3
| | |
|---|---|
| **What's missing** | `ChannelMask` defines `applyBoostedTagSFs` and `applyFatJetSFs` fields, but they are never checked in any weight composition or correction code. These are placeholders for future boosted-tag and fat-jet SF support (see items 3 and 4 above). |
| **Difficulty** | Trivial — remove or leave as reserved. |
| **Approach** | Leave as reserved fields for now; remove them if the ChannelMask struct is ever refactored. |

### Deferred / Analysis-Specific Features

#### 8. Electron HLT trigger SFs (per-path) 🔶 P2
| | |
|---|---|
| **What's missing** | Path-specific trigger SFs (e.g. `HLT_Ele32_WPTight_Gsf`). Generic POG `Electron_triggerSF` already implemented. |
| **External inputs needed** | Per-path knowledge of which leptons are "triggering" — intrinsically analysis-specific. CAT CVMFS `EGM/.../electron.json.gz` contains per-path SFs. |
| **Difficulty** | Medium — requires per-path trigger matching logic. |
| **Approach** | Extend `defineCmsTriggerScaleFactors` to support per-path triggers from config. |

#### 9. Muon HLT trigger SFs (per-path) 🔶 P2
| | |
|---|---|
| **What's missing** | Path-specific muon trigger SFs (e.g. `HLT_IsoMu24`). Generic POG `Muon_triggerSF` already implemented. |
| **External inputs needed** | Analysis-specific trigger combination logic. CAT CVMFS `MUO/.../muon_Z.json.gz` contains per-path SFs. |
| **Difficulty** | Medium — analysis-specific trigger matching (single/double muon). |
| **Approach** | Extend `defineCmsTriggerScaleFactors` per-config path. |

#### 10. Top pT reweighting 🔶 P2
| | |
|---|---|
| **What's missing** | Event-level weight for top pT in ttbar/ttH MC. Needs per-year parameters from TOP POG. |
| **External inputs needed** | **Requires external input** — per-year a,b,c,d parameters from TOP POG recommendations. Cannot be bundled without POG documentation. |
| **Difficulty** | Low — simple Define() column from gen top pT. |
| **Approach** | Add `defineCmsTopPtWeight()` in `CmsWeights` once parameters are confirmed. |

#### 11. Drell-Yan Z pT reweighting 🔶 P3
| | |
|---|---|
| **What's missing** | Z pT event weight for DY+jets MC, used in Higgs measurements (VBF categories). |
| **External inputs needed** | **Requires external input** — typically a 1D histogram or ratio from data/MC comparison in a DY control region. Analysis-specific. |
| **Difficulty** | Low — simple Define() using gen Z pT. |
| **Approach** | Add as a configurable function that loads a histogram from a ROOT file. |

#### 12. Trigger SF per-channel combinations 🔶 P3
| | |
|---|---|
| **What's missing** | Channel-level combinations: μμ di-muon SF, ee di-electron SF, μe cross-trigger SF. |
| **External inputs needed** | **Intrinsically analysis-specific** — depends on which leptons are triggering. |
| **Difficulty** | High — analysis-specific. |
| **Approach** | Leave to analysis code via `cfg.triggerSfRoot` override. |

### Long-Term / Nice-to-Have

| # | Item | Notes |
|---|------|-------|
| 14 | **Embedding weights** | Only for τ-channel analyses. Requires external inputs. |
| 15 | **2026+ year registry entries** | Wait for CAT CVMFS paths. 2026 currently aliased to 2025 defaults for early access. |
| 16 | **Expand unit test coverage** | Current: 30+ tests. Cover more edge cases. |
| 17 | **VHbb STXS migration** | Wait until module is stable. |

## Configuration Reference

### Opt-In Model

**Everything defaults to OFF.** You must explicitly enable each correction and
weight component that your analysis needs. This ensures no hidden or unexpected
behaviour — the pipeline only computes what you ask for.

At minimum, every analysis needs:

```cpp
CmsCorrectionConfig corrCfg;
corrCfg.applyJetCorrections = true;       // AK4 JEC/JER

CmsWeightConfig weightCfg;
weightCfg.applyPileupWeights = true;       // pileup reweighting
weightCfg.applyMuonScaleFactors = true;    // if your analysis uses muons
// ... enable any other components you need

setupCmsFrameworkPlugins(an, corrCfg);
setupCmsStandardWeights(an, weightCfg);
```

### Weight Component Enable Flags (`CmsWeightConfig`)

| Flag | Default | Controls |
|------|---------|----------|
| `applyPileupWeights` | `false` | Pileup reweighting (`defineCmsPileupWeights`) |
| `applyL1PrefireWeights` | `false` | L1 prefiring weights (`defineCmsL1PrefireWeights`) |
| `applyMuonScaleFactors` | `false` | Muon ID/ISO SFs (`defineCmsMuonScaleFactors`) |
| `applyElectronScaleFactors` | `false` | Electron ID/reco SFs (`defineCmsElectronScaleFactors`) |
| `applyBTagScaleFactors` | `false` | b-tag SF weight component |
| `applyCTagScaleFactors` | `false` | c-tag SF weight component |
| `applyTriggerScaleFactors` | `false` | Trigger SFs (`defineCmsTriggerScaleFactors`) |
| `applyTauScaleFactors` | `false` | Tau ID SFs (`defineCmsTauScaleFactors`) |
| `applyPhotonScaleFactors` | `false` | Photon ID SFs (`defineCmsPhotonScaleFactors`) |
| `applyJetPuIdScaleFactors` | `false` | Jet PU ID efficiency SFs (`defineCmsJetPuIdScaleFactors`) |

Set any flag to `true` to enable the corresponding weight computation. The
corresponding `ChannelMask` flags must also be set to `true` for each channel
that should include the component.

### Raw Puppi MET rebuild for 2024 NanoAODv15

`applyMetType1 = true` with `rebuildPuppiMetFromRaw = true` rebuilds Puppi
Type-1 MET from `RawPuppiMET`, rather than adding a jet difference to the
stored Type-1 MET. It currently requires standard 2024 AK4 PUPPI JEC/JER and
rejects regressed raw-jet inputs. The default remains `false` for existing
analysis callers.

The rebuild includes both `Jet` and `CorrT1METJet`, including low-pT jets that
cross the corrected 15 GeV threshold. It uses muon-subtracted directions and
momenta, excludes EM fractions above 0.9, and applies nominal JER on MC.
Missing raw-MET, muon-subtraction, low-jet, or MC gen-matching inputs throw
instead of silently returning the stored MET. The main and low-jet JES/JER
variations share their source names. MC matching checks delta-R below 0.2
for AK4 and below 0.4 for AK8, with the resolution window applied in smearing.

Optional `metMuonMaskColumn` and `metElectronMaskColumn` specify fixed boolean
vectors in original NanoAOD order. Their objects' corrected-minus-original
transverse momenta are subtracted after the jet update. Keep these masks fixed
across variations so a changing object set does not introduce spurious shifts.
With systematics enabled, lepton variations and the stored NanoAOD unclustered
up/down vector differences reach `PuppiMET_pt_corr_nominal` and
`PuppiMET_phi_corr_nominal` through the normal variation mappings. Unclustered
branches are required in that mode. PF MET and MET covariance are unchanged.

The jet sum follows the PUPPI no-L1-offset path in the
[CMSSW MET producer](https://github.com/cms-sw/cmssw/blob/CMSSW_15_0_X/JetMETCorrections/Type1MET/interface/PFJetMETcorrInputProducerT.h)
and the full-jet correction factor applied to the muon-subtracted p4 in
[PATJetCorrExtractor](https://github.com/cms-sw/cmssw/blob/CMSSW_15_0_X/PhysicsTools/PatUtils/interface/PATJetCorrExtractor.h).
Synthetic data/MC regression tests check arithmetic and variation propagation;
a representative production-sample closure check remains necessary before
regenerating final physics results.

### Correction Enable Flags (`CmsCorrectionConfig`)

| Flag | Default | Controls |
|------|---------|----------|
| `applyJetCorrections` | `false` | AK4 JEC/JER, `JetEnergyScaleManager`, `CorrectedJetCollectionManager` |
| `applyFatJetCorrections` | `false` | AK8 JEC/JER, `fatJetEnergyScaleManager`, `CorrectedFatJetCollectionManager` |
| `applyElectronCorrections` | `false` | Electron scale/smear, `ElectronEnergyScaleManager`, `CorrectedElectronCollectionManager` |
| `applyMuonCorrections` | `false` | Muon scale/resolution (RoccoR), `MuonRochesterManager`, `CorrectedMuonCollectionManager` |
| `applyJesSystematics` | `true` | Register the JES uncertainty sources selected by `jesMode`; nominal JEC remains applied |
| `applyJerSystematics` | `true` | Register AK4/AK8 JER up/down variations; nominal JER remains applied |
| `applyLeptonScaleSmearSystematics` | `true` | Register electron scale/smear and muon scale/resolution variations; nominal lepton corrections remain applied |
| `applyBtagCorrections` | `false` | `TaggerWorkingPointManager` (btag) |
| `applyCtagCorrections` | `false` | `TaggerWorkingPointManager` (ctag) |

Pass a `CmsCorrectionConfig` to `setupCmsFrameworkPlugins()` to create only
the plugin instances for enabled corrections. Systematic switches only register
variations when the `SystematicManager` is enabled.

Nominal AK4 and AK8 JEC use the era's DATA compound for data: the default
`_MC_` token is replaced with `_DATA_`. This includes the DATA residual
calibration. Compound inputs follow the payload schema, including `run` and
`JetPhi` where required. An explicit AK4 `jesCompoundCorrectionOverride` is
used verbatim. The chosen names are recorded as `cmsJecCompoundCorrection`
and `cmsFatJetJecCompoundCorrection` (with the configured key prefix).

AK4 JER supports both payload formats. A central SF with a string variation
input uses its nominal/up/down categories. A central SF without that input
requires the separate absolute uncertainty map for JER variations, giving
`SF_up/down = SF_nominal +/- uncertainty`. Run 3 year defaults derive the map
name by replacing `_ScaleFactor_` with `_SFUncertainty_` after registry
overrides. `CmsYearSettings::jerScaleFactorUncertaintyCorrection` and
`jerScaleFactorUncertaintyInputs` can be set through year YAML or registry
overrides; the inputs default to `jerScaleFactorInputs`. A missing map fails
setup when JER variations are enabled instead of silently producing identical
shifts. Nominal smearing remains available when variations are disabled.
Jet pT and mass variations propagate into Type-1 MET when enabled.

Full JES mode loads the unregrouped per-source maps, removing `Regrouped_`
from the default total correction name. Year suffixes remain on variation
labels for correlations, but are omitted from full-source payload names.
Reduced JES continues to use the regrouped maps and their payload year suffixes.

### Additional Configuration Fields

| Field | Applies To | Default | Purpose |
|-------|-----------|---------|---------|
| `correctionNamePrefix` | Both configs | `"cms"` | Prefix for `CorrectionManager::registerCorrection()` names. Change to isolate correction sets (e.g., `"vhbb"`). |
| `configKeyPrefix` | `CmsCorrectionConfig` | `"cms"` | Prefix for config keys pushed to the Analyzer's `IConfigurationProvider`. |
| `btagWorkingPoints` | `CmsWeightConfig` | Zeroed / disabled | Set discriminator thresholds explicitly before using fixed-WP b-tag SFs. Built-in year/tagger defaults are not currently available. |
| `ctagWorkingPoints` | `CmsWeightConfig` | Zeroed / disabled | Set CvL × CvB thresholds explicitly before using fixed-WP c-tag SFs. Built-in year/tagger defaults are not currently available. |

### ChannelMask Flags (Per-Channel)

| Flag | Default | Controls |
|------|---------|----------|
| `applyMuonSFs` | `false` | Include muon ID/ISO SFs in this channel's weight |
| `applyElectronSFs` | `false` | Include electron ID/reco SFs |
| `applyTauSFs` | `false` | Include tau ID SFs |
| `applyPhotonSFs` | `false` | Include photon ID SFs |
| `applyBTagSFs` | `false` | Include b-tag SFs |
| `applyCTagSFs` | `false` | Include c-tag SFs |
| `applyTriggerSFs` | `false` | Include trigger SFs |
| `applyPileupSFs` | `false` | Include pileup weights |
| `applyL1PrefireSFs` | `false` | Include L1 prefiring weights |
| `applyJetPuIdSFs` | `false` | Include jet PU ID SFs |

All default to `false`. A component is only included in a channel's weight if
BOTH the global `CmsWeightConfig` flag AND the per-channel `ChannelMask` flag
are `true`.

## Known Friction Points & Design Notes

This section documents incompatibilities, brittle patterns, and design gaps
discovered while integrating the VHbb STXS analysis with the generic CMS
infrastructure.  Addressing these will make the infrastructure easier to use
for any analysis.

### 1. Call-order fragility (❌ Fixed in VHbb)

## Known Friction Points & Design Notes

This section documents incompatibilities, brittle patterns, and design gaps
discovered while integrating the VHbb STXS analysis with the generic CMS
infrastructure.

### ✅ Resolved Friction Points

| # | Friction Point | Resolution |
|---|----------------|------------|
| 1 | **Call-order fragility** | Enforced invariant: create managers → define columns → configure workflows → define fallbacks.  Documented in [Manager Lifecycle](#manager-lifecycle). |
| 2 | **Missing WP configuration** | `btagWorkingPoints` / `ctagWorkingPoints` structs added to `CmsWeightConfig`. Configured by `setupCmsStandardWeights()`. |
| 5 | **No default working points** | Still unresolved. Fixed-WP users must provide thresholds or register them on the manager. The declared year/tagger helper methods have no definitions; defaults require an approved BTV table. |
| 6 | **Hardcoded correction names** | `correctionNamePrefix` field added to both config structs (default `"cms"`). All registration names use this prefix. |

### 🔶 By Design / Deferred

#### 3. Analysis-specific weight components (🔶 By design)

**Problem**: The VHbb analysis has several weight components that are
analysis-specific and don't belong in generic CMS infrastructure:
- Trigger SFs with per-channel di-muon, di-electron, single-muon, single-e logic
- V+jets stitching weights, dR weights, njetPt weights, NLO reweighting
- STXS acceptance / migration weights
- JES/JER weight components (b-tag × c-tag ratios)
- Lepton energy-scale variations (unity placeholders)

**Workaround**: Use CMS infrastructure for common parts (pileup, muon,
electron, b-tag, c-tag) via `setupCmsStandardWeights()` or individual
`defineCms*()` calls, then layer analysis-specific components on top with
direct `WeightManager` calls.  See the [Custom SF pattern](#custom-scale-factor-extension-pattern) for an example.

#### 4. Column naming divergence (🔶 Documented)

**Problem**: CMS infrastructure uses column names like `pileup_weight_nominal`
while VHbb registers varied-weight columns as `weight_pileup_up`.  Inconsistent
across analyses.

**Recommendation**: Use the CMS convention (`<component>_weight_<variation>`)
for all new code.  See the [Column Naming Convention](#column-naming-convention)
section for the canonical reference.

#### 5. Tagger working point defaults (🔶 Unresolved)

**Status**: The default-working-point functions are declared but have no
definitions, and `setupCmsStandardWeights()` passes configured values through
unchanged. Zero thresholds therefore do not select any working points. For
fixed-WP weights, supply thresholds explicitly or configure the manager
directly:

```cpp
cfg.btagWorkingPoints.medium = 0.35f;
cfg.ctagWorkingPoints.cvlMedium = 0.10f;
```

Adding built-in defaults needs an approved year/tagger table, including which
working points each payload supports. The historical plugin-local test source
contains guessed values and expectations for unsupported years, so those
values are not used as defaults.

### 7. `configureNominalAnalysisWeights` duplicates CMS logic (🔶 Planned)

**Problem**: VHbb's `configureNominalAnalysisWeights()` (used in Pass 1)
duplicates pileup, muon, electron, b-tag, and c-tag weight computation
that exists in CMS infrastructure.  It was written before the CMS module
existed and was never migrated.

**Plan**: Migrate VHbb Pass 1 to use `setupCmsStandardWeights()` for common
components, layering VHbb-specific parts (trigger SFs, stitching, STXS
weights) on top.  See WP-5 in the implementation plan.

## Column Naming Convention

All weight columns produced by the CMS infrastructure follow a consistent naming
scheme.  When adding your own weight components, follow the same pattern so that
histogram configs and downstream tools work uniformly.

### Scale-Factor Columns (per-object, `RVec<float>`)

```
<component>_<type>_sf_<variation>_vec
```

| Part | Meaning | Example |
|------|---------|---------|
| `<component>` | Object type or correction name | `muon_id`, `electron_reco`, `btag` |
| `<type>` | Component subtype | `sf` for scale factors; `trig` for trigger |
| `<variation>` | Systematic variation | `nominal`, `up`, `down`, `systup` |
| `_vec` suffix | Indicates per-object (RVec) column | — |

### Event-Level Weight Columns (scalar, `float`)

```
<component>_weight_<variation>
```

| Part | Meaning | Example |
|------|---------|---------|
| `<component>` | Correction component | `pileup`, `muon_id`, `btag` |
| `weight` | Literal — indicates event-level weight | — |
| `<variation>` | Systematic variation | `nominal`, `up`, `down`, `systup` |

**Examples**: `pileup_weight_nominal`, `muon_id_weight_systup`,
`electron_reco_weight_down`, `btag_weight_jes_up`.

### Nominal Event Weight

```
weight_nominal                           (single-channel)
weight_nominal_<channel>                 (per-channel, e.g. weight_nominal_muon)
```

### Systematic Variation Columns (per-channel)

```
weight_nominal_<channel>_<systematic>Up
weight_nominal_<channel>_<systematic>Down
```

**Example**: `weight_nominal_muon_pileupUp`, `weight_nominal_electron_btagDown`.

These are auto-registered with `SystematicManager` when `cfg.channels` is configured.

## Custom Scale-Factor Extension Pattern

To add a custom scale factor on top of the CMS infrastructure (e.g., top-pT
reweighting, analysis-specific trigger SFs, V+jets stitching), follow this
pattern **after** calling `setupCmsStandardWeights()`:

```cpp
// 1. Compute your per-event SF column(s)
an.Define("my_weight_nominal", computeMyWeight, {...});
an.Define("my_weight_up",      computeMyWeightUp, {...});
an.Define("my_weight_down",    computeMyWeightDown, {...});

// 2. Register with WeightManager
auto wm = an.getPlugin<WeightManager>("weightManager");
wm->addScaleFactor("my_component", "my_weight_nominal");
wm->addWeightVariation("my_component", "my_weight_up", "my_weight_down");
```

For per-channel components, add a new flag to `ChannelMask` and extend the
`SFComponent` list in `defineCmsNominalWeight()`.  See VHbb's `VHqqWeights.cc`
for a real multi-channel example with trigger SFs and stitching weights.

### Custom Correction without Existing Infrastructure

If you need a completely custom correction that doesn't fit any existing
`defineCms*()` function, use the raw `CorrectionManager` + `WeightManager` API:

```cpp
#include "CorrectionManager.h"
#include "WeightManager.h"

auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
corrMgr->registerCorrection(
    cfg.correctionNamePrefix + "_my_custom", filePath, correctionName,
    {"inputVar1", "inputVar2"});
corrMgr->applyCorrectionVec(
    cfg.correctionNamePrefix + "_my_custom", {"nominal"}, {},
    "my_custom_sf_nominal_vec");
an.Define("my_custom_weight_nominal", productOrOne,
          {"my_custom_sf_nominal_vec"});
```

## Architecture Notes for Implementors

### Correction Registration Pattern
```cpp
// 1. Register JSON file + correction name + input variables
corrMgr->registerCorrection(pfx + "my_sf", filePath, correctionName,
                            {"inputVar1", "inputVar2"});

// 2. Evaluate with string arguments → creates column
corrMgr->applyCorrectionVec("cms_my_sf", {"nominal", "WP"}, {},
                            "my_sf_nominal_vec");
// → creates column "my_sf_nominal_vec" containing RVec<float>
// → column name format: cms_my_sf_nominal_WP

// 3. Convert per-object → per-event (product)
an.Define("my_weight_nominal", productOrOne, {"my_sf_nominal_vec"});

// 4. Register in nominal weight composition
wm->addScaleFactor("my_component", "my_weight_nominal");
wm->addWeightVariation("my_component", "my_weight_up", "my_weight_down");
```

### Key Constraints
- `CorrectionManager` evaluates corrections via `correctionlib` — variable order in
  `registerCorrection()` **must match** the JSON definition order
- String arguments are appended to output column names — keep them short but descriptive
- `ObjectEnergyManagerBase::applyCorrectionlib()` delegates to CorrectionManager then
  multiplies pT × SF — use `applyCorrectionVec()` for pure SF evaluation

### Configuration summaries
`CmsCorrectionConfig::print()` and `CmsWeightConfig::print()` write a readable
summary to the stream passed by the caller. The setup functions do not print
automatically, so analyses can choose whether and where to log these summaries:

```
=== CmsCorrectionConfig ===
  jetTagger: DeepFlav
  jesMode: Reduced
  applyJetCorrections: yes
  applyElectronCorrections: no
  ...
=== CmsWeightConfig ===
  isMC: yes
  jetTagger: DeepFlav
  applyPileupWeights: yes
  applyMuonScaleFactors: yes
  ...
```

### Manager Lifecycle
```
setupCmsFrameworkPlugins()          → create all managers
defineCmsCorrectionInputs()         → define helper columns
configureCmsCorrectionWorkflow()    → register corrections, apply JEC/JER/scale
defineCmsCorrectionFallbacks()      → identity columns for disabled corrections
applyCmsHemIssueFilter()            → event filter (2018 only)
applyCmsJetVetoMap()                → jet veto map filter
setupCmsGenWeights()                → LHE/PDF/PS variation weights
setupCmsStandardWeights()           → all event weight components
```

## Building

After updating `core/plugins/CMakeLists.txt` to include this directory,
rebuild with:

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
```

## How to Add a New Correction

Pattern for adding a new object-correction weight function — always use
`cfg.correctionNamePrefix` for registration names:

```cpp
void defineCmsMyScaleFactors(Analyzer &an, const CmsWeightConfig &cfg) {
  if (!cfg.isMC || !cfg.yearSettings) return;
  const auto &ys = *cfg.yearSettings;
  const std::string pfx = cfg.correctionNamePrefix + "_";
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");

  // 1. Register the correction JSON with CorrectionManager
  corrMgr->registerCorrection(
      pfx + "my_sf", ys.myFile, ys.myCorrection,
      {"inputVar1", "inputVar2"});

  // 2. Evaluate per-object SFs
  corrMgr->applyCorrectionVec(pfx + "my_sf", {"nominal", "WP"}, {},
                              "my_sf_nominal_vec");
  corrMgr->applyCorrectionVec(pfx + "my_sf", {"up", "WP"}, {},
                              "my_sf_up_vec");
  corrMgr->applyCorrectionVec(pfx + "my_sf", {"down", "WP"}, {},
                              "my_sf_down_vec");

  // 3. Register in nominal weight composition (in defineCmsNominalWeight)
  wm->addScaleFactor("my_component", "my_weight_nominal");
  wm->addWeightVariation("my_component", "my_weight_up", "my_weight_down");
}
```

## CMS POG Correction File Locations

The base path for all of these files is `/cvmfs/cms-griddata.cern.ch/cat/metadata/`

| System | CAT CVMFS Path Pattern | JSON Contents |
|--------|----------------------|---------------|
| **JME (Jets+MET)** | `JME/<era>/latest/jet_jerc.json.gz` | JEC, JER, JER SF, pileup ID |
| **JME (VetoMaps)** | `JME/<era>/latest/jetvetomaps.json.gz` | Jet veto maps (mandatory Run 3) |
| **BTV (b-tag)** | `BTV/<era>/latest/btagging.json.gz` | b-tag SF, c-tag SF |
| **EGM (e/γ)** | `EGM/<era>/latest/electron.json.gz` | Electron/Photon ID SF, trigger SF, scale/smear |
| **MUO (μ)** | `MUO/<era>/latest/muon_Z.json.gz` | Muon ID, isolation, trigger SF, scale/resolution |
| **LUM (Pileup)** | `LUM/<era>/latest/puWeights.json.gz` | Pileup per-event weights |
| **TAU** | `TAU/<era>/latest/tau_*.json.gz` | Tau ID, anti-lepton discriminant, energy scale |

## Build and Test

`CmsInfrastructure` is built as part of RDFAnalyzerCore's `corePlugins` target.
The former plugin-local standalone preset did not define a CMake project or
load the framework's ROOT and GoogleTest targets, so it did not build or run
the advertised test suite. The CMS test now uses the canonical framework test
source and is registered whenever the framework is configured with
`BUILD_TESTS=ON`.

From the RDFAnalyzerCore repository root, use the same build directory as the
framework:

```bash
cmake --build <build-directory> --target testCmsInfrastructure
ctest --test-dir <build-directory> -R '^CmsInfrastructureTest$' --output-on-failure
```

This target tests the APIs currently implemented by the framework. It does not
provide a separately installable `CmsInfrastructure` package.

## Corrections Reference

Each weight component's correction name, JSON source, and input variables.

| Component | Correction Name | JSON Source | Years | Inputs |
|-----------|----------------|-------------|-------|--------|
| Pileup | `CollisionsXX_*_goldenJSON` | `LUM/.../puWeights.json.gz` | All | `Pileup_nTrueInt` |
| Pileup MB xsec | same (minbias_xsec variations) | `LUM/.../puWeights.json.gz` | All | `Pileup_nTrueInt` |
| Muon ID | `NUM_MediumID_DEN_TrackerMuons` | `MUO/.../muon_Z.json.gz` | All | `abseta, pt` |
| Muon Iso | `NUM_TightPFIso_DEN_MediumID` | `MUO/.../muon_Z.json.gz` | All | `abseta, pt` |
| Muon Trigger | `NUM_IsoMu24_*_DEN_*` | `MUO/.../muon_Z.json.gz` | Year-dep | `abseta, pt` |
| Electron ID | `Electron-ID-SF` | `EGM/.../electron.json.gz` | All | `year, sf, wp` |
| Electron Reco | `Electron-ID-SF` | `EGM/.../electron.json.gz` | All | `year, sf, RecoBin` |
| b-tag (FixedWP) | `deepJet_comb` / `particleNet_comb` | `BTV/.../btagging.json.gz` | All | `flavour, abseta, pt` |
| b-tag (Shape) | `deepJet_shape` / `particleNet_shape` | `BTV/.../btagging.json.gz` | All | `disc, abseta, pt, var` |
| c-tag (FixedWP) | `deepJet_light/wc/tnp` | `BTV/.../ctagging.json.gz` | All | `flavour, abseta, pt` |
| c-tag (Shape) | `deepJet_cshape` | `BTV/.../ctagging.json.gz` | All | `CvsL, CvsB, abseta, pt` |
| Tau ID VSjet | `DeepTau2018v2p5VSjet` | `TAU/.../tau.json.gz` | All | `pt, dm, genmatch, wp, wpVSe, syst, flag` |
| Tau VSe | `DeepTau2018v2p5VSe` | `TAU/.../tau.json.gz` | All | `eta, dm, genmatch, wp, syst` |
| Tau VSmu | `DeepTau2018v2p5VSmu` | `TAU/.../tau.json.gz` | All | `eta, genmatch, wp, wpVSe, wpVSjet, syst` |
| Photon ID | `Photon-ID-SF` | `EGM/.../photon.json.gz` | Run 3 | `year, valType, wp, eta, pt` |
| Photon CSEV | `Photon-CSEV-SF` | `EGM/.../photon.json.gz` | Run 3 | `year, valType, wp, eta, r9` |
| PU Jet ID | `pileupJetId_sf` | `JME/.../jet_jerc.json.gz` | Run 2 | `abseta, pt, wp, syst` |
| L1 Prefire | `L1PreFiringWeight_Nom/Up/Dn` | NanoAOD branches | 2016-17 | event-level |
| MET XY-shift | Embedded parameters | N/A | Run 2 only | `metPt, metPhi, run, npv` |
| MET Type-1 | `JetEnergyScaleManager::propagateMET()` | JEC JSON | All | Fully calibrated JEC/JER-corrected jets |

### Year-Specific Trigger SF Names

| Year | Muon Trigger SF Correction Name |
|------|-------------------------------|
| 2016 Pre/Post-VFP | `NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_TkIsoHIso` |
| 2017 | `NUM_IsoMu27_DEN_CutBasedIdTight_and_TkIsoHIso` |
| 2018, Run 3 (2022–2025) | `NUM_IsoMu24_DEN_CutBasedIdTight_and_TkIsoHIso` |

## Multi-Channel Weight Composition

For analyses with multiple channels (muon, electron, tau), the infrastructure
creates per-channel weight columns with only the relevant scale factors.

### Setup

```cpp
CmsWeightConfig weightCfg;
weightCfg.yearSettings = &yearSettings;
weightCfg.isMC = true;
weightCfg.applyPileupWeights = true;
weightCfg.applyL1PrefireWeights = true;
weightCfg.applyMuonScaleFactors = true;
weightCfg.applyElectronScaleFactors = true;
weightCfg.applyBTagScaleFactors = true;
weightCfg.applyTriggerScaleFactors = true;

// Define channel masks — only SFs enabled in the mask are included
ChannelMask muonCh;
muonCh.applyMuonSFs = true;
muonCh.applyElectronSFs = false;
muonCh.applyBTagSFs = true;
muonCh.applyTriggerSFs = true;
muonCh.applyPileupSFs = true;
muonCh.applyL1PrefireSFs = true;
weightCfg.channels["muon"] = muonCh;

ChannelMask electronCh;
electronCh.applyMuonSFs = false;
electronCh.applyElectronSFs = true;
electronCh.applyBTagSFs = true;
electronCh.applyTriggerSFs = true;
electronCh.applyPileupSFs = true;
electronCh.applyL1PrefireSFs = true;
weightCfg.channels["electron"] = electronCh;

setupCmsStandardWeights(an, weightCfg);
```

### Output Columns

The following columns are produced:
- `weight_nominal_muon` — muon channel weight (no electron SFs)
- `weight_nominal_electron` — electron channel weight (no muon SFs)
- `weight_nominal` — combined weight (all SFs, backward compatible)
- `weight_nominal_muon_pileupUp` / `weight_nominal_muon_pileupDown` — systematic variations
- `weight_nominal_electron_pileupUp` / `weight_nominal_electron_pileupDown` — systematic variations

All per-channel systematic columns are auto-registered with `SystematicManager`
for discovery by `NDHistogramManager`.

### Per-Channel Flag Reference

| `ChannelMask` Flag | Controls |
|---------------------|----------|
| `applyMuonSFs` | Muon ID + Isolation SFs |
| `applyElectronSFs` | Electron ID + Reco SFs |
| `applyTauSFs` | Tau ID SFs |
| `applyPhotonSFs` | Photon ID + CSEV SFs |
| `applyBTagSFs` | b-tag SF weight |
| `applyCTagSFs` | c-tag SF weight |
| `applyTriggerSFs` | Trigger SFs |
| `applyPileupSFs` | Pileup weights |
| `applyL1PrefireSFs` | L1 prefiring weights |
| `applyJetPuIdSFs` | Jet pileup ID SFs |

## Weight Validation

After running an analysis, validate weights using the `WeightManager` audit
entries:

```cpp
auto wm = an.getPlugin<WeightManager>("weightManager");
auto entries = wm->getAuditEntries();
for (const auto &e : entries) {
  std::cout << e.name << ": sum=" << e.sumWeights
            << " mean=" << e.meanWeight
            << " min=" << e.minWeight << " max=" << e.maxWeight << "\n";
}
```

### Common Checks

1. **Sum of weights**: `sum(weight_nominal)` ≈ `integrated_luminosity * cross_section / sum_gen_weights`
2. **Weight distribution**: Mean weight ≈ 1.0 for properly normalized MC
3. **Component audit**: Each SF component should have mean ≈ 1.0 (pileup, b-tag, etc.)
4. **Systematic envelope**: Varied weights should bracket the nominal weight

### Common Pitfalls

- **Missing genWeight**: Ensure `genWeight_nominal` is defined (from `setupCmsGenWeights()`)
- **Wrong lumi fraction**: For HEM filter, verify `hemIssueLumiFraction` matches data/MC ratio
- **Duplicate SFs**: Don't call both `setupCmsStandardWeights()` and individual `defineCms*()` functions
- **Missing columns**: Ensure all `Selected*` columns (e.g. `SelectedMuon_pt`) are defined before weights
