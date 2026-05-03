# CMS Analysis Template

A ready-to-use starting point for a realistic CMS NanoAOD analysis built with
RDFAnalyzerCore.  The template covers the full workflow from raw NanoAOD to
histograms and cutflow tables, demonstrating every major framework feature in
a single, well-commented C++ source file plus a Python wrapper.

---

## What this template demonstrates

| Feature | Where |
|---------|-------|
| **Multiple object collections at different working points** (loose + tight muons, loose + tight electrons, jets) | `analysis.cc` — `buildLooseMuons`, `buildTightMuons`, `buildLooseElectrons`, `buildTightElectrons`, `buildPreJets` |
| **ΔR overlap removal** between jets and selected leptons | `analysis.cc` — `PhysicsObjectCollection::removeOverlap` |
| **Multiple analysis regions** with a shared preselection node | `analysis.cc` — `RegionManager::declareRegion` |
| **Sequential + N-1 cutflow tables** | `analysis.cc` — `CutflowManager::addCut` |
| **Config-driven trigger selection** | `triggers.yaml` + `TriggerManager::applyAllTriggers` |
| **Event weights** (scale factors + normalisations) | `analysis.cc` — `WeightManager` |
| **Config-driven histograms** (no histogram code in analysis.cc) | `histograms.yaml` + `NDHistogramManager` |
| **Python wrapper** for running, config validation, and result inspection | `analysis_wrapper.py` |

---

## Directory structure

```
CMSAnalysisTemplate/
├── analysis.cc              ← C++ analysis core
├── analysis_wrapper.py      ← Python API + CLI wrapper
├── CMakeLists.txt           ← CMake build target
├── cfg.yaml                 ← Main config (data run)
├── cfg_mc.yaml              ← Main config (MC run variant)
├── triggers.yaml            ← Trigger paths (TriggerManager)
├── histograms.yaml          ← Histogram definitions (NDHistogramManager)
├── floats.yaml              ← Float constants
├── ints.yaml                ← Integer constants
└── README.md                ← This file
```

---

## Build

```bash
# Source ROOT and compiler environment
source env.sh

# Build all targets (including the template)
cmake -S . -B build && cmake --build build -j$(nproc)

# Build only this analysis target
cmake --build build --target cms_analysis_template -j$(nproc)
```

---

## Run

### C++ binary directly

```bash
cd build/analyses/CMSAnalysisTemplate
./cms_analysis_template ../../../analyses/CMSAnalysisTemplate/cfg.yaml
```

### Via the Python wrapper

```bash
# Validate config, build, and run
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml \
    --build

# Validate config only (no run)
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml \
    --validate-only

# Run quietly and inspect results
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml \
    --quiet \
    --results output/cms_template_output.root
```

### Single-threaded debugging

Edit `cfg.yaml`: set `threads: "0"`, then rerun.

---

## Object collections

### Muon working points

| Collection | ID | Isolation | pT | |η| |
|------------|----|-----------|----|-----|
| `looseMuons` | `Muon_looseId` | < 0.25 | > 10 GeV | < 2.4 |
| `tightMuons` | `Muon_tightId` | < 0.15 | > 26 GeV | < 2.4 |

Two collections of the same object type exist simultaneously in the
dataframe.  `tightMuons` is a strict subset of `looseMuons`.
The second-muon veto uses the loose collection to count muons not selected
by the tight working point.

### Electron working points

| Collection | ID | pT | |η_SC| |
|------------|----|----|-------|
| `looseElectrons` | cut-based ≥ 1 (veto) | > 10 GeV | < 2.5 (no gap) |
| `tightElectrons` | cut-based ≥ 4 (tight) | > 30 GeV | < 2.5 (no gap) |

The barrel-endcap gap (1.444 < |η_SC| < 1.566) is vetoed for both WPs.

### Jets

| Collection | ID | pT | |η| | Processing |
|------------|----|----|-----|------------|
| `preJets`   | tight ≥ 2 | > 30 GeV | < 4.7 | none |
| `goodJets`  | same | same | same | ΔR(jet,μ) > 0.4 |
| `cleanJets` | same | same | same | + ΔR(jet,e) > 0.4 |

---

## Analysis regions

Regions are declared on the main dataframe after the shared preselection
(trigger + 1 tight muon + no extra muon + no electron + MET > 30 + m_T > 40)
has been applied:

| Region | Filter column | Condition |
|--------|---------------|-----------|
| `signal` | `pass_signal` | nJets ≥ 1 **and** m_T > 60 GeV |
| `wCR`    | `pass_wCR`    | nJets == 0 (W+jets enriched) |
| `topCR`  | `pass_topCR`  | nJets ≥ 2 (top-quark enriched) |

All regions share the same preselection node; they are not exclusive by
construction — you can make them exclusive by adjusting the predicates.

To retrieve per-region dataframes for booking region-specific histograms:

```cpp
auto signalDF = regionMgr->getRegionDataFrame("signal");
auto wCRDF    = regionMgr->getRegionDataFrame("wCR");
```

---

## Adding CMS corrections

The template is written to run without correction files so that it compiles
and executes out of the box.  To add CMS corrections:

### Muon Rochester corrections

```cpp
// In analysis.cc, after registering plugins:
auto cm  = CorrectionManager::create(an);
auto roc = MuonRochesterManager::create(an);

cm->registerCorrection("rochester",
    "RoccoR2016aUL.json.gz",
    "RoccoR2016aUL",
    {"Muon_charge", "Muon_eta", "Muon_phi",
     "Muon_pt", "Muon_genPt", "Muon_nTrackerLayers",
     "Muon_u1", "Muon_u2"});

roc->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
roc->setRochesterInputColumns(
    "Muon_charge_f",      // Int_t → Float_t cast
    "Muon_genPt",         // 0.0 for data
    "Muon_nTrackerLayers_f",
    "Muon_u1",            // reproducible Gaussian random
    "Muon_u2");
roc->applyRochesterCorrection(*cm, "rochester", "Muon_pt", "Muon_pt_roc");
```

Then replace `"Muon_pt"` with `"Muon_pt_roc"` in the collection builders.

### Electron energy-scale corrections

```cpp
auto esc = ElectronEnergyScaleManager::create(an);
esc->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass");
esc->applyCorrectionlib(*cm, "electron_scale",
    {"scale"}, {}, "Electron_pt", "Electron_pt_esc");
```

Then replace `"Electron_pt"` with `"Electron_pt_esc"` in the collection builders.

### Jet energy corrections (JEC/JER)

```cpp
auto jes = JetEnergyScaleManager::create(an);
jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
jes->setMETColumns("MET_pt", "MET_phi");
jes->removeExistingCorrections("Jet_rawFactor");

cm->registerCorrection("jec_l1l2l3",
    "jet_jerc.json.gz",
    "Summer19UL16_V7_MC_L1L2L3Res_AK4PFPuppi",
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
jes->applyCorrectionlib(*cm, "jec_l1l2l3", {}, {}, "Jet_pt_raw", "Jet_pt_jec");
```

See `docs/CMS_CORRECTIONS.md` and `docs/JET_ENERGY_CORRECTIONS.md` for the
full API reference.

---

## Adding MC event weights

Uncomment and adapt the WeightManager block in `analysis.cc`:

```cpp
// Per-event scale-factor columns must be defined before this call.
weightMgr->addScaleFactor("pileup",    "puWeight");
weightMgr->addScaleFactor("muon_id",   "muonIdSF");
weightMgr->addScaleFactor("muon_iso",  "muonIsoSF");
weightMgr->addNormalization("xsec_lumi", xsec * lumi / sumWeights);
weightMgr->defineNominalWeight("weight_nominal");

// Systematic variations (swap one SF component at a time)
weightMgr->addWeightVariation("pileup", "puWeight",
                               "puWeightUp", "puWeightDown");
weightMgr->defineVariedWeight("pileup", "up",   "weight_pileup_up");
weightMgr->defineVariedWeight("pileup", "down", "weight_pileup_down");
```

Then use `weight_pileup_up` / `weight_pileup_down` in `histograms.yaml` for
systematic histogram variants.

---

## Adding histograms

Edit `histograms.yaml` only — no C++ changes are needed as long as the column
already exists in the dataframe:

```yaml
- name: MyNewVariable
  variable: MyNewVariable
  weight: weight_nominal
  bins: 50
  lowerBound: 0.0
  upperBound: 500.0
  label: "My new variable [GeV]"
```

---

## Adding a new analysis region

1. Define a boolean column in `analysis.cc`:

   ```cpp
   an.Define("pass_myRegion", myRegionPredicate, {"someColumn"});
   ```

2. Declare the region after all `pass_*` columns are defined:

   ```cpp
   regionMgr->declareRegion("myRegion", "pass_myRegion");
   ```

3. Book region-specific histograms using the per-region dataframe:

   ```cpp
   ROOT::RDF::RNode regionDF = regionMgr->getRegionDataFrame("myRegion");
   // book histograms on regionDF ...
   ```

---

## References

- [Analysis Guide](../../docs/ANALYSIS_GUIDE.md)
- [CMS Corrections](../../docs/CMS_CORRECTIONS.md)
- [Jet Energy Corrections](../../docs/JET_ENERGY_CORRECTIONS.md)
- [PhysicsObjectCollection](../../core/interface/PhysicsObjectCollection.h)
- [RegionManager](../../core/plugins/RegionManager/RegionManager.h)
- [WeightManager](../../core/plugins/WeightManager/WeightManager.h)
- [CutflowManager](../../core/plugins/CutflowManager/CutflowManager.h)
- [TriggerManager](../../core/plugins/TriggerManager/TriggerManager.h)
- [NDHistogramManager](../../core/plugins/NDHistogramManager/)
