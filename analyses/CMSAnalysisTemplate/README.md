# CMS Analysis Template

A ready-to-use starting point for a realistic CMS NanoAOD analysis built with
RDFAnalyzerCore.  The template covers the full workflow from raw NanoAOD to
histograms and cutflow tables, demonstrating every major framework feature.
It is designed to be **extended, not rewritten**: users add their analysis logic
on top of the provided base without touching the framework boilerplate.

Both a **C++ core** (``analysis.cc``) and a **Python base class**
(``analysis_wrapper.py``) are provided.  They read the same ``cfg.yaml`` and
``collections.yaml``, so you can use either interface or mix both.

---

## What this template demonstrates

| Feature | C++ (`analysis.cc`) | Python (`analysis_wrapper.py`) |
|---------|---------------------|-------------------------------|
| Multiple object collections at different WPs | `buildLooseMuons`, `buildTightMuons`, … | `CMSAnalysisBase` reads `collections.yaml` |
| ΔR overlap removal | `PhysicsObjectCollection::removeOverlap` | `_rdf_drOverlapMask` Cling helper |
| Multiple analysis regions | `RegionManager::declareRegion` | User adds on `analyzer` |
| Sequential + N-1 cutflow | `CutflowManager::addCut` | User adds on `analyzer` |
| Config-driven triggers | `TriggerManager::applyAllTriggers` | `CMSAnalysisBase._apply_triggers` |
| Event weights / SFs | `WeightManager` | User adds on `analyzer` |
| Config-driven histograms | `NDHistogramManager` | `NDHistogramManager` plugin |
| **Extend without rewriting** | Add functions + calls | Subclass `CMSAnalysisBase` |

---

## Directory structure

```
CMSAnalysisTemplate/
├── analysis.cc              ← C++ core (compile & run directly)
├── analysis_wrapper.py      ← Python base class (CMSAnalysisBase)
├── CMakeLists.txt           ← CMake build target
├── cfg.yaml                 ← Main config (data run)
├── cfg_mc.yaml              ← Main config (MC run variant)
├── collections.yaml         ← Object collection definitions (Python + docs)
├── triggers.yaml            ← Trigger paths (TriggerManager)
├── histograms.yaml          ← Histogram definitions (NDHistogramManager)
├── floats.yaml              ← Float constants
├── ints.yaml                ← Integer constants
└── README.md                ← This file
```

---

## Python usage (no compilation required for extensions)

The Python base class uses the compiled ``rdfanalyzer`` module to drive the
analysis.  It reads ``cfg.yaml`` and ``collections.yaml``, sets up the
framework boilerplate, and exposes the underlying analyzer object for extension.

### Prerequisites

```bash
source env.sh                                # set up ROOT + compiler
cmake -S . -B build && cmake --build build -j$(nproc)
export PYTHONPATH=$PWD/build/python:$PYTHONPATH
pip install pyyaml                           # for YAML config parsing
```

### Option 1 — Extend the analyzer directly (simplest)

```python
from analyses.CMSAnalysisTemplate.analysis_wrapper import CMSAnalysisBase
import sys

base = CMSAnalysisBase("analyses/CMSAnalysisTemplate/cfg.yaml")

# base.analyzer is a fully-initialized rdfanalyzer.Analyzer.
# All collections from collections.yaml are already defined as:
#   {name}_mask, {name}_pt, {name}_eta, {name}_phi, {name}_mass, {name}_n
an = base.analyzer

# Add your analysis logic on top:
an.Define("MT",
    "sqrt(2*tightMuons_pt[0]*MET_pt*(1-cos(tightMuons_phi[0]-MET_phi)))")
an.Filter("MT_cut", "MT > 40.f")
an.Define("nJets", "(int)goodJets_n")

base.run()   # saves output defined in cfg.yaml
```

### Option 2 — Subclass for reusable analyses

```python
from analyses.CMSAnalysisTemplate.analysis_wrapper import CMSAnalysisBase

class WmuNuAnalysis(CMSAnalysisBase):
    """W → μν analysis built on the CMS template."""

    def build_analysis(self):
        an = self.analyzer  # collections already built by base class

        # Derived quantities
        an.Define("MT",
            "sqrt(2*tightMuons_pt[0]*MET_pt*(1-cos(tightMuons_phi[0]-MET_phi)))")
        an.Define("nJets", "(int)goodJets_n")

        # Event selection
        an.Filter("one_tight_muon", "(int)tightMuons_n == 1")
        an.Filter("no_loose_muon",  "(int)looseMuons_n == 1")   # no extra loose muon
        an.Filter("no_electron",    "(int)looseElectrons_n == 0")
        an.Filter("MET_cut",        "MET_pt > 30.f")
        an.Filter("MT_cut",         "MT > 40.f")

        # Add extra plugin for cutflow (optional)
        an.AddPlugin("cutflow", "CutflowManager")


if __name__ == "__main__":
    WmuNuAnalysis(sys.argv[1]).run()
```

### Option 3 — Command line

```bash
# Run the base template (collections + triggers; no additional cuts)
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml

# Validate config only
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml --validate-only

# Run and inspect results
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml \
    --results output/cms_template_output.root
```

---

## C++ usage

### Build

```bash
source env.sh
cmake -S . -B build && cmake --build build -j$(nproc)
# or build only this target:
cmake --build build --target cms_analysis_template -j$(nproc)
```

### Run

```bash
cd build/analyses/CMSAnalysisTemplate
./cms_analysis_template ../../../analyses/CMSAnalysisTemplate/cfg.yaml
```

### Single-threaded debugging

Set `threads: "0"` in `cfg.yaml`, then rerun.

---

## Object collections

Collections are specified in `collections.yaml` and built automatically by
`CMSAnalysisBase`.  The C++ `analysis.cc` implements them as standalone
functions that produce `PhysicsObjectCollection` objects.

### Muon working points

| Collection | ID | Isolation | pT | |η| |
|------------|----|-----------|----|-----|
| `looseMuons` | `Muon_looseId` | < 0.25 | > 10 GeV | < 2.4 |
| `tightMuons` | `Muon_tightId` | < 0.15 | > 26 GeV | < 2.4 |

### Electron working points

| Collection | ID | pT | |η_SC| |
|------------|----|----|-------|
| `looseElectrons` | cut-based ≥ 1 (veto) | > 10 GeV | < 2.5 (no gap) |
| `tightElectrons` | cut-based ≥ 4 (tight) | > 30 GeV | < 2.5 (no gap) |

The barrel-endcap gap (1.444 < |η_SC| < 1.566) is vetoed for both WPs.

### Jets (`goodJets`)

Tight jetID (≥ 2), pT > 30 GeV, |η| < 4.7, with ΔR > 0.4 overlap removal
against both `tightMuons` and `tightElectrons`.

### Column naming convention (Python)

For each collection `{name}`, the Python base class defines:

| Column | Type | Description |
|--------|------|-------------|
| `{name}_mask`  | `RVec<bool>`  | per-object selection (after OR removal) |
| `{name}_pt`    | `RVec<float>` | pT of selected objects |
| `{name}_eta`   | `RVec<float>` | η |
| `{name}_phi`   | `RVec<float>` | φ |
| `{name}_mass`  | `RVec<float>` | mass |
| `{name}_n`     | `int`         | multiplicity |

---

## Analysis regions (C++)

Regions are declared on the main dataframe after the shared preselection:

| Region | Filter column | Condition |
|--------|---------------|-----------|
| `signal` | `pass_signal` | nJets ≥ 1 **and** m_T > 60 GeV |
| `wCR`    | `pass_wCR`    | nJets == 0 |
| `topCR`  | `pass_topCR`  | nJets ≥ 2 |

---

## Adding CMS corrections

The template runs without correction files by default.  To add corrections:

### Muon Rochester corrections

```cpp
// In analysis.cc, after registering plugins:
auto roc = MuonRochesterManager::create(an);
roc->applyRochesterCorrection(*cm, "rochester", "Muon_pt", "Muon_pt_roc");
```

Then replace `"Muon_pt"` with `"Muon_pt_roc"` in the collection builders,
**and** update `collections.yaml` to use `pt: Muon_pt_roc` for Python users.

### Electron energy-scale corrections

```cpp
auto esc = ElectronEnergyScaleManager::create(an);
esc->applyCorrectionlib(*cm, "electron_scale", {"scale"}, {}, "Electron_pt", "Electron_pt_esc");
```

Update `collections.yaml`: `pt: Electron_pt_esc`.

### Jet energy corrections (JEC/JER)

See `docs/CMS_CORRECTIONS.md` and `docs/JET_ENERGY_CORRECTIONS.md`.

---

## Adding MC event weights

```cpp
weightMgr->addScaleFactor("pileup",   "puWeight");
weightMgr->addScaleFactor("muon_id",  "muonIdSF");
weightMgr->addNormalization("xsec_lumi", xsec * lumi / sumWeights);
weightMgr->defineNominalWeight("weight_nominal");
```

---

## Adding histograms

Edit `histograms.yaml` only — no C++ changes needed:

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

1. Define a boolean column:
   ```cpp
   an.Define("pass_myRegion", myRegionPredicate, {"someColumn"});
   ```
2. Declare the region:
   ```cpp
   regionMgr->declareRegion("myRegion", "pass_myRegion");
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
