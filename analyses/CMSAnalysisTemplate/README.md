# CMS Analysis Template

A ready-to-use starting point for a realistic CMS NanoAOD analysis built with
RDFAnalyzerCore.  The template covers the full workflow from raw NanoAOD to
histograms and cutflow tables, and is designed to be **extended, not rewritten**.

## Single C++ implementation — two entry points

All analysis logic lives in **`analysis_setup.h`**:

```
analysis_setup.h          ← single C++ implementation (object collections,
    │                         cuts, regions, plugins, weights, histograms)
    │
    ├── analysis.cc           ← standalone binary  (main → setupCMSAnalysis → run)
    │
    └── analysis_bindings.cc  ← Python pybind11 module (cms_analysis_template)
              │
              └── analysis_wrapper.py  ← CMSAnalysisBase Python base class
```

There is **no Python re-implementation** of analysis logic.  The Python
wrapper calls `setupCMSAnalysis(Analyzer&)` in compiled C++ and exposes the
configured `rdfanalyzer.Analyzer` for further extension.

---

## Directory structure

```
CMSAnalysisTemplate/
├── analysis_setup.h        ← single C++ implementation (collections, cuts, …)
├── analysis.cc             ← C++ binary entry point (~30 lines)
├── analysis_bindings.cc    ← pybind11 module (cms_analysis_template)
├── analysis_wrapper.py     ← Python base class (CMSAnalysisBase)
├── CMakeLists.txt          ← builds both the binary and the Python module
├── cfg.yaml                ← main config (data run)
├── cfg_mc.yaml             ← main config (MC variant)
├── collections.yaml        ← collection cut documentation (reference / corrections)
├── triggers.yaml           ← trigger paths (TriggerManager)
├── histograms.yaml         ← histogram definitions (NDHistogramManager)
├── floats.yaml             ← float-valued constants
├── ints.yaml               ← integer-valued constants
└── README.md               ← this file
```

---

## Build

```bash
source env.sh
cmake -S . -B build && cmake --build build -j$(nproc)
export PYTHONPATH=$PWD/build/python:$PYTHONPATH
```

This builds both:

* `build/analyses/CMSAnalysisTemplate/cms_analysis_template` — the C++ binary
* `build/python/cms_analysis_template*.so` — the Python extension module

---

## C++ usage

### Run directly

```bash
cd build/analyses/CMSAnalysisTemplate
./cms_analysis_template ../../../analyses/CMSAnalysisTemplate/cfg.yaml
```

### Extend in C++

To add analysis logic, edit `analysis_setup.h`.  The `setupCMSAnalysis(Analyzer&)`
function contains all plugin registrations, collection definitions, event cuts,
region declarations, and weight setup.  `analysis.cc` remains a thin wrapper:

```cpp
// analysis.cc — stays minimal; all work is in analysis_setup.h
#include "analysis_setup.h"
int main(int argc, char **argv) {
    Analyzer an(argv[1]);
    setupCMSAnalysis(an);   // C++ setup from analysis_setup.h
    an.run();
    return 0;
}
```

---

## Python usage

The Python wrapper uses the compiled `cms_analysis_template` pybind11 module
to run the same `setupCMSAnalysis` C++ function, then exposes the analyzer
object for Python-level extensions.  Performance is identical to the C++ binary
for the parts covered by the template.

### Prerequisites

```bash
pip install pyyaml   # for config validation only
# ensure PYTHONPATH contains build/python
```

### Option 1 — extend the analyzer directly (simplest)

```python
from analyses.CMSAnalysisTemplate.analysis_wrapper import CMSAnalysisBase

base = CMSAnalysisBase("analyses/CMSAnalysisTemplate/cfg.yaml")
an   = base.analyzer   # C++ setup already applied by cms_analysis_template
                       # all collections, regions, plugins from analysis_setup.h
                       # are registered and ready

# Extend with Python-level ROOT JIT expressions
an.Define("myVar", "TransverseMass * 2.0f")
an.Filter("extra_cut", "myVar > 80.f")
base.run()
```

### Option 2 — subclass for a reusable analysis

```python
from analyses.CMSAnalysisTemplate.analysis_wrapper import CMSAnalysisBase

class WmuNuAnalysis(CMSAnalysisBase):
    """W → μν analysis built on the CMS template."""

    def build_analysis(self) -> None:
        # self.analyzer is fully configured by C++ setup at this point
        an = self.analyzer
        an.Define("MT2", "TransverseMass * 2.0f")
        an.Filter("highMT", "MT2 > 100.f")


if __name__ == "__main__":
    import sys
    WmuNuAnalysis(sys.argv[1]).run()
```

### Option 3 — command line

```bash
# Run the full template
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml

# Validate configs and exit
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml --validate-only

# Run and print results summary
python analyses/CMSAnalysisTemplate/analysis_wrapper.py \
    --config analyses/CMSAnalysisTemplate/cfg.yaml \
    --results output/cms_template_output.root
```

---

## How the Python–C++ bridge works

```
Python:  an = rdfanalyzer.Analyzer("cfg.yaml")
                    │
Python:  an._get_analyzer_ptr()  → uintptr_t (raw Analyzer* address)
                    │
C++:     cms_analysis_template.setup_analysis(ptr)
             reinterpret_cast<Analyzer*>(ptr)
             setupCMSAnalysis(*an)   ← same function as the C++ binary uses
                    │
Python:  an.Define(...)  ← extend with ROOT JIT expressions
         an.save()       ← trigger the event loop
```

Both the C++ binary and the Python module call the **identical** compiled
`setupCMSAnalysis` function.  There is no Python JIT duplication.

---

## What `setupCMSAnalysis` registers

| Step | Description |
|------|-------------|
| Plugins | `NDHistogramManager`, `CutflowManager`, `TriggerManager`, `RegionManager`, `WeightManager` |
| Triggers | `TriggerManager::applyAllTriggers()` (reads `triggers.yaml`) |
| Collections | `looseMuons`, `tightMuons`, `looseElectrons`, `tightElectrons`, `preJets`, `goodJets`, `cleanJets` |
| Event cuts | One tight muon, no extra muon, no electron, MET > 30, m_T > 40 (via `CutflowManager`) |
| Derived columns | `TransverseMass`, `nCleanJets`, `LeadMuPt`, `LeadMuEta`, `LeadMuPhi`, `LeadJetPt`, `LeadJetEta` |
| Region booleans | `pass_signal`, `pass_wCR`, `pass_topCR` |
| Regions | `signal` (nJets ≥ 1, m_T > 60), `wCR` (nJets = 0), `topCR` (nJets ≥ 2) |
| Weights | `eventWeight = 1.0` (stub), `weight_nominal` |
| Histograms | `NDHistogramManager::bookConfigHistograms()` (reads `histograms.yaml`) |

---

## Object collections

Collections are implemented in `analysis_setup.h` and produce
`PhysicsObjectCollection` objects on the dataframe.

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

Barrel-endcap gap (1.444 < |η_SC| < 1.566) is vetoed for both WPs.

### Jets

`preJets`: tight jetID ≥ 2, pT > 30 GeV, |η| < 4.7  
`goodJets`: `preJets` with ΔR > 0.4 overlap removal vs. `tightMuons`  
`cleanJets`: `goodJets` with ΔR > 0.4 overlap removal vs. `tightElectrons`

---

## Adding CMS corrections

Corrections are applied in `analysis_setup.h` before the collection builders.
`collections.yaml` documents which corrected column name each collection uses.

### Muon Rochester corrections

```cpp
// In setupCMSAnalysis(), before the collection defines:
auto roc = MuonRochesterManager::create(an);
roc->applyRochesterCorrection(*cm, "rochester", "Muon_pt", "Muon_pt_roc");

// Then use "Muon_pt_roc" in buildLooseMuons / buildTightMuons:
an.Define("looseMuons", buildLooseMuons,
    {"Muon_pt_roc", "Muon_eta", "Muon_phi", "Muon_mass",
     "Muon_looseId", "Muon_pfRelIso04_all"});
```

### Electron energy-scale corrections

```cpp
auto esc = ElectronEnergyScaleManager::create(an);
esc->applyCorrectionlib(*cm, "electron_scale", {"scale"}, {},
    "Electron_pt", "Electron_pt_esc");
// Use "Electron_pt_esc" in buildLooseElectrons / buildTightElectrons
```

### Jet energy corrections (JEC/JER)

See `docs/CMS_CORRECTIONS.md` and `docs/JET_ENERGY_CORRECTIONS.md`.

---

## Adding MC event weights

In `setupCMSAnalysis()`, replace the unit-weight stub:

```cpp
// Replace the placeholder block in analysis_setup.h:
an.Define("puWeight",   ...);  // pileup correction column
an.Define("muonIdSF",  ...);   // muon ID scale factor column
weightMgr->addScaleFactor("pileup",  "puWeight");
weightMgr->addScaleFactor("muon_id", "muonIdSF");
weightMgr->addNormalization("xsec_lumi", xsec * lumi / sumWeights);
weightMgr->defineNominalWeight("weight_nominal");
```

---

## Adding histograms

Edit `histograms.yaml` — no C++ changes needed:

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

In `analysis_setup.h`:

```cpp
// 1. Define a boolean column
an.Define("pass_myRegion", myRegionPredicate, {"someColumn"});

// 2. Declare the region
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
