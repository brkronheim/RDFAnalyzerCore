# CMS Run2016G DoubleMuon Z→μμ Example Analysis

A complete end-to-end CMS Open Data analysis demonstrating a Voigtian +
exponential fit to the dimuon Z peak in bins of jet multiplicity using
the full RDFAnalyzerCore feature set and CMS Combine / CombineHarvester.

**Data source**: CMS Open Data — no credentials required  
**Record**: <https://opendata.cern.ch/record/30522>  
**Dataset**: `/DoubleMuon/Run2016G-UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD`  
**Files served via**: `root://eospublic.cern.ch//eos/opendata/cms/…`

---

## Table of Contents

1. [Overview](#overview)
2. [Physics Analysis](#physics-analysis)
3. [Framework Features Demonstrated](#framework-features-demonstrated)
4. [Configuration Files](#configuration-files)
5. [Building](#building)
6. [Running Locally](#running-locally)
7. [CMS Open Data — Full Dataset via LAW](#cms-open-data--full-dataset-via-law)
8. [Z Peak Fit with CombineHarvester and Combine](#z-peak-fit-with-combineharvester-and-combine)
9. [Extending the Example](#extending-the-example)

---

## Overview

```
CMS Open Data NanoAOD (XRootD — public)
         │
         ▼
   analysis.cc ──► cfg.yaml
                    ├── triggers.yaml   (TriggerManager)
                    ├── histograms.yaml (NDHistogramManager)
                    ├── floats.yaml     (float constants)
                    ├── ints.yaml       (int constants)
                    └── output.yaml     (output branches)
         │
         ▼
   output/dimuon_zpeak.root
    ├── Events tree (DimuonMass, nGoodJets, …)
    ├── histograms/DimuonMass_{0j,1j,2j,ge3j}   ◄── fed to fit_zpeak.py
    ├── histograms/DimuonMass (inclusive)
    ├── histograms/nGoodJets, LeadMuPt, …
    └── cutflow / cutflow_nminus1
         │
         ▼
   fit_zpeak.py                    law run RunCombine
    ├── ws_{0j,1j,2j,ge3j}.root ──────────────────►  FitDiagnostics
    ├── datacard_{0j,…}.txt      (existing task)      per channel +
    └── datacard_combined.txt                         simultaneous
```

---

## Physics Analysis

### Event Selection

| Step | Cut |
|------|-----|
| Trigger | OR of 4 double-muon HLT paths (TriggerManager) |
| Dimuon pair | Exactly 2 tight muons: tightId, pT > 20 GeV, \|η\| < 2.4, iso < 0.15 |
| Opposite sign | μ⁺μ⁻ (Z → μ⁺μ⁻) |
| Mass window | 70 < m(μμ) < 110 GeV |

### Object Definitions

**Muons** — `buildMuonCollection()`:
- `Muon_tightId == true`
- `Muon_pfRelIso04_all < 0.15`
- `Muon_pt > 20 GeV`, `|Muon_eta| < 2.4`

**Jets** — `buildPreJetCollection()` + `removeOverlap(muons, 0.4)`:
- `Jet_jetId >= 2` (tight jet ID)
- `Jet_pt > 30 GeV`, `|Jet_eta| < 2.5`
- ΔR(jet, muon) > 0.4 via `PhysicsObjectCollection::removeOverlap()`

### Jet Categories

| Category | Condition |
|----------|-----------|
| `0j`   | nGoodJets == 0 |
| `1j`   | nGoodJets == 1 |
| `2j`   | nGoodJets == 2 |
| `ge3j` | nGoodJets >= 3 |

### Fit Model

```
f(m) = nsig × Voigtian(m; μZ, ΓZ, σ) + nbkg × Exponential(m; τ)
```
- **Voigtian**: Breit-Wigner ⊗ Gaussian; μZ, σ floating; ΓZ fixed to PDG
- **Exponential**: off-peak background; τ floating
- **nsig, nbkg**: freely floating yields (`rate -1` in Combine datacards)

---

## Framework Features Demonstrated

### TriggerManager Plugin

Trigger selection is fully config-driven via `triggers.yaml`.  No trigger
logic appears in `analysis.cc`:

```yaml
# triggers.yaml
- name: double_muon
  sample: "0"
  triggers: >-
    HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL,
    HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ,
    HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL,
    HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL_DZ
```

```cpp
// analysis.cc
auto trigMgr = std::make_unique<TriggerManager>(an.getConfigurationProvider());
an.addPlugin("triggerManager", std::move(trigMgr));
an.getPlugin<TriggerManager>("triggerManager")->applyAllTriggers();
```

### PhysicsObjectCollection

```cpp
// Muon collection from boolean mask
PhysicsObjectCollection goodMuons(pt, eta, phi, mass, mask);

// Jet overlap removal — built-in ΔR cleaning
auto goodJets = preJets.removeOverlap(goodMuons, 0.4f);

// Dimuon pair via combinatoric helper
auto pairs = makePairs(goodMuons);   // ObjectPair: {p4, first, second}
float mZ   = pairs[0].p4.M();       // dimuon invariant mass
```

### CutflowManager Plugin

```cpp
// All booleans defined BEFORE first addCut() (base node for N-1)
an.Define("hasTwoMuons",     hasExactlyTwoMuons, {"goodMuons"});
an.Define("hasOppositeSign", hasOppositeSign,    {"goodMuons","Muon_charge"});
an.Define("inMassWindow",    inMassWindow,       {"goodMuons"});

auto *cfm = an.getPlugin<CutflowManager>("cutflowManager");
cfm->addCut("DimuonPair",   "hasTwoMuons");      // applies filter + tracks N-1
cfm->addCut("OppositeSign", "hasOppositeSign");
cfm->addCut("MassWindow",   "inMassWindow");
```

### NDHistogramManager — Config-Driven Histograms

All histograms are defined in `histograms.yaml`; no histogram code is in
`analysis.cc`.  Per-jet-bin variables carry sentinel `-999` outside their
category — invisible because `lowerBound: 70.0` in the histogram config:

```yaml
# histograms.yaml (excerpt)
- name: DimuonMass_0j
  variable: DimuonMass_0j
  weight: eventWeight
  bins: 60
  lowerBound: 70.0
  upperBound: 110.0
```

### All-YAML Configuration

Every config file in this analysis uses YAML:

| File | Parser | Format |
|------|--------|--------|
| `cfg.yaml` | main config | key: value map |
| `triggers.yaml` | `parseMultiKeyConfig` | sequence of maps |
| `histograms.yaml` | `parseMultiKeyConfig` | sequence of maps |
| `floats.yaml` | `parsePairBasedConfig` | key: value map |
| `ints.yaml` | `parsePairBasedConfig` | key: value map |
| `output.yaml` | `parseVectorConfig` | sequence of strings |
| `dataset_manifest.yaml` | LAW manifest | structured YAML |

---

## Configuration Files

| File | Purpose |
|------|---------|
| `cfg.yaml` | Main runner config |
| `triggers.yaml` | HLT trigger paths (TriggerManager) |
| `histograms.yaml` | Histogram definitions (NDHistogramManager) |
| `floats.yaml` | Float constant columns |
| `ints.yaml` | Integer constant columns |
| `output.yaml` | Output tree branches |
| `dataset_manifest.yaml` | LAW OpenData task descriptor |
| `fit_zpeak.py` | Workspace + datacard builder; delegates fits to RunCombine |
| `CMakeLists.txt` | CMake build |

---

## Building

```bash
# Source the environment (ROOT, compiler, …)
source env.sh

# Core framework only
cmake -S . -B build && cmake --build build -j$(nproc)

# With CMS Combine (needed to run fits after fit_zpeak.py)
cmake -S . -B build -DBUILD_COMBINE=ON
cmake --build build -j$(nproc)

# With CombineHarvester (structured datacard writing in fit_zpeak.py)
cmake -S . -B build -DBUILD_COMBINE=ON -DBUILD_COMBINE_HARVESTER=ON
cmake --build build -j$(nproc)
```

---

## Running Locally

```bash
cd build/analyses/CMS_Run2016_DoubleMuon
./analysis ../../../analyses/CMS_Run2016_DoubleMuon/cfg.yaml
```

Output: `output/dimuon_zpeak.root` with the skimmed tree, histograms, and cutflow.

### Single-threaded debugging

Edit `cfg.yaml`: `threads: "0"`, then rerun.

### Using a local or different file

```bash
# Minimal override config
cat > /tmp/test_cfg.yaml << 'EOF'
fileList: /path/to/your_nanoaod.root
treeList: Events
saveFile: /tmp/test_out.root
type: "0"
histogramConfig: /abs/path/to/histograms.yaml
floatConfig: /abs/path/to/floats.yaml
intConfig: /abs/path/to/ints.yaml
saveConfig: /abs/path/to/output.yaml
triggerConfig: /abs/path/to/triggers.yaml
threads: "4"
EOF
./analysis /tmp/test_cfg.yaml
```

### Getting the full file list

```bash
# Using cernopendata-client (pip install cernopendata-client)
cernopendata-client get-file-locations --recid 30522 --protocol xrootd
```

Paste the XRootD paths into `fileList` in `cfg.yaml` (comma-separated).

---

## CMS Open Data — Full Dataset via LAW

`dataset_manifest.yaml` uses `das: "30522"` as the CERN Open Data record ID.
The LAW `PrepareOpenDataSample` task queries `https://opendata.cern.ch/api/records/30522`
to discover all files, then creates per-job configs for batch submission.

```bash
source law/env.sh && law index

# Step 1 — Create per-job configs (30 files per job)
law run PrepareOpenDataSample \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis \
    --files 30

# Step 2 — Build HTCondor submission structure
law run BuildOpenDataSubmission \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis

# Step 3 — Submit jobs
law run SubmitOpenDataJobs \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis

# Step 4 — Monitor and auto-resubmit failed jobs
law run MonitorOpenDataJobs \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis
```

### Entry-range splitting (for large files)

```bash
law run PrepareOpenDataSample \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis \
    --partition-mode entry_range \
    --files 500000   # entries per job
```

---

## Z Peak Fit with CombineHarvester and Combine

### Step 1 — Create workspaces and datacards

```bash
python analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py \
    --input  output/dimuon_zpeak.root \
    --outdir zpeak_fit
```

This creates:
- `zpeak_fit/ws_{0j,1j,2j,ge3j}.root` — RooWorkspaces with Voigtian+exp model
- `zpeak_fit/datacard_{0j,…}.txt` — per-channel datacards (CombineHarvester or manual)
- `zpeak_fit/datacard_combined.txt` — combined simultaneous-fit datacard

The script also prints the exact `law run RunCombine` commands to use.

### Step 2 — Run fits via the existing RunCombine LAW task

```bash
source law/env.sh && law index

# Per-channel fit (0j shown; repeat for 1j, 2j, ge3j)
law run RunCombine \
    --datacard-config analyses/CMS_Run2016_DoubleMuon/cfg.yaml \
    --name zpeak_0j \
    --datacard-path zpeak_fit/datacard_0j.txt \
    --method FitDiagnostics \
    --combine-options "--saveShapes --floatAllNuisances --saveNormalizations"

# Simultaneous fit across all jet bins (one shared mZ and σ)
law run RunCombine \
    --datacard-config analyses/CMS_Run2016_DoubleMuon/cfg.yaml \
    --name zpeak_combined \
    --datacard-path zpeak_fit/datacard_combined.txt \
    --method FitDiagnostics \
    --combine-options "--saveShapes --floatAllNuisances --saveNormalizations"
```

Results land in `combineRun_<name>/combine_results/fitDiagnostics_*.root`.

### Step 3 — Inspect results

```bash
python - << 'EOF'
import ROOT
f = ROOT.TFile("combineRun_zpeak_0j/combine_results/fitDiagnostics__0j.root")
f.fit_s.Print("v")   # signal+background fit result with all parameter values
EOF
```

### Combine datacard structure (analytic shapes)

```
# datacard_0j.txt
imax 1
jmax 1
kmax 0
shapes zpeak    0j  zpeak_fit/ws_0j.root  ws_0j:zpeak_0j
shapes bkg      0j  zpeak_fit/ws_0j.root  ws_0j:bkg_0j
shapes data_obs 0j  zpeak_fit/ws_0j.root  ws_0j:data_obs_0j
bin          0j
observation  -1
bin          0j      0j
process      zpeak   bkg
process      0       1
rate         -1      -1   # yields read from workspace extended PDFs
```

---

## Extending the Example

### Add more histograms

Edit `histograms.yaml` only — no C++ changes needed:

```yaml
- name: DimuonRapidity
  variable: DimuonRapidity
  weight: eventWeight
  bins: 50
  lowerBound: -2.5
  upperBound: 2.5
```

Then add one `Define` in `analysis.cc`:

```cpp
an.Define("DimuonRapidity",
    [](const PhysicsObjectCollection &m) {
        return (Float_t)makePairs(m)[0].p4.Rapidity();
    }, {"goodMuons"});
```

### Add a new trigger group (e.g. single-muon)

Edit `triggers.yaml` only:

```yaml
- name: single_muon
  sample: "1"      # must match 'type' in cfg.yaml for that sample
  triggers: HLT_IsoMu24,HLT_IsoTkMu24
```

### Add MC samples for background estimation

1. Add MC entries to `dataset_manifest.yaml`
2. Define `eventWeight` using `xsec × lumi / sum_weights` via `floats.yaml`
3. Create a `datacard_config.yaml` and use `python core/python/create_datacards.py` to
   generate histogram-based shape datacards for the combined fit

### Use RegionManager for analysis regions

```cpp
auto regionMgr = std::make_unique<RegionManager>();
an.addPlugin("regionManager", std::move(regionMgr));
auto *rm = an.getPlugin<RegionManager>("regionManager");
rm->declareRegion("signal",   "inMassWindow");
rm->declareRegion("sideband", "!inMassWindow");
cfm->bindToRegionManager(rm);   // per-region cutflow tables
```

---

## References

- [Config Reference](../../docs/CONFIG_REFERENCE.md)
- [Combine Integration](../../docs/COMBINE_INTEGRATION.md)
- [LAW Batch Submission](../../law/README.md)
- [PhysicsObjectCollection](../../core/interface/PhysicsObjectCollection.h)
- [CutflowManager](../../core/plugins/CutflowManager/CutflowManager.h)
- [TriggerManager](../../core/plugins/TriggerManager/TriggerManager.h)
- [CMS Open Data Record 30522](https://opendata.cern.ch/record/30522)
- [CMS Combine](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/)
- [CombineHarvester](https://cms-analysis.github.io/CombineHarvester/)
