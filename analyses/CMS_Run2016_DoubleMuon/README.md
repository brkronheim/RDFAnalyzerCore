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
    ├── histograms/DimuonMass_{0j,1j,2j,ge3j}   ◄── fit inputs
    ├── histograms/DimuonMass (inclusive)
    ├── histograms/nGoodJets, LeadMuPt, …
    └── cutflow / cutflow_nminus1
         │
         ▼
   zpeak_workspace.yaml          law run AnalyticWorkspaceFitTask
    (Voigtian+exp model) ──────────────────────────────────────►
                                 per-channel + simultaneous fits
                                 ws_*.root, datacard_*.txt
                                 fitDiagnostics_*.root
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
| `zpeak_workspace.yaml` | Generic fit model config (signal+background PDFs, channels) |
| `dataset_manifest.yaml` | LAW OpenData task descriptor |
| `fit_zpeak.py` | Standalone workspace builder; delegates to `AnalyticWorkspaceFitTask` |
| `CMakeLists.txt` | CMake build (target: `cms_doublemu_analysis`) |

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
./cms_doublemu_analysis ../../../analyses/CMS_Run2016_DoubleMuon/cfg.yaml
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

### Option A: File listing + local SkimTask (recommended for smaller datasets)

```bash
source law/env.sh && law index

# Discover files from the Open Data portal (one branch per sample, parallelized)
law run GetOpenDataFileList \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis \
    --workers 4

# Skim all datasets (pre-flight test job runs automatically by default)
# RunSkimTestJob → PrepareSkimJobs → SkimTask
law run SkimTask \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis \
    --submit-config analyses/CMS_Run2016_DoubleMuon/cfg.yaml \
    --dataset-manifest analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --file-source opendata \
    --file-source-name run2016g \
    --workers 4

# Disable the pre-flight test if desired
law run SkimTask \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis \
    --submit-config analyses/CMS_Run2016_DoubleMuon/cfg.yaml \
    --dataset-manifest analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --no-make-test-job \
    --workers 4
```

### Option B: HTCondor batch submission (for large datasets)

The `PrepareOpenDataSample` task queries
`https://opendata.cern.ch/api/records/30522` to discover all files
and create per-job configs for HTCondor batch submission.  File-existence
verification during monitoring uses directory-level `xrdfs ls` queries for
efficiency.

```bash
source law/env.sh && law index

# Step 1 — Create per-job configs (30 files per job, one branch per sample)
law run PrepareOpenDataSample \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis \
    --files 30 \
    --workers 4

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
#           (EOS file checks now use directory-level xrdfs ls for speed)
law run MonitorOpenDataJobs \
    --submit-config analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml \
    --name run2016g_zpeak \
    --exe build/analyses/CMS_Run2016_DoubleMuon/analysis
```

Because law tracks task outputs, you can jump straight to step 4 and law will
automatically run steps 1–3 if they haven't completed yet.

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

## Z Peak Fit — Generic `AnalyticWorkspaceFitTask`

The fit is driven entirely by `zpeak_workspace.yaml` and the generic
`AnalyticWorkspaceFitTask` in `law/combine_tasks.py`.  To fit a different
resonance (J/ψ, Υ, H→γγ, …) you only need a different workspace config YAML
— no code changes.

### Workspace config (`zpeak_workspace.yaml`)

```yaml
observable: {name: mass, title: "m_{#mu#mu} [GeV]", lo: 70.0, hi: 110.0}

signal:
  pdf: voigtian          # voigtian|gaussian|crystalball|double_gaussian|breit_wigner
  parameters:
    mean:  {init: 91.19, min: 88.0, max: 94.0, shared: true}   # shared mZ
    width: {init: 2.495, fixed: true, shared: true}             # PDG width
    sigma: {init: 2.0,   min: 0.3,   max: 6.0,  shared: true}  # shared σ

background:
  pdf: exponential       # exponential|polynomial|chebychev|bernstein
  parameters:
    decay: {init: -0.05, min: -0.5, max: -0.001, shared: false} # per-channel

channels:
  - {name: "0j",   histogram: "DimuonMass_0j",   label: "0 jets"}
  - {name: "1j",   histogram: "DimuonMass_1j",   label: "1 jet"}
  - {name: "2j",   histogram: "DimuonMass_2j",   label: "2 jets"}
  - {name: "ge3j", histogram: "DimuonMass_ge3j", label: "≥3 jets"}
```

Parameters with `shared: true` receive a name **without** a channel suffix
(e.g. `mean`, `sigma`).  Combine ties these across channels automatically in
the combined simultaneous fit, implementing a shared Z mass and detector
resolution across all jet bins.

### Step 1 — Run via `AnalyticWorkspaceFitTask` (recommended)

```bash
source law/env.sh && law index

law run AnalyticWorkspaceFitTask \
    --name zpeak_run2016g \
    --workspace-config analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml \
    --histogram-file output/dimuon_zpeak.root \
    --method FitDiagnostics \
    --combine-options "--saveShapes --floatAllNuisances --saveNormalizations"
```

Outputs land in `analyticFit_zpeak_run2016g/`:

```
analyticFit_zpeak_run2016g/
  ws_0j.root / ws_1j.root / ws_2j.root / ws_ge3j.root
  datacard_0j.txt / datacard_1j.txt / datacard_2j.txt / datacard_ge3j.txt
  datacard_combined.txt   ← simultaneous fit across all jet bins
  combine_0j.log / …
  fitDiagnostics_0j.root / …
  provenance.json
  analytic_fit.perf.json
```

### Step 1 (alternative) — Standalone `fit_zpeak.py`

For quick workspace inspection without a LAW environment:

```bash
python analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py \
    --workspace-config analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml \
    --input  output/dimuon_zpeak.root \
    --outdir zpeak_fit
```

The script builds workspaces and datacards, then prints the exact
`law run AnalyticWorkspaceFitTask` command to run the full fit.

### Step 2 — Inspect results

```bash
python - << 'EOF'
import ROOT
f = ROOT.TFile("analyticFit_zpeak_run2016g/fitDiagnostics_0j.root")
f.fit_s.Print("v")   # all parameter values and uncertainties
EOF
```

### Using a different signal or background model

Simply edit `zpeak_workspace.yaml` (or provide a new config):

```yaml
# Crystal Ball signal + Chebyshev background
signal:
  pdf: crystalball
  parameters:
    mean:  {init: 91.2, min: 88.0, max: 94.0, shared: true}
    sigma: {init: 2.0,  min: 0.3,  max: 8.0,  shared: true}
    alpha: {init: 1.5,  min: 0.5,  max: 5.0,  shared: false}
    n:     {init: 2.0,  min: 0.5,  max: 20.0, shared: false}

background:
  pdf: chebychev
  order: 3
  parameters:
    a0: {init: 0.0, min: -5.0, max: 5.0, shared: false}
    a1: {init: 0.0, min: -5.0, max: 5.0, shared: false}
    a2: {init: 0.0, min: -5.0, max: 5.0, shared: false}
```

Then re-run with `--skip-fit` to build workspaces without requiring Combine:

```bash
law run AnalyticWorkspaceFitTask \
    --name zpeak_cb_cheb \
    --workspace-config /tmp/cb_cheb_workspace.yaml \
    --histogram-file output/dimuon_zpeak.root \
    --skip-fit
```

### Combine datacard structure (analytic shapes)

```
# datacard_0j.txt
imax 1
jmax 1
kmax 0   # shape parameters float freely inside the workspace
shapes sig       0j  .../ws_0j.root  ws_0j:sig_0j
shapes bkg       0j  .../ws_0j.root  ws_0j:bkg_0j
shapes data_obs  0j  .../ws_0j.root  ws_0j:data_obs_0j
bin          0j
observation  <n_obs>
bin          0j      0j
process      sig     bkg
process      0       1
rate         -1      -1   # yields from workspace extended PDFs
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
