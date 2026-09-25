# Getting Started with RDFAnalyzerCore

This guide will help you get up and running with RDFAnalyzerCore quickly — from installation all the way to writing and running your first analysis.

## Prerequisites

- ROOT 6.30/02 or later (progress bar support requires 6.30+)
- CMake 3.19.0 or later
- C++17 compatible compiler
- Git

## Quick Start


> **Important:** ROOT is a C++ framework and is **not** installed via `pip install ROOT`.
> Use `source env.sh` (CVMFS environments) or source an existing ROOT installation with `source <root-install>/bin/thisroot.sh`.

### 1. Clone the Repository

```bash
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
cd RDFAnalyzerCore
```

### 2. Set Up Environment

On a CVMFS-backed HEP host such as lxplus, cmsconnect, or a site-managed analysis node:
```bash
source env.sh
```

This script sets up ROOT and other required dependencies from CVMFS.

If you are using a standalone ROOT installation instead, source that environment before building.

### 3. Build the Framework

```bash
source build.sh
```

This will:
- Configure a Release build in `build/` with the defaults used by `build.sh`
- Download ONNX Runtime automatically
- Build the core framework
- Discover and build any analyses in the `analyses/` directory
- Run the registered CTest suite

The build artifacts will be placed in the `build/` directory.

To rerun the test suite later from the repository root:

```bash
source test.sh
```

### 4. Run the Example Analysis

```bash
cd analyses/ExampleAnalysis
../../build/analyses/ExampleAnalysis/analysis cfg.yaml
```

This runs the Z→μμ example on ATLAS Open Data. Its configured inputs are remote EOS files, so you need XRootD access or must replace `fileList` in `cfg.yaml` with accessible ROOT files. The config sets `enableSkim: false`, so this run does not write an event tree to `saveFile`; `saveConfig: output.txt` is used only if skim writing is enabled. The booked histograms are written to `Output/ZMuMuL_meta.root`, derived from `saveFile` because this config does not set `metaFile`.

## Understanding the Output

`Analyzer::run()` executes registered plugin hooks and writes the outputs requested by the config and analysis:

- **Skim output** (`saveFile`): An event-level ROOT file is written only when `enableSkim` is set to `true`, `True`, or `1`. `saveConfig` controls its branches; without `saveConfig`, the full dataframe is snapshotted.
- **Metadata output** (`metaFile`): Configured histogram managers and services write histograms, counters, and metadata here. If `metaFile` is omitted, the framework derives a sibling path by inserting `_meta` before the `saveFile` extension.

Output locations and skim behavior are specified in the main configuration file.

---

## Your First Analysis — Complete Walkthrough

This section walks you through creating a complete working analysis from scratch. It uses a text-format `cfg.txt` and `histograms.txt`; the actual `analyses/ExampleAnalysis/` uses YAML config files, as shown in the run command above.

Start this walkthrough from the repository root. If you just ran the example
analysis in Quick Start, return from `analyses/ExampleAnalysis` first:

```bash
cd ../..
```

### Step 1: Create Your Analysis Directory

```bash
mkdir -p analyses/MyFirstAnalysis
cd analyses/MyFirstAnalysis
```

### Step 2: Create `CMakeLists.txt`

Every analysis needs a `CMakeLists.txt` so CMake can discover and build it automatically.

```cmake
add_executable(myAnalysis analysis.cc)
target_compile_features(myAnalysis PRIVATE cxx_std_17)

target_include_directories(myAnalysis
    PUBLIC
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/XGBoost-FastForest/include
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/correctionlib/include
        ${RDFAnalyzer_SOURCE_DIR}/core/interface
        ${RDFAnalyzer_SOURCE_DIR}/core/interface/api
)

target_link_libraries(myAnalysis
    PRIVATE
        core
        corePlugins
        ROOT::ROOTDataFrame
        ROOT::ROOTVecOps
        ROOT::Core
        ROOT::Hist
        ROOT::MathCore
        ROOT::RIO
)
```

> **Note**: Do not add a `cmake_minimum_required` or `project()` call — the top-level `CMakeLists.txt` handles that. Just define your target directly.

### Step 3: Create the Main Config File (`cfg.txt`)

The config file tells the framework where to find input data and outputs and gives paths to configuration files used by the analysis or its plugins. Plugins themselves are constructed and registered by analysis code.

```
# cfg.txt — Main analysis configuration
saveTree=Events
threads=-1
saveFile=output/skim.root
metaFile=output/meta.root
enableSkim=true
fileList=/path/to/your/data.root
histogramConfig=histograms.txt
floatConfig=floats.txt
```

Key options:

| Key | Description |
|-----|-------------|
| `fileList` | Comma-separated list of input ROOT files (including `root://` URLs) |
| `saveFile` | Output path for the event-level skim ROOT file when `enableSkim` is true |
| `enableSkim` | Set to `true`, `True`, or `1` to write an event-level skim during `run()` |
| `metaFile` | Optional output path for histograms and metadata; defaults to a sibling of `saveFile` with `_meta` before the extension |
| `saveTree` | Name of the TTree to write in the skim file |
| `threads` | Number of threads (`-1` = all available) |
| `histogramConfig` | Path to the histogram definitions file |
| `floatConfig` | Path to a file defining float constants used by analysis code or histogram weights |
| `treeList` | Comma-separated list of input TTrees to process |
| `antiglobs` | File patterns to exclude from auto-discovered file lists |

For a complete reference, see [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md).

### Step 4: Create the Histogram Configuration (`histograms.txt`)

Each line defines one histogram using `key=value` pairs:

```
# name=<id> variable=<column> weight=<weight_col> bins=<N> lowerBound=<lo> upperBound=<hi> label=<axis label>

name=ZBosonMass     variable=ZBosonMass     weight=normScale bins=60 lowerBound=70.0  upperBound=110.0 label=Z Boson Mass [GeV]
name=LeadingMuonPt  variable=LeadingMuonPt  weight=normScale bins=50 lowerBound=20.0  upperBound=150.0 label=Leading Muon p_{T} [GeV]
name=SubleadingMuonPt variable=SubleadingMuonPt weight=normScale bins=50 lowerBound=20.0 upperBound=150.0 label=Subleading Muon p_{T} [GeV]
```

- `variable` must match a column name defined via `Define()` (or present in the input TTree).
- `weight` is an optional event-weight column (omit the field to use no weight).

Because these histogram examples use `normScale` as a weight, create `floats.txt` beside the config with a definition for that column:

```text
normScale=1.0
```

### Step 5: Write Your Analysis Code (`analysis.cc`)

Below is a complete, self-contained example following the selection and histogram-booking pattern in `ExampleAnalysis`. It uses the tutorial's text-format `cfg.txt` and `histograms.txt` files.

```cpp
#include "analyzer.h"
#include <NDHistogramManager.h>
#include <functions.h>

#include <iostream>

// --- Helper functions ---

// Build a vector of indices for muons passing quality criteria
ROOT::VecOps::RVec<Int_t> getGoodMuons(
    const ROOT::VecOps::RVec<char>  &muonFlag,
    const ROOT::VecOps::RVec<char>  &muonPreselection,
    const ROOT::VecOps::RVec<Float_t> &jetSep,
    ROOT::VecOps::RVec<Float_t>     &pt)
{
    ROOT::VecOps::RVec<Int_t> goodMuons;
    for (int i = 0; i < (int)muonFlag.size(); i++) {
        if (muonFlag[i] == 1 && muonPreselection[i] == 1
                && jetSep[i] > 0.4 && pt[i] >= 20000) {
            goodMuons.push_back(i);
        }
    }
    // Sort so leading muon is first
    if (goodMuons.size() == 2 && pt[goodMuons[0]] < pt[goodMuons[1]])
        std::swap(goodMuons[0], goodMuons[1]);
    return goodMuons;
}

// Build a Lorentz vector for muon at compile-time index `ind`
template<unsigned int ind>
inline ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>>
getMuon(ROOT::VecOps::RVec<Int_t> &goodMuons,
        ROOT::VecOps::RVec<Float_t> &pt,
        ROOT::VecOps::RVec<Float_t> &eta,
        ROOT::VecOps::RVec<Float_t> &phi)
{
    Int_t idx = goodMuons[ind];
    return {pt[idx], eta[idx], phi[idx], 0.f};
}

// Event-level filters
bool twoMuons(ROOT::VecOps::RVec<Int_t> goodMuons) { return goodMuons.size() == 2; }
bool massFilter(Float_t mass)                        { return mass <= 150000; }

// Unit conversion helpers
Float_t scaleDown(Float_t mass)                      { return mass / 1000.0f; }

Float_t getLeadingMuonPt(ROOT::VecOps::RVec<Int_t> &goodMuons,
                          ROOT::VecOps::RVec<Float_t> &pt)
{ return goodMuons.size() > 0 ? pt[goodMuons[0]] / 1000.0f : 0.f; }

Float_t getSubleadingMuonPt(ROOT::VecOps::RVec<Int_t> &goodMuons,
                              ROOT::VecOps::RVec<Float_t> &pt)
{ return goodMuons.size() > 1 ? pt[goodMuons[1]] / 1000.0f : 0.f; }


// --- Main ---

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: myAnalysis <cfg.txt>" << std::endl;
        return 1;
    }

    // 1. Create the Analyzer from your config file
    auto an = Analyzer(argv[1]);

    // 2. Attach the NDHistogramManager plugin (reads histogramConfig from cfg.txt)
    auto histManager = std::make_unique<NDHistogramManager>(
        an.getConfigurationProvider());
    an.addPlugin("histogramManager", std::move(histManager));

    // 3. Define columns and apply filters (chained RDataFrame style)
    an.Define("goodMuons", getGoodMuons,
              {"AnalysisMuonsAuxDyn.DFCommonMuonPassIDCuts",
               "AnalysisMuonsAuxDyn.DFCommonMuonPassPreselection",
               "AnalysisMuonsAuxDyn.DFCommonJetDr",
               "AnalysisMuonsAuxDyn.pt"})
      ->Filter("twoMuons", twoMuons, {"goodMuons"})
      ->Define("LeadingMuonVec",    getMuon<0>,
               {"goodMuons", "AnalysisMuonsAuxDyn.pt",
                "AnalysisMuonsAuxDyn.eta", "AnalysisMuonsAuxDyn.phi"})
      ->Define("SubleadingMuonVec", getMuon<1>,
               {"goodMuons", "AnalysisMuonsAuxDyn.pt",
                "AnalysisMuonsAuxDyn.eta", "AnalysisMuonsAuxDyn.phi"})
      ->Define("ZBosonVec",
               sumLorentzVec<ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>>>,
               {"LeadingMuonVec", "SubleadingMuonVec"})
      ->Define("ZBosonMassScaled",
               getLorentzVecM<Float_t, ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<Float_t>>>,
               {"ZBosonVec"})
      ->Filter("ZBosonMassFilter", massFilter, {"ZBosonMassScaled"})
      ->Define("ZBosonMass",      scaleDown,          {"ZBosonMassScaled"})
      ->Define("LeadingMuonPt",   getLeadingMuonPt,   {"goodMuons", "AnalysisMuonsAuxDyn.pt"})
      ->Define("SubleadingMuonPt",getSubleadingMuonPt,{"goodMuons", "AnalysisMuonsAuxDyn.pt"});

    // 4. Book histograms declared in histograms.txt
    an.bookConfigHistograms();

    // 5. Execute the dataframe and write all output
    an.run();

    return 0;
}
```

**Key concepts illustrated above:**

| Concept | What it does |
|---------|-------------|
| `Analyzer(argv[1])` | Reads `cfg.txt` and sets up the ROOT RDataFrame |
| `addPlugin(...)` | Attaches a plugin (e.g. `NDHistogramManager`) to the analysis |
| `Define(name, func, cols)` | Creates a new derived column from existing branches |
| `Filter(name, func, cols)` | Applies a boolean selection; events failing are dropped |
| `bookConfigHistograms()` | Reads `histogramConfig` and books all histograms in one call |
| `run()` | Runs plugin hooks, conditionally writes the skim, saves registered ND histograms, and finalizes services and provenance |

### Step 6: Build the Analysis

The previous steps leave the shell in `analyses/MyFirstAnalysis`. Return to
the repository root before running `build.sh`; CMake discovers your analysis
directory during configuration:

```bash
cd ../..
source build.sh
```

For faster incremental rebuilds, you can skip the test suite:

```bash
(cd build && make -j$(nproc))
```

### Step 7: Run the Analysis

Run from the analysis directory when your input or output paths are relative,
so those paths resolve from the expected location. Referenced config files are
resolved from the working directory when present, then from the main config's
directory:

```bash
# Return to the analysis directory so relative paths in cfg.txt resolve there.
cd analyses/MyFirstAnalysis
../../build/analyses/MyFirstAnalysis/myAnalysis cfg.txt
```

The executable always takes one argument: the path to `cfg.txt`.

### Step 8: Examine the Output

With the example config above (`enableSkim=true` and an explicit `metaFile`),
the run writes these ROOT files. The skim is written only when `enableSkim` is
enabled, and histogram output requires a registered histogram manager with
booked histograms:

| File | Contents |
|------|---------|
| `output/skim.root` | Flat TTree (`Events`) with all saved branches for selected events |
| `output/meta.root` | Histograms and metadata/counters produced by configured managers and services |

Open the skim with ROOT when it was enabled:
```bash
root -l output/skim.root
# In the ROOT prompt:
# Events->Print()           — list all branches
# Events->Draw("ZBosonMass") — quick plot
```

---

## Configuration Overview

A typical analysis uses up to four configuration files:

```
analyses/MyFirstAnalysis/
├── analysis.cc        ← C++ analysis code
├── CMakeLists.txt     ← build target definition
├── cfg.txt            ← main config (I/O paths, plugins, options)
├── histograms.txt     ← histogram definitions (variable, bins, range)
├── floats.txt         ← float constants such as normScale
└── datasets.yaml      ← (optional) dataset manifest for batch processing
```

### `cfg.txt` — Main Configuration

Controls all runtime behaviour: input files, output paths, thread count, and which histogram/correction/ML config files to load.

```
saveTree=Events
threads=-1
saveFile=output/skim.root
fileList=/path/to/file1.root,/path/to/file2.root
histogramConfig=histograms.txt
```

### `histograms.txt` — Histogram Definitions

One histogram per line, using `key=value` pairs:

```
name=ZBosonMass variable=ZBosonMass weight=normScale bins=60 lowerBound=70.0 upperBound=110.0 label=Z Boson Mass [GeV]
```

### `datasets.yaml` — Dataset Manifest (batch processing)

Used by the LAW workflow manager to describe collections of datasets:

```yaml
lumi: 59740.0
datasets:
  - name: ZMuMu_mc20
    dtype: mc
    files:
      - root://eospublic.cern.ch//eos/opendata/atlas/...file1.root
      - root://eospublic.cern.ch//eos/opendata/atlas/...file2.root
```

---

## Development Workflow

### Creating a New Analysis

1. **Create or clone an analysis repository inside `analyses/`**
   ```bash
   cd analyses
   git clone <your-analysis-repo>
   # OR
   mkdir MyAnalysis && cd MyAnalysis
   ```

2. **Add the four required files**: `CMakeLists.txt`, `cfg.txt`, `histograms.txt`, `analysis.cc`

3. **Build everything from the repo root**
   ```bash
   cd ../../
   source build.sh
   ```

4. **Run your analysis from the config directory**
   ```bash
   cd analyses/MyAnalysis
   ../../build/analyses/MyAnalysis/myAnalysis cfg.txt
   ```

5. **Iterate**: Edit `analysis.cc`, then rebuild with `(cd build && make -j$(nproc))`

### Incremental Development Tips

- Rebuild incrementally with `(cd build && make -j$(nproc))` (faster than re-running `build.sh`).
- From the repository root, use `source cleanBuild.sh` to remove `build/` and configure a clean build after major CMake changes.
- Run from the analysis directory when your input/output paths are relative; referenced config files also fall back to the main config's directory.
- Set `threads=1` in `cfg.txt` while debugging to get deterministic output and cleaner stack traces.
- From the repository root, use `source test.sh` to rerun the registered test suite.

---

## Running with Batch Processing

Once your analysis works locally, scale it up to many datasets using the LAW workflow manager included in `core/python/law/`.

The tutorial run leaves the shell in `analyses/MyFirstAnalysis`; return to the
repository root before using these repository-relative paths:

```bash
cd ../..
```

### Quick Batch Submission with `SkimTask`

1. **Source the LAW environment**
   ```bash
   source core/python/law/env.sh
   ```

2. **Index available tasks**
   ```bash
   law index
   ```

3. **Submit your analysis as a batch job**
   ```bash
   law run SkimTask \
     --exe ./build/analyses/MyFirstAnalysis/myAnalysis \
     --name myRun \
     --dataset-manifest analyses/MyFirstAnalysis/datasets.yaml \
         --submit-config core/python/law/submit_config.txt \
         --workflow htcondor
   ```

`SkimTask` is the common entrypoint across execution modes:

- `--workflow local` runs branches locally.
- `--workflow dask` sends prepared jobs to Dask workers.
- `--workflow htcondor` delegates to the concrete skim submission chain and creates per-dataset Condor bundles under `skimRun_<name>/condor_submissions/`.

For delegated HTCondor runs, submit-side logs live under each dataset bundle's `condor_logs/` directory. If a job is held during input transfer, you may only see the `.log` file because `stdout` and `stderr` are created only after the payload starts.

---

## File Organization

After building, your directory structure will look like:

```
RDFAnalyzerCore/
├── core/                 # Framework source code
│   ├── interface/       # Public headers (analyzer.h, etc.)
│   ├── src/             # Implementation files
│   ├── plugins/         # Built-in plugin implementations
│   └── test/            # Unit tests
├── analyses/            # Your analyses live here
│   ├── ExampleAnalysis/ # Reference Z→μμ analysis
│   └── MyFirstAnalysis/ # Your new analysis
├── build/               # CMake build artifacts and compiled binaries
│   └── analyses/
│       └── MyFirstAnalysis/
│           └── myAnalysis   ← your compiled executable
├── core/
│   ├── python/
│   │   └── law/         # LAW batch-processing workflow
├── docs/                # Documentation
├── cmake/               # CMake helper modules
└── README.md            # Main technical documentation
```

---

## Common Issues

For detailed diagnostics, see [Errors and Tracing](ERRORS_AND_TRACING.md),
[Configuration Validation](CONFIGURATION_VALIDATION.md),
[Output Schema](OUTPUT_SCHEMA.md), and [Batch Submission](BATCH_SUBMISSION.md).

### Build Fails with ROOT Not Found

Ensure ROOT is properly sourced before building:
```bash
source env.sh                          # CVMFS-backed environment
# OR
source <root-install>/bin/thisroot.sh  # Standalone ROOT installation
```

### ONNX Runtime Download Fails

The build downloads ONNX Runtime from GitHub Releases. If the build host cannot
reach that service, download and extract the runtime separately, then configure
CMake with `-DONNXRUNTIME_ROOT_DIR=/path/to/onnxruntime`. That directory must
contain `include/` and `lib/` subdirectories.

### Analysis Not Found During Build

Analyses are auto-discovered by CMake. Your analysis directory must:
- Be located directly under `analyses/`
- Contain a valid `CMakeLists.txt`

Analysis directories are discovered when they are directly under `analyses/`
and contain a `CMakeLists.txt`; Git tracking is not required. If you add or
change CMake files, run `source cleanBuild.sh` from the repository root to
remove the existing build tree and configure/build again.

### Segfault / Crash During Execution

- Check that all column names in `Define`/`Filter` match actual branches in your input TTree
- Use `threads=1` to simplify debugging
- Run with `valgrind` or `gdb` against the binary in `build/analyses/MyAnalysis/`

---

## Testing

Run the full test suite to verify your installation:

```bash
source test.sh
```

Or run individual tests:

```bash
cd build
ctest -R TestName -V   # -V for verbose output
```

---

## Applying Systematics to Physics Object Collections

One of the most powerful features of RDFAnalyzerCore is how easily it supports
applying systematic variations directly to **physics object collections** (jets,
electrons, muons, taus, …). This is particularly streamlined for CMS-style
corrections using correctionlib payloads.

### Manual Kinematic Corrections

`PhysicsObjectCollection` provides first-class support for corrected kinematics.
Once you have a collection, you can produce a corrected version in one call:

```cpp
#include <PhysicsObjectCollection.h>

// Build a jet collection
analyzer.Define("goodJets",
    [](const RVec<float>& pt, const RVec<float>& eta,
       const RVec<float>& phi, const RVec<float>& mass) {
        return PhysicsObjectCollection(pt, eta, phi, mass,
                                       (pt > 25.f) && (abs(eta) < 2.4f));
    },
    {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
);

// Apply a corrected-pT column to the collection — produces a new collection
// with updated 4-vectors and the same object selection.
analyzer.Define("goodJets_corrected",
    [](const PhysicsObjectCollection& jets,
       const RVec<float>& corrected_pt) {
        return jets.withCorrectedPt(corrected_pt);
    },
    {"goodJets", "Jet_pt_corrected"}
);
```

`withCorrectedKinematics(pt, eta, phi, mass)` is available when all four
4-vector components are corrected (e.g. JES corrections with mass rescaling).

### CMS-Style Corrections with JetEnergyScaleManager

For CMS NanoAOD analyses, the **`JetEnergyScaleManager`** plugin handles the
full JES/JER workflow — including stripping the embedded NanoAOD JEC,
applying a new correctionlib-based JEC, propagating uncertainties, building
corrected `PhysicsObjectCollection` outputs, and performing Type-1 MET
propagation — with just a handful of API calls:

```cpp
#include <JetEnergyScaleManager.h>
#include <PhysicsObjectCollection.h>

auto jes = analyzer.getPlugin<JetEnergyScaleManager>("jes");
auto cm  = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare which branches hold jet/MET kinematics.
jes->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");;
jes->setMETColumns("MET_pt", "MET_phi");

// 2. Build a selected jet collection (any selection criteria you like).
analyzer.Define("goodJets",
    [](const RVec<float>& pt, const RVec<float>& eta,
       const RVec<float>& phi, const RVec<float>& mass) {
        return PhysicsObjectCollection(pt, eta, phi, mass,
                                       (pt > 25.f) && (abs(eta) < 2.4f));
    },
    {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
);

// 3. Strip the NanoAOD JEC (applies Jet_rawFactor) to get raw pT.
jes->removeExistingCorrections("Jet_rawFactor");

// 4. Evaluate the nominal compound JEC from the correctionlib payload.
//    Mixed per-jet (RVec) and per-event (scalar rho) inputs are handled automatically.
cm->registerCorrection(
    "jec_nominal",
    "jet_jerc.json.gz",
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
cm->applyCorrectionVec("jec_nominal", {}, {}, "Jet_jec_sf_nominal");
jes->applyCorrection("Jet_pt_raw", "Jet_jec_sf_nominal", "Jet_pt_jec");

// 5. Register CMS JES systematic sources and apply them in one call.
//    Each source automatically produces _up and _down variation columns.
jes->registerSystematicSources("reduced", {"Total"});
jes->applySystematicSet(*cm, "jes_unc", "reduced",
                        "Jet_pt_jec", "Jet_pt_jes");

// 6. Propagate JEC and JES variations to MET (Type-1 correction).
jes->propagateMET("MET_pt", "MET_phi",
                  "Jet_pt_raw", "Jet_pt_jec",
                  "MET_pt_jec", "MET_phi_jec");

// 7. Produce corrected PhysicsObjectCollection columns for nominal + all
//    systematic variations, and bundle them into a variation map.
jes->setInputJetCollection("goodJets");
jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
jes->defineVariationCollections("goodJets_jec", "goodJets",
                                "goodJets_variations");
```

After `analyzer.run()` this creates:

| Column | Description |
|--------|-------------|
| `goodJets_jec` | `PhysicsObjectCollection` — nominal JEC-corrected jets |
| `goodJets_TotalUp` | `PhysicsObjectCollection` — JES Total up variation |
| `goodJets_TotalDown` | `PhysicsObjectCollection` — JES Total down variation |
| `goodJets_variations` | `PhysicsObjectVariationMap` — all of the above keyed by name |

### Automatic Systematic Propagation

Because `JetEnergyScaleManager` registers the nominal collection name with
`SystematicManager`, any **downstream** `Define(...)` call that consumes the
nominal collection is **automatically expanded** into up/down variants:

```cpp
// Defined once — the framework automatically creates:
//   selectedJetPts             (nominal)
//   selectedJetPts_TotalUp     (JES up)
//   selectedJetPts_TotalDown   (JES down)
analyzer.Define("selectedJetPts",
    [](const PhysicsObjectCollection& jets) {
        RVec<float> pts;
        for (std::size_t i = 0; i < jets.size(); ++i)
            pts.push_back(static_cast<float>(jets.at(i).Pt()));
        return pts;
    },
    {"goodJets"}, *sysMgr   // ← passing sysMgr enables automatic propagation
);
```

No manual duplication of your physics logic for each systematic — the
framework handles it.

### Using the Full CMS JES Source Set

For a complete set of CMS Run 3 JES uncertainty sources, expand the
registration call:

```cpp
jes->registerSystematicSources("full", {
    "AbsoluteCal", "AbsoluteScale", "AbsoluteMPFBias",
    "FlavorQCD", "Fragmentation", "PileUpDataMC",
    "PileUpPtRef", "RelativeFSR", "RelativeJEREC1",
    "RelativeJEREC2", "RelativeJERHF",
    "RelativePtBB", "RelativePtEC1", "RelativePtEC2",
    "RelativePtHF", "RelativeBal", "RelativeSample"
});
jes->applySystematicSet(*cm, "jes_unc", "full", "Jet_pt_jec", "Jet_pt_jes");
// Creates 34 variation columns and registers 17 systematic families in one call.
```

---

## Working-Point Tagger Corrections with TaggerWorkingPointManager

For CMS analyses that apply b-tagging or other tagger-based selections (e.g.
with DeepJet, RobustParTAK4, ParticleNet, or DeepTau), the
**`TaggerWorkingPointManager`** plugin handles the full working-point (WP)
workflow for **any physics object** — registering WP thresholds, computing
per-object WP categories, applying correctionlib-based scale factors, and
producing WP-filtered `PhysicsObjectCollection` outputs — with full systematic
variation support.

For CMS BTV **fixed working point** weights, the recommended event-weight
formula is category-dependent and uses analyzer-derived MC efficiencies. Use
`defineFixedWorkingPointWeight()` for that workflow rather than treating the
fixed-WP SFs as a plain product of per-object weights.

### Defining Working Points and Applying Scale Factors

```cpp
#include <TaggerWorkingPointManager.h>

auto twm = analyzer.getPlugin<TaggerWorkingPointManager>("btagManager");
auto cm  = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare object kinematic columns (jets, taus, or fat-jets).
twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");

// 2. Set the tagger discriminator score column.
twm->setTaggerColumn("Jet_btagDeepFlavB");

// 3. Register working points in ascending threshold order.
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);

// 4. Declare the input PhysicsObjectCollection.
twm->setInputObjectCollection("goodJets");

// 5. Apply nominal SF from a correctionlib payload.
twm->applyCorrectionlib(*cm, "deepjet_sf", {"central"},
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
// → creates per-event weight column "deepjet_sf_central_weight"

// 6. Register and apply CMS b-tag systematic sources.
twm->registerSystematicSources("standard",
    {"hf", "lf", "hfstats1", "hfstats2", "lfstats1", "lfstats2",
     "cferr1", "cferr2"});
twm->applySystematicSet(*cm, "deepjet_sf", "standard",
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
// → creates "deepjet_sf_hf_up_weight", "deepjet_sf_hf_down_weight", …
```

For BTV fixed-WP corrections, first merge the appropriate flavour-family SF
payloads (for example `<tagger>_comb` plus `<tagger>_light` for b-tagging),
then build the event weight with:

```cpp
twm->defineFixedWorkingPointWeight(
    {"btag_sf_L_central", "btag_sf_M_central", "btag_sf_T_central"},
    {"btag_eff_L", "btag_eff_M", "btag_eff_T"},
    "btag_weight_nominal");
```

The efficiency columns are analyzer-derived MC tagging efficiencies and are not
shipped by the CMS BTV payloads.

### WP-Based Collection Selections

The manager supports expressive selection strings for filtering object collections:

```cpp
// Jets passing the medium WP (score ≥ 0.3033).
twm->defineWorkingPointCollection("pass_medium", "goodJets_bmedium");

// Jets failing the loose WP (score < 0.0521) — b-tag control region.
twm->defineWorkingPointCollection("fail_loose", "goodJets_bfail");

// Jets passing loose but failing medium (i.e. between the two WPs).
twm->defineWorkingPointCollection("pass_loose_fail_medium", "goodJets_bL_notM");

// Jets passing the tight WP.
twm->defineWorkingPointCollection("pass_tight", "goodJets_btight");
```

The per-object WP category column (`<ptCol>_wp_category`, e.g. `Jet_pt_wp_category`) is also
available for direct use in downstream selections:

| category | meaning for [loose, medium, tight] WPs     |
|----------|--------------------------------------------|
| 0        | score < loose threshold (fail all)         |
| 1        | loose ≤ score < medium                     |
| 2        | medium ≤ score < tight                     |
| 3        | score ≥ tight (pass all)                   |

### Systematic Variation Collections

After defining WP-filtered collections, build systematic variation collections
and a variation map for downstream propagation:

```cpp
twm->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
                                 "goodJets_btag_variations");
// Creates:
//   goodJets_btag_hfUp, goodJets_btag_hfDown     (per-variation collections)
//   goodJets_btag_variations  (PhysicsObjectVariationMap)
```

Because tagger corrections modify only the **event weight** (not object
kinematics), all variation collections contain the same objects as the nominal.
The weight difference is tracked via the separate weight columns.

### Generator-Level Fraction Reweighting

This is an optional analyzer-specific utility. It is not the CMS BTV
recommended fixed-WP event-weight method.

For the most accurate results, use a correctionlib payload encoding the
generator-level MC fractions of objects in each WP category at given (pT, η):

```cpp
// Register the fraction correctionlib.
cm->registerCorrection("deepjet_frac", "deepjet_fractions.json",
                        "DeepJetFractions",
                        {"Jet_pt", "Jet_eta", "Jet_pt_wp_category"});
// Enable fraction-weighted SFs (must be called before applyCorrectionlib).
twm->setFractionCorrection(*cm, "deepjet_frac",
                           {"Jet_pt", "Jet_eta", "Jet_pt_wp_category"});
```

This ensures the per-event weights properly reflect the generator-level object
distributions when making WP-based selections.

### Pre-Processing: Computing Fraction Distributions

The fraction correctionlib payload is computed in a **dedicated pre-processing
run** using `defineFractionHistograms()`:

```cpp
// In a dedicated fraction_calc analysis (run before the main analysis):
twm->defineFractionHistograms(
    "deepjet_frac",                             // output prefix
    {20.f, 30.f, 50.f, 100.f, 200.f, 500.f},  // pT bin edges [GeV]
    {0.f, 1.5f, 2.4f},                          // |η| bin edges
    "Jet_hadronFlavour");                        // flavour-separated histograms
```

This books per-(pT, η, flavour) tagger-score histograms in the metadata ROOT
file under `tagger_fractions/`.  The fraction of objects in each WP category can
then be read from these histograms and stored in a correctionlib JSON for use
in the main analysis.

### Further Reading

- **Complete JES/JER reference**: [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md)
- **Tagger WP corrections reference**: [TAGGER_WORKING_POINTS.md](TAGGER_WORKING_POINTS.md)
- **CMS correction stack**: [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)
- **All PhysicsObjectCollection APIs**: [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md)
- **Analysis guide (JES/JER workflow, electron/muon corrections)**: [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md)

---

## Getting Help

- **Documentation**: See the `docs/` directory for detailed guides
- **Working example**: `analyses/ExampleAnalysis/` is a complete, runnable reference
- **Issues**: Open a GitHub issue if you encounter problems
- **README**: [README.md](../README.md) has comprehensive technical documentation

---

## What's Next?

Now that your first analysis is running, explore the full power of the framework:

1. **Config reference**: All config keys explained in [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)
2. **Analysis guide**: Advanced patterns (WeightManager, RegionManager, systematics, ML) in [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md)
3. **Architecture overview**: How the framework is structured in [ARCHITECTURE.md](ARCHITECTURE.md)
4. **Plugin development**: Write your own plugins in [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md)
5. **Machine learning**: Integrate BDTs or neural networks — [ONNX_IMPLEMENTATION.md](ONNX_IMPLEMENTATION.md), [SOFIE_IMPLEMENTATION.md](SOFIE_IMPLEMENTATION.md)
6. **CMS corrections & systematics**: Apply JES/JER, electron, muon corrections with automatic variation propagation — [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md), [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md)
7. **Tagger working-point corrections**: Apply b-tag / tagger SFs with WP-filtered collections and fraction reweighting — [TAGGER_WORKING_POINTS.md](TAGGER_WORKING_POINTS.md)
8. **Physics object collections**: Overlap removal, combinatorics, corrected kinematics — [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md)
9. **Systematics & nuisance groups**: Register and propagate uncertainties — [NUISANCE_GROUPS.md](NUISANCE_GROUPS.md)
10. **Batch processing**: Submit hundreds of jobs — [LAW_TASKS.md](LAW_TASKS.md) and [BATCH_SUBMISSION.md](BATCH_SUBMISSION.md)
11. **Output validation**: Validate and inspect outputs — [VALIDATION_REPORTS.md](VALIDATION_REPORTS.md)

Happy analyzing!
