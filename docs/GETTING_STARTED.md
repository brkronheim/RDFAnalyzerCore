# Getting Started with RDFAnalyzerCore

This guide will help you get up and running with RDFAnalyzerCore quickly — from installation all the way to writing and running your first analysis.

## Prerequisites

- ROOT 6.30/02 or later (progress bar support was added around 6.28)
- CMake 3.19.0 or later
- C++17 compatible compiler
- Git

## Quick Start

### 1. Clone the Repository

```bash
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
cd RDFAnalyzerCore
```

### 2. Set Up Environment

On lxplus (CERN computing):
```bash
source env.sh
```

This script sets up ROOT and other required dependencies from CVMFS.

For local installations, ensure ROOT is available in your PATH and environment.

### 3. Build the Framework

```bash
source build.sh
```

This will:
- Configure CMake with the appropriate presets
- Download ONNX Runtime automatically
- Build the core framework
- Discover and build any analyses in the `analyses/` directory
- Run tests to verify the installation

The build artifacts will be placed in the `build/` directory.

### 4. Run the Example Analysis

```bash
cd analyses/ExampleAnalysis
../../build/analyses/ExampleAnalysis/analysis cfg.txt
```

This runs a Z→μμ analysis on ATLAS Open Data.

## Understanding the Output

The framework produces two types of output:

1. **Skim Output** (`saveFile`): Event-level ROOT file with selected branches
2. **Metadata/Histogram Output** (`metaFile`): Histograms, counters, and analysis metadata

Output locations are specified in your `cfg.txt` configuration file.

---

## Your First Analysis — Complete Walkthrough

This section walks you through creating a complete working analysis from scratch. The structure mirrors `analyses/ExampleAnalysis/`.

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

The config file tells the framework where to find input data, where to write output, and which plugins to activate.

```
# cfg.txt — Main analysis configuration
saveTree=Events
threads=-1
saveFile=output/skim.root
fileList=/path/to/your/data.root
histogramConfig=histograms.txt
```

Key options:

| Key | Description |
|-----|-------------|
| `fileList` | Comma-separated list of input ROOT files (supports `xrootd://` paths) |
| `saveFile` | Output path for the event-level skim ROOT file |
| `saveTree` | Name of the TTree to write in the skim file |
| `threads` | Number of threads (`-1` = all available) |
| `histogramConfig` | Path to the histogram definitions file |
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

### Step 5: Write Your Analysis Code (`analysis.cc`)

Below is a complete, self-contained example based on the real `ExampleAnalysis`. It selects Z→μμ candidates and books histograms using `NDHistogramManager`.

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
    an.save();

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
| `save()` | Triggers event loop execution and writes skim + histogram files |

### Step 6: Build the Analysis

From the repository root, run a full build. CMake will automatically discover your new analysis directory:

```bash
cd /path/to/RDFAnalyzerCore
source build.sh
```

For faster incremental rebuilds, you can skip the test suite:

```bash
(cd build && make -j$(nproc))
```

### Step 7: Run the Analysis

Always run the analysis executable from the directory containing `cfg.txt` so
that relative paths in the config resolve correctly:

```bash
cd analyses/MyFirstAnalysis
../../build/analyses/MyFirstAnalysis/myAnalysis cfg.txt
```

The executable always takes one argument: the path to `cfg.txt`.

### Step 8: Examine the Output

After a successful run you will find two ROOT files:

| File | Contents |
|------|---------|
| `output/skim.root` | Flat TTree (`Events`) with all saved branches for selected events |
| `output/meta.root` (if configured) | Histograms, cutflow counter, and analysis metadata |

Open them with ROOT:
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
- Use `source cleanBuild.sh` when you add new files or change `CMakeLists.txt`.
- Always run the executable from the analysis directory so relative paths in `cfg.txt` resolve correctly.
- Set `threads=1` in `cfg.txt` while debugging to get deterministic output and cleaner stack traces.
- Use `source buildTest.sh` to build and run the unit test suite.

---

## Running with Batch Processing

Once your analysis works locally, scale it up to many datasets using the LAW workflow manager included in the `law/` directory.

### Quick Batch Submission with `SkimTask`

1. **Source the LAW environment**
   ```bash
   source law/env.sh
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
         --submit-config law/submit_config.txt \
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
├── law/                 # LAW batch-processing workflow
├── docs/                # Documentation
├── cmake/               # CMake helper modules
└── README.md            # Main technical documentation
```

---

## Common Issues

### Build Fails with ROOT Not Found

Ensure ROOT is properly sourced before building:
```bash
source env.sh                          # On lxplus / CVMFS
# OR
source /path/to/root/bin/thisroot.sh   # Local installation
```

### ONNX Runtime Download Fails

The build automatically downloads ONNX Runtime from GitHub Releases. If this fails:
- Check your internet connection and proxy settings
- On restricted clusters, download manually and place in `core/extern/`

### Analysis Not Found During Build

Analyses are auto-discovered by CMake. Your analysis directory must:
- Be located directly under `analyses/`
- Contain a valid `CMakeLists.txt`
- Be tracked by Git (committed, or registered via `.gitmodules` for submodules)

Run `source cleanBuild.sh` to fully reconfigure and rebuild.

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

auto* jes = analyzer.getPlugin<JetEnergyScaleManager>("jes");
auto* cm  = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare which branches hold jet/MET kinematics.
jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
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

After `analyzer.save()` this creates:

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

### Further Reading

- **Complete JES/JER reference**: [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md)
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
7. **Physics object collections**: Overlap removal, combinatorics, corrected kinematics — [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md)
8. **Systematics & nuisance groups**: Register and propagate uncertainties — [NUISANCE_GROUPS.md](NUISANCE_GROUPS.md)
9. **Batch processing**: Submit hundreds of jobs — [LAW_TASKS.md](LAW_TASKS.md) and [BATCH_SUBMISSION.md](BATCH_SUBMISSION.md)
10. **Output validation**: Validate and inspect outputs — [VALIDATION_REPORTS.md](VALIDATION_REPORTS.md)

Happy analyzing!
