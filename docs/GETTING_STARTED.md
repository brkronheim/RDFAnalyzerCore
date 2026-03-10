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
     --submit-config law/submit_config.txt
   ```

LAW handles job splitting, submission to the batch system, and output collection automatically.

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
6. **Scale corrections**: Apply per-event scale factors with `CorrectionManager` — [API_REFERENCE.md](API_REFERENCE.md)
7. **Systematics & nuisance groups**: Register and propagate uncertainties — [NUISANCE_GROUPS.md](NUISANCE_GROUPS.md)
8. **Batch processing**: Submit hundreds of jobs — [LAW_TASKS.md](LAW_TASKS.md) and [BATCH_SUBMISSION.md](BATCH_SUBMISSION.md)
9. **Physics objects**: Overlap removal, combinatorics — [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md)
10. **Output validation**: Validate and inspect outputs — [VALIDATION_REPORTS.md](VALIDATION_REPORTS.md)

Happy analyzing!
