# Analysis examples and workflow scripts

Start with the [Analyzer SDK example](analyzer-sdk/README.md) to compare three
authoring routes with the same signed-weight scalar physics and output contract:

| Route | Starting point | Runtime requirements |
|---|---|---|
| Direct C++ | Build the Analyzer-linked analysis library and executable | Optional AnalyzerSDK, matching ROOT/native dependencies; compiler at build time |
| Python steering of C++ | Run the same executable through `steer.py` | PythonController plus the executable's native runtime; no Python binding or event callback |
| Python-authored physics | Edit `physics.py`, run `python_analysis.py` | Matching Python component and ROOT/Numba; native CFunc registration, no raw function-pointer management |

All three routes pass private installed/relocated core-only CPU acceptance,
including signed sums, histogram flows/Sumw2 and bundled ONNX runtime mapping.
This establishes finite onboarding, with broader SDK configurations, complex ML,
threading coverage and independent novice trials still separate. See
[exact SDK acceptance and limits](../docs/ANALYZER_SDK.md).

For a smaller Python-only first run, use the
[three-event Python/Numba tutorial](python-authoring/README.md), which includes
explicit input creation, expected outputs and nonexecuting descriptions.

The [compiled native preflight example](native-preflight/README.md) shows
generated settings help and explicit ROOT schema inspection, with zero event
loops, in a C++ library linked into an executable. The same binary can be steered
by the lightweight Python controller.

The [linked native result example](native-results/README.md) uses the optional
`RunResult.h` header with a separately compiled ROOT analysis library, a native
CLI and Python steering. Its finite validation checks signed histogram flows,
variances and direct/steered equality; actual acceptance is recorded separately
from source readiness.

This directory contains example scripts demonstrating the use of RDFAnalyzerCore Python bindings and complete analysis workflows.

The [systematics/inference tutorial](systematics-inference/README.md) shows a
synthetic shape shift, independent weight variations and real native BDT setup
inside a Python-authored event graph. Installed native execution, systematic
migrations, persisted histograms and one-event-loop acceptance pass; the tutorial
guide links the retained evidence.

The [small installed Python/Numba example](python-authoring/README.md) starts
with nonexecuting descriptions, then demonstrates explicit custom Python kernels,
interleaved cuts/definitions and detached results on a three-event fixture.

The [installed compiled-controller example](compiled-controller/README.md)
shows Python steering of a separately compiled C++ executable. Its controller
installation needs no native Python bindings; execution uses the analysis's
native runtime. The binding prerequisites below apply to the other examples.

## Analysis Workflow Scripts

### Complete Analysis Workflow

**File**: `complete_analysis_workflow.sh`

Template script showing a complete end-to-end analysis:
1. Run RDFAnalyzerCore analysis to create histograms
2. Generate CMS Combine datacards
3. Run statistical analysis with Combine
4. Extract results

**Usage**:
```bash
# First, build with Combine support
cmake -S . -B build -DBUILD_COMBINE=ON
cmake --build build -j$(nproc)

# Customize the script for your analysis
vim examples/complete_analysis_workflow.sh

# Run the workflow
./examples/complete_analysis_workflow.sh
```

See [Combine Integration Guide](../docs/COMBINE_INTEGRATION.md) for complete documentation.

---

## Advanced binding examples

The examples below demonstrate lower-level interfaces. The high-level Python
route above manages native bridges and their lifetimes; analysis authors need
not manage raw pointers to write custom Python physics.

Note: the Python API supports C++-style names (`Define`, `Filter`, `DefineVector`) and legacy aliases (`DefineJIT`, `FilterJIT`, `DefineFromVector`).

## Prerequisites

Before running these examples, ensure you have:

1. Built RDFAnalyzerCore with Python bindings:
   ```bash
   source env.sh  # On a CVMFS-backed host
   cmake -S . -B build
   cmake --build build -j$(nproc)
   ```

2. Installed Python dependencies:
   ```bash
   pip install pybind11 numpy numba
   ```

3. The matching Python module is available as `build/python/rdfanalyzer*.so`.
   Its ABI suffix depends on the selected interpreter; renaming a binary does
   not make it compatible with another Python runtime.

## Examples

### 1. String-based Expressions

**File:** `example_string_expressions.py`

Demonstrates using C++ expressions as strings (ROOT JIT compilation).

```bash
python examples/example_string_expressions.py config.txt
```

**Features:**
- Simple arithmetic operations
- Vector operations with ROOT::VecOps
- Multiple filters
- Method chaining

### 2. Numba Function Pointers

**File:** `example_numba_functions.py`

Demonstrates using numba-compiled Python functions.

```bash
python examples/example_numba_functions.py config.txt
```

**Features:**
- Compiling Python functions to native code
- Passing function pointers via ctypes
- Type-safe function signatures
- High performance with Python syntax

### 3. Numpy Array Integration

**File:** `example_numpy_arrays.py`

Demonstrates passing numpy arrays via pointer + size.

```bash
python examples/example_numpy_arrays.py config.txt
```

**Features:**
- Direct memory access (no copying)
- Support for multiple dtypes
- Integration with numpy workflows
- Event weights and corrections

### 4. Histogram Booking Structs (ND Histogram Manager)

**File:** `example_hist_booking.py`

Demonstrates constructing `HistInfo` and `SelectionInfo` in Python and booking/saving ND histograms via the histogram plugin.

```bash
python examples/example_hist_booking.py config.txt
```

**Features:**
- `HistInfo` and `SelectionInfo` bindings
- ND histogram plugin setup (`NDHistogramManager`)
- `bookNDHistograms`, `saveNDHistograms`, and `clearNDHistograms`

## Configuration File

All examples require a configuration file. Minimum example:

```
# config.txt
fileList=data.root
saveFile=output.root
threads=-1
saveConfig=output_branches.txt
```

For more details, see the [Configuration Reference](../docs/CONFIG_REFERENCE.md).

## Creating Test Data

If you don't have a ROOT file to test with, you can create one:

```bash
# In ROOT
root -l
root [0] TFile f("data.root", "RECREATE");
root [1] TTree t("Events", "Events");
root [2] Float_t pt, eta, phi;
root [3] t.Branch("pt", &pt);
root [4] t.Branch("eta", &eta);
root [5] t.Branch("phi", &phi);
root [6] for (int i = 0; i < 1000; i++) { pt = i*10; eta = i*0.01; phi = i*0.02; t.Fill(); }
root [7] t.Write();
root [8] f.Close();
root [9] .q
```

## Output Branches Configuration

Create `output_branches.txt` with branches to save:

```
pt
eta
phi
pt_gev
delta_r
```

## Documentation

For complete documentation, see:
- [Python Bindings Guide](../docs/PYTHON_BINDINGS.md)
- [Getting Started](../docs/GETTING_STARTED.md)
- [Configuration Reference](../docs/CONFIG_REFERENCE.md)

## Troubleshooting

### Module not found

Add the Python module to your path:
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path("build/python").resolve()))
```

### ROOT not setup

Ensure ROOT environment is loaded:
```bash
source env.sh  # On a CVMFS-backed host
# or
source <root-install>/bin/thisroot.sh
```

### Performance issues

For best performance:
- Use `-O2` or `-O3` optimization when building
- Enable ROOT's implicit multithreading with `threads=-1` in config
- Pre-compile numba functions before main analysis
- Keep expressions simple for better JIT optimization
