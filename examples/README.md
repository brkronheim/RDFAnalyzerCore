# Python Bindings Examples

This directory contains example scripts demonstrating the use of RDFAnalyzerCore Python bindings.

Note: the Python API supports C++-style names (`Define`, `Filter`, `DefineVector`) and legacy aliases (`DefineJIT`, `FilterJIT`, `DefineFromVector`).

## Prerequisites

Before running these examples, ensure you have:

1. Built RDFAnalyzerCore with Python bindings:
   ```bash
   source env.sh  # On lxplus
   cmake -S . -B build
   cmake --build build -j$(nproc)
   ```

2. Installed Python dependencies:
   ```bash
   pip install pybind11 numpy numba
   ```

3. The Python module is available at `build/python/rdfanalyzer.so`

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
sys.path.insert(0, '/path/to/RDFAnalyzerCore/build/python')
```

### ROOT not setup

Ensure ROOT environment is loaded:
```bash
source env.sh  # On lxplus
# or
source /path/to/root/bin/thisroot.sh
```

### Performance issues

For best performance:
- Use `-O2` or `-O3` optimization when building
- Enable ROOT's implicit multithreading with `threads=-1` in config
- Pre-compile numba functions before main analysis
- Keep expressions simple for better JIT optimization
