# Python Bindings for RDFAnalyzerCore

This document describes the Python bindings for the RDFAnalyzerCore framework, allowing you to use the high-performance C++ framework from Python with the convenience and prototyping speed of Python.

## Overview

The Python bindings provide three main ways to interact with the Analyzer:

1. **String-based expressions** - Write C++ code as strings, compiled by ROOT's JIT
2. **Numba function pointers** - Compile Python functions with numba and pass as function pointers
3. **Numpy array integration** - Pass numpy arrays via pointer + size interface

All three methods provide near-native performance while allowing rapid prototyping in Python.

In addition, the bindings now expose framework-level management APIs:

- C++-style naming parity: `Define`, `Filter`, `DefineVector`
- Plugin lifecycle and actions: `AddPlugin`, `SetupPlugin`, `applyAllOnnxModels`, `applyAllBDTs`, etc.
- Configuration APIs: `setConfig`, `getConfigMap`, `getConfigList`, `parse*Config`
- Systematics APIs: `registerSystematic`, `getSystematics`, `makeSystList`
- Histogram booking structs and helpers: `HistInfo`, `SelectionInfo`, `bookNDHistograms`

## API Parity Highlights

### C++-style method names

The Python `Analyzer` supports C++-style names directly:

```python
analyzer.Define("pt_gev", "pt / 1000.0", ["pt"])
analyzer.Filter("pt_cut", "pt_gev > 20.0", ["pt_gev"])
```

Legacy aliases (`DefineJIT`, `FilterJIT`, `DefineFromVector`) remain available.

### Plugin and framework control

```python
analyzer.setConfig("onnxConfig", "cfg/onnx_models.txt")
analyzer.AddPlugin("onnx", "OnnxManager")
analyzer.applyAllOnnxModels("onnx")
```

Role-based helpers are available for ONNX, BDT, Correction, Trigger, and SOFIE managers.

### Histogram booking structs from Python

```python
h = rdfanalyzer.HistInfo("h_pt", "pt", "pT", "weight", 40, 0.0, 200.0)
s = rdfanalyzer.SelectionInfo("region", 3, 0.0, 3.0, ["SR", "CR", "VR"])

analyzer.AddPlugin("hist", "NDHistogramManager")
region_names = analyzer.bookNDHistograms("hist", [h], [s], "nominal")
```

## Installation

### Prerequisites

- RDFAnalyzerCore built with Python bindings enabled
- Python 3.8 or later
- pybind11, numpy, and numba packages

### Install Python dependencies

```bash
pip install pybind11 numpy numba
```

### Build with Python bindings

```bash
# From the RDFAnalyzerCore root directory
source env.sh  # On lxplus
cmake -S . -B build
cmake --build build -j$(nproc)
```

The Python module `rdfanalyzer.so` will be built in `build/python/`.

### Add to Python path

```python
import sys
sys.path.insert(0, '/path/to/RDFAnalyzerCore/build/python')
import rdfanalyzer
```

## Usage Guide

### 1. String-based Expressions (ROOT JIT)

The most straightforward approach - write C++ expressions as strings that ROOT will compile.

**Advantages:**
- Simple and familiar to ROOT users
- Full access to ROOT functionality
- Automatic vectorization
- Systematic variations handled automatically

**Example:**

```python
import rdfanalyzer

# Create analyzer from config file
analyzer = rdfanalyzer.Analyzer("config.txt")

# Define variables using C++ expressions
analyzer.DefineJIT("pt_gev", "pt / 1000.0", ["pt"])
analyzer.DefineJIT("delta_r", 
                   "sqrt(delta_eta*delta_eta + delta_phi*delta_phi)",
                   ["delta_eta", "delta_phi"])

# Vector operations using ROOT::VecOps
analyzer.DefineJIT("high_pt_jets", "jet_pt > 25000.0", ["jet_pt"])
analyzer.DefineJIT("n_high_pt_jets", "Sum(high_pt_jets)", ["high_pt_jets"])

# Apply filters
analyzer.FilterJIT("pt_cut", "pt_gev > 20.0", ["pt_gev"])
analyzer.FilterJIT("jet_selection", "n_high_pt_jets >= 4", ["n_high_pt_jets"])

# Save results
analyzer.save()
```

**Supported C++ syntax:**
- All ROOT math functions (sqrt, abs, sin, cos, etc.)
- Vector operations (Sum, Max, Min, Any, All, etc.)
- Logical operators (&&, ||, !, ==, !=, <, >, <=, >=)
- Arithmetic operators (+, -, *, /, %)
- Conditional expressions (condition ? true_val : false_val)

### 2. Numba Function Pointers

Use numba to compile Python functions to native code, then pass them as function pointers.

**Advantages:**
- Write analysis logic in Python
- Near-native performance with numba JIT
- Type-safe with explicit signatures
- Can use numpy operations in functions

**Example:**

```python
import rdfanalyzer
import numba
import ctypes
import numpy as np

# Define numba-compiled functions
@numba.cfunc("float64(float64)")
def convert_to_gev(pt_mev):
    """Convert PT from MeV to GeV"""
    return pt_mev / 1000.0

@numba.cfunc("float64(float64, float64)")
def compute_delta_r(delta_eta, delta_phi):
    """Compute delta R"""
    return np.sqrt(delta_eta * delta_eta + delta_phi * delta_phi)

# Get function pointers
convert_ptr = ctypes.cast(convert_to_gev.address, ctypes.c_void_p).value
delta_r_ptr = ctypes.cast(compute_delta_r.address, ctypes.c_void_p).value

# Create analyzer
analyzer = rdfanalyzer.Analyzer("config.txt")

# Use function pointers
analyzer.DefineFromPointer("pt_gev", convert_ptr, 
                           "double(double)", ["pt"])
analyzer.DefineFromPointer("delta_r", delta_r_ptr,
                           "double(double, double)", 
                           ["delta_eta", "delta_phi"])

analyzer.save()
```

**Numba type mappings:**

| Numba Type | C++ Type |
|------------|----------|
| float32    | float    |
| float64    | double   |
| int32      | int      |
| int64      | long     |
| uint32     | unsigned int |
| uint64     | unsigned long |

**Important notes:**
- Use `@numba.cfunc()` decorator, not `@numba.jit()`
- Specify full C function signature
- Functions must be pure (no Python objects, only numeric types)
- Keep functions simple - complex logic may not compile

### 3. Numpy Array Integration

Pass numpy arrays to the analyzer via pointer and size.

**Advantages:**
- Easy integration with existing numpy workflows
- Useful for pre-computed weights or corrections
- Direct memory access (no copying)
- Support for any numeric dtype

**Example:**

```python
import rdfanalyzer
import numpy as np

# Create arrays
event_weights = np.array([1.0, 1.2, 0.8, 1.1, 0.9], dtype=np.float32)
scale_factors = np.array([1.05, 1.05, 1.05, 1.05, 1.05], dtype=np.float64)

# Create analyzer
analyzer = rdfanalyzer.Analyzer("config.txt")

# Define from arrays
analyzer.DefineFromVector("event_weight",
                          event_weights.ctypes.data,
                          len(event_weights),
                          "float")

analyzer.DefineFromVector("scale_factor",
                          scale_factors.ctypes.data,
                          len(scale_factors),
                          "double")

# Use in expressions
analyzer.DefineJIT("weighted_pt", "pt * event_weight", 
                   ["pt", "event_weight"])

analyzer.save()
```

**Important notes:**
- Arrays must remain in memory during analysis (don't let them go out of scope)
- The analyzer stores pointers, not copies
- Ensure array size matches the number of events

**Supported dtypes:**

| Numpy dtype | Element type string |
|-------------|---------------------|
| np.float32  | "float"             |
| np.float64  | "double"            |
| np.int32    | "int"               |
| np.int64    | "long"              |
| np.uint32   | "unsigned int"      |
| np.uint64   | "unsigned long"     |

## Plugin Management

The Python bindings expose full plugin lifecycle and action APIs, allowing you to add, configure, and use plugin managers from Python.

### Adding Plugins

```python
import rdfanalyzer

analyzer = rdfanalyzer.Analyzer("config.txt")

# Add individual plugins
analyzer.AddPlugin("onnx", "OnnxManager")
analyzer.AddPlugin("bdt", "BDTManager")
analyzer.AddPlugin("hist", "NDHistogramManager")

# Or add all default plugins at once
analyzer.AddDefaultPlugins()
```

### ONNX Manager

```python
# Apply all ONNX models from config
analyzer.setConfig("onnxConfig", "cfg/onnx_models.txt")
analyzer.AddPlugin("onnx", "OnnxManager")
analyzer.SetupPlugin("onnx")
analyzer.applyAllOnnxModels("onnx")

# Apply specific model
analyzer.applyOnnxModel("onnx", "model_name")

# Get model information
model_names = analyzer.getOnnxModelNames("onnx")
features = analyzer.getOnnxModelFeatures("onnx", "model_name")
```

### BDT Manager

```python
# Apply all BDT models from config
analyzer.setConfig("bdtConfig", "cfg/bdts.txt")
analyzer.AddPlugin("bdt", "BDTManager")
analyzer.SetupPlugin("bdt")
analyzer.applyAllBDTs("bdt")

# Apply specific BDT
analyzer.applyBDT("bdt", "bdt_name")

# Get BDT information
bdt_names = analyzer.getBDTNames("bdt")
features = analyzer.getBDTFeatures("bdt", "bdt_name")
```

### Correction Manager

```python
# Apply corrections
analyzer.setConfig("correctionConfig", "cfg/corrections.txt")
analyzer.AddPlugin("correction", "CorrectionManager")
analyzer.SetupPlugin("correction")

# Apply specific correction
analyzer.applyCorrection("correction", "muon_sf", ["muon_pt", "muon_eta"])

# Get correction features
features = analyzer.getCorrectionFeatures("correction", "muon_sf")
```

### Trigger Manager

```python
# Setup trigger management
analyzer.setConfig("triggerConfig", "cfg/triggers.txt")
analyzer.AddPlugin("trigger", "TriggerManager")
analyzer.SetupPlugin("trigger")

# Apply all triggers
analyzer.applyAllTriggers("trigger")

# Get trigger information
groups = analyzer.getTriggerGroups("trigger")
triggers = analyzer.getTriggers("trigger", "group_name")
vetoes = analyzer.getVetoes("trigger", "group_name")
```

### SOFIE Manager

```python
# Apply SOFIE models
analyzer.setConfig("sofieConfig", "cfg/sofie_models.txt")
analyzer.AddPlugin("sofie", "SofieManager")
analyzer.SetupPlugin("sofie")

# Apply all SOFIE models
analyzer.applyAllSofieModels("sofie")

# Apply specific model
analyzer.applySofieModel("sofie", "model_name")

# Get model names
model_names = analyzer.getSofieModelNames("sofie")
```

### Histogram Manager

```python
# Setup histogram manager
analyzer.AddPlugin("hist", "NDHistogramManager")

# Book histograms from Python
h = rdfanalyzer.HistInfo("h_pt", "pt", "pT [GeV]", "weight", 40, 0.0, 200.0)
s = rdfanalyzer.SelectionInfo("region", 3, 0.0, 3.0, ["SR", "CR", "VR"])

region_names = analyzer.bookNDHistograms("hist", [h], [s], "nominal")
```

## API Reference

### Analyzer Class

```python
class Analyzer:
    """Main analysis class for event processing."""
    
    def __init__(self, configFile: str):
        """
        Construct analyzer from configuration file.
        
        Parameters
        ----------
        configFile : str
            Path to configuration file
        """
    
    def DefineJIT(self, name: str, expression: str, 
                  columns: list = []) -> Analyzer:
        """
        Define variable using C++ expression string.
        
        Parameters
        ----------
        name : str
            Variable name
        expression : str
            C++ expression
        columns : list of str
            Input column names
            
        Returns
        -------
        Analyzer
            Self for method chaining
        """
    
    def FilterJIT(self, name: str, expression: str,
                  columns: list = []) -> Analyzer:
        """
        Apply filter using C++ expression string.
        
        Parameters
        ----------
        name : str
            Filter name
        expression : str
            C++ expression returning bool
        columns : list of str
            Input column names
            
        Returns
        -------
        Analyzer
            Self for method chaining
        """
    
    def DefineFromPointer(self, name: str, func_ptr: int,
                         signature: str, columns: list = []) -> Analyzer:
        """
        Define variable using function pointer (numba-compatible).
        
        Parameters
        ----------
        name : str
            Variable name
        func_ptr : int
            Function pointer address
        signature : str
            C++ function signature (e.g., "double(double, double)")
        columns : list of str
            Input column names
            
        Returns
        -------
        Analyzer
            Self for method chaining
        """
    
    def DefineFromVector(self, name: str, data_ptr: int,
                        size: int, element_type: str = "float") -> Analyzer:
        """
        Define variable from vector using pointer + size.
        
        Parameters
        ----------
        name : str
            Variable name
        data_ptr : int
            Pointer to data array
        size : int
            Number of elements
        element_type : str
            Type of elements (default: "float")
            
        Returns
        -------
        Analyzer
            Self for method chaining
        """
    
    def save(self) -> None:
        """Trigger computation and save results."""
    
    def configMap(self, key: str) -> str:
        """
        Get configuration value by key.
        
        Parameters
        ----------
        key : str
            Configuration key
            
        Returns
        -------
        str
            Configuration value
        """
```

## Configuration Files

Python bindings use the same configuration files as the C++ framework:

```
# config.txt
fileList=data1.root,data2.root
saveFile=output.root
threads=-1
saveConfig=output_branches.txt
```

See the [Configuration Reference](CONFIG_REFERENCE.md) for complete documentation.

## Performance Considerations

### String-based expressions (DefineJIT)
- **Compilation overhead**: First call may be slow (~seconds) while ROOT compiles
- **Runtime performance**: Near-native C++ speed after compilation
- **Caching**: ROOT caches compiled functions
- **Best for**: Complex expressions, vector operations, one-time definitions

### Numba function pointers (DefineFromPointer)
- **Compilation overhead**: Numba compiles on first function call (milliseconds)
- **Runtime performance**: Native speed, sometimes faster than ROOT JIT
- **Best for**: Custom algorithms, physics calculations, reusable functions

### Numpy arrays (DefineFromVector)
- **Memory overhead**: None (uses pointers, no copying)
- **Runtime performance**: Direct memory access
- **Best for**: Pre-computed weights, lookup tables, corrections

## Examples

Complete examples are available in the `examples/` directory:

- `example_string_expressions.py` - Using ROOT JIT with string expressions
- `example_numba_functions.py` - Using numba-compiled functions
- `example_numpy_arrays.py` - Integrating numpy arrays

## Limitations and Known Issues

1. **Template methods**: C++ template methods (Define<F>, Filter<F>) with lambdas are not directly accessible from Python. Use the wrapper methods instead (DefineJIT, FilterJIT, DefineFromPointer).

2. **Plugin access**: Plugin managers ARE exposed to Python with full lifecycle and action APIs. See "Plugin Management" section for details on using `AddPlugin()`, `applyAllOnnxModels()`, `applyAllBDTs()`, `applyCorrection()`, and other plugin methods.

3. **Memory management**: When using DefineFromVector, ensure arrays remain in memory during analysis. The analyzer stores pointers, not copies.

4. **ROOT environment**: Requires proper ROOT environment setup. Source `env.sh` or `thisroot.sh` before running Python scripts.

5. **Systematic variations**: Currently handled automatically for DefineJIT. Not yet implemented for DefineFromPointer or DefineFromVector.

## Troubleshooting

### Import Error: No module named 'rdfanalyzer'

Ensure the module is in your Python path:
```python
import sys
sys.path.insert(0, '/path/to/RDFAnalyzerCore/build/python')
```

### ROOT JIT compilation errors

Check that your C++ expressions are valid ROOT syntax:
```python
# Good - explicit float literal
analyzer.DefineJIT("pt_gev", "pt / 1000.0", ["pt"])

# Acceptable - ROOT will handle integer division
analyzer.DefineJIT("pt_gev", "pt / 1000", ["pt"])  # Works, but 1000.0 is clearer
```

### Numba compilation errors

Ensure functions use only supported types:
```python
# Good
@numba.cfunc("float64(float64)")
def func(x):
    return x * 2.0

# Bad - Python objects not allowed
@numba.cfunc("float64(float64)")
def func(x):
    return str(x)  # Error: can't use str() in cfunc
```

### Segmentation faults

Common causes:
- Numpy array went out of scope (use DefineFromVector)
- Invalid function pointer (check numba function signature)
- Incorrect C++ signature in DefineFromPointer

## Future Enhancements

Planned features for future releases:

- [ ] Plugin manager bindings (BDT, ONNX, Corrections, Histograms)
- [ ] Direct RDataFrame access for advanced users
- [ ] Systematic variation control from Python
- [ ] Callback functions for custom event processing
- [ ] Helper utilities for common patterns

## Contributing

Contributions are welcome! To add new Python bindings:

1. Edit `core/bindings/python_bindings.cpp`
2. Add new methods to the `AnalyzerPythonWrapper` class
3. Expose via pybind11 in the `PYBIND11_MODULE` section
4. Add documentation and examples
5. Submit a pull request

## Support

- **Documentation**: See `docs/` directory for framework documentation
- **Issues**: Open an issue on GitHub
- **Examples**: Check `examples/` directory for more examples

## License

Python bindings follow the same license as the main RDFAnalyzerCore framework.
