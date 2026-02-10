# Python Bindings for RDFAnalyzerCore

This document describes the Python bindings for the RDFAnalyzerCore framework, allowing you to use the high-performance C++ framework from Python with the convenience and prototyping speed of Python.

## Overview

The Python bindings provide three main ways to interact with the Analyzer:

1. **String-based expressions** - Write C++ code as strings, compiled by ROOT's JIT
2. **Numba function pointers** - Compile Python functions with numba and pass as function pointers
3. **Numpy array integration** - Pass numpy arrays via pointer + size interface

All three methods provide near-native performance while allowing rapid prototyping in Python.

## Installation

### Prerequisites

- RDFAnalyzerCore built with Python bindings enabled
- Python 3.7 or later
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

2. **Plugin access**: Plugin managers are not yet exposed to Python. Future releases will add bindings for BDT, ONNX, Correction, and Histogram managers.

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
