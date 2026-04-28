# Error Handling Guide

## Overview

This guide documents common errors in RDFAnalyzerCore and how to resolve them.

## Common Errors

### 1. ROOT Not Found

**Error:**
```
Could not find the specified library
libRDF.so: file not found
```

**Solution:**
```bash
# Source ROOT environment
source env.sh  # lxplus
# or
source /path/to/root/bin/thisroot.sh
```

**Python Specific:**
```
ImportError: No module named 'ROOT'
```

```bash
# Source the ROOT environment that provides Python bindings
source /path/to/root/bin/thisroot.sh
```

### 2. Python Module Not Found

**Error:**
```
ModuleNotFoundError: No module named 'rdfanalyzer'
```

**Solution:**
```python
import sys
sys.path.insert(0, '/path/to/RDFAnalyzerCore/build/python')
```

**Build Issue:**
```bash
# Ensure built with Python bindings
cmake -S . -B build
# Check if module exists
ls build/python/rdfanalyzer.so
```

### 3. Numba Compilation Error

**Error:**
```
ValueError: Argument 3 must be a string
```

**Cause:** Type signature must be a string

**Solution:**
```python
# WRONG
analyzer.DefineFromPointer('name', ptr, 123, ['x'])

# CORRECT
analyzer.DefineFromPointer('name', ptr, 'double(double)', ['x'])
```

**Error:**
```
TypeError: Numba function requires all arguments to be provided
```

**Cause:** Function signature mismatch

**Solution:**
```python
# Define single-argument function as double(double), not double(double, double)
```

### 4. Memory Management Errors

**Error:**
```
segfault
```

**Cause:** Using DefineFromPointer with arrays that went out of scope

**Solution:**
```python
# Use DefineFromVector for temporary arrays
import numpy as np
weights = np.array([1.0, 2.0])  # Keep reference
analyzer.DefineFromVector('weight', weights.ctypes.data, len(weights), 'double')

# Or ensure array lives long enough
weights = np.array([1.0, 2.0])
analyzer.DefineFromPointer('weight', weights.ctypes.data, len(weights), 'double')
# Ensure 'weights' stays in scope until analyzer.save() or analyzer.run()
```

### 5. Systematic Variations Errors

**Error:**
```
AttributeError: 'NoneType' object has no attribute 'registerSystematic'
```

**Cause:** Not initializing SystematicManager

**Solution:**
```cpp
// C++
analyzer.getSystematicManager()->registerSystematic("jes_up");
```

```python
# Python
analyzer = rdfanalyzer.Analyzer('config.txt')
analyzer.registerSystematic('jes_up', ['jet_pt'])
```
### 6. Configuration Errors

**Error:**
```
YAML: bad indentation
```

**Cause:** Indentation in YAML config

**Solution:**
- Use 2 spaces for indentation
- No tabs
- Check for trailing whitespace

**Error:**
```
Missing required key 'sampleConfig'
```

**Solution:**
```bash
python core/python/validate_config.py --config config.txt
```

Run validation before running analysis.

### 7. CMake Build Errors

**Error:**
```
cmake: error: CMake 3.19.0 or later is required
```

**Solution:**
```bash
# Check version
cmake --version

# Update CMake
sudo apt-get install cmake  # Ubuntu
# or
brew install cmake  # macOS
```

**Error:**
```
Could not find a package configuration file provided by "ROOT"
```

**Solution:**
```bash
# Ensure ROOT is in PATH
export ROOTSYS=/path/to/root
export PATH=$ROOTSYS/bin:$PATH
```

### 8. Numpy Array Errors

**Error:**
```
TypeError: expected string or bytes-like object
```

**Cause:** Wrong element type string

**Solution:**
```python
# Valid types only
analyzer.DefineFromVector('data', ptr, size, 'double')
analyzer.DefineFromVector('data', ptr, size, 'float')
analyzer.DefineFromVector('data', ptr, size, 'int')

# NOT valid
analyzer.DefineFromVector('data', ptr, size, 'float64')  # Use 'float' or 'double' instead
```

### 9. Histogram Booking Errors

**Error:**
```
AttributeError: 'NDHistogramManager' object has no attribute 'bookNDHistograms'
```

**Cause:** Method name incorrect

**Solution:**
```python
# Use the struct-based API
h = rdfanalyzer.HistInfo('h_pt', 'pt', 'pT', 'weight', 40, 0.0, 200.0)
s = rdfanalyzer.SelectionInfo('region', 3, 0.0, 3.0, ['SR'])
analyzer.bookNDHistograms('hist', [h], [s], 'nominal')
```

### 10. Validation Errors

**Error:**
```
Config validation failed: File not found for 'floatConfig'
```

**Solution:**
```bash
# Ensure file exists
ls cfg/float_config.txt

# Check path is relative to config file
cd /path/to/analysis
python core/python/validate_config.py --config cfg/config.txt
```

## Debugging Tips

### Enable Debug Output

```python
import rdfanalyzer
import sys

# Set verbosity
analyzer = rdfanalyzer.Analyzer('config.txt')
analyzer.verbosityLevel = 2  # Higher = more verbose
```

### Check Python Path

```python
import sys
print(sys.path)
```

### Check ROOT Interpreter

```python
import ROOT
print(ROOT.gInterpreter().getNumCommands())
```

### Validate Config Before Run

```bash
python core/python/validate_config.py --config config.txt
```

### Check Build

```bash
# List libraries
cmake --build build --target RDFAnalyzer

# Check for Python module
ls build/python/
```

## Prevention

1. **Always validate configs** before running
2. **Use DefineFromVector** for numpy arrays (safer than DefineFromPointer)
3. **Keep arrays in scope** when using DefineFromPointer
4. **Test numba functions** before passing to analyzer
5. **Run validation** in CI/CD pipelines
6. **Check Python path** in scripts

## Reporting Bugs

If you encounter an error not listed here:
1. Search existing issues on GitHub
2. Check if ROOT version is compatible (6.30+)
3. Provide:
   - Exact error message
   - Full stack trace
   - ROOT version
   - Python version
   - Relevant code snippet

## Common Patterns

### Safe Error Handling

```python
try:
    analyzer = rdfanalyzer.Analyzer('config.txt')
    analyzer.Define('x', 'pt > 25.0', ['pt'])
    analyzer.save()
except RuntimeError as e:
    print(f"Analysis error: {e}")
    sys.exit(1)
```

### Pre-flight Checks

```python
import sys

# Check dependencies
required = ['rdfanalyzer', 'ROOT', 'numba', 'numpy']
missing = [m for m in required if not __import__('importlib').import_module(m)]
if missing:
    print(f"Missing modules: {missing}")
    sys.exit(1)
```

---

**See Also:**
- [Python Bindings Quick Start](./PYTHON_BINDINGS_QUICKSTART.md)
- [Python Bindings Deep Dive](./PYTHON_BINDINGS_DEEP.md)
- [Python Bindings Testing](./PYTHON_BINDINGS_TESTING_DETAILED.md)
