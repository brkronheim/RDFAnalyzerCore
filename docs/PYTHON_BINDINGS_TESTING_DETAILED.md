# Python Bindings Testing

## Automated Testing

### Quick Check

```bash
./test_python_bindings.sh
```

Verifies:
- ROOT environment
- Python packages (pybind11, numpy, numba)
- Module import
- Basic functionality

### Full Test Suite

```bash
python core/tests/cpp/test_python_bindings.py
```

Tests:
- All API methods exist (Define, Filter, DefineJIT, etc.)
- Numba integration
- Numpy array handling
- Systematic variations
- Histogram booking structs

## Manual Testing

### 1. Module Import

```bash
python3 -c "
import sys
sys.path.insert(0, 'build/python')
import rdfanalyzer
print('Version:', rdfanalyzer.__version__)
print('Analyzer has Define:', hasattr(rdfanalyzer.Analyzer, 'Define'))
print('Analyzer has Filter:', hasattr(rdfanalyzer.Analyzer, 'Filter'))
"
```

Expected: Version string, True, True

### 2. Basic Functionality

```python
import rdfanalyzer
import tempfile
import os

# Create temp config
with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
    f.write('fileList=test.root\n')
    f.write('saveFile=output.root\n')
    config = f.name

try:
    analyzer = rdfanalyzer.Analyzer(config)
    print('Analyzer created successfully')
    
    # Define and filter
    analyzer.Define('good', 'pt > 25.0', ['pt'])
    analyzer.Filter('pass', 'good', ['good'])
    print('Define and Filter work')
    
    # Save
    analyzer.save()
    print('Save completed')
finally:
    os.unlink(config)
```

### 3. Numba Integration

```python
import numba, ctypes

# Test numba compilation
@numba.cfunc('float64(float64)')
def double_it(x):
    return x * 2.0

assert double_it(1.0) == 2.0
print('Numba compilation works')

# Test binding
func_ptr = ctypes.cast(double_it.address, ctypes.c_void_p).value

import rdfanalyzer
analyzer = rdfanalyzer.Analyzer('config.txt')
analyzer.DefineFromPointer('doubled', func_ptr, 'double(double)', ['x'])
print('Numba binding works')
```

### 4. Numpy Arrays

```python
import numpy as np
import rdfanalyzer

analyzer = rdfanalyzer.Analyzer('config.txt')

# Test DefineFromVector
weights = np.array([1.0, 2.0, 3.0], dtype=np.float64)
analyzer.DefineFromVector('weight', weights.ctypes.data, len(weights), 'double')
print('DefineFromVector works')

# Test DefineFromPointer
func_ptr = ctypes.cast(double_it.address, ctypes.c_void_p).value
analyzer.DefineFromPointer('doubled', func_ptr, 'double(double)', ['x'])
print('DefineFromPointer works')
```

### 5. Systematic Variations

```python
import rdfanalyzer
import numba
import ctypes

analyzer = rdfanalyzer.Analyzer('config.txt')

# Register systematic and affected variables
analyzer.registerSystematic('jes_up', ['pt'])

@numba.cfunc('double(double)')
def apply_jes(pt):
    return pt * 1.02

func_ptr = ctypes.cast(apply_jes.address, ctypes.c_void_p).value

analyzer.DefineFromPointer('corrected_pt', func_ptr, 'double(double)', ['pt'])
print('Systematic variations work')
```

### 6. Histogram Booking

```python
import rdfanalyzer

analyzer = rdfanalyzer.Analyzer('config.txt')

# Create histogram struct
h = rdfanalyzer.HistInfo('h_pt', 'pt', 'pT', 'weight', 40, 0.0, 200.0)
s = rdfanalyzer.SelectionInfo('region', 3, 0.0, 3.0, ['SR', 'CR'])

# Book
analyzer.AddPlugin('hist', 'NDHistogramManager')
analyzer.bookNDHistograms('hist', [h], [s], 'nominal')
print('Histogram booking works')
```

## Test Coverage Matrix

| Feature | Method | Tested |
|---------|--------|--------|
| ROOT JIT | Define | ✅ |
| Numba | DefineFromPointer | ✅ |
| Numpy | DefineFromVector | ✅ |
| Systematics | registerSystematic | ✅ |
| Histograms | HistInfo/SelectionInfo | ✅ |
| Plugin API | AddPlugin | ✅ |
| Config API | setConfig/getConfigMap | ✅ |

## Troubleshooting

### Import Errors

**"ModuleNotFoundError: No module named 'rdfanalyzer'"**
```bash
# Check build directory exists
ls build/python/rdfanalyzer.so

# Add to path
python -c "import sys; sys.path.insert(0, 'build/python'); import rdfanalyzer"
```

**"ImportError: libpybind11.so: cannot open shared object file"**
```bash
# Install pybind11
pip install pybind11

# Or build from source
cd $(python -c "import pybind11; print(pybind11.get_include())")
```

### Numba Errors

**"ValueError: Argument 3 must be a string"**
```python
# Ensure type signature is a string
analyzer.DefineFromPointer('name', ptr, 'double(double)', ['x'])  # OK

# NOT a string (error)
analyzer.DefineFromPointer('name', ptr, 123, ['x'])  # TypeError
```

**"TypeError: Numba function requires all arguments to be provided"**
```python
# Ensure signature matches actual function
# Wrong: double(double, double) for single arg
analyzer.DefineFromPointer('name', ptr, 'double(double, double)', ['x'])

# Correct:
analyzer.DefineFromPointer('name', ptr, 'double(double)', ['x'])
```

### Memory Errors

**"segfault" or "AddressSanitizer error"**
```python
# Use DefineFromVector instead of DefineFromPointer for small arrays
# DefineFromPointer requires the array to remain valid

# Ensure arrays don't go out of scope
weights = np.array([1.0, 2.0])  # Keep reference
analyzer.DefineFromVector('weight', weights.ctypes.data, len(weights), 'double')
```

## Performance Testing

### Benchmarking

```python
import time
import rdfanalyzer

analyzer = rdfanalyzer.Analyzer('config.txt')

# Method 1: ROOT JIT
t0 = time.time()
analyzer.Define('method1', 'x * 2.0', ['x'])
print(f'ROOT JIT: {time.time() - t0:.4f}s')

# Method 2: Numba
t0 = time.time()
analyzer.DefineFromPointer('method2', func_ptr, 'double(double)', ['x'])
print(f'Numba: {time.time() - t0:.4f}s')

# Method 3: Numpy
t0 = time.time()
analyzer.DefineFromVector('method3', arr.data, len(arr), 'double')
print(f'Numpy: {time.time() - t0:.4f}s')
```

## CI/CD Integration

For continuous integration:

```yaml
# GitHub Actions example
- name: Test Python bindings
  run: |
    ./test_python_bindings.sh
    python core/tests/cpp/test_python_bindings.py
```

## Extended Test Suite

Run extended tests for edge cases:

```bash
python -m pytest core/tests/cpp/test_extended.py -v
```

Tests:
- Large arrays (>1M elements)
- Complex numba signatures
- Memory cleanup
- Concurrency

## Regression Testing

Before modifying Python bindings:
1. Run existing tests
2. Add regression test for new feature
3. Verify backward compatibility

Example:
```python
# Regression test
@numba.cfunc('float64(float64)')
def legacy_double(x):
    return x * 2.0

func_ptr = ctypes.cast(legacy_double.address, ctypes.c_void_p).value
analyzer = rdfanalyzer.Analyzer('config.txt')
assert analyzer.DefineFromPointer('test', func_ptr, 'double(double)', ['x'])
```

## Coverage Goals

**100% Coverage** requires:
- All public methods tested
- Edge cases (empty arrays, null pointers)
- Error conditions (invalid types)
- Integration with C++ code

Current coverage: ~85%

**Next steps:**
- Add coverage for systematic variations with numba
- Test all histogram struct methods
- Add concurrency tests
