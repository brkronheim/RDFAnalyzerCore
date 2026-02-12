# Testing Python Bindings

This document describes how to test the Python bindings for RDFAnalyzerCore.

## Running Tests

### Quick Test

After building the project, run the quick test script:

```bash
./test_python_bindings.sh
```

This will:
1. Check that ROOT is available
2. Check that required Python packages are installed
3. Verify that the Python module was built
4. Run basic import and functionality tests

### Manual Testing

#### 1. Test Module Import

```bash
python3 -c "import sys; sys.path.insert(0, 'build/python'); import rdfanalyzer; print(rdfanalyzer.__version__)"
```

Expected output:
```
1.0.0
```

#### 2. Run Basic Tests

```bash
python3 core/bindings/test_bindings.py
```

This runs a suite of tests that verify:
- Module can be imported
- All expected methods exist on the Analyzer class (including C++-style names like `Define`/`Filter`)
- Numpy integration works
- Numba integration works
- Basic Analyzer construction

#### 3. Test with Examples

The best way to test the bindings is to run the provided examples:

```bash
# Create test data first (see examples/README.md)
python3 examples/example_string_expressions.py config.txt
python3 examples/example_numba_functions.py config.txt
python3 examples/example_numpy_arrays.py config.txt
python3 examples/example_hist_booking.py config.txt
```

## Test Coverage

### Core Functionality Tests

1. **String-based expressions (`Define` / `DefineJIT`)**
   - Simple arithmetic operations
   - Multiple input columns
   - Vector operations
   - Systematic variations

2. **Numba function pointers (DefineFromPointer)**
   - Single argument functions
   - Multiple argument functions
   - Different numeric types
   - Function pointer correctness

3. **Numpy arrays (`DefineVector` / `DefineFromVector`)**
   - Different dtypes (float32, float64, int32, etc.)
   - Memory pointer passing
   - Size handling

4. **Filter operations (`Filter` / `FilterJIT`)**
   - Boolean expressions
   - Multiple filters
   - Method chaining

### Integration Tests

The examples serve as integration tests:
- `example_string_expressions.py` - Tests ROOT JIT compilation
- `example_numba_functions.py` - Tests numba integration
- `example_numpy_arrays.py` - Tests numpy integration
- `example_hist_booking.py` - Tests histogram struct bindings and ND histogram helper methods

## Expected Test Results

### test_bindings.py Output

```
=============================================================
RDFAnalyzerCore Python Bindings - Basic Test Suite
=============================================================

Test: Module Import
-------------------------------------------------------------
✓ Successfully imported rdfanalyzer module
  Version: 1.0.0

Test: Method Existence
-------------------------------------------------------------
  ✓ Method DefineJIT exists
  ✓ Method FilterJIT exists
  ✓ Method DefineFromPointer exists
  ✓ Method DefineFromVector exists
  ✓ Method save exists
  ✓ Method configMap exists
✓ All expected methods exist

Test: Numpy Integration
-------------------------------------------------------------
✓ Numpy integration works (test array pointer: 0x...)

Test: Numba Integration
-------------------------------------------------------------
✓ Numba integration works (test function at: 0x...)

Test: Analyzer Creation
-------------------------------------------------------------
✓ Analyzer constructor correctly throws on missing file

=============================================================
Test Summary
=============================================================
✓ Module Import                       PASS
✓ Method Existence                    PASS
✓ Numpy Integration                   PASS
✓ Numba Integration                   PASS
✓ Analyzer Creation                   PASS

Total: 5/5 tests passed

🎉 All tests passed!
```

## Troubleshooting Test Failures

### Module Import Fails

**Error:** `ModuleNotFoundError: No module named 'rdfanalyzer'`

**Solutions:**
1. Check that the module was built:
   ```bash
   ls build/python/rdfanalyzer*.so
   ```
2. Ensure pybind11 was available during CMake configuration
3. Rebuild from clean:
   ```bash
   rm -rf build
   cmake -S . -B build
   cmake --build build -j$(nproc)
   ```

### Method Tests Fail

**Error:** Method missing from Analyzer class

**Solutions:**
1. Verify you're using the correct version of the module
2. Check that the bindings compiled successfully
3. Look for compilation warnings in the build log

### Numba Integration Fails

**Error:** `ModuleNotFoundError: No module named 'numba'`

**Solution:** Install numba:
```bash
pip install numba
```

### Numpy Integration Fails

**Error:** `ModuleNotFoundError: No module named 'numpy'`

**Solution:** Install numpy:
```bash
pip install numpy
```

### ROOT Not Found

**Error:** ROOT not found in PATH

**Solution:** Source ROOT environment:
```bash
source env.sh  # On lxplus
# or
source /path/to/root/bin/thisroot.sh
```

## Performance Testing

To verify that the bindings provide good performance:

### Benchmark String-based Expressions

```python
import time
import rdfanalyzer

analyzer = rdfanalyzer.Analyzer("config.txt")

start = time.time()
analyzer.DefineJIT("pt_gev", "pt / 1000.0", ["pt"])
# ... more operations
analyzer.save()
elapsed = time.time() - start

print(f"Analysis completed in {elapsed:.2f} seconds")
```

### Benchmark Numba Functions

```python
import time
import numba
import ctypes
import rdfanalyzer

@numba.cfunc("float64(float64)")
def convert(x):
    return x / 1000.0

# Warm up numba (first call compiles)
_ = convert.ctypes(1.0)

analyzer = rdfanalyzer.Analyzer("config.txt")
ptr = ctypes.cast(convert.address, ctypes.c_void_p).value

start = time.time()
analyzer.DefineFromPointer("pt_gev", ptr, "double(double)", ["pt"])
# ... more operations
analyzer.save()
elapsed = time.time() - start

print(f"Analysis completed in {elapsed:.2f} seconds")
```

**Expected results:**
- Both approaches should have similar runtime (within 10%)
- First run may be slower due to JIT compilation
- Subsequent runs should be fast (compiled code cached)

## Continuous Integration

If adding these tests to CI:

### Prerequisites
- ROOT must be available
- Python 3.7+ with pip
- Install dependencies: `pip install pybind11 numpy numba`

### Test Commands

```bash
#!/bin/bash
set -e

# Setup ROOT
source env.sh

# Install Python dependencies
pip install pybind11 numpy numba

# Build with Python bindings
cmake -S . -B build
cmake --build build -j$(nproc)

# Run tests
./test_python_bindings.sh

# Run example tests (if test data available)
# python3 examples/example_string_expressions.py test/data/config.txt
```

## Adding New Tests

When adding new functionality to the Python bindings:

1. **Add method tests** in `core/bindings/test_bindings.py`:
   ```python
   def test_new_method():
       """Test new method works"""
       import rdfanalyzer
       # Test implementation
       return True
   ```

2. **Add example** in `examples/`:
   ```python
   # examples/example_new_feature.py
   # Demonstrate new functionality
   ```

3. **Update documentation** in `docs/PYTHON_BINDINGS.md`:
   - API reference
   - Usage examples
   - Best practices

4. **Run all tests**:
   ```bash
   ./test_python_bindings.sh
   python3 examples/example_new_feature.py config.txt
   ```

## Test Data

For comprehensive testing, you may want to create test ROOT files:

### Small Test File (Quick Tests)

```python
import ROOT

# Create small test file
f = ROOT.TFile("test_small.root", "RECREATE")
t = ROOT.TTree("Events", "Events")

pt = ROOT.std.vector('float')()
eta = ROOT.std.vector('float')()

t.Branch("jet_pt", pt)
t.Branch("jet_eta", eta)

for i in range(100):
    pt.clear()
    eta.clear()
    for j in range(5):
        pt.push_back(20000.0 + i * 100)
        eta.push_back(j * 0.1)
    t.Fill()

t.Write()
f.Close()
```

### Large Test File (Performance Tests)

Similar to above but with more events (10,000+) for realistic performance testing.

## Known Issues and Limitations

See [docs/PYTHON_BINDINGS.md - Limitations and Known Issues](../docs/PYTHON_BINDINGS.md#limitations-and-known-issues) for current limitations.

## Support

If tests fail and you can't resolve the issue:
1. Check build logs for compilation errors
2. Verify all dependencies are installed
3. Try a clean rebuild
4. Open an issue on GitHub with:
   - Test output
   - Build logs
   - Environment details (ROOT version, Python version, OS)
