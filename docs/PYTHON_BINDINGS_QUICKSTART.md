# Python Bindings Quick Start

## Installation

> **Prerequisite:** Do not attempt `pip install ROOT`. Use `source env.sh` on supported hosts or `source <root-install>/bin/thisroot.sh` with a standalone ROOT installation.

```bash
# Build RDFAnalyzerCore with Python bindings
source env.sh  # On a CVMFS-backed host, or source your ROOT installation
cmake -S . -B build
cmake --build build -j$(nproc)

# Install dependencies
pip install pybind11 numpy numba
```

## Minimal Example

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path("build/python").resolve()))
import rdfanalyzer

# Create analyzer
analyzer = rdfanalyzer.Analyzer("config.txt")

# Define variable (ROOT JIT)
analyzer.Define("pt_gev", "pt / 1000.0", ["pt"])

# Apply selection
analyzer.Filter("high_pt", "pt_gev > 25.0", ["pt_gev"])

# Save
analyzer.run()
```

## Numba Integration

```python
import numba, ctypes

# Compile function
@numba.cfunc("float64(float64)")
def to_gev(pt):
    return pt / 1000.0

# Get pointer
func_ptr = ctypes.cast(
    to_gev.address,
    ctypes.c_void_p
).value

# Define
analyzer.DefineFromPointer(
    "pt_gev",
    func_ptr,
    "double(double)",
    ["pt"]
)
```

## Numpy Arrays

```python
import numpy as np

# Create array
weights = np.array([1.0, 2.0, 3.0], dtype=np.float64)

# Define
analyzer.DefineFromVector(
    "weight",
    weights.ctypes.data,
    len(weights),
    "double"
)
```

## Next Steps

- See [Python Bindings Deep Dive](./PYTHON_BINDINGS_DEEP.md) for advanced patterns
- See [Python Bindings Testing](./PYTHON_BINDINGS_TESTING.md) for verification
- See [Examples](../examples/) for complete workflows

## Common Issues

**Module not found:**
```python
import sys
sys.path.insert(0, 'build/python')
```

**Numba not installed:**
```bash
pip install numba
```

**Wrong Python:**
```bash
# Ensure using Python 3.8+
python --version
```

## Testing

```bash
# Run test suite
./test_python_bindings.sh

# Or manually
python core/tests/cpp/test_python_bindings.py
```

## Performance Tips

1. **Compile numba once**: Move numba function definition outside loops
2. **Use DefineFromVector**: Simpler and faster for small arrays
3. **Profile**: Use `timeit` to compare approaches
