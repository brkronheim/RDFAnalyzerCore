# Python Bindings Quick Start

## Installation

Use the Python interpreter and compiler that match your ROOT installation. Follow
the [installation guide](PYTHON_INSTALLATION.md) to build the native binding and
run the Numba doctor check. The examples below assume a repository build in
`build/python` and a config that supplies a scalar `pt` branch in MeV.

## Minimal Example

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path("build/python").resolve()))
import rdfanalyzer

# Create analyzer
analyzer = rdfanalyzer.Analyzer("config.yaml")

# Define variable (ROOT JIT)
analyzer.Define("pt_gev", "pt / 1000.0", ["pt"])

# Apply selection
analyzer.Filter("high_pt", "pt_gev > 25.0", ["pt_gev"])

# Save through the Python wrapper (calls C++ Analyzer::run())
analyzer.save()
```

## Numba Integration

For a scalar double input, use this definition in place of the JIT definition, before filtering and saving. Keep the compiled function alive through `save()`.

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

Before saving, attach a contiguous array as a vector column shared by all events. Keep the array alive and unchanged until `save()` returns.

```python
import numpy as np

# Create array
weights = np.array([1.0, 2.0, 3.0], dtype=np.float64)

# Define
analyzer.DefineVector(
    "weight_table",
    weights.ctypes.data,
    len(weights),
    "double"
)
```

## Next Steps

- See [Python Bindings Deep Dive](./PYTHON_BINDINGS_DEEP.md) for advanced patterns
- See [Python Bindings Testing](./PYTHON_BINDINGS_TESTING.md) for verification
- See the [Python authoring tutorial](PYTHON_TYPING_AND_TUTORIAL.md) for a complete example

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
# Compare the active interpreter and extension suffix with the build.
python3 -c 'import sys, sysconfig; print(sys.executable, sysconfig.get_config_var("EXT_SUFFIX"))'
```

## Testing

```bash
# Run the binding integration test
./test_python_bindings.sh

# Or manually
python3 core/tests/cpp/test_python_bindings.py
```

## Performance Tips

1. **Compile numba once**: Move numba function definition outside loops
2. **Profile**: Use `timeit` to compare approaches
