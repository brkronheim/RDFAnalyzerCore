# Python Bindings Deep Dive

## Advanced Python Integration

This guide covers advanced patterns for using RDFAnalyzerCore from Python, including numba integration, memory management, and systematic variations.

## 1. Numba Integration

### Type Hinting for Numba

Numba requires explicit type hints for optimal performance:

```python
import numba
import ctypes
import numpy as np

# Define a numba-compiled function with explicit types
@numba.cfunc("float64(float64, float64)")
def compute_delta_r(pt, eta):
    return np.sqrt((pt - eta) ** 2)

# Get the function pointer
func_ptr = ctypes.cast(
    compute_delta_r.address,
    ctypes.c_void_p
).value

# Define in analyzer
analyzer.DefineFromPointer(
    "delta_r",
    func_ptr,
    "double(double, double)",  # Return type + signature
    ["jet_pt", "jet_eta"]  # Input columns
)
```

### Type Safety

Numba restricts supported types. Use whitelisted types:

```python
# Supported types
supported_types = [
    "float", "double", "int", "long",
    "int32_t", "int64_t", "uint32_t", "uint64_t"
]

# Define with type string
analyzer.DefineFromVector(
    "weights",
    weights_array.ctypes.data,
    len(weights_array),
    "double"  # Must match supported types
)
```

### Performance Tips

1. **Compile outside the event loop**: Numba compiles on first call
2. **Keep functions pure**: No side effects for better optimization
3. **Use appropriate precision**: `float64` for physics calculations

## 2. Memory Management

### DefineFromVector vs DefineFromPointer

**DefineFromVector** (preferred):
- Copies data if needed
- Handles memory management
- Simpler API

```python
import numpy as np

# Create numpy array
weights = np.array([1.0, 2.0, 3.0], dtype=np.float64)

# Pass to analyzer
analyzer.DefineFromVector(
    "weight",
    weights.ctypes.data,
    len(weights),
    "double"
)
```

**DefineFromPointer** (advanced):
- Zero-copy (faster for large arrays)
- You manage memory
- Requires type signature

```python
# For large arrays or shared memory
analyzer.DefineFromPointer(
    "large_array",
    large_array.ctypes.data,
    len(large_array),
    "double(double)",  # double(double) - return type (double) and signature
    ["input_col"]
)
```

### When to Use Which

| Use Case | DefineFromVector | DefineFromPointer |
|----------|-----------------|------------------|
| Small arrays (<10k elements) | ✅ Preferred | ⚠️ Possible |
| Large arrays (>1M elements) | ⚠️ Memory copy | ✅ Preferred |
| Shared memory | ❌ No | ✅ Yes |
| Complex types | ✅ Handles conversion | ⚠️ Manual casting |

## 3. Systematic Variations

### Handling Systematics in Python

Systematic variations are registered through the Analyzer Python wrapper using the exposed systematic helper methods, not via a separate `SystematicManager` object.

```python
import numba
import ctypes

analyzer.registerSystematic("jes_up", ["jet_pt"])

@numba.cfunc("double(double)")
def apply_jes(pt):
    return pt * 1.02

func_ptr = ctypes.cast(apply_jes.address, ctypes.c_void_p).value

analyzer.DefineFromPointer(
    "jes_corrected_pt",
    func_ptr,
    "double(double)",
    ["jet_pt"],
)
```

The Python binding exposes helper methods such as:

- `analyzer.registerSystematic(...)`
- `analyzer.getSystematics()`
- `analyzer.getVariablesForSystematic(...)`
- `analyzer.getSystematicsForVariable(...)`
- `analyzer.autoRegisterSystematics(...)`
- `analyzer.makeSystList(...)`

### Systematic Variations with Numpy

```python
import numpy as np
import ctypes

weights = np.array([1.0, 2.0, 3.0], dtype=np.float64)
func_ptr = weights.ctypes.data

analyzer.DefineFromVector(
    "weight", 
    func_ptr, 
    len(weights), 
    "double"
)
```

## 4. Advanced Patterns

### Custom Python Classes

You can pass custom Python objects if they support `__iter__` and `__len__`:

```python
class JetCollection:
    def __init__(self, jets):
        self.jets = jets
    
    def __iter__(self):
        return iter(self.jets)
    
    def __len__(self):
        return len(self.jets)

# Define from custom object
analyzer.Define(
    "n_jets",
    len,  # Built-in function
    [JetCollection(jets)]  # Custom object
)
```

### Lambda Functions

Numba supports lambdas:

```python
@numba.cfunc("float64(float64)")
def double_it(x):
    return x * 2.0

# Use directly
analyzer.DefineFromPointer(
    "doubled",
    ctypes.cast(double_it.address, ctypes.c_void_p).value,
    "double(double)",
    ["pt"]
)
```

### Error Handling

```python
try:
    analyzer.DefineFromPointer(
        "bad_func",
        invalid_ptr,
        "double(double)"
    )
except RuntimeError as e:
    print(f"Binding error: {e}")
```

## 5. Performance Benchmarks

### Numba vs ROOT JIT

| Method | Speed | Complexity |
|--------|-------|------------|
| ROOT JIT (Define) | Fast | Low |
| Numba (DefineFromPointer) | Very Fast | Medium |
| Numpy (DefineFromVector) | Fast | Low |

**When to use Numba:**
- Custom logic not expressible in ROOT expressions
- Type-specific operations (e.g., bitwise ops)
- Performance-critical paths

**When to use ROOT JIT:**
- Simple arithmetic and vector ops
- Complex ROOT functionality (TMath, etc.)
- Rapid prototyping

## 6. Debugging

### Common Errors

**"Invalid signature format"**
- Ensure type signature matches: `double(double)` not `double(double, double)`

**"Function pointer is NULL"**
- Verify numba function was compiled
- Check ctypes import is available

**"IndexError: tuple index out of range"**
- Numba function requires all arguments to be provided

### Debug Output

```python
import numba
numba.config.DUMP_CONTEXT = True
numba.config.FORCEobjmode = True
```

## 7. Best Practices

1. **Import at module level**: Compile numba functions once
2. **Use type hints**: Document expected types
3. **Handle edge cases**: Check for empty arrays
4. **Memory management**: Use DefineFromVector for temporary data
5. **Validation**: Test numba functions before passing to analyzer

```python
def test_numba_func():
    # Test numba function before use
    result = compute_delta_r(1.0, 2.0)
    assert result > 0
```

## 8. Example: Full Analysis

```python
import rdfanalyzer
import numba
import ctypes
import numpy as np

# Setup
analyzer = rdfanalyzer.Analyzer("config.txt")

# Numba function for custom delta_R calculation
@numba.cfunc("float64(float64, float64)")
def delta_r_np(eta1, eta2):
    return np.sqrt((eta1 - eta2) ** 2)

func_ptr = ctypes.cast(
    delta_r_np.address,
    ctypes.c_void_p
).value

# Define with numba
analyzer.DefineFromPointer(
    "delta_r",
    func_ptr,
    "double(double, double)",
    ["jet_eta_1", "jet_eta_2"]
)

# Apply systematic variation
analyzer.registerSystematic("eta_syst", ["jet_eta"])

@numba.cfunc("double(double)")
def apply_eta(eta):
    return eta + 0.01

func_ptr = ctypes.cast(apply_eta.address, ctypes.c_void_p).value

analyzer.DefineFromPointer(
    "corrected_eta",
    func_ptr,
    "double(double)",
    ["jet_eta"],
)

# Save
analyzer.save()
```

This demonstrates: numba integration and systematic setup with the Python binding.
