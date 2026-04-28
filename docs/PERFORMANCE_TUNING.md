# Performance Tuning and Optimization

## Overview

This guide covers techniques for maximizing analysis speed in RDFAnalyzerCore.

## 1. Framework-Level Optimizations

### Thread Management

**ROOT ImplicitMT:**
```bash
# Config file
threads=-1  # Auto-detect all cores
# or
threads=32  # Explicit core count
```

**Best Practices:**
- Use `-1` for auto-detection
- Avoid oversubscription (threads > physical cores)
- Test with 1, 2, 4, 8 threads to find optimal

### Lazy Evaluation

**RDataFrame Optimization:**
```cpp
// Define variables before filters
analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"});
analyzer.Filter("quality", isGood, {"jet_quality"});

// Keep chains short
// Avoid: Filter(Filter(Filter(...)))
// Prefer: Filter(name, func, {"col1", "col2", "col3"})
```

### Memory Management

**Avoid Memory Spills:**
```python
# Python - use DefineFromVector for large arrays
# Use DefineFromPointer only for zero-copy needs

# C++ - ensure vectors don't reallocate
reserveVectors();
```

**Histogram Memory:**
```cpp
// Book histograms after all defines/filters
analyzer.bookConfigHistograms();

// Use NDHistogramManager for efficiency
analyzer.addPlugin("hist", histManager);
```

## 2. Python-Specific Optimizations

### Numba vs ROOT JIT

**When to use Numba:**
- Type-specific operations (bitwise, custom math)
- Performance-critical paths
- Repeated function calls

**When to use ROOT JIT:**
- Complex ROOT functions (TMath, vectors)
- Simple arithmetic
- Rapid prototyping

**Benchmark:**
```python
import time
import numba
import ctypes

@numba.cfunc("float64(float64)")
def numba_func(x):
    return x * 2.0

# ROOT JIT (Define)
t0 = time.time()
analyzer.Define('method1', 'x * 2.0', ['x'])
print(f'ROOT JIT: {time.time() - t0:.4f}s')

# Numba (DefineFromPointer)
t0 = time.time()
analyzer.DefineFromPointer('method2', ctypes.cast(numba_func.address, ctypes.c_void_p).value, 'double(double)', ['x'])
print(f'Numba: {time.time() - t0:.4f}s')
```

### Numba Optimization

**Compilation caching:**
```python
# Define numba functions at module level, not inside loops
@numba.cfunc("float64(float64)")
def to_gev(pt):
    return pt / 1000.0

# Use in multiple places
for event in events:
    analyzer.DefineFromPointer('pt_gev', func_ptr, 'double(double)', ['pt'])
```

**Type stability:**
```python
# Use explicit types
@numba.cfunc("float64(float64, float64)")
def delta_r(eta1, eta2):
    return np.sqrt((eta1 - eta2) ** 2)
```

### Memory Layout

**NumPy arrays:**
```python
# Use C-contiguous arrays
data = np.ascontiguousarray(data)

# Ensure right dtype
weights = weights.astype(np.float64)
```

**Batch processing:**
```python
# Process in chunks if memory limited
for i in range(0, len(data), chunk_size):
    chunk = data[i:i+chunk_size]
    # Process chunk
```

## 3. Algorithm Optimizations

### Systematic Variations

**Efficient Systematic Application:**
```cpp
// Register systematics once
sysMgr->registerSystematic("jes_up");

// Use vector operations
analyzer.Define("corrected_pt", [](RVec<float> pt, const std::string& sys) {
    if (sys == "jes_up") return pt * 1.02;
    return pt;
}, {"jet_pt"}, sysMgr);
```

**Avoid in loops:**
```cpp
// BAD: Register inside loop
for (auto& sys : systems) {
    sysMgr->registerSystematic(sys);
}

// GOOD: Register once
for (auto& sys : systems) {
    sysMgr->registerSystematic(sys);
}
// Then use in Define
```

### Histogram Booking

**Batch Booking:**
```cpp
// Book multiple histograms efficiently
analyzer.bookConfigHistograms();

// Or use NDHistogramManager
auto histMgr = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());
analyzer.addPlugin("histogramManager", std::move(histMgr));
```

## 4. Input/Output Optimizations

### Reading Input

**TChain Optimization:**
```cpp
// Use treeList to specify exact trees
cmake -S . -B build -DtreeList="Events;JetTree"

// Or use friend trees
// Don't read unnecessary branches
```

**Compression:**
```bash
# Use compressed ROOT files when possible
# ROOT6 supports LZ4, ZSTD compression
```

### Writing Output

**Skim Optimization:**
```cpp
// Enable skim only if needed
cfg.txt: enableSkim=true

// Use saveConfig to reduce output
cfg.txt: saveConfig=cfg/output_branches.txt
```

**Histogram Output:**
```cpp
// Book after all defines
analyzer.bookConfigHistograms();

// For region-aware booking, bind the histogram manager to a configured RegionManager
// histManager->bindToRegionManager(regionManager);
```

## 5. Profiling

### Tools

**ROOT Profiling:**
```cpp
// Enable progress bar
analyzer.SetProgress(true);

// Check event rate
std::cout << nEvents / totalTime << " events/sec";
```

**Python Profiling:**
```python
import cProfile, pstats

profile = cProfile.Profile()
profile.enable()
analyzer.save()
profile.disable()

stats = pstats.Stats(profile)
stats.sort_stats('cumulative')
stats.print_stats(20)  # Top 20 functions
```

**System Profiling:**
```bash
# Linux perf
top -d 1
htop
```

### Checkpoints

**Monitor Memory:**
```bash
watch -n 1 "ps aux | grep python"
```

**Check Disk I/O:**
```bash
iostat -x 1
```

## 6. Best Practices Checklist

**Before Running:**
- [ ] Validate config: `python core/python/validate_config.py --config config.txt`
- [ ] Set threads: `threads=-1` in config
- [ ] Check Python path: `import sys; sys.path.insert(0, 'build/python')`
- [ ] Verify ROOT: `source env.sh`

**During Execution:**
- [ ] Monitor progress: check ROOT progress bar
- [ ] Check event rate: should be consistent
- [ ] Watch memory: use top/htop
- [ ] Log errors: use analyzer.verbosityLevel > 0

**Optimization Priority:**
1. **Fastest wins**: Use ROOT JIT for simple math
2. **Memory wins**: Use DefineFromVector over DefineFromPointer
3. **Thread wins**: Enable ImplicitMT (threads=-1)
4. **Input wins**: Use compressed ROOT files
5. **Output wins**: Use saveConfig for skim

## 7. Common Bottlenecks

### I/O Bottleneck

**Symptoms:** Slow event loop, disk thrashing

**Solutions:**
- Use compressed ROOT files (LZ4, ZSTD)
- Increase disk cache: `ulimit -v` (Linux)
- Use local XRootD instead of remote
- Read multiple files: `fileList=file1.root,file2.root`

### CPU Bottleneck

**Symptoms:** Single core usage, high CPU% but low events/sec

**Solutions:**
- Increase threads: `threads=-1`
- Check for I/O bound code
- Use vector operations
- Check for cache misses (large arrays)

### Memory Bottleneck

**Symptoms:** OOM kills, slow processing

**Solutions:**
- Use DefineFromVector (copies) vs DefineFromPointer (zero-copy but requires lifetime)
- Check histogram memory
- Use region-aware histograms
- Process in chunks

## 8. Advanced Techniques

### Custom Memory Pool

For repeated allocations:
```cpp
// Use ROOT's TObject with pools
// or custom pools for large arrays
```

### Async I/O

```cpp
// Use ROOT's async I/O if available
// or custom buffering
```

### SIMD Optimization

```cpp
// Use __builtin_popcount for bit counting
// or intrinsics for vector math
```

---

**See Also:**
- [Architecture](./ARCHITECTURE.md)
- [Python Bindings Deep Dive](./PYTHON_BINDINGS_DEEP.md)
