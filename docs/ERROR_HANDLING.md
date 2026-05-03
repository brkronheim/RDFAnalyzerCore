# Error Handling Patterns

## Exception Hierarchy

```cpp
// core/src/Error.h (to be created)

// Base exception
struct AnalyzerException : public std::runtime_error {
  virtual ~AnalyzerException() = default;
};

// Configuration errors
struct ConfigError : public AnalyzerException {
  std::string filename;  // Which file caused the error
};

// Data errors
struct DataError : public AnalyzerException {
  std::string chain_name;  // Which ROOT file
};

// Plugin errors
struct PluginError : public AnalyzerException {
  std::string plugin_name;  // Which plugin failed
};

// Systematic errors
struct SystematicError : public AnalyzerException {
  std::string var_name;     // Which systematic
};
```

## When to Throw vs Error Codes

| Context | Throw Exception | Return Error Code |
|---------|----------------|------------------|
| **Configuration** | Yes | No |
| **File I/O** | Yes | No |
| **Root Objects** | Yes | No |
| **Plugin Validation** | Yes | No |
| **Runtime Calculation** | No | Yes |
| **Batch Processing** | No | Yes |

## Error Code Convention

```cpp
// For runtime errors (non-fatal)
enum class ErrorCode {
  SUCCESS = 0,
  INSUFFICIENT_DATA = 1,
  STATISTICALLY_SIGNIFICANT_FLUCTUATION = 2,
  PLUGIN_NOT_AVAILABLE = 3,
  MEMORY_ALLOCATION_FAILED = 4
};

// Return pattern
ErrorCode ProcessHistogram(unique_ptr<TH1> h) {
  if (!h) return ErrorCode::INSUFFICIENT_DATA;
  return ErrorCode::SUCCESS;
}
```

## Critical Errors (Aborts)

These errors indicate unrecoverable state and should abort the analysis:

1. **Invalid ROOT file** (corrupted, missing histograms)
2. **Memory exhaustion** (OOM in batch jobs)
3. **File lock** (Rucio/Condor interference)
4. **Plugin crash** (segfault in ONNX/BDT)

## Current Issues

### Unclear Error Propagation
`core/src/util.cc:167` contains:
```cpp
// Do not throw here so that higher-level logic can decide
throw(std::runtime_error("..."));
```

**Fix**: Document the error flow in `ErrorHandling.md` and use consistent exception types.

### Silent Failures
Some plugin methods return `bool` but don't log errors:
```cpp
bool LoadModel(const string& path) { ... }
```
**Fix**: Change to `ErrorCode LoadModel()` and log specific errors.

## Logging Strategy

```cpp
// Use ROOT's built-in logging
ROOT::Log::Info("Manager loaded histogram");
ROOT::Log::Error("Failed to load model: path not found");

// Or custom logger
class Logger {
  void Error(const string& msg, ErrorCode code);
};
```

## Verification

```bash
# Test error handling
echo 'invalid_config.yaml' > /tmp/test.yaml
cmake -E cmake_run_bin --config /tmp/test.yaml 2>&1 | grep "ConfigError"
```

---
**Priority**: MEDIUM - Improve error messages for debugging
