# Memory Model & Ownership Contracts

## Overview

RDFAnalyzerCore follows a **RAII-based ownership model** where C++ smart pointers define transfer semantics, and ROOT objects have special ownership rules.

## Root Objects Ownership

### General Rule
ROOT objects (`TH1`, `TChain`, `TDirectory`, etc.) are managed by the ROOT library. When passed to analyzer managers, they typically enter with `std::unique_ptr` ownership.

### Ownership Transfers

| Operation | Ownership | Example |
|-----------|-----------|---------|
| Constructor | Manager owns | `unique_ptr<TH1> h1 = std::make_unique<TH1>();` |
| `TChain::AddFile()` | Chain owns | `chain->AddFile("file.root");` |
| `TFile::Write()` | File owns histograms | `file->Write();` |
| `TTree::Draw()` | Tree owns histograms | `tree->Draw("h1->h2");` |

### Critical Pattern
When a manager receives a ROOT object via `unique_ptr`, it **must not** call `reset()` or `release()` without transferring ownership to another manager or ROOT itself.

## Smart Pointer Usage

### Analyzer Facade
The `Analyzer` class uses `std::shared_ptr` internally for internal bookkeeping but exposes `unique_ptr` to plugin managers.

```cpp
// In Analyzer
shared_ptr<IManager> plugin_ = std::make_shared<BDTManager>();

// Plugin receives unique_ptr for data
unique_ptr<TH1> histogram = std::make_unique<TH1>();
plugin->AddHistogram(std::move(histogram)); // Ownership transferred
```

### DataManager
The DataManager wraps ROOT objects with `unique_ptr` and transfers them to manager constructors.

## Manual Memory (Known Issue)

**Location**: `core/src/util.cc:34`

```cpp
// PROBLEM
tchainVector.emplace_back(new TChain(tree.c_str()));

// FIX
tchainVector.emplace_back(std::make_unique<TChain>(tree.c_str()));
```

This violates the RAII principle and can leak if `tchainVector` is not properly cleared.

## Thread Safety

### Current State
Most managers are **not** thread-safe. The `Analyzer` provides thread-local state via:

- `thread_local` variables for cache
- `RDataFrame` handles multi-threading internally

### Safe Usage
```cpp
// Safe
unique_ptr<TH1> h = manager->GetHistogram(); // Copy constructor creates new object

// Unsafe
unique_ptr<TH1> h = manager->GetHistogram();
h->Fill(x); // Modifying shared histogram from multiple threads
```

## Verification Commands

```bash
# Run tests
cd build
ctest --output-on-failure

# Check for memory leaks in a concrete test binary
valgrind --leak-check=full ./core/tests/cpp/testDataManager

# Profile memory usage
echo 'profile memory' > /tmp/config.yaml
ROOT -l -b -q 'TProfile t("profile"); t.SetProfile("/tmp/profile.root");'
```

## Plugin Development Guidelines

When writing new managers:

1. **Constructor**: Accept `unique_ptr` parameters, never `new`
2. **AddHistogram**: Use `std::move` to transfer ownership
3. **Return values**: Return `unique_ptr` unless the caller explicitly needs shared ownership
4. **Temporary objects**: Use `std::make_unique` inside the function scope

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| `new TChain` | Use `std::make_unique<TChain>` |
| `h->Reset();` on unique_ptr | `h.reset();` instead |
| Keeping ROOT objects after Write() | Transfer ownership to TFile |
| Mixing shared_ptr/unique_ptr | Document which smart pointer is used |

---
**Severity**: HIGH - Memory leaks in ROOT can cause disk exhaustion in batch jobs
**Fix Priority**: 1 (Immediate)
