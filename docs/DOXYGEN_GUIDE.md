# Documentation Standards for Agents

## Header Comments (Doxygen)

All `.h` files must have Doxygen headers:

```cpp
/**
 * @brief Manager for [specific functionality].
 *
 * @details This manager implements the IPluggableManager interface.
 * It processes [data type] using [algorithm].
 *
 * @author Agent Name
 * @date 2026-04-25
 */

#ifndef RDF_ANALYZER_CORE_PLUGIN_NEWMANAGER_H_
#define RDF_ANALYZER_CORE_PLUGIN_NEWMANAGER_H_

#include "IPluggableManager.h"
#include <memory>

class NewManager : public IPluggableManager {
public:
    using IPluggableManager::IPluggableManager;

protected:
    // Private implementation
};

#endif
```

## Naming Conventions

| Convention | Example |
|-----------|---------|
| Manager classes | `XManager` (e.g., `BDTManager`) |
| Interfaces | `IPluggableManager` (prefix `I`) |
| Private members | `m_varName` (lowercase with m_) |
| Public methods | `ProcessHistogram`, `AddSystematic` |
| Constants | `kConstName` (uppercase with k_) |

## Exception Documentation

Every function that throws must document it:

```cpp
/**
 * @brief Load histogram from ROOT file.
 *
 * @param filename Path to histogram file
 * @param name Name of histogram in file
 *
 * @throws DataError if file cannot be opened
 * @throws ConfigError if histogram not found
 */
void LoadHistogram(const std::string& filename) {
  // ...
}
```

## Memory Ownership Documentation

Critical for ROOT objects. Use `@ownership` tag:

```cpp
/**
 * @brief Add histogram to processing.
 *
 * @param h1 Histogram to process. Ownership is transferred to the manager.
 * @note The histogram must remain valid until Process() is called.
 */
void AddHistogram(std::unique_ptr<TH1> h1) override {
  // ...
}
```

## Plugin Documentation Template

For new plugins, create `docs/PLUGIN_TEMPLATE.md`:

```markdown
# [Plugin Name]

## Purpose
[What this plugin does]

## Interface
- Inherits from: `IPluggableManager`
- Required methods: `Process(unique_ptr<TDirectory> dir)`

## Dependencies

- ROOT version: >= 6.30
- External libraries: [list]

## Configuration
YAML keys:
- `plugins.{name}`: {...}

## Testing
- Unit test: `core/tests/cpp/test_{name}.cc`
- Integration test: `core/tests/law/test_{name}.py`

## Examples
```cpp
// Usage example
auto manager = std::make_unique<NewManager>(config);
manager->AddHistogram(std::move(h1));
```
```

## Verification

```bash
# Check all headers have Doxygen
grep -L "@brief" core/src/*.h | xargs echo "Missing:"

# Check for unimplemented pure virtual functions
grep -r "override" core/plugins/ | grep -v "@"

# Validate plugin names
grep -o "std::make_unique<[A-Z].*Manager>" core/src/analyzer.cc | sort -u
```

---
**Enforcement**: All new code must follow these standards
