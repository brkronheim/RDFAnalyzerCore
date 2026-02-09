# YAML Configuration Support

This document describes the YAML configuration support added to RDFAnalyzerCore.

## Overview

RDFAnalyzerCore now supports configuration files in both text (`.txt`) and YAML (`.yaml` or `.yml`) formats. The framework automatically detects which format to use based on the file extension.

## Features

- **Auto-detection**: The format is automatically detected based on file extension
  - `.txt` → Text format (key=value)
  - `.yaml` or `.yml` → YAML format
- **Consistent functionality**: Both formats support the same features:
  - Pair-based configs (key-value pairs)
  - Multi-key configs (lists of entries with multiple key-value pairs)
  - Vector configs (simple lists of values)
- **Format preservation**: Submission file generation scripts preserve the format of the input config

## Format Comparison

### Text Format (`.txt`)
```
directory=/home/user/testDir
saveFile=/home/user/outDir/output.root
threads=-1
globs=root,test
```

### YAML Format (`.yaml` or `.yml`)
```yaml
directory: /home/user/testDir
saveFile: /home/user/outDir/output.root
threads: "-1"
globs: root,test
```

### Multi-Key Config (Text)
```
file=file1.json name=name1 type=type1
file=file2.json name=name2 type=type2
```

### Multi-Key Config (YAML)
```yaml
- file: file1.json
  name: name1
  type: type1
- file: file2.json
  name: name2
  type: type2
```

### Vector Config (Text)
```
var1
var2
var3
```

### Vector Config (YAML)
```yaml
- var1
- var2
- var3
```

## Usage

### C++ Side

The `ConfigurationManager` automatically detects the format:

```cpp
// Automatically uses YamlConfigAdapter for .yaml files
ConfigurationManager config("config.yaml");

// Automatically uses TextConfigAdapter for .txt files
ConfigurationManager config("config.txt");

// Access values the same way regardless of format
std::string dir = config.get("directory");
```

### Python Side

The `submission_backend` module handles both formats:

```python
from submission_backend import read_config, write_config

# Auto-detects format based on extension
config = read_config("config.yaml")  # YAML format
config = read_config("config.txt")   # Text format

# Write in either format
write_config(config, "output.yaml")  # Writes as YAML
write_config(config, "output.txt")   # Writes as text
```

### Submission File Generation

The submission file generation scripts preserve the config format:

```bash
# If you provide a YAML config, submission configs will be YAML
python core/python/generateSubmissionFilesNANO.py -c config.yaml ...

# If you provide a text config, submission configs will be text
python core/python/generateSubmissionFilesNANO.py -c config.txt ...
```

## Migration Guide

To migrate existing text configs to YAML:

1. Change file extension from `.txt` to `.yaml`
2. Convert `key=value` syntax to `key: value` syntax
3. For multi-key configs, convert to YAML list format with `-` prefix
4. For vector configs, add `-` prefix to each item

You can also use both formats side-by-side in the same analysis. The framework will handle them transparently.

## Testing

### Python Tests
Run the Python test suite:
```bash
python3 core/test/test_yaml_config.py
```

### C++ Tests
The C++ test suite includes tests for YAML configs:
```bash
./test.sh
```

Specific YAML tests are in `core/test/testConfigurationManager_YamlParsing.cc`.

## Implementation Details

### C++ Implementation
- `YamlConfigAdapter` implements the `IConfigAdapter` interface
- Uses the `yaml-cpp` library for parsing
- Auto-detection in `ConfigurationManager` constructor based on file extension

### Python Implementation
- `read_config_yaml()` uses PyYAML's `safe_load()`
- `write_config_yaml()` uses PyYAML's `dump()` with readable formatting
- Auto-detection in `read_config()` and `write_config()` based on file extension
- Inline Python scripts in condor submission files support both formats

## Dependencies

### C++
- `yaml-cpp` (version 0.8.0 or later)
  - Ubuntu/Debian: `sudo apt-get install libyaml-cpp-dev`
  - CentOS/RHEL: `sudo yum install yaml-cpp-devel`

### Python
- `PyYAML` (included in most Python distributions)
  - Install if needed: `pip install PyYAML`
