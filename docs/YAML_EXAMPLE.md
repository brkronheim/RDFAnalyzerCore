# Example: Converting a Text Config to YAML

This example demonstrates how to convert an existing text config to YAML format.

## Original Text Config (config.txt)

```
directory=/home/user/data
saveFile=/home/user/output/results.root
saveDirectory=/home/user/output/
saveTree=Events
threads=-1
antiglobs=output.root,hists.root
globs=root,test
saveConfig=cfg/output.txt
bdtConfig=cfg/bdts.txt
correctionConfig=cfg/corrections.txt
triggerConfig=cfg/triggers.txt
floatConfig=cfg/floats.txt
intConfig=cfg/ints.txt
```

## Equivalent YAML Config (config.yaml)

```yaml
directory: /home/user/data
saveFile: /home/user/output/results.root
saveDirectory: /home/user/output/
saveTree: Events
threads: "-1"
antiglobs: output.root,hists.root
globs: root,test
saveConfig: cfg/output.yaml
bdtConfig: cfg/bdts.txt
correctionConfig: cfg/corrections.yaml
triggerConfig: cfg/triggers.txt
floatConfig: cfg/floats.txt
intConfig: cfg/ints.txt
```

## Vector Config Example

### Text format (output.txt)
```
jet_pt
jet_eta
jet_phi
muon_pt
muon_eta
electron_pt
```

### YAML format (output.yaml)
```yaml
- jet_pt
- jet_eta
- jet_phi
- muon_pt
- muon_eta
- electron_pt
```

## Multi-Key Config Example

### Text format (corrections.txt)
```
file=corrections.json name=muonSF type=weight inputVariables=pt,eta
file=corrections.json name=electronSF type=weight inputVariables=pt,eta
file=jec.json name=jetCorrection type=variation inputVariables=pt,eta,phi
```

### YAML format (corrections.yaml)
```yaml
- file: corrections.json
  name: muonSF
  type: weight
  inputVariables: pt,eta

- file: corrections.json
  name: electronSF
  type: weight
  inputVariables: pt,eta

- file: jec.json
  name: jetCorrection
  type: variation
  inputVariables: pt,eta,phi
```

## Using YAML Configs

### From C++
```cpp
#include <ConfigurationManager.h>

// Auto-detects YAML format from .yaml extension
ConfigurationManager config("config.yaml");

// Use exactly as before
std::string directory = config.get("directory");
int threads = std::stoi(config.get("threads"));

// Parse nested configs
auto corrections = config.parseMultiKeyConfig(
    "cfg/corrections.yaml",
    {"file", "name", "type", "inputVariables"}
);

auto outputs = config.parseVectorConfig("cfg/output.yaml");
```

### From Python
```python
from submission_backend import read_config, write_config

# Read YAML config
config = read_config("config.yaml")

# Access values
directory = config['directory']
threads = int(config['threads'])

# Modify and write back (preserves YAML format)
config['threads'] = '4'
write_config(config, "config.yaml")
```

### Generate Submission Files
```bash
# Using YAML config - output will also be YAML
python core/python/generateSubmissionFilesNANO.py \
    -c analysis_config.yaml \
    -n my_analysis \
    -s 30 \
    -x /path/to/x509 \
    -e ./build/analyzer

# The generated submit_config.yaml in each job directory 
# will also be in YAML format
```

## Benefits of YAML

1. **More readable**: Hierarchical structure is clearer
2. **Better for complex configs**: Easier to see relationships
3. **Comments**: Can add descriptive comments anywhere
4. **Type preservation**: Numbers and strings are properly typed
5. **Lists and nested structures**: Native support for complex data

## Conversion Script

You can create a simple script to convert existing configs:

```python
#!/usr/bin/env python3
import sys
from submission_backend import read_config, write_config

if len(sys.argv) != 3:
    print("Usage: convert_config.py input.txt output.yaml")
    sys.exit(1)

config = read_config(sys.argv[1])
write_config(config, sys.argv[2])
print(f"Converted {sys.argv[1]} to {sys.argv[2]}")
```

Usage:
```bash
python convert_config.py config.txt config.yaml
```
