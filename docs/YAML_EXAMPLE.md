# Example: Converting a Text Config to YAML

This example demonstrates how to convert an existing text config to YAML format.

## Original Text Config (config.txt)

```
directory=data
saveFile=output/results.root
saveDirectory=output/
saveTree=Events
threads=1
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
directory: data
saveFile: output/results.root
saveDirectory: output/
saveTree: Events
threads: 1
antiglobs: [output.root, hists.root]
globs: [root, test]
saveConfig: cfg/output.yaml
bdtConfig: cfg/bdts.yaml
correctionConfig: cfg/corrections.yaml
triggerConfig: cfg/triggers.yaml
floatConfig: cfg/floats.yaml
intConfig: cfg/ints.yaml
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

### Convert existing files

Run the supplied converter once for each legacy input. Choose the kind according to the consuming plugin. The converter preserves scalar values and leaves source files in place. Update references in the main YAML to point at the converted files.

```bash
python core/python/convert_config.py config.txt config.yaml
python core/python/convert_config.py --kind records cfg/corrections.txt cfg/corrections.yaml
python core/python/convert_config.py --kind list cfg/output.txt cfg/output.yaml
```

Use the Snakemake production workflow to generate worker configurations. Each generated job receives a YAML configuration. See the [Snakemake guide](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/snakemake/README.md) and the [native YAML reference](YAML_CONFIG_SUPPORT.md).
