# Analyzer SDK: three authoring routes

This example reads an `Events` tree with a float32 `genWeight` branch. It converts
weights to double, defines `value = 2 * weight`, selects nonzero values, and writes
counts, signed sums and a four-bin histogram from -8 to 8 with both flows and
Sumw2. Every action is booked before one event loop. No execution occurs without
`--run`; output paths must be fresh and their parent directories must exist.

For compiled C++, install the optional AnalyzerSDK component and build this
example against that prefix:

```bash
cmake -S /path/to/prefix/share/rdfanalyzer/examples/analyzer-sdk \
  -B /tmp/analyzer-example -DCMAKE_PREFIX_PATH=/path/to/prefix
cmake --build /tmp/analyzer-example --parallel 1
/tmp/analyzer-example/analyzer-onboarding
/tmp/analyzer-example/analyzer-onboarding --run analysis.txt /tmp/cpp-result.json
```

The text configuration contains the actual ROOT URLs directly, not a file-list
filename:

```text
fileList=/absolute/path/input.root
treeList=Events
enableSkim=false
enableSystematics=false
enableCounters=true
batch=true
metaFile=/tmp/example-metadata.root
```

Python steering uses the same compiled executable. Install the independent
PythonController component and add `PREFIX/python` to `PYTHONPATH`:

```bash
python steer.py --executable /tmp/analyzer-example/analyzer-onboarding \
  --config analysis.txt --output /tmp/steered-result.json
python steer.py --executable /tmp/analyzer-example/analyzer-onboarding \
  --config analysis.txt --output /tmp/steered-result.json --run --logs /tmp/steering-logs
```

For Python-authored physics, `physics.py` contains the actual custom functions.
Build/install the matching Python component and use a compatible ROOT/Numba
runtime. `inputs.json` uses `{"files": [{"url": "/absolute/path/input.root"}]}`.
Registration explicitly requires native CFunc bridges:

```bash
python python_analysis.py
python python_analysis.py --run --input-list inputs.json --output /tmp/python-result.json
```

The Python result also exposes the histogram's entry counter. Timings distinguish
setup, processing and detached result extraction; JSON writing is outside the
extraction field. They are examples of scope, not comparative benchmark results.
The SDK's supported build configuration and recorded acceptance limits are in
`docs/ANALYZER_SDK.md` in the matching source checkout.
