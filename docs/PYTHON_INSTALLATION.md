# Install and check Python authoring

For production stages, install the language-free [SnakemakeController component](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/snakemake/README.md#install-a-relocatable-workflow-controller).

For Python orchestration of prebuilt C++ executables, the separate
[`PythonController` installation](COMPILED_CPP_WORKFLOWS.md#build-and-install-the-controller)
needs no ROOT, pybind11 or compiler. It includes controller/result IDE signatures
and a steering example. The shared [input schema preflight](INPUT_SCHEMA_PREFLIGHT.md)
also compares supplied metadata without ROOT; explicit ROOT acquisition needs PyROOT.
The native authoring installation follows below.

Use the Python and compiler environment matching the ROOT build. A local prefix
installs the native module, typed frontend, enabled CMS Python package, and its
ONNX/XRootD adaptor runtime libraries. ROOT, XRootD client and compiler ABIs
remain supplied by that environment.

The Python component also installs authoring/settings/stage/collection IDE
signatures and a [small Python/Numba tutorial](PYTHON_TYPING_AND_TUTORIAL.md).

```bash
source env.sh
export PYTHONPATH="$ROOTSYS/lib:${PYTHONPATH:-}"
python -m pip install -r requirements-python.txt
cmake -S . -B build -DPython_EXECUTABLE="$(command -v python)"
cmake --build build --target rdfanalyzer -j4
cmake --install build --prefix "$PWD/local-python" --component Python
export PYTHONPATH="$PWD/local-python/python:$PYTHONPATH"
python -m rdfanalysis doctor --numba
```

Add `--cms` to require the enabled CMS authoring facade. `doctor` prints the
actual interpreter, Python ABI, imported module paths, ROOT/package versions,
native authoring API check, and a three-event compiled Numba execution. It
returns a nonzero status for a failed required check and opens no user data.
The report also records user-site enablement. Disable it with
`PYTHONNOUSERSITE=1` when checking an isolated environment, and install its full
dependency closure there.

API4 modules can have different optional operations. `doctor` now lists the
loaded module's optional capabilities without constructing an analysis. Require
a needed feature explicitly before submitting or opening analysis inputs:

```bash
python -m rdfanalysis doctor --require-capability correction-batch \
  --require-capability mixed-histogram-batch
```

Repeat the option for several features. Missing optional features fail only when
requested; ordinary older modules remain usable for supported operations. Python
callers use `diagnose_runtime(require_capabilities=[...])`. The common feature
list is shared with `Analysis.describe().capabilities`. This checks entry-point
availability, not payload correctness or a physics/performance guarantee. The
optional `--numba` execution probe remains separately requested.

Install the frontend and native module together. Authoring API4 version checks
reject stale pairs before file access. Rebuild against a different ROOT/Python
ABI when changing the environment. This prefix is an authoring installation;
the production CLI, run bundles and analysis assets retain their checkout-owned
workflows. A portable binary wheel is separate work.

The installed module locates bundled runtime libraries using
`$ORIGIN/../lib`. The adaptor's ROOT dictionary and headers accompany its shared
library. ONNX Runtime licenses/notices accompany the installed libraries.
