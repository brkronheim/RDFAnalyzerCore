---
name: rdfanalyzer-python
description: Write Python analyses or extend RDFAnalyzerCore pybind11 bindings, including C++ expressions, NumPy vector inputs, Numba function pointers, native wrappers, and import/runtime configuration.
---

# Python bindings and tools

For pure Python production/configuration tools use the shared
[run-bundle contract](../../../docs/RUN_BUNDLES.md),
[analysis input helpers](../../../core/python/analysis_inputs.py), and
[category metadata](../../../core/python/category_layout.py). These do not
require importing the native binding; route execution/staging changes to
[production](../rdfanalyzer-production/SKILL.md).

For writing an analysis in Python, read [expressions, NumPy and Numba](references/analysis-authoring.md). It explains the supported interfaces, vector-versus-event semantics, native lifetimes and variation behavior. For remote Python environments use [LAW packaging](../rdfanalyzer-production/references/law-environment.md).

Start with [python_bindings.cpp](../../../core/python/bindings/python_bindings.cpp), [histogram_io_bindings.cpp](../../../core/python/bindings/histogram_io_bindings.cpp), and [bindings/CMakeLists.txt](../../../core/python/bindings/CMakeLists.txt). The extension is `rdfanalyzer` and the configured output directory is `build/python`. Inspect the current build and exported symbols before relying on a documented API; unbuilt source additions are not available in the imported module.

Trace argument conversion, C++ ownership/return-value policy, object lifetime, exception translation, and GIL release around long native work. Do not pass a Python callback to a C++ template merely because the C++ API has that template. Identify the supported expression/wrapper API or add a binding that preserves lifetime and threading semantics.

[cpp_wrapper.py](../../../core/python/cpp_wrapper.py) runs executables and stages shared libraries; it is separate from the pybind extension. [histogram_io.py](../../../core/python/histogram_io.py) and [extract_nd_histograms.py](../../../core/python/extract_nd_histograms.py) read histogram artifacts; use [histograms](../rdfanalyzer-histograms/SKILL.md) for their schema. For PlotSession/request objects use [plotting](../rdfanalyzer-plotting/SKILL.md).

Use [env.sh](../../../env.sh) and inspect the active Python, ROOT installation, extension suffix, and `PYTHONPATH`. When PyROOT is needed, ensure the matching ROOT Python libraries are discoverable; a `.venv` alone does not establish that. Read [PYTHON_BINDINGS](../../../docs/PYTHON_BINDINGS.md) alongside source when examples disagree.

Build the `rdfanalyzer` target and run the closest configured binding tests after a binding change. Existing checks include [test_bindings.py](../../../core/python/bindings/test_bindings.py), [plotting bindings](../../../core/tests/cpp/test_plotting_bindings.py), and [cpp wrapper](../../../core/tests/python/test_cpp_wrapper.py). Run the affected Python tool with a bounded real input when its native import path changes. Report the imported module path so a stale installed extension cannot masquerade as validation.
