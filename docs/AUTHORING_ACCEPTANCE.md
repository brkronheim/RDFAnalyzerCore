# Checking an authoring installation

Use the same ROOT, compiler and Python environment for the producer and consumer. Run `python -m rdfanalysis doctor --numba` from the installed prefix to check imports and native execution. Add `--require-capability` for each optional operation used by your analysis. [Installation guide](PYTHON_INSTALLATION.md).

The [SDK example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/analyzer-sdk/README.md) exercises direct C++, Python steering of the compiled program and independently authored Python kernels. Its single-thread scalar fixture has 15 input events, 14 selected events and a selected signed-weight sum of −1. Compare the bin contents, flows and squared weights as well as the event count.

The CPU SDK requires the producer's compiler and ROOT/Boost versions. Multithreading, other native libraries and production worker environments require checks with the inputs and configuration used there. [SDK requirements](ANALYZER_SDK.md) and [authoring compatibility](AUTHORING_COMPATIBILITY.md).
