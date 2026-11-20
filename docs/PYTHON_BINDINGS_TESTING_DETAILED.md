# Python binding coverage and diagnostics

The [integration-test guide](PYTHON_BINDINGS_TESTING.md) gives the wrapper and CTest commands. The integration test exercises the following contracts in one temporary analysis.

| Contract | Observable check |
|---|---|
| Binding surface | Selected Analyzer methods and both histogram descriptor classes are present. |
| Configuration and plugins | The analyzer reads back configuration values and inspects plugin state with minimal configurations. |
| Callback validation | Null pointers and a mismatched callback arity raise Python errors before execution. |
| Variations | Two chained pointer definitions produce JES-shifted columns. The selected shifted row also depends on a C++ raw string and member access surviving expression rewriting. |
| Histogram booking | Booking returns region metadata. The test clears the booking before saving. |
| Output | uproot reopens the snapshot and checks the entry count and expected nominal and shifted branch names. |

For a narrower failure, list the tests registered in your build and run the relevant one. For example, the boolean-filter test and the variable-histogram test have independent entry points.

```bash
cd build  # use build-clang for the Clang preset
ctest -N -R '^Python'
ctest -R '^PythonBoolColumnFilterTest$' --output-on-failure --no-tests=error
ctest -R '^PythonVariableHistogramTest$' --output-on-failure --no-tests=error
```

## Diagnosing import and callback failures

- For an import failure, compare the interpreter used to build the module with the interpreter printed by `python3 -c 'import sys; print(sys.executable, sys.version)'`. The module filename identifies its Python ABI. A loader error naming a shared library points to the native runtime environment.
- If the extension loads from an unexpected directory, select the intended build with `RDFANALYZER_PYTHON_MODULE_DIR` and check the reported filename again.
- For a callback error, check the declared C++ result and argument types, the input column types, and the number and order of columns. Keep the compiled callback alive until the graph finishes. The [Python bindings guide](PYTHON_BINDINGS.md) explains pointer and vector definitions.
- For a Numba-specific failure, use the environment checks in the [installation guide](PYTHON_INSTALLATION.md). A run using the ctypes fallback establishes pointer integration only.

For a regression, extend the smallest test that reaches the affected public entry point. When the change affects filtering, expression rewriting or systematic columns, check the saved output as well as graph construction.
