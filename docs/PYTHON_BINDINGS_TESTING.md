# Testing the Python bindings

From the repository root, select the Python and ROOT environments used to build the extension. The integration test creates a temporary ROOT input and configuration, runs the analyzer, and checks the saved Events tree. It needs NumPy and uproot. If Numba is available it compiles two callbacks, otherwise it exercises the same pointer signatures with ctypes.

```bash
./test_python_bindings.sh
```

The wrapper invokes the active `python3`. By default the test prepends the checkout’s `build/python` to the module search path. Set `RDFANALYZER_PYTHON_MODULE_DIR` for another build. Check the printed module filename to verify which extension loaded.

| Exit | Interpretation |
|---|---|
| 0 | The integration checks passed. |
| 1 | A check or execution step failed. Inspect the first error and any compiler or loader diagnostics. |
| 77 | The extension or uproot could not be imported. The skip message includes the exception. |

For a CMake build with binding tests enabled, run the registered test against its configured Python interpreter and module directory.

```bash
cd build  # use build-clang for the Clang preset
ctest -R '^PythonBindingsIntegrationTest$' --output-on-failure --no-tests=error
```

CTest reports exit 77 as skipped. Inspect the summary when collecting evidence of successful execution. For setup and targeted checks, see the [installation guide](PYTHON_INSTALLATION.md) and [detailed binding checks](PYTHON_BINDINGS_TESTING_DETAILED.md).
