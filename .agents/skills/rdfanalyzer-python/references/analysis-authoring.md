# Python analysis authoring: expressions, arrays, and native functions

Read the implementation of `AnalyzerPythonWrapper` in [python_bindings.cpp](../../../../core/python/bindings/python_bindings.cpp) and the exports below it. Examples: [string expressions](../../../../examples/example_string_expressions.py), [NumPy arrays](../../../../examples/example_numpy_arrays.py), [Numba functions](../../../../examples/example_numba_functions.py), and [histogram booking](../../../../examples/example_hist_booking.py). These demonstrate APIs; their physics formulas and input branches are not analysis defaults.

## C++ expression strings

Python `Define`/`DefineJIT` accept C++ expressions. An explicit dependency list compiles a typed wrapper and lets the binding resolve supported paired systematic inputs through the manager. With an empty list, the current path uses ROOT automatic discovery and directly attaches the nominal expression. Do not assume both modes provide identical systematic expansion, or that explicit replica labels are handled exactly like paired nuisances. Inspect available/produced columns with the affected variation.

Existing-column definitions currently warn and skip; a repeated name does not replace the old expression. `Filter`/`FilterJIT` build a private predicate and delegate selection masks to `Analyzer::Filter`. Keep dependency lists, unique filter names and scalar boolean return types consistent; replacing this path with raw RDF filtering can change acceptance. See [systematics](../../rdfanalyzer-systematics/SKILL.md).

## NumPy pointer inputs are vector columns

`DefineFromVector` and its Python alias `DefineVector` accept an integer data address, length, and an allowed C++ element type. They construct an `RVec<T>` expression over that address; they do not map array entry `i` to event `i`. Every event receives the configured vector expression. Check the downstream consumer before describing such a column as scalar per-event weights; the neighboring NumPy example uses that wording and scalar-looking filters without establishing this mapping.

Use contiguous, correctly typed storage; element type must match the actual NumPy dtype and platform width. Keep the storage alive through lazy evaluation and avoid concurrent mutation. A raw integer address does not retain a Python owner or validate memory bounds. Inspect ROOT's relevant constructor behavior if ownership/copy semantics matter rather than assuming a safe copy is made. For event-indexed values, choose an explicitly keyed/indexed definition or friend/input tree matching the intended event order.

## Native function pointers

`DefineFromPointer` accepts a non-null native address and a complete C++ signature string whose argument count matches `columns`. Numba uses `@numba.cfunc` and its native address, not an ordinary Python function or arbitrary `njit` object. Match the C ABI, argument/return types, actual dataframe column types and supported signature validation. Keep the compiled function owner alive until all lazy work completes; the wrapper does not turn the address into ownership of the Python object. Native callbacks must be valid under the configured RDF concurrency.

The binding declares a unique Cling pointer/wrapper function and defines columns through the JIT path. Verify a known numerical output, type-mismatch rejection, and relevant varied output when changing this interface. Do not reuse the illustrative invariant-mass formula in the Numba example as a physical definition.

`save()` calls the Analyzer's single `run()` lifecycle. Book requested actions first, verify `enableSkim`/outputs, and do not call save twice on one instance. Inspect plugin registration/application exports for the needed manager instead of assuming all C++ methods are bound.

Verification: [test_python_bindings.py](../../../../core/tests/cpp/test_python_bindings.py), [binding smoke tests](../../../../core/python/bindings/test_bindings.py), and [CMake test registration](../../../../core/tests/cpp/CMakeLists.txt). Report the actual imported extension/interpreter. Run examples only with a matching bounded input/config; successful parsing of a sample script does not demonstrate event execution.
