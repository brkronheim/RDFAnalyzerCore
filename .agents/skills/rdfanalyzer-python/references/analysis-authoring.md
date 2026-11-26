# Python analysis authoring: expressions, arrays, and native functions

For explicit calibration shifts and systematic dependency optimization, use
[safe systematic authoring](../../../../docs/SYSTEMATIC_AUTHORING.md). It covers
terminal `define_nominal` functions, mappings installed before consumers,
cross-call collision hazards, and variation-aware early gates. The shared native
planner's new-request collision check takes effect after rebuilding the module;
source changes do not update an already-loaded Python extension.

Read the implementation of `AnalyzerPythonWrapper` in [python_bindings.cpp](../../../../core/python/bindings/python_bindings.cpp) and the exports below it. Examples: [string expressions](../../../../examples/example_string_expressions.py), [NumPy arrays](../../../../examples/example_numpy_arrays.py), [Numba functions](../../../../examples/example_numba_functions.py), and [histogram booking](../../../../examples/example_hist_booking.py). These demonstrate APIs; their physics formulas and input branches are not analysis defaults.

## C++ expression strings

Start new Python analyses with [rdfanalysis](../../../../core/python/rdfanalysis.py)
and [Python authoring](../../../../docs/PYTHON_AUTHORING.md). `AnalysisConfig`
validates basic run settings without ROOT. Its `options` field retains native
string configuration for features without a typed frontend.

`Stage.validate(analysis, strict=True)` reports structural declaration errors
before lowering; it does not compile kernels or certify native/event types.
Optional `Stage(..., record_sources=True)` retains primitive declaration locations
for lowering diagnostics without holding calling frames or locals. Both use
the existing [stage contract](../../../../docs/TYPED_SETTINGS_AND_STAGES.md);
source recording defaults off and adds no event callbacks.

`CollectionRef` resolves field names to ordinary strings. Optional `FieldMetadata`
declares expected type spelling, unit labels and help. Explicit
`schema_requirements()` bridges those declarations to the existing supplied
schema checker, without inspecting files or changing a graph. Type spellings
remain reader-owned and exact; units stay informational. There is no type
inference, conversion, alignment check or automatic Analysis registration.
See [collection semantics](../../../../docs/NAMED_OUTPUTS_AND_COLLECTIONS.md).

The [compatibility/migration table](../../../../docs/AUTHORING_COMPATIBILITY.md)
distinguishes stdlib helpers, base native API4 and optional methods. Use
`doctor --require-capability` or `diagnose_runtime(require_capabilities=...)` to
check the actual loaded module before inputs; API4 does not imply every optional
batch/axis operation. Offline compiled-controller results can be recovered with
`rdfworkflow result` / `read_command_result` using only persisted run.json;
see [process-scope recovery](../../../../docs/RECORDED_COMMAND_RESULTS.md).

Compiled orchestration can optionally request `LocalExecutionResources(cpus=(...))`.
Construction and description remain host-independent. Preparation/launch checks
the available Linux CPU IDs and captures the resolved `taskset` tool; only the
owned child receives the prefix. This specifies initial affinity rather than
thread counts, CPU reservations or NUMA memory policy. Preserve the real
analysis executable as the specification's executable so recorded build-product
validation remains valid. See [compiled local resources](../../../../docs/COMPILED_CPP_WORKFLOWS.md#optional-local-child-cpu-affinity).
The optional explicit `validate("local-resources")` level checks only host/tool
readiness and reports issues without validating analysis assets or launching.

[Explicit input-schema preflight](../../../../docs/INPUT_SCHEMA_PREFLIGHT.md)
checks every supplied file's requested original columns through an optional ROOT
reader, or compares supplied metadata without native imports. It is never invoked
automatically by configuration/description/construction.

`Analysis.histogram_batch(items)` shares identical resolved selection nodes/counts
within one fixed booking call. `Stage.histograms(group)` automatically chooses
that native path for a multi-histogram group when the loaded module supports the
required regular/mixed axes. Singletons and older modules keep ordinary native
booking. Previews and successful stage descriptions expose the chosen action.
Explicit `batch=True` requires the capability; `batch=False` retains ordinary
booking for compatibility. Direct histogram calls remain immediate. Every path
retains named counts, expansion budgets and filter/booking boundaries; caches
never cross calls or groups. See [stage contracts](../../../../docs/TYPED_SETTINGS_AND_STAGES.md)
for acceptance scope, rather than inferring performance from capability alone.

For category-packed native outputs, use the optional `packed-nd-histograms`
capability and [packed ND authoring guide](../../../../docs/PACKED_ND_HISTOGRAMS.md).
The facade requires an explicit canonical `histogramManager`, preserves native
storage settings, and keeps ND booking metadata separate from TH1 payloads.
Register nuisance labels before the first all-systematics group; staged groups
capture their exact action position and never cross intervening filters.

`VariableAxis`, `Analysis.histogram_variable` and `Stage.histogram_variable`
support explicit finite increasing edges with the same flow/sumw2/variation
contract. Explicit `histogram_batch` can mix regular and variable axes when the
loaded module advertises `mixed-histogram-batch`; its cache remains per call.
See [variable histogram authoring](../../../../docs/VARIABLE_HISTOGRAMS.md).
The optional [correction-batch facade](../../../../docs/CORRECTION_BATCHES.md)
exposes existing native scalar/vector evaluation with fresh explicit outputs,
per-output propagation flags and unchanged native precision. Both features are
private-binding candidates; check actual loaded-module capabilities and do not
replace a running benchmark's frozen module.

Use `Analysis.define_numba` for custom Python/NumPy kernels. It infers native
input types, uses ROOT.Numba.Declare, preserves signature-specific owners and
caches registrations in process. No custom C++ physics helper or raw address is
needed. `project` provides checked native RVec element access. Lazy `histogram`
and `count` results are finalized by an internal PythonActionsManager plugin
before provenance collection; they are in-memory results rather than automatic
ND metadata output. Use the AGC Python kernels as an executable example.

Optional `require_native=True` rejects unrecognized/ctypes wrappers before RDF
column definition. The detached `analysis.numba_bridges` report snapshots each
compiled signature's wrapper class without exposing addresses or generated
source. It does not certify the entire call graph or numerical ABI. Defaults,
stage descriptions and compilation-statistics fields remain unchanged; native
acceptance is separate. See [bridge diagnostics](../../../../docs/NUMBA_BRIDGES.md).

Python `Define`/`DefineJIT` accept C++ expressions. Omitted inputs are inferred
with the identifier scanner; supplied inputs must cover every referenced column.
Both paths compile a typed wrapper and use the shared
[variation planner](../../../../core/interface/detail/DefineVariations.h) for
paired and explicit labels. The scanner excludes comments, literals and members;
it is not a general C++ scope parser. Register variations before consuming them.
Use `define_nominal` for intentionally terminal expressions.

Existing-column Python definitions raise `ValueError`. `Filter`/`FilterJIT` build
a private predicate and delegate selection masks to `Analyzer::Filter`.
`variation_labels`, `selection_masks` and `columns` expose the graph contract.
Keep unique filter names and scalar boolean return types consistent; replacing
this path with raw RDF filtering can change acceptance. See
[systematics](../../rdfanalyzer-systematics/SKILL.md).

## NumPy pointer inputs are vector columns

`DefineFromVector` and its Python alias `DefineVector` accept an integer data address, length, and an allowed C++ element type. They construct an `RVec<T>` expression over that address; they do not map array entry `i` to event `i`. Every event receives the configured vector expression. Check the downstream consumer before describing such a column as scalar per-event weights; the neighboring NumPy example uses that wording and scalar-looking filters without establishing this mapping.

Use contiguous, correctly typed storage; element type must match the actual NumPy dtype and platform width. Keep the storage alive through lazy evaluation and avoid concurrent mutation. A raw integer address does not retain a Python owner or validate memory bounds. Inspect ROOT's relevant constructor behavior if ownership/copy semantics matter rather than assuming a safe copy is made. For event-indexed values, choose an explicitly keyed/indexed definition or friend/input tree matching the intended event order.

## Native function pointers

Optional `Analysis.filter_column(name, column)` consumes an existing scalar bool
and delegates masks/union acceptance to unchanged native `Analyzer::Filter`,
without an extra expression alias. It requires a native module exposing that
method. Older API-4 modules retain existing features and reject this operation
with a clear rebuild error. It does not replace expressions or alter declaration
order. Verify the imported module, [status/usage](../../../../docs/PYTHON_AUTHORING.md)
and [focused tests](../../../../core/tests/cpp/test_python_filter_column.py).

`DefineFromPointer` accepts a non-null native address and a complete C++ signature string whose argument count matches `columns`. Numba uses `@numba.cfunc` and its native address, not an ordinary Python function or arbitrary `njit` object. Match the C ABI, argument/return types, actual dataframe column types and supported signature validation. Keep the compiled function owner alive until all lazy work completes; the wrapper does not turn the address into ownership of the Python object. Native callbacks must be valid under the configured RDF concurrency.

The binding declares a unique Cling pointer/wrapper function and defines columns through the JIT path. Verify a known numerical output, type-mismatch rejection, and relevant varied output when changing this interface. Do not reuse the illustrative invariant-mass formula in the Numba example as a physical definition.

`save()` calls the Analyzer's single `run()` lifecycle. Book requested actions first, verify `enableSkim`/outputs, and do not call save twice on one instance. Inspect plugin registration/application exports for the needed manager instead of assuming all C++ methods are bound.

Verification: [test_python_bindings.py](../../../../core/tests/cpp/test_python_bindings.py), [binding smoke tests](../../../../core/python/bindings/test_bindings.py), and [CMake test registration](../../../../core/tests/cpp/CMakeLists.txt). Report the actual imported extension/interpreter. Run examples only with a matching bounded input/config; successful parsing of a sample script does not demonstrate event execution.


## Shared private declaration and JSON rules

The private [_rdftext](../../../../core/python/_rdftext.py) helper supplies the
same contextual nonempty/NUL/newline rule to modules and stage declarations.
[_rdfjson](../../../../core/python/_rdfjson.py) shares finite immutable JSON
snapshots and restoration between results and validation metadata. Thin callers
retain their established diagnostics and distinct string-key subclass policy.
Both helpers belong in the Python install closure when their callers are shipped;
they perform no I/O, native import, discovery, event callback or physics lowering.

For requested versus observed ROOT threading and physical source-row identity,
use [runtime threading and identity](../../../../docs/PYTHON_THREADING_AND_IDENTITY.md).
The optional private binding snapshot reports pool/graph capacity without running
an event loop; old modules report unavailable observations explicitly. Capacity
is not proof of active thread utilization. Under ROOT IMT, `rdfentry_` is not a
stable TChain input-row index; use validated persisted source/event keys or aligned
provenance friends, with ordinary identifier aliases for qualified friend fields.
