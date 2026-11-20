# Inspecting Python/Numba bridges

`Analysis.numba_bridges` reports the wrapper selected during kernel registration. It is a detached list of dictionaries containing the column, normalized input types, requested return type, wrapper class, classification and whether the RDF column was registered. It contains no function addresses or generated source bodies. Reading the property does not import a runtime or execute a kernel.

```python
analysis.define_numba("ht", selected_ht, ["Jet_pt", "Jet_eta"],
                      return_type="double", require_native=True)
print(analysis.numba_bridges)
```

`require_native` defaults to `False`, preserving existing ROOT wrapper behavior. Explicit `True` requires a recognized `numba.core.ccallback.CFunc` object. A known ctypes wrapper or an unrecognized wrapper raises an actionable error before the RDF column is defined. Objects that merely expose an `address` attribute are insufficient.

The three classifications are:

| `kind` | Meaning |
|---|---|
| `native-cfunc` | The registered object is an instance of the already loaded Numba `CFunc` class. |
| `ctypes-callback` | The registered object is a ctypes function-pointer wrapper, matching ROOT's ctypes fallback mechanism. |
| `unknown` | The object is absent, its class is unrecognized, or an older cache entry lacks a signature-specific snapshot. |

This classifies the wrapper object. It does not prove that the entire kernel call graph is free of interpreter activity, that the C ABI is numerically correct, or that array ownership and concurrency are safe. Generated source and numerical native controls remain separate evidence. `return_type` records the requested spelling; `None` means ROOT inferred it and this report does not certify that inferred type.

Each compiled signature owns an immutable classification snapshot. ROOT can update attributes on the original Python callable when it registers a second signature. A later cache hit therefore uses the first signature's saved classification and retained owners, rather than the callable's latest attributes. Strict mode also checks cache hits. Existing cache tuple/lifetime handling and `compilation_stats` keys remain unchanged.

A failed strict check appears in the report with `definition_registered=False`. ROOT registration/compilation may already have occurred; this is not a rollback of its JIT registry. Direct use can explicitly retry the same still-undefined column with the default policy if the fallback is intended. A failure while applying a stage retains the existing failed-stage behavior; inspect it and construct a fresh stage/analysis as needed.

Named outputs and stages forward the same option:

```python
outputs = KernelOutputs("summary", ("accepted", "ht"))
stage = Stage("selected").define_numba_outputs(
    outputs, summary_kernel, ["Jet_pt"],
    return_type="RVec<double>", require_native=True,
)
```

`Stage.define_numba(..., require_native=True)` is also supported. Stage construction/preview validates this option as a boolean without importing ROOT or executing the callable. Omitting it preserves existing default stage descriptions. Definitions and filters remain interleavable. The `define_numba` stage method retains its existing generic keyword interface; the direct Analysis and named-output signatures expose the option explicitly in stubs.

These diagnostics add registration-time Python state only. They add no event callback, kernel wrapper, input projection, action or event-loop branch. Default `Analysis.describe()` and recorded compilation statistics retain their previous shapes. The existing permissive path remains available for different ROOT/Numba versions and wrapper types.

Source acceptance currently covers stdlib mocks, multi-signature cache mutation, fallback/unknown rejection before RDF lowering, retained owners, detached reports, stage forwarding and structural stub checks. Actual ROOT/Numba/JIT, installed runtime/type-checker and sustained performance acceptance are deferred until the active benchmark matrix is idle. The benchmark matrix uses its frozen older frontend and is unaffected by this source change. See the [Python authoring guide](PYTHON_AUTHORING.md) for native environment requirements and the [compatibility table](AUTHORING_COMPATIBILITY.md) for other optional capabilities.


## Default visibility of unconfirmed bridges

`define_numba` now emits a setup-time `RuntimeWarning` when its observed bridge
is a ctypes callback or has unknown classification, including cache hits. The
warning names the column and classification, explains that a per-event Python
callback may be used, and points to `require_native=True` and `numba_bridges`.
Known CFunc registrations remain quiet. The opt-in strict flag still rejects
before graph registration; signatures, cache keys, diagnostics and numerical
behavior are unchanged. This makes the compatibility/debug path visible without
requiring an expert to discover the optional report first. Classification still
does not certify the complete native call graph. Sixteen stdlib bridge/cache/
warning tests pass; fresh native/installed source qualification is recorded
separately in the authoring manifest.
