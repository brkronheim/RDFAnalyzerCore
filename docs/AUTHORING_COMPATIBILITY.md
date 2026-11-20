# Authoring compatibility and migration

## Systematic definition safety

Use [safe systematic authoring](SYSTEMATIC_AUTHORING.md) for explicit calibrated
variations and dependency optimization. Legacy C++ definitions keep an existing
nominal column; Python definitions reject that duplicate. The shared variation
planner now rejects conflicting new physical-output requests within one
expansion, before attaching ROOT nodes. Existing producer reuse and cross-call
mapping behavior are unchanged, and the separate vector lowering is outside
this check. Existing binaries/modules need a rebuild to acquire the safeguard.
See the guide for checked names, mapping order and validation limits.

## Select optional operations explicitly

```bash
python -m rdfanalysis doctor --require-capability histogram-batch \
  --require-capability correction-batch
```

The report names the actual interpreter/native module and optional operations.
Missing optional operations fail only when requested; they are not new base
requirements for every API4 module. `--numba` remains a separately requested
synthetic execution probe. Method availability does not validate payload schema,
model physics or analysis correctness. [Installation details](PYTHON_INSTALLATION.md).

Capability discovery reports method availability, not a correctness certification
of every operation/payload. The correction guide records known candidate limits
and pending native regression gates. [Pinned Pyright acceptance](PYRIGHT_ACCEPTANCE.md)
checks installed public signatures; native escape hatches remain deliberately
extensible and only partially typed.

Install matching frontend/stubs/native bindings against the selected ROOT/Python
ABI. Loading a different binary by changing its filename is not an ABI migration.
Keep a running worker's frozen module, libraries, models and configuration intact;
rebuild/install into a separate prefix for a new comparison.

## Preserve existing analysis semantics

Existing `histogram(name, variable, bins, low, high, ...)` calls need no migration.
Use `histogram_variable(..., edges, ...)` explicitly for nonuniform bins. Mixed
batch mappings specify exactly one axis kind. Native modules lacking the
requested operation raise a rebuild/path error; they do not quietly change to a
different execution strategy. [Variable histogram guide](VARIABLE_HISTOGRAMS.md).

`Stage.histograms(group)` automatically uses native batching for multiple expanded
histograms when the loaded module supports their axis kinds. Singletons and
modules lacking the applicable batch capability retain ordinary booking.
Explicit `batch=True/False` retains its established effect at that action boundary.
Definitions and filters retain supplied order;
counts/actions on opposite sides of a filter retain their own booking nodes.
No global selection-node cache or implicit action reordering is introduced.
[Stage semantics](TYPED_SETTINGS_AND_STAGES.md).

Existing single correction methods remain available. A correction batch does
not register a nuisance, modify a weight product, add a shape-by-weight Cartesian
policy or cast the native output precision. Use fresh output names after changing
inputs; per-output propagation flags state which corrections should respond to
input shape variations. [Correction guide](CORRECTION_BATCHES.md).

## Keep result contracts distinct

`rdf-run-result-v1` is the common detached envelope. Unknown summaries remain
empty mappings and an unavailable loop count is `null`. Native emitters must
provide the complete documented envelope; extensions go in `details` or
`provenance`. A process-scope command conversion does not manufacture event
counts or certify physics. [Result contract](RUN_RESULTS.md).

`rdf-command-result-v1` remains the controller's persisted/default stdout format.
`--result-format run-result` changes immediate build/run stdout only. Offline
`read_command_result` accepts the writer's optional artifacts and explicitly
documented legacy omissions without deriving missing provenance or identities.
[Recorded-result recovery](RECORDED_COMMAND_RESULTS.md).

Existing native `Analyzer`, plugin lifecycles and direct C++ analysis code remain
available. Python steering of a compiled executable and loading a compiled helper
into Python are separate modes. A common native emitter/recorder interface is
still a proposal requiring compatibility review.
No new mandatory plugin description or Python dependency is imposed on native
analyses by these authoring additions.
