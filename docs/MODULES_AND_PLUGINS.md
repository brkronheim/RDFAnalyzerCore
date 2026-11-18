# Optional modules and plugin descriptions

Installed module/provider IDE signatures and optional CMS recipe signatures are
documented in [the typing guide](PYTHON_TYPING_AND_TUTORIAL.md). Metadata remains
optional, and native implementations retain their existing lifecycle.

`ModuleSpec`, `AnalysisModule` and `ModuleSequence` add optional descriptions and
construction-time composition to the [Python authoring API](PYTHON_AUTHORING.md).
They use existing graph/plugin operations. They add no Python event callbacks,
implicit sorting, collection copies, native virtual methods or scheduler.

## Compose existing graph builders

```python
from rdfanalysis import AnalysisModule, ModuleSpec, ModuleSequence, Stage

def build_selection(analysis, settings):
    stage = Stage("selection")
    stage.define("accepted", "jet_ht > ht_min")
    stage.filter("ht", "accepted")
    stage.define("selected_ht", "jet_ht")
    analysis.apply(stage)

selection = AnalysisModule(
    ModuleSpec("analysis.selection", requires=("jet_ht", "ht_min"),
               produces=("accepted", "selected_ht"), dependencies=("example.ht",)),
    build_selection,
)
sequence = ModuleSequence((ht_module, selection))
print(sequence.describe().explain())  # metadata only; no builder calls
print(sequence.preview(analysis).explain())  # preflight declared boundaries
analysis.use(ht_module, selection)
analysis.histogram("ht", "selected_ht", 25, 50, 550)
analysis.run()
```

`ht_module` is supplied by a provider; the external example below implements it.
The input/threshold columns must exist before the relevant builder runs.
Definitions, projections and filters retain the exact order written in each
builder, and modules execute **in supplied order**. Missing, cyclic or forward
dependencies fail instead of hoisting work across filters. `dependencies` names
prior module instances; native plugin **role** dependencies remain owned by the
existing Analyzer lifecycle.

Use existing `define_numba`/named-output helpers with the original function.
Constructing a new wrapper kernel per graph can defeat Numba cache reuse;
module composition does not create such wrappers. Builders receive
`(analysis, settings)` once during explicit `use`/`apply`; ordinary callable
objects and functions are supported. Their return value is ignored. Only plain
detached metadata is retained in `analysis.inspect()["modules"]`/`describe()`;
the analysis does not retain setup builders. Existing native kernel owners are
retained through the normal authoring path.

## Declare fields, resources and action destinations

`ModuleSpec` describes:

- `requires`: existing input columns, including outputs of preceding modules;
- `produces`: new output column names;
- `dependencies`: module instance IDs that must precede this module;
- `resources`: exclusive logical tokens such as `cms.jet_corrections` or a
  plugin instance, to prevent applying an advertised correction twice;
- `actions`: explicit `ActionClaim("histogram", "group/ht")`, `count` or `sum`
  destinations; histogram/count namespaces and ROOT directory collisions are
  checked consistently with the existing frontend;
- `implementation`, `capabilities`, `documentation`, and optional
  `settings_schema`: descriptive, provider-owned metadata.

Every module in a sequence is preflighted before its first builder runs. Duplicate
module IDs, reserved/existing output columns, resource conflicts, missing input
columns/dependencies and declared action conflicts raise without changing the
graph. With the current binding, point queries avoid exporting the entire native
column inventory.
Repeated application is an error rather than silent deduplication. Distinct
instances can use distinct IDs/output names when appropriate; exclusive resource
tokens still prevent accidental duplicate corrections.

After each successful setup builder, declared outputs and action destinations
are checked. No rollback is attempted after native lowering starts. A builder
error or missing advertised output/action marks `module_failure` and blocks
frontend `run()`/further `use()`; start a fresh Analysis. Frontend `run()` inside
a builder and nested `use()` are rejected. Expert access through `analysis.native`
remains available and is outside these frontend execution guards.

Contracts describe a **declared boundary**, not a complete DAG. Unadvertised
internal columns/actions and direct native calls remain opaque; native guards
still catch conflicts encountered during lowering. Descriptions do not prove
that arbitrary provider code implements its claimed physics or resources.
Descriptions do not replace native provenance or frozen execution identities.

## Settings and schemas stay with the provider

Settings and optional schemas must contain finite JSON-compatible mappings,
lists, strings, numbers, booleans or null. Construction freezes independent
copies. Builders and validators receive detached settings, so mutating those
does not change a reusable module. Descriptions serialize to detached JSON.

An optional `validate_settings(settings)` callable runs when constructing
`AnalysisModule`, before graph lowering. It may return the common
`ValidationReport` or use legacy raise/None behavior. Returned failing reports
are enforced; boolean/dict returns are contract errors. `module.validate()`
returns the cached configuration report without invoking the provider again.
Without a validator it reports validation unavailable. Core does not silently
enforce the descriptive `settings_schema`. See [provider validation and schema
help](PROVIDER_VALIDATION.md) for locations, explicit asset checks and discovery.
The CMS-owned `CmsCorrections.as_module()` is a native recipe adapter using this
contract and the existing native correction setup.

## Native managers and compiled C++

An annotated native builder can call existing `AddPlugin`, manager operations or
an extension's existing recipe. Use `resources` to advertise the manager or
correction instance. If outputs/actions are deferred until the native lifecycle,
set `outputs_at="native-lifecycle"`: names are reserved but are not treated as
available inputs to setup-time consumers, and Python does not verify their
production. Native lifecycle and validation retain that responsibility.

Existing plugins need no descriptions and continue to work directly. Such work
remains opaque in the partial graph explanation. Native enabled extensions can
still be queried through the existing `rdfanalyzer.enabled_extensions` and
`has_extension` API; module descriptions do not replace that build registry.

For a standalone compiled analysis, `ModuleSpec(implementation="compiled-cpp",
outputs_at="native-lifecycle", ...)` can document its components alongside a
[CppAnalysisSpec](COMPILED_CPP_WORKFLOWS.md) description, without importing ROOT
or the native Python binding. Compile/link/run still uses the existing compiled
controller. `Analysis.use()` composes in-process graph builders; it does not
translate Python modules into a C++ executable or launch subprocesses. Production
adapters and a common cross-backend result envelope remain separate work.

## External provider and explicit discovery

The [external authoring example](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/authoring-extension/README.md)
supplies a Numba HT module and its settings/schema. It owns its policy, package,
manifest and optional CMake INTERFACE target. No core plugin-dispatch edits are
required. The function sums array elements above a supplied threshold column;
empty arrays return zero.

Install its Python package, or select its source checkout using the existing
extension mechanism. A manifest may advertise:

```json
"python_services": {"authoring_modules": "modules"}
```

That provider exports `module_specs()` returning `ModuleSpec` objects. Discovery
is explicit:

```python
from rdfanalysis import discover_module_specs
for spec in discover_module_specs():
    print(spec.describe().explain())
```

Importing `rdfanalysis`/`rdfmodules` does not scan manifests or import providers.
The generic source-manifest loader honors existing extension enable flags;
disabled/absent providers contribute nothing, duplicate description IDs fail,
and enabled provider errors propagate. An installed provider without a source
manifest can simply supply specs/factories through a normal Python import.
Discovery lists descriptions; it does not automatically apply modules.
