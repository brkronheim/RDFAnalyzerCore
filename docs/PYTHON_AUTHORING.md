# Python authoring

[IDE signatures and the installed Python/Numba tutorial](PYTHON_TYPING_AND_TUTORIAL.md)
cover the frontend, settings, collections and stages while preserving ordered
definitions/filters and the native escape hatch.

[Detached run results](RUN_RESULTS.md) provides explicit snapshots after a
successful Python run and a common envelope for session/compiled results.

[Provider validation and schema help](PROVIDER_VALIDATION.md) supports shared
configuration reports, explicit asset checks and the native CMS recipe adapter.

For an existing scalar boolean column, use `analysis.filter_column(name, column)`. Check `bool-column-filter` in `analysis.describe().capabilities` when loading older native modules.

[Input schema preflight](INPUT_SCHEMA_PREFLIGHT.md) can explicitly check every
supplied file before construction. Imports/config validation do not open inputs.

Use `analysis.histogram_batch(items)` to book several histograms at one graph node. [Typed stages](TYPED_SETTINGS_AND_STAGES.md) describes automatic grouping and explicit batch selection.

[Native correction batches](CORRECTION_BATCHES.md) expose existing scalar/vector
operations with explicit output names and per-output propagation. Native precision
is retained, with no Python event callbacks.

The `rdfanalysis` frontend supplies typed run settings and a small fluent API.
The existing `rdfanalyzer` native module remains available for plugins,
histogram booking, plotting and expert interfaces. Build the `rdfanalyzer`
target and add `build/python` to `PYTHONPATH` in the matching ROOT environment.

Optional [typed input/skim settings and explicit declaration stages](TYPED_SETTINGS_AND_STAGES.md)
add schema help, local config-asset checks, ordered regions and bounded histogram
groups. They lower to the existing immediate/native operations before execution.

[Named Numba outputs and collection references](NAMED_OUTPUTS_AND_COLLECTIONS.md)
add field names, delayed projections and declared alignment metadata while
preserving the same typed native graph and original kernel callable.

[Optional modules and plugin descriptions](MODULES_AND_PLUGINS.md) add reusable
setup builders, declared dependency/resource/action checks and explicit external
provider discovery. They preserve supplied order and existing native lifecycles.

Python can also orchestrate an ahead-of-time compiled C++ analysis executable,
using the existing executable/production paths without Python event kernels.
The additive [compiled-C++ controller](COMPILED_CPP_WORKFLOWS.md) now supplies
typed local build/run descriptions without importing the native authoring stack.
`AnalysisConfig.describe()` and `validate("configuration")` work without native
imports or input opening. `python -m rdfanalysis explain analysis.yaml` prints
the configuration description; this is distinct from inspecting a built graph.
For an existing Analysis, `describe()` returns the same versioned envelope with
the current graph inventory and tracked authoring operations.
`describe(include_types=True)` additionally queries column types explicitly.
Neither triggers an event loop. Native/plugin internals remain opaque.

After building the graph, inspect a column or action before running:

```python
print(analysis.explain())
print(analysis.explain("jet_ht", variation="jesUp"))
graph = analysis.describe().as_dict()["details"]["graph"]
```

The `rdf-authoring-graph-v1` records contain resolved dependencies, native
materializations, construction-time variation mappings, selection masks and
action booking stages. A focused explanation follows upstream recorded inputs.
It reports whether a definition uses another output, reuses its nominal output,
or explicitly disables propagation with `define_nominal`/`nominal=True`.
Late registrations are marked as absent at construction; inspecting the graph
does not repair or retroactively expand it. Register variations before consumers.

Stages count successful binding `filter()` calls. A histogram booked before a
later filter retains its earlier node and mask. Its booked `variation` also
stays fixed when requesting an explanation for another variation. `count()`
counts rows retained by the variation union; `sum()` applies the nominal mask;
histograms apply their booked variation and optional selection/region mask.
Region chains and weight component/normalization declarations are also available
in the description inventory. Native weight products expose both propagated
component inputs and explicit component replacements.

The graph is deliberately **partial**: initial configuration, service hooks,
plugin internals and other untracked native changes are not reconstructed.
An input branch may have no authoring record. These records describe construction,
not executed kernel counts or a full ROOT DAG. They add setup metadata, with no
Python dispatch, extra RDF actions or variation branches in the event loop.
Use `Analysis(config, record_graph=False)` to omit these construction records
when setup memory is more important than explanations. Inventory descriptions
remain available. The legacy native `rdfanalyzer.Analyzer` leaves recording off
unless `record_graph=True` is explicitly supplied.
An older native module still gives an inventory; rebuild it to enable tracked
explanations. The compiled-executable controller continues to treat its graph
as analysis-owned and opaque.

```python
from rdfanalysis import Analysis, AnalysisConfig

analysis = Analysis(AnalysisConfig(
    files=["input.root"],
    output="selected.root",
    threads=1,
    save_columns=["event", "leading_pt"],
    options={"skimSystematics": "true"},
))
analysis.define("leading_pt", "Jet_pt.empty() ? -1.f : Jet_pt[0]")
analysis.filter("leading_jet", "leading_pt > 30.f")
print(analysis.variation_labels)
analysis.run()
```

Expressions are C++ compiled by Cling. Their expression path adds no Python
event callback. Custom Python kernels use the Numba bridge described below;
ROOT's optional ctypes fallback needs separate inspection. Omitted dependencies are inferred from available column
identifiers, excluding strings, raw strings, comments and member names. An
explicit list has the same systematic behavior and must include every column
used by the expression. Use simple expressions or externally declared C++
functions; this identifier scanner is not a general C++ scope/type parser.
For nonidentifier branch names, define a suitable input alias in native config.

For custom algorithms, write Python/NumPy kernels and use Numba:

```python
import numpy as np

def jet_summary(pt):
    total = 0.0
    count = 0
    for value in pt:
        if value > 30:
            total += value
            count += 1
    return np.array([total, count], dtype=np.float64)

analysis.define_numba("jet_summary", jet_summary, ["Jet_pt"],
                      return_type="RVec<double>")
analysis.project("jet_ht", "jet_summary", 0)
```

Input types are inferred from RDF columns. RVec inputs appear as NumPy arrays.
`ROOT.Numba.Declare` compiles the kernel in nopython mode; users do not write
C++ wrappers or manage native addresses. Specify a return type for vector
results. `nominal=True` suppresses expansion for already-resolved outputs.
Registrations are cached by callable/signature in process; owners for every
signature stay alive for lazy execution. `compilation_stats` separates Numba
compile time and cache hits. Install optional dependencies from
`requirements-python.txt` with the Python matching ROOT.

Use `analysis.numba_bridges` to inspect each registered signature's wrapper.
For an explicit native-wrapper requirement, pass `require_native=True` to
`define_numba`, `define_numba_outputs`, or their stage counterparts. Unknown
wrappers and ROOT's ctypes fallback then fail before defining the RDF column.
The default stays permissive. The [bridge guide](NUMBA_BRIDGES.md) explains
cache/lifetime behavior, diagnostic limits and pending actual runtime acceptance.

Pass existing column names directly when no transformation is needed. In the
expression binding, `define_nominal("jets", "Jet_pt")` creates a by-value RVec
definition; it is not a zero-copy alias. A `CollectionRef` supplies column names
without creating this intermediate definition. The private HT10 alias control
will measure the effect separately; removing a copy is not yet a measured
end-to-end speedup.

`project` is a native checked RVec<float>/RVec<double> element selection and
propagates variations. It avoids compiling a new analysis kernel for a simple
projection. It is distinct from event-indexing an external array.

Conventional complete `variable_jesUp`/`variable_jesDown` input pairs are
discovered before propagating Python definitions. Other names can be registered:

```python
analysis.register_variation_pair("Jet_pt", "jes", "jets_high", "jets_low")
analysis.register_variation("Jet_pt", "replica_0", "jets_replica_0")
analysis.define("leading_pt", "Jet_pt.empty() ? -1.f : Jet_pt[0]")
```

Register variations before definitions which consume them. Derived definitions
propagate paired families and explicit labels through the same internal planner
used by C++ `Analyzer::Define`. `filter(name, expression)` uses the C++ cumulative
selection masks and retains an event if any variation passes the full selection.
`selection_masks` maps labels to mask-column names. Users booking raw RDF actions
must apply the corresponding mask; the ND histogram machinery does this itself.

Standard one-dimensional slices can also be booked directly:

```python
analysis.count("input")  # book before filters for a preselection denominator
analysis.histogram("jet_ht", "jet_ht", 40, 0, 1000)
analysis.run()
print(analysis.histograms["jet_ht"])  # edges, sumw and sumw2, including flows
print(analysis.counts)
```

These are lazy TH1D results, consumed within the native plugin lifecycle before
provenance finalization. By default they stay in memory. Set
`AnalysisConfig(persist_histograms=True, ...)` to also store them under
`python_histograms/<histogram name>` in the metadata ROOT file, together with
the canonical job provenance. Signed contents, flows, variances and axis edges
are preserved in the same event loop. Use the existing NDHistogramManager for
configured multidimensional outputs. `weight`, boolean-column `selection` and
exact `variation` keyword arguments select each slice; the frontend applies
that variation's cumulative acceptance mask. Results are available after
`run()`. Scalar float/double histograms avoid inferred-type action JIT.

Persistent names are relative ROOT paths. Use `Nominal/ht`, `jesUp/ht`, etc.;
a histogram named `ht` cannot also be a directory containing `ht/jesUp`.
Empty/dot path components and ROOT cycle delimiters are rejected. A successful
rerun replaces the owned `python_histograms` namespace so removed slices do not
survive as stale output. Counters, ND outputs and canonical provenance keep
their existing ownership.

`define_nominal` explicitly suppresses expansion for terminal, already-resolved
quantities. Duplicate Python output names raise `ValueError`; definitions no
longer silently skip existing columns. `Define`, `DefineJIT`, `Filter`,
`FilterJIT`, and `save` remain available on the native module alongside
`define`, `filter`, and `run` aliases. Public C++ signatures are unchanged.

`AnalysisConfig` validates required fields, booleans, threads, save columns,
unknown YAML keys and attempts to override typed fields through `options`.
`options` is an explicit escape hatch for existing native string configuration;
it does not yet give every plugin a typed schema. A generated private config is
retained until execution completes. Explicit calibration dependencies should
still be frozen using [run bundles](RUN_BUNDLES.md) for production.

```yaml
files: [input.root]
output: selected.root
tree: Events
threads: 1
systematics: true
skim: true
save_columns: [event, leading_pt]
persist_histograms: true
options:
  skimSystematics: "true"
```

Load this with `AnalysisConfig.from_file("analysis.yaml")`. Input/output paths
in this format resolve relative to its YAML file; URLs are preserved. Native
paths inside `options` retain native resolution rules. Pass an existing native worker YAML directly to `rdfanalyzer.Analyzer("cfg/worker.yaml")` to retain its relative auxiliary paths and provenance. [Shared configuration examples](YAML_CONFIG_SUPPORT.md)

Validate typed settings without ROOT
or input-file access:

```bash
PYTHONPATH=core/python .venv/bin/python -m rdfanalysis validate analysis.yaml
```

The [Python installation guide](PYTHON_INSTALLATION.md) covers a local installed
prefix and the real ROOT/Numba execution probe. Native authoring API versions
are checked before opening inputs, catching a newer frontend paired with an old
native module. [CMS authoring](https://gitlab.cern.ch/bkronhei/RDFAnalyzerCMS/-/blob/505e81b28e5ec39bf6761c7a151e6d18dd98850f/docs/PYTHON_AUTHORING.md) wraps
the existing CMS correction recipes with typed settings.
Low-level pointer APIs retain their lifetime/type requirements in
[Python bindings](PYTHON_BINDINGS.md). NumPy pointer-vector definitions describe
a vector supplied to each event, not an array automatically indexed by event.

Verification: `PythonAuthoringTest`, `PythonBindingsIntegrationTest`,
`SystematicPropagationTest` and `SystematicBundleTest`. The tests inspect actual
skim values/masks for inferred and explicit dependencies, arbitrary variation
names, chained replicas, nominal-only definitions and disabled systematics.

Lazy scalar reductions can be booked with `analysis.sum("total", "double_column")`;
read `analysis.sums` after `run()`. The column must be a scalar double. Sums use
the current graph node and nominal acceptance, with no automatic systematic
expansion. Book before running; all sums, counts and histograms share the lazy
event loop, including jobs without a skim.

## Independent jobs in one worker

`AnalysisSession` reuses process-local Numba registrations across independently
constructed graphs. Each job has its own native Analyzer, outputs and provenance;
returned `AnalysisResult` objects contain detached Python data, not live graphs.
A graph builder runs once during setup, never as a per-event Python callback.

```python
from rdfanalysis import AnalysisConfig, AnalysisSession

def build(analysis):
    analysis.count("input")
    analysis.define_numba("observable", my_python_kernel, ["input_column"],
                          return_type="double", nominal=True)
    analysis.histogram("observable", "observable", 50, 0, 100)

with AnalysisSession(max_jobs=8, max_numba_signatures=64) as worker:
    for index, files in enumerate(disjoint_input_partitions):
        config = AnalysisConfig(files=files, output=f"part{index}.root", skim=False)
        result = worker.run(config, build)
        print(result.counts, result.compilation, result.timing)
```

The default metadata path is now the output's stem plus `.metadata.root`, even
when no skim is written. An explicit `options["metaFile"]` overrides it. Sessions
reject reused skim/metadata destinations. `result.output` is `None` without a
skim; `result.metadata` identifies its provenance output. `as_dict()` returns a
serializable copy. `analysis.inspect()` reports columns, variation labels,
selection masks and compilation statistics without running the event loop.

The job limit and process-wide attempted-signature limit raise
`WorkerRecycleRequired`. Start a fresh worker process at that point. ROOT cannot
safely unload generated JIT code; clearing a Python dictionary would not bound
that native memory. Failed compilation attempts count toward the signature
budget. Existing signatures can still be reused at that limit. Closing a session
prevents new jobs but does not claim to unload native code or clear ROOT state.

Construct graphs serially; RDF event execution may use configured threads.
Reuse the same imported kernel functions to get cache hits. Code, defaults,
closures, referenced numeric globals/NumPy arrays and helper functions contribute
to the registration fingerprint. Mutating a captured parameter array therefore
recompiles the kernel; an unchanged subsequent job reuses it. Native input/return
types also contribute. Opaque captured objects require an explicit
`cache_version="calibration-v2"` argument to `define_numba`; update that token
whenever their compiled behavior changes. This fingerprint is not universal
introspection of arbitrary Python state. Pass a raw Python function or a Numba
dispatcher's `.py_func`, since ROOT's bridge performs its own compilation.
This is an
in-process cache, not disk caching or cross-process callback-address reuse.
Models and correction payloads are still owned by each job's native managers;
this API does not claim to share those resources yet. Live provenance discovery
remains the default unless the caller explicitly supplies a frozen snapshot.

Disjoint range workers can use typed half-open bounds:
`AnalysisConfig(..., entry_range=(500000,1000000), range_mode="source")`.
Both range modes require one thread. `filter` is the default, with unsigned32-bit
bounds; experimental `source` accepts signed64-bit bounds and currently requires
ROOT 6.40.x or 6.42.00-rc1. Invalid bounds/types and conflicting native options fail before
opening inputs. The native version guard remains authoritative for source mode.
Existing `options` range keys remain usable when no typed range is supplied.

For jobs staged from one fixed analysis revision, the caller can collect Git
context once and supply `metadataAnalysisGitHash` and `metadataAnalysisGitDirty`
in `options`. Refresh that pair whenever the source changes; preserve artifact
hashes as well. The framework's default still discovers the current working
directory live. See [metadata snapshots](METADATA.md#explicit-analysis-git-snapshots).

The native event loop releases the Python GIL, allowing scheduler heartbeats and
background Python threads to progress. Build graphs serially and avoid concurrent
access to one Analysis. Execution/reentry and primary mutation guards reject
unsafe lifecycle transitions; this is not a general concurrent graph API.

## Native weights and region hierarchies

```python
weights = analysis.weights(
    {"generator": "gen_weight", "lepton": "lepton_sf"},
    normalizations={"lumi_xsec_over_sumw": 0.25},
    variations={"leptonSF": ("lepton", "lepton_sf_up", "lepton_sf_down")},
    output="event_weight",
)
# Define every region boolean before the first region declaration.
analysis.define("pass_base", "leading_pt > 30.")
analysis.define("pass_signal", "leading_pt > 100.")
analysis.region("base", "pass_base")
analysis.region("signal", "pass_signal", parent="base")
for label in analysis.variation_labels:
    analysis.histogram(label + "/leading_pt", "leading_pt", 50, 0, 500,
                       weight=weights["Nominal"], region="signal", variation=label)
```

`weights` uses the existing native WeightManager for products, normalization,
component replacements and audits. Each nuisance substitutes one component
rather than multiplying an up/down ratio; zero nominal factors and signed weights
are valid. Input factors must be scalar float/double columns. Conversion from
other input types can be an explicit definition. Normalization is caller-owned;
raw event counts and signed generator sums are different denominators.

Already-declared object variations on factor columns propagate into the final
weight. A nuisance that both shifts object-dependent factors and explicitly
replaces a weight component must supply a combined final weight explicitly;
the helper rejects that label overlap before modifying the graph. It does not
silently double count or assume the two effects are independent. Weight-only
labels do not reexecute unrelated features. Declare variations before consumers.
Native weight audits cover the manager's nominal and component-replacement
outputs at its execution graph; automatically propagated shape products remain
in the systematic mapping but are not additional WeightManager audit records.

`region` registers the existing native RegionManager hierarchy and a
variation-aware boolean chain for standard histogram booking. Parent regions
must precede children; all boolean inputs must precede the first declaration
because the native manager captures a base node then. Overlapping root regions
are permitted. This is not an exclusive-category assumption or a sequential
weighted cutflow. Use `region=` or `selection=` for one histogram, not both.
`inspect()` reports declared weights, output mappings and region chains without
executing events. Canonical metadata retains native region and weight audits.
The native weight/region hooks were introduced in authoring API3. The current
frontend requires API4, which also guards source-range capability: an older
module must not silently truncate a requested64-bit range.

Declare one event-weight product per Analysis in this convenience interface;
include all components/nuisances in that call. The low-level native APIs remain
available for specialized products. Combined normalizations must be finite.

Explicit [variable-width histograms](VARIABLE_HISTOGRAMS.md) support immutable
`VariableAxis` declarations, immediate/staged booking and mixed opt-in batches.
They preserve ROOT edge/flow semantics and loaded-module capability checks.
The candidate is built/tested separately; the shared module remains unchanged.

The [compatibility and migration table](AUTHORING_COMPATIBILITY.md) distinguishes
base API4 support, optional loaded-module features, stdlib controller operations
and candidate acceptance without changing existing authoring defaults.

For native category/systematic packing, see [packed ND histogram declarations](PACKED_ND_HISTOGRAMS.md). These retain native accumulation and automatic canonical-manager persistence, with ND metadata separate from ordinary TH1 results.

[Runtime threading and stable identity](PYTHON_THREADING_AND_IDENTITY.md)
separates requested threads from ROOT pool/dataframe capacity and explains stable
source identifiers under IMT. The new observation hook's installed scope is
tracked explicitly; older modules return unknown observations without guessing.


### Discover plugin types

`rdfanalyzer.core_plugin_types()` returns the manager names and aliases accepted by
`analysis.native.AddPlugin(role, type_name)`. Names and aliases are case-insensitive.
Extension-specific managers are registered by the enabled extension. Inspect
`rdfanalyzer.enabled_extensions` to see which extensions the binding includes.
