# Packed histogram declarations in Python

The optional `Analysis.nd_histograms()` interface books the existing native
`NDHistogramManager` at the current dataframe node. Each observable has the
five axes `[channel, control, sample, systematic, observable]`. This is category
packing, not an arbitrary-dimensional continuous histogram API.

```python
from rdfanalysis import (
    Analysis, AnalysisConfig, NDHistogramSettings, NDHistogramGroup,
    NDHistogramSpec, NDCategoryAxis, RegularAxis, VariableAxis, Stage,
)

settings = NDHistogramSettings(backend='boost', precision='float',
                               sparse_backend='map', dense_limit_mib=0)
analysis = Analysis(AnalysisConfig(files=inputs, output='analysis.root',
                                  skim=False, options=settings.native_options()))
analysis.native.AddPlugin('histogramManager', 'NDHistogramManager')

plots = NDHistogramGroup('dileptons', (
    NDHistogramSpec('jet_pt', 'jet_pt', VariableAxis([0, 30, 60, 120, 300]),
                    weight='event_weight', label='Jet pT'),
    NDHistogramSpec('jet_eta', 'jet_eta', RegularAxis(30, -3, 3),
                    weight='event_weight', force_sparse=True),
), channel=NDCategoryAxis('channel_code', RegularAxis(2, .5, 2.5), ('ee', 'mumu')))

# Define the input columns before this action. Booking processes no events.
analysis.nd_histograms(plots)
analysis.run()
```

Custom Python physics should continue to use Numba kernels; the histogram
interface performs setup and invokes compiled managers, with no Python event
callback. C++ analyses retain their direct manager/RNode interfaces, and Python
can continue to steer a separately compiled C++ executable through the
[compiled workflow](COMPILED_CPP_WORKFLOWS.md).

## Declarations and storage

Declarations are frozen snapshots. An observable requires an explicit existing
weight column; for unit weighting, define a unit column before booking. Category
axes consume existing numeric columns, have regular bins, and require one unique
label per bin. `None` uses native `zero__`/`Default`. Category axes do not refer to
named frontend regions and do not introduce a region/object Cartesian product.

Regular/variable observable axes, scalar and supported aligned collection
transport remain native. Observable **and category** bounds pass through native
float fields. Descriptions report effective rounded bounds; finite bounds that
overflow or collapse as floats are rejected. Variable edges retain double
precision. Flat ASCII identifiers form native histogram keys (`jet_pt_` with the
native empty suffix); group names do not implicitly prefix output names.

Storage settings serialize existing native options. Defaults stay Boost float,
sparse map, zero dense threshold. ROOT/Boost, float/double, available sparse
containers and dense thresholds remain explicit overrides. `force_sparse=True`
uses the native override; false permits the configured threshold. A logical
cell budget is not an allocation estimate: sparse grids may have many logical
cells but few occupied cells. Boost double with variable observable edges keeps
the existing ROOT fallback.

Use explicit analysis counters for event counts. A histogram can receive several
object/category/systematic fills per event. Existing Boost conversion also
sets `THnSparseF.GetEntries()` to the number of exported occupied cells, so that
value is currently unsuitable as a fill-count oracle. Signed contents and Sumw2
have independent checks. A private correction
passes direct-action ROOT oracles and all108private manager/configuration/
persistence tests. Matched sustained performance/memory acceptance remains
required before promotion. `analysis.count('input')` before cuts records input
events; a counter after a variation-aware filter counts its retained variation
union, as described in [the authoring guide](PYTHON_AUTHORING.md).

## Direct C++ booking and saving

Native `bookND` and configuration-driven booking retain the declarations used by
the manager's no-argument `saveHists()` during the Analyzer lifecycle. The raw
`BookSingleHistogram` method requires an explicit histogram/axis-label list when
saving. Keep `histInfo` in a named variable because that existing method accepts
a non-const reference:

{% raw %}
```cpp
histInfo info("jet_pt", "jet_pt", "Jet pT", "event_weight", 30, 0, 300);
manager.BookSingleHistogram(info);
// Finish all graph declarations before saving pending lazy results.
std::vector<std::vector<histInfo>> outputs{{info}};
std::vector<std::vector<std::string>> labels{
    {"Default"}, {"Default"}, {"Default"}, {"Nominal"}};
manager.saveHists(outputs, labels);
```
{% endraw %}

This example uses the default category axes and nominal systematic label. Supply
the actual channel/control/sample/systematic labels when booking other axes.
The same native path works in a standalone executable or one launched by the
Python compiled controller. These are existing manager contracts; no wrapper or
per-event dispatch is added.

## Systematics and ordering

`variations='all'` uses the native available systematic axis, subject to global
and histogram systematic enablement. `variations='nominal'` books only Nominal
for that group without disabling other groups. Selected label/family subsets
are unsupported by the existing manager interface and are rejected.

Register all intended nuisance labels before the first all-systematics ND
booking. The native counter axis becomes fixed then. Typed booking rejects a
changed registered label list, including a stale axis previously materialized
through raw booking. Raw plugin internals and automatic late discovery are not
fully described by the frontend recorder; explicit early registration remains
the supported contract. This is one systematic axis, without shape-by-weight
Cartesian expansion.

Each group is one `bookND` call. It retains that call's pack sharing and captures
the exact booking node. Definitions and filters may interleave before an action:

As an alternative to the immediate booking above, construct the stage before
calling `analysis.run()`:

```python
selected = NDHistogramGroup('selected_plots', (
    NDHistogramSpec('selected_jet_pt', 'selected_jet_pt', RegularAxis(30, 0, 300),
                    weight='event_weight'),
))
analysis.apply(Stage('selected')
    .define('pass_jets', 'n_jets >= 2', ['n_jets'])
    .filter_column('jets', 'pass_jets')
    .define('selected_jet_pt', 'leading_jet_pt', ['leading_jet_pt'])
    .nd_histograms(selected))
```

Use separate stages/groups for actions on opposite sides of a filter. Stage
preview checks declared input existence, destinations and the common histogram
budget without compiling/booking. It remains provisional for native-discovered
metadata. No sharing crosses later definitions, filters, Redefine or calls.

The default `collection_layout='fixed'` retains the existing transport. Do not use
it for changing object counts across systematic slots: the
direct diagnosis
reproduces misassignment when an early slot is empty and another contains objects.

The opt-in native candidate supports actual per-slot collection lengths:

```python
settings = NDHistogramSettings(collection_layout='per-slot')
analysis = Analysis(AnalysisConfig(files=inputs, output='analysis.root',
    skim=False, options=settings.native_options()))
analysis.native.AddPlugin('histogramManager', 'NDHistogramManager')
```

The observable must stay an `RVec` in every slot. Each weight/category input
must keep its scalar or collection kind across slots. Scalars broadcast within
their slot; collection lengths must match that slot's observable exactly,
including empty slots. A length-one array is not a scalar. All slots are
validated, including rejected slots. Missing variation mappings reuse the
nominal column; an object-valued nominal weight/category must therefore still
match the shifted observable length, or supply its own varied aligned column.
The path retains the native `Float_t` transport for numeric coordinates and
weights. `precision='double'` selects Boost accumulation precision; it does not
turn that transport into double-valued histogram inputs.

This path keeps the existing ROOT/Boost fill actions and output schema, passing
nonempty accepted slices through a private native adapter with no Python event
callbacks. It rejects implicit overlapping region/object expansion. Scalar
observable graphs retain their original path. Older native
modules lacking `packed-nd-per-slot` produce a rebuild-required error before
typed booking.

## Outputs, errors and expert APIs

The facade requires an explicitly registered `NDHistogramManager` in canonical
role `histogramManager`, which `Analysis.run()` saves automatically. It does not
register plugins, parse new files or invoke manual saves during booking. Missing
or wrong-role managers produce an actionable error. Expert alternative roles
and lifecycle ownership remain accessible through raw native methods.

`Analysis.histograms` retains ordinary TH1 payloads. ND contents are persisted
under `nd_histograms_v1`, with `__rdf_histogram_schema_v1` recording actual axes,
labels and edges. ND booking metadata appears separately in `inspect()`,
`describe()` and `result().details`. Detached `AnalysisSession` results retain
these declarations in `nd_histograms` without retaining a graph or reading sparse
contents. Reading contents/projections is explicit
through [schema-aware histogram I/O](CONFIG_HISTOGRAMS.md); the facade does not
copy entire sparse histograms into Python or create fake count actions.

Raw `bookNDHistograms()` bookings may coexist when tracked keys are unique.
Typed destinations reject existing raw/config keys, and subsequent raw bookings
reject collisions with the typed manager. Clearing a typed manager is rejected
to avoid stale metadata; start a fresh analysis. Untracked native booking state
is rejected at booking/run setup boundaries rather than guessed. Arbitrary
plugin execution internals remain outside the complete frontend recorder.
Structural declaration/input errors occur
before booking. Native failures can leave partial graph setup and prevent the
frontend from running; create a fresh Analysis. Ordinary TH1 and ND destinations
have separate output namespaces.

Older modules produce a rebuild-required error. The `packed-nd-histograms`
capability indicates native booking support.
