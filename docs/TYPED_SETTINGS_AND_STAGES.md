# Typed settings and explicit declaration stages

Extension-owned settings can use the shared [provider validation and schema
help](PROVIDER_VALIDATION.md), including explicit scoped CMS asset checks and a
native correction recipe/module adapter.

[Named Numba outputs and collection references](NAMED_OUTPUTS_AND_COLLECTIONS.md)
also work in these recipes, with projections remaining in declaration order.

[Optional modules](MODULES_AND_PLUGINS.md) can compose these stages with declared
inputs/outputs, dependencies and exclusive resources, in supplied order.

These are optional additions to [Python authoring](PYTHON_AUTHORING.md).
Immediate methods, native plugins and the
[compiled-C++ controller](COMPILED_CPP_WORKFLOWS.md) remain available. Nothing
in this layer introduces a Python event callback or changes a public C++ API.

An explicit scalar-bool filter can also be interleaved in a stage:
`Stage('jets').define('accepted', 'pt>30.', ['pt']).filter_column('cut', 'accepted')`
followed by further definitions or expression/column filters. The predicate must
exist at that declaration position; it is never moved ahead to satisfy a later
definition. This requires the optional bool-column native capability.

`stage.histograms(group)` now selects native batching automatically for an
expanded group with multiple histograms when the loaded module supports its
axis kinds. Regular groups require `book_histograms`; groups containing variable
edges require `book_histograms_mixed`. Older modules and singleton groups keep
ordinary native booking. `preview()` and successful stage descriptions show the
chosen action. Expansion budgets, names, counts, source diagnostics and each
group's action position stay explicit; no cache crosses groups or filters.

The existing `batch=True` explicitly requires batch support, while `batch=False`
retains ordinary booking for compatibility/diagnostic isolation. The default
`None` chooses by capability, without introducing another performance switch.
Direct `Analysis.histogram()` and `histogram_variable()` still book immediately.

## Common input and skim settings

```python
from rdfanalysis import AnalysisConfig, InputSettings, SkimSettings

config = AnalysisConfig(
    files=["input.root"], output="selected.root",
    input_settings=InputSettings(
        float_constants="cuts.txt",
        aliases="aliases.txt",
        cache_statistics=False,
    ),
    skim_settings=SkimSettings(
        tree="Selected", metadata_file="histograms.root",
        systematics=True, compression_algorithm="zstd", compression_level=7,
    ),
)
print(config.validate("assets").as_dict())
print(config.describe().explain())
```

Input settings cover existing cache enable/size/statistics/verbosity choices and
native floating/integer constants, alias, optional-branch and friend config files.
Skim settings cover output tree/metadata paths, skim systematic columns and skim
compression. These are the existing native settings, with type/range checks.
Skim compression does not configure histogram compression. Skim systematic output
is separate from defining variations and choosing histogram slices.

Every optional setting defaults to `None`, which omits the native key. It retains
native defaults or an explicit legacy `options` entry. There is no new default
I/O policy, precision choice or histogram backend. For an explicit cache size,
also use `cache_enabled=True`; native code otherwise ignores the size. A typed
size conflicts with the legacy factor-based cache-size options as well.

The same definitions generate validation and JSON reference/help:

```bash
python -S -m rdfanalysis schema
python -m rdfanalysis validate analysis.yaml
python -m rdfanalysis validate analysis.yaml --level assets
python -m rdfanalysis explain analysis.yaml
```

The schema command requires only the standard library. YAML loading requires
PyYAML, and these configuration commands do not import ROOT, Numba, NumPy or CMS.
Default `validate` output remains the native settings mapping; explicit asset
validation returns the shared validation report. Native schema/compile validation
is still a separate, unimplemented configuration validation level.

Nested YAML mappings work without constructing Python settings objects:

```yaml
files: [input.root]
output: selected.root
input_settings:
  float_constants: cuts.txt
  aliases: aliases.txt
skim_settings:
  tree: Selected
  metadata_file: histograms.root
  compression_algorithm: zstd
  compression_level: 7
```

Typed paths in YAML are anchored to the YAML file's directory. Paths supplied
to Python constructors are anchored to the current directory. This keeps them
valid when the frontend writes its private native configuration elsewhere.
Input and skim tree names can differ. Describe reports the resolved native
mapping and whether each emitted setting came from the frontend, a typed group
or a legacy option; omitted native defaults are not claimed to be resolved.

Unknown typed keys, bad values and typed/legacy conflicts fail early and YAML
errors include the source file. `options` remains an explicit string mapping for
advanced and extension settings; its values, path interpretation and payload
grammar are opaque. It may use an optional typed key when that typed value is
unset. Core does not whitelist CMS or third-party recipes; existing extension
facades/provider schemas keep ownership of those settings.

Asset validation checks only the existence of explicitly supplied local input
configuration files. Analysis construction performs that check before native
imports. It does not scan input ROOT files, contact remote URLs, parse every
native text grammar or recursively validate payloads/friend trees. Legacy config
paths are unchanged and are not included in this typed asset check. Freezing and
staging those dependencies still uses the existing bundle/production contracts.

## Explicit declaration stages

```python
from rdfanalysis import Analysis, Stage, HistogramGroup, Observable, RegularAxis

analysis = Analysis(config)
analysis.apply(Stage("input").count("input"))

def ht(pt, jet_pt_min):
    total = 0.0
    for value in pt:
        if value > jet_pt_min:
            total += value
    return total

selected = Stage("selected", max_histograms=100)
selected.define_numba("ht", ht, ["Jet_pt", "jet_pt_min"], return_type="double")
selected.define("base_pass", "ht > base_ht_min")
selected.define("signal_pass", "ht > signal_ht_min")
selected.filter("quality", "base_pass")
selected.region("signal", "signal_pass", parent="base")
selected.region("base", "base_pass")
selected.count("selected")
selected.histograms(HistogramGroup(
    "kinematics", (Observable("ht", "ht", RegularAxis(40, 0, 1000)),),
    regions=("base", "signal"), weight="event_weight",
    variations=("Nominal",), families=("jes",),
))

# The input config must supply the named cuts; define/register event_weight and
# the jes family before this stage. Preview reports the proposed action expansion.
print(selected.preview(analysis).explain())
analysis.apply(selected)
print(analysis.explain("ht", variation="jesUp"))
analysis.run()
```

A recipe has four boundaries:

1. Existing variation registration methods.
2. Freely interleaved `define`, `define_nominal`, `define_numba`, `project` and
   global `filter` calls, **in exactly their declaration order**.
3. Regions, with parents ordered before children and all predicates already defined.
4. `count`, `sum`, individual `histogram` calls and `histograms(group)` expansion.

For example, this keeps each later computation downstream of the preceding cut:

```python
stage = Stage("selection")
stage.define("loose", "Jet_pt.size() >= 4")
stage.filter("four_jets", "loose")
stage.define_numba("summary", jet_summary, ["Jet_pt"], return_type="RVec<double>")
stage.project("ht", "summary", 0)
stage.filter("high_ht", "ht > ht_min")
stage.define("fit_input", "ht * scale")
```

Calling an earlier phase after a later phase raises an error; declarations are
not silently moved across filters. Use another stage when a count/histogram
should capture a different node. Individual histograms retain the immediate
API's output names and explicit `selection`/`region`/`variation` arguments.
`sum` stays nominal-selection-only; `count` stays a variation-union count.

Recipes are reusable on fresh analyses and are sealed against adding methods
when lowering begins. Stage names must be unique within one analysis. Custom
algorithms still use the existing NumPy/Numba compiler and cache. Lowering calls
the same native-backed methods as immediate authoring; it adds no automatic
filters, kernels, actions or nuisance combinations. Stage summaries are available
in the existing inspect/describe inventory.

Planning uses native point queries for individual column-existence checks when
available. `Analysis.has_column(name)` also exposes this check without exporting
every column. Older native modules retain a whole-inventory fallback. These are
setup operations; no column lookup or planning runs inside an event kernel.

For a structured preflight report, call `stage.validate(analysis)`. It checks the
same structural plan as preview without lowering, sealing the recipe or running
events. `strict=True` additionally requires requested variation labels to exist
already; it can catch unknown histogram labels before earlier definitions are
lowered. The returned common `ValidationReport` has level `declarations`:

```python
selected.validate(analysis, strict=True).raise_if_invalid()
analysis.apply(selected)
```

This level does not compile expressions/Numba kernels, infer opaque plugin
dependencies, inspect event-level array alignment or validate native types.
A valid report is a structural planning result, not a compiled-graph guarantee.
Native-discovered labels can still change final expansion during lowering.

To trace lowering failures back to their declaration, construct
`Stage('selection', record_sources=True)`. Each step records its Python filename,
line and function, including groups that later expand into many histograms.
Descriptions expose these values, successful stage summaries retain them, and
lowering errors append the declaration location while preserving the original
exception as their cause. Only primitive values are retained: no calling frame,
locals, source-file contents or analysis object is captured. Locations identify
the call to the stage method, which may be an analysis helper's call site.
Structural preflight errors retain their existing stage-level diagnostics;
step locations remain available in the recipe description. Recording is disabled
by default and introduces no event callbacks or native graph changes.

The native RegionManager captures its base node at its first declaration.
Define **all** region predicates before that declaration, including predicates
for regions you intend to add later. A stage diagnoses incompatible later inputs
instead of pretending the native base can be moved. Native plugin changes outside
the frontend remain opaque; failures there may still be detected by native code.

## Histogram expansion and failure handling

A group produces names of the form
`group/region/variation/observable`; no region uses `inclusive`. Its default is
one nominal slice. Explicit `variations=(...)` selects labels, `families=(...)`
selects registered Up/Down labels, and `variations="all"` selects the current
registered labels. Labels are deduplicated on one variation axis. There is no
implicit product such as JES-Up times weight-Up. Unknown labels fail.

`RegularAxis` validates uniform bins and finite increasing bounds; `VariableAxis`
validates and copies finite, strictly increasing double edges. Both are supported
by ordinary groups and individual staged histograms. `NDHistogramGroup` provides
the native fixed category/systematic/observable axes through `nd_histograms()`;
it is not an arbitrary-dimensional continuous histogram builder.
Histogram groups/individual staged histograms
use portable relative ROOT paths, and reject duplicate or file/directory
destinations. Default `max_histograms=1000` limits expansion within one stage;
set an explicit budget for larger declared inventories.

`preview()` compiles nothing and books nothing. Its expansion is provisional:
native expression construction may discover conventional paired input variations.
`apply()` resolves those labels and checks expansion again **before booking**.
Region-parent and structural failures detected in preflight leave the analysis
untouched. Native compilation/type/late-expansion failures after lowering starts
can leave a partial graph; the frontend blocks `run()` and reports the stage,
operation and original exception. Start a fresh analysis after correcting it.
Automatic rollback is not provided.

Variable-width observables use `VariableAxis(edges)` beside `RegularAxis` in
`HistogramGroup`. `Stage.histogram_variable` adds individual declarations, and
`Stage.histograms(group)` automatically batches eligible mixed-axis groups at
that stage; explicit booleans retain their established effect.
See [variable histogram semantics and compatibility](VARIABLE_HISTOGRAMS.md).

Packed ND groups can be booked with `Stage.nd_histograms(group)` at the same explicit action boundary. They share the stage object budget with ordinary histograms; definitions/filters remain interleaved before actions. See [packed ND declarations](PACKED_ND_HISTOGRAMS.md) for categories, native precision, all/nominal policy and persistence.
