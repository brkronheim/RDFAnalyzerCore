# Named kernel outputs and collection references

These optional Python authoring helpers resolve names during graph construction.
They use the existing native definitions and checked projections; they do not
add Python event callbacks, packing kernels, automatic projections or copies.
Raw column strings, `define_numba`, positional `project` and expert native APIs
remain available. Python orchestration of compiled/linked C++ analyses remains
supported independently through the [compiled controller](COMPILED_CPP_WORKFLOWS.md).

## Name an existing homogeneous vector layout

```python
import numpy as np
from rdfanalysis import KernelOutputs, Stage

def summarize(pt):
    return np.array([float(len(pt) >= 4), pt.sum()], dtype=np.float64)

summary = KernelOutputs("summary", ("accepted", "ht"))
selected = Stage("selected")
selected.define_numba_outputs(summary, summarize, ["Jet_pt"], return_type="RVec<double>")
selected.project("accepted", summary["accepted"])
selected.filter("four_jets", "accepted != 0.")
selected.project("ht", summary["ht"])
selected.histogram("ht", "ht", 25, 50, 550)
analysis.apply(selected)
```

The layout names vector positions, starting at zero. `summary["ht"]` is a
`KernelField` description, **not a dataframe column**. Explicit `project` creates
the column at that point in the graph. You can project fields in separate stages,
or omit unused fields entirely. Definitions and filters retain their interleaved
declaration order. Actions on opposite sides of a filter still need separate
stages, as described in the [stage guide](TYPED_SETTINGS_AND_STAGES.md).

`Analysis.define_numba_outputs` supports the same immediate pattern. The helper
passes the **original callable** to `define_numba`; field names do not create
wrapper functions or new Numba cache signatures. Its definition is identical to
`define_numba("summary", summarize, ["Jet_pt"], return_type="RVec<double>")`.
The expensive result remains one shared dataframe definition per entry/state,
with the same native reuse and lazy-consumption rules as positional projections.

The supported named return types are explicitly `RVec<float>` and `RVec<double>`.
Projections preserve float/double precision. There is no implicit conversion to
represent mixed bool/int/float records. For example, `sum()` requires a scalar
double: explicitly define `double(float_field)` if a float projection must be
summed. Other result types remain available through the existing expert APIs.

Names must be unique identifiers. Unknown field names fail at construction.
Vector lengths are checked **when each projected field is consumed**: empty or
short vectors raise the existing native out-of-range exception. Unconsumed names
do not assert an exact vector length or introduce hidden validation actions.
The kernel and its callers own empty-input behavior; return a documented sentinel
or filter empty collections explicitly when appropriate for the analysis.

Systematic propagation follows the existing input dependencies. Register
variations before consumers. For intentionally terminal outputs, declare
`KernelOutputs("weights", ("nominal", "up", "down"), nominal=True)`.
Both the kernel and its named projections then use nominal-only definitions.
An explicit projection policy conflicting with the field's policy is rejected.
Calling positional `project` remains an expert escape hatch.

`analysis.inspect()["kernel_outputs"]` and `describe()` expose detached layout
metadata with the return type; native graph descriptions show the same original
definition/projection operations. `Stage.describe()` works without ROOT, NumPy,
Numba or input opening. Defining a schema alone never compiles or runs anything.

## Refer to raw or corrected collection fields

```python
from rdfanalysis import CollectionRef

raw = CollectionRef("raw_jets", {"pt": "Jet_pt", "eta": "Jet_eta"},
                    alignment="input-jet-order")
shifted = CollectionRef("shifted_jets", {"pt": "corrected_pt", "eta": "Jet_eta"},
                        alignment="input-jet-order")
raw.require_aligned(shifted)
# Pass ordinary column strings to existing analysis methods.
analysis.define_numba("ht", my_ht, [shifted["pt"]], return_type="double")

selected = CollectionRef("selected_jets", {"pt": "selected_pt"},
                         alignment="selected-jet-order",
                         index_column="selected_original_index", index_target="raw_jets")
```

The field dictionary is frozen on construction; indexing returns the column
string. This works with immediate, staged and expert consumers accepting strings.
References do not create alias columns, copies, masks, corrected objects, sorting
or index gathers. No column existence or runtime type check occurs until an
existing graph consumer resolves the string.

Alignment is **user-declared metadata**, describing element order.
`require_aligned` succeeds only for equal, explicitly supplied alignment labels;
unknown or differing labels raise a construction error. It does not inspect
lengths or establish that the declaration is true. Empty and unequal-length
arrays retain the consuming kernel's existing behavior. In a multi-array kernel,
check lengths/index bounds explicitly if the source's contract does not ensure
them. `index_column` and `index_target` must be supplied together and document a
mapping into another collection's order. They do not automatically align fields:
sorting, filtering or joining different orders requires explicit analysis code.
`CollectionRef.describe()` returns detached JSON-compatible metadata.

Optional field annotations document expected native type spellings, units and
help text without changing the column strings passed to consumers:

```python
from rdfanalysis import CollectionRef, FieldMetadata

jets = CollectionRef(
    'jets', {'pt': 'corrected_pt', 'eta': 'Jet_eta'},
    alignment='input-jet-order',
    metadata={'pt': FieldMetadata(dtype='RVec<float>', unit='GeV',
                                  description='Corrected jet transverse momentum')},
)
print(jets.describe()['metadata'])
analysis.define_numba('ht', my_ht, [jets['pt']], return_type='double')
```

Annotations are optional and field names must exist in `fields`. The metadata
mapping and its values are immutable; descriptions return independent copies.
References without annotations keep their existing description shape. Unknown
field access reports the collection name and available fields.

`dtype` is an author-declared expectation, not a Numba signature or an observed
type. `unit` is a label, not a unit conversion. Neither annotation validates
runtime types/alignment, changes systematic dependencies, adds graph nodes, nor
registers metadata automatically with an Analysis. `require_aligned` continues
to compare the explicit ordering labels only. Use explicit schema checks for
physical input types and analysis code for unit conversions. The
installed metadata acceptance
checks immutability, unchanged consumer columns/plans, stdlib-only import and
strict editor signatures. There is no new throughput claim.

### Explicit schema requirements from collection fields

`CollectionRef.schema_requirements()` converts the declared fields into an
immutable tuple of `rdfschema.ColumnRequirement` values. It is an explicit
metadata operation: it imports the stdlib schema checker lazily and neither
opens inputs nor modifies an analysis graph.

```python
from rdfschema import compare_schema

# Type strings must match the spelling returned by the chosen schema reader.
inputs = CollectionRef(
    'input_jets', {'pt': 'Jet_pt', 'eta': 'Jet_eta'},
    metadata={'pt': FieldMetadata(dtype='ROOT::VecOps::RVec<float>', unit='GeV')},
)
requirements = inputs.schema_requirements()
report = compare_schema(requirements, {
    'Jet_pt': 'ROOT::VecOps::RVec<float>', 'Jet_eta': '',
})
report.raise_if_invalid()
```

Fields without a declared `dtype` require presence only; an unknown observed
type (`''`) therefore passes for `eta` here. Typed requirements compare strings
exactly: `RVec<float>` and `ROOT::VecOps::RVec<float>` are different expectations.
Missing columns, wrong types and unknown types on typed fields use the existing
schema diagnostics with the collection and logical field location.

Two logical fields may name one physical column. Requirements deduplicate that
column in first-field order and retain both logical locations in diagnostics.
A presence-only alias and a typed alias merge into one typed requirement.
Conflicting declared types raise `ValueError` when `schema_requirements()` is
called, identifying both typed fields and their expectations. Ordinary indexing
and descriptions do not request these checks automatically.

For physical inputs, pass the tuple to the existing explicit
`rdfschema.check_input_schemas(files, requirements, reader=...)` operation when
per-file acquisition is wanted. A collection containing derived columns should
instead be checked against metadata for the appropriate graph point; an input
reader cannot observe definitions that have not been created. The caller chooses
the source of observed schema metadata. This bridge does not infer types, inspect
array lengths, require vector types, follow `index_target`, request `index_column`,
verify alignment, convert units or register an automatic preflight. No new event
actions or native callbacks are introduced.

## Example and validation

```bash
python analyses/AGC/simple/named_analysis.py --config analysis.yaml --output result.json
```
