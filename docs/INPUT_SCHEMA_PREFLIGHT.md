# Check input schemas before a long run

`rdfschema` provides opt-in, per-file schema checks for Python/Numba analyses and
Python orchestration of compiled C++ executables. It is installed with both the
`Python` and language-free `PythonController` components. Importing it and
comparing supplied metadata use only the standard library. Reading ROOT metadata
explicitly requires a matching PyROOT installation; the compiled analysis itself
can still run without Python bindings or a Python event loop.

```python
from rdfschema import ColumnRequirement, check_input_schemas

required = [
    ColumnRequirement("event", "ULong64_t", "analysis.event_id"),
    ColumnRequirement("Jet_pt"),  # Presence only; no implicit type conversion.
]
# Supply the existing file list, e.g. dataset.files; no new dataset format.
check = check_input_schemas(files, required, tree="Events")
print(check.as_dict())
check.raise_if_invalid()
# Construct Analysis or launch the compiled executable only after this check.
```

For the Python authoring facade, the equivalent explicit adapter is:

```python
config.check_schema(required).raise_if_invalid()
analysis = Analysis(config)
```

Ordinary `config.validate()`, `config.describe()`, imports and analysis
construction do not request this check. There is no added event-time dispatch,
extra physics pass or implicit check repeated by each worker. In a partitioned
campaign the caller chooses where and how often to preflight.

## Coverage and type policy

Every supplied path or URL is checked separately, in order, including duplicates.
Missing/unreadable files, missing trees, missing columns and mismatched types are
reported with file index, source, tree and requirement location. A failure does
not hide diagnostics from later files. Empty input lists are rejected. A passing
result requires a metadata observation for every supplied file; checking only the
first file cannot certify a mixed chain.

Types compare **exactly**. ROOT typedefs, `RVec<float>` and
`ROOT::VecOps::RVec<float>` are not normalized or considered interchangeable.
Use a presence-only requirement to inspect the reader's actual spelling before
choosing an exact type. An unknown observed type fails a typed requirement.
Unrequested extra columns are allowed. Snapshots are detached, immutable metadata;
`as_dict()` returns fresh JSON-compatible data.

The ROOT reader opens one `TFile` at a time, gets the selected `TTree`, constructs
a non-executed dataframe, and queries requested column types. It books no actions
and checks `GetNRuns()==0`. It closes each input on success or failure, and does
not change implicit multithreading. It may perform metadata I/O; zero RDF event
loops is not a promise of zero network traffic or zero initialization cost.

This checks **original input columns**. Configured aliases, optional-column
fallbacks, friend trees, corrected objects and derived definitions are owned by
their existing providers. List only requirements that must physically be present.
Schema success is a point-in-time metadata check; it does not establish basket
readability, event alignment, payload validity, graph compilation, complete model
dependencies or physics correctness. Files may change after observation.

Explicit remote URLs use ROOT's configured I/O policy. This helper supplies no
independent opening deadline. Use the existing process controller with a timeout
when remote preflight needs a hard wall-clock bound. There is no discovery,
wildcard expansion, retry policy, hash-based schema cache or production submission.

## Supplied metadata and custom readers

Offline checks and custom storage adapters need no ROOT:

```python
from rdfschema import InputSchema, compare_schema, check_input_schemas

report = compare_schema(required, {"event": "ULong64_t", "Jet_pt": "RVec<float>"})
report.raise_if_invalid()

def reader(source, tree, names):
    # Fetch header metadata with the analysis's chosen storage library.
    columns = metadata_by_source[source]
    return InputSchema(source, tree, {name: columns[name] for name in names if name in columns})

check_input_schemas(files, required, reader=reader).raise_if_invalid()
```

A reader returns metadata for exactly the requested source/tree. Acquisition
failures raise `OSError` and are aggregated. Incorrect return values, mismatched
source identities and other programming exceptions propagate; a broken reader
is not silently converted into a successful or partial check.

The built-in ROOT reader verifies zero RDF event loops. A custom reader owns its
acquisition behavior; the generic checker books no actions and cannot certify
what arbitrary user reader code executes.

`compare_schema()` shares `rdf-validation-v1` diagnostics and exact type semantics
with the C++ [`NativePreflight.h`](NATIVE_SETTINGS_AND_VALIDATION.md) helper.
The Python acquisition adapter is additional; it does not change the native
Analyzer or add a Python dependency to native executables.

## Command-line use

Save a JSON requirement list, for example:

```json
[
  {"name": "event", "type": "ULong64_t", "location": "analysis.event_id"},
  {"name": "Jet_pt"}
]
```

```bash
python -m rdfschema requirements.json input-one.root input-two.root --tree Events > schema-check.json
```

Exit status is 0 for passing checks, 1 for acquired-input/schema failures, and 2
for invalid setup or unavailable reader dependencies. Result JSON contains
`rdf-input-schema-check-v1` coverage, snapshots and a nested validation report;
setup errors print a standalone validation report. JSON redirection is explicitly
caller-owned. This CLI does not start an analysis or alter any dataset manifest.
