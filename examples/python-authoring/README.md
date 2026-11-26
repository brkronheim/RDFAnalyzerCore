# Small Python/Numba analysis

The installed `analysis.py` example describes settings by default. It opens no
input, constructs no native analysis and imports no ROOT, NumPy or Numba until
explicit execution or data creation is requested. The custom event algorithms
are Python functions compiled through `Analysis.define_numba`; there are no
user-written C++ helper functions or raw function pointers.

Install the full Python component in the matching ROOT/Python environment, as
described in `docs/PYTHON_INSTALLATION.md` in the checkout. The controller-only
component is for a separately compiled executable and does not install native
Python authoring. After installation:

```bash
export PYTHONPATH="/path/to/prefix/python${PYTHONPATH:+:$PYTHONPATH}"
python /path/to/prefix/share/rdfanalyzer/examples/python-authoring/analysis.py
python /path/to/prefix/share/rdfanalyzer/examples/python-authoring/analysis.py --mode schema
```

Add the prefix's `python` directory to your IDE's interpreter/search paths for
adjacent authoring, settings, staged-declaration, collection and result `.pyi`
signatures. These files have no runtime dispatch or event-loop cost. Native
expert access through `analysis.native` stays deliberately open (`Any`), and
static signatures do not prove column existence, array alignment or Numba support.
Provider/native plugin typing remains partial.

The following explicit commands create a new three-event file and run it. They
require Awkward, NumPy, uproot and the ROOT/Numba authoring installation. Use new
input/output paths; the example refuses to overwrite tutorial input or metadata.
Installed native acceptance has exercised these commands on fresh paths:

```bash
python /path/to/prefix/share/rdfanalyzer/examples/python-authoring/analysis.py \
  --mode make-input --input /tmp/new-tutorial.root
python /path/to/prefix/share/rdfanalyzer/examples/python-authoring/analysis.py \
  --mode run --input /tmp/new-tutorial.root --output /tmp/new-tutorial-output.root
```

The input pT arrays are `[50, 40]`, `[10]`, `[]`; their eta arrays are
`[0.2, -0.2]`, `[0.1]`, `[]`. Jet HT sums pT strictly above30 and |eta| strictly
below2, accumulating in double precision. The graph books an input count,
defines a Numba nonempty flag, filters, defines HT, filters HT>60, then defines
twice HT and books selected count, HT sum and two histograms. Exact definition/
filter order is preserved. Basic filter predicates use the existing native
expression API; the event algorithms are the Python kernels in this file.

Expected:3 input,1 selected, total HT90. The HT and double-HT histograms contain
one entry at90 and180 respectively, unit variance, no flows. They have10 bins
over0–200. No skim is produced; histograms persist in
`/tmp/new-tutorial-output.metadata.root`. Results print the common detached
envelope. Use a missing input to see an actionable error before native import.

## Collection requirements without ROOT

[collection_schema.py](collection_schema.py) runs with only the installed Python
helpers. It demonstrates `FieldMetadata`, explicit `schema_requirements()` and
the existing supplied-schema checker. It opens no event data or native bindings:

```bash
PYTHONPATH=/your/authoring-prefix/python python3 -S \
  examples/python-authoring/collection_schema.py
```

`--schema columns.json` explicitly reads a name-to-type JSON mapping. Missing
columns or exact reader-type mismatches produce contextual issues and exit1;
the default supplied example passes. Type spellings stay exact, and unit labels
perform no conversion. See [collection semantics](../../docs/NAMED_OUTPUTS_AND_COLLECTIONS.md).
