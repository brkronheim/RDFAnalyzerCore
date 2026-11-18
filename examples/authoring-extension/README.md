# External authoring provider example

This independently importable Python package supplies an optional HT module.
It is analysis policy rather than a core/CMS default. Its original Python kernel
is JIT-compiled by Numba; metadata imports need only the standard library.
No C++ physics wrapper or core plugin-dispatch change is required.

For a normal installed package:

```bash
python -m pip install ./examples/authoring-extension
```

Then construct a module through an ordinary import:

```python
from rdfexample_authoring import make_ht_module

ht_module = make_ht_module(input_column="Jet_pt", threshold_column="jet_pt_min",
                           output_column="jet_ht", name="example.ht")
print(ht_module.describe().explain())
analysis.use(ht_module)
```

The explicit threshold column can come from an input branch or an analysis-owned
native float-constants file (`jet_pt_min=30.0`), selected through
`InputSettings(float_constants="cuts.txt")`. The module does not choose a cut
threshold or cast/copy the input collection. It returns zero for an empty array.

For source-based explicit discovery without installation:

```bash
export RDF_EXTENSION_PATHS="$PWD/examples/authoring-extension"
python -S -c 'from rdfmodules import discover_module_specs; print(discover_module_specs()[0].describe().explain())'
```

The framework's `core/python` or installed `python` directory must already be on
PYTHONPATH. The existing extension loader locates the provider's `python` directory.
Set `RDF_ENABLE_EXAMPLE_AUTHORING=OFF` to disable the provider. Discovery does not
construct an Analysis or execute the builder. In a configured framework build,
the manifest's optional CMake hook registers an INTERFACE target through the
existing extension registry; the provider introduces no native runtime code.

See [module contracts and composition](../../docs/MODULES_AND_PLUGINS.md) for
dependency order, action/resource claims, settings validation, native managers,
failure semantics and compiled-C++ metadata.
