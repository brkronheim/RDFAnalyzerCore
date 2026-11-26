# Python authoring signatures and small tutorial

The Python installation includes adjacent `.pyi` signatures for `rdfanalysis`,
`rdfsettings`, `rdfcolumns`, `rdfdeclarations`, `rdfworkflow` and `rdfresults`.
It also includes `rdfmodules` and `rdfvalidation` signatures. The optional CMS
package owns its `CmsCorrections` signatures and `py.typed` marker; core-only
installation does not discover or require CMS.
Add the installed prefix's `python` directory to your IDE's interpreter/search
paths. The signatures describe existing runtime APIs and are not imported during
analysis execution. They add no wrappers, casts or event callbacks.

This slice has lightweight structural checks and full installed native acceptance.
Both tutorials run successfully with actual Numba/native execution, persisted
histogram checks and one event loop. Strict typed client checks and separately
checked examples pass pinned mypy1.18.2; optional imports/dynamic native escape
hatches retain their documented limits. See the
acceptance evidence.
These are correctness/typing checks, not a performance promotion.

Six strict installed-interface clients and three basic installed tutorials also
pass pinned Pyright1.1.414;13 deliberate invalid calls are rejected at their
expected sites. See [Pyright acceptance and limits](PYRIGHT_ACCEPTANCE.md).
The [lightweight hosted workflow](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/.github/workflows/python-authoring.yml)
is now defined for pull requests/manual runs. Its exact nine-module command
passes locally from a frozen core-only source tree on Python3.12.11:57 tests,
two optional-CMS skips. A separate local checkout run passes all57, including
CMS. This is local verification of the job, not a hosted-CI result. Native
compilation, performance and third-party typing remain separate gates.

## What completion covers

- `AnalysisConfig`, typed input/skim settings and their descriptions/validation.
- Fluent Python/Numba definitions, projections, filters, counts, sums, histograms,
  weights, regions, variations, result access and reusable sessions.
- Named kernel outputs/fields, collection references, declaration stages and
  histogram groups. Frozen fields and normalized tuple/string attributes are
  reflected in the signatures; constructors accept their supported sequences.
- The separately compiled executable controller and detached result envelope.

The native escape hatch is `Any`: core/plugins can extend it without waiting
for a stub for every C++ method. Provider configuration/module signatures are
supplied; native-binding typing remains partial. CMS signatures stay in its
optional extension.
Column names and return-type strings remain explicit strings; static signatures
cannot establish that a branch exists, that jagged arrays align, or that a
Python function is supported by Numba. Runtime checks retain their authority.
Signatures introduce no new numeric conversions or mixed-result bridge.

## Start with the installed example

After the [full native Python installation](PYTHON_INSTALLATION.md), use:

```bash
python /path/to/prefix/share/rdfanalyzer/examples/python-authoring/analysis.py
python /path/to/prefix/share/rdfanalyzer/examples/python-authoring/analysis.py --mode schema
```

Both modes work without input data or importing the native/numeric stack. They
describe configuration, not a constructed RDF graph. The
[example guide](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/python-authoring/README.md) gives explicit data
creation/execution commands and expected results. Those installed execution
checks now pass, including native histogram persistence and one traversal.

The custom kernels are ordinary Python functions using scalar operations and
array indexing; `define_numba` compiles them. The tutorial filters nonempty
collections before defining HT, filters HT>60 before the next definition, then
books two histograms. Definition/filter order remains exact. Counts on opposite
sides of the cuts therefore retain different meanings. Basic predicates use the
existing native expression API; no custom C++ helper is required.

The three-event fixture expects3 input,1 selected, HT sum90 and one entry each
at90/180 in the HT/double-HT histograms, including unit variances and empty flows.
A missing-input invocation reports how to create a file before importing native
code. Input creation and metadata output use fresh paths.

## Existing workflows remain available

| Workflow | Current return contract | Additive convenience |
|---|---|---|
| Immediate Python analysis | `Analysis.run()` returns the Analysis | Explicit detached `result()` after success |
| Reusable Python session | `AnalysisSession.run()` returns `AnalysisResult` | `to_run_result()` |
| Interleaved stage recipe | `apply()` returns the Analysis after ordered lowering | Metadata-only preview/IDE signatures |
| Compiled C++ controller | `run()` returns `CommandResult` | Common result, optional build identity and controller-only installation |
| Expert C++/native plugins | Existing native APIs and lifecycle | Optional Python/provider descriptions; native access remains open |

The [compiled controller](COMPILED_CPP_WORKFLOWS.md) remains independent of the
native Python frontend. Neither execution mode replaces the other. Session
Numba reuse/budgets, explicit variation semantics, typed settings and legacy
advanced options retain their existing behavior.

## Checks and remaining work

The [synthetic systematics/inference tutorial](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/examples/systematics-inference/README.md)
adds explicit shape and weight slices, signed weights and the native BDT manager
inside the event graph. Its mathematical oracle and recorded graph setup pass
lightweight tests. Native BDT/Numba output, signed-bin variances, varied acceptance,
persistence and single-loop checks now pass as well. No trained CMS model or
evaluator call-count measurement is implied by the toy example.

Structural tests compare stub public classes/functions, dataclass fields,
constructor defaults, variadic and keyword-only parameters and property/class
methods with runtime. Installed tests use the same shared frontend installation
rules with a language-free CMake consumer, from outside the checkout. They check
descriptions, kernel calculations as ordinary Python, missing-input diagnostics
and a recorded builder's exact declaration order; they do not execute Numba/RDF.

Full framework installation, actual Numba compilation, one-loop histogram
persistence and authored/provider/CMS mypy client checks now pass. Complete
native/Numba binding typing, other checkers, calibrated/trained-model tutorials
and hosted CI remain separate acceptance gates.
