# Safe systematic definitions and efficient dependency graphs

Use this guide when adding explicit calibration uncertainties, debugging missing
variation effects, or reducing systematic graph work. These practices apply to
both compiled C++ and Python-authored analyses.

## Distinguish the three operations

| Operation | Behavior | Appropriate use |
| --- | --- | --- |
| `Define` / Python `define` | Defines a nominal computation and expands registered input variations | Derived observables that should follow varied objects |
| `DefineNominal` / Python `define_nominal` | Defines a single terminal computation without expansion | Explicit already-resolved variation functions or genuinely nominal-only helpers |
| Variation registration | Maps a logical quantity and label to a physical column | Connect an existing varied computation to downstream propagation |

A registered label does not prove its intended function was installed or that
histograms consume it. Resolve physical columns through the systematic manager;
do not assume `<quantity>_<label>` is always the mapping.

**Compatibility hazard:** legacy C++ `Define` and `DefineNominal` silently keep
an existing nominal column; Python authoring rejects duplicate nominal names.
Automatic propagation also reuses existing variation columns. Consequently,
`DefineNominal("met_ResponseUp", calibratedUp, ...)` can silently keep an earlier
automatic `met_ResponseUp` with a different computation. Current registration
methods can overwrite mappings, and changing a mapping does not rebuild
already-defined descendants. These are not general strict-definition APIs.

The shared typed-C++/Python expression planner now preflights **one expansion**:
it rejects newly requested physical output names that collide with different
resolved input lists, before that expansion attaches nodes or registers derived
outputs. Identical requests can share the existing single producer. This check
does not inspect the producer of a pre-existing column, detect conflicts across
separate calls, validate arbitrary mapping replacements, or cover the separate
`DataManager::DefineVector` lowering. It is a useful guard, not a complete
ownership system. The source change takes effect when its callers are rebuilt;
a previously built Python module or worker retains its compiled behavior.

## Define calibrated variations under distinct names

Use a correction-specific physical namespace. Resolve the inputs for the exact
variation deliberately; for a calibration's own uncertainty this commonly means
the nominal prepared geometry. Avoid applying an unrelated input shift again.
Install the complete mappings before defining MET components, selections,
weights or histograms that consume the corrected quantity.

C++ example, with analysis-provided `applyNominal`, `applyUp`, `applyDown` and
`extractPt` callables:

```cpp
an.Define("met", applyNominal, {"prepared"});
const std::string up = "met_calibrated_ResponseUp";
const std::string down = "met_calibrated_ResponseDown";
if (an.HasColumn(up) || an.HasColumn(down))
  throw std::runtime_error("Calibrated Response columns already exist");
an.DefineNominal(up, applyUp, {"prepared"});
an.DefineNominal(down, applyDown, {"prepared"});
an.getSystematicManager().registerVariationColumns("met", "Response", up, down);
an.Define("met_pt", extractPt, {"met"});
```

`DefineNominal` is essential for these terminal functions: a propagated `Define`
can create unintended combinations or inherit a nuisance that is already being
applied explicitly. Check both target names before defining either. Scope the
functions by process, calibration applicability and enabled uncertainties.

Equivalent Python expression pattern, where the correction functions are
analysis-provided compiled helpers:

```python
analysis.define("met", "applyNominal(prepared)", ["prepared"])
analysis.define_nominal("met_calibrated_ResponseUp", "applyUp(prepared)", ["prepared"])
analysis.define_nominal("met_calibrated_ResponseDown", "applyDown(prepared)", ["prepared"])
analysis.register_variation_pair(
    "met", "Response", "met_calibrated_ResponseUp", "met_calibrated_ResponseDown"
)
analysis.define("met_pt", "extractPt(met)", ["met"])
```

The same terminal/registration ordering applies to compiled or Numba-authored
functions. For an explicit one-sided label use `registerVariationColumn` /
`register_variation`, with its exact label. Do not strip `Up`/`Down` from replica
names or manufacture absent partners.

An explicit label can coincide with a paired family on another variable.
For example, if `jet` has a paired `scale` shift and `weight` has a single
explicit `scale` alternative, the three axis entries are `scaleUp` and
`scaleDown` with the shifted jet, and `scale` with the nominal jet and
alternate weight. If the weight's explicit label is `scaleUp` instead,
the upward entry uses both the shifted jet and the alternate weight.
Use `getVariationColumnName(variable, label)` when binding an axis entry.
The bare-family membership query `getVariablesForSystematic("scale")` is
also used to enumerate inputs with paired shifts.

When a mapping must change after descendants exist, reconstruct that branch
from a valid earlier node with all mappings installed. Renaming the final output
or re-registering a label alone cannot retroactively fix descendants or saved
histograms. Freeze corrected behavior in a new build/bundle identity.

## Keep computation inputs honest

A callable's declared inputs must include every actual numerical dependency.
Keep MET/channel/region context out of geometry-only jet cleaning when the
geometry does not read it. Channel acceptance belongs in the channel/selection
mask. If a physics decision really changes the cleaned object set, it remains a
real dependency and must propagate.

Decompose shared expensive preparation from cheap nuisance-specific evaluation.
Register variation labels against the prepared computation's actual outputs.
Do not duplicate expensive physics merely because two labels have different
names; conversely, do not deduplicate distinct input bindings because their
values happened to agree in a small sample. Aliases and `Redefine` have binding
semantics; replacing identity definitions with aliases requires explicit tests.

Do not attach an expensive observable as a filter argument and expect an early
`if` inside the callable to avoid its evaluation: RDF retrieves the arguments
before entering that callable. Put a cheap necessary condition in an earlier
filter on the intended branch. A helper returning several fields can likewise
force computation of unused expensive fields; split only where actual consumers
and measurements justify it.

ROOT already evaluates demanded definitions lazily and caches them per
entry/slot. Unused definitions still cost graph construction/JIT/metadata, but
their presence alone does not prove their physics bodies execute. Use call counts
or profiling to distinguish setup overhead, dependency demand and body work.

## Preserve selection migrations and independent actions

Prefer `Analyzer::Filter` for ordinary systematic-aware selections. It retains
the union of passing variation masks while preserving individual decisions.
Never use a nominal-only raw `RNode.Filter` ahead of a varied selection unless
nominal-fail/variation-pass events are provably impossible.

For an explicit necessary prefilter, form the OR union of the relevant resolved
jet/lepton/other shape variations. A nominal mask is insufficient. Preserve each
consumer's individual mask, including nominal, and test both directions of
migration. Do not move filters across independently booked counters,
normalization sums, tag-and-probe, cutflow, preselection histograms or retained
nodes: those actions may intentionally observe a different event population.
Shared object calculations can be reused; action populations must remain exact.

## Diagnose and verify the full path

1. Trace one family from corrected objects through prepared values, mapped
   physical columns, selections, weights, histogram slots and fit inputs.
2. Before adding an explicit variation, check whether its desired physical name
   already exists and inspect the logical mapping. Include paired and explicit
   registrations with shared labels. Use graph descriptions where available;
   raw native graph operations may not be recorded.
3. Reproduce with a small synthetic event whose expected nominal, Up and Down
   values differ. Test both with and without an upstream inherited registration,
   and verify against direct calculation of the intended transformation.
4. Book downstream values and selection masks. Test nominal-fail/variation-pass,
   nominal-pass/variation-fail, process exclusions and disabled systematics.
   Registration inventory alone is insufficient.
5. Compare all unaffected bins, flows, signed sums, Sumw2, counters and independent
   banks exactly. Assess changed uncertainty templates against an independent
   physics oracle; do not preserve an erroneous template to claim equivalence.
6. For avoided-work claims, count actual calls with all actions booked before
   triggering one loop. Then profile a representative real workload. Distinguish
   setup/JIT, event loop, export, total time and memory. A sparse/empty selected
   fixture tests execution, not the affected physics template.

For a collision in one automatic expansion, the diagnostic names the logical
output, physical column, competing labels and resolved inputs. Give each distinct
producer its own physical name and map it intentionally, or correct the input
mapping if the requests should be identical. Repeated identical requests are
allowed. A pre-existing producer is still the caller's responsibility.

See [the API contract](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/api/IDataFrameProvider.h),
[variation planner](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/detail/DefineVariations.h),
[systematic propagation tests](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/tests/cpp/testSystematicPropagation.cc),
[Python authoring](PYTHON_AUTHORING.md), and
[the broader framework plan](SYSTEMATIC_GRAPH_CORRECTNESS_AND_WORK_PLAN_20261115.md).
