# Variable-width Python histograms

`Analysis.histogram_variable` books a lazy ROOT `TH1D` with explicit bin edges:

```python
analysis.histogram_variable(
    'pt/nominal', 'pt', [0., 25., 50., 100., 250.],
    weight='event_weight', variation='Nominal', region='selected',
)
```

Edges must be a list or tuple of at least two Python integers/floats. They are
copied to immutable double values and must remain finite and strictly increasing
after conversion; booleans, repeated edges, NaNs and infinities fail before
booking. Mutating the original list later cannot change the axis. The native
entry point independently checks finite, increasing edges.

The existing `histogram(name, variable, bins, low, high, ...)` API and its regular
axis booking path retain their behavior. Both APIs use the requested variation's
resolved observable, weight, acceptance mask and optional selection/region.
Counts remain available under every histogram's name. Results have the same
`edges`, `sumw` and `sumw2` fields, with underflow and overflow in the first and
last entries of the weight arrays. An observation equal to the final edge belongs
to overflow, following ROOT semantics. Optional histogram persistence writes the
same variable edges to the metadata file's `python_histograms/` tree.

`Observable` accepts either axis kind, so one staged group can contain both:

```python
from rdfanalysis import Stage, HistogramGroup, Observable, RegularAxis, VariableAxis

group = HistogramGroup('kinematics', (
    Observable('pt', 'pt', VariableAxis([0., 25., 50., 100., 250.])),
    Observable('eta', 'eta', RegularAxis(30, -3., 3.)),
), regions=('selected',), variations='all', weight='event_weight')
analysis.apply(Stage('selected_plots').histograms(group))
```

`Stage.histogram_variable` also supports individual declarations. Stage phases,
expansion limits, output-name validation and explicit boundaries around filters
apply equally to both axis kinds. Describing or previewing declarations does not
import ROOT or process events.

Explicit batching can mix the two axis kinds at one current node:

```python
analysis.histogram_batch([
    dict(name='pt', variable='pt', edges=[0., 25., 50., 100., 250.],
         weight='event_weight', region='selected'),
    dict(name='eta', variable='eta', bins=30, low=-3., high=3.,
         weight='event_weight', region='selected'),
])
# Eligible staged groups select the available native batch automatically:
# analysis.apply(Stage('selected_plots').histograms(group))
# batch=True explicitly requires it; batch=False selects ordinary booking.
```

Each mapping specifies either `edges` or all three regular-axis fields
`bins`, `low`, `high`. Explicit batches share a selected RDF node and count only
when the resolved acceptance mask and selection match. The cache lives inside
that one booking call, including mixed-axis calls; later definitions, filters,
stages and separate calls never inherit it. Staged groups with multiple expanded
histograms automatically select the applicable native batch capability;
singletons and older capabilities retain ordinary booking. An explicitly
requested unavailable batch still fails. Booking and batching introduce no event
passes.

Feature detection follows the loaded native module. `describe().capabilities`
includes `variable-histograms` and `mixed-histogram-batch` only when their native
entry points exist. Older modules continue to support their existing regular
histograms and 8-tuple native batches; requesting either new operation raises a
clear rebuild error. There is no silent fallback from an explicitly requested
mixed batch. Rebuild/install matching native bindings to use these capabilities.
Structural frontend batch errors are checked before native booking; later native
errors can leave a partially booked graph, as with existing batch APIs. Start a
fresh `Analysis` after such a failure.

## Collection values, signed weights and filter boundaries

With existing `Jet_pt` and signed `genWeight` input columns, two stages preserve
histograms on opposite sides of a filter. This uses ordinary Python for the
per-jet weight kernel and the compiled native callable path:

```python
import numpy as np

def per_jet_weight(pt, weight):
    result = np.empty(len(pt), dtype=np.float64)
    for index in range(len(pt)):
        result[index] = np.float64(weight)
    return result

analysis.define_nominal('weight', 'double(genWeight)', ['genWeight'])
analysis.define_numba('jet_weights', per_jet_weight, ['Jet_pt', 'weight'],
                      return_type='RVec<double>', nominal=True, require_native=True)
observables = (
    Observable('regular', 'Jet_pt', RegularAxis(8, 30., 60.)),
    Observable('variable', 'Jet_pt', VariableAxis((30., 31., 40., 50., 60.))),
)
analysis.apply(Stage('before').count('before').histograms(
    HistogramGroup('before', observables, weight='jet_weights')))
analysis.apply(Stage('after').filter('four_jets', 'Jet_pt.size() > 3', ['Jet_pt'])
    .count('after').histograms(
        HistogramGroup('after', observables, weight='jet_weights')))
```

Using `weight='weight'` instead broadcasts each scalar event weight to its jet
fills. The vector-weight form requires aligned per-element weights. Named counts
remain event counts even when an event fills multiple bins; Sumw2 accumulates
the squared weight of each fill. The cache stays within each group, so the first
group includes events removed by the later filter. Both forms and ordinary/
automatic grouping pass the independent signed collection control,
including bins, edges, flows, Sumw2, all counts and selected rows. This is finite
correctness evidence, not collection throughput acceptance.
