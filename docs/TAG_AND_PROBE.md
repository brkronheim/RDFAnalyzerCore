# Reusable tag-and-probe helpers

`core/interface/TagAndProbe.h` is a header-only C++17 utility with no ROOT or
experiment dependency. `rdftnp` is the optional lazy Python/RDataFrame adapter.
Selections, physics weights, background fits and efficiency exports remain owned
by each measurement. The native header ships in `NativeAuthoring`; the adapter
and type stub ship in the `Python` component together with the required header.

```cpp
#include "TagAndProbe.h"
rdf::tagprobe::PairConfig selection;
selection.minMass = 60.; selection.maxMass = 120.;
selection.seed = 101;
auto pair = rdf::tagprobe::selectPair(
    pt, eta, phi, mass, charge, denominator, tags, run, lumi, event, selection);
// pair = {tag index, probe index, eligible unordered denominator-pair count}.
// A negative tag index means the event was rejected.
```

All object containers must be aligned. Denominator membership and kinematics
choose the opposite-charge pair nearest the reference mass. An event-key hash
then chooses its orientation **before testing the tag**. A failed chosen tag
rejects the event: there is no swap, alternative-pair or tag fallback. The
measured probe-pass decision is deliberately absent from the API. An event
contributes at most one pair, avoiding duplicate-pair event correlations.
Approximately half of events with only one valid tag are lost. This is an
intentional price for selection independence; it does not remove physical
correlations between the two leptons or substitute for a background fit.

Python equivalent:

```python
from rdftnp import define_pairs
node = define_pairs(node, 'z', {
    'pt': 'lepton_pt', 'eta': 'lepton_eta', 'phi': 'lepton_phi',
    'mass': 'lepton_mass', 'charge': 'lepton_charge',
    'denominator': 'probe_denominator', 'tags': 'tag_mask',
    'run': 'run', 'lumi': 'luminosityBlock', 'event': 'event'}, seed=101)
# z_pair and z_valid are new columns; no action/event loop is executed here.
```

Use `matchTriggerObjects` / `define_trigger_matches` with an explicit fired-path
gate, absolute particle ID, minimum trigger-object pT, maximum delta-R and a
required filter-bit mask. **Every required bit** must be present. The result is
the closest qualifying trigger-object index per reconstructed object, or -1.
Filter bits are experiment/campaign specific; the generic helper does not
supply them. All arrays are checked for alignment. Wrapped azimuth, finite
coordinates and invalid configuration are handled explicitly.

Keep pass/fail probes together and store joint working-point decisions when
working points are not nested. Do not apply the measured probe SF to its own
measurement. State the exact denominator: an efficiency conditional on a loose
ID does not measure the reconstruction or loose-ID efficiency. Fit the signal
and background mass distributions and propagate category covariance before
exporting a calibration. The helpers alone provide no statistical fit model.

The first analysis consumer is
the 23post vjet measurement.
