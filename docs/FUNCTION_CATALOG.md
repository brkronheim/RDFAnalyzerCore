# Analysis function catalog

| Task | C++ entry points | Python graph entry points and contract |
|---|---|---|
| Get an indexed object attribute | `rdf::indexedValue` in AnalysisMath.h | `rdfkinematics.indexed_value` takes source index and fallback columns. Use a Boolean or unsigned fallback of the intended type. Create a separate validity flag if a real value can equal the fallback. |
| Retrieve one four-vector | `rdf::indexedPtEtaPhiM` in AnalysisMath.h | `rdfkinematics.indexed_pt_eta_phi_m` returns a ROOT PtEtaPhiMVector. Inconsistent lengths and invalid indices raise. Keep the original object index when selecting or sorting a collection. |
| Angular separation | `rdf::signedDeltaPhi`, `rdf::absDeltaPhi`, `rdf::deltaR2` in AnalysisMath.h | `rdfkinematics.signed_delta_phi`, `abs_delta_phi`, `delta_r2`. Radians, first minus second, both π ties map to −π. Nonfinite azimuths yield NaN. Use squared radii for squared-distance cuts. |
| Sum selected objects in the transverse plane | `rdf::selectedTransverseSum(mask, pt, phi)` in AnalysisMath.h | `rdfkinematics.selected_transverse_sum` books one double-precision `(px, py)` pair. Use `.first` and `.second` in derived columns. Arrays must align. Invalid selected momenta raise, masked values are ignored and an empty selection gives zero. Apply the minus sign explicitly for missing momentum. |
| Transverse kinematics and weights | `rdf::transverseVectorPt`, `transverseVectorPhi`, `transverseMass`, `covarianceQuadratic`, `selectedProduct`, `heldOutScore` in AnalysisMath.h | Call `rdfkinematics.declare_analysis_math()`, then use the qualified C++ name in `Analysis.define`. Check the invalid-input policy of the operation. |
| Keep a single computation shared by several columns | `defineProjectedColumns`, `memberProjection`, `indexProjection` in ColumnProjection.h | Named fields and `Analysis.project` allow multiple outputs of one kernel to be reused. |
| Build and combine selected objects | `PhysicsObjectCollection`, `makePairs`, `makeCrossPairs`, `makeTriplets` in PhysicsObjectCollection.h | Use collection fields with their original source indices. The combination functions materialize all combinations. For one known pair, access its two elements directly. |
| Tag-and-probe and trigger-object matching | TagAndProbe.h | `rdftnp.define_pairs` and `rdftnp.define_trigger_matches` book ROOT graph nodes. The sample and measurement select the tag, denominator and decision. |
| Bins, efficiencies, working points | CartesianBinning.h, EfficiencyMeasurement.h, WorkingPointRecipe.h | These native recipes share bin definitions across booking and output. See the [analysis guide](ANALYSIS_GUIDE.md). |
| CMS jet ID, MET and gen ancestry | CmsJetId.h, CmsMetCorrections.h, CmsGenParticles.h in the CMS extension | Available with the [CMS extension](CMS_EXTENSION.md). |

### Book the same selection in C++ and Python

```cpp
#include "AnalysisMath.h"
analysis.Define("dphi", rdf::signedDeltaPhi, {"phi1", "phi2"});
```

```python
from rdfkinematics import signed_delta_phi
signed_delta_phi(analysis, "dphi", "phi1", "phi2")
```

The C++ example takes double input columns. For float inputs, pass a lambda with two float arguments that calls the same function. Both paths use the same C++ computation. The Python helper declares the header once and books a variation-aware graph definition. Each shifted input uses its matching physical column. Python executes at graph construction. Existing analyses using EvalDeltaPhi or indexVector retain their previous conventions.

[Scalar operations and signatures](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/AnalysisMath.h) · [Python signatures and docstrings](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/python/rdfkinematics.py) · [Collection APIs](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/PhysicsObjectCollection.h) · [Tag-and-probe APIs](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/TagAndProbe.h)
