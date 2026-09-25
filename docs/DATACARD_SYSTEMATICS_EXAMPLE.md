# Systematic variations in histograms

This example shows the current C++ API for propagating a paired variation
through a derived column, a selection, and an `NDHistogramManager` histogram.
It assumes the input dataframe contains `jet_pt`, `jet_pt_jesUp`,
`jet_pt_jesDown`, and `event_weight` columns.

```cpp
#include "analyzer.h"
#include "NDHistogramManager.h"

int main() {
  Analyzer analyzer("config.txt");
  auto histograms = NDHistogramManager::create(analyzer);

  auto& systematics = analyzer.getSystematicManager();
  systematics.registerVariationColumns(
      "jet_pt", "jes", "jet_pt_jesUp", "jet_pt_jesDown");

  // DataFrameProvider uses the mapping above to define corrected_pt_jesUp
  // and corrected_pt_jesDown from the corresponding shifted inputs.
  analyzer.Define("corrected_pt",
                  [](Float_t pt) { return pt * 1.01f; },
                  {"jet_pt"});

  // Analyzer::Filter carries each variation's cumulative pass mask, so an
  // event passing only a shifted selection remains available to that shape.
  analyzer.Filter("high_pt",
                  [](Float_t pt) { return pt > 30.0f; },
                  {"corrected_pt"});

  histInfo info("corrected_pt", "corrected_pt", "jet pT",
                "event_weight", 40, 0.0f, 400.0f);
  histograms->BookSingleHistogram(info);

  analyzer.run();
  return 0;
}
```

The systematic manager maps a base variable and label to the actual shifted
column names. `registerVariationColumns` is useful when input columns do not
follow the conventional `<base>_<family>Up` and `<base>_<family>Down` names.
The generic `Analyzer::Define` callable receives its declared input columns;
the framework creates varied outputs by applying that callable to mapped
shifted inputs.

`NDHistogramManager` books the nominal and registered variations into its
systematic axis by default. This example does not create separate one
dimensional ROOT objects named `corrected_pt_jesUp` and
`corrected_pt_jesDown`. The Python datacard generator documents a separate
input convention based on per-process one dimensional templates. A conversion
from the ND histogram output to those templates must be provided and checked
before using this example as a datacard production workflow; the framework's
current documentation does not establish that round trip.

For a hand-checkable example of variation-specific event migration and
histogram contents, see the `SystematicPropagationTest` cases in
`core/tests/cpp/testSystematicPropagation.cc`.

The Python datacard generator keeps ROOT bin variances when it writes the
per-process templates and emits `* autoMCStats 0 1`, which asks Combine to add
automatic template statistical uncertainties, including signal processes.
Variable rebin edges must match existing source-bin boundaries. A declared
shape systematic must have both input directions for every applicable
component sample; generation reports the process, systematic, direction, and
sample instead of constructing a scaled nominal substitute. A regeneration
invalidates the previous card and shapes paths before staging a matching pair,
so a failed attempt cannot leave the old pair as an apparent success.
