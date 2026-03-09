# Region-Aware Histogram & Cutflow Auto-Binding

This guide shows how to use the **region-aware binding** API introduced by
`RegionManager` integration.  The goals are:

- **Minimal user code** – declare regions once, bind managers once.
- **Single event-loop pass** – all region-scoped bookings share the same
  ROOT RDataFrame computation graph.
- **Large multi-dimensional histograms** – one `THnSparse` per histogram
  configuration entry with a *region axis* instead of N separate 1-D
  histograms.
- **Validation reporting** – missing or unknown region references are surfaced
  as errors in `ValidationReport`.

---

## 1 — Declare Regions with `RegionManager`

```cpp
// Register RegionManager with the Analyzer.
auto* rm = analyzer.getPlugin<RegionManager>("regions");

// Define the boolean filter columns on the dataframe first.
analyzer.Define("pass_presel",  [](float pt) { return pt > 20.f; }, {"pt"});
analyzer.Define("pass_signal",  [](float mll) { return mll > 80.f; }, {"mll"});
analyzer.Define("pass_control", [](float mll) { return mll < 60.f; }, {"mll"});

// Declare regions (child regions automatically inherit parent filters).
rm->declareRegion("presel",  "pass_presel");
rm->declareRegion("signal",  "pass_signal",  "presel");   // presel AND pass_signal
rm->declareRegion("control", "pass_control", "presel");   // presel AND pass_control
```

---

## 2 — Region-Scoped Cutflows

```cpp
auto* cfm = analyzer.getPlugin<CutflowManager>("cutflow");

// Register cuts (applied to the *global* dataframe as usual).
cfm->addCut("ptCut",  "pass_ptCut");
cfm->addCut("etaCut", "pass_etaCut");

// Bind to RegionManager BEFORE the event loop (i.e. before analyzer.run()).
cfm->bindToRegionManager(rm);

analyzer.run();

// After run(), access per-region results.
for (const auto& regionName : rm->getRegionNames()) {
    std::cout << "=== Region: " << regionName << " ===\n";
    std::cout << "  total: " << cfm->getRegionTotalCount(regionName) << "\n";
    for (const auto& [name, count] : cfm->getRegionCutflowCounts(regionName)) {
        std::cout << "  after " << name << ": " << count << "\n";
    }
}
```

### Output file

`finalize()` writes the following histograms to the meta ROOT file:

| Histogram key        | Type   | Description                                     |
|----------------------|--------|-------------------------------------------------|
| `cutflow`            | TH1D   | Global sequential cutflow                       |
| `cutflow_nminus1`    | TH1D   | Global N-1 table                                |
| `cutflow_regions`    | TH2D   | Regions × cuts (one cell per region-cut pair).  |

The `cutflow_regions` histogram has:
- **X-axis**: `global`, `presel`, `signal`, `control` (bin 1 = global totals)
- **Y-axis**: `total`, cut names in registration order

Using a single `TH2D` keeps the file compact and avoids proliferating many
small `TH1D` objects.

---

## 3 — Region-Scoped Histograms

```cpp
auto* hm = analyzer.getPlugin<NDHistogramManager>("histograms");

// Bind BEFORE bookConfigHistograms().
hm->bindToRegionManager(rm);

// bookConfigHistograms() uses the histogram config file as usual,
// but adds a region axis automatically.
hm->bookConfigHistograms();

analyzer.run();
hm->saveHists();
```

### How it works

1. `bookConfigHistograms()` calls `ensureRegionMembershipColumn()` which
   defines two helper column families on the dataframe:
   - `__rm_in_region_<name>__` – boolean, true when the event satisfies the
     full filter chain for region `<name>` (including all ancestors).
   - `__rm_ridx_<name>__` – float index: `float(in_region) * (i+1)`, where
     `i` is the 0-based position in the region list. Events not in a region
     contribute 0.0, which maps to the underflow bin and is silently ignored.
2. `DataManager::DefineVector` packs these per-region floats into a single
   `ROOT::VecOps::RVec<Float_t>` column named `__rm_region_membership__`.
3. Each config histogram is booked **once** as a multi-dimensional `THnSparse`
   with `N` bins on the channel axis (one per declared region), where
   `lowerBound = 0.5` and `upperBound = N + 0.5`.
4. Multi-fill: an event in both `presel` and `signal` fills the `presel` bin
   **and** the `signal` bin in the same event-loop pass.  No extra passes.

### Example histogram config (unchanged syntax)

```ini
# histogram_config.txt
name    = pt
variable = pt
weight   = w
bins     = 50
lowerBound = 0.0
upperBound = 200.0
```

With regions bound, `SaveHists()` extracts each region's slice of the
`THnSparse` and writes a standard `TH1F` named `pt` into the appropriate
region directory inside the ROOT file.

> **Note**: The region-aware booking path supports *scalar* (per-event) base
> variables.  For per-object collections (RVec variables) keep using the
> standard `bookND()` interface.

---

## 4 — Validation Reporting

The Python `ValidationReport` class gains a `region_references` section that
tracks histogram/cutflow configurations referencing unknown regions.

```python
from validation_report import ValidationReport, validate_region_references

report = ValidationReport(stage="histogram")

# Simulate references discovered from config files.
validate_region_references(
    report,
    known_regions=["presel", "signal", "control"],  # from RegionManager
    referenced=[
        {"config_type": "histogram", "config_name": "pt",     "region": "signal"},
        {"config_type": "histogram", "config_name": "pt",     "region": "unknown"},  # ERROR
        {"config_type": "cutflow",   "config_name": "myCF",   "region": "control"},
    ],
)

print(report.to_text())
# ...
# ============================================================
#   REGION REFERENCES
# ============================================================
#   Type            Config Name              Referenced Region       Known
#   ----------------------------------------------------------------
#   histogram       pt                       signal                    yes
#   histogram       pt                       unknown                UNKNOWN
#   cutflow         myCF                     control                   yes
```

`report.has_errors` returns `True` whenever any `RegionReferenceEntry` has
`is_known = False`.

---

## 5 — Complete Example

```cpp
// analysis.cc

Analyzer analyzer;

// 1. Define boolean columns.
analyzer.Define("pass_presel",  preselFn,  {"pt", "eta"});
analyzer.Define("pass_signal",  signalFn,  {"mll"});
analyzer.Define("pass_control", controlFn, {"mll"});
analyzer.Define("pass_ptCut",   ptCutFn,   {"pt"});

// 2. Declare regions.
auto* rm = analyzer.getPlugin<RegionManager>("regions");
rm->declareRegion("presel",  "pass_presel");
rm->declareRegion("signal",  "pass_signal",  "presel");
rm->declareRegion("control", "pass_control", "presel");

// 3. Register cuts and bind cutflow manager.
auto* cfm = analyzer.getPlugin<CutflowManager>("cutflow");
cfm->addCut("ptCut", "pass_ptCut");
cfm->bindToRegionManager(rm);  // must be before run()

// 4. Bind histogram manager and book.
auto* hm = analyzer.getPlugin<NDHistogramManager>("histograms");
hm->bindToRegionManager(rm);  // must be before bookConfigHistograms()
hm->bookConfigHistograms();

// 5. Single event-loop pass covers everything.
analyzer.run();

// 6. Save histograms.
hm->saveHists();
```

### Single execution guarantee

Both the `CutflowManager` and `NDHistogramManager` book all lazy RDataFrame
actions (counts, `THnSparse` bookings) **before** the event loop is triggered.
Because all region-specific nodes share the same underlying computation graph
rooted at the original `RDataFrame`, ROOT's scheduler executes a **single**
event-loop pass covering global, per-region cutflows, and all histograms
simultaneously.  There is no hidden extra pass.
