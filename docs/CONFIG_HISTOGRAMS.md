# Config-Driven Histogram Feature

## Overview

This feature allows histograms to be defined in a configuration file and automatically booked at runtime. Histograms respect filters and defines applied to the dataframe and work with systematic variations just like manually-defined histograms.

## Configuration File Format

The file extension selects the parser. Text files (`.txt`) use one histogram definition per line with `key=value` fields. YAML files (`.yaml` or `.yml`) use a sequence of mappings, as in `analyses/ExampleAnalysis/histograms.yaml`.

### Required Fields

- `name`: Histogram name
- `variable`: Variable to histogram
- `weight`: Weight variable
- `bins`: Number of bins
- `lowerBound`: Lower bound of histogram
- `upperBound`: Upper bound of histogram

### Optional Fields

- `label`: Axis label (defaults to variable name)
- `suffix`: Suffix to append to histogram name
- `channelVariable`: Variable for channel axis
- `channelBins`: Number of channel bins
- `channelLowerBound`: Lower bound for channel
- `channelUpperBound`: Upper bound for channel
- `channelRegions`: Comma-separated list of channel region names
- `controlRegionVariable`: Variable for control region axis
- `controlRegionBins`: Number of control region bins
- `controlRegionLowerBound`: Lower bound for control region
- `controlRegionUpperBound`: Upper bound for control region
- `controlRegionRegions`: Comma-separated list of control region names
- `sampleCategoryVariable`: Variable for sample category axis
- `sampleCategoryBins`: Number of sample category bins
- `sampleCategoryLowerBound`: Lower bound for sample category
- `sampleCategoryUpperBound`: Upper bound for sample category
- `sampleCategoryRegions`: Comma-separated list of sample category names

## Example Configuration File

Text format (`histograms.txt`):

```txt
# Simple 1D histogram
name=pt_hist variable=jet_pt weight=event_weight bins=50 lowerBound=0.0 upperBound=500.0 label=Jet pT [GeV] suffix=jets

# Histogram with channel separation
name=eta_hist variable=jet_eta weight=event_weight bins=40 lowerBound=-5.0 upperBound=5.0 channelVariable=channel channelBins=2 channelLowerBound=0.0 channelUpperBound=2.0 channelRegions=ee,mumu suffix=dilep

# Full N-dimensional histogram with all separations
name=mass_hist variable=dimuon_mass weight=event_weight bins=60 lowerBound=70.0 upperBound=110.0 channelVariable=channel channelBins=2 channelLowerBound=0.0 channelUpperBound=2.0 channelRegions=signal,control controlRegionVariable=region controlRegionBins=3 controlRegionLowerBound=0.0 controlRegionUpperBound=3.0 controlRegionRegions=SR,CR1,CR2 sampleCategoryVariable=sample sampleCategoryBins=2 sampleCategoryLowerBound=0.0 sampleCategoryUpperBound=2.0 sampleCategoryRegions=data,MC suffix=analysis
```

Equivalent YAML format (`histograms.yaml`):

```yaml
- name: pt_hist
  variable: jet_pt
  weight: event_weight
  bins: 50
  lowerBound: 0.0
  upperBound: 500.0
  label: "Jet pT [GeV]"
```

## Usage in Code

### Basic Setup

```cpp
#include <analyzer.h>
#include <NDHistogramManager.h>

// Create analyzer with config file
auto analyzer = Analyzer("config.txt");

// Add NDHistogramManager plugin
auto histManager = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());
analyzer.addPlugin("histogramManager", std::move(histManager));
```

### Configuration File Reference

In your main config file (e.g., `config.txt`), add:

```txt
histogramConfig=histograms.txt
```

### Define Variables and Apply Filters

```cpp
// Define variables
analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"})
        .Define("jet_eta", computeEta, {"jet_px", "jet_py", "jet_pz"})
        .Define("event_weight", computeWeight, {"mc_weight", "pu_weight"})
        .Define("channel", getChannel, {"lepton_type"});

// Apply filters
analyzer.Filter("good_events", isGoodEvent, {"event_flags"})
        .Filter("jet_quality", hasGoodJets, {"jet_pt", "jet_eta"});
```

### Book Config Histograms

After all defines and filters are applied, book the histograms from config:

```cpp
// Book histograms defined in config file
analyzer.bookConfigHistograms();
```

### Save Results

```cpp
// Trigger dataframe execution and save histograms
analyzer.run();
```

## Complete Example

```cpp
#include <analyzer.h>
#include <NDHistogramManager.h>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config.txt>" << std::endl;
        return 1;
    }

    // Create analyzer
    auto analyzer = Analyzer(argv[1]);

    // Add histogram manager plugin
    auto histManager = std::make_unique<NDHistogramManager>(
        analyzer.getConfigurationProvider());
    analyzer.addPlugin("histogramManager", std::move(histManager));

    // Define variables
    analyzer.Define("jet_pt", computePt, {"jet_px", "jet_py"})
            .Define("jet_eta", computeEta, {"jet_px", "jet_py", "jet_pz"})
            .Define("event_weight", computeWeight, {"mc_weight", "pu_weight"})
            .Define("channel", getChannel, {"lepton_type"});

    // Apply event selection
    analyzer.Filter("trigger", passTrigger, {"trigger_bits"})
            .Filter("lepton_selection", hasGoodLeptons, {"lepton_pt", "lepton_eta"})
            .Filter("jet_selection", hasGoodJets, {"jet_pt", "jet_eta"});

    // Book histograms from config file
    analyzer.bookConfigHistograms();

    // Manually book additional histograms if needed
    // (config histograms and manual histograms work together)
    
    // Execute dataframe and save results
    analyzer.run();

    return 0;
}
```

## Features

### Flexible Dimensionality

- **1D histograms**: Only specify required fields (name, variable, weight, bins, bounds)
- **Multi-dimensional**: Add channel, control region, and/or sample category dimensions as needed
- **Mixed usage**: Combine config-defined and manually-defined histograms in the same analysis

### Systematic Variations

Histogram systematic handling is enabled by default. Before the first booking, `NDHistogramManager` scans available dataframe column names for complete `baseVariable_nameUp` / `baseVariable_nameDown` pairs and registers the matching systematic against the base variable. You can disable this behavior with `histogramSystematics: false` in the main config to book nominal-only histograms. For a complete nominal-only framework graph, use `enableSystematics: false`; this also prevents systematic registration and propagation outside histogram booking. A registered name alone does not calculate a shifted value: the analysis or an input source must provide the varied columns, or the analysis must define and map the variations itself.

```cpp
// Define a complete Up/Down pair using the expected baseVar_nameUp/Down form.
analyzer.Define("jet_pt_JESUp", applyJESUp, {"jet_pt"});
analyzer.Define("jet_pt_JESDown", applyJESDown, {"jet_pt"});

// NDHistogramManager can detect and register the pair during booking.
analyzer.bookConfigHistograms();
```

### Deferred Booking

Histograms are booked after all defines and filters have been applied, ensuring they:
- Respect all event selections
- See all defined variables
- Work with the final dataframe state

### Backward Compatibility

The existing methods for booking histograms remain unchanged:
- `NDHistogramManager::BookSingleHistogram()`
- `NDHistogramManager::bookND()`

You can use config-driven histograms alongside manually-defined histograms in the same analysis.

## Boost Histogram Backend

NDHistogramManager supports both ROOT (`THnMulti`) and Boost.Histogram (`BHnMulti`) backends for N-dimensional histograms. The framework selects storage using a configurable dense-memory estimate; the best CPU/memory tradeoff depends on occupancy and workload.

### Storage selection and output

The default is **Boost float with sparse standard-map accumulation**:

```yaml
histogramBackend: boost
histogramBoostPrecision: float
histogramBoostSparseBackend: map
histogramDenseLimitMiB: 0
```

Float Boost uses ROOT-compatible axes, including variable edges, float bin
contents and double sumw2. Explicit `histogramBoostPrecision=double` selects
double weighted sums and routes variable-width observables through the ROOT
action. Explicit `histogramBackend=root` selects `THnF` dense or `THnSparseF`
sparse slot accumulators according to the same configured dense threshold.

`histogramDenseLimitMiB` defaults to 0 (sparse). With a nonzero budget, dense storage is selected when
`product(bins + 2) * RDF_slots * bytes_per_cell` fits within that limit, unless
the C++ `histInfo` explicitly requests sparse storage. ROOT cells use 12 bytes
(float content plus double sumw2); Boost double cells use 16 bytes. Boost
float contents with double sumw2 use custom separate arrays at 12 bytes per
cell; `weighted_sum<float>` alone would incorrectly reduce variance precision.
The limit covers
**all slots for one histogram**, including underflow/overflow cells. It is not
a per-thread or process-wide limit: hundreds of eligible histograms can still
allocate substantial memory. Set it to `8` or `64`, for example, to permit
dense accumulation for histograms that fit within that per-histogram budget.

When saving to the meta file, each Boost sparse histogram warns once if its
observed container allocations exceed the estimated dense arrays for the same
precision, logical grid (including flows) and RDF slot count. Counting allocators
include map nodes and bucket/capacity storage for both `map` and `flat`. Counters
are slot-local; finalization samples them before merging and after each slot
merge, preserving the largest observed total after containers are released.
The warning names the histogram and output file, reports both byte counts,
and suggests increasing `histogramDenseLimitMiB`. It does not switch storage.
This comparison excludes allocator overhead, transient event-time rehash peaks,
the common ROOT output representation, and other process memory; it is not an
RSS measurement. ROOT and dense Boost actions do not emit this warning.

Both backends always persist `THnSparseF` under `nd_histograms_v1`, accompanied
by `__rdf_histogram_schema_v1`. Dense ROOT finalization visits the allocated
array, decodes occupied cells only, and transfers integer coordinates and
sumw2 directly. Sparse ROOT finalization reuses slot zero and merges other
slots into it. Boost sparse merging/export visits allocated map entries rather
than the complete logical grid. Flow bins and zero-content cells with nonzero
variance are retained. Compiled projections discard flow coordinates; legacy
TH1 extraction retains observable underflow/overflow while rejecting invalid
category coordinates.

ROOT and the default float Boost accumulate contents in float.
`histogramBoostPrecision=double` rounds double contents to float at export;
`histogramBoostPrecision=float` matches ROOT
arithmetic and axis boundary handling. This is a numerical choice, especially
with large
positive and negative weights; a faster backend is not automatically an exact
replacement. Both accumulate sumw2 in double. Boost sparse storage can use
`histogramBoostSparseBackend=map` (the default node-based standard hash map)
or `flat` (Boost flat hash map, when available in the installed Boost). Both
merge/export allocated entries only. Float sparse cells still have ordinary
structure padding; the 12-byte saving applies to the dense separate arrays.
Sparse storage does not imply a universal speedup; benchmark the analysis workload.

### Extraction and direct reading

`core/python/histogram_io.py` exposes compiled batch projection to NumPy, with
a Python fallback. Readers that understand the schema can consume sparse
files directly. Reconstructing every category/systematic combination as TH1F
can produce many empty objects and duplicate the original sparse data.

`extract_nd_histograms.py` remains available for legacy consumers. When the
compiled extension is present, its validation/repair pass is followed by
native slice creation, filling and writing, with one source histogram and
one output TH1 resident at a time. Only nonempty slice data are grouped in
small bin buffers; empty TH1 combinations are streamed directly to the file.
The Python implementation is retained as a fallback. Both preserve
empty combinations, variable edges, flow bins, sumw2 and unrelated objects;
publication still uses an atomic replacement of a copied input file.

The generic partial merger retains its compatibility default. Pass
`python -m partial_merge ... --sparse-only` to omit legacy extraction.
`analyses/vjet-njetpt/mergeResults.sh` defaults to sparse-only output because
its plotting and fit readers consume that format. Use `--extract-histograms`
to retain the former expanded layout. `--nominal-only` still performs schema
postprocessing and nominal TH1 extraction; it cannot be combined with an
explicit `--sparse-only`.

Set `RDF_HISTOGRAM_PROFILE=1` for per-action storage/finalization diagnostics.
See `benchmarks/histogram_lifecycle_20261030/REPORT.md` for the measured vjet
tradeoffs and reproduction commands.

## Notes

- Histogram variables and weights must be defined before calling `bookConfigHistograms()`
- If no `histogramConfig` is specified in the main config, no config histograms are loaded
- Missing required fields in the histogram config will throw an exception
- Invalid config files will throw an exception with details
- Histograms are saved to `metaFile` when specified. Otherwise, the framework derives a sibling path by inserting `_meta` before the `saveFile` extension (for example, `output.root` becomes `output_meta.root`).
