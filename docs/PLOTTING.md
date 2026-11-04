# Physics plotting with PlottingUtility

`PlottingUtility` renders ROOT histograms, sparse projections, or supplied arrays
using persistent C++ drawing objects. It supports data/MC stacks, comparisons,
signed response matrices, and configurable diagnostic panels. Existing
`makeStackPlot()` and `makeStackPlots()` calls continue to work.

The default presentation follows CMS conventions: white background, a bold CMS
header, luminosity above the frame, inward ticks, an inside legend, and aligned
panels. ATLAS, LHCb, ALICE, and custom text headers are also supported. These are
text/style presets, not official collaboration logos or an approval workflow.
Set the appropriate status text yourself; none is assumed.

## Quick start

```cpp
#include <PlottingUtility.h>

PlotRequest request;
request.metaFile = "merged.root";
request.outputFile = "plots/mass.png";
request.additionalOutputs = {"plots/mass.pdf", "plots/mass.svg", "plots/mass.C"};
request.title = "Z #rightarrow #mu#mu, two jets";
request.xAxisTitle = "Dijet mass [GeV]";
request.yAxisTitle = "Events / bin";
request.luminosityLabel = "100 fb^{-1} (13.6 TeV)";
request.experimentExtra = "Preliminary";
request.panels = {{PlotPanel::Ratio}};
request.processes = {
    {"background", "mass", "Background", kBlue + 1, 1., "", false},
    {"data", "mass", "Data", kBlack, 1., "", true}};

PlottingUtility::Session session; // Keep alive for the entire plotting campaign.
auto result = session.render(request);
if (!result.success) throw std::runtime_error(result.message);
// Modify the request and render again, or call session.renderBatch(requests).
```

Every exported primitive remains alive until painting/export finishes. Canvases,
pads, graphs, legend primitives, and scratch arrays are reused within the
session. The batch wrapper creates one session for its entire batch. Repeated
one-shot wrapper calls construct independent sessions; use a session to obtain
reuse across calls.

Python exposes the same types and a shared YAML/JSON parser:

```python
import rdfanalyzer as rdf

requests = rdf.PlottingUtility.readRequests("plots.yaml")
options = rdf.PlotBatchOptions()
options.workers = 4                       # Input preparation only.
session = rdf.PlotSession(options)         # Also PlottingUtility.Session.
results = session.renderBatch(requests)   # C++ work releases the GIL.
for result in results:
    if not result.success:
        raise RuntimeError(result.message)
print(session.stats().drawableObjects)
```

The compiled executable uses exactly the same parser:

```bash
cmake --build build --target rdf-plot
build/core/rdf-plot --config plots.yaml --workers 4 --report plots/report.yaml
# For export-bound batches, use independent renderer processes:
build/core/rdf-plot --config plots.yaml --render-processes 4 --report plots/report.yaml
```

Reports include per-request status, verified output paths, visible data/MC
integrals, warnings, configuration path, and cumulative timings/resource counters.
They do not copy observed yields from blinded bins.

## ROOT macro export

Use a `.C` output path, either as `outputFile` or in `additionalOutputs`. The
macro contains standard ROOT canvases, pads, histograms, graphs, labels, and
palette definitions, with no dependency on RDFAnalyzerCore or the original
input files. It is an unnamed ROOT macro, so it can be renamed and run using:

```bash
root -l plot.C
# Or from a ROOT prompt: .x plot.C
```

Only visible data points are serialized; blinded observations are omitted.
Saving a macro also preserves selection text, panel geometry, asymmetric
errors, and heatmap color scales. Macro serialization runs only for `.C`
outputs; ordinary image exports continue to use the persistent drawing objects.

## Configuration example

```yaml
schema_version: 1
plots:
  - outputFile: plots/mass.png
    additionalOutputs: [plots/mass.pdf, plots/mass.C]
    title: "Z #rightarrow #mu#mu, two jets"
    experiment: cms                 # cms (default), atlas, lhcb, alice, custom
    experimentExtra: Simulation    # Optional italic status text.
    luminosityLabel: "100 fb^{-1} (13.6 TeV)"
    xAxisTitle: "Mass [GeV]"
    yAxisTitle: Events
    binEdges: [60, 90, 120, 150]
    panels: [significance, signal_background, ratio]
    blindBins: [false, true, false]
    processes:
      - legendLabel: Background
        color: "#4477AA"
        values: [100, 80, 40]
        variances: [20, 16, 8]
        variations:
          - name: calibration
            up: [105, 84, 42]
            down: [95, 76, 38]
      - legendLabel: Signal
        color: "#CCAA44"
        isSignal: true
        displayScale: 5
        values: [2, 10, 3]
        variances: [0.2, 1, 0.3]
      - legendLabel: Data
        isData: true
        values: [101, 91, 42]
        variances: [101, 91, 42]
```

Arrays must contain finite values and explicit nonnegative variances. Optional
`errorLow`/`errorHigh` supply asymmetric absolute errors for in-memory data;
otherwise errors come from sumw2. Set Poisson intervals upstream when desired.
`PlottingUtility.parseRequests(text)` accepts the same document as a string. A
bare sequence of requests also works for compatibility with existing JSON files.

## Presentation controls

* `experiment` selects the header preset. C++ uses `PlotExperiment`; Python uses
  `rdf.PlotExperiment`. `experimentLabel` overrides the preset text. Use
  `experiment: custom` with an empty label to omit the experiment name.
* `style.legendInside` defaults to `true`. The renderer reserves vertical
  headroom for the selection text and legend before choosing the y range. `legendWidth` is a
  fraction of the main frame width; `legendColumns` is 1–4. Crowded legends wrap
  at spaces. Use `legendInside: false` for a separate legend below the axes.
* `style.width`, `height`, `labelSize`, `titleSize`, `legendSize`, `leftMargin`,
  `rightMargin`, and `grid` control geometry. Sizes are pixels, consistent across
  differently sized panels. Zero (the default) selects sizes proportional to
  canvas width: tick labels 24 px, titles/header 28 px, and legends/annotations
  22 px at 1000 px width (43.2/50.4/39.6 px at 1800 px). Positive values
  override this with explicit pixel sizes. Axis titles use the tick size plus
  4 px. The automatic height preserves header, legend,
  categorical labels, and panel space, giving the main histogram most of the
  canvas height. Impossible explicit heights fail with a
  useful error instead of producing overlapping content.
* Text supports ROOT TLatex notation, for example `p_{T}`, `#mu`, and `#sqrt{s}`.
  `title` and `annotations` appear in the upper-left of the main plot without
  a surrounding box, including heatmaps. `style.annotationBoxWidth` controls
  their wrapping width as a fraction of the frame (default 0.72). The top strip contains only the
  experiment name, status text, luminosity, and energy. Long category names can
  require larger margins/width; dense matrix labels may require a taller canvas.
* `panels` selects the ordered ratio, pull, S/B, and S/sqrt(S+B) panels. In C++
  use `PlotPanelConfig`; JSON/YAML accepts names or mappings such as
  `{type: ratio, autoRange: false, minimum: 0.7, maximum: 1.3}`. Empty `panels`
  preserves the legacy `drawRatio` behavior: ratio+pull for data/MC, a ratio for
  multi-curve overlays. Set `drawRatio: false` to request no panels.
* `kind: overlay` compares curves without stacking; `referenceProcess` chooses
  the display-group index used by ratio/pull panels (default 0). Those panels
  propagate independent MC statistical errors. A process's
  `referenceCovariance` optionally supplies per-bin covariance with the reference
  after physical scaling and before density conversion. Display multipliers
  never enter the ratios. A covariance must be compatible with the variances.

## Input and physics policies

For file inputs, each process has `histogramName`, an optional legacy `directory`,
and optional `files` (otherwise the request's `metaFile`). `scale` multiplies the
input yields; variances scale by its square. If `normalizationHistogram` is set,
the scale is divided by the sum of that counter's bin 1 across the process's
files. Supply luminosity/cross-section/filter-efficiency/k-factor information in
`scale` with explicit units; the renderer does not infer physics normalization.
Missing, zero, or nonfinite counters fail by default. Setting
`strictNormalization: false` retains the old manual-scale fallback with a warning.

Processes with equal nonempty `group` names aggregate before drawing. Their
signal, overlay, and display-scale settings must agree. Multiple data processes
are always summed. A signal enters the physical stack/denominator at nominal
normalization; `displayScale` affects only its separate outline. Set
`signalOverlay: false` to omit signal outlines. A process with `overlay: true`
is excluded from stack totals and appears only as a curve.

Uncertainties retain sumw2 even when signed weights cancel to zero. Data/MC ratio
point errors use data errors; MC uncertainty is a separate band, avoiding double
counting. Pulls include data variance, MC stat variance, and the appropriate side
of the systematic envelope. `computeRatioSummary()` remains available for ratios
with per-bin covariance; it now retains numerator uncertainty at numerator=0 and
checks bin edges. The legacy histogram ratio helper still returns zero for an
invalid denominator, while the renderer omits invalid points.

Equal nuisance names identify correlated shifts: sum variation-minus-nominal
across samples before forming an asymmetric Up/Down envelope. Independent names
combine in quadrature. Missing directions contribute zero shift. A stored
variation with no entries in a selected sparse region represents migration to
zero, not a missing variation. `systematicLow`/`systematicHigh` add independent
absolute prediction errors. Statistical and systematic bands remain separate.
Supply fit factors, prefit/postfit templates, or derived covariances explicitly;
the renderer does not perform fits or infer nuisance correlations from physics
process names.

`blindBins` removes data from points, ratios, pulls, auto-ranging, and reported
data integrals. `density: true` divides yields/errors by bin widths and variances
by width squared; significance still uses event counts. `flow: drop` is the
default, and `flow: fold` folds observable under/overflow with sumw2. Binning must
match unless `rebin: true` explicitly requests coarsening to source-aligned
`binEdges` with the same outer range. Fractional redistribution is rejected.

Signed stacks keep positive and negative contributions separate. Log-y plots
clip nonpositive drawing coordinates and omit nonpositive data points without
modifying inputs. Tiny signal tails do not determine the log-axis floor.

### Native sparse output

Without an explicit legacy directory, the adapter prefers
`__rdf_histogram_schema_v1` / `nd_histograms_v1` when the requested observable is
present. This prevents adding native and extracted copies together. Selection is
explicit by schema role and label, for example:

```yaml
metaFile: sample.root
processes:
  - histogramName: boson_pt
    legendLabel: V+jets
    selections:
      channel: [Zmumu]
      control: [signal_region]
    readSystematics: true
```

The adapter validates schema versions and full axis edges and traverses filled
bins directly. Only nominal values enter the main prediction. Unspecified
category axes are summed, so adapters must explicitly exclude inclusive parents
when selecting overlapping child categories. A nonempty `directory` instead
selects a legacy TH1/TH2 path; directory and sparse selections cannot be mixed.
`readSystematics` also reads exact `<name>_<nuisance>Up/Down` legacy keys. Keep
analysis-specific flattened response decoding and category grouping in the
analysis and pass the resulting arrays/matrix to the utility.

Python workflows that need custom projections use the shared
`core/python/histogram_io.py` `iter_sparse_bins` primitive for allocated-bin
coordinates, content, and sumw2. This keeps sparse iteration and variance
reading consistent with the legacy extractor; physics labels and projection
rules remain in each analysis adapter.

### Response matrices

Use `kind: heatmap` with `binEdges`, `yBinEdges`, and row-major `matrix` (x varies
fastest), a TH2 file input, or a sparse projection with `heatmapYRole`. Set
`binLabels`, `yBinLabels`, `horizontalSeparators`, and `zAxisTitle` as needed.
The default diverging palette has a symmetric range about zero and preserves
negative cells. `symmetricColorRange: false` uses the observed range; `logZ: true`
requires positive content. Row/column normalization and analysis-specific
efficiency/purity definitions should be computed explicitly before plotting.

## Parallelism, reuse, and limits

Sessions use fixed preparation workers and an ordered, bounded window of ready
plots. Each worker owns its ROOT input handles and numerical buffers. Graphics
construction, configuration, painting, export, and destruction are serialized
across sessions in the process. Use a session on its constructing/main thread;
do not draw unrelated ROOT graphics concurrently. Calling
`makeStackPlots(requests, true)` enables up to four preparation workers, not
concurrent canvas rendering. No RDataFrame implicit-MT pool is created.

`maxOpenFiles` bounds cached file handles across workers. `maxPreparedBytes`
limits prepared numerical payloads and nuisance/projection sizes per window;
it does not limit ROOT file decompression, image encoding, or total process RSS.
Input files are assumed immutable during a session; construct a new session
after replacing/updating merged inputs. Sparse histograms are read on demand,
rather than retaining an unbounded cache of projections.

`session.stats()` reports plots, workspaces, persistent drawable count, drawable
buffer growth/rebinding events, file opens, cumulative worker preparation time,
layout time, and painting/export time. Layouts are cached by diagnostic-panel
count, plus one heatmap layout. Pools grow to observed bin/process/label capacity;
exact histogram-axis changes may rebind their storage. ROOT painters, text
parsers, encoders, and input decoding can still allocate internally. Keeping the
session on the stack does not make ROOT's internal arrays stack allocated.

`rdf-plot --render-processes N` starts a bounded pool of fresh compiled renderer
processes, each with one persistent session. Requests are partitioned in input
order and reports merged back into that order. Output paths must be unique
across requests; worker failures produce a failing exit status. Temporary worker
configs/reports are removed after collection. This uses POSIX executable launches,
never forks a running ROOT session. Process count is explicit (maximum 32), with
up to `--workers N` preparation threads per process. Choose these together with
LAW concurrency to avoid oversubscription and excessive memory use. A process
pays its own ROOT initialization/font/image-plugin memory cost.

LAW branches can also each run an independent session. The library deliberately
does not draw different canvases on multiple threads. A general fitting engine
and dedicated unbinned ROC/limit renderers are not included.

## LAW and validation examples

`MakePlots --plots-per-branch 32` groups requests into one reusable batch per
branch; its default remains one request per branch for compatibility.
`ManifestPlotTask` submits all region-expanded requests as a batch. Its
configuration accepts explicit `{region}` placeholders in legacy directories or
sparse selections; it does not guess a region's axis role from its name. Failed
renders fail the task after its provenance is written.

Generate the reproducible gallery, including a matplotlib reference:

```bash
python docs/examples/plotting/generate_gallery.py --output build/plot-gallery \
  --renderer build/core/rdf-plot --matplotlib
python docs/examples/plotting/benchmark.py --plots 1000 --matplotlib 20
ctest --test-dir build -R 'PlottingUtilityTest|PythonPlottingBindingsTest' --output-on-failure
```

The benchmark separates warmup, preparation, layout, and painting/export; reports
persistent object/buffer growth and RSS; and compares sessions against repeated
one-shot calls. Its matplotlib comparison is illustrative rather than an exact
style/content match. The C++ tests also compare a resized/reused PNG against a
fresh-session PNG to detect stale raster geometry, in addition to numerical,
ownership, threading, schema, and failure-recovery tests.
