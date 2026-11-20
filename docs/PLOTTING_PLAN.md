# Plan: extend PlottingUtility for reusable, fast physics plotting

This document records the original design. The implementation and current
capabilities are described in [Physics Plotting](PLOTTING.md). The persistent
session, rendering/input layers, bounded workers, headers, CLI, Python/LAW
integration, gallery, regression coverage, and optional compiled renderer process
pool are implemented. Specialized unbinned plot families remain extensions.
Extend `PlottingUtility`, preserving its existing C++,
Python, and LAW entry points. Add a persistent session that owns reusable drawing
objects and separates input preparation, physics calculations, rendering, and
export. Keep analysis-specific grouping, selections, fits, and response decoding
in analysis adapters.

## Evidence from the current code

| Source | Behavior to preserve or reuse |
| --- | --- |
| `analyses/VH-RDF/plotZMuMu/plot.py:1377` | Ordered process stacks, separate signal display scales, linear/log output, four aligned panels: yields, S/sqrt(S+B), S/B, and data/MC. Region labels, luminosity, fitted normalization annotations, and blinding. |
| `analyses/VH-RDF/plotZMuMu/plot.py:1149` | Dataset/category aggregation and bin-edge validation; data category selection prevents duplicate data copies. |
| `analyses/VH-RDF/plotZMuMu/plot.py:1266` | Top/V+jets normalization fit to a reference Z-mass distribution. Supply its results to plotting; do not silently run this fit inside the generic renderer. |
| `analyses/vjet-njetpt/plot.py:986` | Two-panel stacks with separate statistical and asymmetric systematic bands, sumw2 data errors, categorical labels, and MC-only support. |
| `analyses/vjet-njetpt/plot.py:497` | Sum correlated nuisance variations across samples before combining independent nuisance envelopes in quadrature. |
| `analyses/vjet-njetpt/plot.py:698` | Fit-observable projections retain variances and distinguish absent variations from migrations to empty regions. |
| `analyses/vjet-njetpt/plot.py:783` | Signed response heatmaps with a diverging palette, long category labels, separators, and a color bar. |
| `core/interface/PlottingUtility.h`, `core/src/PlottingUtility.cc` | Existing requests/results, TH1F/TH1D conversion, stacks, ratios, pulls, ratio-summary and PCA helpers. Main implementation to extend. |
| `core/python/bindings/python_bindings.cpp:1429` | Existing public Python interface. |
| `core/python/law/plot_tasks.py`, `manifest_plot_tasks.py` | Existing workflow integration and provenance recording. |
| `core/python/stitching_plotting.py` | Independent MC ratio/pull calculations and axis-range behavior worth using as numerical references. |
| `core/python/extract_nd_histograms.py`, `core/plugins/NDHistogramManager/NDHistogramManager.cc:1120` | Existing embedded sparse schema and legacy reconstruction semantics. |

`plots.h` primarily implements histogram accumulation, including per-slot
accumulators; `plotsNew.h` is an accumulator prototype. Neither provides the
persistent rendering infrastructure needed here. Prior renderer work is already
in `PlottingUtility` (including history commits `3a7ab6ea24`, `fa2f49fe46`, and
`04334d1093`).

The VH script currently retains counts without MC sumw2 in `PlotValues` and uses
sqrt(data) errors; vjet retains variances. Provide both error policies explicitly,
and preserve variances in the new common representation. Do not interpret the
VH reference fit as a full postfit prediction with propagated fit covariance.

The inspected stored VH image also has overlapping legend/caption text. Match
the useful style and information content while improving layout. Existing image
files are examples, not proof of what the current source renders.

## Problems to resolve in PlottingUtility

* `makeStackPlot()` opens a file and constructs every canvas, pad, histogram,
  stack, and legend again. Moving to C++ alone does not remove this work.
* Objects such as `mcUnc`, `mcRel`, `ratio`, `pullHist`, and reference lines leave
  scope before `canvas.SaveAs()`. ROOT drawing retains references; every visible
  object must survive painting and export. The pull pad is also drawn with the
  top pad current, and its geometry needs explicit parenting and panel bounds.
* Each data process replaces `dataHist` instead of summing multiple data inputs.
  Empty/data-only requests can reach `stack.GetXaxis()` without a valid stack.
* There is no explicit bin compatibility check, and missing/zero normalization
  counters silently fall back to the manual scale.
* The negative pull drawing uses `-v`, putting negative bars above zero. The
  relative-error ratio formula also loses numerator uncertainty at numerator=0.
* `makeStackPlots(parallel=true)` launches one `std::async` per request. Graphics
  preparation includes a `TLegend` outside the canvas mutex, while destruction
  also extends beyond it. Replace this with a clear concurrency boundary.
* The Python binding says plots are rendered concurrently; actual canvas work
  is locked. Update that documentation along with the implementation.
* Tests mostly check integrals, helper arithmetic, and nonempty image files.
  They do not establish that the exported panels and bands are present.

## Public interface and data flow

Keep the existing `PlotRequest`, `PlotProcessConfig`, `PlotResult`,
`makeStackPlot()`, and `makeStackPlots()` working. Add new fields at the end of
aggregate configurations, preserving existing initializer order. Preserve the
existing defaults where practical; document correctness fixes and any visible
layout changes. Introduce new capabilities through an explicit extended spec.

The proposed API adds a public `PlottingUtility::Session` with batch options and
a `renderBatch()` operation. A caller can construct the session on the stack and
reuse it across batches. Internally, use stable ownership for ROOT objects.
Existing one-shot methods delegate to a temporary session; the batch method
creates one session for the entire batch. Repeated one-shot calls cannot deliver
cross-call reuse, so migrate framework callers to a session or a batch call.

```text
File/schema adapters or caller-supplied arrays
                  |
        bounded preparation workers
                  |
       reusable prepared-data buffers
                  |
       one ROOT renderer on caller/main thread
                  |
          PNG / PDF / SVG + provenance
```

Use C++17-compatible views for borrowed arrays and explicit owning buffers at
asynchronous boundaries. A prepared payload contains edges, labels, yields,
sumw2, asymmetric errors, validity/blinding masks, process roles, nuisance or
covariance information, and display metadata. It has no canvas or pad pointers.
Caller-supplied data allows analyses to migrate rendering before migrating I/O.
All per-bin calculations and rendering run in C++; Python remains an optional
configuration and workflow front end.

Implement these layers within the PlottingUtility feature, splitting internal
files as needed rather than creating another competing plotting package:

1. **Input adapters:** legacy TH1 directories, embedded-schema THnSparse, and
   in-memory arrays/matrices. Accept multiple files per dataset and multiple
   datasets per display group. Select one representation when raw and extracted
   copies coexist. Resolve axes by metadata, validate their labels/edges, and
   avoid inferring physics from directory-name substrings in generic code.
2. **Preparation:** normalize, aggregate, rebin, form uncertainty bands, and
   compute requested diagnostic panels. Expose independent numerical functions
   with caller-provided output/scratch buffers; extend existing helpers with
   allocation-free variants while retaining their return-value APIs.
3. **Rendering session:** persistent panel layouts, process slots, graphs,
   axes, annotations, legend slots, and export handling.

## Allocation and object-reuse contract

Create one workspace per active layout within each session: a single-panel
layout, configurable aligned 1D panels, and a heatmap layout. A four-panel VH
plot is a layout configuration, not a separate physics-specific class.

* Construct canvases and pads once per layout. Keep drawing references attached
  where possible; update values, geometry, labels, limits, and visibility.
  Avoid per-plot `Clear()`, `Clone()`, `DrawCopy()`, and `DrawLatex()` factories.
  Register labels, colors, and reusable legend primitives during initialization.
* Plan bin/point/process capacities before a known batch; reserve numerical
  scratch space and drawable slots. Streaming workloads may grow capacities at
  explicit, instrumented boundaries. Hide unused slots and reset every mutable
  attribute, including log flags, masks, palettes, errors, and category labels.
* Reuse graph/step-polygon storage for filled stacks and asymmetric bands.
  ROOT's `THStack::BuildStack()` clones component histograms when rebuilding its
  cumulative stack. Prefer explicitly computed cumulative boundaries in the
  optimized path; retain THStack only if benchmarks justify it. Merely retaining
  a THStack wrapper would not meet the allocation objective.
* Use a persistent frame for continuous axes. If exact TH1/TH2 binning or
  categorical axes require reconfiguration, do it only on shape changes and
  report it. Reuse compatible buffers; do not cache one full workspace per
  histogram name. Bound any shape cache by memory and entry count.
* ROOT objects must have stable addresses and explicit owners. Use value members
  where lifetime rules permit and one-time `unique_ptr` allocations elsewhere;
  avoid storing drawable values in vectors that can relocate. Detach owned
  histograms from directories and detach pad references before destroying their
  owners. Audit pad cleanup flags and destruction order.
* A stack-allocated session still contains ROOT objects with heap-backed
  internals. Promise reuse of framework-owned objects and buffers after warmup,
  not zero allocations inside ROOT painters, text handling, image encoders, file
  decoding, or arbitrary changes of binning. Measure remaining allocations.

Process linear/log variants and multiple export formats from the same prepared
payload. Recompute physics only when its inputs change; preserve numerical
arrays when changing display scale or signal visibility multipliers.

## Physics and presentation coverage

| Capability | Initial implementation | Extension after parity |
| --- | --- | --- |
| Distributions | Data/background stacks, signal overlays, MC-only/data-only plots, independent curve overlays | Shape-normalized comparisons and configurable small multiples |
| Panels | Configurable ordered ratio, pull, S/B, and S/sqrt(S+B) panels | Fractional residuals, cumulative yields, alternative significance metrics |
| Uncertainties | sumw2, asymmetric data errors, separate stat/syst bands, supplied total covariance | Correlation views and supplied prefit/postfit predictions |
| Axes | Variable-width bins, categorical labels, linear/log scales, explicit ranges, density mode, flow policy | Secondary axes where scientifically justified |
| Matrices | TH2/array heatmaps, signed diverging colors, labels and separator lines | Row/column normalization, efficiency, purity, stability summaries |
| Other tasks | Generic supplied graphs and reference curves | Efficiencies with suitable intervals, profiles, ROC curves, cutflows, fit/residual overlays |
| Style/export | Reference-inspired palette, shared axis alignment, configurable panel heights, readable text, luminosity/energy/experiment labels, PNG/PDF/SVG | Reusable collaboration style presets |

Reserve separate space for title/luminosity and legend; support external legends
and label wrapping for crowded plots. Make typography consistent in physical
or pixel units across differently sized panels. The generic engine should not
assume CMS, a particular year, or a particular process vocabulary.

Define numerical policies before coding:

* Preserve signed weights and sumw2, including bins with zero yield but nonzero
  variance. Apply scale squared to variances. Aggregate all data contributions.
* Validate full bin edges and finite inputs; require explicit aligned-edge
  rebinning. Define fold/drop/show underflow and overflow. Density transformations
  divide variances by bin width squared; unit-area normalization must state how
  its induced correlations are treated.
* Keep background normalization, signal prediction normalization, and signal
  display multipliers separate. Renderer annotations accept externally fitted
  factors/covariance; the VH fit and process-flavor mapping stay in its adapter.
* For data/MC points, default to data-only error bars plus a separate MC band.
  For independent/correlated MC ratios use the direct variance expression:
  `Var(n/d) = Var(n)/d^2 + n^2 Var(d)/d^4 - 2 n Cov(n,d)/d^3`.
  This preserves uncertainty when the numerator is zero. Validate covariance
  inputs; do not silently repair materially invalid covariance matrices.
* Pull definitions must state included uncertainties and covariance. Local
  per-bin pulls and covariance-whitened residuals are distinct diagnostics.
  Invalid denominators produce a validity mask, not a fabricated zero point.
* Apply blinding masks to data points, ratios, pulls, auto-ranging, and exported
  visible summaries. Never turn a blinded bin into an observed zero.
* Correlated nuisance shifts are summed across samples/eras sharing a declared
  correlation identity before forming envelopes. Distinct identities combine
  independently or through supplied covariance. Support explicit missing-shape
  policies; distinguish an absent variation from a present shape with zero
  entries in a region. Keep stat and syst bands distinct unless combining is
  requested. Reuse existing manifest nuisance metadata where available.
* Provide signed-stack and signed-overlay policies for negative bins. Log plots
  mask/clip only the drawing with an annotation, retain original numbers, and
  handle no-positive-content plots. Mask invalid S/B or significance bins and
  label the selected significance approximation. Response matrices preserve
  negative values and distinguish yields from row/column-normalized matrices.
* Make normalization sources and units explicit: luminosity, cross section,
  efficiency, k factor, extra scale, and full/sign generator-weight denominator.
  VH and vjet currently choose different fallback/default policies; retain these
  in adapters or explicit configuration. New strict mode fails on missing or
  invalid counters; legacy behavior, if retained, emits a structured warning.

## Safe parallel execution

ROOT's thread-safety switch does **not** make drawing/printing canvases safe
across threads. Its ROOT 6.40 documentation explicitly excludes graphics.
Use the following boundary:

* Initialize ROOT/thread safety and warm required dictionaries, fonts, palettes,
  painters, and export plugins before starting preparation workers. Keep global
  style changes in the renderer; never toggle global registration/style flags
  from workers.
* Use a fixed, configurable preparation pool. Each worker owns its TFile handles,
  mutable ROOT inputs, and scratch space; use directory contexts and transfer
  plain buffers to the renderer. Never share mutable sparse histograms or axis
  ranges. Prefer one sparse traversal filling selected outputs over repeated
  projections/materializing every category combination.
* Perform all graphics object construction, mutation, drawing, export, and
  destruction on the caller/main rendering thread. Sessions are thread-affine.
  A process-wide coordination rule must also cover multiple utility instances;
  worker callbacks cannot invoke unrelated ROOT graphics concurrently.
* Bound open files, prepared-buffer bytes, queue depth, and cache memory. Recycle
  buffers after export. Use deterministic aggregation and stable result order.
  Avoid starvation when enforcing ordered consumption; reserve admission for
  the next required job or use a bounded reorder window.
* Map legacy `parallel=true` to bounded preparation, documenting the new
  behavior. Do not enable RDataFrame implicit MT for this, and avoid nesting
  thread pools with LAW workers.
* If export dominates, add an optional bounded pool of **fresh processes**, each
  running a persistent PlottingUtility session serially. Batch many plots per
  process to amortize startup and object creation. Use independent output paths,
  propagate errors, and cap process count by memory/storage throughput. Spawn
  workers before ROOT/thread initialization or use fresh executable launches;
  do not fork a live multithreaded ROOT application.

See [ROOT 6.40 thread-safety documentation](https://root.cern.ch/doc/v640/namespaceROOT.html),
[ROOT pad drawing/lifetime semantics](https://root.cern.ch/doc/master/classTPad.html),
[ROOT object ownership](https://root.cern.ch/manual/object_ownership/), and
[THStack implementation](https://root.cern.ch/doc/v640/THStack_8cxx_source.html).

## Delivery sequence and acceptance gates

1. **Baseline and correctness contract.** Capture representative two-panel,
   four-panel, log, categorical, and signed-matrix reference cases. Separate
   arithmetic expectations from visual style. Instrument current PlottingUtility
   and the two Python workflows on identical prepared data; do not assume that
   allocation is the dominant cost. Fix ownership, panel parenting, multi-data
   aggregation, validation, and pull/ratio defects with focused regression tests.
2. **Persistent session and fast preparation.** Add prepared arrays, reusable
   scratch buffers, persistent 1D layouts, and batch routing through one session.
   Preserve old API entry points. First gate: correct two/four-panel images and
   no reconstruction of framework-owned drawables for repeated compatible jobs.
3. **Reference parity and adapters.** Add masks, signal diagnostics, asymmetric
   bands, signed heatmaps, styles, and sparse input. Supply thin migration
   examples for VH and vjet; keep their physics choices and fits local. Cross-
   check sparse and extracted representations, migrations, and parent/child
   category exclusions against the existing analysis tests.
4. **Bounded parallel preparation.** Replace unbounded async, add buffer/file
   budgets and deterministic scheduling, then compare serial/parallel results.
   Gate optional process rendering on profiling evidence.
5. **Bindings, workflows, and broader task coverage.** Expose sessions and batch
   specs through pybind11, releasing the GIL only around C++ work that never
   calls Python. Have ManifestPlotTask submit a batch/session; provide chunked
   branches for MakePlots so reuse survives LAW task boundaries. Add a compiled
   batch CLI. Version the new spec and provenance; record input/config identity,
   normalization, warnings, and timings. Publish API/style examples and add the
   remaining generic graphs and diagnostic types incrementally.

Extend `core/tests/cpp/testPlottingUtility.cc` and the LAW/binding tests. Use
temporary output directories rather than writing generated plots into source
fixtures. Numerical cases must cover zero denominators/numerators, signed-weight
cancellation, multiple data samples, absent data/MC, incompatible edges,
blinding, covariance, correlated variations, and missing-versus-empty shapes.
Exercise alternating layouts, process counts, bin counts, labels, log flags, and
failure-then-success sequences to detect state leakage. Check drawable lifetimes
with sanitizers and check graphics are confined to the rendering thread.

Use a pinned ROOT/font environment for tolerance-based visual comparisons and a
gallery review. Verify actual panels/bands/labels, including legend collisions;
nonempty PNG files are insufficient. Test export failures and report success
only when every requested output is verified. Keep concurrency and numerical
tests useful when image plugins are unavailable.

Benchmark a short batch and at least 1,000 plots, fixed and mixed binning, few and
many processes, systematics, sparse inputs, and response matrices. Compare the
Python references, current utility, persistent serial session, bounded preparation,
and optional process rendering using equal dimensions, content, and output
formats. Record warmup separately, per-stage wall time, plots/second, allocation
counts/bytes, object-construction counters, peak RSS, open files, and throughput
versus worker count. Include preparation-only and render-without-export runs to
isolate encoding/I/O costs. Use optimized builds; the inspected build cache
currently selects Release and ROOT 6.42.00-rc1.

Acceptance requires correct numerical/visual parity (with documented fixes),
stable memory over repeated batches after warmup, bounded resources, and measured
object reuse. Set speedup targets after the baseline establishes the bottleneck;
report end-to-end and per-stage gains separately.

Inspection preserved the analysis worktrees. VH-RDF was at
`318524028ffc9c8e955fd766e50ecff3ec923e29` with existing untracked artifacts;
vjet-njetpt had no valid HEAD and its files were untracked. Migration examples
must not assume that changes inside either analysis appear in root git status.
