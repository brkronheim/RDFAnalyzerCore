# Plotting validation and measured performance

The implementation was exercised with ROOT 6.40.04, GNU 11.5.0, a Release
build, and the repository Python 3.12 environment. The reproducible gallery
generator covers two/four-panel distributions, blinding, log scales,
categorical labels, curve comparisons, and signed matrices. PNG, PDF, and SVG
exports were inspected; legends default inside the main frame with reserved
headroom. CMS and ATLAS examples are in the gallery, and tests also exercise
LHCb, ALICE, and custom headers.

The header strip now contains only experiment/status/luminosity/energy. Selection
titles and annotations appear in the upper-left of every plot, including response
matrices, without a surrounding box. Subplot margins reserve space in pixels for full endpoint labels.
Standalone `.C` macros are replayed in fresh ROOT processes by the binding tests,
which check annotation placement, y-axis padding, matrix ranges, palette
presence, and omission of blinded observations. The gallery exports macros
alongside its images.

The focused CTest targets are `PlottingUtilityTest` and
`PythonPlottingBindingsTest`. Coverage includes numerical normalization and
variances, correlated nuisance cancellation, sparse selections/migrations,
aligned rebinning, blinding, safe preparation workers, process renderer error
propagation, multi-format export, object reuse, and recovery from bad requests.
Tests compare both directions of categorical/numeric canvas reuse against fresh
PNG output. The session suite was also built and run with AddressSanitizer
(`detect_leaks=0` because ROOT keeps process-global caches).

The LAW plot/manifest suites (8 cases) and stitching plotting suite (9 cases)
pass when run directly with `unittest`. Their existing CTest wrappers invoke
pytest, which is absent from the repository virtualenv. The broader,
pre-existing `PythonBindingsIntegrationTest` fails in its event-input fixture:
the installed uproot writes `Events` as `ROOT::RNTuple`, while that test uses
the framework's TTree input path. This failure is outside plotting; the event
fixture and analysis lifecycle were not changed.

## Reuse benchmark

These measurements precede the annotation-box/macro-export layout update. Rerun
the benchmark for current layouts; its workload explicitly exports only PNGs.

`benchmark.py --plots 1000 --single-shots 20 --matplotlib 20` produced:

| Measurement | Result |
| --- | --- |
| Warmed session, 1,000 PNG exports | 54.11 s, 18.48 plots/s |
| New framework drawing objects after warmup | 0 |
| Drawing-buffer growth events after warmup | 0 |
| RSS before / after the 1,000-plot loop | 300,572 / 300,576 KiB |
| Cumulative preparation / layout | 0.018 / 0.136 s |
| Painting and PNG export | 53.93 s |
| New renderer, repeated one-shot calls, 20 plots | 1.07 s |
| Illustrative matplotlib reference, 20 plots | 1.89 s |

The matplotlib reference has similar dimensions and panels but different
styling/content, so this is not an exact backend comparison. Reuse establishes
stable object/storage behavior; it does not materially change PNG throughput in
this small workload, where painting and encoding dominate. ROOT can still
allocate internally. These counters do not claim zero total heap allocations.

For an identical batch of 128 distinct PNG outputs, the compiled CLI measured
9.29 s in serial painting/export versus 3.04 s total wall time with four fresh
renderer processes (including their startup). The parent report preserves
request order and all outputs were verified. Each process has its own ROOT
memory footprint; worker counts should be chosen for the host and workload.
The process measurements and the 1,000-plot test used generated in-memory
histograms; file I/O and larger matrices/systematic sets can have different
bottlenecks. Timings are observations from this environment, not performance
guarantees.

Generated artifacts and full machine-readable reports are under
`build/plot-gallery/`, `build/plot-benchmark/`, and
`build/plot-process-benchmark/`. They can be regenerated without modifying
either analysis checkout.
