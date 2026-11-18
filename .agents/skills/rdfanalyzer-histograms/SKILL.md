---
name: rdfanalyzer-histograms
description: Book, inspect, or modify NDHistogramManager outputs, THnSparse/dense backends, region/systematic axes, histogram schemas, and compiled sparse projections.
---

# Histograms and sparse schemas

For persisted category indexing use fixed-size helpers in
[CartesianBinning.h](../../../core/interface/CartesianBinning.h) and
[category_layout.py](../../../core/python/category_layout.py). Their shared
contract is last-axis-fastest indexing with an explicit ID base. Category
codes, failure cells and source-bin policy remain caller-owned.

## Trace booking to stored axes

Read [NDHistogramManager.h](../../../core/plugins/NDHistogramManager/NDHistogramManager.h), [implementation](../../../core/plugins/NDHistogramManager/NDHistogramManager.cc), and [CONFIG_HISTOGRAMS](../../../docs/CONFIG_HISTOGRAMS.md). Locate explicit booking and `bookConfigHistograms`, including any exclusion set. Define variables, weights, category columns, and filters before the booking that captures them. The `histogramConfig` key does not construct the plugin; `Analyzer::run()` saves the recognized `histogramManager` role.

Follow histogram `name`, variable/weight, numeric bounds or edges, channel, control-region, sample-category, and systematic axes into the output. Inspect `histogramBackend`, `histogramSystematics`, and per-histogram `nominalOnly` separately. Dense/legacy split objects and packed sparse storage need different readers. With RegionManager, overlapping membership can create multiple fills; check broadcasting and ordering of value/weight/category vectors.

The optional [Python ND facade](../../../docs/PACKED_ND_HISTOGRAMS.md) uses the
canonical manager lifecycle with explicit category axes and all/nominal groups;
it does not expose arbitrary continuous ND axes or inferred region expansion.
`histogramShareCollectionTransport` is a separate opt-in experiment, default
false: it shares exact-reference collection broadcasts/fill counts within a
booking call. Do not infer support for unequal systematic-slot collection
lengths from that cache. Check the linked guide and source for actual layouts.
`histogramCollectionLayout=per-slot` is a separate opt-in layout candidate. It
uses each slot's actual input lengths, requires object-valued weights/categories
to align exactly, and skips empty slots through a private action adapter.
`fixed` remains the default; implicit region/object expansion is unsupported in
the new path. Do not infer a speed improvement or production applicability from
its local correctness acceptance.

NDHistogramManager defaults to `histogramBackend=boost`,
`histogramBoostPrecision=float`, `histogramBoostSparseBackend=map`, and
`histogramDenseLimitMiB=0` (sparse). Explicit overrides retain ROOT, double,
flat maps and dense storage. `histogramDenseLimitMiB` bounds dense accumulation across all slots for one
histogram, not total process memory; zero selects sparse storage. Output stays
`THnSparseF` for both backends. ROOT and default Boost use float contents.
`histogramBoostPrecision=float` selects float contents
with double sumw2, dense separate arrays and ROOT-compatible axis lookup.
`histogramBoostSparseBackend` selects `map` or `flat` sparse containers. Test
cancellations and exact edges before switching backends. Double-precision
Boost-selected variable-width observables use the ROOT action. Set
`RDF_HISTOGRAM_PROFILE=1` for storage/finalization measurements.
Metadata saving warns once per histogram if slot-local counting allocators
show sparse Boost storage exceeding the equivalent dense arrays. The retained
samples cover pre-merge and slot-merge allocations, including nodes/buckets;
they exclude event-time rehash peaks, malloc overhead, ROOT output and process
RSS. See CONFIG_HISTOGRAMS for details; the warning does not migrate storage.

Read the embedded `__rdf_histogram_schema_v1` record before decoding ROOT object names, axis order, or display labels. Do not assume a prettified name is an actual key or silently project a different category. [extract_nd_histograms.py](../../../core/python/extract_nd_histograms.py) and [histogram_io.py](../../../core/python/histogram_io.py) provide Python readers; [histogram_io_bindings.cpp](../../../core/python/bindings/histogram_io_bindings.cpp) supplies compiled I/O/projection paths. Inspect availability in the current build before choosing a Python bin-by-bin loop.

Use [ColumnProjection](../../../core/interface/ColumnProjection.h), [CartesianBinning](../../../core/interface/CartesianBinning.h), and [RDFColumnPacker](../../../core/src/RDFColumnPacker.cc) for packing/flattening changes. Preserve axis order, under/overflow treatment, systematic selection masks, negative weights, and sumw2. Check changes against a small histogram with explicit expected bins and errors.

Legacy TH1 extraction uses the compiled `extract_sparse_slices` binding when
available and retains a Python fallback. Native sparse readers do not need
these duplicate TH1s. The vjet merge wrapper defaults to sparse-only output;
`--extract-histograms` restores legacy slices and `--nominal-only` still runs
schema postprocessing/extraction.

## Verify and route

Tests: [NDHistogramManager](../../../core/tests/cpp/testNDHistogramManager.cc), [config](../../../core/tests/cpp/testNDHistogramManagerConfig.cc), [projection](../../../core/tests/cpp/testColumnProjection.cc), [Cartesian binning](../../../core/tests/cpp/testCartesianBinning.cc), and [extraction](../../../core/tests/python/test_extract_nd_histograms.py). For presentation use [plotting](../rdfanalyzer-plotting/SKILL.md); for artifact manifests/merge compatibility use [outputs](../rdfanalyzer-outputs/SKILL.md); for shape/card construction use [statistics](../rdfanalyzer-statistics/SKILL.md).
