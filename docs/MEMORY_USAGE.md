# Memory use in RDFAnalyzerCore

Event processing streams the input. That bounds the live event data, but the
process also holds the RDF graph, generated code, correction payloads, input
caches and histogram accumulators. These are separate memory costs.

## Keep systematic graphs small

The framework propagates registered variations by creating explicit RDF
columns. A derived variable with 60 input variations can therefore create 61
definitions. Each definition retains dependency information and per-slot
results. In the ROOT 6.40 implementation used for the investigation, column
registers use copy-on-write vectors: a sequence of many definitions can retain
many growing registry snapshots. Graph overhead can grow much faster than the
number of observables. It is paid before processing events.

Use ordinary `Define` for quantities that should follow shifted kinematics,
including the nominal event weight. For a terminal, explicitly varied weight,
use `DefineNominal`, then register its variation mapping. This avoids creating
unused combinations such as a pileup-up weight evaluated under every JES
source. Only use this when the consuming workflow varies one nuisance at a
time; simultaneous variations require their corresponding computations.

`CorrectionManager::applyCorrections` and `applyCorrectionsVec` share numeric
feature packing within one call. They accept an optional propagation flag per
output. For scalar SFs, `{true, false, false}` propagates object shifts through
the nominal SF and computes the explicit SF up/down at nominal inputs. Defaults
preserve the single-output API's propagation behavior. Sharing across arbitrary
calls is deliberately avoided: an intervening `Redefine` changes a reader even
when its column name stays the same.

Vector concatenation, row-major correction/JER inputs, generic weight products
and region conjunctions use one typed definition instead of append chains.
Factories are cached by actual input types and requested layout, independently
of column names. Small common products and vector packs retain native compiled
paths to avoid JIT setup for trivial jobs. Packed vectors still hold actual
values; they are not views into buffers whose lifetime could end early.

Histogram booking shares identical axis packs within each booking batch and
avoids scalar-to-vector broadcasts for scalar observables. Collection and
region multi-fill paths keep their required broadcasts. The cache resets
between booking calls so new histograms see intervening redefinitions.

## Bound event-loop and output memory

- Match ROOT `threads` to allocated CPUs. Object results, histogram accumulators
  and input caches can multiply with the number of processing slots. A
  single-CPU worker should explicitly use `threads=1`.
- Save sparse response histograms directly. Histogram memory depends on bin
  occupancy and slot count, not just the current event. Sparse storage can grow
  as new bins are filled; dense storage allocates the full Cartesian product.
- ROOT sparse accumulators use slot zero as the published result. Finalization
  merges other slots into it and releases them, avoiding a full copy of slot
  zero. Dense and Boost accumulators release their storage after conversion.
- The dense-storage heuristic permits up to 64 MiB **per histogram**, including
  all its slots. Booking many individually small histograms can still add up.
  Use `histInfo(..., binEdges, true)` to force sparse storage for lightly
  occupied large axes. Sparse storage does not cap a fully populated histogram.
- Set an explicit skim column list through `saveConfig`, or disable skimming
  when only histograms are needed. Saving every intermediate systematic column
  adds branch/basket buffers and unnecessary output I/O.
- `treeCacheSizeMB` bounds the requested input tree cache in MiB. The optional
  next-file prefetcher holds at most two warmup files, each limited by the
  current tree-cache size and `xrdPrefetchCacheMB`; it is not an event store.
  Warmup is disabled under implicit multithreading. Cache buffers and ROOT's
  internal read buffers still count toward process RSS.

CorrectionManager reuses payload sets by canonical path within the manager.
Weight audits and counters use aggregate actions rather than retaining event
weights in `Take` arrays. The vjet production path does not call RDF `Cache` or
collect all processed events into an in-memory table.

## Measure setup separately from event processing

Compare identical inputs, configurations, slot counts and output actions using
preserved before/after executables. Record RSS after correction setup, graph
construction, histogram booking and execution. Repeat with substantially more
entries to distinguish fixed graph cost from histogram occupancy or event
retention. Compare all stored bin contents, sumw2, axes and systematic labels.
A short replay fitting into a worker's memory request is useful evidence, but
does not establish the maximum of every production sample.

The analysis-local investigations under
`analyses/vjet-njetpt/investigations/` include replay scripts and measurements.
Further large reductions would require a different systematic representation
(e.g. native ROOT variations or a variation bundle throughout reconstruction).
That changes filtering, custom mappings, histogram packing and skim behavior
and needs a separate compatibility implementation. The current optimizations
retain the explicit-column model.
