# ROOT input caching

Enable the single-threaded input policy in the executable's normal text or flat
YAML configuration (including a LAW submit-config template):

```yaml
threads: 1
treeCacheEnabled: 1
treeCacheSizeMB: 128
treeCacheAsyncPrefetch: 1
treeCacheAsyncOpenNext: 1
treeCacheExplicitBranches: 1
treeCacheVerbosity: 1
treeCacheStats: 1
```

The same names work with `key=value` text configuration. The policy is **opt-in**
to preserve existing memory budgets and ROOT process settings. When enabled,
the defaults are 128 MiB, asynchronous prefetch on, next-file asynchronous
open off, explicit branches on, verbosity zero, and statistics on. Statistics
are independent of optimization;
existing configurations also report reads without `treeCacheEnabled=1`. `treeCacheSizeMB` accepts 0–512 MiB (zero
removes the cache). Existing factor-based `rootTTreeCacheSize` /
`ROOT_TTREECACHE_SIZE` settings remain alternatives to the fixed size.
Actual memory can exceed the requested cache size because asynchronous reading
uses additional buffers; measure resident memory as well as throughput.

The policy applies to the primary input chain, regardless of local or remote
protocol. It does not enable IMT or parallel decompression. If IMT is already
active, the policy reports a fallback and leaves ROOT's existing behavior in
place. Friends keep their existing cache behavior, including indexed access.
Disabling `treeCacheEnabled` restores the ordinary ROOT cache policy and preserves
the existing independent cache controls; it does **not** mean disabling ROOT's
own cache. To benchmark entirely uncached reads, also set `treeCacheSizeMB=0`.

`treeCacheAsyncOpenNext=1` opts into opening one upcoming XRootD file while the
current file is being processed. It uses ROOT's `TFile::AsyncOpen`; when the
chain reaches that URL, ROOT adopts the queued request. This overlaps file-open
latency only: tree metadata and event baskets are still read as the chain
switches files. It is disabled under IMT and does not apply to friend chains.
If processing stops before a queued file is reached, ROOT keeps that request in
its pending-open list for a later matching open.

## Architecture and dependency resolution

`makeTChain()` constructs one primary chain from `fileList` or directory discovery,
with optional friend chains. `DataManager` constructs RDF over that primary chain
and applies `firstEntry`/`lastEntry` as an initial `Range`. Providers and plugins
expose raw `RNode`s, so there is no complete framework-owned graph of all
Define/Filter/action dependencies. `Analyzer::run()` invokes plugin `execute()`
hooks, then output sinks, histograms, and finalizers materialize results. Eager
Snapshot or actions on raw nodes can also run outside that method.

Rather than duplicate ROOT's graph parser, the policy uses the physical branch
proxies built by RDF's TTreeReader at loop startup. ROOT resolves callable input
lists, string expressions, aliases, redefinitions, actions, array count leaves,
and split-branch metadata. It adds those branch names with subbranches enabled,
then stops learning before evaluating events. Derived RDF names never reach the
cache. This also covers actions booked through raw nodes and new actions in
subsequent loops. The disabled baseline already benefits from ROOT's native
branch registration: this feature's throughput improvements come primarily from
cache sizing and asynchronous reads, not a new dependency-discovery algorithm.

`InputCacheChain` intercepts the public TChain cache calls to retain learning on
registration failures or a declared incomplete dependency set. Setting
`treeCacheExplicitBranches=0` retains normal learning while still accepting
ROOT's known branch registrations. A custom operation that reads TBranches
outside its declared RDF inputs must call
`provider.markInputDependenciesIncomplete("reason")` **before** running.
Ordinary RDF operations, including string expressions and raw RNodes, do not
need this call. No static analysis can discover hidden I/O in arbitrary C++
callables. In-memory frames have no input cache.

TChain itself transfers the cache and rebinds physical branch pointers when
changing files. The policy preserves ROOT's notification chain, applies
`TFileCacheRead::SetEnablePrefetching` to each current cache, and translates a
configured global entry interval into each file's local coordinates. It never
passes global chain offsets as local tree entries. With
`treeCacheAsyncOpenNext=1`, it also queues at most one next-file open after a
successful file transition. Without a configured range, ROOT retains control
of the range. No event data is copied by the policy.

The repository environment currently selects ROOT 6.40.04; CMake does not declare
a minimum ROOT release. The implementation uses public APIs and a compile-time
fallback for builds without the prefetch setter. Integration coverage is run on
6.36.08 and 6.40.04; other releases need the same tests before claiming compatibility. No
private RDF graph structures or version-specific JIT internals are used.

Relevant ROOT implementation/API references:

- [TTreeReader proxy setup and cache registration](https://root.cern/doc/v636/TTreeReader_8cxx_source.html)
- [TChain file transitions and cache rebinding](https://root.cern/doc/master/TChain_8cxx_source.html)
- [TFile asynchronous open API](https://root.cern.ch/doc/v640/classTFile.html)
- [Public asynchronous prefetch API](https://root.cern/doc/v636/classTFileCacheRead.html)

## Diagnostics

`treeCacheVerbosity=1` reports the requested size and observed cache state when
ROOT completes proxy registration: file name, actual buffer size, prefetch state,
learning state, registered branch count, dependency status, fallback reason, and
configured range. Level 2 also lists branch names and reports file transitions.
`treeCacheStats=1` is the **default**, even when `treeCacheEnabled` is absent or
false. Set `treeCacheStats=0` to silence automatic statistics. No optimization,
cache allocation, prefetching, or thread count is changed by enabling reporting.
The primary chain and friends report before file transitions/end of
input and at destruction (including jobs ending partway through a file).
Unchanged final snapshots are suppressed. The last report is normally near the
end of the executable's output, when the DataManager is destroyed.

Reports contain the file URL/backend, tree/file index, entries, last loaded entry,
file bytes and read calls, readahead overhead, average bytes per read, observed
wall time, calls/second and MiB/second. Rates include processing and graph setup
within the observed file lifetime; they are **not pure network bandwidth or
I/O-wait measurements**. File counters include metadata reads. The cache state
line also reports whether next-file asynchronous opening is enabled; verbosity
1 logs each queued URL and ROOT's async-open status.

When a cache exists, reports also include buffer size/payload, registered branch
count, learning/prefetch state, cached and uncached byte/transaction counts,
ROOT's efficiency and relative efficiency, secondary miss-cache efficiencies,
miss optimization state, learning interval, and prefetched blocks. ROOT's own
`Print()` report supplies additional diagnostics such as prefetch wait time when
available. Uncached transactions are not event-level misses, and ROOT's secondary
miss-cache efficiency is not the complement of the primary hit efficiency.
Exact basket hit/miss counts are not exposed by the public getters used here.

Cache counters are **cache-lifetime snapshots, not additive per-file totals**;
ROOT may reuse/reset cache state at transitions. Process-wide ROOT byte/read-call
counters are cumulative and include all ROOT readers, not just this chain.
There is no cache report when caching is disabled, but file counters still print.
Full branch names remain optional at `treeCacheVerbosity=2`.

With IMT, ROOT opens worker-owned readers: the supplied chain cannot expose their
individual caches. The log explicitly identifies the master-chain limitation;
process-wide read counters include worker I/O. Friend chains use reporting-only
wrappers, preserving their existing cache and indexed-access behavior.
Statistics output can affect tiny benchmarks. In-memory frames have no input
chain and therefore no reading report.

The existing standalone `treeCacheAsyncPrefetch` control sets ROOT's legacy
`TFile.AsyncReading` option. With `treeCacheEnabled=1`, the policy additionally
uses the per-cache public asynchronous prefetch API; it does not require changing
global `TFile.AsyncPrefetching` defaults for unrelated files.
`treeCacheAsyncOpenNext` is independent of basket prefetching and requires no
cache-size or branch-policy change.

## Optional benchmark

Build the microbenchmark explicitly (it is not a CI timing test):

```bash
source env.sh
cmake --build build --target benchmarkInputCache -j4
```

For a NanoAOD-like dataset, run the same expression in separate processes:

```bash
# Use the same local path or root:// URL in every configuration.
input='root://host.example/path/input.root'
for size in 64 128 256 512; do
  for mode in disabled explicit async; do
    enabled=1
    prefetch=0
    if [ "$mode" = disabled ]; then enabled=0; fi
    if [ "$mode" = async ]; then prefetch=1; fi
    cfg="/tmp/io-${mode}-${size}.txt"
    cat > "$cfg" <<CFG
threads=1
batch=True
treeList=Events
fileList=$input
treeCacheEnabled=$enabled
treeCacheSizeMB=$size
treeCacheAsyncPrefetch=$prefetch
treeCacheExplicitBranches=1
treeCacheStats=1
CFG
    build/core/tests/cpp/benchmarkInputCache "$cfg" 'Sum(Jet_pt) + MET_pt'
  done
done
```

Output includes wall and process CPU time, CPU/wall efficiency, events/second,
ROOT read bytes and calls, and a checksum to compare numerical results. Timing
covers loop startup/JIT and the event loop, excluding initial file discovery.
Use a sufficiently long dataset and repeat/reorder runs to account for warm
filesystem and remote caches. The disabled mode still uses ROOT's native branch
selection at the requested size; this makes the comparison honest about what
ROOT already provides. Also benchmark the actual analysis with these same keys:
a microbenchmark's compute/I/O balance can differ substantially. No remote
throughput improvement is assumed from correctness tests alone.
