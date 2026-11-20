---
name: rdfanalyzer-input-io
description: Diagnose or develop ROOT input chains, InputCache, standalone XrdAdaptor source selection, XRootD runtime linkage, and remote read performance in RDFAnalyzerCore.
---

# ROOT input, caches, and XRootD

## Separate the layers

Trace [DataManager.cc](../../../core/src/DataManager.cc), [InputCache.h](../../../core/interface/InputCache.h), [InputCache.cc](../../../core/src/InputCache.cc), and [util.cc](../../../core/src/util.cc). Determine the actual URL, selected tree, backend, thread count, enabled branch set, and cache options from the generated config. For missing columns/trees also use [config/data](../rdfanalyzer-config-data/SKILL.md).

The standalone backend lives in [core/xrd](../../../core/xrd): `XrdTFile`, `XrdSourceManager`, and `XrdOpenRetry`. Read [xrd-adaptor](../../../docs/xrd-adaptor.md), [INPUT_CACHE](../../../docs/INPUT_CACHE.md), and their current implementation before recommending knobs. `RDF_ENABLE_XRD_ADAPTOR` controls the build; `rootInputBackend` selects runtime behavior. Inspect the configured build and worker library, not just the source default.

Framework defaults for `treeCacheSizeMB` (when the input policy is enabled)
and `xrdPrefetchCacheMB` are 32 MiB. Explicit saved settings override them;
rebuilding or restarting a monitor does not rewrite prepared worker configs.
Keep next-file warmup and cache sizing separate when tracing memory changes.
Overlapping RDFXrdFile handles share live read-only transports by URL and
configuration generation. The weak registry joins in-flight opens without
retaining idle readers; each ROOT file retains its own cursor. Closing a warm
file releases its ownership without stopping active peers. Config-backed
DataManager startup validates with `LoadTree(0)` rather than counting the
entire chain, leaving later file metadata lazy. `xrdOpenTimeoutSeconds`
defaults to 30 for open/stat/discovery; `xrdReadTimeoutSeconds` remains 120
for vectored reads. Both accept explicit values from 1 through 300.
Adaptor private read payloads have a process-wide 64 MiB budget and 8 MiB
windows; successful buffers can also be shared with the per-URL handoff cache.
`xrdReadPipelineDepth` defaults to 2 (1--4); depth 1 is serial. All submitted
windows drain before an error propagates or destination buffers return to the
caller. Logical cache priority is independent of completion order. Future-file
reads and backup copies leave 16 MiB and the second per-site slot for demand
reads; active ROOT asynchronous prefetch retains demand priority. The default
backup delay is `xrdReadHedgeDelayMs=50` (0 restores immediate racing).
`treeCacheOpenAheadFiles=2` retains at most two future transports without
additional trees or payload warmups; 0 disables early transport opens. This
requires the sequential next-file warmup path and is disabled under IMT.
Use `xrdReadDiagnostics=1` to expose successful request and scheduling diagnostics
on workers, where they are normally suppressed.
Initial file opens use bounded asynchronous metadata location queries, then
origin/live-ranked connection attempts (two concurrent, four distinct sites).
Metadata queries use NoWait throughout the recursive walk: cold federation
metadata can request a five-second wait that exceeds the three-second budget.
Results are always best-effort/partial; Open/Stat still validates candidates.
Partial location results suffice; losing attempts retain their own transports.
The defaults are a three-second locate deadline and an eight-second candidate
open/stat deadline. `RDF_XRD_LOCATE_TIMEOUT_SECONDS=0` restores ordinary global
opens. Retained located candidates seed later discovery before global fallback.
See [initial connection controls](../../../docs/xrd-adaptor.md#ranked-initial-connections).
Endpoints beaten in initial open/stat and full-read races enter a process-local
60-second cooldown immediately on the winner, before late callbacks finish.
Candidate/read selection and replica discovery skip them while a usable
alternative exists, retaining a cooled endpoint as a last resort. Identity is
hostname plus port (including advertised and known resolved endpoints), never
a site-wide or unresolved-global-redirector ban. History is capped at 512;
winning clears cooldown and late losers do not prolong it.
`RDF_XRD_ENDPOINT_COOLDOWN_SECONDS=0` disables the cooldown (maximum 3600).
Already staged production archives require a separate authorized refresh.

Open/stat starts and redirect-chain results remain visible without diagnostics.
An initial global destination is unresolved until XrdCl reports its host list;
do not attribute unresolved failures to a storage site. Historical preferences
are read by [XrdSiteRanking](../../../core/xrd/XrdSiteRanking.h), scoped by worker
origin, and replaced by live observations. Use the
[ranking tool](../../../core/python/xrd_site_rankings.py) to refine snapshots;
see [format and staging](../../../docs/xrd-adaptor.md#historical-site-preferences-and-open-attempt-logging).
Loaded priors are never exported as actual samples or blacklists.
The budget excludes ROOT destinations, handoff caches and XrdCl internals.
Poor-file history saturates at two paths per site and the host lookup cache
at 256 entries; site quality and blacklists persist. See
[reader memory tests](../../../core/tests/cpp/testXrdReadMemory.cc) for local
XrdCl transport, cache and budget coverage; these do not exercise federation
replica races.


For performance, distinguish ROOT TTreeCache learning/branch dependencies, next-file warmup, URL-keyed handoff caching, replica probing, full-read races, and exploration samples. Their memory and network costs overlap. Verify which lookahead behavior is disabled under implicit multithreading. Report event throughput, read latency, memory, and actual input conditions; changing thread count or a cache cap alone is not evidence of improvement.

For timeouts/failures, match source-manager handle, original URL, replica site, request type, and exact error. Per-request timeouts do not bound the whole event/file transition. Global fallback rules for public CMS namespaces must not be assumed valid for private user/group storage.

Read [xrd/CMakeLists.txt](../../../core/xrd/CMakeLists.txt) and [env.sh](../../../env.sh) for matched XrdCl headers/libraries and ROOT runtime paths. Use [production diagnosis](../rdfanalyzer-production/references/diagnosis.md) when the executable/library is staged. A read failure, worker crash, and failed output upload require different remedies.

## Verify

Tests: [InputCache](../../../core/tests/cpp/testInputCache.cc), [XrdOpenRetry](../../../core/tests/cpp/testXrdOpenRetry.cc), [ROOT environment guard](../../../core/tests/cpp/testRootEnvironmentGuard.cc), and [runtime refresh](../../../core/tests/python/test_refresh_xrootd_runtime.py). Local/synthetic tests do not validate remote replica behavior; when requested and available, use a bounded read with matching runtime, credentials, and URL, and state exactly what was exercised.
