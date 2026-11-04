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
