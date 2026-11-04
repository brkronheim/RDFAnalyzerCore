# Standalone XrdAdaptor input

RDFAnalyzerCore reads `root://` URLs through its XrdCl-backed `XrdAdaptor` by
default. The build option `RDF_ENABLE_XRD_ADAPTOR` is ON by default. Set
`rootInputBackend: root` to select ROOT's normal `TFile` reader for a job, or
configure with `-DRDF_ENABLE_XRD_ADAPTOR=OFF` to build without the adaptor.
When the adaptor is built and `rootInputBackend` is omitted, the framework
selects it automatically. The backend starts a second XrdCl connection as soon
as the primary file opens, retries source discovery at the configured interval,
and discovers up to three distinct replica sites.
For each read it sends the complete requested ranges to up to two quality-ranked
sites in parallel. The first successful response supplies ROOT's buffers;
the other full read drains in the background for site timing. An untested
third site can receive a bounded asynchronous exploration sample.
After a read wins, the adaptor does not start another full copy solely to
collect timing data. Already running losing requests still drain safely.
Overlapping ROOT file objects for the same URL share a live transport, while
keeping their own file cursors; closing one does not stop its peers.

The first alternate source is requested from the same redirector with the
current data server in XRootD's `tried` list. The implementation is local to
RDFAnalyzerCore and does not require CMSSW libraries at runtime.

## Build

The default project configuration builds the backend:

```sh
cmake -S . -B build
```

To opt out explicitly:

```sh
cmake -S . -B build -DRDF_ENABLE_XRD_ADAPTOR=OFF
```

The build needs the XrdCl development headers and `libXrdCl`. CMake selects
the headers and client library from one CMS EL9 CVMFS XRootD release so they
stay version matched. `env.sh` selects the same release at runtime for the
analysis and XRootD helper tools. This avoids the system XRootD 5.9.6 client
seen on some EL9 hosts, which can crash during library initialization. Set
`XROOTD_ROOT` or `XRD_ROOT` to a different XRootD package prefix to override
the CMS CVMFS package.

## Configuration

```yaml
rootInputBackend: xrd-adaptor
xrdSourceProbeIntervalSeconds: 5
xrdPrefetchCacheMB: 32
xrdPreferredSiteBandPercent: 20
xrdExplorationSharePercent: 10
xrdMaxReadSources: 2
xrdReadTimeoutSeconds: 120
xrdOpenTimeoutSeconds: 30
treeCacheEnabled: true
treeCacheSizeMB: 32
treeCacheAsyncPrefetch: true
treeCachePrefetchNextFile: true
```

`treeCachePrefetchNextFile` is for sequential, single-threaded chains. After
ROOT has learned the current tree's cached branches, RDFAnalyzerCore opens the
next file in the background, enables those branches, and reads entry zero to
prime that file's first TTreeCache cluster. The warmup also attempts to queue
ROOT's matching asynchronous open, but the XrdAdaptor may report `not-async`;
the active TChain can then still open its own file synchronously at the
boundary. Successfully read XrdCl ranges are retained in a bounded, URL-keyed
cache shared by the warmup and active file handles. This includes ROOT cache
reads that arrive with a destination buffer as well as explicit asynchronous
prefetches. If the warmup is still opening at the file transition, the active
reader joins that in-flight open. If its data read is still running, normal
reads can use ranges already available. The lookahead repeats at each
file boundary after the active file's branch set has been resolved.

Both cache-size defaults are 32 MiB. Explicit values in saved or generated
configs take precedence; rebuilding does not rewrite existing worker configs.
Next-file warmup remains available with the smaller caches.

Each handoff cache also caps its bookkeeping at 4096 entries, preventing tiny
metadata reads from accumulating map/vector allocation overhead below the
payload-byte limit.

Private vectored-read buffers use a shared 64 MiB budget across all adaptor
readers and sites. Requests are processed in windows of at most 8 MiB;
additional full reads wait for budget availability and optional exploration
samples are skipped when it is exhausted. A losing request retains its
reservation until all its callbacks complete, even if its reader has closed.
Completed losing buffers are released immediately. Successful buffers are
shared with the handoff cache rather than copied into another allocation.
Stopping a reader releases its cache ownership even if its closed ROOT file
object remains alive; active peer readers and in-progress windows retain their
own shared references.
These bounds cover adaptor-owned read payloads, not ROOT's destination buffers,
per-URL handoff caches, XrdCl connection buffers, or allocator-retained memory.
Large requests can require more network round trips than a single full vector;
measure throughput as well as RSS for remote workloads. Windows are visited
from the end of the request so the earliest ranges remain newest in the
handoff cache, preserving next-file warmup priority.

`xrdPrefetchCacheMB` caps the temporary warmup ROOT cache and each URL-keyed
Xrd handoff cache. The active file and lookahead file can be live at the same
time, so their separate caches can use memory concurrently. The source probe
interval can be set to zero to disable alternate-source discovery while
keeping XrdCl as the read backend.

`xrdOpenTimeoutSeconds` sets the per-request timeout for initial open/stat and
replica discovery opens. It defaults to 30 seconds so an unresponsive endpoint
can reach the existing fallback redirectors sooner. `xrdReadTimeoutSeconds`
sets the vectored-read timeout and defaults to 120 seconds. Both accept values
from 1 through 300. When a vectored read times out, that replica is
treated as failed for the current file and the manager advances to another
discovered site. At most `xrdMaxReadSources` full reads are in flight at once;
the timeout is applied to each site request, not to the complete `LoadTree`
call.
Under ROOT implicit multithreading, the next-file warmup is disabled because
this lookahead currently assumes one sequential chain reader.

The XrdAdaptor log identifies each source-manager handle and its primary data
server. Replica-probe lines show the hosts excluded from the next redirector
request and the server returned. `full read queued` and `full read result`
identify complete copies, including which result won and whether the loser's
data was used. `exploration sample` identifies the optional third-site sample.
Handoff-cache hits are marked as requests that required no replica read.

Each opened server is assigned an XRootD `sitename` using the server's Config
query. If that query is unavailable, its hostname domain is used as the site
key. Site quality is shared across files for the lifetime of the process. The
monitor keeps five one-minute weighted averages (weights 16, 8, 4, 2, 1), with
a 260 ms initial value, matching CMSSW's XrdAdaptor thresholds. Since this
backend receives vectored reads of varying sizes, each elapsed-time sample is
scaled to a 256 KiB equivalent for larger vectors; smaller reads use their raw
response time so tiny metadata reads do not get extrapolated to a fictitious
multi-minute delay.

A site is avoided for the current file after a read failure, a rolling quality
above 5130 ms, or a quality above 260 ms and more than four times a successful
peer site's quality. The monitor keeps a poor site as a last-resort source when
no better source is available. When a site is marked poor on two distinct file
paths, it is blacklisted for normal selection for the rest of the process.
Blacklisted sites are retained as last-resort sources if discovery finds no
other usable replica within 30 seconds, following CMSSW's rule to keep a sole
source active rather than fail the file open. Logs include `site quality
warning` and `site blacklisted` lines when those decisions are first made;
`site-blacklisted-fallback` identifies the last-resort case. Repeated redirector
responses for an already-tried host are backed off for 30 seconds.
The monitor retains at most two poor-file paths per site, enough to establish
the process-wide blacklist; the summary's `poor_files` count saturates at two.
The server-to-site lookup cache retains at most 256 hosts; evicted hosts are
queried again, while accumulated site quality and blacklist decisions persist.
Site-name requests use a one-second transport timeout matching the caller's
wait, so abandoned queries do not retain their handlers for the normal read
timeout.

Site selection adapts across files. A site enters the preferred set after at
least three successful read observations and 768 KiB of successful data, when
its rolling quality is within `xrdPreferredSiteBandPercent` of the best
measured site. The default band is 20%. A preferred site stays preferred until
it falls more than 10 percentage points outside the current band; blacklisting
or crossing the existing poor-quality threshold removes it immediately. The
best preferred sites are selected first for two complete reads, each using
separate buffers. The first successful complete read is copied to ROOT; the
other request remains in flight and contributes only its eventual timing and
success result. XrdCl has no per-`VectorRead` cancellation call, so losing
requests are allowed to finish. Up to two full reads per site can be in flight
process-wide, allowing the active file and next-file warmup to use the same
site concurrently. Once both per-site slots are occupied, further requests to
that site wait for a slot; the `site-full-read-limit` diagnostic is emitted at
most once per file handle and site. Discovery continues while a primary read
is pending, allowing a newly found second site to join the race before the
primary finishes.

When a third distinct, healthy site has insufficient measurements, it receives
a duplicate sample up to `xrdExplorationSharePercent` of the request and capped
at 2 MiB per vectored read (default 10%). Samples use separate buffers and
complete asynchronously. They use whole 512 KiB chunks, so small reads may not
produce a sample. Only one sample per site can be in flight. Set the exploration
share to zero to disable sampling. If both full reads fail, another discovered
source is promoted to a full retry.

`xrdMaxReadSources` controls the number of full-read sites; it accepts `1` or
`2` and defaults to `2`. With the default exploration share, discovery also
keeps a third distinct site available for samples. Set `xrdMaxReadSources: 1`
to use a single full read; an untested second site can still receive samples.
Set `xrdExplorationSharePercent: 0` to disable the third-site sample. These
settings do not affect the initial ROOT file-open redirect.

Initial file open and stat use up to three attempts, each with a fresh
XrdCl handle and the configured `xrdOpenTimeoutSeconds` request timeout. Retries
wait one and then two seconds. Public CMS dataset paths (`/store/mc/`,
`/store/data/`, `/store/relval/`, `/store/himc/`) can fall back through other
CMS global/regional redirectors. Site-routing prefixes are removed for those
fallbacks so they can select another replica. Other namespaces, including
private `/store/user/` and `/store/group/` paths, retry their original URL.
Open/stat failures and attempt numbers are logged even in Condor jobs. If all
attempts fail, the job still fails; no input file is skipped. Replica discovery
continues from the URL that successfully opened the file.

At normal process exit, the backend prints a site ranking from lowest to
highest rolling quality, with preferred and blacklisted flags, failed read
count, poor-file count, sample count, and successful bytes. `read_errors`
counts failed read requests attributed to a site; redirector/open errors that
occur before a data server is identified remain in the per-probe logs and
cannot be assigned to a site.

ROOT's cache size and asynchronous prefetch settings remain in effect for the
active file. The XrdAdaptor backend does not change the framework's threading
configuration.
