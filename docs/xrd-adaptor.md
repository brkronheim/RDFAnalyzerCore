# Standalone XrdAdaptor input

RDFAnalyzerCore reads `root://` URLs through its XrdCl-backed `XrdAdaptor` by
default. The build option `RDF_ENABLE_XRD_ADAPTOR` is ON by default. Set
`rootInputBackend: root` to select ROOT's normal `TFile` reader for a job, or
configure with `-DRDF_ENABLE_XRD_ADAPTOR=OFF` to build without the adaptor.
When the adaptor is built and `rootInputBackend` is omitted, the framework
selects it automatically. The backend starts a second XrdCl connection as soon
as the primary file opens, retries source discovery at the configured interval,
and discovers up to three distinct replica sites.
For each read window it starts the best available quality-ranked site, then
permits a second full copy after a short hedge delay or a failure. The first
successful response supplies ROOT's buffers;
the other full read drains in the background for site timing. An untested
third site can receive a bounded asynchronous exploration sample.
After a read wins, the adaptor does not start another full copy solely to
collect timing data. Already running losing requests still drain safely.
Overlapping ROOT file objects for the same URL share a live transport, while
keeping their own file cursors; closing one does not stop its peers.

Initial opens first discover advertised replicas through bounded metadata
queries to the supplied redirector. Located candidates are ranked before opening;
subsequent discovery uses retained candidates before asking the redirector again
with the current data server in XRootD's `tried` list. The implementation is local to
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
xrdReadPipelineDepth: 2
xrdReadHedgeDelayMs: 50
xrdReadDiagnostics: 0
xrdSiteRankingFile: auto
xrdSiteRankingMaxAgeDays: 30
treeCacheEnabled: true
treeCacheSizeMB: 32
treeCacheAsyncPrefetch: true
treeCachePrefetchNextFile: true
treeCacheOpenAheadFiles: 2
```

### Ranked initial connections

For `root://` and `roots://` inputs the adaptor issues asynchronous `Locate`
metadata queries before opening the file. It follows up to 32 advertised
managers, with at most four queries in flight per file, and retains at most
256 online endpoints. Duplicate endpoints and pending/offline advertisements
are excluded. Hostname responses are requested for historical endpoint mapping.
Queries use `NoWait` to accept partial advertisements. Otherwise a cold lookup
can request a five-second server wait, which XrdCl immediately rejects against
the shorter request budget. Results are always logged as best-effort/partial.
The discovery deadline is three seconds. When two historically or currently
well-performing sites arrive, a 150 ms settling interval allows additional
results to arrive before selection. Partial discovery results are sufficient.

Candidates use the same origin-scoped historical and live scores as reads.
The adaptor tries at most four distinct sites and at most two concurrent
open/stat attempts, with a 100 ms backup delay. The first successful open **and
stat** wins. The candidate phase has an eight-second caller deadline, bounded
by `xrdOpenTimeoutSeconds`. Late callbacks own and close their transports;
the winning caller never waits for losing connections. Across files/lookahead,
at most 32 discovery/open operations can remain outstanding. Existing global
open/retry routing remains the fallback when discovery or candidates fail.
Located candidates also seed ordinary replica discovery after the initial open.

Library-level environment controls work with existing analysis executables:

* `RDF_XRD_LOCATE_TIMEOUT_SECONDS=3`: discovery deadline, capped at 10 seconds;
  `0` disables ranked initial discovery and restores ordinary global opens.
* `RDF_XRD_INITIAL_OPEN_TIMEOUT_SECONDS=8`: ranked connection deadline, capped
  at 30 seconds and bounded by the configured open timeout (minimum one second).

An endpoint beaten in an initial open/stat or full-read race enters a
process-local 60-second cooldown immediately when a winner is available, without
waiting for the losing callback. Both the advertised endpoint and a resolved
backend are tracked when known. The key is hostname plus port; file paths,
credentials, site names and unresolved global redirectors are not cooldown keys.
The bounded history retains at most 512 endpoints and is never exported as site
outage evidence or a persistent blacklist. Winning again clears the endpoint's
cooldown; late loser callbacks do not extend it.

Candidate selection and later replica discovery prefer healthy endpoints outside
cooldown. A cooled endpoint remains a last resort when no usable alternative
remains; failures of the alternatives cannot turn cooldown into a file-open
failure. Metadata advertisements remain available for reconsideration after
expiry. Full-read selection similarly retains a sole cooled source. Active
requests still drain safely. Global alternate discovery temporarily excludes
cooled hostnames only while a usable source is already available, preserving
ordinary global recovery when it is the last resort.

`RDF_XRD_ENDPOINT_COOLDOWN_SECONDS` sets the duration (default 60, maximum 3600;
`0` disables it). Flushed `endpoint-cooldown` records identify the endpoint,
reason and duration. This control affects newly started processes using the
updated library; it does not change already staged archives.

`locate-start`, `locate-result` (including partial status), and
`initial-selection mode=ranked|global-fallback` are always logged. Candidate
attempts use `role=ranked-primary`. Location advertisements do not establish
current read throughput; actual reads continue to refine the ranking.

To update an active LAW campaign's **idle, never-started jobs only**, preview:

```sh
python3 core/python/update_queued_xrd_runtime.py \
  --library build/core/xrd/libRDFXrdAdaptor.so /path/to/run
```

Add `--apply` to create a verified immutable archive under `runtime_patches/`
and edit the matching jobs' `TransferInput` using scheduler-side idle/start-date
preconditions. Previously patched jobs can be upgraded from an older immutable
archive; the tool also recognizes its own Condor `strcat` transfer expressions.
Original archives, executable/config payloads, running jobs, worker scripts and
monitor state remain intact by default. Receipts retain original job
ads and commands. Jobs that start before an edit are skipped; already spooled
or previously started jobs are skipped. No hold/release or resubmission is
performed, so active monitors can remain running. Future retry submissions made
from the original prepared artifacts retain their original runtime by default.

Add `--refresh-retries --apply` when future automatic retries should also use
the new library. This backs up and atomically replaces the original
`shared_inputs.tar.gz` and loose adaptor library, verifying every unrelated
archive member is unchanged. Transfers see a complete old or new archive;
active monitors, their state, worker scripts and existing worker processes are
preserved. This library-only operation does not require stopping the monitors.
It is distinct from the broader `refresh_xrootd_runtime.py` worker-script refresh.

### Historical site preferences and open-attempt logging

Open and stat attempts always emit flushed `open-start`/`stat-start` records,
including on Condor workers with `xrdReadDiagnostics=0`. They identify the
handle, attempt, original file path, role, endpoint, origin and epoch time.
`open-hop` and `open-result` retain XrdCl's redirect host list on success and
failure. Before a global redirect, the eventual storage endpoint is unknown:
the start record describes the endpoint actually supplied to XrdCl, and the
redirect path becomes available in its completion callback. We preserve native
XrdCl redirection/recovery rather than reimplementing redirects for logging.
If no destination/host classification is supplied, the result stays unresolved;
the ranker does not blame the global redirector as a storage site. Site names
come from previous endpoint identity or a hostname fallback until the server's
`sitename` can be queried. Query strings and credentials are excluded from the
logged file identity and redirect URLs.

Each process emits `context` and final `site-stat` records with its actual
observed read counters and rolling score. Loaded priors are not included in
those counters. These records are accepted alongside older text tables by:

```sh
python3 core/python/xrd_site_rankings.py /path/to/returned/run \
  --base extensions/cms/data/xrd_site_rankings.tsv \
  --output /path/to/new_site_rankings.tsv
```

Arguments may be log files, directories or quoted glob patterns. `--origin`
supplies an execution-origin identifier for legacy logs; explicit new worker
context wins. For legacy Condor logs the submit schedd and execution host are
read from matching event records. A submit schedd is only a proxy for origin;
use `--origin` for pools with a different egress identity. New workers use their
site tags (`CMS_LOCAL_SITE`, `GLIDEIN_CMSSite`, etc.) or hostname, never a frozen
submit-host geography. Unknown worker networks remain unseeded until profiled.
When no site tag is available, a Condor worker reads `Machine` from its own
`_CONDOR_MACHINE_AD`; this preserves the sampled cluster's identity when its
public hostname differs from the private Condor name. Other processes fall back
to their hostname.

The tool writes an atomic TSV snapshot plus a JSON evidence report. It
deduplicates overlapping paths and repeated Condor job IDs within an origin,
prefers structured summaries over duplicate legacy tables, and requires five
adequate job observations by default (`--min-jobs`). Adequacy requires three
successful requests and 768 KiB of successful read data. Scores are medians of
final rolling scores, not lifetime latency or RTT. Storage open/stat failure
fractions can raise a score to the observed fraction times 5130 ms; unclassified
redirector/discovery failures cannot. Open-only failures require independent
job support and may contribute even when a process crashes without a summary.
The base preserves other origins; qualifying new observations replace matching
origin/site rows, without counting inherited historical samples again.

Set the following **before generating new submission scripts** to ship a
custom snapshot through the shared submission backend:

```sh
export RDF_XRD_SITE_RANKING_FILE=/path/to/new_site_rankings.tsv
```

The backend uses an explicitly selected snapshot first, then the learned user
cache, then the optional CMS provider's bundled cluster-scoped seed. It embeds
validated bytes and their SHA-256 in each generated
worker script and materializes `rdf_xrd_site_rankings.tsv` after setup, so shared
archives, loose transfers and container workers need no extension checkout or
submit-side file. Already prepared/running scripts remain unchanged. This
mechanism performs no worker redirector preflight scans or WAN probes.

After `MonitorSkimJobs` verifies every job in a LAW Condor production (including
the inherited stitching monitor), it automatically refines the user cache.
The default path is `$XDG_CACHE_HOME/rdfanalyzer/xrd_site_rankings.tsv`, or
`~/.cache/rdfanalyzer/xrd_site_rankings.tsv` when `XDG_CACHE_HOME` is unset.
Future generated workers use this snapshot automatically. The bundled CMS seed
and already generated scripts/archives are unchanged.

The updater retains a registry of up to 32 completed runs from the last 30 days
and re-aggregates their available returned logs. Thus several small runs can
meet the five-job evidence threshold together. Failed attempts within a
successfully completed run remain useful observations, but incomplete/permanently
failed runs do not trigger learning. Cleaned-up logs are skipped; existing cache
rows remain available until their observation timestamps expire in the adaptor.
Repeated completion of an unchanged run is a no-op. A file lock and atomic
snapshot replacement prevent simultaneous monitors from losing each other's
origin profiles. Replayed updates do not add historical pseudo-observations.

Each successful production records the update result in
`xrd_site_rankings_update.json` in its run directory. Cache errors produce a
warning/receipt without invalidating verified physics outputs. Completion
checks are read-only; existing completed targets are not backfilled or rerun.
Runs with no site measurements leave the user cache untouched. This completion
hook applies to newly started LAW Condor skim/stitching monitors, not standalone
executables or unrelated LAW plotting/fitting tasks.

Controls:

* `RDF_XRD_SITE_RANKING_CACHE=/path/to/sites.tsv` relocates the learned cache.
  `none` disables both its automatic update and automatic selection.
* `RDF_XRD_SITE_RANKING_AUTO_UPDATE=0` disables learning while retaining reuse
  of an existing cache.
* `RDF_XRD_SITE_RANKING_FILE=/path/to/pinned.tsv` remains an explicit override
  for submissions; unset it to use the continually learned cache. The updater
  never overwrites a pinned file. `none` disables both rankings and learning.

For local execution, set the same environment variable or use the analysis
configuration `xrdSiteRankingFile: /path/to/file.tsv`. `auto`/an empty value use
the environment. `none` disables loading. `xrdSiteOrigin` or
`RDF_XRD_SITE_ORIGIN` selects an explicit execution-origin identity; otherwise
the hostname and snapshot aliases determine the scope. Only exact origin/site
matches apply. `xrdSiteRankingMaxAgeDays` defaults to 30 (0 disables priors;
maximum 365). Old scores weaken linearly toward the unmeasured default and
expire. Future-dated observations beyond five minutes are ignored.

Historical scores order cold sources alongside the default score of untested
sources. Each prior has the weight of one observation and is completely replaced
after three actual observations; established live preferences take precedence.
They never seed process blacklists or pretend that a replica exists. Exploration
and last-resort fallback remain available. Ranking cannot steer the initial
global `Open` before discovery has revealed available sites. A change to initial
routing would also require per-file replica information.

The transport format starts with `# rdf-xrd-site-rankings-v1` and has records:

```text
alias WORKER_HOST ORIGIN
endpoint STORAGE_HOST SITE
site ORIGIN SITE QUALITY_MS ADEQUATE_JOBS OBSERVED_UNIX
```

Fields are separated by whitespace; generated files use tabs. Duplicate or
malformed records are rejected rather than partially loaded. Alias resolution
is one level. Ambiguous worker aliases are omitted. Endpoint mappings identify
hosts for diagnostics; they do not rewrite URLs or select nonexistent replicas.

`treeCachePrefetchNextFile` is for sequential, single-threaded chains. After
ROOT has learned the current tree's cached branches, RDFAnalyzerCore opens the
next file in the background, enables those branches, and reads entry zero to
prime that file's first TTreeCache cluster. The XrdAdaptor warmup bypasses
ROOT's asynchronous-open mechanism and shares the live transport with the
active TChain's independent ROOT file object. Successfully read XrdCl ranges
are retained in a bounded, URL-keyed
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
`xrdReadPipelineDepth` permits 1 through 4 windows in flight per logical read
(default 2). Depth 1 restores serial window execution. Submitted windows are
always drained before returning or propagating an error, so no callback writes
the caller's destination after the read returns. Windows retain their logical
handoff-cache priority regardless of completion order; the earliest requested
ranges remain newest. The 64 MiB private-payload budget is unchanged.

Future-file warmup has background priority, including reads issued by ROOT's
prefetch helper for the warm file. Active-file asynchronous prefetch keeps
demand priority. Background and backup requests cannot allocate the last
16 MiB of the shared budget or occupy the second per-site full-read slot.
These are non-preemptive limits: requests already submitted must drain safely.
This reserves capacity for active reads without increasing the payload budget.

`xrdReadHedgeDelayMs` controls when a second full replica read joins a pending
read (0 through 1000 ms, default 50). A failed first read permits immediate
replacement. Zero restores immediate racing. Successful reads do not launch a
late backup just to collect timings. Already submitted losing requests retain
their buffers and reservations until their callbacks finish.

`treeCacheOpenAheadFiles` opens and retains transports up to two files ahead
by default (0 through 4; 0 disables these additional opens). This applies only
to the sequential XrdAdaptor path with `treeCachePrefetchNextFile` enabled.
It does not construct extra ROOT trees or prefetch additional file payloads:
data warmup still targets only the immediately following file. Outstanding and
retained lookahead opens are capped by this setting. Files reached before an
open completes join the existing pooled open. IMT disables this lookahead.

Set `xrdReadDiagnostics=1` for per-request and replica diagnostics on workers;
normally worker logs suppress successful low-level adaptor messages. Scheduling
diagnostics include memory-budget and per-site slot deferral counts. Collect
these selectively when measuring contention and duplicate traffic.

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
best preferred sites are selected first for up to two complete reads, each using
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
