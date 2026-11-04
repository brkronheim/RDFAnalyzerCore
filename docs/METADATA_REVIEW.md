# Job metadata storage and merging review

Reviewed 2026-10-30. The historical inventory and prototype measurements below
motivated the implemented [metadata contract](METADATA.md). Production writers,
readers, LAW merges and the vjet wrapper now use that contract. Existing staged
jobs and saved analysis products were not modified. The original experiment is
in [`benchmarks/metadata_20261030`](../benchmarks/metadata_20261030/README.md).

## Recommendation

Keep numerical ROOT outputs, but give descriptive metadata a separate merge
operation. Validate shared definitions, write them once, reduce measured
statistics using their actual mathematical rules, and archive job provenance
in a compressed document with common fields factored out. A ROOT key cycle
must not be the mechanism for representing a job's identity or merge ancestry.

Start with the merger so existing job files benefit without reprocessing.
Then consolidate the per-job writer. Simply putting provenance into one JSON
ROOT object does not solve merge duplication: ordinary `hadd` can still copy
that object once per input.

## What was written before this change

`meta.root` is an artifact containing both physics histograms and metadata.
Removing everything other than the main analysis histograms would also remove
normalization and derivation inputs. These objects need distinct policies:

| Producer | Objects | Required treatment |
| --- | --- | --- |
| `NDHistogramManager::SaveHistsImpl` | `nd_histograms_v1/*`, `__rdf_histogram_schema_v1` | Add compatible histograms; validate schema and retain one copy. Keep schema in ROOT so standalone histogram readers still work. |
| `CounterService::finalize` | `counter_weightSum_*`, `counter_weightSignSum_*`, optional integer-variable weight/sign histograms | Add by matching sample and bin meaning. These are normalization inputs, not disposable diagnostics. |
| `CutflowManager::finalize` | `cutflow`, `cutflow_nminus1`, `cutflow_regions` | Add counts after validating cut order, labels and region meaning. |
| `TaggerEfficiencyService::finalize`, `TaggerWorkingPointManager::finalize` | Efficiency/category derivation histograms | Add the underlying count/weighted-count histograms with axis validation. Derive efficiencies after aggregation. |
| `StxsManager::finalize` | Auto-booked STXS histograms | Retain as numerical products with declared merge meaning. |
| `RegionManager::finalize` | `regions/<name>` as `TNamed` | Validate definitions within a compatible group and store once. |
| `WeightManager::finalize` | `weight_norm_*` as `TNamed` | Store by normalization/sample scope; equality-check within a scope. Do not sum normalization constants. |
| `WeightManager::finalize` | `weight_audit_*` four-bin histograms and negative/zero counts as `TNamed` | Use a typed statistical reducer; ordinary histogram addition is incorrect for three of the four bins. |
| `ProvenanceService::finalize` | One `TNamed` for each `provenance/*` entry | Deduplicate shared values and preserve job-specific values with explicit job/source identity. |

Source locations:
[`analyzer.cc`](../core/src/analyzer.cc),
[`ProvenanceService.cc`](../core/src/ProvenanceService.cc),
[`CounterService.cc`](../core/src/CounterService.cc),
[`NDHistogramManager.cc`](../core/plugins/NDHistogramManager/NDHistogramManager.cc),
[`WeightManager.cc`](../core/plugins/WeightManager/WeightManager.cc),
[`CutflowManager.cc`](../core/plugins/CutflowManager/CutflowManager.cc),
[`RegionManager.cc`](../core/plugins/RegionManager/RegionManager.cc),
[`TaggerEfficiencyService.cc`](../core/src/TaggerEfficiencyService.cc),
[`TaggerWorkingPointManager.cc`](../core/plugins/TaggerWorkingPointManager/TaggerWorkingPointManager.cc),
[`StxsManager.cc`](../core/plugins/StxsManager/StxsManager.cc).

`reportMetadata()` mostly reports to the logger. It should not be confused with
`finalize()` methods that actually open and update the ROOT file. Most of these
writers independently open the same file in `UPDATE`; the current
[`IOutputSink`](../core/interface/api/IOutputSink.h) supplies file resolution and
dataframe output, not a common object-writing or metadata-reduction contract.

## Observed duplication

The local 2024 vjet cache contains 288 `DATA_Muon_2024` job files. A representative
file is 691,319 bytes and contains 213 sparse histograms, 76 provenance entries,
one histogram schema and one normalization constant. Its compressed schema key
and payload occupy 28,109 bytes; the 76 provenance keys and payloads occupy
11,686 bytes. Directory tables, file headers and free space are additional.

The existing merged `DATA_Muon_2024_partial.root` is 11,314,303 bytes and already
excludes provenance. It nevertheless contains:

| Object family | Keys/cycles | Compressed key and payload bytes |
| --- | ---: | ---: |
| Histogram schema | 288 | 8,095,392 |
| Total normalization constant | 288 | 29,664 |
| Sparse histograms | 213 | 858,169 |
| Exported TH1 histograms | 2,711 | 1,918,203 |

All 288 schema strings are identical. Their repeated payloads alone are 71.6%
of this final file. This is independent of the cost of keeping both sparse
histograms and exported TH1 projections, which is outside this review's scope.

The existing analysis wrapper
[`mergeResults.sh`](../analyses/vjet-njetpt/mergeResults.sh) excludes only the
`provenance` directory and uses eight `hadd` workers for DATA. That avoids one
source of duplication but loses provenance from the merged artifact and still
copies the schema and normalization keys per job. The implementation now routes the wrapper through the shared metadata merger.

## Measured merge experiment

The prototype merged all 288 cached DATA job files into new scratch outputs
using ROOT 6.40.04, serial `hadd`, identical input order and warmed local input
files. Three execution orders were rotated. Times below are medians and include
the compact mode's metadata scan, validation, gzip serialization and ROOT
reference write. Exporting legacy TH1 projections was excluded in every mode.

| Merge policy | ROOT bytes | Compressed metadata sidecar bytes | Total wall time |
| --- | ---: | ---: | ---: |
| Unrestricted `hadd` | 38,648,993 | 0 | 37.63 s |
| Exclude provenance only | 23,475,279 | 0 | 12.15 s |
| Validate/store definitions once, preserve compact provenance | 913,729 | 49,021 | 13.39 s |

Compact output plus sidecar is 97.5% smaller than unrestricted merging and
95.9% smaller than the provenance-exclusion result. It is 2.81 times faster
than unrestricted merging, but **10.2% slower than excluding provenance alone**
on this workload: preserving and validating full job metadata costs a separate
scan. This establishes a storage improvement and a speed improvement over the
generic merger, not an additional speed improvement over the existing analysis
workaround. Folding metadata collection into existing validation is a future
optimization, not a measured result.

In these serial benchmark outputs, unrestricted/provenance-excluded `hadd`
actually produced 796 schema cycles from 288 inputs, amplifying duplication
beyond one copy per input. The existing production-style output above contains
288. The prototype contains one schema cycle. Do not assume ROOT's internal
merge grouping preserves a one-to-one key-cycle/job correspondence.

All 213 sparse histograms matched the unrestricted baseline exactly, comparing
60,093 occupied bins: coordinates, contents, sumw2, axis edges/labels, histogram
inventory and entries. The compact provenance has 71 shared fields and only
five varying fields per job; reconstruction of each original provenance map
was checked exactly. The gzip document also round-tripped exactly.

A 44-job MC cross-check reduced total output from 4,795,830 to 2,249,100 bytes,
with similar median timing (2.52 versus 2.60 s). All 225 sparse histograms and
both generator-weight counters matched the baseline exactly; counter totals
also matched the input sums. This confirms that the storage gain depends on
the numerical payload size and does not imply a speed improvement for small
merges.

For the worker writer, metadata-only packing fixtures reduced 76 DATA
provenance entries from a 19,242-byte ROOT file to 2,350 bytes, and 89 MC entries
from 28,519 to 3,037 bytes. This demonstrates the benefit of one compressed
document, but does not project the same reduction onto entire histogram files.

This is a warm local, serial comparison, not an eight-worker DATA-wrapper
benchmark, remote-I/O benchmark, full production run, or timing of TH1 export.
An exploratory 32-job run showed that startup/cache/scan overhead can dominate
small merges; it should not be used to claim a universal speedup.

## Merge correctness issues

### Weight audits are not additive histograms

The four stored bins are `sum`, `mean`, `min`, `max`. Adding two audit
histograms sums all four bins. A fixture using jobs with weights `[1, 1]` and
`[3, 4, 5]` reproduces:

| Statistic | Ordinary `hadd` | Correct merged value |
| --- | ---: | ---: |
| Sum | 14 | 14 |
| Mean | 5 | 2.8 |
| Minimum | 4 | 1 |
| Maximum | 6 | 5 |

Negative counts of 1 and 2 remain two `TNamed` cycles containing `"1"` and
`"2"`, rather than one count of 3. Zero counts have the same representation.
The current persisted four-bin audit lacks an explicit event count, so a
general exact mean cannot be reconstructed from arbitrary legacy audit files.
Do not infer counts from `sum/mean` or ROOT histogram entries.

Store `n`, `sum_w`, `min_w`, `max_w`, `n_negative`, `n_zero` as typed values.
Sum counts and `sum_w`, take the minimum/maximum over nonempty jobs, then derive
`mean = sum_w/n`. If variance is needed, add a declared sufficient-statistics
representation. Legacy jobs with missing counts must report an unavailable
aggregate mean, rather than silently returning the sum of means.

### A pile of key cycles is not a provenance table

The current [`load_provenance_from_root`](../core/python/reproducibility_report.py)
iterates keys, removes cycle suffixes and assigns values into one flat map.
It cannot express multiple jobs' differing values. A merged file therefore
needs a separate aggregate-report interface; returning one arbitrary job's
configuration/build value as the merged identity is misleading.

[`MergeMetadata`](../core/python/law/merge_tasks.py) is already a separate LAW
task, but its summary records only manifest path, framework hash, user-repository
hash and config mtime. It does not read the complete C++ provenance or reduce
the numerical ROOT diagnostics. It cannot currently replace those records.
`MergeHistograms` and `MergeCutflows` also each invoke unrestricted `hadd` on
their role's physical file. When both roles name the same `meta.root`, both
operations can copy the entire container. Merge planning should recognize a
shared physical artifact and select objects by role, or share one physical
merge where the resulting artifact contract permits it.

## Proposed storage contract

### In each job

Continue writing the same physics histograms and normalization counters to
the existing ROOT artifact path. Keep a single embedded histogram schema for
standalone access. Replace the many descriptive `TNamed` objects with one
versioned, compressed ROOT string containing structured job metadata. This
keeps the worker output self-contained and avoids introducing another required
stage-out file in the first rollout.

The document should distinguish:

- Shared run definitions: framework/extension build, analysis revision,
  plugin settings, region definitions and the identity of staged payloads.
- Dataset definitions: sample identity, normalization components and dataset
  manifest selection.
- Job execution: logical job ID, attempt ID, input subset/range, effective job
  config identity, output identity, thread count and measured diagnostics.

Retain unknown/unavailable values explicitly. In the sampled worker output,
analysis revision is `unknown` while dirty status is `false`; the current git
query treats failure like a clean checkout. Neither value establishes a known
clean analysis revision.

Archive shared definitions once at submission/run level when available, using
content-derived IDs. Keep sufficient information in a standalone job artifact
to resolve its identity after moving it; a reference to a submit-host-only
absolute path is insufficient.

### In the merger

1. Inspect input format versions and all schema/definition compatibility
   before producing an accepted final artifact. Group by dataset and declared
   compatibility; report conflicting builds/settings instead of taking the
   first value. Different datasets can legitimately have different constants.
2. Merge numerical products with declared reducers. Do not infer addition
   merely from an object's `TH1` class. Unknown plugin outputs need an explicit
   policy, or a clear error/preserve-as-job-record fallback.
3. Exclude descriptive provenance, invariant schemas/definitions and
   nonadditive audits from `hadd`. Write each validated schema once into a
   freshly created output. Deleting cycles after an unrestricted merge still
   pays the copy cost and may leave unused space in the file.
4. Produce one compressed `metadata.json.gz` per merged artifact/bundle.
   Factor shared fields out; retain one compact job record for input/attempt
   identity and differences. Deduplicate reusable definition records by
   canonical content hash. Store aggregated numerical diagnostics once.
5. Embed a small versioned reference with the sidecar's relative path, digest
   and input count in ROOT. Keep the ROOT file and sidecar together as one
   artifact bundle; verify the digest when reading. The digest detects a wrong
   sidecar but does not authenticate the producer.
6. Write to temporary bundle paths, validate both products, then publish a
   completion manifest last. Reject duplicate logical contributions/retries
   before adding numerical histograms. Record the selected attempt policy.

For hierarchical merging, retain reusable definition IDs and child-manifest
digests. Resolve or flatten leaf job records without embedding all ancestors'
complete summaries again. Preserve job identity across grouping so a repeated
leaf cannot be counted twice. The storage target is proportional to unique
definitions plus genuine per-job information, not jobs times full metadata.

The prototype uses an intersection of identical fields plus per-job differences
as a simple lossless demonstration. A production format should retain the
explicit run/dataset/job scopes above, rather than infer physics scope solely
from coincidental value equality. Shared definitions belong once in the bundle;
the prototype redundantly includes the histogram schema in its compressed
sidecar as well as ROOT to keep its evidence self-contained.

## Provenance identity needs clearer semantics

[`ProvenanceService::hashFile`](../core/src/ProvenanceService.cc) hashes a local
path/size/mtime fingerprint, or a remote URI string. It does not hash the input
file contents. Use an explicitly named reference fingerprint for that evidence;
hash small configs/models/payload manifests by content once during staging,
and record the deployed executable/archive identity separately from source Git.
Do not start reading every large remote ROOT input merely to compute a digest.

The effective `config.hash` includes per-job file/output paths; sampled records
also contain `file.hash.__orig_saveFile` and `file.hash.__orig_metaFile`. Keep
the effective job identity, but add a shared semantic config identity that
excludes only declared execution-routing fields. Do not globally discard keys
just because they differ: physics-affecting settings must remain distinguishable.

## Implementation order

1. Add a shared metadata reader/reducer for legacy and versioned documents;
   implement merge-time factoring and schema retention first. Use it in LAW
   merging and expose it to analysis wrappers, so each analysis does not need
   its own ROOT object exclusion list.
2. Add typed statistical records and correct weight-audit reductions. Preserve
   the existing normalization-counter paths used by workers and downstream
   tools. Validate overlapping physical-file roles and unknown plugin outputs.
3. Consolidate C++ metadata contributions into one writer/document and update
   `MetadataSchema`, ROOT readers, reproducibility/validation reports and
   worker staging/monitor completion checks together. Read legacy outputs
   throughout migration. Keep format-version changes separate from cache
   identity/provenance changes.
4. Add a central output session to avoid repeated open/update/close operations
   if profiling justifies it. This is secondary to stopping repeated merge
   copies and requires a deliberate plugin API migration.

Required production tests include incompatible schema/region/normalization
rejection; exact histogram and counter preservation; correct statistics over
empty/unequal-size jobs; lossless metadata round trips; duplicate attempts;
hierarchical associativity; relocation/missing-sidecar detection; and legacy
readers. The bounded experiment does not establish those production guarantees.
