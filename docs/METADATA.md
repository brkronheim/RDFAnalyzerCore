# Job metadata and reduction

Jobs write one compressed JSON `TObjString`, `__rdf_job_metadata_v1`, alongside
their numerical ROOT outputs. It contains `format: rdf-job-metadata`,
`schema_version: 1`, a string-valued `provenance` map, `regions`, scoped
`normalizations`, and typed `weight_audits`. The histogram schema remains one
embedded `__rdf_histogram_schema_v1` object for standalone histogram readers.
Normalization counters retain their existing `counter_*` paths.
Failure to open or write a configured metadata output fails the job; omitting
an output channel remains supported for intentionally output-free analyses.

`RegionManager` and `WeightManager` contribute through
`IPluggableManager::collectOutputMetadata()` after `finalize()`. `Analyzer`
collects these contributions before `ProvenanceService` writes the document.
Duplicate contribution keys are errors. A plugin used outside `Analyzer` can
retrieve its typed record directly; `finalize()` no longer writes the legacy
descriptive `TNamed` objects or four-bin weight-audit histograms.

## Merge existing or new ROOT files

```bash
python core/python/metadata_io.py merged.root job0.root job1.root
python core/python/metadata_io.py merged.root job0.root job1.root --jobs 8 --extract-histograms
```

The Python API is `metadata_io.merge_root_files(output, inputs, role="all",
jobs=1, postprocess=None)`. `role="cutflow"` selects counters and cutflows;
histogram/all roles retain numerical histogram products and counters. The
merger uses ROOT's numerical histogram reducers. Known nonadditive weight
audits are excluded and reduced separately; custom diagnostic statistics
should use typed metadata rather than masquerade as ordinary histogram bins.
Unknown descriptive `TNamed`/`TObjString` objects are archived per job. Other
non-histogram classes, including trees in a metadata artifact, are rejected.

Before addition it checks the declared sparse schemas, physical sparse
inventory/axes, numerical histogram labels/edges, framework/extension labels,
and per-sample region/plugin/normalization definitions. Different datasets can
have different normalization constants. Configure timestamps are retained but
are not compatibility IDs. Legacy Git labels cannot identify changes in dirty
sources; compact jobs prepared by LAW additionally record the expected staged
executable's SHA-256. No automatic override accepts conflicting definitions.
`--nominal-only` changes the exported view; it does not waive incompatible
physical input binning.

The default duplicate policy is rejection: two logical contributions or retry
attempts for the same job must not both add their histograms. `metadataJobId`
can specify a logical ID; otherwise the C++ writer derives it from the original
metadata output path (absolute for local paths). Direct callers whose retries
use unrelated working directories should supply the same `metadataJobId`.
`metadataAttemptId` records an optional attempt label.
The AGC range driver supplies a stable SHA-256 identity from input content hash
(or URL when unavailable) and half-open entry bounds. It intentionally excludes
output paths, worker names and partition ordinals, so computed-but-uncommitted
retries cannot contribute twice when their ROOT outputs are accidentally mixed.
For legacy jobs, config/file-list fingerprints identify contributions, with
the source path as an explicit fallback when no identity evidence exists.
Distinct copies of otherwise unidentified legacy files cannot be proven to
represent the same job.

## Portable merged artifacts

Each output bundle consists of:

- `merged.root`: numerical products, one histogram schema, and a small
  `__rdf_metadata_reference_v1` JSON object.
- `merged.root.metadata.<sha256>.json.gz`: identified job records and reusable,
  content-addressed run/dataset definitions. Genuine differences are retained.
- `merged.root.merge.json`: completion record written last, referencing the
  sidecar identity and ROOT file size.

Keep these siblings together when moving or archiving an output. References
use sibling basenames and are digest checked. A missing sidecar, wrong digest,
or missing/stale completion record invalidates a merged artifact. Completion
does not attest to every numerical ROOT byte; it checks the metadata contract.
Sidecars are immutable. A failed validation/merge/postprocessor preserves an
existing accepted ROOT artifact. A crash during publication leaves completion
invalid, so the workflow rebuilds. Older unreferenced sidecars may remain after
replacing an output; removal is an operator retention decision.

Hierarchical merges expand child job records, reuse definitions, and reduce
from leaf statistics. They do not embed recursive copies of ancestor summaries.
A leaf appearing through two parents is rejected before numerical addition.

## Reading and reporting

```python
from metadata_io import load_metadata_records, validate_metadata_bundle, reduce_audits

records = load_metadata_records("merged.root")  # also reads legacy job files
validate_metadata_bundle("merged.root")         # includes completion checks
audits = reduce_audits(records)                  # indexed by sample and column
```

`reproducibility_report.load_provenance_from_root` returns only values shared
by all input jobs. Use `load_provenance_records_from_root` for complete records.
The report CLI saves a factored `metadata_bundle` with all jobs in JSON/YAML,
and labels the shared values in the text report. Readers accept legacy
`provenance/` directories; differing legacy cycles are rejected because their
job association is ambiguous.

Audits store integer `n`, `n_negative`, `n_zero`, and numerical `sum_w`, `min_w`,
`max_w`. Counts/sums add; extrema ignore empty jobs; mean is derived from
`sum_w/n`. Empty aggregates have no mean/extrema. Old four-bin audits lack an
event count: their sum and available negative/zero counts can be reduced, but
aggregate mean/extrema are unavailable. Original legacy values remain in each
job record. No event count is inferred from ROOT entries or from sum/mean.

`MetadataSchema` keeps its manifest-envelope version 1, adds auto-detected
storage/document/reference fields, and validates embedded format versions
separately. Schema compatibility and provenance/cache identity remain separate.
Worker output checks validate compact documents without requiring another
stage-out file. LAW merge completion and validation reports check the bundle
companions. The vjet merge wrapper calls the same shared merger.

`file.reference_fingerprint.*` explicitly names the inexpensive path/size/mtime
evidence; `file.hash.*` remains a legacy alias. These are not content digests.
`run.bundle_id` identifies an available sealed run bundle. LAW preparation
records `run.expected_executable_sha256` and, for sealed executions,
`run.execution_id`; the executable hash is the preparation-time expectation,
not a C++ measurement of the running process. `config.hash` retains the complete
effective configuration; `config.semantic_hash` excludes only the documented
routing fields in `ProvenanceService.cc`. Failed analysis Git discovery records
dirty status as unknown.

For already-produced files, `metadata_io.py`, `partial_merge.py`, and the vjet
`mergeResults.sh` wrapper accept `--semantic-config-compatibility FILE`. This
explicit JSON policy relaxes only semantic hash equality for the listed sample
and exact hashes, after the operator has verified that the differing effective
configurations are equivalent for the analysis (for example, cache sizes):

```json
{"schema_version": 1, "reason": "Verified cache sizes are the only difference",
 "samples": {"dataset_name": ["original_semantic_hash", "retry_semantic_hash"]}}
```

The default remains strict. Unlisted samples/hashes, build differences, plugin
definitions, normalizations, duplicate jobs, histogram inventories and axes
retain their checks. Original per-job hashes are preserved; used exceptions and
their reason are embedded in the digest-protected metadata sidecar, and partial
merge snapshots record the supplied policy. A later merge of these products
must explicitly supply the policy again; reading an earlier exception does not
authorize another merge. Changing the producer's hashing rules cannot repair
hashes already embedded in worker outputs.

### Explicit analysis Git snapshots

By default, provenance queries `git rev-parse HEAD` and `git status --porcelain`
in the worker's current directory for every Analyzer. A runner creating multiple
Analyzers over one fixed analysis revision can collect this once at worker start
and supply both `metadataAnalysisGitHash` and `metadataAnalysisGitDirty`.
The hash must be a full 40/64-digit hexadecimal Git revision or `unknown`; dirty
state must be `true`, `false` or `unknown`. Partial or malformed pairs fail.

Configured values skip Git subprocesses and record
`analysis.git_source=configured-snapshot`; normal collection records
`live-current-working-directory`. This is an explicit caller-owned snapshot,
not a process-wide cache: refresh it when the analysis working tree changes.
The two metadata settings do not affect `config.semantic_hash`. The complete
configuration hash still records them. Keep source/artifact hashes as well;
a Git dirty flag does not identify uncommitted source contents.

The broader numerical-output session remains unchanged: counter, STXS and
histogram writers can still open ROOT independently. Consolidating those opens
is a separate API migration, conditional on profiling showing a benefit.

### Registry construction diagnostics and merge compatibility

The per-job `root.registry_definitions`,
`root.registry_input_bindings_before`, and
`root.registry_input_bindings_after` values measure graph construction. They
remain in portable leaf provenance but are excluded from build-identity equality
checks. Graph sizes can change under an output-equivalent analysis optimization.
The registry implementation mode, ROOT/framework/extension build identities,
expected executable identity, plugin definitions and semantic configuration
compatibility remain subject to the existing strict merge guards.

### Analysis-owned sparse histogram schemas

`merge_root_files(..., sparse_validator=callback)` supports an explicitly chosen
analysis schema. The callback validates the complete physical sparse inventory,
classes, axes and owned declarations, and returns canonical per-object
axis/schema signatures. The merger compares those signatures across all inputs;
build, normalization, logical-job and semantic configuration guards remain
unchanged. Without a callback the framework ND schema remains mandatory.
Uniform category axes can be validated by bin count and bounds without
materializing millions of redundant edges. The caller can use `postprocess` to
publish its common, validated schema in the atomically committed merged ROOT file.
`partial_merge --metadata-merger PATH` delegates to a chosen analysis merger and
records the entry point's SHA-256 in its derived-artifact snapshot.
