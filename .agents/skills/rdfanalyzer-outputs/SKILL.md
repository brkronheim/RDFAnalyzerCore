---
name: rdfanalyzer-outputs
description: Trace or change skim/meta sinks, counters, provenance, validation/reproducibility reports, output schema versions, cache sidecars, and merge compatibility in RDFAnalyzerCore.
---

# Outputs, provenance, and artifact schemas

For generated validation/reproducibility artifacts, read [reports and evidence limits](references/reports.md). For concrete merge tasks and `CollectHistogramsTask`, read [LAW merges](references/law-merges.md).

For optional analysis-owned result JSON, use [RunResult.h](../../../core/interface/RunResult.h)
and its [value/serialization contract](../../../docs/NATIVE_RUN_RESULTS.md). It
serializes already evaluated C++ values to the Python-compatible result envelope;
it does not evaluate actions, inspect paths, certify physics or replace production
metadata/completion schemas. The explicit analysis owns acquisition and scope.

## Trace what a run writes

Read [IOutputSink](../../../core/interface/api/IOutputSink.h), [RootOutputSink.cc](../../../core/src/RootOutputSink.cc), [NullOutputSink](../../../core/interface/NullOutputSink.h), and `Analyzer::run` in [analyzer.cc](../../../core/src/analyzer.cc). Skim and metadata are separate channels. `enableSkim`, `saveFile`, `metaFile`, branch selection, `skimFilter`, and `skimSystematics` affect different parts of the contract. `skimFilter` restricts the output branch while histogram actions retain their booked graph.

Inspect exact branch/glob expansion and empty-selection behavior. The current sink can snapshot all columns when no configured entries match; do not use an empty list to imply a zero-branch skim. Histogram, counter, and plugin outputs can still exist when skimming is disabled. Verify output paths against the generated worker config rather than a source template.

ROOT skim compression is configured by `skimCompressionAlgorithm`
(`zstd`, `lzma`, `zlib`, `lz4`) and `skimCompressionLevel` (0 through 9).
Defaults remain ZSTD level 5; these keys do not change histogram/meta output.
Compare the same branches/events when measuring compression savings.

[CounterService](../../../core/src/CounterService.cc) and [ProvenanceService](../../../core/src/ProvenanceService.cc) own accounting and reproducibility metadata. Trace prefilter capture, weighted counters, task metadata, build hash/dirty status, config/file hashes, and plugin contributions. `plugin.<role>.*` entries and service-provided keys are collected at the end; do not manually invent evidence of a payload or variation being used.

For multiple Analyzers over a fixed checkout, callers may supply the validated
`metadataAnalysisGitHash`/`metadataAnalysisGitDirty` pair to avoid repeated Git
subprocesses. Refresh this explicit snapshot when sources change; inspect
`analysis.git_source` and the [snapshot contract](../../../docs/METADATA.md#explicit-analysis-git-snapshots).

New jobs use one versioned `__rdf_job_metadata_v1` document. Plugins contribute
typed definitions/audits through [OutputMetadata](../../../core/interface/OutputMetadata.h)
after finalize; `RegionManager` and `WeightManager` no longer write legacy
descriptive objects separately. Read [metadata contracts](../../../docs/METADATA.md)
and [metadata_io.py](../../../core/python/metadata_io.py) before changing writers,
legacy readers, numerical reducers or portable merge bundles. Keep counters at
their existing paths. A merged ROOT file, immutable gzip sidecar and completion
record travel together; validate all companions before declaring completion.

## Preserve schema and merge meaning

Native registry optimization reports its actual mode and cumulative construction
counts in provenance; see [the contract](../../../docs/ROOT_REGISTRY_OPTIMIZATION.md).
These counters describe metadata construction, not live memory or event yields.

[output_schema.py](../../../core/python/output_schema.py) and [OUTPUT_SCHEMA](../../../docs/OUTPUT_SCHEMA.md) define `OutputManifest`, artifact roles, region/nuisance definitions, version compatibility, and cache sidecars. A YAML manifest, embedded histogram schema, and ROOT file contents are separate evidence; validate their agreement where the workflow uses all three.

Follow `validate_merge_inputs` and `merge_manifests` in the actual merge task. Compatible schemas do not alone prove every declared ROOT object exists or has equal axes. Check embedded schema and physical files, then update merged output paths. Provenance changes can mark cached artifacts stale independently of format-version incompatibility.

Closest tests: [RootOutputSink](../../../core/tests/cpp/testRootOutputSink.cc), [CounterService](../../../core/tests/cpp/testCounterService.cc), [ProvenanceService](../../../core/tests/cpp/testProvenanceService.cc), [output schemas](../../../core/tests/python/test_output_schema.py), and [merge tasks](../../../core/tests/law/test_merge_tasks.py). Use [histograms](../rdfanalyzer-histograms/SKILL.md) for object naming/axes and [production](../rdfanalyzer-production/SKILL.md) for staged artifacts.
