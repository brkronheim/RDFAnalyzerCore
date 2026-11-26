# LAW merging and histogram collection

Read [merge_tasks.py](../../../../core/python/law/merge_tasks.py), the caller's output manifests, and [output schema](../SKILL.md). Inspect resolved workspace, `input_dir`, `output_dir`, manifest glob, role filters and `complete()` before reusing or regenerating a product.

| Task | Actual contract to follow |
|---|---|
| `MergeSkims` | Collect skim-role inputs, resolve paths, merge groups and write updated skim manifests. |
| `MergeHistograms` | Validate histogram-role manifests and embedded histogram schemas, then merge physical ROOT files and record outputs. |
| `MergeCutflows` | Merge cutflow-role ROOT inputs with their metadata contract. |
| `MergeMetadata` | Aggregate provenance/manifests; its name does not imply numerical ROOT histogram addition. |
| `MergeAll` | Select applicable merge subtasks and consolidate their output manifests; inspect skip flags and actual discovered roles. |

`MergeMixin` discovers manifests and checks completion/compatibility. Relative ROOT paths are resolved from the input manifest's directory; supported `root://` paths are passed to ROOT tools while manifest discovery/output directories remain local. Inspect the actual helper implementation despite names such as `_require_local_root_input`.

`MergeHistograms` groups files by basename and validates category-to-bin mapping and exact binning before writing. Mixed inputs with and without the embedded sparse histogram schema are rejected within a group. Preserve categorical labels, underflow/overflow policy, sumw2 and nuisance coverage. Schema compatibility is a precondition, not a substitute for inspecting the merged result. Use [histograms](../../rdfanalyzer-histograms/SKILL.md) for projections and embedded schemas.

Histogram/cutflow tasks use [metadata_io](../../../../core/python/metadata_io.py),
which excludes descriptive and nonadditive objects from hadd, validates physical
axes, preserves leaf provenance and publishes a sidecar/completion bundle.
Cutflow merges select only counters/cutflows when roles share a physical file.
`MergeMetadata` additionally archives full ROOT/child-summary records when
available. See [the contract](../../../../docs/METADATA.md) for duplicate policy,
scope/format compatibility, legacy audit limits and relocation requirements.
For verified runtime-only differences in existing outputs, the standalone
metadata/partial mergers accept `--semantic-config-compatibility FILE` with an
exact sample/hash policy. See the metadata contract for the format and audit
record; all other compatibility guards remain active. This is an explicit
exception, not an automatic inference from differing hashes.

After merging, verify referenced output paths, physical files, expected keys/axes and accounting totals. Read completion rules for companion manifests and reports rather than assuming a `.done` file verifies all content. Tests: [test_merge_tasks.py](../../../../core/tests/law/test_merge_tasks.py) and [test_output_schema.py](../../../../core/tests/python/test_output_schema.py).

## CollectHistogramsTask has a different implementation

In [analysis_tasks.py](../../../../core/python/law/analysis_tasks.py), `CollectHistogramsTask` requires a named `SkimTask`, recursively discovers **`skim.root`** under `skimRun_<skim_name>/outputs`, and runs `hadd -f` into `histCollect_<name>/histograms.root` with `collect.done`. This disagrees with its description as merging histogram outputs. It is not the manifest-aware `MergeHistograms` implementation and does not select `meta.root` by that name. For a requested collection, first establish where the analysis actually wrote histograms; report this discrepancy before using the task as evidence of merged metadata histograms. Do not silently substitute a different input glob in a production diagnosis.
