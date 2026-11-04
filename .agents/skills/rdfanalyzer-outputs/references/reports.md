# Validation, reproducibility, and version reports

Use to create or interpret reports from an output manifest or metadata ROOT file. Read the report generator's implementation and the actual evidence it consumes; a populated section can represent a declaration or placeholder rather than a measured check.

## Validation reports

[validation_report.py](../../../../core/python/validation_report.py) provides structured event, cutflow, branch, config, systematic, weight, region and integrity entries. A CLI example with existing local inputs is:

```bash
python core/python/validation_report.py output_manifest.yaml --stage skim --check-files --out-json validation.json
```

`generate_report_from_manifest` validates schema/region declarations and creates report sections. Important limits:

- `--check-files` checks local existence/size and metadata bundle integrity for local ROOT metadata/histogram/cutflow artifacts. It does not inspect skim branches or validate remote outputs.
- Expected branch lists become stubs with empty missing lists. ROOT branch inspection is not performed by that helper.
- Declared cutflow keys become entries with zero counts until measured counts are supplied.
- Nuisance coverage entries need actual column/variation evidence; `VariationOrchestrator.build_validation_report` can supply missing-column details when given the relevant inputs.

Populate those fields from actual files/columns/actions before claiming branch coverage, event totals or successful systematic production. Read the caller to see which checks it performs. CLI exit status reflects recorded report errors, so an empty stub can yield no error without establishing a measured result. See [VALIDATION_REPORTS](../../../../docs/VALIDATION_REPORTS.md) alongside code.

## Reproducibility and version metadata

[reproducibility_report.py](../../../../core/python/reproducibility_report.py) reads legacy TNamed directories, compact job documents and merged provenance bundles via [metadata_io](../../../../core/python/metadata_io.py), or reloads an existing report. Example:

```bash
python core/python/reproducibility_report.py sample_meta.root --json reproducibility.json
```

The flat reader returns only shared values for merged outputs; the CLI JSON/YAML report retains all identified job records in a factored bundle. Missing sidecars, wrong digests and ambiguous legacy cycles fail explicitly. An absent provenance directory/document yields an empty map, not proof of reproducibility. Reformatting an older report does not refresh its facts against current workers.

[version_info.py](../../../../core/python/version_info.py) records framework/user Git hashes and config mtime. Repositories sharing a Git root can have the same hash; non-Git/unborn contexts can lack one. Mtime is not a content hash, and a commit hash does not identify dirty source changes or a deployed executable. For output/cache reuse combine this label with [ProvenanceService](../../../../core/src/ProvenanceService.cc) and [schema/cache rules](../SKILL.md) as actually used by the caller.

Tests: [validation reports](../../../../core/tests/python/test_validation_report.py), [reproducibility](../../../../core/tests/python/test_reproducibility_report.py), [version info](../../../../core/tests/python/test_version_info.py), and [variation orchestration](../../../../core/tests/python/test_variation_orchestrator.py). Verify missing/placeholder evidence remains distinguishable from measured success when changing report behavior.
