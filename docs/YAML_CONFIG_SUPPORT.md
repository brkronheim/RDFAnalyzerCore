# Native YAML configuration and migration

Use `.yaml` or `.yml` for new analysis and submission files. The filename extension selects the decoder independently for the main file and each referenced file. The C++ configuration provider keeps its string-map, vector and record interfaces. Parsing happens during setup.

## Use the same worker YAML from Python and C++

Open the original native main YAML by path in either language. Both constructors use the native configuration manager, apply the same file-resolution rules and retain the worker settings used for provenance. Auxiliary config paths are checked in the working directory and then beside the main file. Input ROOT file paths and output paths retain their native working-directory or URL semantics.

```cpp
Analyzer analyzer("cfg/worker.yaml");
```

```python
import rdfanalyzer

analyzer = rdfanalyzer.Analyzer("cfg/worker.yaml")
analyzer.book_count("selected")
analyzer.run()
print(analyzer.counts["selected"])
```

For Python's typed authoring syntax, `AnalysisConfig.from_file("job.yaml")` accepts fields such as `files`, `tree`, `output` and `save_columns`. `rdfanalysis.Analysis(config)` lowers them to native keys such as `fileList`, `treeList` and `saveFile`, writes a temporary native YAML plus any projected-column list, and opens that YAML through the same binding. `config.native_config()` shows the lowered settings before optional skim-column file materialization. Typed input/output paths resolve beside the authoring YAML. Use absolute paths for config references passed through the typed `options` mapping when the worker's working directory may change. [Python authoring guide](PYTHON_AUTHORING.md)

## Choose the document for the consuming setting

| Setting or consumer | YAML document | Example |
| --- | --- | --- |
| Main analysis, submit template, `floatConfig`, `intConfig` | Mapping | `{threads: 4, saveTree: Events}` |
| Aliases (`existingName`, `newName`), histogram, correction, model and other plugin record loaders | Sequence of mappings | `[{name: muon_pt, variable: Muon_pt, bins: 50, lowerBound: 0, upperBound: 100}]` |
| Skim columns and file lists loaded through `parseVectorConfig` | Sequence of strings | `['Muon_*', run, luminosityBlock]` |
| Dataset discovery | Dataset manifest mapping with a `datasets` list | See [dataset manifest](DATASET_MANIFEST.md) |
| STXS analysis bin definitions | Sequence of mappings | `[{analysis_bin: 1, label: signal, fine_categories: [71, 72]}]` |
| STXS migration bins | Sequence of mappings | `[{fine_category: 71, pt_bin: 1, jet_pt_bin: 2, scale_bin: 3, acceptance_fallback: true}]` |

Use the native option's expected value syntax. The native input settings `fileList`, `treeList`, `globs` and `antiglobs` accept YAML sequences or comma-separated scalars. Each sequence item must be a scalar without an embedded comma. The configuration map exposes these four options as comma-separated strings for existing clients. `stxsBinConfig` refers to a file. Keys and values with special YAML characters such as a leading wildcard should be quoted. Native scalar values preserve their spelling when lowered to strings. The typed Python authoring API and dataset manifests have their own schemas. A record file can contain multiple record types. The native `requiredEntryKeys` argument selects matching records.

## Convert an analysis

Run from the repository root. Select the converter mode by the setting that reads the file.

```sh
python core/python/convert_config.py cfg.txt cfg.yaml
python core/python/convert_config.py --kind records histograms.txt histograms.yaml
python core/python/convert_config.py --kind list output.txt output.yaml
python core/python/convert_config.py --kind samples samples.txt datasets.yaml
```

Then update paths in the main YAML and submit template, including `histogramConfig`, `saveConfig` and `sampleConfig`. The converter operates on the named file. If a legacy record includes ambiguous free text, the converter reports the line and token so the author can write the intended YAML string. Legacy `prefix_cern` entries map to `base_dirs`. The optional `record_ids` list preserves historical Open Data `recids` discovery requests, and per-sample analysis metadata is retained in `extra`. Use `--kind stxs_bins` or `--kind stxs_migration` for historical positional STXS tables. The converter emits the field names shown above, preserving category order and the acceptance-fallback flag.

## Worker configuration

New Snakemake jobs receive `submit_config.yaml`. All native job writers use `native_config_io.write_config`, which atomically publishes YAML and rejects text output paths. Replace calls to the removed Python `write_config_text` with `write_config(settings, "file.yaml")`. `read_config` still decodes already-produced text inputs. A wrapper should pass the config path from its argument list to `Analyzer` or `read_config`. Condor preparation ships the pure-Python YAML reader for worker helpers. Include PyYAML in a separately packaged Python analysis environment when the analysis reads YAML itself.

| Producer or execution path | YAML artifacts and compatibility |
| --- | --- |
| Snakemake, LAW compatibility adapters, local and Dask execution | Partition, smoke-test and worker configs are YAML. When executing an archived text input directly, a new sibling `*.runtime.yaml` preserves config-relative references. Provenance continues to hash the original input bytes. |
| Condor preparation and staged input/output handling | New transfer bundles carry YAML. Worker helpers stage inputs and update output paths in the working copy. A config returned by Condor is remapped to the job logs. |
| Historical Condor retries | Split retries get an isolated `submit_config.yaml`. Repeating the same retry preserves existing receipts. A conflicting partition requires a new attempt. Retrying a recognized historical wrapper materializes its YAML, reader and launcher under `resubmissions/`. For a custom historical launcher, prepare a new campaign from its execution settings. Already-running workers keep their original submission. The XRootD runtime refresh tool requires a YAML worker before replacing its config helpers. |
| Standalone ProductionManager | New jobs use `job_config.yaml`, `floats.yaml` and `ints.yaml`. Mixed or historical prepared inputs get separate runtime YAMLs during submission. A retry keeps the original launch descriptor and uses the selected job directories. Condor process numbers identify attempts within each new cluster. |
| Sealed bundles and auxiliary files | The compiler converts declared, recognized legacy recipes into their native mapping, record list, skim list or sample-manifest shape. Record `file` references continue to address the immutable model and calibration payloads. For extension-defined keys, declare the shape at compilation (for example, `asset_kinds={"myFitConfig": "records"}`) or convert the recipe before sealing it. Existing sealed bundles with opaque external recipe formats retain their original bytes and sealed asset paths. |
| Python authoring, worker profiling and sizing | Typed analyses lower to `analysis.yaml` and optional `columns.yaml`. Paired pilot measurements write separate YAML configs for the prefix and complete input. |

The selector `xrdReadPolicy: adaptive` or `xrdReadPolicy: legacy` travels with the other worker settings through preparation, staging and partition retries.

## Compatibility inventory

- The native `TextConfigAdapter` keeps its exported symbols. Its three readers translate historical text into the same document decoder used by `YamlConfigAdapter`. Explicit custom `IConfigAdapter` implementations remain supported.
- Python `native_config_io` serves analysis mappings in submission tooling, job indexing, worker staging and conversion. `submission_backend` retains the older import names. Legacy sample files are isolated by `load_legacy_sample_config`. Historical sample validation and text histogram diagnostics still support old inputs.
- Text positional STXS bins and migration bins remain readable. STXS acceptance specs, migration weight specs and signal rules that previously had placeholder YAML readers now report a configuration error. Programmatic clients can supply these through `stxs::Config`.
- The CMS jet-veto setting accepts a direct correction JSON path or a YAML sequence naming one period-specific map. Historical path lists use the same vector adapter. Golden JSON file lists already use the framework vector reader. Rochester calibration tables, ROOT/JSON payloads, trained text models, file catalogs and job completion receipts retain their domain formats.
- Archived campaigns, sealed bundles, benchmark input snapshots and compatibility test fixtures may still contain text files. Existing files can be migrated into a new run bundle before starting the next production.

Four C++ text fixtures remain for explicit mapping, record, vector and histogram compatibility tests: `config.txt`, `correction.txt`, `output.txt` and `test_histograms.txt`. Ordinary plugin and integration fixtures use YAML. Two trained BDT text files under the tests and one under the inference example are model payloads. File inventory excludes CMake and dependency inputs, external analysis checkouts and archived benchmark snapshots.


New AGC benchmark workers, VH-RDF and NLO foundation profiles, stitching bundles,
and V+jets prepared bundles emit YAML for native worker inputs. V+jets publishes
`dataset_manifest.yaml` and accepts the old manifest basename in sealed bundles.
Archival receipts continue to refer to the original files and hashes. A file
list, statistical datacard, exported model or other framework's benchmark
settings retain the format expected by the consuming tool.
