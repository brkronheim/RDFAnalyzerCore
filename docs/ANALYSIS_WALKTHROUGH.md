# Tracing an RDFAnalyzer Analysis

This walkthrough shows how to understand an analysis built with RDFAnalyzerCore. It uses the small `ExampleAnalysis` project as a worked example, while keeping the method applicable to analyses with different physics, managers, and production systems.

## Separate framework behavior from analysis choices

An analysis project supplies its physics choices: object definitions, selections, derived observables, regions, and weights. RDFAnalyzerCore supplies the `Analyzer` facade, a ROOT RDataFrame-backed graph, reusable managers/plugins, configuration access, and output lifecycle. An analysis entry point connects the two.

The typical path is:

```text
input files and configuration
  → Analyzer and selected managers
  → compatibility/correction/weight columns, if configured
  → analysis Defines and Filters
  → booked histograms and other RDataFrame actions
  → Analyzer::run() and configured outputs
```

`Define` and `Filter` build a lazy RDataFrame graph. A configured manager may still require an explicit creation, application, or booking call in the analysis source. Follow the calls to establish what actually runs; configuration alone is not evidence that a plugin's work is applied.

## A practical source-reading order

For any analysis directory:

1. Read its `CMakeLists.txt` to identify the executable target, linked libraries, and entry-point source.
2. Read `main()` and note the `Analyzer` constructor, configuration path, manager setup, `Define`/`Filter` order, booked actions, and final execution call.
3. Resolve each referenced configuration file. Follow input files or manifests, plugin settings and payloads, constants, output settings, and histogram definitions. Verify path semantics in code when relative paths matter.
4. Match each important output column to its source: an input branch, a framework manager, or an analysis-specific definition. Trace event weights and systematic variations the same way.
5. Compare histogram variables, axes, category labels, and weights with the graph. A region/category column only categorizes entries unless an event `Filter` rejects them.
6. Inspect the run lifecycle and output managers to determine which files and objects are actually written. If production tasks exist, trace their inputs/outputs separately from the executable.

Use [Architecture](ARCHITECTURE.md) for framework ownership and lifecycle, [API Reference](API_REFERENCE.md) for the public API, and the relevant implementation under `core/src/` or `core/plugins/` when a behavior claim needs verification.

## Worked example: `analyses/ExampleAnalysis`

The project is a dimuon example over ATLAS PHYSLITE inputs. It is intentionally compact and should not be read as a required manager stack or analysis structure.

| File | What it tells you |
|---|---|
| `analyses/ExampleAnalysis/CMakeLists.txt` | Builds the `analysis` executable and links RDFAnalyzerCore and ROOT |
| `analyses/ExampleAnalysis/analysis.cc` | Constructs `Analyzer`, registers a histogram manager, defines the selection and observables, books histograms, then calls `run()` |
| `analyses/ExampleAnalysis/cfg.yaml` | Input file URLs, tree names, threading, constants/config paths, skim settings, and save path |
| `analyses/ExampleAnalysis/floats.yaml` and `ints.yaml` | Constant columns loaded by the data manager after its working-directory existence check |
| `analyses/ExampleAnalysis/histograms.yaml` | Three one-dimensional histogram definitions, their ranges, and `normScale` weights |
| `analyses/ExampleAnalysis/output.txt` | Branch list used only if skim writing is enabled |

The source implements this sequence:

1. `getGoodMuons` selects muons passing the two ID/preselection flags, `DFCommonJetDr > 0.4`, and `pt >= 20000` MeV. The next filter requires exactly two.
2. It makes massless four-vectors for those muons, combines them, and accepts a dimuon mass of at most 150000 MeV. There is no lower mass cut in the current code.
3. It converts the mass and the leading/subleading muon pT values to GeV. The histogram ranges are 70–110 GeV for mass and 20–150 GeV for each pT; values outside those ranges are histogram underflow/overflow, not additional event cuts.
4. `NDHistogramManager::create(an)` registers the histogram plugin, `bookConfigHistograms()` reads `histograms.yaml`, and `an.run()` executes the analysis lifecycle.

The three histogram weights all point to the `normScale` constant. `sampleNorm` and the integer `type` constant are present in configuration but are not referenced by `analysis.cc`. The source does not explicitly define or register systematic variations. `NDHistogramManager` auto-detects paired variation columns by default, but this example sets `histogramSystematics: false` so its histogram output is nominal-only even if the input contains such branches. For another analysis, check both its shifted columns/mappings and this histogram-manager setting.

The example currently does not create a `TriggerManager`, apply trigger filters, or enable skim writing. `cfg.yaml` sets `enableSkim: false`, so `output.txt` does not cause a tree snapshot in this run. The histogram manager writes its ROOT histograms to the metadata output channel. With no explicit `metaFile`, `RootOutputSink` derives that path from `saveFile` by inserting `_meta` before the extension (for example, `ZMuMuL_meta.root`).

Run from the analysis source directory so the main config and the `floatConfig`/`intConfig` files are found. `DataManager` checks those constant-config paths against the process working directory before registering them. Other referenced config paths are resolved from the working directory first, then relative to the main config file:

```bash
cmake -S . -B build
cmake --build build -j$(nproc) --target analysis
cd analyses/ExampleAnalysis
../../build/analyses/ExampleAnalysis/analysis cfg.yaml
```

The configured input files are remote EOS URLs, so a local run requires XRootD access to them or replacement with accessible ROOT files. This walkthrough does not describe a LAW or batch workflow for `ExampleAnalysis`; use production documentation only for analyses that define such tasks.

## Applying the method elsewhere

For a larger analysis, the same trace may involve correction, trigger, certification, object-energy, weight, region, counter, or output managers. Find where each manager is constructed or retrieved, how it is configured, and which method adds columns or books actions. Follow those effects through the final histogram or skim rather than assuming all managers follow the same lifecycle.

For production-enabled projects, connect the executable config to the dataset manifest and task graph, then distinguish per-job inputs, intermediate skims, merged histograms, metadata counters, and final statistical products. See [LAW Tasks](LAW_TASKS.md) and [Batch Submission](BATCH_SUBMISSION.md) for framework-level task patterns.
