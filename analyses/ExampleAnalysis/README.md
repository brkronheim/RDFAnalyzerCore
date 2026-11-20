# ExampleAnalysis: a dimuon workflow

`ExampleAnalysis` is a compact RDFAnalyzerCore analysis over ATLAS PHYSLITE input. It demonstrates how an analysis connects an `Analyzer`, an RDataFrame selection graph, a histogram manager, and config-driven histogram booking. See the repository's [analysis walkthrough](../../docs/ANALYSIS_WALKTHROUGH.md) for the reusable method to trace an analysis and for framework context.

## What the current code does

`analysis.cc`:

1. Constructs `Analyzer` from the config path passed on the command line and creates the `NDHistogramManager` plugin.
2. Selects muons passing `DFCommonMuonPassIDCuts == 1`, `DFCommonMuonPassPreselection == 1`, `DFCommonJetDr > 0.4`, and `pt >= 20000` MeV.
3. Requires exactly two selected muons, combines their massless four-vectors, and accepts dimuon mass up to 150000 MeV. There is no lower mass cut.
4. Defines dimuon mass and leading/subleading muon pT in GeV and books the three histograms listed in `histograms.yaml`.
5. Calls `an.run()` to execute the configured workflow.

The histogram axis ranges (70–110 GeV for mass and 20–150 GeV for each muon pT) determine underflow and overflow; they are not additional event selections. Each histogram uses the `normScale` constant as its weight.

The analysis source does not explicitly define or register systematic variations. `NDHistogramManager` can auto-detect Up/Down input-column pairs by default; this example sets `histogramSystematics: false` so its configured output is nominal-only regardless of which variation branches are present in the input. The configured integer `type` and float `sampleNorm` constants are not referenced by this analysis source. No trigger manager or trigger selection is set up here.

## Configuration and outputs

- `cfg.yaml` specifies remote EOS input files, tree names, thread count, constant and histogram configuration files, and output settings.
- `floats.yaml` defines `normScale` and the currently unused `sampleNorm` constant; `ints.yaml` defines an unused `type` constant.
- `histograms.yaml` defines `ZBosonMass`, `LeadingMuonPt`, and `SubleadingMuonPt`, all weighted by `normScale`.
- `output.txt` lists the `ZBosonMass` branch for a skim, but the current config sets `enableSkim: false`, so `an.run()` does not write an event tree. Set `enableSkim: true` only when a skim is intended.

The histogram manager writes ROOT histograms to the metadata output channel. Because `cfg.yaml` has no explicit `metaFile`, the output sink derives the path by adding `_meta` before the `saveFile` extension. The configured `saveFile` is `Output/ZMuMuL.root`, so histogram output is `Output/ZMuMuL_meta.root`.

The main config and the `floatConfig`/`intConfig` paths are opened relative to the process working directory in this setup. `DataManager` checks those constant-file paths from the working directory before registering them, so run from the analysis directory or configure absolute paths. Other referenced config files are resolved from the working directory first and then relative to the main config file. Run from the analysis directory for this example:

```bash
# From the repository root
source build.sh
cd analyses/ExampleAnalysis
../../build/analyses/ExampleAnalysis/analysis cfg.yaml
```

The configured input URLs are remote EOS files. A local run requires access to them through XRootD or edits to `fileList` pointing at accessible ROOT files. The framework can build the executable independently of whether those inputs are reachable.

## Reading or adapting the example

Start in `analysis.cc` and match each input branch to its `Define` and `Filter` uses. Follow the histogram variable and weight names into `histograms.yaml` and `floats.yaml`. For a similar analysis, preserve this tracing method but verify its own manager setup, configuration-path behavior, normalization, event selections, and output actions. Production manifests and batch task graphs are optional analysis-level features; this example does not define them.
