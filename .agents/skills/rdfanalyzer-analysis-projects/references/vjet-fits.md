# vjet-njetpt fits, plots, and export

`fit_response.py` and `fit_polynomial.py` accept `--backend combine|rabbit` for
prepared fits and preparation-plus-fit. Both delegate to the central statistical
runtime; Rabbit evaluates the saved polynomial/ttbar metadata. See
[backend setup and limitations](../../../../docs/RABBIT_INTEGRATION.md).
Correction plots/export read either backend through `fit_section()`. Rabbit bin
plots consume saved JSON predictions/covariance; Combine-only workspace/GoF
diagnostics must not be applied to Rabbit outputs.

Read the [event reference](vjet-events.md) when a discrepancy could originate upstream. For a saved result, begin at that result directory's final card, `fit_inputs.json`, `poi_map.json`, logs, shapes/workspace, and available result JSON/ROOT files. Do not replace them with present-day `study_defaults.yaml` assumptions.

| Stage | Entry points |
|---|---|
| Response/labelled templates and cards | [fit_response.py](../../../../analyses/vjet-njetpt/aux/fit_response.py), [labelled_templates.py](../../../../analyses/vjet-njetpt/aux/labelled_templates.py) |
| Polynomial model and Combine plugin | [polynomial.py](../../../../analyses/vjet-njetpt/aux/polynomial.py), [VJetPolynomialModel.py](../../../../analyses/vjet-njetpt/aux/VJetPolynomialModel.py), [fit_polynomial.py](../../../../analyses/vjet-njetpt/aux/fit_polynomial.py) |
| Era inputs and correlation policy | [multi_era.py](../../../../analyses/vjet-njetpt/aux/multi_era.py), [era_correlations.py](../../../../analyses/vjet-njetpt/aux/era_correlations.py) |
| ttbar and QCD treatment | [ttbar.py](../../../../analyses/vjet-njetpt/aux/ttbar.py), fit_response and saved metadata |
| QA and response plots | [plot.py](../../../../analyses/vjet-njetpt/plot.py), [met_debug_plots.py](../../../../analyses/vjet-njetpt/aux/met_debug_plots.py) |
| Fit-bin/polynomial diagnostics | [plot_fit_bins.py](../../../../analyses/vjet-njetpt/aux/plot_fit_bins.py), [plot_polynomial.py](../../../../analyses/vjet-njetpt/aux/plot_polynomial.py), [summarize_fit_bins.py](../../../../analyses/vjet-njetpt/aux/summarize_fit_bins.py) |
| Impacts and goodness of fit | [fit_impacts.py](../../../../analyses/vjet-njetpt/aux/fit_impacts.py), [fit_gof.py](../../../../analyses/vjet-njetpt/aux/fit_gof.py) |
| Correctionlib export | [export_correctionlib.py](../../../../analyses/vjet-njetpt/aux/export_correctionlib.py) |

`fit_response.py` has `--stage` choices separating preparation from fit execution. Inspect its current CLI and generated commands. Existing cards fix merge/region/binning policy; `--stage fit` does not necessarily regenerate it. Distinguish merged observed templates from retained source processes/nuisances, era pooling from correlation scopes, and reconstructed-lepton merging from truth/POI merging.

For an audit report explicit channels, edges, overflow/tail rules, mass slices, reco/gen axes, QCD sharing, nuisance scopes, POI mapping, fit status and covariance/rank evidence. For exported curves such as `polynomial_plots/theory_profiled/vjets_curves.json`, identify the producing command, nuisance treatment, and metadata of that particular result; a folder name is not proof of a refit.

For slow plotting, inspect projection plus `_systematic_uncertainty` and the actual selected `--response`/`--histogram` path in plot.py. Compiled sparse projection can coexist with a dominant Python aggregation stage. Benchmark a requested patch on representative inputs with yield/error agreement; do not report an unimplemented recommendation as an optimization.

Use [aux/tests](../../../../analyses/vjet-njetpt/aux/tests) for merge-era/lepton, polynomial, response, export, histogram-I/O and plot tests. Export covariance exclusions and robust-Hesse treatment must match the saved fit contract. Use [statistics](../../rdfanalyzer-statistics/SKILL.md), [histograms](../../rdfanalyzer-histograms/SKILL.md), and [plotting](../../rdfanalyzer-plotting/SKILL.md) for shared layers.
