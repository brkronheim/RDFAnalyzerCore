# Examples and CMS open data

## ExampleAnalysis

[ExampleAnalysis/CMakeLists](../../../../analyses/ExampleAnalysis/CMakeLists.txt) builds target `analysis` from [analysis.cc](../../../../analyses/ExampleAnalysis/analysis.cc). Trace [cfg.yaml](../../../../analyses/ExampleAnalysis/cfg.yaml), constants, [histograms.yaml](../../../../analyses/ExampleAnalysis/histograms.yaml), definitions/filters, histogram-manager creation, booking and `run()`. This is a compact API illustration, not a physics or production default for other analyses. [ANALYSIS_WALKTHROUGH](../../../../docs/ANALYSIS_WALKTHROUGH.md) gives a longer explanation; verify it against the current source when reproducing behavior.

## CMS_Run2016_DoubleMuon

[CMS_Run2016_DoubleMuon/CMakeLists](../../../../analyses/CMS_Run2016_DoubleMuon/CMakeLists.txt) builds `cms_doublemu_analysis` from [analysis.cc](../../../../analyses/CMS_Run2016_DoubleMuon/analysis.cc). Read [cfg.yaml](../../../../analyses/CMS_Run2016_DoubleMuon/cfg.yaml), trigger/constant/histogram files and [dataset_manifest.yaml](../../../../analyses/CMS_Run2016_DoubleMuon/dataset_manifest.yaml). Follow trigger application, muon selection, dimuon observables, cutflow and booked output.

The neighboring [fit_zpeak.py](../../../../analyses/CMS_Run2016_DoubleMuon/fit_zpeak.py) and [zpeak_workspace.yaml](../../../../analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml) belong to its specific Z-peak fit workflow. Determine whether a task uses the generic Combine tasks, analytic workspace task, or a local script. Open-data discovery is a separate operation from reading an existing local sample and should be invoked only when the requested run needs it.

These optional directories may inherit the framework Git root; check `--show-toplevel` rather than assuming a nested repository. Read each build target from the checkout and use a bounded local input for execution when available.
