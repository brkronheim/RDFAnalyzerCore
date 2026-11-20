# vjet-njetpt event workflow

Optional checkout: `analyses/vjet-njetpt`. Read [CMakeLists](../../../../analyses/vjet-njetpt/CMakeLists.txt) for the `vjet_njetpt` target and [src/analysis.cc](../../../../analyses/vjet-njetpt/src/analysis.cc) for the actual orchestration. Check Git status/HEAD independently; this checkout can contain an unborn Git repository and many untracked run directories.

| Question | First implementation to read |
|---|---|
| Sample/process/channel routing | [SampleRouting.cc](../../../../analyses/vjet-njetpt/src/SampleRouting.cc) and analysis main |
| Reconstructed leptons/jets/regions | [RecoSelections.cc](../../../../analyses/vjet-njetpt/src/RecoSelections.cc) |
| Gen definitions and fiducial jets | [GenDefinitions.cc](../../../../analyses/vjet-njetpt/src/GenDefinitions.cc) |
| Truth/reconstructed ttbar | [GenTTbar.cc](../../../../analyses/vjet-njetpt/src/GenTTbar.cc), [RecoTTbar.cc](../../../../analyses/vjet-njetpt/src/RecoTTbar.cc) |
| MET vectors, propagation, QA | [MetDefinitions.cc](../../../../analyses/vjet-njetpt/src/MetDefinitions.cc), [MetDiagnostics.cc](../../../../analyses/vjet-njetpt/src/MetDiagnostics.cc) |
| V+jets LHE-njets and event products | [EventWeights.cc](../../../../analyses/vjet-njetpt/src/EventWeights.cc) plus analysis main |
| Response/flat index layout | [TemplateIndex.cc](../../../../analyses/vjet-njetpt/src/TemplateIndex.cc) and histogram booking in main |
| Generated configs | [compile_config.py](../../../../analyses/vjet-njetpt/aux/compile_config.py), [cfg](../../../../analyses/vjet-njetpt/cfg) |
| Multi-era production | [submit_condor.sh](../../../../analyses/vjet-njetpt/scripts/submit_condor.sh), [submit_eras.py](../../../../analyses/vjet-njetpt/scripts/submit_eras.py), [run_condor.sh](../../../../analyses/vjet-njetpt/scripts/run_condor.sh) |

Trace `cfg/design.yaml`, `cfg/study_defaults.yaml`, production/era inputs, samples, and generated constants/histograms into the actual submitted config. Defaults and saved bundles can diverge. Numeric selection, channel codes, template index order, and reader decode must agree; changing event behavior requires reprocessing.

For MET diagnostics distinguish PF/PUPPI vectors, covariance provenance, stored/recomputed significance, cleaning populations, and final selected-event modeling. For b-tag efficiency work inspect [pnet_efficiency.py](../../../../analyses/vjet-njetpt/aux/pnet_efficiency.py), [build_efficiency_maps.py](../../../../analyses/vjet-njetpt/aux/build_efficiency_maps.py), and [build_reco_btag_efficiency_maps.py](../../../../analyses/vjet-njetpt/aux/build_reco_btag_efficiency_maps.py). Read numerator/denominator and axes before calling an output a usable fixed-WP efficiency.

C++ tests are in [tests](../../../../analyses/vjet-njetpt/tests); config/response/acceptance/plot/fit tests are in [aux/tests](../../../../analyses/vjet-njetpt/aux/tests). Select the affected tests and build target. Use [vjet fits](vjet-fits.md) for saved cards and plotting, [CMS](../../rdfanalyzer-cms/SKILL.md) for shared corrections, and [production](../../rdfanalyzer-production/SKILL.md) for worker staging.
