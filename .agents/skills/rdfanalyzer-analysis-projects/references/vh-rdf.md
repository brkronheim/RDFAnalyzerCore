# VH-RDF

Optional independent checkout: [analyses/VH-RDF](../../../../analyses/VH-RDF). Read its [CMakeLists](../../../../analyses/VH-RDF/CMakeLists.txt): the `vh_mumu_hbb` target includes [analysis.cc](../../../../analyses/VH-RDF/analysis.cc) and [VHSelections.cc](../../../../analyses/VH-RDF/VHSelections.cc). The target name does not restrict the implemented channels to dimuons; resolve the chosen cfg and wrapper.

Start with `cfg_*`, `submit_config_*`, triggers, float/int constants, histogram configs and manifests for the requested resolved/boosted 0l/1l/2l mode. Follow [VHSelections.h](../../../../analyses/VH-RDF/VHSelections.h) and implementation for object masks, boosted fat-jet selection, overlap removal, regressed-jet inputs, channel/region columns, and tagger discriminants. Branch names, score ratios, and documentation nicknames are not interchangeable; establish the exact formula and threshold from code/config.

Analysis main wires NanoAOD compatibility, CMS corrections/cleaning, trigger logic, ONNX inputs/models, boosted/resolved kinematic fits, regions, and standard weight composition. Trace the chosen path and explicit calls in that order. Check configured `onnx_models`/fit files and `runVar` inputs rather than assuming every model/fit is applied to all modes.

For tagger efficiencies inspect [VHTaggerEfficiency.h](../../../../analyses/VH-RDF/VHTaggerEfficiency.h), its plugin registration in main, and the exporter/finalizer scripts actually called by the workflow. Keep numerator/denominator and selected reconstructed collection consistent with downstream weights.

Read the selected `submit_condor*.sh`, local wrapper, or [submit_condor_parallel.py](../../../../analyses/VH-RDF/submit_condor_parallel.py) through delegated tasks before constructing a production command. [tests](../../../../analyses/VH-RDF/tests) include selection and efficiency-workflow coverage; CMake registers `VHSelectionsTest` for the C++ selection tests.

`AnalysisTools/` and `CMSRun3VHbbSTXS/` can be separate external projects under this checkout. If the request crosses into one, identify its own Git root and applicable AGENTS.md before reading its implementation. Do not infer that a neighboring Coffea/PocketCoffea workflow is executed by the RDF C++ analysis.

Related: [CMS](../../rdfanalyzer-cms/SKILL.md), [tagging](../../rdfanalyzer-tagging/SKILL.md), [inference](../../rdfanalyzer-inference/SKILL.md), [kinematic fits](../../rdfanalyzer-kinematic-fit/SKILL.md), and [STXS](../../rdfanalyzer-stxs/SKILL.md).
