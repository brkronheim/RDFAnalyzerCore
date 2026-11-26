# CMS extraction validation

The initial extraction checks below preceded the generic extension registry.
The follow-up moves `RDF_ENABLE_CMS` into the CMS configure hook and uses
`rdf-extension.json` plus the generic registration contract documented in
[Source extensions](EXTENSIONS.md). The refreshed local bundle and lock file
include that contract.

Follow-up checks passed 80 targeted Python tests (including 13 generic
registration tests that build two non-CMS extensions) and six targeted
core-only CTest suites. All 16 selected CMS-enabled CTest checks also passed,
including the extension's native factory registration check. With CMS physically
absent, a fresh configure introduced
no CMS options, generic event processing passed, and a required-CMS request
failed early. Both external and automatic discovery use the same registry.
The full core-only suite was not repeated for this follow-up; the existing
baseline failures recorded below remain separate from these targeted checks.

## Ownership follow-up

The CMS revision used for this validation was pinned at the time. It shipped four
CMS-owned skills and a standalone skill checker. `StxsManager` and its STXS skill
belong to core; migration-bin mappings and category fallback policy are explicit
analysis inputs. The default decay-channel input is `analysis_channel`, and
automatic histogram binning is derived from the configured analysis bins.

Analysis-specific preflight, sample process/group inference, VH migration-bin
conventions and historical analysis docs are preserved in the separate local
`analyses/VHbbSTXS` tools repository. The full original VHqqRDF checkout is
absent. Core now provides `analysis_preflight.py` and
`convert_samples_to_manifest.py`; CMS provides reusable `CmsPreflight` payload
checks. Core's former analysis-specific CLI flags and import shims were removed.

The final targeted Python run passed 74 tests, including tests loaded from an
independent clone of the analysis-tools bundle. STXS built and passed its
numerical/configuration tests with CMS enabled and disabled. CMS infrastructure,
MET, Rucio, preflight, native registration, generic event processing and
provenance checks passed. With CMS physically absent, core STXS, generic
preflight/conversion, native event processing and the core skill checker passed.
No event-level analysis reprocessing was performed.

The CMS bundle was refreshed and independently cloned; all four CMS skills and
links validate without a neighboring core checkout. The analysis-tools repo is
also preserved in the local `extensions/VHbbSTXSTools.bundle` artifact. Both
repositories have clean worktrees. Detailed follow-up logs and the pre-change
snapshot are under `/tmp/rdf-ownership`. Earlier extraction checks below describe
the historical extraction state; ownership and intentional default changes in
this follow-up supersede that source snapshot.

The CMS extension is a separate local Git repository at `extensions/cms`.
Its tested revision was pinned at the time of extraction. A complete,
independently cloneable history is distributed in the ignored local artifact
`extensions/RDFAnalyzerCMS.bundle`. No remote repository was created.

## Successful checks

- The extension's 34 moved C++ implementation/header files match the saved
  pre-extraction worktree byte for byte, including existing local physics
  changes. Canonical CMS tests moved with their fixtures.
- Built `rdfanalyzer`, `cms_doublemu_analysis`, `vh_mumu_hbb`, `vjet_njetpt`,
  and the affected CMS and vjet test targets with CMS enabled. Existing external
  analysis CMake files were left unchanged and still link `coreAll`.
- All 15 selected CMS/provenance/transport/native/vjet CTest checks passed.
- Physically removed the default CMS checkout temporarily, configured AUTO,
  built the generic framework and ExampleAnalysis, and passed eight targeted
  runtime checks. The checkout was restored afterwards.
- Native event-processing checks passed with CMS enabled and disabled, using a
  real three-event TTree, a derived column, a selection, a written skim, plugin
  availability checks, and enabled/revision provenance checks.
- The broad core-only run passed 82 of 88 CTest suites. Remaining failures are
  documented below.
- Configured Combine and CombineHarvester with `RDF_ENABLE_CMS=OFF`. Their
  build/setup code, datacard tools and workflow tasks remain in the framework.
- Verified the bundle, cloned it through `scripts/fetch_cms.py` at the pinned
  revision, cloned it with ordinary Git, and configured the framework against
  a clone outside the framework source tree.
- Tested missing/incompatible extension errors, disabled Python providers,
  sample parsing without CMS, preservation of an existing checkout directory,
  and generated worker scripts containing resolved policy without importing
  the CMS package. Both Python validation modes passed their targeted checks.
- Skill navigation checks pass with the extension present and physically absent.

## Existing failures outside the extraction

The six failing suites in the broad core-only run were:

| Suite | Existing problem |
|---|---|
| `LawAnalysisTasksTest` | Completion/cache/normalization expectations and an unavailable analysis-owned stitching adapter |
| `LawCombineTasksTest` | Analytic workspace output expectation |
| `LawMergeTasksTest` | Histogram output layout expectation |
| `PythonSubmissionBackendTest` | Condor requirements formatting/contents expectation |
| `ConfigurationManagerConfigParsingTest` | Multikey parser test expects two entries and receives three |
| `PythonBindingsIntegrationTest` | Installed Uproot writes an RNTuple fixture, while this test requires a TTree |

All 15 individual pytest failures across the first four suites reproduced by
loading the saved pre-extraction implementations. The generic configuration
parser and the older native Python test were not changed by the extraction.
The new native smoke test explicitly writes a TTree and uses synchronous local
reading; it passes in both build configurations.

An initial build of all installed external analyses also failed in
`analyses/nlo-reweight`, whose source calls private `Analyzer::getDF()`. That
analysis was left unchanged. Generic framework validation selected
`-DRDF_ANALYSES=ExampleAnalysis`; CMS analysis targets were built explicitly.

## Limits and reproduction

These checks establish source preservation, build/link compatibility, synthetic
correction contracts, local event processing, checkout portability, and worker
script generation. They do not constitute a new production campaign or deploy
rebuilt binaries to submitted workers. Existing job bundles were not modified.

Detailed logs and the source snapshot are under `/tmp/rdf-cms-extraction` in the
working environment. Pytest was installed into a temporary test directory;
the existing virtual environment was not modified. The CMS-enabled `build`
and generic `build-core-only` directories retain their respective configurations.
