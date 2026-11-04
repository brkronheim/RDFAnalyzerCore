# Run bundles and shared analysis building blocks

The framework separates frozen analysis inputs, collections of independent
profiles, execution state, and derived products. The implementations are in
[run_bundle.py](../core/python/run_bundle.py),
[run_collection.py](../core/python/run_collection.py), and
[partial_merge.py](../core/python/partial_merge.py).

## Frozen inputs

A bundle contains generated configuration files and declared asset copies.
`bundle_manifest.json` records their SHA-256 hashes, entry points, analysis
metadata, and the bundle identity. Publication is atomic and refuses to replace
an existing bundle. Explicit reuse is permitted only after verifying the files
and matching the compilation request. Editing a source payload cannot change
an already materialized asset.

Analysis compilers own their configuration grammar and physics validation.
`snapshot_assets` copies declared dependencies under unique, content-derived
names. `compile_config_bundle` handles YAML submit templates, declared top-level
file keys, nested YAML `file` references, and text model `file=` references.
References use bundle-relative paths. Source paths are provenance, rather than
fallback search locations. Conflicting basenames remain distinct.

```bash
PYTHONPATH=core/python .venv/bin/python -m run_bundle /path/to/bundle

.venv/bin/python analyses/VH-RDF/prepare_run.py \
  --config analyses/VH-RDF/submit_config_zll.yaml \
  --manifest analyses/VH-RDF/dataset_manifest_zll.yaml \
  --destination /tmp/vh-zll-bundle
```

These commands verify or prepare local files; they do not submit jobs. Vjet's
existing compiler emits the shared manifest alongside its analysis-specific
`run_manifest.json` and `bin_map.json`. VH's local and Condor wrappers prepare
their selected profile before invoking LAW. The VH efficiency preparation
wrapper also uses atomic publication and exact-request reuse.

CMS registry defaults and the external software environment remain separately
resolved dependencies. Explicitly supplied calibration assets are copied;
this contract does not turn an unpinned CVMFS environment or container tag into
a hermetic runtime image. Set explicit payload paths when freezing their bytes
is required.

## Collections and execution

`collection_manifest.json` identifies independently sealed children. It is
deliberately distinct from a job bundle. Vjet retains `era_inputs.yaml` for its
era routing and writes the shared collection record as well. Both analyses use
shared member-name/output derivation and bounded concurrent launching. VH
checks every selected profile's dependencies before launching the first child.

For sealed bundles, LAW verifies inputs before preparing jobs and records:

- `execution_identity.json`: bundle identity, dataset/discovery hashes,
  partition settings, executable and local-library content signatures,
  setup-script identity, container selection, and output destination;
- `execution_plan.json`: the prepared partition/config identities;
- `runBundleId` and execution metadata in worker configuration.

An exact resume is accepted. Changed execution inputs require a new run name;
they do not silently refresh an existing run's workers. Existing legacy runs
remain supported through their previous preparation path. Credentials are
handled by the submission workflow rather than copied into an analysis bundle.

Staging uses the declared dependency closure for sealed bundles and rewrites
nested configuration references for either direct or `cfg/` worker layouts.
Mutable text configurations are materialized before rewriting. The source
bundle is never rewritten through a symlink. Legacy configs retain legacy
resolution behavior.

During LAW preparation, sealed YAML/text payloads are rewritten once when the
shared archive is built. Their rewritten bytes are reused for independent
per-job files, so creating more partitions does not repeat YAML parsing. This
cache exists only for that preparation invocation; subsequent preparations
still verify bundle integrity and construct a fresh cache.

The generic executor accepts `RDF_GPU_DEVICE_IDS`, `RDF_VERBOSE`, and
`RDF_JOB_SUMMARY`. The historical `RDF_VH_*` equivalents remain aliases; a set
generic variable takes precedence.

## Reductions, plotting, and fits

Both `mergeResults.sh` wrappers delegate completed-output staging, cache
management, locking, and dataset snapshots to `partial_merge`. The adapter
calls [metadata_io.py](../core/python/metadata_io.py) for numerical reduction,
schema validation, and metadata preservation. The merger's completion record
and metadata sidecar remain authoritative. A separate `.snapshot.json` records
completed-job membership, input content hashes, and reduction configuration.
Failed refreshes retain the previous ROOT result. Nominal-only extraction is
an explicit option and does not waive the merger's compatibility checks.

[analysis_inputs.py](../core/python/analysis_inputs.py) selects one dataset
representation in order: final result, partial snapshot, then job files. It
also returns normalization factors together with denominator mode, luminosity,
and contributing files. Vjet and VH plotters use these helpers. VH prefers
counters from the actual consumed files over whole-production normalization
metadata when those counters are present. Negative denominators require the
caller's explicit legacy policy.

Vjet accepts `--study-config` for an independent fit study. Preparation reads
that configuration once, writes `fit_study.yaml`, and records final cards,
shapes, mappings, and policies in `artifact.json`. Fit-only loading verifies
sealed preparation artifacts. Preparing over a sealed card requires a new
output directory; `--stage fit` reuses it. Legacy prepared cards remain readable.
Diagnostic ensembles use content hashes of their saved fit and workspace,
including detection of same-size, same-mtime changes.

## Physics ownership

| Shared API | Owner | Analysis responsibility |
| --- | --- | --- |
| `CmsJetId.h` composition ID and v12 repair | CMS | Recipe parameters, pT/eta cuts, overlap removal |
| `applyCmsJetId` | CMS | Select/use the returned aligned mask |
| `defineCmsMetXY` | CMS | Explicit payload, epoch, MET representation, stage ordering |
| `CmsAnalysisRecipes` correction batches and trigger-component grammar | CMS | Channels, selected input columns, payload choices, nuisance grouping |
| `cmsYearSettingsFromConfig` | CMS | Full era, overrides, specialized calibration choices |
| `CmsGenParticles` and generator compatibility columns | CMS | Boson/top definitions, fiducial cuts, flavor classification |
| `AnalysisMath.h` | Core | Covariance provenance and validity policy, selected indices, inference-fold mapping |
| `CategoryLayout.h` / `category_layout.py` | Core | Axis/category definitions and sentinel mapping |
| `EfficiencyMeasurement.h` / `efficiency.py` | Core | Measurement stage, collection, weight, flavor/WP definitions, bins |
| Statistical diagnostics and covariance modes | Core | Likelihood model, correlations, fit-quality acceptance policy |

The shared efficiency tools sum pass/fail sufficient statistics before taking
ratios, preserve sumw2 under normalization and aligned rebinning, and reject
unphysical export bins. VH supplies its process groups and UParT calibration
conventions in thin exporter/finalizer adapters. GenJet-matched diagnostic
efficiencies and reconstructed-jet calibration efficiencies remain separate
measurements. The prefilter `TaggerEfficiencyService` is not a substitute for
booking an analysis-selected collection at the required graph stage.

`applyCmsMetXYShift` now has a definition requiring `metXyFile` and `metXyEpoch`.
No implicit Run 2 coefficient table is supplied by that compatibility entry
point. `defineCmsMetXY` supports explicit pass-through with an empty file.

## Validation

Shared unit suites cover atomic failure/publication, copied dependencies,
relocation, tamper detection, collection identity, exact resumes, diagnostic
hashes, worker layouts, normalization, and category decoding. The CMS shared
algorithm test covers jet-ID boundaries, covariance validity, ancestry cycles,
fold selection, and selected-object efficiency sumw2. Analysis selection tests
exercise the retained physics policies.

The implementation was also checked with five VH profile compilations, vjet's
five-era staging tests, a 10,000-event before/after vjet replay with all 228
histogram/counter/schema objects equal, and local partial-merge integration.
An additional 1,000-event MC smoke run consumed a newly compiled bundle through
the worker `cfg/` layout, including its frozen nested calibration references.
The focused Python checks passed 70 tests, and all eight selected CTest targets
passed. A prepared-card integration test verifies the saved identity, rejects
overwriting a sealed preparation, and detects a modified study snapshot.
The subsequent failure-repair pass corrected local legacy `aux/` staging and
made both monitor polling and completion validate ROOT contents. It also
updated old preflight fixtures, removed core tests' dependency on an optional
analysis adapter, and corrected statistics fixtures for the current response
layout and stack limit. The complete LAW test directory passed 543 tests;
the complete vjet auxiliary test directory passed 109 tests, with nine opt-in
Combine integration tests skipped.
The labelled-response reader also reserves explicit packed-coordinate values
for ttbar's negative truth sentinels, preventing a failed classification from
decoding as a physical bin of another decay. Existing affected cards need fresh
preparation from the saved histograms; their run bundles are unchanged.
Muon working-point nuisance masks also match the physical channel underlying
each `ge1b` category, retaining those stored ID/isolation variations in cards.
These results do not establish that the full checkout test suite passes.
These checks do not certify remote submission, all CMS eras/channels, or a new
production campaign.
