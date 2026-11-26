# RDFAnalyzerCore Snakemake workflows

This Snakefile runs selected eras of one campaign in parallel and merges each
era's verified histogram outputs. It inspects all selected specs and validates
their runtimes before starting any batch stage. RDFAnalyzerCore owns worker
staging, Condor submission and retries, and the verified output inventory.

Install the controller with `pip install -r requirements-snakemake.txt`.
The workflow uses the shared framework services in `core/python` and
requires no LAW or Luigi installation. Set up ROOT for the selected runtime.
See the [Python source map](../../core/python/README.md#production-package)
for the implementation behind the workflow.
For a Python worker environment archive, use
`bash core/python/setup_python_env.sh --help` in a checkout or
`"$RDF_CONTROLLER/python/setup_python_env.sh" --help` after installation.

## Inspect the graph offline

Run from the RDFAnalyzerCore checkout with Snakemake 9 available:

```bash
snakemake --snakefile examples/snakemake/Snakefile \
  --configfile examples/snakemake/fixtures/campaign.yaml \
  --cores 2 --dry-run --printshellcmds
```

The preview shows the shared campaign plan and runtime preflight, followed
by two batch stages and two same-era merges. Use `--dag` in place of
`--dry-run --printshellcmds` to print the graph. The fixture specs use a sealed
demo bundle with zero datasets and `/bin/false` as a placeholder executable.
Inspect the complete plan from the checkout:

```bash
PYTHONPATH="$PWD/core/python" python -m production_campaign inspect \
  examples/snakemake/fixtures/campaign.yaml
```

## Configure a campaign

Point the campaign at an existing sealed era collection. The workflow reads
the child bundles' `submit_config` and `datasets` entrypoints and creates stable
specs under `output_root/.campaign/specs`. It appends the era to the run name and
creates an era subdirectory under the remote destination. The example below
builds and validates one shared C++ executable for the two selected eras.

```yaml
collection: bundles/vjets-run3
select_eras: [2022_preEE, "2024"]
output_root: results/vjets
max_parallel_eras: 2
production:
  name: vjets_campaign
  exe: build-campaign/analyses/vjet-njetpt/vjet_njetpt
  build:
    source_directory: .
    build_directory: build-campaign
    target: vjet_njetpt
    parallel: 4
    configure_args:
      - -DCMAKE_BUILD_TYPE=Release
      - -DRDF_ANALYSES=vjet-njetpt
      - -DRDF_REQUIRED_EXTENSIONS=cms
  options:
    file_source: rucio
    output_directory: root://eos.example.org//path/to/vjets/campaign
    x509: /path/to/proxy
    root_setup: env.sh
era_options:
  "2024":
    output_directory: root://other.example.org//path/to/vjets/campaign/2024
    request_memory: 4096
metadata_merger: analyses/vjet-njetpt/aux/merge_joint_histograms.py
data_merge_threads: 2
hadd_jobs: 2
xrdcp_jobs: 4
```

Omit `select_eras` to run every child in the collection. Configuration paths,
including paths in the `production` template, are relative to the Snakemake
working directory. `era_options` overrides individual production options for
each listed era. Its `output_directory` is the full destination for that era.
Eras without a destination override use the shared prefix with their era
name appended. You can configure every child and select a subset for a run.
Set `options.root_setup` for the ABI selected at build time.
See the [batch-stage guide](../../docs/BATCH_SUBMISSION.md#standalone-batch-stage-pilot)
for runtime and validation controls.

A Python analysis uses `production.python` with an `entrypoint` that accepts
the staged job config as its positional argument. Remove the `exe` and
`build` entries when selecting this runtime:

```yaml
production:
  name: python_campaign
  python:
    entrypoint: analysis/run.py
    modules:
      helpers.py: analysis/helpers.py
  options:
    output_directory: root://eos.example.org//path/to/python/campaign
```

The preflight syntax-checks the declared sources and prepares the worker
launcher. Use `options.python_env` for the worker's Python dependencies when
needed. The campaign's top-level `python` setting selects the *controller*
interpreter if Snakemake runs from another environment.

### Use existing per-era specs

An explicit `eras` mapping also works. In this mode, paths inside each spec
are relative to the spec itself. Each concurrent era needs a distinct run name
and a distinct remote output destination.

For example, `campaign/2022_preEE.yaml` could contain:

```yaml
version: 1
name: vjets_pilot_2022_preEE
exe: /path/to/vjet_njetpt
submit_config: bundles/2022_preEE/submit_config.yaml
dataset_manifest: bundles/2022_preEE/datasets.yaml
options:
  file_source: rucio
  output_directory: root://eos.example.org//path/to/vjets/pilot/2022_preEE
  x509: /path/to/proxy
```

The configuration and dataset manifest must be members of the same sealed
bundle. Then point the Snakefile at your production specs:

```yaml
output_root: results/vjets
eras:
  2022_preEE: campaign/2022_preEE.yaml
  "2024": campaign/2024.yaml
metadata_merger: analyses/vjet-njetpt/aux/merge_joint_histograms.py
data_merge_threads: 2
```

Set `metadata_merger` for joint V+jets histograms. Omit it to use the framework
merger. The selected merger script is an input to the merge rule, so changing
it rebuilds the merge without changing the production spec. Set `python` to
the framework interpreter path if Snakemake runs from a separate environment.

## Run the campaign

Use the configured framework and ROOT environment. Make the submit-side
credential available to merge through `X509_USER_PROXY` when the output
endpoint requires it. The production spec supplies the worker credential
reference. Preview the campaign before running it:

```bash
export PYTHONPATH="$PWD/core/python${PYTHONPATH:+:$PYTHONPATH}"
python -m production_campaign inspect campaign.yaml
snakemake --snakefile examples/snakemake/Snakefile \
  --configfile campaign.yaml --cores 4 --dry-run
```

The default target merges every selected era. Pick `plan` to materialize the
campaign plan, `preflight` to build and validate the runtimes, `prepare` to
discover and stage inputs, `smoke` to run local one-file tests, `production`
to run or resume all batches, or `merge` for all final merges. The `smoke`
target requires `options.make_test_job`, which defaults to true. Use `submit`
to submit and return without waiting for the production jobs. `verify` checks
all merged artifacts and can refresh XRootD signatures when
`verify_storage: true` is configured.
For an explicit read-only check of existing receipts, invoke
`rdf_production verify` directly. Snakemake builds dependencies requested by
the selected target.

`max_parallel_eras` defaults to two and bounds concurrent batch-rule
invocations. Override it for a run with `--resources condor_runs=1`.
Submission, preparation, production and merge logs are retained under each
era's `logs` directory. Use `data_merge_threads`, `hadd_jobs` and
`xrdcp_jobs` to tune each merge's local resource use. Set `nominal_only: true`
to merge nominal outputs only. Set `semantic_config_compatibility` to a
reviewed JSON policy when explicitly allowing differing configuration
identities in a merge.

Use `python -m production_campaign status campaign.yaml` to see the recorded
batch state, cluster IDs, partition counts and artifact paths for the selected
eras. The `stages` records follow each era through optional stitching, refill,
and fit diagnostics. Each stage lists its expected products and log path.
`pending` means there is no stage output or log, and `incomplete` means a log or
partial output exists without all completion artifacts. Add `--verify` to
check local merge and downstream artifact hashes and report invalid products.
Use `--format text` for a compact summary with log paths for incomplete,
failed, or invalid stages.
The command uses local records and works before submission. Pass `--workdir`
to `status` or `inspect` when using Snakemake's `--directory` option. The same
`status` command accepts a standalone downstream configuration.

For CMS NanoAOD, set `options.file_source` to `rucio` to load the CMS discovery
provider. The framework applies its automatic Condor partition sizing, adaptive
retry, and XRootD read-quality policies. To calibrate the job size with real
Condor jobs before production, set `options.sizing_pilots: true` and choose
`submit`, `production`, or `merge`. The `prepare` and `smoke` targets stay
submission-free, so they reject calibration-enabled specs.

Set `options.file_source: manifest` to partition a manifest's explicit file
lists without querying a catalogue. Rucio, XRootD, and Open Data can reuse an
existing complete inventory via `options.file_source_name`. XRootD discovery
also accepts `xrdfs_server`, `xrdfs_pattern`, and `xrdfs_recursive`.

Choose an intermediate output index or final merge receipt as the Snakemake
target to run only the requested era and its dependencies. After a local
interruption, repeat the command with `--rerun-incomplete` and the same specs so
the framework can resume the existing runs. Treat the executable and bundle
referenced by a spec as immutable. For a physics or runtime change, use a new
run name, spec, and output root.

## Local production

Set `backend: local` in an explicit era spec or `production.backend: local`
in a collection campaign. The default backend is `htcondor`. Local production
uses the same runtime validation, input discovery and partition planning,
then runs every partition on the current host. It writes the ROOT outputs
under each `skimRun_<name>/outputs/` directory and publishes the verified
index used by the existing merge and plot rules. Omit the remote
`output_directory` and Condor `sizing_pilots` from local specs. Explicit and
learned sizing profiles still apply when the selected partition mode uses them.

`workers` sets the number of concurrent local partitions per era, subject
to Snakemake's available `--cores`. Account separately for the ROOT threads
each partition requests in its analysis configuration. Use the `production`
target to run or resume local production, or leave the target unset to include
the merge. The `submit` target is for Condor eras.

## Dask production

Set `backend: dask` in an explicit era spec or `production.backend: dask`
in a collection campaign. Install `dask[distributed]` in the controller
environment and on external workers. The `production` and `merge` targets
use the same prepared partitions and verified output index as local
production. Omit `output_directory` and Condor sizing pilots.

- Set `options.dask_workers` and `options.dask_threads_per_worker` for a local
  Dask cluster. Each defaults to one. Snakemake bounds their product by that
  rule's available cores. ROOT thread settings still apply inside each analysis
  process.
- Set `options.dask_scheduler` to connect to a running cluster, for example
  `tcp://scheduler.example:8786`. That cluster controls its own capacity.
  Its workers need the same shared paths for the checkout, local inputs, and
  `skimRun_<name>` directory. Put `core/python` on the
  worker environment's `PYTHONPATH` so it can import the execution adapter.
  The staged executable, Python environment archive, and ROOT setup are
  used by the analysis subprocess on each worker.

The framework starts local Dask clusters as needed and closes them when
their execution stage ends. Existing schedulers stay running when a stage
disconnects. The `smoke` target still runs a representative job on the
submit host.

## Two-pass stitching

The top-level campaign describes application, using either a collection or
explicit `eras`. Add a second campaign under `stitching.source` for counters.
Use distinct run names and, for Condor passes, non-overlapping remote
destinations. Both campaigns must select the same era keys. Each pass can
choose local, Dask, or HTCondor independently.

```yaml
output_root: results/stitch
eras:
  2022_preEE: specs/application-2022.yaml
  "2024": specs/application-2024.yaml
stitching:
  source:
    eras:
      2022_preEE: specs/counters-2022.yaml
      "2024": specs/counters-2024.yaml
  configs:
    2022_preEE: cfg/stitch-2022.yaml
    "2024": cfg/stitch-2024.yaml
```

Set `options.stitch_mode: none` in the counter specs and use an analysis
configuration that books the signed stitching counters. Set `enableSkim:
false` in the counter bundle if you only need metadata. Leave stitching
options out of the application specs. For each era, the workflow reads
the exact verified counter partitions, derives the correction and cross
sections, then generates the application spec. Use `samples` or `select` in
the [stitching YAML](../../docs/STITCHING.md) so the counter index supplies the
metadata paths, including any split retry outputs.

Use `stitch_counters` or `stitch_weights` to stop at those milestones.
The `stitch` target runs both passes and validates the application, and
the default target also merges the application output. The validation uses
the existing stitching histogram checks and plots. Set
`stitching.require_stitched_histograms: false` for a counter-only validation
that checks each application metadata file and correction group.

For Condor campaigns, `condor_retry_limit` can increase the retry budget
without changing the sealed job specification. With `condor_allow_partial:
true`, an era can publish after the retry budget is exhausted if every sample
has at least one verified partition. Its output index records the failed
partitions and their inputs. For stitching, binding a partial counter result
requires explicit whole-file manifest partitioning. The application starts
from the verified counter file lists. Any later application omissions are
recorded in its output index, and the correction remains the one derived from
the counter inputs.

In a stitching campaign, `plan` shows both pass definitions, and
`preflight`, `prepare`, and `smoke` operate on the counter pass without
submitting jobs. Application runtimes are preflighted together after all
selected eras have produced weights. `submit` therefore waits for the
counter pass and derivation before returning application submission receipts.
Counter stage results are under `.stitch-source/<era>/` within
`output_root`, and the correction, cross sections, effective application spec,
and validation report are under `<era>/stitch/`.

## Downstream analysis

Add a `downstream` mapping keyed by era to extend the production graph from
verified outputs through manifest merges, plots, datacards, fits, and fit diagnostics.
Each era can request its own stages.

```yaml
downstream:
  "2024":
    refill: specs/histfill-2024.yaml
    plots: cfg/manifest-plots-2024.yaml
    datacards:
      config: cfg/cards-2024.yaml
      strict_coverage: true
    fit:
      fitting_backend: combine
      method: FitDiagnostics
      fit_dataset: asimov
      combine_options: "--saveShapes --saveWithUncertainties"
    fit_diagnostics:
      required: [prefit_postfit, correlations]
```

`refill` is a second sealed production spec with a distinct run name.
Set `enableSkim: false` and use the original pass's `saveTree` as its
`treeList`. The rule replaces each selected dataset's file inventory with
the complete set of verified skim files for that dataset. It retains the
refill bundle's other dataset and analysis settings. Local, Dask, and
HTCondor are available. By default the refill reads whole skim files with
one prepared job per selected dataset. Set
`file_source: manifest` on the refill spec to partition its verified skim
inventory using the ordinary planner. For example, `partition: file` runs
one job per skim file. The refill owns this inventory, so leave
`file_source_name` and `sizing_pilots` unset. Use the refill configuration
to define the event weights and histogram definitions for the second pass.

The manifest merge writes one skim ROOT file per dataset and separate
histogram, cutflow, and provenance products. With a refill, histogram,
cutflow, and metadata inputs come from the refill pass. Skim inputs come
from the original pass. The manifest plot rule reads the histogram
manifest and requires a working `rdfanalyzer` plotting binding.

The `datacards` rule reads the same manifest and applies the shared nuisance
coverage controls. The `fit` rule consumes its card and shape products
and accepts `fitting_backend: combine`, `rabbit`, or `analysis`. The
`analysis` backend runs the framework's lightweight chi-square calculation.
Add `analytic_fit: {config: cfg/workspace.yaml, skip_fit: true}` to publish
RooFit workspaces and their cards from the merged histograms. Omit
`skip_fit` to run Combine with the analytic model.

Run `downstream` for all requested stages, or request one of `refill`,
`manifest_merge`, `manifest_plots`, `datacards`, `fits`, `analytic_fits`,
and `fit_diagnostics`.
Outputs are under `output_root/<era>/downstream/`. The existing `merge`
and `plots` targets remain available as the per-dataset merge and native
plotting paths. Each merge, plot, card and fit stage publishes its JSON
receipt beside its product directory after verifying its outputs.

### Fit diagnostics

Add `fit_diagnostics: {}` to plot the saved results for an era. Set
`source: fit` or `source: analytic_fit` if the era runs both.
The rule verifies the selected fit receipt before reading the fit
artifacts. It writes PNGs and a `report.json` with plot paths,
the source artifact ID, available fit status fields, and explanations
for unavailable plots. Add `required` to require diagnostic types from
every fit in the selected stage. A fit with no plottable result fails the
rule. `max_parameters` sets the number displayed per parameter plot and
defaults to 24.

| Diagnostic type | Saved inputs |
| --- | --- |
| `postfit` | Rabbit predictions or Combine `FitDiagnostics` with `--saveShapes`. Includes a data-minus-fit panel and a postfit band when uncertainties were saved. |
| `prefit_postfit` | Requires every postfit plot to include the prefit prediction. |
| `correlations` | A saved covariance from Rabbit, Combine, or analytic Combine. Use `--saveFitResult` with analytic `MultiDimFit`. |
| `pulls` | Fitted nuisances with prefit constraints, such as Rabbit card shape and rate variations or the Combine `FitDiagnostics` nuisance snapshot. |
| `fit_summary` | Saved parameter estimates, Combine limits or significance, or the `analysis` backend's aggregate yield and chi-square comparison. |

For analytic Combine, use a workspace config with positive
`combine_rates` and pass `--saveShapes --saveWithUncertainties` to retain
postfit predictions and uncertainties. Use `required: [postfit, correlations]`
when the analytic model produces only postfit shapes.
The plotting stage uses Matplotlib. Reading Combine shapes requires Uproot,
and reading native RooFit results requires PyROOT.

### Start from saved products

Use `examples/snakemake/DownstreamSnakefile` with only `output_root` and
the `downstream` mapping. Specify `manifest: path/to/output_manifest.yaml`
for an already merged histogram manifest, or `job_manifests: [a.yaml, b.yaml]`
to merge an explicit set of job manifests. Relative ROOT paths resolve
from their manifest files. Start at `fit` with
`saved_datacards: {directory: path/to/datacards, receipt: path/to/datacards.json}`.
Start at `fit_diagnostics` with
`saved_fit: {directory: path/to/fit, receipt: path/to/fit.json}`.
These input receipts come from a completed downstream run. The workflow
validates their product hashes before consumption.

### Fit labeled cards jointly

Add `joint_fit` to fit explicitly selected cards in one likelihood. Each
entry gives the card a unique channel label. Combine's `combineCards.py`
preserves shared nuisance names across cards. Encode shared or era-specific
correlations in the prepared nuisance names and select `correlations: by_name`
to use them as written.

```yaml
joint_fit:
  correlations: by_name
  cards:
    preEE:
      era: 2022_preEE
      card: datacard_SR.txt
    Run2024:
      era: "2024"
      card: datacard_SR.txt
  fit:
    fitting_backend: combine
    method: FitDiagnostics
    fit_dataset: asimov
    combine_options: "--saveShapes --saveWithUncertainties"
  fit_diagnostics:
    required: [postfit, correlations]
```

An `era` entry uses that era's configured `downstream.datacards` stage.
For saved cards, replace `era` with `directory` and `receipt` and keep the
`card` relative to the directory. The standalone Snakefile accepts a
configuration containing only `joint_fit` when all source cards are saved.
The combination stages the verified local shape files beside the combined
card. It requires `combineCards.py` for both the Combine and Rabbit fit
backends. The Rabbit reader requires a uniform `autoMCStats` policy across
channels. Custom external RooWorkspace arguments require a separately prepared
card. Use the `joint_fit` target to run the combination and fit through the
selected diagnostics. Its outputs and receipts are under
`output_root/.joint/`.

## Optional plots

Use the `plots` target to run the native renderer for the eras with declared
plot configs. Each config can point at that era's merged ROOT artifacts.
Changing a plot config reruns the affected plot rule while retaining the
verified merge. The renderer records its output paths in the era's
`plots/report.yaml`.

```yaml
plotting:
  executable: build/core/rdf-plot
  workers: 2
  configs:
    2022_preEE: campaign/plots_2022_preEE.yaml
    "2024": campaign/plots_2024.yaml
```

### Starting from completed products

Use a fresh output directory for the new workflow. Paths in the workflow YAML are relative to the Snakemake working directory. The original controller retains ownership of active jobs and their staging directories.

```bash
snakemake -s examples/snakemake/DownstreamSnakefile --configfile downstream.yaml --cores 4 --dry-run fits
```

| Available input | Stage configuration | Target |
| --- | --- | --- |
| Completed, verified production skims and original run state | `downstream.<era>.saved_production: {spec: source.yaml, index: output_index.json}` and `refill: refill.yaml` | `refill`, then `manifest_merge` |
| Individual job manifests or an already merged manifest | `job_manifests: [a.yaml, b.yaml]` or `manifest: merged.yaml` | `manifest_merge` for job manifests, then `manifest_plots`, `datacards` or `analytic_fits` |
| Completed histogram ROOT files | `datacards: {config: legacy-card-inputs.yaml, raw_inputs: true}` | `datacards` |
| One completed external or LAW datacard | `input_card: old/datacard_SR.txt` and `fit: {fitting_backend: combine}` | `fits` |
| A receipt plus its original datacard or fit directory | `saved_datacards` or `saved_fit` | `fits` or `fit_diagnostics` |
| Local histograms and an rdf-plot YAML | Top-level `plotting` below | `plots` |

Raw datacard generation uses the generator's `input_files.<sample>.path` convention. Local paths resolve against the working directory. Snakemake tracks each local file and the YAML. Remote XRootD URLs are read during generation and must be immutable for a reproducible restart. For a completed datacard, keep local shape files at the paths referenced by its `shapes` declarations. A separate era key can identify each independent fit. Keep external ROOT workspace files referenced by local extArg or rateParam declarations beside their card at the declared paths.

```yaml
output_root: results-from-completed-products
plotting:
  executable: build/core/rdf-plot
  configs: {"2024": cfg/plots-2024.yaml}
  inputs: {"2024": [saved/data.root, saved/background.root]}
downstream:
  "2024":
    input_card: saved/datacard_SR.txt
    fit: {fitting_backend: combine, method: FitDiagnostics}
```

The standalone workflow reads local `metaFile` and per-process `files` dependencies from the rdf-plot YAML. It tracks the primary image, additional outputs and report. Provide `plotting.inputs` to list extra inputs or to select saved histogram files in a full campaign. Omitting an era from that mapping in a full campaign retains its normal merge dependency.

For a standard production merge from a completed verified index, set
`merge_indexes: {"2024": saved/output_index.json}` and target `merge` in the
standalone workflow. This uses the same index validation, metadata merger and
resource options as the campaign rule. It needs the indexed products and their
recorded provenance, and starts zero production jobs.

Explicit production specifications accept YAML (`.yaml` or `.yml`) or existing
JSON. Set `state_root: /shared/campaign-state` in a spec or a collection's
shared `production` template to place controller locks, inventories,
`skimRun_<name>` and local outputs on persistent shared storage. Relative
state roots resolve beside the spec (or against the campaign working directory
for a collection template). Planning leaves the directory untouched. Preflight
or execution creates it. Omitting the key keeps existing claim identities and
the historical checkout location. Leave an active run's state root unchanged.

### Install a relocatable workflow controller

```bash
cmake -S core/python/controller -B build-controller
cmake --install build-controller --component SnakemakeController --prefix "$HOME/rdf-controller"
export RDF_CONTROLLER="$HOME/rdf-controller"
export PYTHONPATH="$RDF_CONTROLLER/python${PYTHONPATH:+:$PYTHONPATH}"
python -m pip install -r "$RDF_CONTROLLER/share/rdfanalyzer/requirements-snakemake.txt"
snakemake -s "$RDF_CONTROLLER/share/rdfanalyzer/snakemake/DownstreamSnakefile" --configfile downstream.yaml --cores 2 -- fits
```

The language-free installer also exposes the smaller `PythonController` component for steering compiled analyses. `SnakemakeController` supplies the stage command modules, workflow files and requirements. Supply experiment-specific discovery providers through their installed Python package or `RDF_EXTENSION_PATHS`. Worker executables and optional native tools (ROOT with hadd, rdf-plot, CMS Combine) come from the selected runtime. Installed controllers default to the current working directory for new mutable state. Set an explicit absolute `state_root` in production for reproducible resume from different working directories.

For event-level inference, use a production spec selecting the inference-enabled
analysis executable and its YAML model configuration. The dataset manifest
can list already-produced skims as its input files. Target `production` for
the resulting event products. For histogram-only inference on verified skims,
use the independent `refill` target with the model configured in the refill
bundle. The same worker runtime loads the model in either workflow. Include
model data and model recipes among the bundle's worker assets. The bundle
compiler follows nested YAML `file` references, for example:

```python
from run_bundle import compile_config_bundle

compile_config_bundle("analysis.yaml", "bundle",
                      asset_keys=("sampleConfig", "bdtConfig"))
```

Here `analysis.yaml` names a dataset manifest under `sampleConfig` and a BDT
model list under `bdtConfig`. Set the production spec's `submit_config` to
`bundle/submit_config.yaml` and `dataset_manifest` to
`bundle/dataset_manifest.yaml`. The worker gets the sealed model file even
when its original analysis checkout is absent.

### Completed LAW products and external wrappers

Import existing per-sample inventory JSON with
`python -m rdfproduction.filelist_manifest --base-manifest datasets.yaml --filelist-dir inventories --output resolved.yaml`.
Existing scripts can continue to call `core/python/law/build_manifest_from_filelist_dir.py`.
An existing dataset manifest named `law_dataset_manifest.yaml` is an ordinary data input.

The compatibility tasks and `requirements-production.txt` remain available for analysis-owned V+jets, VH and stitching wrappers, the separately maintained NLO training workflow, and finishing historical campaigns under their original scheduler ownership. They are excluded from the installed Snakemake component. Preserve existing monitor state, batch IDs, run directory names, and privately maintained submit scripts for those campaigns. New campaigns should use separate writable state and select completed artifacts as inputs. The core framework does not port or restart an externally owned campaign.
