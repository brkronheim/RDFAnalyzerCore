# Generic Stitching Support

RDFAnalyzerCore provides reusable infrastructure for analyses that combine
overlapping Monte Carlo samples. The analysis remains responsible for defining
the event-level stitch-bin classifier and for choosing physically meaningful
sample groups and cross sections.

## Setup and Build

The stitching workflow requires ROOT, the Core C++ library, the analysis
executable, and the LAW Python dependencies. On the supported CVMFS host:

```bash
cd RDFAnalyzerCore
source env.sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-production.txt
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DBUILD_TESTS=ON \
  -DBUILD_COMBINE=OFF \
  -DBUILD_COMBINE_HARVESTER=OFF \
  -DONNXRUNTIME_USE_CUDA=OFF \
  -DUSE_CUDA=OFF
cmake --build build --target vjetStitch testVjetStitch -j"$(nproc)"
source core/python/law/env.sh
law index
```

`source build.sh` performs the same environment setup and a complete build.
Building only `vjetStitch` and `testVjetStitch` is useful when unrelated
analysis targets are not needed. The executable is written to
`build/analyses/rdfAnalyzerStitch/vjetStitch`.

## Core Contract

An analysis should:

1. Define an integer column with one value per event.
2. Use `Analyzer::bookCounterIntHistogram` before the event loop.
3. Configure `counterWeightBranch` and `counterIntWeightBranch` when using the
   automatic CounterService booking path.
4. Run `StitchingDerivationTask` over the resulting metadata files.
5. Apply the generated correctionlib correction with the sample name and the
   integer stitch-bin column.

The signed counter written by CounterService is named
`counter_intWeightSignSum_<sample>`. The derivation computes, independently in
each integer bin,

```
b_n(k) = max(C_n(k), 0) / sum_m max(C_m(k), 0)
```

where `C_n(k)` is the net signed event count for sample `n`. Existing
underflow/filter handling is intentionally left to the analysis classifier and
its configured filters.

The Core/analysis boundary is deliberately narrow. Core owns counter
aggregation, signed-count derivation, correctionlib construction, nested
inclusive-anchor resolution, Cartesian coordinate flattening, and generic
validation rendering. An analysis owns event branches, physics-specific
classification, sample-range exclusions, veto policy, campaign layouts,
cross-section values, and any labels or boundaries that cannot be inferred
from histogram metadata.

An analysis-specific validation adapter can be selected with
`validation.adapter` in the stitch YAML. The adapter is loaded only for
presentation and analysis-owned exclusions; it does not change the Core
counter or correction contract. For example, the V+jets configs select
`analyses.rdfAnalyzerStitch.stitching_validation`, while a generic analysis can
omit the adapter and provide boundaries directly under `validation`.

The complete concrete V+jets workflow, configuration matrix, branch contract,
Run 2/Run 3 behavior, campaign wrappers, and troubleshooting guide is in the
[rdfAnalyzerStitch analysis README](../analyses/rdfAnalyzerStitch/README.md).

## Cartesian Coordinates

The public `CartesianBinning` utility in
`core/interface/CartesianBinning.h` handles only the generic coordinate part
of multidimensional binning:

- construct it from the number of bins on each axis;
- flatten coordinates with the last axis changing fastest;
- unflatten an integer ID for inspection;
- use `flattenOptional` when an analysis classifier has an invalid coordinate;
- use `NumericBinningAxis` for finite, half-open numeric bins with an open final
  bin.

`NumericBinningAxis` takes lower boundaries, so `[0, 40, 100]` represents
`[0,40)`, `[40,100)`, and `[100,Inf)`. Values below the first boundary and
non-finite values return no coordinate. `CartesianBinning` uses the last axis
as the fastest-changing coordinate, which makes a layout such as
`{3, n_pt_bins}` directly compatible with three nJet slices.

Analyses can map event values to axis coordinates using their own categories,
edges, filters, and physics policy. They can also bypass this utility and
provide a custom classifier when the binning is not Cartesian.

## Inclusive Anchors

The optional `inclusive_sample` field names one inclusive anchor. The
`inclusive_samples` field accepts a flat or nested collection and sums all
listed signed counters before deriving bin fractions. A mapping can select a
different anchor collection for each configured cross-section era:

```yaml
groups:
  generic_group:
    meta_files:
      sample_a: outputs/sample_a/meta.root
      sample_b: outputs/sample_b/meta.root
      sample_c: outputs/sample_c/meta.root
    inclusive_samples:
      run3_22_23: sample_a
      run3_24:
        - [sample_a, sample_b]
        - sample_c

inclusive_cross_sections:
  generic_group:
    run3_22_23: 100.0
    run3_24: 100.0
```

Core does not provide process-specific cross-section defaults. If an inclusive
anchor or cross section is not configured, the correction factors can still be
derived, but the cross-section report needed for normalized comparison plots
is omitted.

## Validation Plots

`StitchingValidationTask` retains compatibility defaults for the existing
V+jets validation histograms. Other analyses can declare their own histogram
pairs in the stitch YAML:

```yaml
validation:
  histograms:
    - name: observable
      stitched_name: observable_stitched
      label: Observable
      boundaries: [10.0, 25.0]
      xlim: [0.0, 100.0]
      ratio_range: [0.8, 1.2]
      log_y: true
```

The validator uses these names for output checks and, when cross-section
information is available, produces inclusive-versus-stitched plots. Analyses
with validation requirements that cannot be inferred from histogram metadata
may keep a dedicated plotting adapter.

The reusable renderer is `core/python/stitching_plotting.py`. It provides the
three-panel comparison used by `StitchingValidationTask`, including weighted
statistical error bars, independent numerator/denominator ratio uncertainties,
and a statistical pull panel. V+jets process labels, year grouping, pT
boundaries, and output names remain in `core/python/law/analysis_tasks.py`. The
old exploratory files under `analyses/rdfAnalyzerStitch/oldPlot` are not runtime
dependencies.

Validation writes `stitchValidation_<name>/stitch_validation.json` and one
comparison image per process/year/observable, for example
`stitch_DY_UL_LHE_Vpt_comparison.png`. Each plot compares the configured
inclusive anchor collection against the sum of all normalized stitched
samples. The report also records active-bin normalization and excluded bins.

## LAW Workflow

`StitchingApplyTask` runs the counter pass, derivation, and application pass in
one dependency graph. A direct local example is:

```bash
source core/python/law/env.sh
law run StitchingApplyTask \
  --module analysis_tasks \
  --local-scheduler \
  --workers 1 \
  --workflow local \
  --submit-config analyses/rdfAnalyzerStitch/cfg/cfg_22.txt \
  --stitch-config analyses/rdfAnalyzerStitch/cfg/stitch_config.yaml \
  --dataset-manifest analyses/rdfAnalyzerStitch/cfg/samples_22.yaml \
  --exe build/analyses/rdfAnalyzerStitch/vjetStitch \
  --name vjetStitch22
```

The counter-only task can be run separately when its metadata already exists:

```bash
law run StitchingDerivationTask \
  --module analysis_tasks \
  --stitch-config analyses/rdfAnalyzerStitch/cfg/stitch_config.yaml \
  --name vjetStitch22 \
  --source-skim-name vjetStitch22_counts \
  --source-submit-config analyses/rdfAnalyzerStitch/cfg/cfg_22.txt \
  --source-exe build/analyses/rdfAnalyzerStitch/vjetStitch \
  --source-dataset-manifest analyses/rdfAnalyzerStitch/cfg/samples_22.yaml
```

Validate an application run with:

```bash
law run StitchingValidationTask \
  --module analysis_tasks \
  --local-scheduler \
  --name vjetStitch22 \
  --stitch-config analyses/rdfAnalyzerStitch/cfg/stitch_config.yaml \
  --submit-config analyses/rdfAnalyzerStitch/cfg/cfg_22.txt \
  --dataset-manifest analyses/rdfAnalyzerStitch/cfg/samples_22.yaml
```

Use `--workflow dask` with `--dask-scheduler` and `--dask-workers`, or
`--workflow htcondor` with `--x509`, `--root-setup`, and `--container-setup`.
For remote file discovery, add `--file-source xrdfs` or `--file-source rucio`
and a matching `--file-source-name`; `PrepareSkimJobs` then materializes the
discovered file lists before partitioning jobs. Common tuning parameters are
`--files-per-job`, `--partition`, `--entries-per-job`, `--stage-in`,
`--max-runtime`, and `--max-retries`.

The standard outputs are:

* `skimRun_<name>_counts/` – counter-pass jobs and metadata;
* `stitchWeights_<name>/stitch_weights.json` – correctionlib factors;
* `stitchWeights_<name>/stitch_cross_sections.json` – optional binned cross sections;
* `skimRun_<name>/` – application jobs and stitched metadata;
* `stitchValidation_<name>/` – validation report and comparison plots.

The V+jets campaign wrappers in
`analyses/rdfAnalyzerStitch/submit_run2.sh` and
`analyses/rdfAnalyzerStitch/submit_run3.sh` select the correct campaign
manifests and stitch configuration, use Rucio discovery, and run validation
after application. Set `RDF_STITCH_X509`, `RDF_STITCH_EXE`,
`RDF_STITCH_CONTAINER`, `RDF_STITCH_FILES_PER_JOB`, and
`RDF_STITCH_WORKERS` to override their defaults. `RDF_STITCH_ROOT_SETUP`,
`RDF_STITCH_MAX_RETRIES`, and `RDF_STITCH_LAW` override the ROOT setup,
Condor retry limit, and LAW executable respectively. Set
`RDF_STITCH_RESET_APPLICATION=1` to remove local application and validation
state before resubmitting with a rebuilt executable; counter and derived-weight
directories are preserved. The default is `0`, which preserves resumable
application state. The wrappers require a valid VOMS proxy and
`voms-proxy-info`; set `RDF_STITCH_X509` to the proxy path when the repository-
local `x509` path is not appropriate. Pass `--print-deps 1` to inspect all
campaign task graphs without submitting analysis jobs; this mode does not
remove state. After a
Condor run, validation refuses to consume partial application output when
`skimRun_<name>/all_outputs_verified.txt` reports permanent failures. Inspect
that summary and the per-dataset Condor logs, then rerun the wrapper after
repairing the failed inputs or increasing `RDF_STITCH_MAX_RETRIES`.
VOMS proxy and `voms-proxy-info`.