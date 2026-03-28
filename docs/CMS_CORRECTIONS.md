# CMS Correction Stack

This guide documents the current RDFAnalyzerCore workflow for CMS correctionlib payloads and the object-energy managers built on top of it. The goal is to keep analysis code short while still exposing the full correction content from the JSON payloads shipped with the analysis.

## Overview

The correction stack is split into two layers:

1. `CorrectionManager`
   Loads correctionlib JSON entries and turns them into RDF columns.
2. Object-specific managers
   Apply those columns to physics objects, propagate them to MET, and register systematic variations.

In the current implementation this covers:

- Regular correctionlib `corrections`
- correctionlib `compound_corrections`
- Scalar event corrections
- Per-object vector corrections
- Mixed vector/scalar inputs such as `(Jet_eta, Jet_pt, rho)`
- Jet JEC/JER workflows
- Electron scale and smearing workflows
- Muon Rochester and Run 3 scale-resolution workflows
- Config-driven corrected `PhysicsObjectCollection` outputs for jets, fatjets,
  electrons, muons, taus, and photons

## CorrectionManager

`CorrectionManager` is the generic correctionlib adapter.

### What it now supports

- `registerCorrection(...)` auto-detects whether the named JSON entry is a regular correction or a compound correction.
- `applyCorrection(...)` works for both regular and compound corrections.
- `applyCorrectionVec(...)` works for both regular and compound corrections.
- `applyCorrectionVec(...)` accepts mixed per-object and per-event numeric inputs.

That last point is important for CMS payloads. A common JEC/JER pattern is:

- `Jet_area`: `RVec<float>`
- `Jet_eta`: `RVec<float>`
- `Jet_pt`: `RVec<float>`
- `Rho_fixedGridRhoFastjetAll`: scalar `float`

The manager now broadcasts the scalar input across the jet loop automatically. Analysis code no longer needs to hand-build `RVec<RVec<double>>` inputs for this case.

### Registration pattern

```cpp
auto* cm = analyzer.getPlugin<CorrectionManager>("correctionManager");

cm->registerCorrection(
    "jec_nominal",
    "jet_jerc.json.gz",
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
```

The same API also works for a regular correction:

```cpp
cm->registerCorrection(
    "jer_sf",
    "jet_jerc.json.gz",
    "Summer22_22Sep2023_JRV1_MC_ScaleFactor_AK4PFPuppi",
    {"Jet_eta", "Jet_pt_corr_jec"});
```

### Scalar application

Use `applyCorrection(...)` when every numeric input is event-level scalar data.

```cpp
cm->applyCorrection(
    "pileup_weight",
    {"nominal"},
    {},
    "pileup_weight_nominal");
```

### Vector application

Use `applyCorrectionVec(...)` when at least one input is an object collection.

```cpp
cm->applyCorrectionVec(
    "muon_id_sf",
    {"nominal"},
    {},
    "muon_id_sf_nominal_vec");
```

### Compound-correction conventions

Compound corrections are not a special case in analysis code anymore, but two payload conventions matter:

1. Compound JEC entries like `..._L1L2L3Res_AK4PFPuppi` usually take only numeric inputs.
2. Compound electron scale entries like `Scale` can still have leading string arguments such as `"scale"`.

Examples:

```cpp
cm->applyCorrectionVec("jec_nominal", {}, {}, "Jet_jec_sf_nominal");
cm->applyCorrectionVec("electron_scale_data", {"scale"}, {}, "electron_scale_sf");
```

## JetEnergyScaleManager

`JetEnergyScaleManager` turns JEC/JER scale factors into corrected jet and MET columns.

### Recommended Run 2 / Run 3 pattern

1. Define raw jet columns.
2. Evaluate nominal JEC with `CorrectionManager`.
3. Apply the nominal JEC with `JetEnergyScaleManager::applyCorrection(...)`.
4. Register JES sources as separate corrections and apply each up/down shift.
5. Register JER resolution and scale-factor payloads.
6. Call `setJERSmearingColumns(...)` once.
7. Schedule `applyJERSmearing(...)` for nominal, up, and down.
8. Propagate nominal and shifted jets to MET.

### New JER API

The manager now supports explicit JER smearing from the CMS resolution and scale-factor payloads:

```cpp
jes->setJERSmearingColumns(
    "Jet_genMatchedPt",
    "Rho_fixedGridRhoFastjetAll",
    "event");

jes->applyJERSmearing(
    *cm,
    "jer_pt_resolution",
    "jer_scale_factor",
    "Jet_pt_corr_jec",
    "Jet_pt_corr_nominal",
    "nom",
    true,
    "Jet_mass_corr_jec",
    "Jet_mass_corr_nominal");
```

This is the preferred entry point when the JSON provides:

- a pt-resolution correction
- a scale-factor correction
- a stochastic fallback for unmatched jets

The implementation derives reproducible random numbers from the event id and jet index, so repeated runs are stable.

### JES source coverage

Many JME payloads store JES sources as separate correction names rather than a single correction with the source passed as a string input. RDFAnalyzerCore supports that pattern directly:

- register one correction per source
- evaluate the up/down scale factors with `CorrectionManager`
- feed those scale factors into `JetEnergyScaleManager::applyCorrection(...)`

This is the pattern used by the updated VHqq analysis.

## ElectronEnergyScaleManager and ObjectEnergyManagerBase

`ElectronEnergyScaleManager` is a thin object-specific wrapper around `ObjectEnergyManagerBase`.

Use the shared base functionality for:

- multiplicative scale corrections
- additive resolution smearing
- reproducible Gaussian random numbers
- MET propagation
- `PhysicsObjectCollection` outputs and per-variation collections

### Reproducible smearing

For MC smearing, first define a deterministic Gaussian column:

```cpp
electronEnergyManager->defineReproducibleGaussian(
    "electron_smear_random",
    "Electron_pt",
    "run",
    "luminosityBlock",
    "event",
    "electron_smear");
```

Then apply the resolution payload output as the sigma:

```cpp
electronEnergyManager->applyResolutionSmearing(
    "Electron_pt",
    "electron_smear_sigma_nominal",
    "electron_smear_random",
    "Electron_pt_corr_nominal");
```

### Run 3 electron payload pattern

The Run 3 electron payloads used by VHqq expose:

- `SmearAndSyst` as a regular correction
- `Scale` as a compound correction

Typical usage is:

```cpp
cm->registerCorrection(
    "electron_smear",
    electronJson,
    "SmearAndSyst",
    {"Electron_pt", "Electron_r9", "Electron_scEta"});

cm->registerCorrection(
    "electron_scale",
    electronJson,
    "Scale",
    {"run", "Electron_scEta", "Electron_r9", "Electron_pt", "Electron_seedGain"});
```

For data, the nominal scale comes from the compound correction. For MC, the payload typically provides smearing plus scale-uncertainty terms that you can convert into up/down multiplicative factors before calling `applyCorrection(...)`.

## Corrected full-object collection plugins

The new corrected collection wrappers sit above `JetEnergyScaleManager` and
`ObjectEnergyManagerBase`-derived managers.

Available plugins:

- `CorrectedJetCollectionManager`
- `CorrectedFatJetCollectionManager`
- `CorrectedElectronCollectionManager`
- `CorrectedMuonCollectionManager`
- `CorrectedTauCollectionManager`
- `CorrectedPhotonCollectionManager`

These plugins are intentionally thin. They do three things:

1. Build a full `PhysicsObjectCollection` input if the analysis only has raw
    `(pt, eta, phi, mass)` branches.
2. Optionally replay a declarative `workflowConfig` into `CorrectionManager`
    plus the underlying correction manager.
3. Ask the underlying correction manager to materialize a nominal corrected
    collection plus per-systematic varied collections.
4. Register the nominal collection base name with `SystematicManager` so
    analyses can consume the collection directly under systematic substitution.

The systematic contract is now the same for jets, leptons, taus, and photons:

- a base systematic family such as `jes_total`, `ees`, `stat`, or `pes` is
    registered once
- explicit directional mappings connect the nominal corrected branch or
    collection to the real source columns for `...Up` and `...Down`
- downstream `IDataFrameProvider::Define(...)` calls can consume the nominal
    branch or collection name and automatically receive derived
    `..._systUp` and `..._systDown` outputs

This is what allows analyses like VHqq to write selection code directly against
`CorrectedJets`, `CorrectedElectrons`, and `CorrectedMuons` while still getting
systematic propagation for later derived quantities.

### Config format

Each wrapper uses a small pair-based config file:

```text
ptColumn=Jet_pt
etaColumn=Jet_eta
phiColumn=Jet_phi
massColumn=Jet_mass
correctedPtColumn=Jet_pt_corr_nominal
correctedMassColumn=Jet_mass_corr_nominal
outputCollection=CorrectedJets
variationMapColumn=CorrectedJets_variations
workflowConfig=cfg/corrected_jets_workflow.txt
```

If `inputCollection` is omitted, the plugin auto-builds one from
`ptColumn/etaColumn/phiColumn/massColumn`. If `inputCollection` is provided,
the raw component columns are not needed.

If `workflowConfig` is present, the wrapper executes its rows in order during
`setupFromConfigFile()`. Each row must define `type=...`. Optional
`sample=mc|data|all` gating lets one workflow file encode MC and data branches.
Values of the form `${key}` resolve against either the top-level analysis
config or corrected-wrapper spec values such as `${correctedPtColumn}` and
`${outputCollection}`.

This lets analyses move setup code such as correction registration,
`setJetColumns()`, `setObjectColumns()`, `removeExistingCorrections()`,
`applyCorrectionlib()`, `applyJERSmearing()`, `defineReproducibleGaussian()`,
`applyResolutionSmearing()`, `applyScaleAndResolution()`, `addVariation()`, and
`propagateMET()` out of analysis C++ and into declarative configs while keeping
the same `CorrectedJets`, `CorrectedElectrons`, and `CorrectedMuons` interface.

### Systematic behavior

The wrapper plugins now register collection-level systematic propagation using
the same `systematicNameUp` / `systematicNameDown` convention as branch-level
corrections. That means a collection like `CorrectedJets` cleanly maps to:

- `CorrectedJets_jes_totalUp`
- `CorrectedJets_jes_totalDown`
- `CorrectedJets_jerUp`
- `CorrectedJets_jerDown`

Internally this is implemented through `ISystematicManager::registerVariationColumns(...)`,
which maps a nominal corrected branch or collection name to explicit Up/Down
source columns when those source names do not follow the default
`nominalName_variationLabel` pattern.

This removes the need for analyses to manually thread corrected `pt` and `mass`
 arrays through every selection helper.

### Mass-aware variation collections

Variation collection outputs now preserve corrected masses whenever the
underlying variation defines both `upMassColumn` and `downMassColumn`.
Previously, varied collections updated only `pt`, which was insufficient for
physics-object consumers operating directly on four-vectors.

### VHqq usage

The VHqq RDF analysis now configures:

- `CorrectedJets`
- `CorrectedElectrons`
- `CorrectedMuons`

and uses those corrected collections directly in the lepton and jet selection
helpers. This reduces branch-level boilerplate while keeping the existing
candidate-building logic intact.

### VHqq JES mode switch

The VHqq corrected-jet workflow is now driven from
`analyses/VHbbcc/VHqqRDF/cfg/corrected_jets_workflow.txt`, and the number of
JES families it activates is controlled from
`analyses/VHbbcc/VHqqRDF/cfg/cfg.yaml` with:

```yaml
jesSystematicsMode: "single"
```

Supported values are:

- `single`
    Activates only the base `jes_total` family.
- `full`
    Activates the year-aware VHqq JES source list from
    `VHqqConfig.cc`.

In practice:

- `single` produces one JES family and two directional collection/branch
    variations: `...jes_totalUp` and `...jes_totalDown`.
- `full` expands to the full VHqq source list for the chosen year, which is
    currently the `recommendedJesSources(...)` list in `VHqqConfig.cc`.
    For Run 2 and Run 3 years in this analysis, that is typically about 12 JES
    families, which means about 24 up/down directional variations.

To switch VHqq from the base single-JES setup to the fuller source set, change
only the top-level config value:

```yaml
jesSystematicsMode: "full"
```

No C++ changes are needed for that switch. The analysis populates the
year-dependent source names into the corrected-jet workflow placeholders before
the plugins are constructed, and the corrected wrapper replays the corresponding
workflow rows automatically.

If you want to change which sources are included in `full`, update
`recommendedJesSources(...)` in `analyses/VHbbcc/VHqqRDF/VHqqConfig.cc`.

## MuonRochesterManager

`MuonRochesterManager` supports two workflows.

### Traditional Rochester correctionlib workflow

Use:

- `setObjectColumns(...)`
- `setRochesterInputColumns(...)`
- `applyRochesterCorrection(...)`
- `applyRochesterSystematicSet(...)`

This remains the right interface for the classic Rochester payloads.

### Run 3 split-schema scale and resolution workflow

The newer CMS muon payloads can be scheduled directly with:

```cpp
muonRochesterManager->setScaleResolutionEventColumns(
    "luminosityBlock",
    "event");

muonRochesterManager->applyScaleAndResolution(
    muonJson,
    false,
    "Muon_pt",
    "Muon_pt_corr_nominal",
    "nom",
    "nom");
```

Supported variations are:

- scale: `nom`, `up`, `down`
- resolution: `nom`, `up`, `down`

This path is useful when the JSON contains separate `a_*`, `m_*`, `k_*`, `cb_params`, and `poly_params` corrections rather than a single Rochester-style wrapper entry.

## Manager execution model

The object-energy managers use deferred scheduling. Calls like `applyCorrection(...)`, `applyResolutionSmearing(...)`, `propagateMET(...)`, and `addVariation(...)` only queue work until `execute()` runs.

Two details matter for analysis authors:

1. You may call `execute()` explicitly if downstream `Define(...)` calls need the corrected columns immediately.
2. `ObjectEnergyManagerBase` and `JetEnergyScaleManager` now clear their queued steps after execution, so explicit execution is idempotent and does not re-register the same transformations repeatedly.

That makes the following pattern safe:

```cpp
electronEnergyManager->applyResolutionSmearing(...);
electronEnergyManager->execute();

analyzer.Define("leadElectronPt", [](const ROOT::VecOps::RVec<float>& pt) {
    return pt.empty() ? 0.f : pt[0];
}, {"Electron_pt_corr_nominal"});
```

## Minimal-code design pattern

When possible, keep user code split like this:

1. `CorrectionManager`
   Only registers payload entries and produces RDF columns.
2. Object-energy manager
   Only consumes those columns to build corrected objects, mass shifts, MET shifts, and systematic variations.
3. Analysis logic
   Only references the final nominal and shifted columns.

That separation is what allows the VHqq analysis to use more CMS payload content without embedding correctionlib-specific logic directly in the physics code.

## Related documentation

- [API Reference](API_REFERENCE.md)
- [Jet Energy Corrections](JET_ENERGY_CORRECTIONS.md)
- [Physics Objects](PHYSICS_OBJECTS.md)