# JetEnergyScaleManager Reference

> **Plugin class**: `JetEnergyScaleManager`
> **Header**: `core/plugins/JetEnergyScaleManager/JetEnergyScaleManager.h`
> **Source**: `core/plugins/JetEnergyScaleManager/JetEnergyScaleManager.cc`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Quick Start](#2-quick-start)
3. [Jet Column Configuration](#3-jet-column-configuration)
4. [MET Column Configuration](#4-met-column-configuration)
5. [Removing Existing Corrections](#5-removing-existing-corrections)
6. [Applying Corrections](#6-applying-corrections)
7. [CMS Systematic Source Sets](#7-cms-systematic-source-sets)
8. [Type-1 MET Propagation](#8-type-1-met-propagation)
9. [PhysicsObjectCollection Integration](#9-physicsobjectcollection-integration)
10. [Systematic Variation Registration](#10-systematic-variation-registration)
11. [Accessors](#11-accessors)
12. [Lifecycle Hooks](#12-lifecycle-hooks)
13. [Provenance and Metadata](#13-provenance-and-metadata)
14. [Complete CMS NanoAOD Workflow](#14-complete-cms-nanoaod-workflow)
15. [API Summary](#15-api-summary)

---

## 1. Overview

`JetEnergyScaleManager` is a framework plugin that manages Jet Energy Scale
(JES) and Jet Energy Resolution (JER) corrections for CMS-style NanoAOD
analyses.  It operates on ROOT `RDataFrame` columns and integrates with
the framework's systematic-variation infrastructure.

### Key capabilities

| Feature | API |
|---------|-----|
| Strip existing NanoAOD JEC | `removeExistingCorrections(rawFactorColumn)` |
| Apply scale-factor correction | `applyCorrection(inputPt, sfCol, outputPt)` |
| Apply correctionlib correction | `applyCorrectionlib(cm, name, args, inputPt, outputPt)` |
| Register named CMS source sets | `registerSystematicSources(setName, sources)` |
| Apply entire source set at once | `applySystematicSet(cm, name, setName, inputPt, prefix)` |
| Type-1 MET propagation | `propagateMET(basePt, basePhi, nomPt, varPt, outPt, outPhi)` |
| Input jet collection (POC) | `setInputJetCollection(column)` |
| Corrected jet collection output | `defineCollectionOutput(correctedPt, outputCol)` |
| Per-variation collections + map | `defineVariationCollections(nomCol, prefix, mapCol)` |
| Register up/down variation | `addVariation(name, upPt, downPt)` |

### Design philosophy

All configuration calls (e.g. `removeExistingCorrections`, `applyCorrection`,
`defineCollectionOutput`) are **deferred**: they register work to be done when
`execute()` is called.  `execute()` is invoked automatically by the framework
after all plugins are configured.

---

## 2. Quick Start

```cpp
#include <JetEnergyScaleManager.h>
#include <PhysicsObjectCollection.h>

// Assume analyzer already has a JetEnergyScaleManager plugin registered.
auto* jes = analyzer.getPlugin<JetEnergyScaleManager>("jes");
auto* cm  = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare jet and MET column names.
jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
jes->setMETColumns("MET_pt", "MET_phi");

// 2. Strip NanoAOD JEC to get raw pT.
jes->removeExistingCorrections("Jet_rawFactor");
// → Jet_pt_raw, Jet_mass_raw defined in execute()

// 3. Apply new L1L2L3 JEC via correctionlib.
jes->applyCorrectionlib(*cm, "jec_l1l2l3", {"L3Residual"},
                        "Jet_pt_raw", "Jet_pt_jec");
// → Jet_pt_jec, Jet_mass_jec defined in execute()

// 4. Register reduced systematic set and apply.
jes->registerSystematicSources("reduced", {"Total"});
jes->applySystematicSet(*cm, "jes_unc", "reduced", "Jet_pt_jec", "Jet_pt_jes");
// → Jet_pt_jes_Total_up, Jet_pt_jes_Total_down defined in execute()

// 5. Propagate JEC to MET.
jes->propagateMET("MET_pt", "MET_phi",
                  "Jet_pt_raw", "Jet_pt_jec",
                  "MET_pt_jec", "MET_phi_jec");
// → MET_pt_jec, MET_phi_jec defined in execute()
```

---

## 3. Jet Column Configuration

```cpp
void setJetColumns(const std::string& ptColumn,
                   const std::string& etaColumn,
                   const std::string& phiColumn,
                   const std::string& massColumn);
```

Declares the names of the four per-jet kinematic columns in the RDataFrame.
All four are `RVec<Float_t>` columns with one entry per jet in the event.

- `ptColumn`  — transverse momentum (e.g. `"Jet_pt"`); **required**, must
  not be empty.
- `etaColumn` — pseudorapidity (e.g. `"Jet_eta"`).
- `phiColumn` — azimuthal angle (e.g. `"Jet_phi"`); also used as the source
  of jet directions for MET propagation.
- `massColumn` — invariant mass (e.g. `"Jet_mass"`); pass an empty string to
  disable automatic mass-column derivation.

Must be called before `execute()`.

**Throws** `std::invalid_argument` if `ptColumn` is empty.

---

## 4. MET Column Configuration

```cpp
void setMETColumns(const std::string& metPtColumn,
                   const std::string& metPhiColumn);
```

Declares the base Missing Transverse Energy (MET) columns for use in
metadata logging and provenance recording.  These do not need to match the
columns passed to `propagateMET()`.

- `metPtColumn`  — scalar MET-pT column (`Float_t`), e.g. `"MET_pt"`.
- `metPhiColumn` — scalar MET-φ column (`Float_t`), e.g. `"MET_phi"`.

**Throws** `std::invalid_argument` if either argument is empty.

---

## 5. Removing Existing Corrections

### `removeExistingCorrections`

```cpp
void removeExistingCorrections(const std::string& rawFactorColumn);
```

Schedules the definition of raw-pT and raw-mass columns by undoing the
NanoAOD-level JEC:

```
Jet_pt_raw   = Jet_pt   × (1 − rawFactorColumn)
Jet_mass_raw = Jet_mass × (1 − rawFactorColumn)
```

CMS NanoAOD convention: `rawFactorColumn = "Jet_rawFactor"` where
`Jet_rawFactor = 1 − pt_raw / pt_jec`.

The output column names are `<ptColumn>_raw` and `<massColumn>_raw`
(derived automatically from the names set in `setJetColumns`).

**Throws**:
- `std::invalid_argument` if `rawFactorColumn` is empty.
- `std::runtime_error` if `setRawPtColumn()` was already called.

### `setRawPtColumn`

```cpp
void setRawPtColumn(const std::string& rawPtColumn);
```

Alternative to `removeExistingCorrections`: declare that a raw-pT column
already exists in the dataframe (e.g. pre-computed by user code).  Mutually
exclusive with `removeExistingCorrections`.

**Throws**:
- `std::invalid_argument` if `rawPtColumn` is empty.
- `std::runtime_error` if `removeExistingCorrections()` was already called.

---

## 6. Applying Corrections

### `applyCorrection`

```cpp
void applyCorrection(const std::string& inputPtColumn,
                     const std::string& sfColumn,
                     const std::string& outputPtColumn,
                     bool applyToMass = true,
                     const std::string& inputMassColumn  = "",
                     const std::string& outputMassColumn = "");
```

Schedules an element-wise pT (and optionally mass) multiplication:

```
outputPtColumn   = inputPtColumn   × sfColumn
outputMassColumn = inputMassColumn × sfColumn  // when applyToMass == true
```

When `inputMassColumn` or `outputMassColumn` are empty, names are derived
automatically by replacing the `ptColumn_m` prefix in the pt column names
with `massColumn_m`.  For example, if `ptColumn = "Jet_pt"` and
`massColumn = "Jet_mass"`, then `"Jet_pt_raw"` → `"Jet_mass_raw"`.

**Throws** `std::invalid_argument` if any required column name is empty.

### `applyCorrectionlib`

```cpp
void applyCorrectionlib(CorrectionManager& cm,
                        const std::string& correctionName,
                        const std::vector<std::string>& stringArgs,
                        const std::string& inputPtColumn,
                        const std::string& outputPtColumn,
                        bool applyToMass = true,
                        const std::string& inputMassColumn  = "",
                        const std::string& outputMassColumn = "",
                        const std::vector<std::string>& inputColumns = {});
```

Evaluates a CMS correctionlib correction via `CorrectionManager::applyCorrectionVec`,
then schedules the pT/mass multiplication.  The intermediate SF column name
follows `CorrectionManager`'s naming convention:

```
sfColumnName = correctionName + "_" + stringArgs[0] + "_" + stringArgs[1] + ...
```

For CMS JES/JER uncertainty sources the `stringArgs` follow the convention
`{sourceName, "up"}` or `{sourceName, "down"}`.

**Example: apply L1L2L3 JEC**
```cpp
jes->applyCorrectionlib(*cm, "jec_l1l2l3", {"L3Residual"},
                        "Jet_pt_raw", "Jet_pt_jec");
// Intermediate SF column: "jec_l1l2l3_L3Residual"
// Output column:          "Jet_pt_jec"
```

---

## 7. CMS Systematic Source Sets

The CMS jet energy uncertainty framework uses named *source* strings that
identify individual uncertainty contributions.  The `JetEnergyScaleManager`
supports registering named lists of sources and applying them all at once.

### `registerSystematicSources`

```cpp
void registerSystematicSources(const std::string& setName,
                               const std::vector<std::string>& sources);
```

Registers a named list of CMS JES/JER uncertainty source names.  Multiple
calls with the same `setName` replace the previous list.

Typical CMS source sets:

| Set name | Sources |
|----------|---------|
| `"full"` | All 17 individual JES uncertainty sources: `AbsoluteCal`, `AbsoluteScale`, `AbsoluteMPFBias`, `FlavorQCD`, `Fragmentation`, `PileUpDataMC`, `PileUpPtRef`, `RelativeFSR`, `RelativeJEREC1`, `RelativeJEREC2`, `RelativeJERHF`, `RelativePtBB`, `RelativePtEC1`, `RelativePtEC2`, `RelativePtHF`, `RelativeBal`, `RelativeSample` |
| `"reduced"` | Combined: `{"Total"}` |
| `"jer"` | JER smearing: `{"JER"}` |

**Throws**:
- `std::invalid_argument` if `setName` is empty, `sources` is empty, or any
  element of `sources` is empty.

### `getSystematicSources`

```cpp
const std::vector<std::string>& getSystematicSources(
    const std::string& setName) const;
```

Returns the list of sources registered under `setName`.

**Throws** `std::out_of_range` if `setName` was not registered.

### `applySystematicSet`

```cpp
void applySystematicSet(CorrectionManager& cm,
                        const std::string& correctionName,
                        const std::string& setName,
                        const std::string& inputPtColumn,
                        const std::string& outputPtPrefix,
                        bool applyToMass = true,
                        const std::vector<std::string>& inputColumns = {},
                        const std::string& inputMassColumn = "");
```

For each source `S` in the registered set `setName`:

1. Calls `cm.applyCorrectionVec(correctionName, {S, "up"}, inputColumns)`
   → SF column `correctionName_S_up`.
2. Calls `cm.applyCorrectionVec(correctionName, {S, "down"}, inputColumns)`
   → SF column `correctionName_S_down`.
3. Schedules pT/mass corrections:
   - `outputPtPrefix_S_up`
   - `outputPtPrefix_S_down`
4. Registers the variation `S` with up = `outputPtPrefix_S_up`, down =
   `outputPtPrefix_S_down`.

The string argument order for each source is `{S, "up"}` / `{S, "down"}` as
required by the CMS correctionlib uncertainty format.

**Throws**:
- `std::runtime_error` if `setName` is not registered.
- `std::invalid_argument` if any mandatory argument is empty.

**Example: apply full CMS JES set**

```cpp
jes->registerSystematicSources("full", {
    "AbsoluteCal", "AbsoluteScale", "AbsoluteMPFBias",
    "FlavorQCD", "Fragmentation", "PileUpDataMC",
    "PileUpPtRef", "RelativeFSR", "RelativeJEREC1",
    "RelativeJEREC2", "RelativeJERHF",
    "RelativePtBB", "RelativePtEC1", "RelativePtEC2",
    "RelativePtHF", "RelativeBal", "RelativeSample"
});
jes->applySystematicSet(*cm, "jes_unc", "full", "Jet_pt_jec", "Jet_pt_jes");
// Produces columns: Jet_pt_jes_AbsoluteCal_up, Jet_pt_jes_AbsoluteCal_down, ...
// Registers 34 systematics (17 × 2 directions) with ISystematicManager.
```

---

## 8. Type-1 MET Propagation

When jet energies change, the Missing Transverse Energy must be updated to
maintain momentum conservation.  The Type-1 correction formula propagates
jet-pT changes into the MET vector.

### `propagateMET`

```cpp
void propagateMET(const std::string& baseMETPtColumn,
                  const std::string& baseMETPhiColumn,
                  const std::string& nominalJetPtColumn,
                  const std::string& variedJetPtColumn,
                  const std::string& outputMETPtColumn,
                  const std::string& outputMETPhiColumn,
                  float jetPtThreshold = 15.0f);
```

Registers a deferred MET propagation step.  In `execute()`, defines:

```
new_MET_x = baseMETPt·cos(baseMETφ) − Σⱼ [(varPtⱼ − nomPtⱼ)·cos(φⱼ)]
new_MET_y = baseMETPt·sin(baseMETφ) − Σⱼ [(varPtⱼ − nomPtⱼ)·sin(φⱼ)]
outputMETPtColumn  = √(new_MET_x² + new_MET_y²)
outputMETPhiColumn = atan2(new_MET_y, new_MET_x)
```

The sum runs over jets where `nomPtⱼ > jetPtThreshold` (CMS standard: 15 GeV).
The jet φ column is taken from `setJetColumns()`.

Multiple `propagateMET` calls can be chained, e.g. first propagating the
nominal JEC correction, then propagating a JES variation on top.

**Parameters**:
- `baseMETPtColumn`    — input scalar MET-pT column (`Float_t`).
- `baseMETPhiColumn`   — input scalar MET-φ column (`Float_t`).
- `nominalJetPtColumn` — jet pT **before** the change.
- `variedJetPtColumn`  — jet pT **after** the change.
- `outputMETPtColumn`  — output MET-pT column name.
- `outputMETPhiColumn` — output MET-φ column name.
- `jetPtThreshold`     — minimum nominal jet pT for inclusion (default: 15 GeV).

**Throws**:
- `std::invalid_argument` if any column name is empty.
- `std::runtime_error` if `setJetColumns()` has not been called.

**Example: propagate JEC and a single JES variation**

```cpp
// Step 1: propagate nominal JEC.
jes->propagateMET("MET_pt", "MET_phi",
                  "Jet_pt_raw", "Jet_pt_jec",
                  "MET_pt_jec", "MET_phi_jec");

// Step 2: propagate JES Total up variation on top of the JEC-corrected MET.
jes->propagateMET("MET_pt_jec", "MET_phi_jec",
                  "Jet_pt_jec", "Jet_pt_jes_Total_up",
                  "MET_pt_jes_Total_up", "MET_phi_jes_Total_up");
```

---

## 9. PhysicsObjectCollection Integration

The `JetEnergyScaleManager` can consume and produce
[`PhysicsObjectCollection`](PHYSICS_OBJECTS.md) objects directly, enabling a
clean, type-safe workflow where the user builds a jet selection once and
receives corrected collections back.

### Workflow overview

```
User builds jet selection:
  "goodJets" (PhysicsObjectCollection)

JetEnergyScaleManager:
  setInputJetCollection("goodJets")
  ↓
  defineCollectionOutput("Jet_pt_jec", "goodJets_jec")
  ↓ execute() ↓
  "goodJets_jec" (PhysicsObjectCollection, corrected pT)

  defineVariationCollections("goodJets_jec", "goodJets",
                             "goodJets_variations")
  ↓ execute() ↓
  "goodJets_TotalUp"   (PhysicsObjectCollection)
  "goodJets_TotalDown" (PhysicsObjectCollection)
  "goodJets_variations" (PhysicsObjectVariationMap)
    ├── "nominal"    → goodJets_jec collection
    ├── "TotalUp"   → goodJets_TotalUp collection
    └── "TotalDown" → goodJets_TotalDown collection
```

### `setInputJetCollection`

```cpp
void setInputJetCollection(const std::string& collectionColumn);
```

Declares the name of the RDataFrame column that holds the input jet
`PhysicsObjectCollection` (one collection per event).  Must be called before
`defineCollectionOutput()` or `defineVariationCollections()`.

**Throws** `std::invalid_argument` if `collectionColumn` is empty.

### `defineCollectionOutput`

```cpp
void defineCollectionOutput(const std::string& correctedPtColumn,
                            const std::string& outputCollectionColumn,
                            const std::string& correctedMassColumn = "");
```

Schedules the definition of a new `PhysicsObjectCollection` column
`outputCollectionColumn` in `execute()`.  For each jet in the input
collection:
- pT is replaced with the corresponding entry in `correctedPtColumn`.
- If `correctedMassColumn` is non-empty, mass is also replaced from
  `correctedMassColumn`; eta and phi are reconstructed from the stored
  4-vectors (they are unchanged by JES/JER corrections).
- When `correctedMassColumn` is empty, only pT changes
  (`withCorrectedPt` is used internally).

`correctedPtColumn` (and `correctedMassColumn` if provided) must be
full-size `RVec<Float_t>` columns covering the **original unfiltered**
collection — the `PhysicsObjectCollection` uses its stored indices to look
up each selected jet's corrected value.

**Throws**:
- `std::invalid_argument` if `correctedPtColumn` or `outputCollectionColumn`
  is empty.
- `std::runtime_error` if `setInputJetCollection()` was not called.

**Example**:

```cpp
// Build a selected jet collection (user code, before execute)
analyzer.Define("goodJets",
    [](const RVec<float>& pt, const RVec<float>& eta,
       const RVec<float>& phi, const RVec<float>& mass) {
        return PhysicsObjectCollection(pt, eta, phi, mass,
                                       (pt > 25.f) && (abs(eta) < 2.4f));
    },
    {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
);

jes->setInputJetCollection("goodJets");

// After applyCorrection / applyCorrectionlib defines "Jet_pt_jec":
jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
// After execute(): "goodJets_jec" column available with JEC-corrected pT.
```

### `defineVariationCollections`

```cpp
void defineVariationCollections(const std::string& nominalCollectionColumn,
                                const std::string& collectionPrefix,
                                const std::string& variationMapColumn = "");
```

For each variation registered with `addVariation()` (or by
`applySystematicSet()`), schedules in `execute()`:
- `<collectionPrefix>_<variationName>Up`   — `PhysicsObjectCollection`
  with up-shifted pT.
- `<collectionPrefix>_<variationName>Down` — `PhysicsObjectCollection`
  with down-shifted pT.

If `variationMapColumn` is non-empty, also defines a
`PhysicsObjectVariationMap` column built as:

```
{
  "nominal"           → nominalCollectionColumn
  "<variationName>Up"   → <collectionPrefix>_<variationName>Up
  "<variationName>Down" → <collectionPrefix>_<variationName>Down
  ...
}
```

**Throws**:
- `std::invalid_argument` if `nominalCollectionColumn` or
  `collectionPrefix` is empty.
- `std::runtime_error` if `setInputJetCollection()` was not called.

**Example**:

```cpp
// Assumes addVariation / applySystematicSet already registered "Total".
jes->defineVariationCollections("goodJets_jec", "goodJets",
                                "goodJets_variations");
// After execute():
//   "goodJets_TotalUp"        : PhysicsObjectCollection (pT up)
//   "goodJets_TotalDown"      : PhysicsObjectCollection (pT down)
//   "goodJets_variations"     : PhysicsObjectVariationMap
//       ["nominal"]           → goodJets_jec
//       ["TotalUp"]           → goodJets_TotalUp
//       ["TotalDown"]         → goodJets_TotalDown
```

---

## 10. Systematic Variation Registration

### `addVariation`

```cpp
void addVariation(const std::string& systematicName,
                  const std::string& upPtColumn,
                  const std::string& downPtColumn,
                  const std::string& upMassColumn   = "",
                  const std::string& downMassColumn = "");
```

Registers a named JES/JER systematic variation.  In `execute()`, registers
both directions with `ISystematicManager` so that downstream histogram
booking and validation tools propagate the systematic correctly.

`applySystematicSet()` calls `addVariation()` automatically for every source
in the set.

**Throws** `std::invalid_argument` if `systematicName`, `upPtColumn`, or
`downPtColumn` is empty.

---

## 11. Accessors

```cpp
const std::string& getRawPtColumn()                const;
const std::string& getPtColumn()                   const;
const std::string& getMassColumn()                 const;
const std::string& getMETPtColumn()                const;
const std::string& getMETPhiColumn()               const;
const std::string& getInputJetCollectionColumn()   const;
const std::vector<JESVariationEntry>& getVariations() const;
```

| Accessor | Returns |
|----------|---------|
| `getRawPtColumn()` | Raw-pT column name (non-empty after `removeExistingCorrections` or `setRawPtColumn`). |
| `getPtColumn()` | Original pT column (from `setJetColumns`). |
| `getMassColumn()` | Original mass column (from `setJetColumns`). |
| `getMETPtColumn()` | Base MET-pT column (from `setMETColumns`). |
| `getMETPhiColumn()` | Base MET-φ column (from `setMETColumns`). |
| `getInputJetCollectionColumn()` | Input jet collection column (from `setInputJetCollection`). |
| `getVariations()` | All registered `JESVariationEntry` structs. |

### `JESVariationEntry` struct

```cpp
struct JESVariationEntry {
    std::string name;           // Systematic name (e.g. "jesTotal")
    std::string upPtColumn;     // Corrected-pT column for the up shift
    std::string downPtColumn;   // Corrected-pT column for the down shift
    std::string upMassColumn;   // Corrected-mass column for the up shift
    std::string downMassColumn; // Corrected-mass column for the down shift
};
```

---

## 12. Lifecycle Hooks

```cpp
void setContext(ManagerContext& ctx) override;
void setupFromConfigFile() override;   // no-op
void execute()   override;
void finalize()  override;             // no-op
void reportMetadata() override;
```

`execute()` is called by the framework after all plugins are configured.  It
processes the registered steps in this order:

| Step | Work done |
|------|-----------|
| 1 | Raw-pT and raw-mass columns (if `removeExistingCorrections` was called). |
| 2 | Each correction step (from `applyCorrection` / `applyCorrectionlib`). |
| 3 | Each MET propagation step (from `propagateMET`). |
| 4 | Each collection output step (from `defineCollectionOutput`). |
| 5 | Per-variation collection columns and optional variation map (from `defineVariationCollections`). |
| 6 | Register all systematic variations with `ISystematicManager`. |

`reportMetadata()` logs a human-readable summary of the complete
configuration (jet/MET columns, correction steps, systematic sets,
variations, MET propagation steps, and collection integration).

---

## 13. Provenance and Metadata

`collectProvenanceEntries()` contributes the following keys to the
framework's provenance system:

| Key | Description |
|-----|-------------|
| `jet_pt_column` | Input jet pT column name. |
| `jet_eta_column` | Input jet η column name. |
| `jet_phi_column` | Input jet φ column name. |
| `jet_mass_column` | Input jet mass column name. |
| `met_pt_column` | Input MET-pT column name. |
| `met_phi_column` | Input MET-φ column name. |
| `raw_factor_column` | Raw-factor column (if `removeExistingCorrections` was used). |
| `raw_pt_column` | Raw-pT column name. |
| `correction_steps` | Comma-separated summary of correction steps. |
| `systematic_sets` | Semicolon-separated summary of registered source sets. |
| `variations` | Comma-separated list of registered variations. |
| `met_propagation_steps` | Comma-separated summary of MET propagation steps. |
| `input_jet_collection_column` | Input jet collection column name. |
| `collection_output_steps` | Summary of `defineCollectionOutput` steps. |
| `variation_collection_steps` | Summary of `defineVariationCollections` steps. |

---

## 14. Complete CMS NanoAOD Workflow

The following example shows a production-ready CMS NanoAOD jet correction and
systematic workflow using all features of the plugin.

```cpp
#include <Analyzer.h>
#include <JetEnergyScaleManager.h>
#include <PhysicsObjectCollection.h>

int main(int argc, char** argv) {
    Analyzer analyzer(argv[1]);

    auto* jes = analyzer.getPlugin<JetEnergyScaleManager>("jes");
    auto* cm  = analyzer.getPlugin<CorrectionManager>("corrections");

    // -----------------------------------------------------------------------
    // 1. Declare column names
    // -----------------------------------------------------------------------
    jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
    jes->setMETColumns("MET_pt", "MET_phi");

    // -----------------------------------------------------------------------
    // 2. Build a selected jet collection (any standard Define call)
    // -----------------------------------------------------------------------
    analyzer.Define("goodJets",
        [](const RVec<float>& pt, const RVec<float>& eta,
           const RVec<float>& phi, const RVec<float>& mass) {
            return PhysicsObjectCollection(pt, eta, phi, mass,
                                           (pt > 25.f) && (abs(eta) < 2.4f));
        },
        {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"}
    );

    // -----------------------------------------------------------------------
    // 3. Strip NanoAOD JEC → raw pT
    // -----------------------------------------------------------------------
    jes->removeExistingCorrections("Jet_rawFactor");
    // Defines: Jet_pt_raw, Jet_mass_raw

    // -----------------------------------------------------------------------
    // 4. Apply new L1L2L3 JEC via correctionlib
    // -----------------------------------------------------------------------
    jes->applyCorrectionlib(*cm, "jec_l1l2l3", {"L3Residual"},
                            "Jet_pt_raw", "Jet_pt_jec");
    // Defines: Jet_pt_jec, Jet_mass_jec

    // -----------------------------------------------------------------------
    // 5. Nominal corrected jet collection
    // -----------------------------------------------------------------------
    jes->setInputJetCollection("goodJets");
    jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec",
                                "Jet_mass_jec"); // with mass correction too
    // Defines: goodJets_jec (PhysicsObjectCollection)

    // -----------------------------------------------------------------------
    // 6. Propagate JEC correction to MET (Type-1)
    // -----------------------------------------------------------------------
    jes->propagateMET("MET_pt", "MET_phi",
                      "Jet_pt_raw", "Jet_pt_jec",
                      "MET_pt_jec", "MET_phi_jec");
    // Defines: MET_pt_jec, MET_phi_jec

    // -----------------------------------------------------------------------
    // 7. Register CMS JES systematic source set (reduced: "Total" only)
    // -----------------------------------------------------------------------
    jes->registerSystematicSources("reduced", {"Total"});
    jes->applySystematicSet(*cm, "jes_unc", "reduced",
                            "Jet_pt_jec", "Jet_pt_jes");
    // Defines: Jet_pt_jes_Total_up, Jet_pt_jes_Total_down
    // Registers: TotalUp, TotalDown with ISystematicManager

    // -----------------------------------------------------------------------
    // 8. Per-variation jet collections + variation map
    // -----------------------------------------------------------------------
    jes->defineVariationCollections("goodJets_jec", "goodJets",
                                    "goodJets_variations");
    // Defines: goodJets_TotalUp, goodJets_TotalDown
    //          goodJets_variations (PhysicsObjectVariationMap)

    // -----------------------------------------------------------------------
    // 9. Propagate JES Total variation to MET
    // -----------------------------------------------------------------------
    jes->propagateMET("MET_pt_jec", "MET_phi_jec",
                      "Jet_pt_jec", "Jet_pt_jes_Total_up",
                      "MET_pt_jes_Total_up", "MET_phi_jes_Total_up");
    jes->propagateMET("MET_pt_jec", "MET_phi_jec",
                      "Jet_pt_jec", "Jet_pt_jes_Total_down",
                      "MET_pt_jes_Total_down", "MET_phi_jes_Total_down");

    // -----------------------------------------------------------------------
    // 10. Use the corrected collections in downstream analysis
    // -----------------------------------------------------------------------
    // Nominal di-jet invariant mass from corrected collection:
    analyzer.Define("dijetMass",
        [](const PhysicsObjectCollection& jets) -> float {
            auto pairs = makePairs(jets);
            if (pairs.empty()) return -1.f;
            return static_cast<float>(pairs[0].p4.M());
        },
        {"goodJets_jec"}
    );

    // Systematic variations accessed via the map:
    analyzer.Define("dijetMass_TotalUp",
        [](const PhysicsObjectVariationMap& vm) -> float {
            const auto& jets = vm.at("TotalUp");
            auto pairs = makePairs(jets);
            if (pairs.empty()) return -1.f;
            return static_cast<float>(pairs[0].p4.M());
        },
        {"goodJets_variations"}
    );

    // -----------------------------------------------------------------------
    // 11. Book histograms and save
    // -----------------------------------------------------------------------
    auto* nhm = analyzer.getPlugin<INDHistogramManager>("histograms");
    nhm->bookHistogram("dijetMass",    "dijetMass",    "event_weight", 50, 0, 1000);
    nhm->bookHistogram("dijetMassUp",  "dijetMass_TotalUp", "event_weight", 50, 0, 1000);

    analyzer.save();
    return 0;
}
```

---

## 15. API Summary

### Construction

```cpp
JetEnergyScaleManager();   // default-constructible
```

Register with the framework via `Analyzer::registerPlugin()` before calling
`Analyzer::save()`.

### Configuration (call before execute)

```cpp
// Jet columns
void setJetColumns(ptCol, etaCol, phiCol, massCol);
void setMETColumns(metPtCol, metPhiCol);

// Stripping existing corrections
void removeExistingCorrections(rawFactorColumn);    // or:
void setRawPtColumn(rawPtColumn);

// Applying corrections
void applyCorrection(inputPt, sfCol, outputPt,
                     applyToMass=true, inputMass="", outputMass="");
void applyCorrectionlib(cm, correctionName, stringArgs,
                        inputPt, outputPt,
                        applyToMass=true, inputMass="", outputMass="",
                        inputColumns={});

// CMS source sets
void registerSystematicSources(setName, sources);
void applySystematicSet(cm, correctionName, setName,
                        inputPt, outputPtPrefix,
                        applyToMass=true, inputColumns={}, inputMass="");

// MET propagation
void propagateMET(basePt, basePhi, nomPt, varPt,
                  outPt, outPhi, threshold=15.f);

// PhysicsObjectCollection integration
void setInputJetCollection(collectionColumn);
void defineCollectionOutput(correctedPt, outputCol, correctedMass="");
void defineVariationCollections(nominalCol, prefix, mapCol="");

// Direct variation registration
void addVariation(name, upPt, downPt, upMass="", downMass="");
```

### Accessors

```cpp
const std::string& getRawPtColumn()              const;
const std::string& getPtColumn()                 const;
const std::string& getMassColumn()               const;
const std::string& getMETPtColumn()              const;
const std::string& getMETPhiColumn()             const;
const std::string& getInputJetCollectionColumn() const;
const std::vector<JESVariationEntry>& getVariations() const;
const std::vector<std::string>& getSystematicSources(setName) const;
```

### IPluggableManager interface

```cpp
std::string type()           const override;  // "JetEnergyScaleManager"
void setContext(ManagerContext&)  override;
void setupFromConfigFile()        override;   // no-op
void execute()                    override;
void finalize()                   override;   // no-op
void reportMetadata()             override;
std::unordered_map<std::string, std::string>
     collectProvenanceEntries() const override;
```

---

## See Also

- [Physics Objects Reference](PHYSICS_OBJECTS.md) — `PhysicsObjectCollection`,
  `PhysicsObjectVariationMap`, `withCorrectedPt`, `withCorrectedKinematics`
- [API Reference – CorrectionManager](API_REFERENCE.md#correctionmanager) —
  correctionlib evaluation
- [API Reference – WeightManager](API_REFERENCE.md#weightmanager) — event
  weight systematics
- [Nuisance Groups](NUISANCE_GROUPS.md) — configuring nuisance groups for
  datacard production
- [Configuration Validation](CONFIGURATION_VALIDATION.md) — YAML analysis
  config schema
