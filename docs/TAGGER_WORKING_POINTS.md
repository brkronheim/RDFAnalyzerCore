# TaggerWorkingPointManager Reference

This document describes the `TaggerWorkingPointManager` plugin for
working-point-based tagger corrections and systematics on **any physics
object collection** — including Jets, FatJets, and Taus.

---

## Contents

1. [Overview](#overview)
2. [Working-point categories](#working-point-categories)
3. [Quick start: Jets (b-tagging)](#quick-start-jets-b-tagging)
4. [Quick start: Taus (DeepTau ID)](#quick-start-taus-deeptau-id)
5. [API reference](#api-reference)
6. [Generator-level fraction reweighting](#generator-level-fraction-reweighting)
7. [Pre-processing: computing fraction histograms](#pre-processing-computing-fraction-histograms)
8. [Systematic variations and collections](#systematic-variations-and-collections)
9. [Complete CMS NanoAOD workflow](#complete-cms-nanoaod-workflow)
10. [Integration with WeightManager](#integration-with-weightmanager)
11. [Further reading](#further-reading)

---

## Overview

`TaggerWorkingPointManager` applies **working-point-based tagger scale
factors** to **any `PhysicsObjectCollection`** with a discriminator score.
Common use cases include:

- **Jets**: b-tagging (DeepJet, RobustParTAK4, ParticleNet)
- **Taus**: DeepTau ID (VSjet, VSe, VSmu discriminators)
- **FatJets**: Xbb / Xcc boosted taggers

Key features:
- **Working-point definitions** — register any number of named WPs with score
  thresholds (e.g. loose / medium / tight).
- **WP category column** — per-object integer category (0 = fail all, …, N = pass all).
- **Correctionlib SF application** — per-object SFs from a correctionlib payload
  reduced to per-event weight columns.
- **Generator-level fraction correction** — optional second correctionlib encoding
  MC object fractions per WP category, for proper distribution reweighting.
- **Systematic source sets** — bulk application of named uncertainty sources
  matching the JetEnergyScaleManager API.
- **WP-filtered PhysicsObjectCollections** — objects passing/failing specific WP
  criteria as named RDF columns.
- **Variation collections + map** — per-systematic up/down object collections and a
  `PhysicsObjectVariationMap` for downstream propagation.
- **Fraction histogram utility** — book per-(pT, η, flavour) tagger-score
  histograms for calculating MC fractions in a dedicated pre-processing run.

---

## Working-point categories

For working points **[loose=0.05, medium=0.30, tight=0.75]**, the per-object
WP category column (`<ptColumn>_wp_category` by default) is assigned as:

| category | tagger score range       | meaning                         |
|----------|--------------------------|---------------------------------|
| 0        | score < 0.05             | fail all WPs                    |
| 1        | 0.05 ≤ score < 0.30      | pass loose, fail medium         |
| 2        | 0.30 ≤ score < 0.75      | pass medium, fail tight         |
| 3        | score ≥ 0.75             | pass all WPs                    |

The categories are assigned as the **count of WPs passed** (i.e. the number of
WP thresholds that are ≤ the object's tagger score).

---

## Quick start: Jets (b-tagging)

```cpp
#include <TaggerWorkingPointManager.h>
#include <CorrectionManager.h>
#include <PhysicsObjectCollection.h>

// Get plugin instances from the analyzer.
auto *twm = analyzer.getPlugin<TaggerWorkingPointManager>("btagManager");
auto *cm  = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare object kinematic columns.
twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");

// 2. Set the tagger discriminator score column.
twm->setTaggerColumn("Jet_btagDeepFlavB");

// 3. Register working points in order of increasing threshold.
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);

// 4. Declare the input jet collection.
twm->setInputObjectCollection("goodJets");

// 5. Apply a central (nominal) correctionlib SF.
twm->applyCorrectionlib(*cm, "deepjet_sf", {"central"},
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
// → defines per-event weight column "deepjet_sf_central_weight"

// 6. Register and apply systematic source sets.
twm->registerSystematicSources("standard",
    {"hf", "lf", "hfstats1", "hfstats2", "lfstats1", "lfstats2",
     "cferr1", "cferr2"});
twm->applySystematicSet(*cm, "deepjet_sf", "standard",
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});

// 7. Define WP-filtered jet collections.
twm->defineWorkingPointCollection("pass_medium",      "goodJets_bmedium");
twm->defineWorkingPointCollection("fail_loose",       "goodJets_bfail");
twm->defineWorkingPointCollection("pass_loose_fail_medium", "goodJets_btight");

// 8. Build systematic variation collections + variation map.
twm->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
                                 "goodJets_btag_variations");
```

After `analyzer.save()`:

| Column                        | Type                        | Description                                    |
|-------------------------------|-----------------------------|------------------------------------------------|
| `Jet_pt_wp_category`          | `RVec<Int_t>`               | Per-object WP category (0 = fail all)          |
| `deepjet_sf_central_weight`   | `Float_t`                   | Per-event weight (product of per-object SFs)   |
| `deepjet_sf_hf_up_weight`     | `Float_t`                   | Per-event weight for hf up variation           |
| `deepjet_sf_hf_down_weight`   | `Float_t`                   | Per-event weight for hf down variation         |
| `goodJets_bmedium`            | `PhysicsObjectCollection`   | Jets passing medium WP                         |
| `goodJets_btag_hfUp`          | `PhysicsObjectCollection`   | Nominal jet collection (same as medium)        |
| `goodJets_btag_variations`    | `PhysicsObjectVariationMap` | All collections keyed by variation name        |

---

## Quick start: Taus (DeepTau ID)

The same plugin works identically for taus — the only difference is the
input column names and working-point thresholds.

```cpp
auto *tauTwm = analyzer.getPlugin<TaggerWorkingPointManager>("tauIdManager");
auto *cm     = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare tau kinematic columns.
tauTwm->setObjectColumns("Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass");

// 2. Set the DeepTau VSjet discriminant score column.
tauTwm->setTaggerColumn("Tau_rawDeepTau2018v2p5VSjet");

// 3. Register DeepTau VSjet working points (example Run 3 values).
tauTwm->addWorkingPoint("VVVLoose", 0.05f);
tauTwm->addWorkingPoint("Loose",    0.20f);
tauTwm->addWorkingPoint("Medium",   0.49f);
tauTwm->addWorkingPoint("Tight",    0.75f);

// 4. Declare the input tau collection.
tauTwm->setInputObjectCollection("goodTaus");

// 5. Apply tau ID SFs from correctionlib.
tauTwm->applyCorrectionlib(*cm, "tau_id_sf", {"pt_binned", "central"},
                           {"Tau_pt", "Tau_decayMode", "Tau_genPartFlav"});
// → defines per-event weight column "tau_id_sf_pt_binned_central_weight"

// 6. Register and apply tau ID systematic sources.
tauTwm->registerSystematicSources("tau_unc", {"stat0", "stat1", "syst"});
tauTwm->applySystematicSet(*cm, "tau_id_sf", "tau_unc",
                           {"Tau_pt", "Tau_decayMode", "Tau_genPartFlav"});

// 7. Define WP-filtered tau collections.
tauTwm->defineWorkingPointCollection("pass_Medium", "goodTaus_medium");
tauTwm->defineWorkingPointCollection("fail_Loose",  "goodTaus_antiIso");

// 8. Build variation collections + map.
tauTwm->defineVariationCollections("goodTaus_medium", "goodTaus_id",
                                    "goodTaus_id_variations");
```

---

## API reference

### `setObjectColumns(pt, eta, phi, mass)`

Declare the input jet kinematic column names.  The `pt` column must not be
empty; `mass` may be empty to disable mass handling.

```cpp
twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
```

### `setTaggerColumn(taggerScoreColumn)`

Declare the per-object tagger discriminator score column (type `RVec<Float_t>`).
This column is used in `execute()` to compute the per-object WP category.

```cpp
twm->setTaggerColumn("Jet_btagDeepFlavB");
```

### `addWorkingPoint(name, threshold)`

Register a named WP with a score threshold.  WPs must be added in **ascending
threshold order**.

```cpp
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);
```

Throws:
- `std::invalid_argument` if name is empty, already registered, or threshold
  order is violated.

### `setInputObjectCollection(collectionColumn)`

Declare the RDF column holding the input `PhysicsObjectCollection`.  Required
before any collection-based methods.

```cpp
twm->setInputObjectCollection("goodJets");
```

### `applyCorrectionlib(cm, correctionName, stringArgs, inputColumns)`

Apply a correctionlib payload per jet and produce a per-event weight column.

The correctionlib is evaluated once per jet via
`CorrectionManager::applyCorrectionVec()`.  The resulting per-object
`RVec<Float_t>` SF column is then reduced to a per-event `Float_t` weight
(product of all per-object SFs for objects in the input collection).

Output column name: `<correctionName>_<stringArgs...>_weight`

```cpp
// Central (nominal) SF:
twm->applyCorrectionlib(*cm, "deepjet_sf", {"central"},
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
// → creates "deepjet_sf_central" (per-object SFs) and
//   "deepjet_sf_central_weight" (per-event weight)
```

### `setFractionCorrection(cm, fractionCorrectionName, inputColumns)`

Enable generator-level fraction reweighting.  See
[Generator-level fraction reweighting](#generator-level-fraction-reweighting).

### `registerSystematicSources(setName, sources)`

Register a named set of uncertainty source names for bulk application.

```cpp
twm->registerSystematicSources("standard",
    {"hf", "lf", "hfstats1", "hfstats2", "lfstats1", "lfstats2",
     "cferr1", "cferr2"});
```

### `applySystematicSet(cm, correctionName, setName, inputColumns)`

Apply all sources in a named set.  For each source `S`, two SF columns and two
weight columns are produced:
- `<correctionName>_<S>_up` (per-object SFs), `<correctionName>_<S>_up_weight`
- `<correctionName>_<S>_down` (per-object SFs), `<correctionName>_<S>_down_weight`

Each source is automatically registered with `ISystematicManager`.

```cpp
twm->applySystematicSet(*cm, "deepjet_sf", "standard",
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
```

### `addVariation(systematicName, upSFColumn, downSFColumn, [upWeightColumn, downWeightColumn])`

Register a single systematic variation manually (for SFs not produced by
`applySystematicSet`).

```cpp
twm->addVariation("btagXY", "my_btag_xy_up_sf", "my_btag_xy_down_sf");
// Weight columns auto-derived as: my_btag_xy_up_sf_weight, etc.
```

### `defineWorkingPointCollection(selection, outputCollectionColumn)`

Define a `PhysicsObjectCollection` of jets satisfying a WP selection.

**Selection syntax** (WP names must match those registered via `addWorkingPoint`):

| Selection string          | Jets kept                                         |
|---------------------------|---------------------------------------------------|
| `"pass_<wp>"`             | category ≥ index(`<wp>`) + 1                      |
| `"fail_<wp>"`             | category < index(`<wp>`) + 1                      |
| `"pass_<wp1>_fail_<wp2>"` | index(`<wp1>`) + 1 ≤ category ≤ index(`<wp2>`)   |

```cpp
twm->defineWorkingPointCollection("pass_medium",      "goodJets_bmedium");
twm->defineWorkingPointCollection("fail_loose",       "goodJets_bfail");
twm->defineWorkingPointCollection("pass_loose_fail_medium", "goodJets_bL_notM");
twm->defineWorkingPointCollection("pass_tight",       "goodJets_btight");
```

### `defineVariationCollections(nominalCollectionColumn, collectionPrefix, [variationMapColumn])`

Build up/down collection aliases and an optional `PhysicsObjectVariationMap`
for all registered systematic variations.

Because tagger corrections change only the **event weight** (not jet
kinematics), all variation collections contain the same jets as the nominal.
The weight difference is tracked via the separate weight columns.

```cpp
twm->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
                                 "goodJets_btag_variations");
// Creates:
//   goodJets_btag_hfUp, goodJets_btag_hfDown, ...
//   goodJets_btag_variations (PhysicsObjectVariationMap)
```

### `defineFractionHistograms(outputPrefix, ptBinEdges, etaBinEdges, [flavourColumn])`

Book tagger-score distribution histograms for the fraction pre-processing run.
See [Pre-processing: computing fraction histograms](#pre-processing-computing-fraction-histograms).

---

## Generator-level fraction reweighting

### Motivation

When applying b-tag SFs, the scale factor for a jet in WP category `c` is:

```
SF(pt, η, flavour, c) = eff_data(pt, η, flavour, c) / eff_MC(pt, η, flavour, c)
```

The per-event weight `W = Π SF_i` is unbiased for events where all jets are
in their true WP category.  However, when the MC simulation has a slightly
different tagger-score distribution from data, the SFs alone may not fully
correct the event-weight distribution.

The **fraction-weighted** approach additionally divides each per-jet SF by the
MC fraction of objects in that category at the given (pT, η):

```
W_frac = Π [SF(pt_i, η_i, flav_i, c_i) / f_MC(pt_i, η_i, c_i)]
```

where `f_MC(pt_i, η_i, c_i)` is the fraction of MC jets at `(pt_i, η_i)` that
fall in category `c_i`.  This ensures the total event weight reflects the
generator-level jet distribution.

### Usage

First, create a correctionlib JSON containing the MC fraction payload (see
[Pre-processing: computing fraction histograms](#pre-processing-computing-fraction-histograms)).
Then register it:

```cpp
// Register the fraction correction with CorrectionManager.
cm->registerCorrection(
    "deepjet_fractions",
    "deepjet_fractions.json",
    "DeepJetFractions",
    {"Jet_pt", "Jet_eta", "Jet_wp_category"});
// Note: "Jet_wp_category" is defined by execute() after calling
// setTaggerColumn + addWorkingPoint.

// Enable fraction reweighting.
twm->setFractionCorrection(*cm, "deepjet_fractions",
                           {"Jet_pt", "Jet_eta",
                            "Jet_pt_wp_category"});
// All subsequent applyCorrectionlib weight columns will be fraction-weighted.
```

---

## Pre-processing: computing fraction histograms

To build the fraction correctionlib payload, run a dedicated pre-processing
analysis that calls `defineFractionHistograms()`:

```cpp
// In a dedicated pre-processing analysis (fraction_calc.cc):

twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
twm->setTaggerColumn("Jet_btagDeepFlavB");
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);
twm->setInputObjectCollection("goodJets");

// Book histograms for each (pT, |η|, flavour) combination.
twm->defineFractionHistograms(
    "deepjet_frac",                      // output prefix
    {20.f, 30.f, 50.f, 100.f, 200.f, 500.f},  // pT bin edges [GeV]
    {0.f, 1.5f, 2.4f},                   // |η| bin edges
    "Jet_hadronFlavour");                 // optional flavour column
```

After running over the MC sample, the metadata ROOT file will contain
histograms named `deepjet_frac_pt<I>_eta<J>_<flavour>` under the
`tagger_fractions/` directory.  Each histogram records the tagger-score
distribution for jets in that (pT, η, flavour) bin.

**Converting to fractions:** From each histogram, integrate the bins
corresponding to each WP category range to obtain the fraction:

```python
import uproot
import numpy as np

with uproot.open("meta.root") as f:
    h = f["tagger_fractions/deepjet_frac_pt2_eta0_b"]
    counts, edges = h.to_numpy()
    # Working-point thresholds:
    wp_loose = 0.0521; wp_medium = 0.3033; wp_tight = 0.7489
    total = counts.sum()
    # Fraction in each category:
    cat0 = counts[edges[:-1] < wp_loose].sum() / total   # fail all
    cat1 = counts[(edges[:-1] >= wp_loose) & (edges[:-1] < wp_medium)].sum() / total
    cat2 = counts[(edges[:-1] >= wp_medium) & (edges[:-1] < wp_tight)].sum() / total
    cat3 = counts[edges[:-1] >= wp_tight].sum() / total
```

These fractions can then be stored in a correctionlib JSON and consumed by
`setFractionCorrection()`.

---

## Systematic variations and collections

### Weight columns and WeightManager integration

After calling `applySystematicSet()`, the manager produces:
- `deepjet_sf_central_weight` — nominal per-event weight.
- `deepjet_sf_hf_up_weight`, `deepjet_sf_hf_down_weight` — per-variation weights.

Register these with `WeightManager` to apply them:

```cpp
auto *wm = analyzer.getPlugin<WeightManager>("weights");
wm->addScaleFactor("btag_nominal", "deepjet_sf_central_weight");
wm->addWeightVariation("btagHF",
    "deepjet_sf_hf_up_weight",
    "deepjet_sf_hf_down_weight");
```

### Automatic systematic propagation

Because `TaggerWorkingPointManager` registers variation column names with
`ISystematicManager` via `registerVariationColumns()`, any downstream
`Define(...)` call that consumes the nominal WP-filtered collection will
automatically have up/down variants created:

```cpp
// Defined once — the framework automatically creates:
//   selectedBJetPts              (nominal)
//   selectedBJetPts_btagHFUp     (up variation)
//   selectedBJetPts_btagHFDown   (down variation)
analyzer.Define("selectedBJetPts",
    [](const PhysicsObjectCollection& jets) {
        RVec<float> pts;
        for (size_t i = 0; i < jets.size(); ++i)
            pts.push_back(jets.at(i).Pt());
        return pts;
    },
    {"goodJets_bmedium"}, *sysMgr  // ← passing sysMgr enables auto-propagation
);
```

---

## Complete CMS NanoAOD workflow

The following shows the complete workflow for CMS Run 3 AK4 b-tagging with
DeepJet, including fraction reweighting and systematic variations:

```cpp
#include <TaggerWorkingPointManager.h>
#include <CorrectionManager.h>
#include <WeightManager.h>

// -------------------------------------------------------------------------
// Setup
// -------------------------------------------------------------------------
auto *jtm = analyzer.getPlugin<TaggerWorkingPointManager>("btagManager");
auto *cm  = analyzer.getPlugin<CorrectionManager>("corrections");
auto *wm  = analyzer.getPlugin<WeightManager>("weights");

// 1. Declare jet columns.
twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");

// 2. Set tagger discriminator.
twm->setTaggerColumn("Jet_btagDeepFlavB");

// 3. Register working points (Run 3 UL values for DeepJet).
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);

// 4. Declare input jet collection.
twm->setInputObjectCollection("goodJets");

// -------------------------------------------------------------------------
// 5. (Optional) enable fraction-weighted SFs
// -------------------------------------------------------------------------
cm->registerCorrection("deepjet_frac", "deepjet_fractions.json",
                        "DeepJetFractions",
                        {"Jet_pt", "Jet_eta", "Jet_pt_wp_category"});
twm->setFractionCorrection(*cm, "deepjet_frac",
                           {"Jet_pt", "Jet_eta", "Jet_pt_wp_category"});

// -------------------------------------------------------------------------
// 6. Load the scale-factor correctionlib.
// -------------------------------------------------------------------------
cm->registerCorrection("deepjet_sf", "deepjet_sf.json",
                        "deepJet_shape",
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});

// 7. Apply nominal SF.
twm->applyCorrectionlib(*cm, "deepjet_sf", {"central"},
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
// → "deepjet_sf_central_weight"

// 8. Apply CMS b-tag systematic source set.
twm->registerSystematicSources("standard",
    {"hf", "lf", "hfstats1", "hfstats2", "lfstats1", "lfstats2",
     "cferr1", "cferr2"});
twm->applySystematicSet(*cm, "deepjet_sf", "standard",
                        {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                         "Jet_btagDeepFlavB"});
// → "deepjet_sf_hf_up_weight", "deepjet_sf_hf_down_weight", …

// -------------------------------------------------------------------------
// 9. Register weights with WeightManager.
// -------------------------------------------------------------------------
wm->addScaleFactor("btag", "deepjet_sf_central_weight");
for (const auto &src : twm->getSystematicSources("standard")) {
    wm->addWeightVariation(
        "btag_" + src,
        "deepjet_sf_" + src + "_up_weight",
        "deepjet_sf_" + src + "_down_weight");
}

// -------------------------------------------------------------------------
// 10. Define WP-filtered jet collections.
// -------------------------------------------------------------------------
twm->defineWorkingPointCollection("pass_medium",      "goodJets_bmedium");
twm->defineWorkingPointCollection("fail_loose",       "goodJets_bfail");
twm->defineWorkingPointCollection("pass_loose_fail_medium", "goodJets_bL_notM");
twm->defineWorkingPointCollection("pass_tight",       "goodJets_btight");

// -------------------------------------------------------------------------
// 11. Build systematic variation collections + variation map.
// -------------------------------------------------------------------------
twm->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
                                 "goodJets_btag_variations");
// Creates: goodJets_btag_hfUp, goodJets_btag_hfDown, …
//          goodJets_btag_variations (PhysicsObjectVariationMap)
```

---

## Integration with WeightManager

The per-event weight columns produced by `TaggerWorkingPointManager` are
designed to plug directly into `WeightManager`:

```cpp
// Nominal SF as a scale factor:
wm->addScaleFactor("btag", "deepjet_sf_central_weight");

// Systematic variation (up/down weight columns):
wm->addWeightVariation("btagHF",
    "deepjet_sf_hf_up_weight",
    "deepjet_sf_hf_down_weight");
```

This integrates the b-tag SFs into the global event weight together with other
corrections (muon/electron SFs, pileup weights, etc.).

---

## Further reading

- **Jet energy corrections**: [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md)
- **All physics-object corrections (electron, muon, tau, photon)**: [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)
- **Physics object collections**: [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md)
- **Systematics and nuisance groups**: [NUISANCE_GROUPS.md](NUISANCE_GROUPS.md)
- **WeightManager reference**: [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md)
- **Getting started guide**: [GETTING_STARTED.md](GETTING_STARTED.md)
