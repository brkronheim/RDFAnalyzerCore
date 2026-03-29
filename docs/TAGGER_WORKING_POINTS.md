# TaggerWorkingPointManager Reference

This document describes the `TaggerWorkingPointManager` plugin for
working-point-based tagger corrections and systematics on **any physics
object collection** — including Jets, FatJets, and Taus.

---

## Contents

1. [Overview](#overview)
2. [Working-point categories](#working-point-categories)
3. [Quick start: Jets (b-tagging)](#quick-start-jets-b-tagging)
4. [Quick start: Jets (charm-tagging, multi-score)](#quick-start-jets-charm-tagging-multi-score)
5. [Quick start: Taus (DeepTau ID)](#quick-start-taus-deeptau-id)
6. [API reference](#api-reference)
7. [Generator-level fraction reweighting](#generator-level-fraction-reweighting)
8. [Pre-processing: computing fraction histograms](#pre-processing-computing-fraction-histograms)
9. [Systematic variations and collections](#systematic-variations-and-collections)
10. [Complete CMS NanoAOD workflow](#complete-cms-nanoaod-workflow)
11. [Integration with WeightManager](#integration-with-weightmanager)
12. [Config-file driven setup](#config-file-driven-setup)
13. [No-filter annotated collection](#no-filter-annotated-collection)
14. [Further reading](#further-reading)

---

## Overview

`TaggerWorkingPointManager` applies **working-point-based tagger scale
factors** to **any `PhysicsObjectCollection`** with a discriminator score.
Common use cases include:

- **Jets**: b-tagging (DeepJet, RobustParTAK4, ParticleNet)
- **Jets (multi-score)**: charm-tagging (CvsL + CvsB simultaneously)
- **Taus**: DeepTau ID (VSjet, VSe, VSmu discriminators)
- **FatJets**: Xbb / Xcc boosted taggers

Key features:
- **Working-point definitions** — register any number of named WPs with score
  thresholds (e.g. loose / medium / tight).
- **Multi-score WPs** — a WP can require passing thresholds on *multiple*
  discriminant scores simultaneously (e.g. CMS charm-tagging uses both CvsL and
  CvsB; see [`setTaggerColumns`](#settaggercolumns) and the vector overload of
  [`addWorkingPoint`](#addworkingpoint)).
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

The categories are assigned as the **count of WPs passed** (incrementing for
each WP whose thresholds are met, stopping at the first failure).

For **multi-score WPs** (e.g. charm-tagging), the same integer encoding applies
but an object "passes" WP `i` only if **all** its discriminant scores meet their
respective thresholds for WP `i`:

| category | meaning (CvsL/CvsB charm-tagging with 2 WPs)         |
|----------|------------------------------------------------------|
| 0        | fails loose (at least one score below loose thresholds) |
| 1        | passes loose, fails medium                           |
| 2        | passes both loose and medium (pass all)              |

The `defineWorkingPointCollection()` selection strings (`pass_<wp>`, `fail_<wp>`,
`pass_<wp1>_fail_<wp2>`) work identically for both single-score and multi-score WPs.

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

## Quick start: Jets (charm-tagging, multi-score)

CMS charm-tagging uses **two** discriminants simultaneously: CvsL (charm vs
light) and CvsB (charm vs b).  Each working point requires *both* discriminants
to exceed their respective thresholds.  Use `setTaggerColumns` (plural) and the
vector overload of `addWorkingPoint`.

```cpp
#include <TaggerWorkingPointManager.h>
#include <CorrectionManager.h>

auto *ctwm = analyzer.getPlugin<TaggerWorkingPointManager>("ctagManager");
auto *cm   = analyzer.getPlugin<CorrectionManager>("corrections");

// 1. Declare object kinematic columns.
ctwm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");

// 2. Set BOTH charm-tagger score columns (order matters — must match the
//    threshold order used in addWorkingPoint below).
ctwm->setTaggerColumns({"Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});

// 3. Register 2D WPs: {CvsL threshold, CvsB threshold}.
//    An object passes a WP only if ALL its discriminant scores meet their
//    respective thresholds.  WPs should be defined in globally nested order
//    (passing tight implies passing medium implies passing loose).
ctwm->addWorkingPoint("loose",  {0.042f, 0.135f});
ctwm->addWorkingPoint("medium", {0.108f, 0.285f});
ctwm->addWorkingPoint("tight",  {0.274f, 0.605f});
// → execute() will define "Jet_pt_wp_category" (0=fail all, 1=pass loose only,
//                                                2=pass medium, 3=pass all)

// 4. Declare the input jet collection.
ctwm->setInputObjectCollection("goodJets");

// 5. Apply charm-tagging SFs from correctionlib.
ctwm->applyCorrectionlib(*cm, "ctag_sf", {"central"},
                         {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                          "Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});
// → defines "ctag_sf_central_weight"

// 6. Apply systematic sources.
ctwm->registerSystematicSources("standard",
    {"Extrap", "Interp", "LHEScaleWeight_muF", "LHEScaleWeight_muR",
     "PSWeight_ISR", "PSWeight_FSR", "PUWeight", "StatCsJet", "StatLJet"});
ctwm->applySystematicSet(*cm, "ctag_sf", "standard",
                         {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
                          "Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});

// 7. Define WP-filtered jet collections.
ctwm->defineWorkingPointCollection("pass_medium",      "goodJets_cmedium");
ctwm->defineWorkingPointCollection("fail_loose",       "goodJets_cfail");
ctwm->defineWorkingPointCollection("pass_loose_fail_medium", "goodJets_cloose_notM");

// 8. Build systematic variation collections.
ctwm->defineVariationCollections("goodJets_cmedium", "goodJets_ctag",
                                  "goodJets_ctag_variations");
```

**Note:** For multi-score WPs, `defineFractionHistograms()` fills the histogram
of the first discriminant (CvsL in this case).  If you need separate fraction
histograms for each discriminant, create two separate
`TaggerWorkingPointManager` instances.

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

Declare the input object kinematic column names.  The `pt` column must not be
empty; `mass` may be empty to disable mass handling.

```cpp
twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
```

### `setTaggerColumn(taggerScoreColumn)`

Declare a **single** per-object tagger discriminator score column
(type `RVec<Float_t>`).  Shorthand for `setTaggerColumns({taggerScoreColumn})`.

```cpp
twm->setTaggerColumn("Jet_btagDeepFlavB");
```

### `setTaggerColumns(taggerScoreColumns)`

Declare **multiple** per-object tagger discriminator score columns for
multi-score working points.  Each column must be `RVec<Float_t>`.  The column
order here must match the threshold order in `addWorkingPoint`.

```cpp
// Charm-tagging: both CvsL and CvsB required.
ctwm->setTaggerColumns({"Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});
```

Throws:
- `std::invalid_argument` if the list is empty or any element is empty.

### `addWorkingPoint(name, threshold)` / `addWorkingPoint(name, thresholds)`

Register a named working point.

**Single-score** (float overload) — WPs must be added in **ascending threshold
order**:

```cpp
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);
```

**Multi-score** (vector overload) — one threshold per discriminant column:

```cpp
// {CvsL threshold, CvsB threshold} — no ordering constraint enforced.
ctwm->addWorkingPoint("loose",  {0.042f, 0.135f});
ctwm->addWorkingPoint("medium", {0.108f, 0.285f});
ctwm->addWorkingPoint("tight",  {0.274f, 0.605f});
```

An object passes a multi-score WP only if **all** its discriminant scores are
≥ the respective threshold.  The WP category column uses the same integer
encoding as single-score (count of WPs passed, incrementing until the first
failure).  WPs should be defined in a globally nested order where possible
(e.g. passing the medium WP should imply passing the loose WP for all objects).

Throws:
- `std::invalid_argument` if name is empty, already registered, thresholds
  vector is empty, or (for single-score) threshold order is violated.

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

After running over the MC sample, the metadata ROOT file will contain two
types of histograms under the `tagger_fractions/` directory:

1. **Score histograms** — named `deepjet_frac_pt<I>_eta<J>[_<flavour>]`:
   record the tagger-score distribution (x-axis [0,1]) for jets in each
   (pT, η, flavour) bin.  These are used to derive WP fractions analytically.

2. **Category histograms** — named `deepjet_frac_cat_pt<I>_eta<J>[_<flavour>]`
   (always produced when working points are defined): record the WP category
   distribution (integers 0..N) directly.  Each bin corresponds to one WP
   category, making it trivial to read off category fractions without
   re-binning the score histograms.

**Converting score histograms to fractions:** From each score histogram,
integrate the bins corresponding to each WP category range to obtain the
fraction:

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

**Using category histograms directly:**

```python
with uproot.open("meta.root") as f:
    hcat = f["tagger_fractions/deepjet_frac_cat_pt2_eta0_b"]
    counts, _ = hcat.to_numpy()   # bins: [0, 1, 2, 3] for 3 WPs
    total = counts.sum()
    fractions = counts / total    # fractions[i] = fraction in category i
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
designed to plug directly into `WeightManager`.

### Manual registration

```cpp
// Nominal SF as a scale factor:
wm->addScaleFactor("btag", "deepjet_sf_central_weight");

// Systematic variation (up/down weight columns):
wm->addWeightVariation("btagHF",
    "deepjet_sf_hf_up_weight",
    "deepjet_sf_hf_down_weight");
```

### Convenience method

`registerWeightsWithWeightManager()` does both steps in one call:

```cpp
// After applyCorrectionlib() and applySystematicSet():
twm->registerWeightsWithWeightManager(*wm,
    "btag",                       // human-readable SF name
    "deepjet_sf_central_weight"); // nominal weight column
// Automatically calls wm->addWeightVariation() for every registered
// variation (hf, lf, hfstats1, …).
```

This integrates the b-tag SFs into the global event weight together with
other corrections (muon/electron SFs, pileup weights, etc.).

---

## Config-file driven setup

`setupFromConfigFile()` reads optional tagger configuration from a file
pointed to by the `<role>Config` or `taggerConfig` key in the analysis
config.  If the key is absent, the method returns silently — config is
entirely optional and programmatic setup continues to work unchanged.

### Config file format

Each line defines one setup block using space-separated `key=value` pairs:

```
# Working-point block (1D — single tagger column)
type=working_points taggerColumns=Jet_btagDeepFlavB wpNames=loose,medium,tight wpThresholds=0.0521,0.3033,0.7489

# Working-point block (ND — multi-score, colon-separated thresholds)
type=working_points taggerColumns=Jet_btagDeepFlavCvL,Jet_btagDeepFlavCvB wpNames=loose,medium,tight wpThresholds=0.042:0.135,0.108:0.285,0.274:0.605

# Correction block (stored for deferred application)
type=correction correctionName=deepjet_sf stringArgs=central inputColumns=Jet_hadronFlavour,Jet_eta,Jet_pt,Jet_btagDeepFlavB

# Systematics block
type=systematics setName=standard sources=hf,lf,hfstats1,hfstats2,lfstats1,lfstats2,cferr1,cferr2
```

### Role-based config key lookup

When multiple tagger managers exist (e.g. b-tag and c-tag), use
`setRole()` to give each a distinct lookup key:

```cpp
btwm->setRole("btag");  // looks for "btagConfig" in the analysis config
ctwm->setRole("ctag");  // looks for "ctagConfig"
```

Then in the analysis config:
```
btagConfig=cfg/btag_config.txt
ctagConfig=cfg/ctag_config.txt
```

### Applying deferred corrections

Correction blocks are stored and applied later when a `CorrectionManager`
is available:

```cpp
twm->setupFromConfigFile();   // parses config, stores correction entries
// ... register corrections in cm ...
twm->applyConfiguredCorrections(*cm);  // calls applyCorrectionlib() for each
```

Alternatively, iterate manually:
```cpp
for (const auto &cc : twm->getConfiguredCorrections()) {
    cm->registerCorrection(cc.correctionName, ...);
    twm->applyCorrectionlib(*cm, cc.correctionName, cc.stringArgs, cc.inputColumns);
}
```

---

## No-filter annotated collection

`defineUnfilteredCollection()` schedules an output collection that contains
**all** input objects without any WP filter.  The per-object WP category
column is still computed (by the normal `execute()` logic), so the objects
are fully annotated and can be sliced downstream:

```cpp
twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
twm->setTaggerColumn("Jet_btagDeepFlavB");
twm->addWorkingPoint("loose",  0.0521f);
twm->addWorkingPoint("medium", 0.3033f);
twm->addWorkingPoint("tight",  0.7489f);
twm->setInputObjectCollection("goodJets");

// All goodJets, annotated with Jet_pt_wp_category:
twm->defineUnfilteredCollection("allJets_tagged");
```

After `execute()`, `allJets_tagged` is a `PhysicsObjectCollection`
identical to `goodJets` in content but with `Jet_pt_wp_category`
available per-object for downstream use.

---

## Further reading

- **Jet energy corrections**: [JET_ENERGY_CORRECTIONS.md](JET_ENERGY_CORRECTIONS.md)
- **All physics-object corrections (electron, muon, tau, photon)**: [CMS_CORRECTIONS.md](CMS_CORRECTIONS.md)
- **Physics object collections**: [PHYSICS_OBJECTS.md](PHYSICS_OBJECTS.md)
- **Systematics and nuisance groups**: [NUISANCE_GROUPS.md](NUISANCE_GROUPS.md)
- **WeightManager reference**: [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md)
- **Getting started guide**: [GETTING_STARTED.md](GETTING_STARTED.md)
