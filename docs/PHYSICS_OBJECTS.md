# PhysicsObjectCollection Reference

> **Header**: `core/interface/PhysicsObjectCollection.h`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Building a Collection](#2-building-a-collection)
3. [Accessing Objects](#3-accessing-objects)
4. [Feature Caching](#4-feature-caching)
5. [Overlap Removal](#5-overlap-removal)
6. [Combinatorics](#6-combinatorics)
7. [TypedPhysicsObjectCollection\<T\>](#7-typedphysicsobjectcollectiont)
8. [PhysicsObjectVariationMap](#8-physicsobjectvariationmap)
9. [Complete C++ Examples](#9-complete-c-examples)

---

## 1. Overview

`PhysicsObjectCollection` is a lightweight C++ class that stores a
**sub-selection** of physics objects (jets, muons, electrons, taus, …)
together with their ROOT Lorentz 4-vectors and their original indices in the
full NanoAOD-style branch.

It is designed to be used as the return type of `RDataFrame::Define` lambdas.
A typical workflow:

1. Build a collection from pt/eta/phi/mass branches + a boolean mask (or
   explicit index list).
2. Use `getValue<T>()` to extract any feature branch for the selected objects.
3. Cache derived quantities with `cacheFeature` / `getCachedFeature`.
4. Remove objects that overlap with another collection via `removeOverlap`.
5. Build di-object or tri-object combinations with `makePairs`,
   `makeCrossPairs`, or `makeTriplets`.

Key capabilities:

| Feature | API |
|---------|-----|
| 4-vector access | `at(i)`, `vectors()` |
| Index tracking | `index(i)`, `indices()` |
| Feature extraction | `getValue<T>(branch)` |
| Derived-property cache | `cacheFeature`, `getCachedFeature`, `hasCachedFeature` |
| Overlap removal | `removeOverlap(other, deltaRMin)` |
| Same-collection pairs | `makePairs(col)` |
| Cross-collection pairs | `makeCrossPairs(col1, col2)` |
| Same-collection triplets | `makeTriplets(col)` |
| User-object attachment | `TypedPhysicsObjectCollection<T>` |
| Systematic variations | `PhysicsObjectVariationMap` |

### Lorentz-vector type

```cpp
using LorentzVec = ROOT::Math::LorentzVector<ROOT::Math::PxPyPzM4D<Float_t>>;
```

Internally, objects are stored in the PxPyPzM representation.  The
constructor converts from (pt, η, φ, mass) automatically.

---

## 2. Building a Collection

### From pt/eta/phi/mass + boolean mask

```cpp
PhysicsObjectCollection(
    const ROOT::VecOps::RVec<Float_t>& pt,
    const ROOT::VecOps::RVec<Float_t>& eta,
    const ROOT::VecOps::RVec<Float_t>& phi,
    const ROOT::VecOps::RVec<Float_t>& mass,
    const ROOT::VecOps::RVec<bool>&    mask);
```

Objects at positions where `mask[i]` is `true` are included.  All input
vectors must have the same size; a `std::runtime_error` is thrown on
mismatch.

```cpp
RVec<bool> mask = (pt > 30.f) && (abs(eta) < 2.4f);
PhysicsObjectCollection goodJets(pt, eta, phi, mass, mask);
```

### From pt/eta/phi/mass + explicit indices

```cpp
PhysicsObjectCollection(
    const ROOT::VecOps::RVec<Float_t>& pt,
    const ROOT::VecOps::RVec<Float_t>& eta,
    const ROOT::VecOps::RVec<Float_t>& phi,
    const ROOT::VecOps::RVec<Float_t>& mass,
    const ROOT::VecOps::RVec<Int_t>&   indices);
```

Only objects at the specified indices are included.  Indices that are
negative or outside the valid range are **silently skipped** (consistent with
the `-9999` sentinel convention used elsewhere in the framework).  A
`std::runtime_error` is thrown if pt/eta/phi/mass sizes are inconsistent.

```cpp
RVec<Int_t> idx = {0, 2, 5};
PhysicsObjectCollection selectedJets(pt, eta, phi, mass, idx);
```

### Default (empty) constructor

```cpp
PhysicsObjectCollection() = default;
```

Creates an empty collection.  Useful as a default-constructed return value
or placeholder.

### Error handling

Any size mismatch among the input vectors throws:

```
std::runtime_error: "PhysicsObjectCollection: input vector size mismatch"
```

---

## 3. Accessing Objects

### `size()` and `empty()`

```cpp
std::size_t size()  const;   // number of selected objects
bool        empty() const;   // true when size() == 0
```

### `at(i)` — single 4-vector

```cpp
const LorentzVec& at(std::size_t i) const;
```

Returns the Lorentz 4-vector of the *i*-th selected object (0-based index
within the collection).  Throws `std::out_of_range` if `i >= size()`.

### `vectors()` — all 4-vectors

```cpp
const std::vector<LorentzVec>& vectors() const;
```

Returns a const reference to the internal vector of all Lorentz vectors.

### `index(i)` — original branch index

```cpp
Int_t index(std::size_t i) const;
```

Returns the index of the *i*-th selected object **in the original full
branch** (before selection).  Throws `std::out_of_range` if `i >= size()`.
This is the value to use when looking up per-object properties in other
branches.

### `indices()` — all original indices

```cpp
const std::vector<Int_t>& indices() const;
```

Returns a const reference to the vector of all original branch indices.

### `getValue<T>(branch)` — extract sub-selection from any branch

```cpp
template <typename T>
ROOT::VecOps::RVec<T> getValue(const ROOT::VecOps::RVec<T>& branch) const;
```

Given a feature branch `branch` (one entry per object in the *full* original
collection), returns an `RVec<T>` containing the values at the stored indices.
Entries with an out-of-range stored index are replaced with `T(-9999)`.

```cpp
auto btagScores = goodJets.getValue(Jet_btagDeepFlavB);
// btagScores[i] == Jet_btagDeepFlavB[goodJets.index(i)]
```

---

## 4. Feature Caching

The feature cache stores arbitrary per-collection derived quantities keyed
by a user-defined name.  The cache uses `std::any` internally, providing
type-safe storage for any copyable/movable type.

**Intended use**: cache expensive derived quantities (b-tag scores, isolation
values, ΔR to another collection) so they can be accessed later in the
analysis without recomputation.

### `cacheFeature<T>(name, value)`

```cpp
template <typename T>
void cacheFeature(const std::string& name, T value);
```

Stores `value` under `name`.  Overwrites any previously cached entry with
the same name.  The value is moved into the internal store.

### `getCachedFeature<T>(name)`

```cpp
template <typename T>
const T& getCachedFeature(const std::string& name) const;
```

Retrieves a previously cached feature by name.

- Throws `std::runtime_error` if `name` was not previously cached.
- Throws `std::bad_any_cast` if `T` does not match the stored type.

### `hasCachedFeature(name)`

```cpp
bool hasCachedFeature(const std::string& name) const;
```

Returns `true` if a feature has been cached under `name`.

### Cache propagation

> **Warning**: The cached-feature store is **not** propagated to collections
> produced by `removeOverlap()` because the per-object indices change after
> overlap removal.  If you need cached features on the cleaned collection,
> rebuild them explicitly after calling `removeOverlap()`.

### Example: caching b-tag scores

```cpp
// Inside a Define lambda
auto buildJetsWithBtag = [](
    const RVec<float>& pt, const RVec<float>& eta,
    const RVec<float>& phi, const RVec<float>& mass,
    const RVec<bool>&  mask,
    const RVec<float>& btag)
{
    PhysicsObjectCollection jets(pt, eta, phi, mass, mask);
    jets.cacheFeature("btagDeepFlavB", jets.getValue(btag));
    return jets;
};

// Later, in another Define
auto nBtagMedium = [](const PhysicsObjectCollection& jets) {
    auto& scores = jets.getCachedFeature<RVec<float>>("btagDeepFlavB");
    return static_cast<int>(Sum(scores > 0.2783f));
};
```

---

## 5. Overlap Removal

### `removeOverlap(other, deltaRMin)`

```cpp
PhysicsObjectCollection removeOverlap(
    const PhysicsObjectCollection& other,
    float deltaRMin) const;
```

Returns a **new** `PhysicsObjectCollection` that contains only the objects
from `*this` that do not overlap with any object in `other`.

**Algorithm**: object *i* is considered to overlap if there exists any object
*j* in `other` with `ΔR(i, j) < deltaRMin` (strictly less-than).

The cached-feature store is **not** copied to the returned collection
(indices change after removal).  Rebuild any needed cached features on
the result if required.

```cpp
// Remove jets that are within ΔR < 0.4 of a selected electron
auto cleanJets = goodJets.removeOverlap(goodElectrons, 0.4f);
```

### `deltaR(v1, v2)` — static helper

```cpp
static float deltaR(const LorentzVec& v1, const LorentzVec& v2);
```

Computes `ΔR = √(Δη² + Δφ²)` between two Lorentz vectors.  The φ difference
is wrapped to `(−π, π]`.

```cpp
float dr = PhysicsObjectCollection::deltaR(jets.at(0), muons.at(0));
```

---

## 6. Combinatorics

Three free functions build all unique combinations from one or two collections
and return them as vectors of lightweight structs.

### `ObjectPair` struct

```cpp
struct ObjectPair {
    using LorentzVec = PhysicsObjectCollection::LorentzVec;

    LorentzVec  p4;     // Sum of the two individual 4-vectors
    std::size_t first;  // Index of first  object in its source collection
    std::size_t second; // Index of second object in its source collection
};
```

`first` and `second` are **within-collection** indices (i.e. values for use
with `col.at(i)` and `col.index(i)`), not original branch indices.

### `ObjectTriplet` struct

```cpp
struct ObjectTriplet {
    using LorentzVec = PhysicsObjectCollection::LorentzVec;

    LorentzVec  p4;     // Sum of the three individual 4-vectors
    std::size_t first;
    std::size_t second;
    std::size_t third;
};
```

### `makePairs(col)` — same-collection pairs

```cpp
inline std::vector<ObjectPair> makePairs(const PhysicsObjectCollection& col);
```

Returns all unique pairs `(i, j)` with `i < j`.  For `N` objects, produces
`N*(N-1)/2` pairs ordered by `(i, j)`.

### `makeCrossPairs(col1, col2)` — cross-collection pairs

```cpp
inline std::vector<ObjectPair> makeCrossPairs(
    const PhysicsObjectCollection& col1,
    const PhysicsObjectCollection& col2);
```

Returns all `N1 * N2` pairs where `first` indexes `col1` and `second`
indexes `col2`.

### `makeTriplets(col)` — same-collection triplets

```cpp
inline std::vector<ObjectTriplet> makeTriplets(const PhysicsObjectCollection& col);
```

Returns all unique triplets `(i, j, k)` with `i < j < k`.

### Usage in RDataFrame

```cpp
// Invariant mass of all jet pairs
auto diJetMasses = df.Define("diJetMass", [](
    const PhysicsObjectCollection& jets)
{
    auto pairs = makePairs(jets);
    RVec<float> masses;
    masses.reserve(pairs.size());
    for (const auto& p : pairs) {
        masses.push_back(static_cast<float>(p.p4.M()));
    }
    return masses;
}, {"goodJets"});
```

---

## 7. TypedPhysicsObjectCollection\<T\>

```cpp
template <typename ObjectType>
class TypedPhysicsObjectCollection : public PhysicsObjectCollection
```

Extends `PhysicsObjectCollection` to additionally store a user-defined object
of type `ObjectType` for each selected entry.  All base-class functionality
(4-vector access, feature cache, overlap removal, combinatorics) is
inherited.

**Use case**: attaching analysis-specific structs (calibration results,
resolved-object records, decorated objects) to the standard representation
without copying data into the generic feature cache.

### Additional Constructors

**From mask + parallel object vector:**

```cpp
TypedPhysicsObjectCollection(
    const RVec<Float_t>&         pt,
    const RVec<Float_t>&         eta,
    const RVec<Float_t>&         phi,
    const RVec<Float_t>&         mass,
    const RVec<bool>&            mask,
    const std::vector<ObjectType>& objectsAll);
```

`objectsAll` must have the same length as `pt`.  Only entries where
`mask[i]` is `true` are stored.  Throws `std::runtime_error` on size
mismatch.

**From explicit indices + parallel object vector:**

```cpp
TypedPhysicsObjectCollection(
    const RVec<Float_t>&         pt,
    const RVec<Float_t>&         eta,
    const RVec<Float_t>&         phi,
    const RVec<Float_t>&         mass,
    const RVec<Int_t>&           indices,
    const std::vector<ObjectType>& objectsAll);
```

Out-of-bounds indices are silently skipped (consistent with the base class).

### Additional Accessors

```cpp
const ObjectType& object(std::size_t i) const;          // i-th user object
const std::vector<ObjectType>& objects() const;         // all user objects
```

Both throw `std::out_of_range` if `i >= size()`.

### Example

```cpp
struct JetInfo {
    float btagScore;
    int   hadronFlavour;
};

// Build typed collection in a Define lambda
auto buildTypedJets = [](
    const RVec<float>& pt, const RVec<float>& eta,
    const RVec<float>& phi, const RVec<float>& mass,
    const RVec<bool>&  mask,
    const RVec<float>& btag, const RVec<int>& flav)
{
    std::vector<JetInfo> allInfo;
    allInfo.reserve(pt.size());
    for (std::size_t i = 0; i < pt.size(); ++i) {
        allInfo.push_back({btag[i], flav[i]});
    }
    return TypedPhysicsObjectCollection<JetInfo>(pt, eta, phi, mass, mask, allInfo);
};

// Access in a filter or further Define
auto countBJets = [](const TypedPhysicsObjectCollection<JetInfo>& jets) {
    int n = 0;
    for (std::size_t i = 0; i < jets.size(); ++i) {
        if (jets.object(i).btagScore > 0.2783f) ++n;
    }
    return n;
};
```

---

## 8. PhysicsObjectVariationMap

```cpp
using PhysicsObjectVariationMap =
    std::unordered_map<std::string, PhysicsObjectCollection>;
```

A named map of `PhysicsObjectCollection` instances representing systematic
variations of the same object type.

### Convention

| Key | Meaning |
|-----|---------|
| `"nominal"` | Central-value (nominal) collection |
| `"JEC_up"` / `"JEC_down"` | Jet Energy Correction up/down variation |
| `"JER_up"` / `"JER_down"` | Jet Energy Resolution variation |
| `"MuonSF_up"` / `"MuonSF_down"` | Muon scale-factor variation |

Any string key is valid.  The `"nominal"` key is the strongly recommended
choice for the central value.

### Usage with SystematicManager

```cpp
// Build nominal and varied jet collections
PhysicsObjectVariationMap jetVariations;

jetVariations["nominal"] = PhysicsObjectCollection(
    Jet_pt_nom, Jet_eta, Jet_phi, Jet_mass_nom, mask_nom);
jetVariations["JEC_up"]  = PhysicsObjectCollection(
    Jet_pt_jec_up, Jet_eta, Jet_phi, Jet_mass_jec_up, mask_jec_up);
jetVariations["JEC_down"] = PhysicsObjectCollection(
    Jet_pt_jec_dn, Jet_eta, Jet_phi, Jet_mass_jec_dn, mask_jec_dn);

// Access a specific variation
const auto& nominalJets = jetVariations.at("nominal");
const auto& jecUpJets   = jetVariations.at("JEC_up");
```

The `PhysicsObjectVariationMap` type alias integrates directly with
`SystematicManager`, which iterates over the map keys to fill varied
histograms.

---

## 9. Complete C++ Examples

All examples below use `analyzer.Define()` and `analyzer.Filter()` — the
framework wrappers that ensure variables and filters are registered with the
provenance system and remain compatible with the `SystematicManager`.

### Example 1: Jet selection with pT/eta cuts

```cpp
// Define goodJets using analyzer.Define()
analyzer.Define("goodJets", [](
    const RVec<float>& pt,  const RVec<float>& eta,
    const RVec<float>& phi, const RVec<float>& mass)
{
    RVec<bool> mask = (pt > 30.f) && (abs(eta) < 2.4f);
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}, {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass"});

// Filter to events with at least 2 good jets
analyzer.Filter("goodJets.size() >= 2", "atLeastTwoJets");
```

### Example 2: Jet-lepton overlap removal

```cpp
// Define electron collection
analyzer.Define("goodElectrons", [](
    const RVec<float>& pt,  const RVec<float>& eta,
    const RVec<float>& phi, const RVec<float>& mass,
    const RVec<bool>&  id_pass)
{
    RVec<bool> mask = (pt > 15.f) && (abs(eta) < 2.5f) && id_pass;
    return PhysicsObjectCollection(pt, eta, phi, mass, mask);
}, {"Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass", "Electron_mvaIso_WP90"});

// Remove jets overlapping with electrons (ΔR < 0.4)
analyzer.Define("cleanJets", [](
    const PhysicsObjectCollection& jets,
    const PhysicsObjectCollection& electrons)
{
    return jets.removeOverlap(electrons, 0.4f);
}, {"goodJets", "goodElectrons"});
```

### Example 3: Di-jet invariant mass with `makePairs`

```cpp
// Build invariant masses of all unique jet pairs
analyzer.Define("dijetMass", [](
    const PhysicsObjectCollection& jets)
{
    auto pairs = makePairs(jets);
    RVec<float> masses;
    masses.reserve(pairs.size());
    for (const auto& p : pairs) {
        masses.push_back(static_cast<float>(p.p4.M()));
    }
    return masses;
}, {"cleanJets"});

// Select events with any pair above 500 GeV
analyzer.Define("hasVBFpair",
    [](const RVec<float>& mjj){ return !mjj.empty() && Max(mjj) > 500.f; },
    {"dijetMass"});
analyzer.Filter("hasVBFpair", "VBFjetPairCut");
```

### Example 4: Jet-lepton cross pairs for HH→bbτν-style analyses

```cpp
// Build all (b-jet, tau) cross pairs
analyzer.Define("bjetTauPairs", [](
    const PhysicsObjectCollection& bjets,
    const PhysicsObjectCollection& taus)
{
    return makeCrossPairs(bjets, taus);
}, {"selectedBJets", "selectedTaus"});

// Find the pair with the smallest ΔR(b, τ)
analyzer.Define("bestBTauDR", [](
    const std::vector<ObjectPair>& pairs,
    const PhysicsObjectCollection& bjets,
    const PhysicsObjectCollection& taus)
{
    float minDR = 999.f;
    for (const auto& p : pairs) {
        float dr = PhysicsObjectCollection::deltaR(
            bjets.at(p.first), taus.at(p.second));
        if (dr < minDR) minDR = dr;
    }
    return minDR;
}, {"bjetTauPairs", "selectedBJets", "selectedTaus"});
```

### Example 5: Caching b-tag scores on a collection

```cpp
analyzer.Define("btaggedJets", [](
    const RVec<float>& pt,  const RVec<float>& eta,
    const RVec<float>& phi, const RVec<float>& mass,
    const RVec<float>& btag)
{
    RVec<bool> mask = (pt > 25.f) && (abs(eta) < 2.5f);
    PhysicsObjectCollection jets(pt, eta, phi, mass, mask);

    // Cache the b-tag discriminant for selected jets
    jets.cacheFeature("btagDeepFlavB", jets.getValue(btag));

    return jets;
}, {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass", "Jet_btagDeepFlavB"});

// Count medium b-tags in a subsequent Define
analyzer.Define("nBtagMedium", [](const PhysicsObjectCollection& jets)
{
    if (!jets.hasCachedFeature("btagDeepFlavB")) return 0;
    const auto& scores = jets.getCachedFeature<RVec<float>>("btagDeepFlavB");
    return static_cast<int>(Sum(scores > 0.2783f));  // Medium WP
}, {"btaggedJets"});
```
