/**
 * @file PhysicsObjectCollection.h
 * @brief Collection of physics objects (e.g. jets, leptons) passing a
 *        selection, with their ROOT 4-vectors and original indices.
 *
 * PhysicsObjectCollection stores, for each selected object:
 *  - A ROOT::Math::LorentzVector (PxPyPzM4D) representing the 4-momentum.
 *  - The object's index in the original full collection.
 *
 * It also provides:
 *  - A generic @ref getValue helper that maps a full-collection feature branch
 *    to values for the selected objects only.
 *  - @ref cacheFeature / @ref getCachedFeature for storing and retrieving
 *    arbitrary per-collection derived quantities by name.
 *  - @ref removeOverlap to veto objects within a given ΔR of another
 *    collection.
 *
 * Combinatoric helpers (@ref makePairs, @ref makeCrossPairs,
 * @ref makeTriplets) build pairs and triplets with their combined 4-vectors.
 *
 * @ref TypedPhysicsObjectCollection<T> extends the base class to additionally
 * store a user-defined object alongside each selected entry.
 *
 * @ref PhysicsObjectVariationMap provides a named map of collections for
 * systematic variations (e.g. "nominal", "JEC_up", "JEC_down").
 *
 * ### Typical usage in a RDataFrame lambda
 * @code
 * // Build a collection of jets with pT > 30 GeV and |eta| < 2.4
 * auto jetCol = df.Define("goodJets", [](
 *         const RVec<float>& pt, const RVec<float>& eta,
 *         const RVec<float>& phi, const RVec<float>& mass) {
 *     RVec<bool> mask = (pt > 30.f) && (abs(eta) < 2.4f);
 *     return PhysicsObjectCollection(pt, eta, phi, mass, mask);
 * }, {"Jet_pt","Jet_eta","Jet_phi","Jet_mass"});
 *
 * // Later, retrieve b-tag scores for the selected jets
 * auto bscores = goodJets.getValue(Jet_btagScore);
 *
 * // Remove jets overlapping with selected electrons (ΔR < 0.4)
 * auto cleanJets = goodJets.removeOverlap(goodElectrons, 0.4f);
 *
 * // Cache a derived feature
 * goodJets.cacheFeature("btagScores", goodJets.getValue(Jet_btagScore));
 * auto& scores = goodJets.getCachedFeature<RVec<float>>("btagScores");
 *
 * // Build all same-collection pairs
 * auto pairs = makePairs(goodJets);
 * for (auto& p : pairs) { float mJJ = p.p4.M(); }
 * @endcode
 */
#ifndef PHYSICSOBJECTCOLLECTION_H_INCLUDED
#define PHYSICSOBJECTCOLLECTION_H_INCLUDED

#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PxPyPzM4D.h>
#include <ROOT/RVec.hxx>
#include <any>
#include <cmath>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @class PhysicsObjectCollection
 * @brief An event-level collection of physics objects that pass a selection.
 *
 * Objects can be selected either by a boolean mask (one entry per object in
 * the original collection) or by an explicit list of indices.  In both cases
 * the class stores:
 *  - One ROOT LorentzVector per selected object (built from pt/eta/phi/mass
 *    or px/py/pz/mass columns).
 *  - The corresponding index in the original (unfiltered) collection.
 *
 * The @ref getValue method maps any feature branch (an RVec with one entry
 * per object in the *full* collection) to a vector containing the values for
 * the selected objects only.
 */
class PhysicsObjectCollection {
public:
    /// Lorentz-vector type used throughout the collection.
    using LorentzVec =
        ROOT::Math::LorentzVector<ROOT::Math::PxPyPzM4D<Float_t>>;

    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /**
     * @brief Default constructor – creates an empty collection.
     */
    PhysicsObjectCollection() = default;

    /**
     * @brief Build a collection from pt/eta/phi/mass columns and a boolean
     *        selection mask.
     *
     * Objects at positions where @p mask is @c true are included.
     *
     * @param pt   Transverse momenta of all objects.
     * @param eta  Pseudorapidities of all objects.
     * @param phi  Azimuthal angles of all objects.
     * @param mass Masses of all objects.
     * @param mask Boolean selection mask (same length as pt).
     * @throws std::runtime_error if the input vectors have inconsistent sizes.
     */
    PhysicsObjectCollection(const ROOT::VecOps::RVec<Float_t> &pt,
                            const ROOT::VecOps::RVec<Float_t> &eta,
                            const ROOT::VecOps::RVec<Float_t> &phi,
                            const ROOT::VecOps::RVec<Float_t> &mass,
                            const ROOT::VecOps::RVec<bool> &mask) {
        const auto n = pt.size();
        if (eta.size() != n || phi.size() != n || mass.size() != n ||
            mask.size() != n) {
            throw std::runtime_error(
                "PhysicsObjectCollection: input vector size mismatch");
        }
        for (std::size_t i = 0; i < n; ++i) {
            if (mask[i]) {
                indices_m.push_back(static_cast<Int_t>(i));
                vectors_m.push_back(
                    makePtEtaPhiM(pt[i], eta[i], phi[i], mass[i]));
            }
        }
    }

    /**
     * @brief Build a collection from pt/eta/phi/mass columns and an explicit
     *        list of indices.
     *
     * Only objects at the specified indices are included.  Indices outside
     * the valid range are silently skipped (consistent with the -9999 sentinel
     * convention used elsewhere in the framework).
     *
     * @param pt      Transverse momenta of all objects.
     * @param eta     Pseudorapidities of all objects.
     * @param phi     Azimuthal angles of all objects.
     * @param mass    Masses of all objects.
     * @param indices Indices into the full collection to include.
     * @throws std::runtime_error if the pt/eta/phi/mass vectors have
     *         inconsistent sizes.
     */
    PhysicsObjectCollection(const ROOT::VecOps::RVec<Float_t> &pt,
                            const ROOT::VecOps::RVec<Float_t> &eta,
                            const ROOT::VecOps::RVec<Float_t> &phi,
                            const ROOT::VecOps::RVec<Float_t> &mass,
                            const ROOT::VecOps::RVec<Int_t> &indices) {
        const auto n = pt.size();
        if (eta.size() != n || phi.size() != n || mass.size() != n) {
            throw std::runtime_error(
                "PhysicsObjectCollection: input vector size mismatch");
        }
        for (Int_t idx : indices) {
            if (idx < 0 || static_cast<std::size_t>(idx) >= n) {
                continue;
            }
            indices_m.push_back(idx);
            vectors_m.push_back(
                makePtEtaPhiM(pt[idx], eta[idx], phi[idx], mass[idx]));
        }
    }

    // ------------------------------------------------------------------
    // Size / emptiness
    // ------------------------------------------------------------------

    /**
     * @brief Number of selected objects in this collection.
     * @return Count of objects.
     */
    std::size_t size() const { return vectors_m.size(); }

    /**
     * @brief Returns true if the collection contains no objects.
     * @return @c true if empty.
     */
    bool empty() const { return vectors_m.empty(); }

    // ------------------------------------------------------------------
    // 4-vector access
    // ------------------------------------------------------------------

    /**
     * @brief Access the 4-vector of the @p i -th selected object.
     * @param i Index within the collection (0-based).
     * @return Const reference to the LorentzVector.
     * @throws std::out_of_range if @p i is out of bounds.
     */
    const LorentzVec &at(std::size_t i) const { return vectors_m.at(i); }

    /**
     * @brief Access all 4-vectors.
     * @return Const reference to the internal vector of LorentzVectors.
     */
    const std::vector<LorentzVec> &vectors() const { return vectors_m; }

    // ------------------------------------------------------------------
    // Index access
    // ------------------------------------------------------------------

    /**
     * @brief Original index (in the full collection) of the @p i -th
     *        selected object.
     * @param i Index within the collection (0-based).
     * @return Index in the original collection.
     * @throws std::out_of_range if @p i is out of bounds.
     */
    Int_t index(std::size_t i) const { return indices_m.at(i); }

    /**
     * @brief Access all original indices.
     * @return Const reference to the internal index vector.
     */
    const std::vector<Int_t> &indices() const { return indices_m; }

    // ------------------------------------------------------------------
    // Feature-branch lookup
    // ------------------------------------------------------------------

    /**
     * @brief Extract the values for the selected objects from a feature branch.
     *
     * Given a feature branch @p branch (one entry per object in the *full*
     * collection), this method returns an RVec containing the values at the
     * indices stored in this collection.  Entries whose stored index is
     * outside @p branch are replaced with the sentinel value @c T(-9999).
     *
     * @tparam T Element type of the feature branch.
     * @param branch Feature branch for the full object collection.
     * @return RVec<T> with one entry per selected object.
     */
    template <typename T>
    ROOT::VecOps::RVec<T>
    getValue(const ROOT::VecOps::RVec<T> &branch) const {
        ROOT::VecOps::RVec<T> result;
        result.reserve(indices_m.size());
        for (Int_t idx : indices_m) {
            if (idx < 0 || static_cast<std::size_t>(idx) >= branch.size()) {
                result.push_back(T(-9999));
            } else {
                result.push_back(branch[idx]);
            }
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Cached derived features
    // ------------------------------------------------------------------

    /**
     * @brief Store an arbitrary derived feature under @p name.
     *
     * Any value that can be stored in a @c std::any (e.g. an
     * @c ROOT::VecOps::RVec<float>) can be cached here for later retrieval
     * without recomputation.  An existing entry with the same name is
     * overwritten.
     *
     * @tparam T  Type of the value to store.
     * @param name  Unique feature name.
     * @param value Value to cache (copied/moved into the internal store).
     */
    template <typename T>
    void cacheFeature(const std::string &name, T value) {
        cachedFeatures_m[name] = std::move(value);
    }

    /**
     * @brief Retrieve a previously cached feature by name.
     *
     * @tparam T  Expected type of the stored value.
     * @param name  Feature name passed to @ref cacheFeature.
     * @return Const reference to the stored value.
     * @throws std::runtime_error if the name was not previously cached.
     * @throws std::bad_any_cast  if @p T does not match the stored type.
     */
    template <typename T>
    const T &getCachedFeature(const std::string &name) const {
        auto it = cachedFeatures_m.find(name);
        if (it == cachedFeatures_m.end()) {
            throw std::runtime_error(
                "PhysicsObjectCollection: cached feature not found: " + name);
        }
        return std::any_cast<const T &>(it->second);
    }

    /**
     * @brief Check whether a feature has been cached under @p name.
     * @param name Feature name.
     * @return @c true if the feature exists in the cache.
     */
    bool hasCachedFeature(const std::string &name) const {
        return cachedFeatures_m.count(name) != 0;
    }

    // ------------------------------------------------------------------
    // Overlap removal
    // ------------------------------------------------------------------

    /**
     * @brief Return a new collection with objects that overlap with @p other
     *        removed.
     *
     * An object at position @p i is considered to overlap with @p other if
     * any object in @p other satisfies ΔR(i, j) < @p deltaRMin.  Overlapping
     * objects are excluded from the returned collection.
     *
     * The cached-feature store is *not* propagated to the result because the
     * indices into the original collection change after the removal.
     *
     * @param other      Reference collection to test against.
     * @param deltaRMin  Minimum ΔR threshold; objects closer than this are
     *                   considered overlapping (strictly less-than comparison).
     * @return New @c PhysicsObjectCollection without the overlapping objects.
     */
    PhysicsObjectCollection removeOverlap(const PhysicsObjectCollection &other,
                                          float deltaRMin) const {
        PhysicsObjectCollection result;
        for (std::size_t i = 0; i < size(); ++i) {
            bool overlaps = false;
            for (std::size_t j = 0; j < other.size(); ++j) {
                if (deltaR(at(i), other.at(j)) < deltaRMin) {
                    overlaps = true;
                    break;
                }
            }
            if (!overlaps) {
                result.vectors_m.push_back(vectors_m[i]);
                result.indices_m.push_back(indices_m[i]);
            }
        }
        return result;
    }

    // ------------------------------------------------------------------
    // ΔR utility
    // ------------------------------------------------------------------

    /**
     * @brief Compute ΔR = √(Δη² + Δφ²) between two Lorentz vectors.
     *
     * The φ difference is wrapped to (−π, π].
     *
     * @param v1 First Lorentz vector.
     * @param v2 Second Lorentz vector.
     * @return ΔR between @p v1 and @p v2.
     */
    static float deltaR(const LorentzVec &v1, const LorentzVec &v2) {
        const float dEta = static_cast<float>(v1.Eta() - v2.Eta());
        float dPhi = static_cast<float>(v1.Phi() - v2.Phi());
        // Wrap dPhi to (-pi, pi]
        constexpr float kPi = 3.14159265f;
        while (dPhi >  kPi) dPhi -= 2.f * kPi;
        while (dPhi < -kPi) dPhi += 2.f * kPi;
        return std::sqrt(dEta * dEta + dPhi * dPhi);
    }

    // ------------------------------------------------------------------
    // Sub-collection filtering
    // ------------------------------------------------------------------

    /**
     * @brief Return a new collection containing only the objects where
     *        @p mask is @c true.
     *
     * The mask is indexed relative to *this* collection (position 0 to
     * @ref size() - 1), not the original full collection.  The cached-feature
     * store is *not* propagated to the result.
     *
     * @param mask Boolean mask of length @ref size().
     * @return New @c PhysicsObjectCollection with only the selected objects.
     * @throws std::runtime_error if @p mask has a different size than this
     *         collection.
     */
    PhysicsObjectCollection withFilter(const ROOT::VecOps::RVec<bool> &mask) const {
        if (mask.size() != size()) {
            throw std::runtime_error(
                "PhysicsObjectCollection::withFilter: mask size mismatch");
        }
        PhysicsObjectCollection result;
        for (std::size_t i = 0; i < size(); ++i) {
            if (mask[i]) {
                result.vectors_m.push_back(vectors_m[i]);
                result.indices_m.push_back(indices_m[i]);
            }
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Correction application
    // ------------------------------------------------------------------

    /**
     * @brief Return a new collection with updated 4-vectors from corrected
     *        kinematic arrays.
     *
     * Each input array is indexed by position in the *original* (full,
     * unfiltered) collection — the same indexing used to build this collection.
     * The stored indices are used to look up each object's corrected values.
     *
     * The cached-feature store is *not* propagated to the result.
     *
     * @param correctedPt   Corrected transverse momenta for the full collection.
     * @param correctedEta  Corrected pseudorapidities for the full collection.
     * @param correctedPhi  Corrected azimuthal angles for the full collection.
     * @param correctedMass Corrected masses for the full collection.
     * @return New @c PhysicsObjectCollection with corrected 4-vectors.
     * @throws std::runtime_error if the corrected arrays have inconsistent sizes.
     * @throws std::out_of_range  if a stored index is out of range for the
     *         corrected arrays.
     */
    PhysicsObjectCollection withCorrectedKinematics(
        const ROOT::VecOps::RVec<Float_t> &correctedPt,
        const ROOT::VecOps::RVec<Float_t> &correctedEta,
        const ROOT::VecOps::RVec<Float_t> &correctedPhi,
        const ROOT::VecOps::RVec<Float_t> &correctedMass) const {
        const auto n = correctedPt.size();
        if (correctedEta.size() != n || correctedPhi.size() != n ||
            correctedMass.size() != n) {
            throw std::runtime_error(
                "PhysicsObjectCollection::withCorrectedKinematics: "
                "input vector size mismatch");
        }
        PhysicsObjectCollection result;
        result.indices_m.reserve(size());
        result.vectors_m.reserve(size());
        for (std::size_t i = 0; i < size(); ++i) {
            const auto idx = indices_m[i];
            if (idx < 0 || static_cast<std::size_t>(idx) >= n) {
                throw std::out_of_range(
                    "PhysicsObjectCollection::withCorrectedKinematics: "
                    "index out of range for corrected arrays");
            }
            result.indices_m.push_back(idx);
            result.vectors_m.push_back(
                makePtEtaPhiM(correctedPt[idx], correctedEta[idx],
                              correctedPhi[idx], correctedMass[idx]));
        }
        return result;
    }

    /**
     * @brief Return a new collection with corrected transverse momenta only.
     *
     * A convenience wrapper around @ref withCorrectedKinematics that replaces
     * only pt; eta, phi, and mass are taken from the existing 4-vectors.
     *
     * @p correctedPt is indexed by position in the *original* (full,
     * unfiltered) collection.
     *
     * @param correctedPt Corrected transverse momenta for the full collection.
     * @return New @c PhysicsObjectCollection with updated pt.
     * @throws std::out_of_range if a stored index is out of range for
     *         @p correctedPt.
     */
    PhysicsObjectCollection withCorrectedPt(
        const ROOT::VecOps::RVec<Float_t> &correctedPt) const {
        const auto n = correctedPt.size();
        PhysicsObjectCollection result;
        result.indices_m.reserve(size());
        result.vectors_m.reserve(size());
        for (std::size_t i = 0; i < size(); ++i) {
            const auto idx = indices_m[i];
            if (idx < 0 || static_cast<std::size_t>(idx) >= n) {
                throw std::out_of_range(
                    "PhysicsObjectCollection::withCorrectedPt: "
                    "index out of range for correctedPt");
            }
            const LorentzVec &old = vectors_m[i];
            const Float_t eta  = static_cast<Float_t>(old.Eta());
            const Float_t phi  = static_cast<Float_t>(old.Phi());
            const Float_t mass = static_cast<Float_t>(old.M());
            result.indices_m.push_back(idx);
            result.vectors_m.push_back(
                makePtEtaPhiM(correctedPt[idx], eta, phi, mass));
        }
        return result;
    }

protected:
    std::vector<LorentzVec> vectors_m; ///< 4-vectors of selected objects.
    std::vector<Int_t>      indices_m; ///< Original indices of selected objects.

    /// Build a PxPyPzM LorentzVector from pt, eta, phi, mass.
    static LorentzVec makePtEtaPhiM(Float_t pt, Float_t eta, Float_t phi,
                                     Float_t mass) {
        const Float_t px = pt * std::cos(phi);
        const Float_t py = pt * std::sin(phi);
        const Float_t pz = pt * std::sinh(eta);
        return LorentzVec(px, py, pz, mass);
    }

private:
    /// Cache of arbitrary derived quantities, keyed by user-defined names.
    std::unordered_map<std::string, std::any> cachedFeatures_m;
};

// ============================================================================
// ObjectPair – two objects from (possibly different) collections
// ============================================================================

/**
 * @struct ObjectPair
 * @brief A pair of physics objects, identified by their within-collection
 *        indices, together with their combined Lorentz 4-vector.
 *
 * Produced by @ref makePairs and @ref makeCrossPairs.
 */
struct ObjectPair {
    /// Lorentz-vector type (re-exported for convenience).
    using LorentzVec = PhysicsObjectCollection::LorentzVec;

    LorentzVec  p4;     ///< Sum of the two individual 4-vectors.
    std::size_t first;  ///< Index of the first  object in its source collection.
    std::size_t second; ///< Index of the second object in its source collection.
};

// ============================================================================
// ObjectTriplet – three objects from (possibly different) collections
// ============================================================================

/**
 * @struct ObjectTriplet
 * @brief A triplet of physics objects, identified by their within-collection
 *        indices, together with their combined Lorentz 4-vector.
 *
 * Produced by @ref makeTriplets.
 */
struct ObjectTriplet {
    /// Lorentz-vector type (re-exported for convenience).
    using LorentzVec = PhysicsObjectCollection::LorentzVec;

    LorentzVec  p4;    ///< Sum of the three individual 4-vectors.
    std::size_t first;  ///< Index of the first  object in its source collection.
    std::size_t second; ///< Index of the second object in its source collection.
    std::size_t third;  ///< Index of the third  object in its source collection.
};

// ============================================================================
// Combinatoric builders
// ============================================================================

/**
 * @brief Build all unique same-collection pairs from @p col.
 *
 * Iterates over all (i, j) pairs with i < j and creates an
 * @ref ObjectPair for each.
 *
 * @param col Source collection.
 * @return Vector of all unique pairs, ordered by (i, j) with i < j.
 */
inline std::vector<ObjectPair> makePairs(const PhysicsObjectCollection &col) {
    std::vector<ObjectPair> pairs;
    const std::size_t n = col.size();
    pairs.reserve(n * (n - 1) / 2);
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = i + 1; j < n; ++j) {
            pairs.push_back({col.at(i) + col.at(j), i, j});
        }
    }
    return pairs;
}

/**
 * @brief Build all cross-collection pairs between @p col1 and @p col2.
 *
 * Every object in @p col1 is paired with every object in @p col2.
 * The @c first index refers to @p col1; @c second refers to @p col2.
 *
 * @param col1 First source collection.
 * @param col2 Second source collection.
 * @return Vector of all cross pairs.
 */
inline std::vector<ObjectPair> makeCrossPairs(const PhysicsObjectCollection &col1,
                                              const PhysicsObjectCollection &col2) {
    std::vector<ObjectPair> pairs;
    pairs.reserve(col1.size() * col2.size());
    for (std::size_t i = 0; i < col1.size(); ++i) {
        for (std::size_t j = 0; j < col2.size(); ++j) {
            pairs.push_back({col1.at(i) + col2.at(j), i, j});
        }
    }
    return pairs;
}

/**
 * @brief Build all unique same-collection triplets from @p col.
 *
 * Iterates over all (i, j, k) triplets with i < j < k.
 *
 * @param col Source collection.
 * @return Vector of all unique triplets, ordered by (i, j, k).
 */
inline std::vector<ObjectTriplet>
makeTriplets(const PhysicsObjectCollection &col) {
    std::vector<ObjectTriplet> triplets;
    const std::size_t n = col.size();
    for (std::size_t i = 0; i < n; ++i) {
        for (std::size_t j = i + 1; j < n; ++j) {
            for (std::size_t k = j + 1; k < n; ++k) {
                triplets.push_back(
                    {col.at(i) + col.at(j) + col.at(k), i, j, k});
            }
        }
    }
    return triplets;
}

// ============================================================================
// TypedPhysicsObjectCollection<T>
// ============================================================================

/**
 * @class TypedPhysicsObjectCollection
 * @brief Extends PhysicsObjectCollection to additionally store a
 *        user-defined object of type @p ObjectType for each selected entry.
 *
 * This allows analyses to attach arbitrary per-object data (e.g. custom
 * calibration structs, resolved-object records, or decorated objects) to the
 * standard 4-vector + index representation.
 *
 * @tparam ObjectType  The user-defined type stored per selected object.
 *
 * ### Example
 * @code
 * struct JetInfo { float btagScore; int flavour; };
 *
 * auto col = TypedPhysicsObjectCollection<JetInfo>(
 *     pt, eta, phi, mass, mask,
 *     allJetInfo);   // std::vector<JetInfo> with one entry per jet
 *
 * JetInfo& info = col.object(0);
 * @endcode
 */
template <typename ObjectType>
class TypedPhysicsObjectCollection : public PhysicsObjectCollection {
public:
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /**
     * @brief Default constructor – creates an empty typed collection.
     */
    TypedPhysicsObjectCollection() = default;

    /**
     * @brief Build from pt/eta/phi/mass, a boolean mask, and a parallel
     *        vector of user-defined objects.
     *
     * The @p objectsAll vector must have the same length as @p pt.  Only
     * entries where @p mask is @c true are included.
     *
     * @param pt         Transverse momenta of all objects.
     * @param eta        Pseudorapidities of all objects.
     * @param phi        Azimuthal angles of all objects.
     * @param mass       Masses of all objects.
     * @param mask       Boolean selection mask (same length as @p pt).
     * @param objectsAll Full vector of user objects (same length as @p pt).
     * @throws std::runtime_error if @p objectsAll has a different size than
     *         @p pt, or if the pt/eta/phi/mass vectors are inconsistent.
     */
    TypedPhysicsObjectCollection(const ROOT::VecOps::RVec<Float_t> &pt,
                                 const ROOT::VecOps::RVec<Float_t> &eta,
                                 const ROOT::VecOps::RVec<Float_t> &phi,
                                 const ROOT::VecOps::RVec<Float_t> &mass,
                                 const ROOT::VecOps::RVec<bool>    &mask,
                                 const std::vector<ObjectType>     &objectsAll)
        : PhysicsObjectCollection(pt, eta, phi, mass, mask) {
        if (objectsAll.size() != pt.size()) {
            throw std::runtime_error(
                "TypedPhysicsObjectCollection: objects size mismatch");
        }
        for (std::size_t i = 0; i < mask.size(); ++i) {
            if (mask[i]) {
                objects_m.push_back(objectsAll[i]);
            }
        }
    }

    /**
     * @brief Build from pt/eta/phi/mass, an explicit index list, and a
     *        parallel vector of user-defined objects.
     *
     * The @p objectsAll vector must have the same length as @p pt.  Only
     * entries at the given @p indices are included; out-of-bounds indices are
     * silently skipped (consistent with the base-class behaviour).
     *
     * @param pt         Transverse momenta of all objects.
     * @param eta        Pseudorapidities of all objects.
     * @param phi        Azimuthal angles of all objects.
     * @param mass       Masses of all objects.
     * @param indices    Indices into the full collection to include.
     * @param objectsAll Full vector of user objects (same length as @p pt).
     * @throws std::runtime_error if @p objectsAll has a different size than
     *         @p pt, or if the pt/eta/phi/mass vectors are inconsistent.
     */
    TypedPhysicsObjectCollection(const ROOT::VecOps::RVec<Float_t> &pt,
                                 const ROOT::VecOps::RVec<Float_t> &eta,
                                 const ROOT::VecOps::RVec<Float_t> &phi,
                                 const ROOT::VecOps::RVec<Float_t> &mass,
                                 const ROOT::VecOps::RVec<Int_t>   &indices,
                                 const std::vector<ObjectType>     &objectsAll)
        : PhysicsObjectCollection(pt, eta, phi, mass, indices) {
        if (objectsAll.size() != pt.size()) {
            throw std::runtime_error(
                "TypedPhysicsObjectCollection: objects size mismatch");
        }
        for (Int_t idx : indices) {
            if (idx >= 0 &&
                static_cast<std::size_t>(idx) < objectsAll.size()) {
                objects_m.push_back(objectsAll[static_cast<std::size_t>(idx)]);
            }
        }
    }

    // ------------------------------------------------------------------
    // User-object access
    // ------------------------------------------------------------------

    /**
     * @brief Access the user-defined object for the @p i -th selected entry.
     * @param i Index within the collection (0-based).
     * @return Const reference to the stored user object.
     * @throws std::out_of_range if @p i is out of bounds.
     */
    const ObjectType &object(std::size_t i) const { return objects_m.at(i); }

    /**
     * @brief Access all stored user-defined objects.
     * @return Const reference to the internal vector of user objects.
     */
    const std::vector<ObjectType> &objects() const { return objects_m; }

    // ------------------------------------------------------------------
    // Sub-collection filtering (typed override)
    // ------------------------------------------------------------------

    /**
     * @brief Return a new typed collection containing only objects where
     *        @p mask is @c true.
     *
     * Both 4-vectors and user-defined objects are filtered consistently.
     * The mask is indexed relative to *this* collection (position 0 to
     * @ref size() - 1).  The cached-feature store is *not* propagated.
     *
     * @param mask Boolean mask of length @ref size().
     * @return New @c TypedPhysicsObjectCollection with only selected objects.
     * @throws std::runtime_error if @p mask has a different size than this
     *         collection.
     */
    TypedPhysicsObjectCollection<ObjectType>
    withFilter(const ROOT::VecOps::RVec<bool> &mask) const {
        if (mask.size() != this->size()) {
            throw std::runtime_error(
                "TypedPhysicsObjectCollection::withFilter: mask size mismatch");
        }
        TypedPhysicsObjectCollection<ObjectType> result;
        for (std::size_t i = 0; i < this->size(); ++i) {
            if (mask[i]) {
                result.vectors_m.push_back(this->vectors_m[i]);
                result.indices_m.push_back(this->indices_m[i]);
                result.objects_m.push_back(objects_m[i]);
            }
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Correction application (typed overrides)
    // ------------------------------------------------------------------

    /**
     * @brief Return a new typed collection with updated 4-vectors from
     *        corrected kinematic arrays, preserving user-defined objects.
     *
     * Each input array is indexed by position in the *original* (full,
     * unfiltered) collection.  User-defined objects are carried over
     * unchanged since corrections affect only the 4-momenta.
     * The cached-feature store is *not* propagated.
     *
     * @param correctedPt   Corrected pt for the full collection.
     * @param correctedEta  Corrected eta for the full collection.
     * @param correctedPhi  Corrected phi for the full collection.
     * @param correctedMass Corrected mass for the full collection.
     * @return New @c TypedPhysicsObjectCollection with corrected 4-vectors.
     * @throws std::runtime_error if the corrected arrays have inconsistent sizes.
     * @throws std::out_of_range  if a stored index is out of range.
     */
    TypedPhysicsObjectCollection<ObjectType>
    withCorrectedKinematics(
        const ROOT::VecOps::RVec<Float_t> &correctedPt,
        const ROOT::VecOps::RVec<Float_t> &correctedEta,
        const ROOT::VecOps::RVec<Float_t> &correctedPhi,
        const ROOT::VecOps::RVec<Float_t> &correctedMass) const {
        const auto n = correctedPt.size();
        if (correctedEta.size() != n || correctedPhi.size() != n ||
            correctedMass.size() != n) {
            throw std::runtime_error(
                "TypedPhysicsObjectCollection::withCorrectedKinematics: "
                "input vector size mismatch");
        }
        TypedPhysicsObjectCollection<ObjectType> result;
        result.indices_m.reserve(this->size());
        result.vectors_m.reserve(this->size());
        result.objects_m.reserve(this->size());
        for (std::size_t i = 0; i < this->size(); ++i) {
            const auto idx = this->indices_m[i];
            if (idx < 0 || static_cast<std::size_t>(idx) >= n) {
                throw std::out_of_range(
                    "TypedPhysicsObjectCollection::withCorrectedKinematics: "
                    "index out of range for corrected arrays");
            }
            result.indices_m.push_back(idx);
            result.vectors_m.push_back(
                PhysicsObjectCollection::makePtEtaPhiM(
                    correctedPt[idx], correctedEta[idx],
                    correctedPhi[idx], correctedMass[idx]));
            result.objects_m.push_back(objects_m[i]);
        }
        return result;
    }

    /**
     * @brief Return a new typed collection with corrected transverse momenta,
     *        preserving user-defined objects.
     *
     * A convenience wrapper around @ref withCorrectedKinematics that replaces
     * only pt; eta, phi, and mass are taken from the existing 4-vectors.
     * @p correctedPt is indexed by position in the *original* collection.
     *
     * @param correctedPt Corrected transverse momenta for the full collection.
     * @return New @c TypedPhysicsObjectCollection with updated pt.
     * @throws std::out_of_range if a stored index is out of range.
     */
    TypedPhysicsObjectCollection<ObjectType>
    withCorrectedPt(const ROOT::VecOps::RVec<Float_t> &correctedPt) const {
        const auto n = correctedPt.size();
        TypedPhysicsObjectCollection<ObjectType> result;
        result.indices_m.reserve(this->size());
        result.vectors_m.reserve(this->size());
        result.objects_m.reserve(this->size());
        for (std::size_t i = 0; i < this->size(); ++i) {
            const auto idx = this->indices_m[i];
            if (idx < 0 || static_cast<std::size_t>(idx) >= n) {
                throw std::out_of_range(
                    "TypedPhysicsObjectCollection::withCorrectedPt: "
                    "index out of range for correctedPt");
            }
            const LorentzVec &old = this->vectors_m[i];
            const Float_t eta  = static_cast<Float_t>(old.Eta());
            const Float_t phi  = static_cast<Float_t>(old.Phi());
            const Float_t mass = static_cast<Float_t>(old.M());
            result.indices_m.push_back(idx);
            result.vectors_m.push_back(
                PhysicsObjectCollection::makePtEtaPhiM(
                    correctedPt[idx], eta, phi, mass));
            result.objects_m.push_back(objects_m[i]);
        }
        return result;
    }

private:
    std::vector<ObjectType> objects_m; ///< User-defined objects for each selected entry.
};

// ============================================================================
// PhysicsObjectVariationMap – systematic variation support
// ============================================================================

/**
 * @brief A named map of PhysicsObjectCollection instances representing
 *        systematic variations of the same object type.
 *
 * Typical convention:
 *  - Key @c "nominal" holds the central-value collection.
 *  - Keys like @c "JEC_up", @c "JEC_down" hold the corresponding varied
 *    collections.
 *
 * ### Example
 * @code
 * PhysicsObjectVariationMap jetVariations;
 * jetVariations["nominal"] = buildJets(pt_nom, eta, phi, mass_nom, mask);
 * jetVariations["JEC_up"]  = buildJets(pt_up,  eta, phi, mass_up,  mask_up);
 * jetVariations["JEC_down"]= buildJets(pt_dn,  eta, phi, mass_dn,  mask_dn);
 *
 * const auto& jets = jetVariations.at("nominal");
 * @endcode
 */
using PhysicsObjectVariationMap =
    std::unordered_map<std::string, PhysicsObjectCollection>;

#endif // PHYSICSOBJECTCOLLECTION_H_INCLUDED
