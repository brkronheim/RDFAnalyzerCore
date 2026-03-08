/**
 * @file PhysicsObjectCollection.h
 * @brief Collection of physics objects (e.g. jets, leptons) passing a
 *        selection, with their ROOT 4-vectors and original indices.
 *
 * PhysicsObjectCollection stores, for each selected object:
 *  - A ROOT::Math::LorentzVector (PxPyPzM4D) representing the 4-momentum.
 *  - The object's index in the original full collection.
 *
 * It also provides a generic @ref getValue helper that, given any feature
 * branch (ROOT::VecOps::RVec) from the full collection, returns the values
 * for the selected objects only.
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
 * @endcode
 */
#ifndef PHYSICSOBJECTCOLLECTION_H_INCLUDED
#define PHYSICSOBJECTCOLLECTION_H_INCLUDED

#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PxPyPzM4D.h>
#include <ROOT/RVec.hxx>
#include <cmath>
#include <stdexcept>
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

private:
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
};

#endif // PHYSICSOBJECTCOLLECTION_H_INCLUDED
