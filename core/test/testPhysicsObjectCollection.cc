/**
 * @file testPhysicsObjectCollection.cc
 * @brief Unit tests for PhysicsObjectCollection.
 *
 * Covers:
 *  - Construction from a boolean mask.
 *  - Construction from an explicit index list.
 *  - Default (empty) construction.
 *  - 4-vector content and correctness.
 *  - Index tracking.
 *  - getValue() feature-branch lookup.
 *  - Edge cases: empty input, out-of-bounds indices, mismatched sizes.
 */

#include <PhysicsObjectCollection.h>
#include <test_util.h>
#include <gtest/gtest.h>

#include <ROOT/RVec.hxx>
#include <cmath>
#include <vector>

using ROOT::VecOps::RVec;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Rough floating-point comparison for Lorentz-vector components.
static bool approxEq(Float_t a, Float_t b, Float_t tol = 1e-4f) {
    return std::fabs(a - b) < tol;
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class PhysicsObjectCollectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Four objects: pt, eta, phi, mass
        pt_   = {10.f, 30.f, 50.f, 20.f};
        eta_  = {0.0f, 1.0f, -1.5f, 2.0f};
        phi_  = {0.5f, -0.5f, 1.0f, -1.0f};
        mass_ = {0.0f, 0.0f, 0.0f, 0.0f};

        // Select objects at index 1 and 2 (pt > 25)
        mask_ = {false, true, true, false};

        // Extra feature branch aligned with the full collection
        btag_ = {0.1f, 0.8f, 0.9f, 0.2f};
    }

    RVec<Float_t> pt_;
    RVec<Float_t> eta_;
    RVec<Float_t> phi_;
    RVec<Float_t> mass_;
    RVec<bool>    mask_;
    RVec<Float_t> btag_;
};

// ---------------------------------------------------------------------------
// Default construction
// ---------------------------------------------------------------------------

TEST(PhysicsObjectCollectionBasic, DefaultIsEmpty) {
    PhysicsObjectCollection col;
    EXPECT_EQ(col.size(), 0u);
    EXPECT_TRUE(col.empty());
    EXPECT_TRUE(col.vectors().empty());
    EXPECT_TRUE(col.indices().empty());
}

// ---------------------------------------------------------------------------
// Construction from mask
// ---------------------------------------------------------------------------

TEST_F(PhysicsObjectCollectionTest, MaskSelectsCorrectCount) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    EXPECT_EQ(col.size(), 2u);
    EXPECT_FALSE(col.empty());
}

TEST_F(PhysicsObjectCollectionTest, MaskStoresCorrectIndices) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    EXPECT_EQ(col.index(0), 1);
    EXPECT_EQ(col.index(1), 2);
}

TEST_F(PhysicsObjectCollectionTest, MaskStoresCorrectPt) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    EXPECT_TRUE(approxEq(col.at(0).Pt(), 30.f));
    EXPECT_TRUE(approxEq(col.at(1).Pt(), 50.f));
}

TEST_F(PhysicsObjectCollectionTest, MaskStoresCorrectEta) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    EXPECT_TRUE(approxEq(col.at(0).Eta(), 1.0f, 1e-3f));
    EXPECT_TRUE(approxEq(col.at(1).Eta(), -1.5f, 1e-3f));
}

TEST_F(PhysicsObjectCollectionTest, AllFalseMaskGivesEmpty) {
    RVec<bool> none = {false, false, false, false};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, none);
    EXPECT_EQ(col.size(), 0u);
    EXPECT_TRUE(col.empty());
}

TEST_F(PhysicsObjectCollectionTest, AllTrueMaskSelectsAll) {
    RVec<bool> all = {true, true, true, true};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, all);
    EXPECT_EQ(col.size(), 4u);
    for (std::size_t i = 0; i < 4u; ++i) {
        EXPECT_EQ(col.index(i), static_cast<Int_t>(i));
    }
}

// ---------------------------------------------------------------------------
// Construction from explicit indices
// ---------------------------------------------------------------------------

TEST_F(PhysicsObjectCollectionTest, IndicesSelectsCorrectCount) {
    RVec<Int_t> idx = {1, 2};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    EXPECT_EQ(col.size(), 2u);
}

TEST_F(PhysicsObjectCollectionTest, IndicesStoresCorrectIndices) {
    RVec<Int_t> idx = {0, 3};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    EXPECT_EQ(col.index(0), 0);
    EXPECT_EQ(col.index(1), 3);
}

TEST_F(PhysicsObjectCollectionTest, IndicesStoresCorrectPt) {
    RVec<Int_t> idx = {0, 3};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    EXPECT_TRUE(approxEq(col.at(0).Pt(), 10.f));
    EXPECT_TRUE(approxEq(col.at(1).Pt(), 20.f));
}

TEST_F(PhysicsObjectCollectionTest, OutOfBoundsIndexSkipped) {
    // Index 99 is out of range; should be silently skipped.
    RVec<Int_t> idx = {1, 99, 2};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    EXPECT_EQ(col.size(), 2u);
    EXPECT_EQ(col.index(0), 1);
    EXPECT_EQ(col.index(1), 2);
}

TEST_F(PhysicsObjectCollectionTest, NegativeIndexSkipped) {
    RVec<Int_t> idx = {-1, 0};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    EXPECT_EQ(col.size(), 1u);
    EXPECT_EQ(col.index(0), 0);
}

TEST_F(PhysicsObjectCollectionTest, EmptyIndexListGivesEmpty) {
    RVec<Int_t> idx = {};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    EXPECT_TRUE(col.empty());
}

// ---------------------------------------------------------------------------
// getValue – feature branch lookup
// ---------------------------------------------------------------------------

TEST_F(PhysicsObjectCollectionTest, GetValueReturnsCorrectBtagForMask) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    auto scores = col.getValue(btag_);
    ASSERT_EQ(scores.size(), 2u);
    EXPECT_TRUE(approxEq(scores[0], btag_[1]));
    EXPECT_TRUE(approxEq(scores[1], btag_[2]));
}

TEST_F(PhysicsObjectCollectionTest, GetValueReturnsCorrectBtagForIndices) {
    RVec<Int_t> idx = {0, 3};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, idx);
    auto scores = col.getValue(btag_);
    ASSERT_EQ(scores.size(), 2u);
    EXPECT_TRUE(approxEq(scores[0], btag_[0]));
    EXPECT_TRUE(approxEq(scores[1], btag_[3]));
}

TEST_F(PhysicsObjectCollectionTest, GetValueOnEmptyCollectionReturnsEmpty) {
    PhysicsObjectCollection col;
    auto scores = col.getValue(btag_);
    EXPECT_TRUE(scores.empty());
}

TEST_F(PhysicsObjectCollectionTest, GetValueWorksWithIntBranch) {
    RVec<Int_t> flavor = {0, 5, 4, 0};
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    auto flavors = col.getValue(flavor);
    ASSERT_EQ(flavors.size(), 2u);
    EXPECT_EQ(flavors[0], 5);
    EXPECT_EQ(flavors[1], 4);
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

TEST_F(PhysicsObjectCollectionTest, MismatchedSizeThrowsForMask) {
    RVec<bool> short_mask = {true, false}; // length 2, not 4
    EXPECT_THROW(
        PhysicsObjectCollection(pt_, eta_, phi_, mass_, short_mask),
        std::runtime_error);
}

TEST_F(PhysicsObjectCollectionTest, MismatchedEtaSizeThrowsForMask) {
    RVec<Float_t> short_eta = {0.f, 1.f}; // length 2, not 4
    EXPECT_THROW(
        PhysicsObjectCollection(pt_, short_eta, phi_, mass_, mask_),
        std::runtime_error);
}

TEST_F(PhysicsObjectCollectionTest, MismatchedSizeThrowsForIndices) {
    RVec<Float_t> short_pt = {10.f, 20.f}; // length 2
    RVec<Int_t>   idx      = {0, 1};
    EXPECT_THROW(
        PhysicsObjectCollection(short_pt, eta_, phi_, mass_, idx),
        std::runtime_error);
}

// ---------------------------------------------------------------------------
// at() / index() bounds-checking
// ---------------------------------------------------------------------------

TEST_F(PhysicsObjectCollectionTest, AtThrowsOutOfRange) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    EXPECT_THROW(col.at(99), std::out_of_range);
}

TEST_F(PhysicsObjectCollectionTest, IndexThrowsOutOfRange) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    EXPECT_THROW(col.index(99), std::out_of_range);
}

// ---------------------------------------------------------------------------
// vectors() / indices() bulk accessors
// ---------------------------------------------------------------------------

TEST_F(PhysicsObjectCollectionTest, BulkVectorsMatchIndividualAt) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    const auto &vecs = col.vectors();
    ASSERT_EQ(vecs.size(), col.size());
    for (std::size_t i = 0; i < col.size(); ++i) {
        EXPECT_TRUE(approxEq(vecs[i].Pt(), col.at(i).Pt()));
    }
}

TEST_F(PhysicsObjectCollectionTest, BulkIndicesMatchIndividualIndex) {
    PhysicsObjectCollection col(pt_, eta_, phi_, mass_, mask_);
    const auto &idxs = col.indices();
    ASSERT_EQ(idxs.size(), col.size());
    for (std::size_t i = 0; i < col.size(); ++i) {
        EXPECT_EQ(idxs[i], col.index(i));
    }
}

// ---------------------------------------------------------------------------
// Physical correctness: verify 4-vector components
// ---------------------------------------------------------------------------

TEST(PhysicsObjectCollectionPhysics, PxPyPzConsistentWithPtEtaPhi) {
    RVec<Float_t> pt   = {40.f};
    RVec<Float_t> eta  = {0.5f};
    RVec<Float_t> phi  = {1.2f};
    RVec<Float_t> mass = {4.f};
    RVec<bool>    mask = {true};

    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    ASSERT_EQ(col.size(), 1u);

    const auto &v = col.at(0);
    EXPECT_TRUE(approxEq(v.Pt(), 40.f, 1e-3f));
    EXPECT_TRUE(approxEq(v.Eta(), 0.5f, 1e-3f));
    EXPECT_TRUE(approxEq(v.Phi(), 1.2f, 1e-3f));
    EXPECT_TRUE(approxEq(v.M(), 4.f, 1e-3f));
}
