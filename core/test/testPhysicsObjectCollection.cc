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
 *  - cacheFeature / getCachedFeature / hasCachedFeature.
 *  - removeOverlap / deltaR.
 *  - makePairs / makeCrossPairs / makeTriplets combinatoric builders.
 *  - TypedPhysicsObjectCollection<T> user-defined object type.
 *  - PhysicsObjectVariationMap systematic variation map.
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

// ---------------------------------------------------------------------------
// Cached derived features
// ---------------------------------------------------------------------------

class PhysicsObjectCollectionCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        RVec<Float_t> pt   = {10.f, 30.f, 50.f};
        RVec<Float_t> eta  = {0.0f, 1.0f, -1.5f};
        RVec<Float_t> phi  = {0.5f, -0.5f, 1.0f};
        RVec<Float_t> mass = {0.0f, 0.0f, 0.0f};
        RVec<bool>    mask = {false, true, true};
        col_ = PhysicsObjectCollection(pt, eta, phi, mass, mask);
    }
    PhysicsObjectCollection col_;
};

TEST_F(PhysicsObjectCollectionCacheTest, HasCachedFeatureReturnsFalseInitially) {
    EXPECT_FALSE(col_.hasCachedFeature("btag"));
}

TEST_F(PhysicsObjectCollectionCacheTest, CacheAndRetrieveRVecFloat) {
    RVec<Float_t> scores = {0.8f, 0.9f};
    col_.cacheFeature("btag", scores);
    ASSERT_TRUE(col_.hasCachedFeature("btag"));
    const auto &retrieved = col_.getCachedFeature<RVec<Float_t>>("btag");
    ASSERT_EQ(retrieved.size(), 2u);
    EXPECT_TRUE(approxEq(retrieved[0], 0.8f));
    EXPECT_TRUE(approxEq(retrieved[1], 0.9f));
}

TEST_F(PhysicsObjectCollectionCacheTest, CacheAndRetrieveScalarInt) {
    col_.cacheFeature<int>("count", 42);
    ASSERT_TRUE(col_.hasCachedFeature("count"));
    EXPECT_EQ(col_.getCachedFeature<int>("count"), 42);
}

TEST_F(PhysicsObjectCollectionCacheTest, CacheOverwritesExistingEntry) {
    col_.cacheFeature<int>("val", 1);
    col_.cacheFeature<int>("val", 99);
    EXPECT_EQ(col_.getCachedFeature<int>("val"), 99);
}

TEST_F(PhysicsObjectCollectionCacheTest, GetMissingFeatureThrows) {
    EXPECT_THROW(col_.getCachedFeature<int>("nonexistent"), std::runtime_error);
}

TEST_F(PhysicsObjectCollectionCacheTest, GetWrongTypeThrows) {
    col_.cacheFeature<int>("val", 5);
    EXPECT_THROW(col_.getCachedFeature<float>("val"), std::bad_any_cast);
}

// ---------------------------------------------------------------------------
// deltaR static helper
// ---------------------------------------------------------------------------

TEST(PhysicsObjectCollectionDeltaR, SameVectorIsZero) {
    RVec<Float_t> pt   = {40.f};
    RVec<Float_t> eta  = {0.5f};
    RVec<Float_t> phi  = {1.2f};
    RVec<Float_t> mass = {0.f};
    RVec<bool>    mask = {true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    EXPECT_TRUE(approxEq(PhysicsObjectCollection::deltaR(col.at(0), col.at(0)), 0.f, 1e-5f));
}

TEST(PhysicsObjectCollectionDeltaR, KnownDeltaR) {
    // Two objects separated by Δeta=1, Δphi=0  → ΔR=1
    RVec<Float_t> pt   = {40.f, 40.f};
    RVec<Float_t> eta  = {0.0f, 1.0f};
    RVec<Float_t> phi  = {0.0f, 0.0f};
    RVec<Float_t> mass = {0.f,  0.f};
    RVec<bool>    mask = {true, true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    EXPECT_TRUE(approxEq(PhysicsObjectCollection::deltaR(col.at(0), col.at(1)), 1.f, 1e-3f));
}

TEST(PhysicsObjectCollectionDeltaR, PhiWrapping) {
    // Objects near ±pi: Δphi should be ~0 after wrapping, not ~2*pi
    RVec<Float_t> pt   = {40.f, 40.f};
    RVec<Float_t> eta  = {0.0f, 0.0f};
    RVec<Float_t> phi  = {3.10f, -3.10f};
    RVec<Float_t> mass = {0.f,  0.f};
    RVec<bool>    mask = {true, true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    // Δphi = 3.10 - (-3.10) = 6.2 → after wrap ≈ 6.2 - 2π ≈ -0.083
    float dr = PhysicsObjectCollection::deltaR(col.at(0), col.at(1));
    EXPECT_LT(dr, 1.f);  // Should be small, not ~2*pi
}

// ---------------------------------------------------------------------------
// removeOverlap
// ---------------------------------------------------------------------------

class PhysicsObjectCollectionOverlapTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Jets: three objects
        RVec<Float_t> jpt  = {40.f, 35.f, 30.f};
        RVec<Float_t> jeta = {0.0f, 2.0f, 0.0f};
        RVec<Float_t> jphi = {0.0f, 0.0f, 3.0f};
        RVec<Float_t> jmass= {0.f,  0.f,  0.f};
        RVec<bool>    jmask= {true, true, true};
        jets_ = PhysicsObjectCollection(jpt, jeta, jphi, jmass, jmask);

        // Electron overlapping with jet[0] (same eta/phi)
        RVec<Float_t> ept  = {20.f};
        RVec<Float_t> eeta = {0.0f};
        RVec<Float_t> ephi = {0.0f};
        RVec<Float_t> emass= {0.f};
        RVec<bool>    emask= {true};
        elecs_ = PhysicsObjectCollection(ept, eeta, ephi, emass, emask);
    }
    PhysicsObjectCollection jets_;
    PhysicsObjectCollection elecs_;
};

TEST_F(PhysicsObjectCollectionOverlapTest, OverlappingJetIsRemoved) {
    // jet[0] overlaps with the electron (ΔR=0), jet[1] and jet[2] do not
    auto clean = jets_.removeOverlap(elecs_, 0.4f);
    ASSERT_EQ(clean.size(), 2u);
    // Remaining jets should be at original indices 1 and 2
    EXPECT_EQ(clean.index(0), 1);
    EXPECT_EQ(clean.index(1), 2);
}

TEST_F(PhysicsObjectCollectionOverlapTest, EmptyOtherRemovesNothing) {
    PhysicsObjectCollection empty;
    auto clean = jets_.removeOverlap(empty, 0.4f);
    EXPECT_EQ(clean.size(), jets_.size());
}

TEST_F(PhysicsObjectCollectionOverlapTest, SmallDeltaRThresholdKeepsAll) {
    // Threshold of 0 → nothing removed (strict less-than, so ΔR=0 is NOT < 0)
    auto clean = jets_.removeOverlap(elecs_, 0.f);
    EXPECT_EQ(clean.size(), jets_.size());
}

TEST_F(PhysicsObjectCollectionOverlapTest, LargeDeltaRThresholdRemovesAll) {
    // Threshold of 10 → all jets within ΔR<10 of the electron are removed
    auto clean = jets_.removeOverlap(elecs_, 10.f);
    EXPECT_EQ(clean.size(), 0u);
}

TEST_F(PhysicsObjectCollectionOverlapTest, CachedFeaturesNotCopied) {
    jets_.cacheFeature<int>("n", 3);
    auto clean = jets_.removeOverlap(elecs_, 0.4f);
    EXPECT_FALSE(clean.hasCachedFeature("n"));
}

// ---------------------------------------------------------------------------
// makePairs
// ---------------------------------------------------------------------------

class PhysicsObjectCollectionPairsTest : public ::testing::Test {
protected:
    void SetUp() override {
        RVec<Float_t> pt   = {10.f, 20.f, 30.f};
        RVec<Float_t> eta  = {0.f,  0.f,  0.f};
        RVec<Float_t> phi  = {0.f,  1.f,  2.f};
        RVec<Float_t> mass = {1.f,  1.f,  1.f};
        RVec<bool>    mask = {true, true, true};
        col_ = PhysicsObjectCollection(pt, eta, phi, mass, mask);
    }
    PhysicsObjectCollection col_;
};

TEST_F(PhysicsObjectCollectionPairsTest, CorrectPairCount) {
    // 3 objects → C(3,2) = 3 pairs
    auto pairs = makePairs(col_);
    EXPECT_EQ(pairs.size(), 3u);
}

TEST_F(PhysicsObjectCollectionPairsTest, PairIndicesAreOrdered) {
    auto pairs = makePairs(col_);
    EXPECT_EQ(pairs[0].first, 0u);  EXPECT_EQ(pairs[0].second, 1u);
    EXPECT_EQ(pairs[1].first, 0u);  EXPECT_EQ(pairs[1].second, 2u);
    EXPECT_EQ(pairs[2].first, 1u);  EXPECT_EQ(pairs[2].second, 2u);
}

TEST_F(PhysicsObjectCollectionPairsTest, PairP4IsSumOfIndividuals) {
    auto pairs = makePairs(col_);
    // p4 of pair(0,1) should equal col.at(0) + col.at(1)
    auto expected = col_.at(0) + col_.at(1);
    EXPECT_TRUE(approxEq(static_cast<float>(pairs[0].p4.Pt()),
                         static_cast<float>(expected.Pt()), 1e-3f));
    EXPECT_TRUE(approxEq(static_cast<float>(pairs[0].p4.M()),
                         static_cast<float>(expected.M()),  1e-3f));
}

TEST(PhysicsObjectCollectionPairs, EmptyCollectionGivesNoPairs) {
    PhysicsObjectCollection empty;
    EXPECT_TRUE(makePairs(empty).empty());
}

TEST(PhysicsObjectCollectionPairs, SingleObjectGivesNoPairs) {
    RVec<Float_t> pt   = {10.f};
    RVec<Float_t> eta  = {0.f};
    RVec<Float_t> phi  = {0.f};
    RVec<Float_t> mass = {0.f};
    RVec<bool>    mask = {true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    EXPECT_TRUE(makePairs(col).empty());
}

// ---------------------------------------------------------------------------
// makeCrossPairs
// ---------------------------------------------------------------------------

TEST(PhysicsObjectCollectionCrossPairs, CorrectCrossCount) {
    RVec<Float_t> pt1   = {10.f, 20.f};
    RVec<Float_t> eta1  = {0.f,  0.f};
    RVec<Float_t> phi1  = {0.f,  1.f};
    RVec<Float_t> mass1 = {0.f,  0.f};
    RVec<bool>    mask1 = {true, true};
    PhysicsObjectCollection col1(pt1, eta1, phi1, mass1, mask1);

    RVec<Float_t> pt2   = {30.f, 40.f, 50.f};
    RVec<Float_t> eta2  = {0.f,  0.f,  0.f};
    RVec<Float_t> phi2  = {2.f,  3.f, -1.f};
    RVec<Float_t> mass2 = {0.f,  0.f,  0.f};
    RVec<bool>    mask2 = {true, true, true};
    PhysicsObjectCollection col2(pt2, eta2, phi2, mass2, mask2);

    // 2 x 3 = 6 cross pairs
    auto pairs = makeCrossPairs(col1, col2);
    EXPECT_EQ(pairs.size(), 6u);
}

TEST(PhysicsObjectCollectionCrossPairs, CrossPairP4IsSumOfComponents) {
    RVec<Float_t> pt1   = {10.f};
    RVec<Float_t> eta1  = {0.f};
    RVec<Float_t> phi1  = {0.f};
    RVec<Float_t> mass1 = {1.f};
    RVec<bool>    mask1 = {true};
    PhysicsObjectCollection col1(pt1, eta1, phi1, mass1, mask1);

    RVec<Float_t> pt2   = {20.f};
    RVec<Float_t> eta2  = {0.f};
    RVec<Float_t> phi2  = {1.f};
    RVec<Float_t> mass2 = {1.f};
    RVec<bool>    mask2 = {true};
    PhysicsObjectCollection col2(pt2, eta2, phi2, mass2, mask2);

    auto pairs = makeCrossPairs(col1, col2);
    ASSERT_EQ(pairs.size(), 1u);
    auto expected = col1.at(0) + col2.at(0);
    EXPECT_TRUE(approxEq(static_cast<float>(pairs[0].p4.Pt()),
                         static_cast<float>(expected.Pt()), 1e-3f));
}

TEST(PhysicsObjectCollectionCrossPairs, EmptyFirstCollectionGivesNoPairs) {
    PhysicsObjectCollection empty;
    RVec<Float_t> pt   = {10.f};
    RVec<Float_t> eta  = {0.f};
    RVec<Float_t> phi  = {0.f};
    RVec<Float_t> mass = {0.f};
    RVec<bool>    mask = {true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    EXPECT_TRUE(makeCrossPairs(empty, col).empty());
    EXPECT_TRUE(makeCrossPairs(col, empty).empty());
}

// ---------------------------------------------------------------------------
// makeTriplets
// ---------------------------------------------------------------------------

TEST(PhysicsObjectCollectionTriplets, CorrectTripletCount) {
    RVec<Float_t> pt   = {10.f, 20.f, 30.f, 40.f};
    RVec<Float_t> eta  = {0.f,  0.f,  0.f,  0.f};
    RVec<Float_t> phi  = {0.f,  1.f,  2.f,  3.f};
    RVec<Float_t> mass = {0.f,  0.f,  0.f,  0.f};
    RVec<bool>    mask = {true, true, true, true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);

    // C(4,3) = 4 triplets
    auto triplets = makeTriplets(col);
    EXPECT_EQ(triplets.size(), 4u);
}

TEST(PhysicsObjectCollectionTriplets, TripletIndicesAreOrdered) {
    RVec<Float_t> pt   = {10.f, 20.f, 30.f};
    RVec<Float_t> eta  = {0.f,  0.f,  0.f};
    RVec<Float_t> phi  = {0.f,  1.f,  2.f};
    RVec<Float_t> mass = {0.f,  0.f,  0.f};
    RVec<bool>    mask = {true, true, true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);

    auto triplets = makeTriplets(col);
    ASSERT_EQ(triplets.size(), 1u);
    EXPECT_EQ(triplets[0].first,  0u);
    EXPECT_EQ(triplets[0].second, 1u);
    EXPECT_EQ(triplets[0].third,  2u);
}

TEST(PhysicsObjectCollectionTriplets, TripletP4IsSumOfThree) {
    RVec<Float_t> pt   = {10.f, 20.f, 30.f};
    RVec<Float_t> eta  = {0.f,  0.f,  0.f};
    RVec<Float_t> phi  = {0.f,  1.f,  2.f};
    RVec<Float_t> mass = {1.f,  1.f,  1.f};
    RVec<bool>    mask = {true, true, true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);

    auto triplets = makeTriplets(col);
    ASSERT_EQ(triplets.size(), 1u);
    auto expected = col.at(0) + col.at(1) + col.at(2);
    EXPECT_TRUE(approxEq(static_cast<float>(triplets[0].p4.Pt()),
                         static_cast<float>(expected.Pt()), 1e-3f));
}

TEST(PhysicsObjectCollectionTriplets, LessThanThreeObjectsGivesNoTriplets) {
    RVec<Float_t> pt   = {10.f, 20.f};
    RVec<Float_t> eta  = {0.f,  0.f};
    RVec<Float_t> phi  = {0.f,  1.f};
    RVec<Float_t> mass = {0.f,  0.f};
    RVec<bool>    mask = {true, true};
    PhysicsObjectCollection col(pt, eta, phi, mass, mask);
    EXPECT_TRUE(makeTriplets(col).empty());
}

// ---------------------------------------------------------------------------
// TypedPhysicsObjectCollection
// ---------------------------------------------------------------------------

struct TestJetInfo {
    float btagScore;
    int   flavour;
};

class TypedPhysicsObjectCollectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        pt_   = {10.f, 30.f, 50.f, 20.f};
        eta_  = {0.0f, 1.0f, -1.5f, 2.0f};
        phi_  = {0.5f, -0.5f, 1.0f, -1.0f};
        mass_ = {0.0f, 0.0f, 0.0f, 0.0f};
        mask_ = {false, true, true, false};
        idx_  = {1, 2};

        allInfo_ = {
            {0.1f, 0},  // idx 0 – not selected by mask_
            {0.8f, 5},  // idx 1 – selected
            {0.9f, 4},  // idx 2 – selected
            {0.2f, 0},  // idx 3 – not selected by mask_
        };
    }

    RVec<Float_t>          pt_, eta_, phi_, mass_;
    RVec<bool>             mask_;
    RVec<Int_t>            idx_;
    std::vector<TestJetInfo> allInfo_;
};

TEST_F(TypedPhysicsObjectCollectionTest, MaskConstructorSelectsCorrectObjects) {
    TypedPhysicsObjectCollection<TestJetInfo> col(
        pt_, eta_, phi_, mass_, mask_, allInfo_);
    ASSERT_EQ(col.size(), 2u);
    ASSERT_EQ(col.objects().size(), 2u);
    EXPECT_TRUE(approxEq(col.object(0).btagScore, 0.8f));
    EXPECT_EQ(col.object(0).flavour, 5);
    EXPECT_TRUE(approxEq(col.object(1).btagScore, 0.9f));
    EXPECT_EQ(col.object(1).flavour, 4);
}

TEST_F(TypedPhysicsObjectCollectionTest, IndexConstructorSelectsCorrectObjects) {
    TypedPhysicsObjectCollection<TestJetInfo> col(
        pt_, eta_, phi_, mass_, idx_, allInfo_);
    ASSERT_EQ(col.size(), 2u);
    EXPECT_TRUE(approxEq(col.object(0).btagScore, 0.8f));
    EXPECT_EQ(col.object(0).flavour, 5);
    EXPECT_TRUE(approxEq(col.object(1).btagScore, 0.9f));
    EXPECT_EQ(col.object(1).flavour, 4);
}

TEST_F(TypedPhysicsObjectCollectionTest, Base4VectorsAreConsistent) {
    TypedPhysicsObjectCollection<TestJetInfo> col(
        pt_, eta_, phi_, mass_, mask_, allInfo_);
    EXPECT_TRUE(approxEq(col.at(0).Pt(), 30.f));
    EXPECT_TRUE(approxEq(col.at(1).Pt(), 50.f));
}

TEST_F(TypedPhysicsObjectCollectionTest, DefaultConstructorIsEmpty) {
    TypedPhysicsObjectCollection<TestJetInfo> col;
    EXPECT_TRUE(col.empty());
    EXPECT_TRUE(col.objects().empty());
}

TEST_F(TypedPhysicsObjectCollectionTest, ObjectAtThrowsOutOfRange) {
    TypedPhysicsObjectCollection<TestJetInfo> col(
        pt_, eta_, phi_, mass_, mask_, allInfo_);
    EXPECT_THROW(col.object(99), std::out_of_range);
}

TEST_F(TypedPhysicsObjectCollectionTest, MismatchedObjectsSizeThrows) {
    std::vector<TestJetInfo> wrongSize = {{0.5f, 1}};  // too short
    EXPECT_THROW(
        (TypedPhysicsObjectCollection<TestJetInfo>(
            pt_, eta_, phi_, mass_, mask_, wrongSize)),
        std::runtime_error);
}

TEST_F(TypedPhysicsObjectCollectionTest, OutOfBoundsIndexSkippedInObjects) {
    RVec<Int_t> badIdx = {1, 99, 2};
    TypedPhysicsObjectCollection<TestJetInfo> col(
        pt_, eta_, phi_, mass_, badIdx, allInfo_);
    // Index 99 is out of bounds: base class skips it, so 2 objects remain
    ASSERT_EQ(col.size(), 2u);
    ASSERT_EQ(col.objects().size(), 2u);
}

// ---------------------------------------------------------------------------
// PhysicsObjectVariationMap – systematic variation support
// ---------------------------------------------------------------------------

TEST(PhysicsObjectVariationMap, StoreAndRetrieveNominal) {
    RVec<Float_t> pt   = {40.f};
    RVec<Float_t> eta  = {0.f};
    RVec<Float_t> phi  = {0.f};
    RVec<Float_t> mass = {0.f};
    RVec<bool>    mask = {true};

    PhysicsObjectVariationMap varMap;
    varMap["nominal"] = PhysicsObjectCollection(pt, eta, phi, mass, mask);

    ASSERT_EQ(varMap.count("nominal"), 1u);
    EXPECT_EQ(varMap.at("nominal").size(), 1u);
    EXPECT_TRUE(approxEq(varMap.at("nominal").at(0).Pt(), 40.f, 1e-3f));
}

TEST(PhysicsObjectVariationMap, MultipleVariationsAreIndependent) {
    RVec<Float_t> pt_nom = {40.f};
    RVec<Float_t> pt_up  = {44.f};   // +10% JEC
    RVec<Float_t> pt_dn  = {36.f};   // -10% JEC
    RVec<Float_t> eta    = {0.f};
    RVec<Float_t> phi    = {0.f};
    RVec<Float_t> mass   = {0.f};
    RVec<bool>    mask   = {true};

    PhysicsObjectVariationMap varMap;
    varMap["nominal"]  = PhysicsObjectCollection(pt_nom, eta, phi, mass, mask);
    varMap["JEC_up"]   = PhysicsObjectCollection(pt_up,  eta, phi, mass, mask);
    varMap["JEC_down"] = PhysicsObjectCollection(pt_dn,  eta, phi, mass, mask);

    EXPECT_EQ(varMap.size(), 3u);
    EXPECT_TRUE(approxEq(varMap.at("nominal").at(0).Pt(),  40.f, 1e-3f));
    EXPECT_TRUE(approxEq(varMap.at("JEC_up").at(0).Pt(),   44.f, 1e-3f));
    EXPECT_TRUE(approxEq(varMap.at("JEC_down").at(0).Pt(), 36.f, 1e-3f));
}

TEST(PhysicsObjectVariationMap, EmptyMapHasNoEntries) {
    PhysicsObjectVariationMap varMap;
    EXPECT_TRUE(varMap.empty());
}
