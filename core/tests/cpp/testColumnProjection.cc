#include <gtest/gtest.h>

#include <ColumnProjection.h>
#include <functions.h>
#include <analyzer.h>
#include <test_util.h>

#include <ROOT/RVec.hxx>

#include <limits>
#include <string>

namespace {

struct TestClassification {
  int count = 0;
  bool enabled = false;
};

using FloatVec = ROOT::VecOps::RVec<Float_t>;

} // namespace

TEST(ColumnProjection, MemberProjectionDefinesMultipleColumns) {
  ChangeToTestSourceDir();

  Analyzer analyzer("cfg/test_data_config.txt");
  analyzer.Define("classification",
                  []() { return TestClassification{7, true}; }, {});

  rdfanalysis::column::defineProjectedColumns<TestClassification>(
      analyzer, "classification",
      rdfanalysis::column::memberProjection("count_value",
                                            &TestClassification::count),
      rdfanalysis::column::memberProjection("enabled_flag",
                                            &TestClassification::enabled));

  auto df = analyzer.getDataFrameUnsafe();
  auto counts = df.Take<int>("count_value");
  auto flags = df.Take<bool>("enabled_flag");

  ASSERT_EQ(counts->size(), 2UL);
  ASSERT_EQ(flags->size(), 2UL);
  EXPECT_EQ(counts->at(0), 7);
  EXPECT_EQ(counts->at(1), 7);
  EXPECT_TRUE(flags->at(0));
  EXPECT_TRUE(flags->at(1));
}

TEST(ColumnProjection, IndexProjectionUnpacksPackedVectors) {
  ChangeToTestSourceDir();

  Analyzer analyzer("cfg/test_data_config.txt");
  analyzer.Define("packed_weights",
                  []() { return FloatVec{1.0f, 2.0f, 3.0f}; }, {});

  rdfanalysis::column::defineProjectedColumns<FloatVec>(
      analyzer, "packed_weights",
      rdfanalysis::column::indexProjection<FloatVec>("weight_nominal", 0),
      rdfanalysis::column::indexProjection<FloatVec>("weight_up", 1),
      rdfanalysis::column::indexProjection<FloatVec>("weight_down", 2));

  auto df = analyzer.getDataFrameUnsafe();
  auto nominal = df.Take<float>("weight_nominal");
  auto up = df.Take<float>("weight_up");
  auto down = df.Take<float>("weight_down");

  ASSERT_EQ(nominal->size(), 2UL);
  ASSERT_EQ(up->size(), 2UL);
  ASSERT_EQ(down->size(), 2UL);
  EXPECT_FLOAT_EQ(nominal->at(0), 1.0f);
  EXPECT_FLOAT_EQ(up->at(0), 2.0f);
  EXPECT_FLOAT_EQ(down->at(0), 3.0f);
}

TEST(ColumnProjection, SizeProjectionCountsCollectionEntries) {
  ChangeToTestSourceDir();

  Analyzer analyzer("cfg/test_data_config.txt");
  analyzer.Define("values", []() { return FloatVec{5.0f, 6.0f, 7.0f, 8.0f}; },
                  {});

  rdfanalysis::column::defineProjectedColumns<FloatVec>(
      analyzer, "values",
      rdfanalysis::column::sizeProjection<FloatVec>("value_count"));

  auto df = analyzer.getDataFrameUnsafe();
  auto counts = df.Take<int>("value_count");

  ASSERT_EQ(counts->size(), 2UL);
  EXPECT_EQ(counts->at(0), 4);
  EXPECT_EQ(counts->at(1), 4);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(VectorUtilities, SelectsRequestedNumberAndPadsMissingRanks) {
  ROOT::VecOps::RVec<int> values{4, 1, 7};
  auto ranks = selectTop<int, 2>(values);
  EXPECT_EQ(std::vector<int>(ranks.begin(), ranks.end()), (std::vector<int>{2, 0}));
  EXPECT_TRUE((selectTop<int, 0>(values).empty()));
  EXPECT_EQ((selectTop<int, 4>(values)[3]), -1);
}

TEST(VectorUtilities, EmptyCollectionsYieldZeroFlattenedRows) {
  FloatVec empty, shortInput{3}, longInput{1, 2};
  EXPECT_TRUE(FlattenRVec(9, empty, longInput).empty());
  EXPECT_TRUE(FlattenRVecCast<int>(empty).empty());
  auto rows = FlattenRVec(9, shortInput, longInput);
  EXPECT_EQ(std::vector<double>(rows.begin(), rows.end()),
            (std::vector<double>{9, 3, 1, 9, 3, 2}));
  EXPECT_THROW(maximumVector(longInput, shortInput), std::invalid_argument);
  EXPECT_EQ(maximumVector(shortInput, longInput)[0], 3);
}

TEST(VectorUtilities, SelectTopRanksNaNsAfterNumericValues) {
  const float nan = std::numeric_limits<float>::quiet_NaN();
  const float inf = std::numeric_limits<float>::infinity();
  FloatVec values{nan, -inf, 2, nan, inf};
  auto ranks = selectTop<float, 5>(values);
  ASSERT_EQ(ranks.size(), 5U);
  EXPECT_EQ(ranks[0], 4);
  EXPECT_EQ(ranks[1], 2);
  EXPECT_EQ(ranks[2], 1);
  EXPECT_TRUE(std::isnan(values.at(ranks[3])));
  EXPECT_TRUE(std::isnan(values.at(ranks[4])));
  EXPECT_NE(ranks[3], ranks[4]);
}
