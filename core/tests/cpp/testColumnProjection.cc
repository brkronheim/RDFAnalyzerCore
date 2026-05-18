#include <gtest/gtest.h>

#include <ColumnProjection.h>
#include <analyzer.h>
#include <test_util.h>

#include <ROOT/RVec.hxx>

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

  auto df = analyzer.getDF();
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

  auto df = analyzer.getDF();
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

  auto df = analyzer.getDF();
  auto counts = df.Take<int>("value_count");

  ASSERT_EQ(counts->size(), 2UL);
  EXPECT_EQ(counts->at(0), 4);
  EXPECT_EQ(counts->at(1), 4);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}