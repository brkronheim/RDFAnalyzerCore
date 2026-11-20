#include <gtest/gtest.h>

#include <WorkingPointRecipe.h>
#include <analyzer.h>
#include <test_util.h>

#include <string>
#include <vector>

namespace {

struct FakeTaggerManager {
  std::vector<std::string> lastScaleFactorColumns;
  std::vector<std::string> lastEfficiencyColumns;
  std::string lastOutputColumn;

  void defineFixedWorkingPointWeight(
      const std::vector<std::string> &scaleFactorColumns,
      const std::vector<std::string> &efficiencyColumns,
      const std::string &outputColumn) {
    lastScaleFactorColumns = scaleFactorColumns;
    lastEfficiencyColumns = efficiencyColumns;
    lastOutputColumn = outputColumn;
  }
};

} // namespace

TEST(WorkingPointRecipe, DefineConstantColumnsWritesFallbackValues) {
  ChangeToTestSourceDir();

  Analyzer analyzer("cfg/test_data_config.txt");
  rdfanalysis::weights::defineConstantColumns(
      analyzer, {"w_nominal", "w_up", "w_down"}, 1.0f);

  auto df = analyzer.getDataFrameUnsafe();
  auto nominal = df.Take<float>("w_nominal");
  auto up = df.Take<float>("w_up");
  auto down = df.Take<float>("w_down");

  ASSERT_EQ(nominal->size(), 2UL);
  ASSERT_EQ(up->size(), 2UL);
  ASSERT_EQ(down->size(), 2UL);
  EXPECT_FLOAT_EQ(nominal->at(0), 1.0f);
  EXPECT_FLOAT_EQ(up->at(0), 1.0f);
  EXPECT_FLOAT_EQ(down->at(0), 1.0f);
}

TEST(WorkingPointRecipe, CreateWorkingPointColumnsPreservesOrderAndNames) {
  std::vector<std::string> applied;
  const auto columns = rdfanalysis::weights::createWorkingPointColumns(
      {"L", "M", "T"}, "eff",
      [&](const std::string &workingPoint, const std::string &outputColumn) {
        applied.push_back(workingPoint + ":" + outputColumn);
      });

  EXPECT_EQ(columns,
            (std::vector<std::string>{"eff_L", "eff_M", "eff_T"}));
  EXPECT_EQ(applied,
            (std::vector<std::string>{"L:eff_L", "M:eff_M", "T:eff_T"}));
}

TEST(WorkingPointRecipe, DefineFixedWorkingPointVariationForwardsColumns) {
  FakeTaggerManager taggerManager;
  std::vector<std::string> builtColumns;
  const std::vector<std::string> efficiencies{"eff_L", "eff_M", "eff_T"};

  const auto scaleFactorColumns =
      rdfanalysis::weights::defineFixedWorkingPointVariation(
          taggerManager, {"L", "M", "T"}, efficiencies, "event_weight",
          [&](const std::string &workingPoint) {
            const std::string column = "sf_" + workingPoint;
            builtColumns.push_back(column);
            return column;
          });

  EXPECT_EQ(scaleFactorColumns,
            (std::vector<std::string>{"sf_L", "sf_M", "sf_T"}));
  EXPECT_EQ(builtColumns,
            (std::vector<std::string>{"sf_L", "sf_M", "sf_T"}));
  EXPECT_EQ(taggerManager.lastScaleFactorColumns, scaleFactorColumns);
  EXPECT_EQ(taggerManager.lastEfficiencyColumns, efficiencies);
  EXPECT_EQ(taggerManager.lastOutputColumn, "event_weight");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}