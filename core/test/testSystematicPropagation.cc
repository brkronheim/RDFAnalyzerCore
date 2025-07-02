#include "analyzer.h"
#include "DataManager.h"
#include "SystematicManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <ROOT/RDataFrame.hxx>
#include <algorithm>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <NDHistogramManager.h>
#include <TriggerManager.h>
#include <BDTManager.h>
/**
 * @file testSystematicPropagation.cc
 * @brief Full stack integration tests for systematic propagation using Analyzer, DataManager, and SystematicManager.
 *
 * This test verifies that systematics registered in SystematicManager are correctly propagated through DataManager and Analyzer,
 * and that defining and filtering variables with systematics results in the expected systematic variation columns in the output.
 */

class SystematicPropagationTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::cout << "Setting up SystematicPropagationTest" << std::endl;
    ChangeToTestSourceDir();
    // Set up SystematicManager and register a systematic for "energy"
    auto sysMgr = std::make_unique<SystematicManager>();
    sysMgr->registerSystematic("testSyst", {"energy"});

    // Set up DataManager with 5 in-memory entries
    auto dataMgr = std::make_unique<DataManager>(5);

    // Set up dummy ConfigurationManager
    auto configMgr = std::make_unique<ConfigurationManager>("cfg/test_data_config_minimal.txt");

    // Set up dummy CorrectionManager
    auto corrMgr = std::make_unique<CorrectionManager>(*configMgr);

    // Set up dummy NDHistogramManager
    auto ndHistMgr = std::make_unique<NDHistogramManager>(*dataMgr, *configMgr, *sysMgr);

    // Set up dummy TriggerManager
    auto trigMgr = std::make_unique<TriggerManager>(*configMgr);

    // Set up dummy BDTManager
    auto bdtMgr = std::make_unique<BDTManager>(*configMgr);

    // Transfer ownership to Analyzer
    analyzer = std::make_unique<Analyzer>(
      std::move(configMgr),
      std::move(dataMgr),
      std::move(bdtMgr),
      std::move(corrMgr),
      std::move(trigMgr),
      std::move(ndHistMgr),
      std::move(sysMgr)
    );

    std::cout << "Analyzer set up" << std::endl;
    // After this, do not use dataMgr or sysMgr
  }

  void TearDown() override {
    // Unique pointers clean up
  }

  std::unique_ptr<Analyzer> analyzer;
};

/**
 * @brief Test that defining a variable with systematics results in systematic variation columns.
 */
TEST_F(SystematicPropagationTest, DefineWithSystematicsPropagates) {
  IDataFrameProvider* idata = &analyzer->getDataFrameProvider();
  auto* dataManager = dynamic_cast<DataManager*>(idata);
  ASSERT_NE(dataManager, nullptr);
  ISystematicManager* sysManager = &analyzer->getSystematicManager();
  ASSERT_NE(sysManager, nullptr);
  // Use makeSystList to define systematic index variables for 'energy'
  dataManager->makeSystList("energy", *sysManager);

  auto df = dataManager->getDataFrame();
  auto columns = df.GetColumnNames();
  // These are index variables for systematic bins, not physics variables
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy"), columns.end());
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystUp"), columns.end());
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystDown"), columns.end());
}

/**
 * @brief Test that filtering on a variable with systematics does not remove systematic columns.
 */
TEST_F(SystematicPropagationTest, FilterWithSystematicsKeepsSystematicColumns) {
  IDataFrameProvider* idata = &analyzer->getDataFrameProvider();
  auto* dataManager = dynamic_cast<DataManager*>(idata);
  ASSERT_NE(dataManager, nullptr);
  ISystematicManager* sysManager = &analyzer->getSystematicManager();
  ASSERT_NE(sysManager, nullptr);
  dataManager->makeSystList("energy", *sysManager);
  // Apply a filter on the systematic index variable (e.g., energy > -1)
  analyzer->Filter("energy_valid", [](float x) { return x > -1.0f; }, {"energy"});
  auto df = dataManager->getDataFrame();
  auto columns = df.GetColumnNames();
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystUp"), columns.end());
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystDown"), columns.end());
}

/**
 * @brief Test that the values in the systematic columns are as expected.
 */
TEST_F(SystematicPropagationTest, SystematicColumnValuesAreCorrect) {
  IDataFrameProvider* idata = &analyzer->getDataFrameProvider();
  auto* dataManager = dynamic_cast<DataManager*>(idata);
  ASSERT_NE(dataManager, nullptr);
  ISystematicManager* sysManager = &analyzer->getSystematicManager();
  ASSERT_NE(sysManager, nullptr);
  dataManager->makeSystList("Systematic", *sysManager);
  auto df = dataManager->getDataFrame();
  auto up = df.Take<float>("Systematic_testSystUp");
  auto down = df.Take<float>("Systematic_testSystDown");
  ASSERT_EQ(up->size(), 5);
  ASSERT_EQ(down->size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ((*up)[i], 1);
    EXPECT_EQ((*down)[i], 2);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
} 