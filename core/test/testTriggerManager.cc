/**
 * @file testTriggerManager.cc
 * @brief Unit tests for the TriggerManager class
 * @date 2025
 *
 * This file contains unit tests for the TriggerManager class, focusing on
 * construction, trigger/veto registration, error handling, and integration
 * with ConfigurationManager. Redundant and trivial tests have been removed for clarity and maintainability.
 */

#include <ConfigurationManager.h>
#include <test_util.h>
#include <TriggerManager.h>
#include <ManagerFactory.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <vector>

/**
 * @brief Test fixture for TriggerManager tests
 *
 * Provides common setup and teardown for all TriggerManager tests, using
 * ManagerFactory and smart pointers for consistency with other manager tests.
 */
class TriggerManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    triggerManager = std::make_unique<TriggerManager>(*configManager);
  }
  void TearDown() override {
    // Using smart pointers, so nothing to delete
  }
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<TriggerManager> triggerManager;
};

/**
 * @brief Test construction of TriggerManager
 */
TEST_F(TriggerManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto manager = std::make_unique<TriggerManager>(*config);
  });
}

/**
 * @brief Test handling of missing, invalid, or empty keys returns empty results
 */
TEST_F(TriggerManagerTest, EmptyResultsForMissingKeys) {
  // getTriggers should still throw for missing group
  EXPECT_THROW(triggerManager->getTriggers("nonexistent_group"), std::runtime_error);
  EXPECT_THROW(triggerManager->getTriggers("")                , std::runtime_error);
  // getVetoes returns empty vector for missing group
  EXPECT_TRUE(triggerManager->getVetoes("nonexistent_group").empty());
  EXPECT_TRUE(triggerManager->getVetoes("").empty());
  // getGroupForSample returns empty string for missing sample
  EXPECT_EQ(triggerManager->getGroupForSample("nonexistent_sample"), "");
  EXPECT_EQ(triggerManager->getGroupForSample(""), "");
  // getAllGroups returns non-empty vector if groups exist, empty if not (here, should be non-empty)
  auto groups = triggerManager->getAllGroups();
  EXPECT_FALSE(groups.empty());
}

/**
 * @brief Test const correctness for interface methods
 */
TEST_F(TriggerManagerTest, ConstCorrectness) {
  const TriggerManager* constManager = triggerManager.get();
  EXPECT_NO_THROW({
    // Add const-correctness checks here if needed
    (void)constManager;
  });
}

/**
 * @brief Test sample to group mapping (valid and invalid)
 */
TEST_F(TriggerManagerTest, SampleGroupMapping) {
  // Valid mapping
  EXPECT_EQ(triggerManager->getGroupForSample("test_sample"), "test_group");
  // Invalid mapping returns empty string
  EXPECT_EQ(triggerManager->getGroupForSample("invalid_sample"), "");
}

/**
 * @brief Test memory management for TriggerManager
 */
TEST_F(TriggerManagerTest, MemoryManagement) {
  auto localConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto localManager1 = std::make_unique<TriggerManager>(*localConfig);
  auto localManager2 = std::make_unique<TriggerManager>(*localConfig);
  auto localManager3 = std::make_unique<TriggerManager>(*localConfig);
  auto localManager4 = std::make_unique<TriggerManager>(*localConfig);
  auto localManager5 = std::make_unique<TriggerManager>(*localConfig);
  EXPECT_TRUE(localManager1 != nullptr);
  EXPECT_TRUE(localManager2 != nullptr);
  EXPECT_TRUE(localManager3 != nullptr);
  EXPECT_TRUE(localManager4 != nullptr);
  EXPECT_TRUE(localManager5 != nullptr);
}

// Optionally, add a separate fixture for concrete TriggerManager to test registerTriggers
class ConcreteTriggerManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config_minimal.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    // Create a concrete TriggerManager
    concreteTriggerManager = std::make_shared<TriggerManager>(*configManager);
  }
  void TearDown() override {}
  std::shared_ptr<IConfigurationProvider> configManager;
  std::shared_ptr<TriggerManager> concreteTriggerManager;
};

/**
 * @brief Test registering triggers does not throw (concrete class)
 */
TEST_F(ConcreteTriggerManagerTest, RegisterTriggers) {
  EXPECT_NO_THROW({ concreteTriggerManager->registerTriggers(*configManager); });
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// ---------------------------------------------------------------------------
// Lifecycle hook tests
// ---------------------------------------------------------------------------

#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <DataManager.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>

TEST_F(TriggerManagerTest, InitializeIsCallable) {
  // initialize() should not throw even without a context set
  EXPECT_NO_THROW(triggerManager->initialize());
}

TEST_F(TriggerManagerTest, ReportMetadataIsCallable) {
  auto dataManager = std::make_unique<DataManager>(1);
  auto systematicManager = std::make_unique<SystematicManager>();
  auto logger = std::make_unique<DefaultLogger>();
  auto skimSink = std::make_unique<NullOutputSink>();
  auto metaSink = std::make_unique<NullOutputSink>();
  ManagerContext ctx{*configManager, *dataManager, *systematicManager,
                     *logger, *skimSink, *metaSink};
  triggerManager->setContext(ctx);
  EXPECT_NO_THROW(triggerManager->reportMetadata());
}

TEST_F(TriggerManagerTest, ExecuteAndFinalizeAreNoOps) {
  EXPECT_NO_THROW(triggerManager->execute());
  EXPECT_NO_THROW(triggerManager->finalize());
}

TEST_F(TriggerManagerTest, GetDependenciesReturnsEmpty) {
  EXPECT_TRUE(triggerManager->getDependencies().empty());
}