/**
 * @file testManagerFactory.cc
 * @brief Direct contract tests for ManagerFactory
 * @date 2026
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <ManagerFactory.h>
#include <SystematicManager.h>
#include <test_util.h>
#include <gtest/gtest.h>
#include <memory>

TEST(ManagerFactoryTest, CreateConfigurationManagerProducesValidProvider) {
  ChangeToTestSourceDir();
  auto configManager = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  ASSERT_NE(configManager, nullptr);
  EXPECT_EQ(configManager->get("threads"), "1");
  EXPECT_EQ(configManager->get("saveFile"), "test_output.root");
  EXPECT_FALSE(configManager->getConfigMap().empty());
}

TEST(ManagerFactoryTest, CreateSystematicManagerReturnsValidManager) {
  auto systematicManager = ManagerFactory::createSystematicManager();
  ASSERT_NE(systematicManager, nullptr);
  EXPECT_TRUE(systematicManager->getSystematics().empty());
}

TEST(ManagerFactoryTest, CreateDataManagerWithConfigurationProvider) {
  ChangeToTestSourceDir();
  auto configManager = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto dataManager = ManagerFactory::createDataManager(*configManager);
  ASSERT_NE(dataManager, nullptr);
}
