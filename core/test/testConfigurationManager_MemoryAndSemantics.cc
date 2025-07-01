/**
 * @file testConfigurationManager_MemoryAndSemantics.cc
 * @brief Memory management and copy/move semantics tests for ConfigurationManager
 * @date 2025
 *
 * This file contains unit tests for memory management and copy/move semantics in the ConfigurationManager class.
 */

#include "ConfigurationManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <string>

class BaseConfigSetup : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/config.txt";
    config = new ConfigurationManager(configFile);
  }
  void TearDown() override { delete config; }
  ConfigurationManager *config = nullptr;
};

/**
 * @brief Test memory management
 */
TEST_F(BaseConfigSetup, MemoryManagement) {
  for (int i = 0; i < 100; ++i) {
    ConfigurationManager *localConfig = new ConfigurationManager("cfg/config.txt");
    EXPECT_FALSE(localConfig->getConfigMap().empty());
    delete localConfig;
  }
}

/**
 * @brief Test const correctness
 */
TEST_F(BaseConfigSetup, ConstCorrectness) {
  const ConfigurationManager *constConfig = config;
  auto map = constConfig->getConfigMap();
  std::string value = constConfig->get("saveFile");
  auto list = constConfig->getList("globs");
  auto split = constConfig->splitString("a,b,c", ",");
  EXPECT_EQ(value, "/home/user/outDir/output.root");
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(split.size(), 3);
}

/**
 * @brief Test copy semantics
 */
TEST_F(BaseConfigSetup, CopySemantics) {
  ConfigurationManager copyConfig = *config;
  EXPECT_EQ(copyConfig.get("saveFile"), config->get("saveFile"));
  EXPECT_EQ(copyConfig.get("saveDirectory"), config->get("saveDirectory"));
  EXPECT_THROW({ copyConfig.set("saveFile", "copied_value"); }, std::runtime_error);
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
}

/**
 * @brief Test move semantics
 */
TEST_F(BaseConfigSetup, MoveSemantics) {
  ConfigurationManager moveConfig = std::move(*config);
  EXPECT_EQ(moveConfig.get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(moveConfig.get("saveDirectory"), "/home/user/outDir/");
  EXPECT_EQ(moveConfig.get("saveTree"), "Events");
} 