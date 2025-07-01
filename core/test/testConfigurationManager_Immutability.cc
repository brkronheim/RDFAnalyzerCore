/**
 * @file testConfigurationManager_Immutability.cc
 * @brief Immutability tests for ConfigurationManager
 * @date 2025
 *
 * This file contains unit tests for immutability in the ConfigurationManager class.
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
 * @brief Test immutability after construction
 */
TEST_F(BaseConfigSetup, ImmutabilityAfterConstruction) {
  auto map = config->getConfigMap();
  EXPECT_EQ(map.at("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(map.at("saveDirectory"), "/home/user/outDir/");
  map["saveFile"] = "modified_value";
  map["newKey"] = "newValue";
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(config->get("newKey"), "");
  auto internalMap = config->getConfigMap();
  EXPECT_EQ(internalMap.at("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(internalMap.find("newKey"), internalMap.end());
  EXPECT_EQ(map.at("saveFile"), "modified_value");
  EXPECT_EQ(map.at("newKey"), "newValue");
}

/**
 * @brief Test immutability with const reference
 */
TEST_F(BaseConfigSetup, ImmutabilityWithConstReference) {
  const auto &constMap = config->getConfigMap();
  EXPECT_EQ(constMap.at("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(constMap.at("saveDirectory"), "/home/user/outDir/");
  auto nonConstMap = config->getConfigMap();
  nonConstMap["saveFile"] = "modified_via_copy";
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(constMap.at("saveFile"), "/home/user/outDir/output.root");
}

/**
 * @brief Test immutability after set operations
 */
TEST_F(BaseConfigSetup, ImmutabilityAfterSetOperations) {
  auto originalMap = config->getConfigMap();
  config->set("testKey", "testValue");
  EXPECT_EQ(config->get("testKey"), "testValue");
  EXPECT_EQ(originalMap.find("testKey"), originalMap.end());
  EXPECT_EQ(originalMap.at("saveFile"), "/home/user/outDir/output.root");
  auto newMap = config->getConfigMap();
  EXPECT_EQ(newMap.at("testKey"), "testValue");
  EXPECT_EQ(newMap.at("saveFile"), "/home/user/outDir/output.root");
} 