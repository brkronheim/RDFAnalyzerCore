/**
 * @file testConfigurationManager_ErrorHandling.cc
 * @brief Error handling tests for ConfigurationManager
 * @date 2025
 *
 * This file contains unit tests for error handling in the ConfigurationManager class.
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
 * @brief Test file not found errors for vector config
 */
TEST_F(BaseConfigSetup, ParseVectorConfigFileNotFound) {
  EXPECT_THROW({ config->parseVectorConfig("nonexistent_file.txt"); }, std::runtime_error);
}

/**
 * @brief Test file not found errors for multi-key config
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigFileNotFound) {
  EXPECT_THROW({ config->parseMultiKeyConfig("nonexistent_file.txt", {"key1", "key2"}); }, std::runtime_error);
}

/**
 * @brief Test getting nonexistent keys
 */
TEST_F(BaseConfigSetup, GetReturnsEmptyForNonexistentKey) {
  EXPECT_EQ(config->get("nonexistentKey"), "");
}

/**
 * @brief Test setting existing keys
 */
TEST_F(BaseConfigSetup, SetThrowsForExistingKey) {
  EXPECT_THROW({ config->set("saveFile", "new_value.txt"); }, std::runtime_error);
  EXPECT_THROW({ config->set("saveFile", "new_value"); }, std::runtime_error);
} 