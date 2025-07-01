/**
 * @file testConfigurationManager_Basic.cc
 * @brief Basic unit tests for the ConfigurationManager class
 * @date 2025
 *
 * This file contains basic unit tests for the ConfigurationManager class, including
 * key-value retrieval, setting/getting values, and basic constructor tests.
 */

#include "ConfigurationManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <string>
#include <filesystem>
#include <fstream>

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
 * @brief Test basic key-value retrieval functionality
 */
TEST_F(BaseConfigSetup, GetReturnsCorrectValue) {
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(config->get("saveDirectory"), "/home/user/outDir/");
  EXPECT_EQ(config->get("saveTree"), "Events");
  EXPECT_EQ(config->get("comment"), "");
  EXPECT_EQ(config->get("antiglobs"), "output.root , hists.root");
  EXPECT_EQ(config->get("globs"), "root,test");
}

/**
 * @brief Test setting and getting values
 */
TEST_F(BaseConfigSetup, SetAndGetWorks) {
  config->set("foo", "bar");
  EXPECT_EQ(config->get("foo"), "bar");
  auto map = config->getConfigMap();
  EXPECT_EQ(map.at("foo"), "bar");
}

/**
 * @brief Test adding new values to configuration
 */
TEST_F(BaseConfigSetup, SetAddsNewValue) {
  config->set("newKey", "newValue");
  EXPECT_EQ(config->get("newKey"), "newValue");
  auto map = config->getConfigMap();
  EXPECT_EQ(map.at("newKey"), "newValue");
}

/**
 * @brief Test constructor with non-existent file
 */
TEST(ConfigurationManagerConstructor, NonExistentFile) {
  EXPECT_THROW({ ConfigurationManager config("nonexistent_config.txt"); }, std::runtime_error);
}

/**
 * @brief Test constructor with empty file
 */
TEST(ConfigurationManagerConstructor, EmptyFile) {
  std::string tempFile = (std::filesystem::current_path() / "temp_empty_config.txt").string();
  std::ofstream file(tempFile);
  file.close();
  EXPECT_NO_THROW({
    ConfigurationManager config(tempFile);
    auto map = config.getConfigMap();
    EXPECT_EQ(map.size(), 0);
  });
  std::remove(tempFile.c_str());
}

/**
 * @brief Test constructor with file containing only comments
 */
TEST(ConfigurationManagerConstructor, CommentsOnlyFile) {
  std::string tempFile = (std::filesystem::current_path() / "temp_comments_only.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "\n";
  file << "   # Another comment with leading whitespace\n";
  file << "# key=value # commented out key-value pair\n";
  file.close();
  EXPECT_NO_THROW({
    ConfigurationManager config(tempFile);
    auto map = config.getConfigMap();
    EXPECT_EQ(map.size(), 0);
  });
  std::remove(tempFile.c_str());
} 