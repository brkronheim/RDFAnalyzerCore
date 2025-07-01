/**
 * @file testConfigurationManager_ConfigParsing.cc
 * @brief Configuration file parsing tests for ConfigurationManager
 * @date 2025
 *
 * This file contains unit tests for multi-key, vector, and pair-based configuration file parsing in the
 * ConfigurationManager class.
 */

#include "ConfigurationManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <filesystem>

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

// Multi-Key Configuration Tests
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWorks) {
  std::string correctionConfigFile = "cfg/correction.txt";
  auto mapVector = config->parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables"});
  ASSERT_EQ(mapVector.size(), 1);
  auto map = mapVector.at(0);
  EXPECT_EQ(map.at("file"), "aux/correction.json");
  EXPECT_EQ(map.at("correctionName"), "test_correction");
  EXPECT_EQ(map.at("name"), "test_correction");
  EXPECT_EQ(map.at("inputVariables"), "float_arg,int_arg,str_arg");
  auto map2 = config->parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables", "nonexistent"});
  EXPECT_EQ(map2.size(), 0);
}
TEST_F(BaseConfigSetup, ParseMultiKeyConfigMultipleEntries) {
  std::string tempFile = (std::filesystem::current_path() / "temp_multi.txt").string();
  std::ofstream file(tempFile);
  file << "file=file1.json correctionName=corr1 name=name1 inputVariables=var1,var2\n";
  file << "file=file2.json correctionName=corr2 name=name2 inputVariables=var3,var4\n";
  file.close();
  auto mapVector = config->parseMultiKeyConfig(
      tempFile, {"file", "correctionName", "name", "inputVariables"});
  ASSERT_EQ(mapVector.size(), 2);
  EXPECT_EQ(mapVector[0].at("file"), "file1.json");
  EXPECT_EQ(mapVector[0].at("correctionName"), "corr1");
  EXPECT_EQ(mapVector[0].at("name"), "name1");
  EXPECT_EQ(mapVector[0].at("inputVariables"), "var1,var2");
  EXPECT_EQ(mapVector[1].at("file"), "file2.json");
  EXPECT_EQ(mapVector[1].at("correctionName"), "corr2");
  EXPECT_EQ(mapVector[1].at("name"), "name2");
  EXPECT_EQ(mapVector[1].at("inputVariables"), "var3,var4");
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWithComments) {
  std::string tempFile = (std::filesystem::current_path() / "temp_comments.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "file=file1.json correctionName=corr1 name=name1 inputVariables=var1,var2 # inline comment\n";
  file << "\n";
  file << "file=file2.json correctionName=corr2 name=name2 inputVariables=var3,var4\n";
  file.close();
  auto mapVector = config->parseMultiKeyConfig(
      tempFile, {"file", "correctionName", "name", "inputVariables"});
  ASSERT_EQ(mapVector.size(), 2);
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWithPartialKeys) {
  std::string tempFile = (std::filesystem::current_path() / "temp_partial.txt").string();
  std::ofstream file(tempFile);
  file << "file=file1.json correctionName=corr1 name=name1 inputVariables=var1,var2\n";
  file << "file=file2.json correctionName=corr2 name=name2\n";
  file << "file=file3.json name=name3 inputVariables=var3,var4\n";
  file.close();
  auto mapVector = config->parseMultiKeyConfig(
      tempFile, {"file", "correctionName", "name", "inputVariables"});
  ASSERT_EQ(mapVector.size(), 1);
  EXPECT_EQ(mapVector[0].at("file"), "file1.json");
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWithDuplicateKeysInEntry) {
  std::string tempFile = (std::filesystem::current_path() / "temp_duplicate_entry.txt").string();
  std::ofstream file(tempFile);
  file << "file=file1.json correctionName=corr1 name=name1 inputVariables=var1,var2 name=name2\n";
  file.close();
  EXPECT_THROW({
    config->parseMultiKeyConfig(tempFile, {"file", "correctionName", "name", "inputVariables"});
  }, std::runtime_error);
  std::remove(tempFile.c_str());
}

// Vector Configuration Tests
TEST_F(BaseConfigSetup, ParseVectorConfigWorks) {
  std::string vectorConfigFile = "cfg/output.txt";
  EXPECT_EQ(config->parseVectorConfig(vectorConfigFile), (std::vector<std::string>{"var1", "var2", "var3", "var4", "var6"}));
}
TEST_F(BaseConfigSetup, ParseVectorConfigWithCommentsAndEmptyLines) {
  std::string tempFile = (std::filesystem::current_path() / "temp_vector.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "var1\n";
  file << "\n";
  file << "var2 # inline comment\n";
  file << "var3\n";
  file.close();
  auto vector = config->parseVectorConfig(tempFile);
  EXPECT_EQ(vector, (std::vector<std::string>{"var1", "var2", "var3"}));
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParseVectorConfigEmptyFile) {
  std::string tempFile = (std::filesystem::current_path() / "temp_empty.txt").string();
  std::ofstream file(tempFile);
  file.close();
  auto vector = config->parseVectorConfig(tempFile);
  EXPECT_EQ(vector.size(), 0);
  std::remove(tempFile.c_str());
}

// Pair-Based Configuration Tests
TEST_F(BaseConfigSetup, ParsePairBasedConfigComprehensive) {
  std::string tempFile = (std::filesystem::current_path() / "temp_pairs.txt").string();
  std::ofstream file(tempFile);
  file << "key1=value1\n";
  file << "key2 = value2\n";
  file << "key3=value3 # comment\n";
  file << "# comment line\n";
  file << "key4=value4\n";
  file << "\n";
  file.close();
  auto map = config->parsePairBasedConfig(tempFile);
  EXPECT_EQ(map.size(), 4);
  EXPECT_EQ(map.at("key1"), "value1");
  EXPECT_EQ(map.at("key2"), "value2");
  EXPECT_EQ(map.at("key3"), "value3");
  EXPECT_EQ(map.at("key4"), "value4");
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParsePairBasedConfigEmptyFile) {
  std::string tempFile = (std::filesystem::current_path() / "temp_empty.txt").string();
  std::ofstream file(tempFile);
  file.close();
  auto map = config->parsePairBasedConfig(tempFile);
  EXPECT_TRUE(map.empty());
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParsePairBasedConfigOnlyComments) {
  std::string tempFile = (std::filesystem::current_path() / "temp_comments_only.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "# Another comment\n";
  file << "\n";
  file.close();
  auto map = config->parsePairBasedConfig(tempFile);
  EXPECT_TRUE(map.empty());
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParsePairBasedConfigMalformedLines) {
  std::string tempFile = (std::filesystem::current_path() / "temp_malformed.txt").string();
  std::ofstream file(tempFile);
  file << "key1=value1\n";
  file << "malformed_line_without_equals\n";
  file << "=value_without_key\n";
  file << "key2=value2\n";
  file << "key3=\n";
  file.close();
  auto map = config->parsePairBasedConfig(tempFile);
  EXPECT_EQ(map.size(), 3);
  EXPECT_EQ(map.at("key1"), "value1");
  EXPECT_EQ(map.at("key2"), "value2");
  EXPECT_EQ(map.at("key3"), "");
  std::remove(tempFile.c_str());
}
TEST_F(BaseConfigSetup, ParsePairBasedConfigWithDuplicateKeys) {
  std::string tempFile = (std::filesystem::current_path() / "temp_duplicate.txt").string();
  std::ofstream file(tempFile);
  file << "key1=value1\n";
  file << "key2=value2\n";
  file << "key1=value3\n";
  file.close();
  EXPECT_THROW({ config->parsePairBasedConfig(tempFile); }, std::runtime_error);
  std::remove(tempFile.c_str());
} 