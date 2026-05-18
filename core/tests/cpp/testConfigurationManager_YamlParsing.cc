/**
 * @file testConfigurationManager_YamlParsing.cc
 * @brief YAML configuration file parsing tests for ConfigurationManager
 * @date 2025
 *
 * This file contains unit tests for YAML configuration file parsing in the
 * ConfigurationManager class to verify YAML configs work identically to text configs.
 */

#include <ConfigurationManager.h>
#include <test_util.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <filesystem>

class YamlConfigSetup : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/config.yaml";
    config = new ConfigurationManager(configFile);
  }
  void TearDown() override { delete config; }
  ConfigurationManager *config = nullptr;
};

// Test that YAML config produces same results as text config
TEST_F(YamlConfigSetup, YamlConfigLoadsProperly) {
  EXPECT_EQ(config->get("directory"), "/home/user/testDir");
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(config->get("saveDirectory"), "/home/user/outDir/");
  EXPECT_EQ(config->get("saveTree"), "Events");
  EXPECT_EQ(config->get("threads"), "-1");
  EXPECT_EQ(config->get("antiglobs"), "output.root , hists.root");
  EXPECT_EQ(config->get("globs"), "root,test");
}

// Multi-Key Configuration Tests with YAML
TEST_F(YamlConfigSetup, ParseMultiKeyYamlConfigWorks) {
  std::string correctionConfigFile = "cfg/correction.yaml";
  auto mapVector = config->parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables"});
  ASSERT_EQ(mapVector.size(), 2);
  auto map1 = mapVector.at(0);
  auto map2 = mapVector.at(1);
  EXPECT_EQ(map1.at("file"), "aux/correction.json");
  EXPECT_EQ(map1.at("correctionName"), "test_correction");
  EXPECT_EQ(map1.at("name"), "test_correction");
  EXPECT_EQ(map1.at("inputVariables"), "float_arg,int_arg");
  EXPECT_EQ(map2.at("file"), "aux/correction.json");
  EXPECT_EQ(map2.at("correctionName"), "test_correction2");
  EXPECT_EQ(map2.at("name"), "test_correction2");
  EXPECT_EQ(map2.at("inputVariables"), "float_arg2,int_arg2");
  auto map2vec = config->parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables", "nonexistent"});
  EXPECT_EQ(map2vec.size(), 0);
}

// Vector Configuration Tests with YAML
TEST_F(YamlConfigSetup, ParseVectorYamlConfigWorks) {
  std::string vectorConfigFile = "cfg/output.yaml";
  EXPECT_EQ(config->parseVectorConfig(vectorConfigFile), 
            (std::vector<std::string>{"var1", "var2", "var3", "var4", "var6"}));
}

// Test auto-detection: .yaml extension should use YamlConfigAdapter
TEST(ConfigurationManagerAutoDetection, YamlExtensionUsesYamlAdapter) {
  ChangeToTestSourceDir();
  // This should auto-detect and use YamlConfigAdapter based on .yaml extension
  ConfigurationManager yamlConfig("cfg/config.yaml");
  EXPECT_EQ(yamlConfig.get("directory"), "/home/user/testDir");
  EXPECT_EQ(yamlConfig.get("threads"), "-1");
}

// Test auto-detection: .txt extension should use TextConfigAdapter
TEST(ConfigurationManagerAutoDetection, TxtExtensionUsesTextAdapter) {
  ChangeToTestSourceDir();
  // This should auto-detect and use TextConfigAdapter based on .txt extension
  ConfigurationManager textConfig("cfg/config.txt");
  EXPECT_EQ(textConfig.get("directory"), "/home/user/testDir");
  EXPECT_EQ(textConfig.get("threads"), "-1");
}

// Test that both formats produce identical results
TEST(ConfigurationManagerConsistency, YamlAndTextProduceSameResults) {
  ChangeToTestSourceDir();
  ConfigurationManager yamlConfig("cfg/config.yaml");
  ConfigurationManager textConfig("cfg/config.txt");
  
  EXPECT_EQ(yamlConfig.get("directory"), textConfig.get("directory"));
  EXPECT_EQ(yamlConfig.get("saveFile"), textConfig.get("saveFile"));
  EXPECT_EQ(yamlConfig.get("threads"), textConfig.get("threads"));
  EXPECT_EQ(yamlConfig.get("globs"), textConfig.get("globs"));
}
