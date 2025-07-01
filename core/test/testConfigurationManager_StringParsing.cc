/**
 * @file testConfigurationManager_StringParsing.cc
 * @brief String parsing and list processing tests for ConfigurationManager
 * @date 2025
 *
 * This file contains unit tests for string splitting and list processing in the
 * ConfigurationManager class.
 */

#include "ConfigurationManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

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
 * @brief Test string splitting with various edge cases
 */
TEST_F(BaseConfigSetup, SplitStringEdgeCases) {
  EXPECT_EQ(config->splitString("", ","), (std::vector<std::string>{}));
  EXPECT_EQ(config->splitString("   ", ","), (std::vector<std::string>{}));
  EXPECT_EQ(config->splitString("a,b,c", ","), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a::b::c", "::"), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("no delimiter here", "|"), (std::vector<std::string>{"no delimiter here"}));
  EXPECT_EQ(config->splitString(",a,b,", ","), (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(config->splitString("a,,b,,c", ","), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("root,test", ","), (std::vector<std::string>{"root", "test"}));
  EXPECT_EQ(config->splitString("root , test", ","), (std::vector<std::string>{"root", "test"}));
  EXPECT_EQ(config->splitString("root , test", " "), (std::vector<std::string>{"root", ",", "test"}));
  EXPECT_EQ(config->splitString("root , test", ":"), (std::vector<std::string>{"root , test"}));
}

/**
 * @brief Test string splitting with various delimiters
 */
TEST_F(BaseConfigSetup, SplitStringWithVariousDelimiters) {
  EXPECT_EQ(config->splitString("a:b:c", ":"), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a|b|c", "|"), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a;b;c", ";"), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a--b--c", "--"), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a  b  c", "  "), (std::vector<std::string>{"a", "b", "c"}));
}

/**
 * @brief Comprehensive test of getList functionality
 */
TEST_F(BaseConfigSetup, GetListComprehensive) {
  EXPECT_EQ(config->getList("antiglobs"), (std::vector<std::string>{"output.root", "hists.root"}));
  EXPECT_EQ(config->getList("globs"), (std::vector<std::string>{"root", "test"}));
  config->set("customList", "a|b|c");
  EXPECT_EQ(config->getList("customList", {}, "|"), (std::vector<std::string>{"a", "b", "c"}));
  std::vector<std::string> defaultValue = {"default1", "default2"};
  EXPECT_EQ(config->getList("nonexistentKey", defaultValue), defaultValue);
  config->set("emptyList", "");
  EXPECT_EQ(config->getList("emptyList"), (std::vector<std::string>{}));
  config->set("whitespaceList", "   ,   ,   ");
  EXPECT_EQ(config->getList("whitespaceList"), (std::vector<std::string>{}));
  config->set("singleItem", "onlyOne");
  EXPECT_EQ(config->getList("singleItem"), (std::vector<std::string>{"onlyOne"}));
  config->set("trailingList", "a,b,c,");
  EXPECT_EQ(config->getList("trailingList"), (std::vector<std::string>{"a", "b", "c"}));
  config->set("leadingList", ",a,b,c");
  EXPECT_EQ(config->getList("leadingList"), (std::vector<std::string>{"a", "b", "c"}));
}

/**
 * @brief Test getList with custom delimiters
 */
TEST_F(BaseConfigSetup, GetListWithCustomDelimiter) {
  config->set("semicolonList", "a;b;c");
  EXPECT_EQ(config->getList("semicolonList", {}, ";"), (std::vector<std::string>{"a", "b", "c"}));
  config->set("pipeList", "x|y|z");
  EXPECT_EQ(config->getList("pipeList", {}, "|"), (std::vector<std::string>{"x", "y", "z"}));
}

/**
 * @brief Test getList with default values
 */
TEST_F(BaseConfigSetup, GetListWithDefaultValue) {
  std::vector<std::string> defaultVal = {"default1", "default2", "default3"};
  EXPECT_EQ(config->getList("nonexistent", defaultVal), defaultVal);
  config->set("emptyKey", "");
  EXPECT_EQ(config->getList("emptyKey", defaultVal), (std::vector<std::string>{}));
} 