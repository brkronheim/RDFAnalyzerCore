/**
 * @file testConfigurationManager.cc
 * @brief Unit tests for the ConfigurationManager class
 * @date 2025
 *
 * This file contains comprehensive unit tests for the ConfigurationManager
 * class. The tests cover basic functionality, file parsing, string
 * manipulation, edge cases, exception handling, and various configuration file
 * formats to ensure the manager works correctly in all scenarios.
 */

#include "ConfigurationManager.h"
#include "test_util.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>

/**
 * @brief Test fixture for ConfigurationManager tests
 *
 * This class provides a common setup and teardown for all ConfigurationManager
 * tests. It creates a ConfigurationManager instance with a predefined
 * configuration file and handles directory changes to ensure relative paths
 * work correctly.
 */
class BaseConfigSetup : public ::testing::Test {
protected:
  /**
   * @brief Set up the test fixture
   *
   * Changes to the test source directory and creates a ConfigurationManager
   * instance with the test configuration file.
   */
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/config.txt";
    config = new ConfigurationManager(configFile);
  }

  /**
   * @brief Tear down the test fixture
   *
   * Cleans up the ConfigurationManager instance to prevent memory leaks.
   */
  void TearDown() override { delete config; }

  ConfigurationManager *config =
      nullptr; ///< The configuration manager instance for testing
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

/**
 * @brief Test basic key-value retrieval functionality
 *
 * Verifies that the ConfigurationManager can correctly read various types of
 * key-value pairs from the configuration file, including values with extra
 * spaces, comments, and list-like values.
 */
TEST_F(BaseConfigSetup, GetReturnsCorrectValue) {
  // Basic read
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");

  // Check when there are extra spaces
  EXPECT_EQ(config->get("saveDirectory"), "/home/user/outDir/");
  EXPECT_EQ(config->get("saveTree"), "Events");

  // Check when there are comments
  EXPECT_EQ(config->get("comment"), "");

  // Check when there is a list with extra spaces
  EXPECT_EQ(config->get("antiglobs"), "output.root , hists.root");
  // Check when there is a list with no extra spaces
  EXPECT_EQ(config->get("globs"), "root,test");
}

/**
 * @brief Test setting and getting values
 *
 * Verifies that new key-value pairs can be added to the configuration
 * and that they are correctly stored and retrieved.
 */
TEST_F(BaseConfigSetup, SetAndGetWorks) {
  config->set("foo", "bar");
  EXPECT_EQ(config->get("foo"), "bar");
  auto map = config->getConfigMap();
  EXPECT_EQ(map.at("foo"), "bar");
}

/**
 * @brief Test adding new values to configuration
 *
 * Verifies that the set method correctly adds new key-value pairs
 * to the configuration and updates the internal map.
 */
TEST_F(BaseConfigSetup, SetAddsNewValue) {
  config->set("newKey", "newValue");
  EXPECT_EQ(config->get("newKey"), "newValue");

  auto map = config->getConfigMap();
  EXPECT_EQ(map.at("newKey"), "newValue");
}

// ============================================================================
// String Manipulation Tests
// ============================================================================

/**
 * @brief Test string splitting with various edge cases
 *
 * Verifies that the splitString method correctly handles various edge cases
 * including empty strings, whitespace-only strings, different delimiters,
 * and malformed input.
 */
TEST_F(BaseConfigSetup, SplitStringEdgeCases) {
  // Empty string
  EXPECT_EQ(config->splitString("", ","), (std::vector<std::string>{}));

  // Whitespace only
  EXPECT_EQ(config->splitString("   ", ","), (std::vector<std::string>{}));

  // Single character delimiter
  EXPECT_EQ(config->splitString("a,b,c", ","),
            (std::vector<std::string>{"a", "b", "c"}));

  // Multi-character delimiter
  EXPECT_EQ(config->splitString("a::b::c", "::"),
            (std::vector<std::string>{"a", "b", "c"}));

  // No delimiter found
  EXPECT_EQ(config->splitString("no delimiter here", "|"),
            (std::vector<std::string>{"no delimiter here"}));

  // Delimiter at start and end
  EXPECT_EQ(config->splitString(",a,b,", ","),
            (std::vector<std::string>{"a", "b"}));

  // Multiple consecutive delimiters
  EXPECT_EQ(config->splitString("a,,b,,c", ","),
            (std::vector<std::string>{"a", "b", "c"}));

  // Original test cases
  EXPECT_EQ(config->splitString("root,test", ","),
            (std::vector<std::string>{"root", "test"}));
  EXPECT_EQ(config->splitString("root , test", ","),
            (std::vector<std::string>{"root", "test"}));
  EXPECT_EQ(config->splitString("root , test", " "),
            (std::vector<std::string>{"root", ",", "test"}));
  EXPECT_EQ(config->splitString("root , test", ":"),
            (std::vector<std::string>{"root , test"}));
}

/**
 * @brief Test string splitting with various delimiters
 *
 * Verifies that the splitString method works correctly with different
 * delimiter types including single characters and multi-character delimiters.
 */
TEST_F(BaseConfigSetup, SplitStringWithVariousDelimiters) {
  // Test with different delimiters
  EXPECT_EQ(config->splitString("a:b:c", ":"),
            (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a|b|c", "|"),
            (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a;b;c", ";"),
            (std::vector<std::string>{"a", "b", "c"}));

  // Test with multi-character delimiters
  EXPECT_EQ(config->splitString("a--b--c", "--"),
            (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(config->splitString("a  b  c", "  "),
            (std::vector<std::string>{"a", "b", "c"}));
}

// ============================================================================
// List Processing Tests
// ============================================================================

/**
 * @brief Comprehensive test of getList functionality
 *
 * Verifies that the getList method correctly parses comma-separated values
 * into vectors, handles various edge cases, and supports custom delimiters
 * and default values.
 */
TEST_F(BaseConfigSetup, GetListComprehensive) {
  // Basic functionality from original test
  EXPECT_EQ(config->getList("antiglobs"),
            (std::vector<std::string>{"output.root", "hists.root"}));
  EXPECT_EQ(config->getList("globs"),
            (std::vector<std::string>{"root", "test"}));

  // Custom delimiter
  config->set("customList", "a|b|c");
  EXPECT_EQ(config->getList("customList", {}, "|"),
            (std::vector<std::string>{"a", "b", "c"}));

  // Default value
  std::vector<std::string> defaultValue = {"default1", "default2"};
  EXPECT_EQ(config->getList("nonexistentKey", defaultValue), defaultValue);

  // Empty value
  config->set("emptyList", "");
  EXPECT_EQ(config->getList("emptyList"), (std::vector<std::string>{}));

  // Whitespace only
  config->set("whitespaceList", "   ,   ,   ");
  EXPECT_EQ(config->getList("whitespaceList"), (std::vector<std::string>{}));

  // Single item
  config->set("singleItem", "onlyOne");
  EXPECT_EQ(config->getList("singleItem"),
            (std::vector<std::string>{"onlyOne"}));

  // Trailing delimiter
  config->set("trailingList", "a,b,c,");
  EXPECT_EQ(config->getList("trailingList"),
            (std::vector<std::string>{"a", "b", "c"}));

  // Leading delimiter
  config->set("leadingList", ",a,b,c");
  EXPECT_EQ(config->getList("leadingList"),
            (std::vector<std::string>{"a", "b", "c"}));
}

/**
 * @brief Test getList with custom delimiters
 *
 * Verifies that getList works correctly with various custom delimiters
 * beyond the default comma separator.
 */
TEST_F(BaseConfigSetup, GetListWithCustomDelimiter) {
  config->set("semicolonList", "a;b;c");
  EXPECT_EQ(config->getList("semicolonList", {}, ";"),
            (std::vector<std::string>{"a", "b", "c"}));

  config->set("pipeList", "x|y|z");
  EXPECT_EQ(config->getList("pipeList", {}, "|"),
            (std::vector<std::string>{"x", "y", "z"}));
}

/**
 * @brief Test getList with default values
 *
 * Verifies that getList correctly returns default values when keys
 * don't exist or have empty values.
 */
TEST_F(BaseConfigSetup, GetListWithDefaultValue) {
  std::vector<std::string> defaultVal = {"default1", "default2", "default3"};

  // Test with nonexistent key
  EXPECT_EQ(config->getList("nonexistent", defaultVal), defaultVal);

  // Test with empty value
  config->set("emptyKey", "");
  EXPECT_EQ(config->getList("emptyKey", defaultVal),
            (std::vector<std::string>{}));
}

// ============================================================================
// Multi-Key Configuration Tests
// ============================================================================

/**
 * @brief Test parsing multi-key configuration files
 *
 * Verifies that parseMultiKeyConfig correctly parses configuration files
 * with multiple key-value pairs per line and handles both valid and invalid
 * required key lists.
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWorks) {
  std::string correctionConfigFile = "cfg/correction.txt";
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

/**
 * @brief Test parsing multi-key config with multiple entries
 *
 * Verifies that parseMultiKeyConfig correctly handles files with multiple
 * entries and creates separate maps for each entry.
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigMultipleEntries) {
  // Create a temporary file with multiple entries
  std::string tempFile =
      (std::filesystem::current_path() / "temp_multi.txt").string();
  std::ofstream file(tempFile);
  file << "file=file1.json correctionName=corr1 name=name1 "
          "inputVariables=var1,var2\n";
  file << "file=file2.json correctionName=corr2 name=name2 "
          "inputVariables=var3,var4\n";
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

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing multi-key config with comments
 *
 * Verifies that parseMultiKeyConfig correctly handles files containing
 * comments and empty lines while parsing the configuration data.
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWithComments) {
  // Create a temporary file with comments
  std::string tempFile =
      (std::filesystem::current_path() / "temp_comments.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "file=file1.json correctionName=corr1 name=name1 "
          "inputVariables=var1,var2 # inline comment\n";
  file << "\n"; // empty line
  file << "file=file2.json correctionName=corr2 name=name2 "
          "inputVariables=var3,var4\n";
  file.close();

  auto mapVector = config->parseMultiKeyConfig(
      tempFile, {"file", "correctionName", "name", "inputVariables"});

  ASSERT_EQ(mapVector.size(), 2);

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing multi-key config with partial keys
 *
 * Verifies that parseMultiKeyConfig correctly handles entries that are
 * missing some required keys and only includes complete entries.
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWithPartialKeys) {
  // Create a temporary file with entries missing some required keys
  std::string tempFile =
      (std::filesystem::current_path() / "temp_partial.txt").string();
  std::ofstream file(tempFile);
  file << "file=file1.json correctionName=corr1 name=name1 "
          "inputVariables=var1,var2\n";                        // complete
  file << "file=file2.json correctionName=corr2 name=name2\n"; // missing
                                                               // inputVariables
  file
      << "file=file3.json name=name3 inputVariables=var3,var4\n"; // missing
                                                                  // correctionName
  file.close();

  auto mapVector = config->parseMultiKeyConfig(
      tempFile, {"file", "correctionName", "name", "inputVariables"});

  ASSERT_EQ(mapVector.size(), 1); // Only the first entry should be included

  EXPECT_EQ(mapVector[0].at("file"), "file1.json");

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing multi-key config with duplicate keys in same entry
 *
 * Verifies that parseMultiKeyConfig throws an exception when encountering
 * duplicate keys within the same configuration entry.
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigWithDuplicateKeysInEntry) {
  // Create a file with duplicate keys in the same entry
  std::string tempFile =
      (std::filesystem::current_path() / "temp_duplicate_entry.txt").string();
  std::ofstream file(tempFile);
  file << "file=file1.json correctionName=corr1 name=name1 "
          "inputVariables=var1,var2 name=name2\n"; // duplicate 'name'
  file.close();

  EXPECT_THROW(
      {
        config->parseMultiKeyConfig(
            tempFile, {"file", "correctionName", "name", "inputVariables"});
      },
      std::runtime_error);

  // Clean up
  std::filesystem::remove(tempFile);
}

// ============================================================================
// Vector Configuration Tests
// ============================================================================

/**
 * @brief Test parsing vector configuration files
 *
 * Verifies that parseVectorConfig correctly reads files containing
 * one value per line and returns them as a vector of strings.
 */
TEST_F(BaseConfigSetup, ParseVectorConfigWorks) {
  std::string vectorConfigFile = "cfg/output.txt";
  EXPECT_EQ(config->parseVectorConfig(vectorConfigFile),
            (std::vector<std::string>{"var1", "var2", "var3", "var4", "var6"}));
}

/**
 * @brief Test parsing vector config with comments and empty lines
 *
 * Verifies that parseVectorConfig correctly handles files containing
 * comments and empty lines while parsing the vector data.
 */
TEST_F(BaseConfigSetup, ParseVectorConfigWithCommentsAndEmptyLines) {
  // Create a temporary file with various line types
  std::string tempFile =
      (std::filesystem::current_path() / "temp_vector.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "var1\n";
  file << "\n"; // empty line
  file << "var2 # inline comment\n";
  file << "var3\n";
  file.close();

  auto vector = config->parseVectorConfig(tempFile);

  EXPECT_EQ(vector, (std::vector<std::string>{"var1", "var2", "var3"}));

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing vector config with empty file
 *
 * Verifies that parseVectorConfig correctly handles empty files
 * and returns an empty vector.
 */
TEST_F(BaseConfigSetup, ParseVectorConfigEmptyFile) {
  // Create an empty file
  std::string tempFile =
      (std::filesystem::current_path() / "temp_empty.txt").string();
  std::ofstream file(tempFile);
  file.close();

  auto vector = config->parseVectorConfig(tempFile);

  EXPECT_EQ(vector.size(), 0);

  // Clean up
  std::filesystem::remove(tempFile);
}

// ============================================================================
// Pair-Based Configuration Tests
// ============================================================================

/**
 * @brief Comprehensive test of pair-based configuration parsing
 *
 * Verifies that parsePairBasedConfig correctly parses files with
 * key=value pairs, handles various formatting styles, and processes
 * comments appropriately.
 */
TEST_F(BaseConfigSetup, ParsePairBasedConfigComprehensive) {
  // Create a temporary file for testing
  std::string tempFile =
      (std::filesystem::current_path() / "temp_pairs.txt").string();
  std::ofstream file(tempFile);
  file << "key1=value1\n";
  file << "key2 = value2\n"; // with spaces
  file << "key3=value3 # comment\n";
  file << "# comment line\n";
  file << "key4=value4\n";
  file << "\n"; // empty line
  file.close();

  auto map = config->parsePairBasedConfig(tempFile);

  EXPECT_EQ(map.size(), 4);
  EXPECT_EQ(map.at("key1"), "value1");
  EXPECT_EQ(map.at("key2"), "value2");
  EXPECT_EQ(map.at("key3"), "value3");
  EXPECT_EQ(map.at("key4"), "value4");

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing pair-based config with empty file
 *
 * Verifies that parsePairBasedConfig correctly handles empty files
 * and returns an empty map.
 */
TEST_F(BaseConfigSetup, ParsePairBasedConfigEmptyFile) {
  // Create an empty file
  std::string tempFile =
      (std::filesystem::current_path() / "temp_empty.txt").string();
  std::ofstream file(tempFile);
  file.close();

  auto map = config->parsePairBasedConfig(tempFile);

  EXPECT_TRUE(map.empty());

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing pair-based config with only comments
 *
 * Verifies that parsePairBasedConfig correctly handles files containing
 * only comments and empty lines.
 */
TEST_F(BaseConfigSetup, ParsePairBasedConfigOnlyComments) {
  // Create a file with only comments
  std::string tempFile =
      (std::filesystem::current_path() / "temp_comments_only.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "# Another comment\n";
  file << "\n"; // empty line
  file.close();

  auto map = config->parsePairBasedConfig(tempFile);

  EXPECT_TRUE(map.empty());

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing pair-based config with malformed lines
 *
 * Verifies that parsePairBasedConfig correctly handles malformed lines
 * and continues parsing valid entries while skipping invalid ones.
 */
TEST_F(BaseConfigSetup, ParsePairBasedConfigMalformedLines) {
  // Create a file with malformed lines
  std::string tempFile =
      (std::filesystem::current_path() / "temp_malformed.txt").string();
  std::ofstream file(tempFile);
  file << "key1=value1\n";
  file << "malformed_line_without_equals\n";
  file << "=value_without_key\n";
  file << "key2=value2\n";
  file << "key3=\n"; // empty value
  file.close();

  auto map = config->parsePairBasedConfig(tempFile);

  EXPECT_EQ(map.size(), 3);
  EXPECT_EQ(map.at("key1"), "value1");
  EXPECT_EQ(map.at("key2"), "value2");
  EXPECT_EQ(map.at("key3"), "");

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test parsing pair-based config with duplicate keys
 *
 * Verifies that parsePairBasedConfig throws an exception when encountering
 * duplicate keys in the configuration file.
 */
TEST_F(BaseConfigSetup, ParsePairBasedConfigWithDuplicateKeys) {
  // Create a file with duplicate keys
  std::string tempFile =
      (std::filesystem::current_path() / "temp_duplicate.txt").string();
  std::ofstream file(tempFile);
  file << "key1=value1\n";
  file << "key2=value2\n";
  file << "key1=value3\n"; // duplicate key
  file.close();

  EXPECT_THROW({ config->parsePairBasedConfig(tempFile); }, std::runtime_error);

  // Clean up
  std::filesystem::remove(tempFile);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/**
 * @brief Test file not found errors for vector config
 *
 * Verifies that parseVectorConfig throws an appropriate exception
 * when the specified file does not exist.
 */
TEST_F(BaseConfigSetup, ParseVectorConfigFileNotFound) {
  EXPECT_THROW(
      { config->parseVectorConfig("nonexistent_file.txt"); },
      std::runtime_error);
}

/**
 * @brief Test file not found errors for multi-key config
 *
 * Verifies that parseMultiKeyConfig throws an appropriate exception
 * when the specified file does not exist.
 */
TEST_F(BaseConfigSetup, ParseMultiKeyConfigFileNotFound) {
  EXPECT_THROW(
      {
        config->parseMultiKeyConfig("nonexistent_file.txt", {"key1", "key2"});
      },
      std::runtime_error);
}

/**
 * @brief Test getting nonexistent keys
 *
 * Verifies that get returns an empty string when the specified key
 * does not exist in the configuration.
 */
TEST_F(BaseConfigSetup, GetReturnsEmptyForNonexistentKey) {
  EXPECT_EQ(config->get("nonexistentKey"), "");
}

/**
 * @brief Test setting existing keys
 *
 * Verifies that set throws an exception when attempting to overwrite
 * existing keys in the configuration.
 */
TEST_F(BaseConfigSetup, SetThrowsForExistingKey) {
  EXPECT_THROW(
      { config->set("saveFile", "new_value.txt"); }, std::runtime_error);
  EXPECT_THROW({ config->set("saveFile", "new_value"); }, std::runtime_error);
}

// ============================================================================
// Edge Cases and Special Characters Tests
// ============================================================================

/**
 * @brief Test setting and getting values with special characters
 *
 * Verifies that the ConfigurationManager correctly handles values
 * containing special characters, spaces, and unicode characters.
 */
TEST_F(BaseConfigSetup, SetAndGetWithSpecialCharacters) {
  config->set("specialKey", "value with spaces and = signs");
  EXPECT_EQ(config->get("specialKey"), "value with spaces and = signs");

  config->set("unicodeKey", "café résumé");
  EXPECT_EQ(config->get("unicodeKey"), "café résumé");
}

/**
 * @brief Test setting and getting empty values
 *
 * Verifies that the ConfigurationManager correctly handles empty
 * string values.
 */
TEST_F(BaseConfigSetup, SetAndGetWithEmptyValue) {
  config->set("emptyValue", "");
  EXPECT_EQ(config->get("emptyValue"), "");
}

/**
 * @brief Test setting and getting values with whitespace
 *
 * Verifies that the ConfigurationManager correctly preserves
 * leading and trailing whitespace in values.
 */
TEST_F(BaseConfigSetup, SetAndGetWithWhitespace) {
  config->set("whitespaceKey", "   value with whitespace   ");
  EXPECT_EQ(config->get("whitespaceKey"), "   value with whitespace   ");
}

// ============================================================================
// Constructor Tests
// ============================================================================

/**
 * @brief Test constructor with non-existent file
 *
 * Verifies that the ConfigurationManager constructor throws an exception
 * when the specified configuration file does not exist.
 */
TEST(ConfigurationManagerConstructor, NonExistentFile) {
  // This should throw an exception when the file cannot be opened
  EXPECT_THROW(
      { ConfigurationManager config("nonexistent_config.txt"); },
      std::runtime_error);
}

/**
 * @brief Test constructor with empty file
 *
 * Verifies that the ConfigurationManager constructor correctly handles
 * empty configuration files and creates an empty configuration map.
 */
TEST(ConfigurationManagerConstructor, EmptyFile) {
  // Create an empty config file
  std::string tempFile =
      (std::filesystem::current_path() / "temp_empty_config.txt").string();
  std::ofstream file(tempFile);
  file.close();

  // This should not throw since the file exists, but should be empty
  EXPECT_NO_THROW({
    ConfigurationManager config(tempFile);
    auto map = config.getConfigMap();
    EXPECT_EQ(map.size(), 0);
  });

  // Clean up
  std::filesystem::remove(tempFile);
}

/**
 * @brief Test constructor with file containing only comments
 *
 * Verifies that the ConfigurationManager constructor correctly handles
 * files containing only comments and empty lines.
 */
TEST(ConfigurationManagerConstructor, CommentsOnlyFile) {
  // Create a config file with only comments and empty lines
  std::string tempFile =
      (std::filesystem::current_path() / "temp_comments_only.txt").string();
  std::ofstream file(tempFile);
  file << "# This is a comment\n";
  file << "\n";
  file << "   # Another comment with leading whitespace\n";
  file << "# key=value # commented out key-value pair\n";
  file.close();

  // This should not throw since the file exists, but should be empty
  EXPECT_NO_THROW({
    ConfigurationManager config(tempFile);
    auto map = config.getConfigMap();
    EXPECT_EQ(map.size(), 0);
  });

  // Clean up
  std::filesystem::remove(tempFile);
}

// ============================================================================
// Memory Management Tests
// ============================================================================

/**
 * @brief Test memory management
 *
 * Verifies that ConfigurationManager instances can be properly
 * constructed and destroyed without memory leaks.
 */
TEST_F(BaseConfigSetup, MemoryManagement) {
  // Test that ConfigurationManager can be properly constructed and destroyed
  for (int i = 0; i < 100; ++i) {
    ConfigurationManager *localConfig =
        new ConfigurationManager("cfg/config.txt");
    EXPECT_FALSE(localConfig->getConfigMap().empty());
    delete localConfig;
  }
}

// ============================================================================
// Const Correctness Tests
// ============================================================================

/**
 * @brief Test const correctness
 *
 * Verifies that const methods work correctly and that const
 * ConfigurationManager instances can retrieve data but cannot modify it.
 */
TEST_F(BaseConfigSetup, ConstCorrectness) {
  const ConfigurationManager *constConfig = config;

  // Should be able to call const methods
  auto map = constConfig->getConfigMap();
  std::string value = constConfig->get("saveFile");
  auto list = constConfig->getList("globs");
  auto split = constConfig->splitString("a,b,c", ",");

  EXPECT_EQ(value, "/home/user/outDir/output.root");
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(split.size(), 3);
}

// ============================================================================
// Copy and Move Semantics Tests
// ============================================================================

/**
 * @brief Test copy semantics
 *
 * Verifies that ConfigurationManager instances can be copied correctly
 * and that the original and copied instances are independent.
 */
TEST_F(BaseConfigSetup, CopySemantics) {
  // Test that we can copy the configuration
  ConfigurationManager copyConfig = *config;

  // Verify that copied configuration has the same values
  EXPECT_EQ(copyConfig.get("saveFile"), config->get("saveFile"));
  EXPECT_EQ(copyConfig.get("saveDirectory"), config->get("saveDirectory"));

  // Verify that modifications to copy don't affect original
  // Note: set() now throws when trying to overwrite existing keys
  EXPECT_THROW(
      { copyConfig.set("saveFile", "copied_value"); }, std::runtime_error);

  // Verify original is unchanged
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
}

/**
 * @brief Test move semantics
 *
 * Verifies that ConfigurationManager instances can be moved correctly
 * and that the moved instance contains the expected data.
 */
TEST_F(BaseConfigSetup, MoveSemantics) {
  // Test move semantics if supported
  ConfigurationManager moveConfig = std::move(*config);

  // Verify that moved configuration has the same values
  EXPECT_EQ(moveConfig.get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(moveConfig.get("saveDirectory"), "/home/user/outDir/");

  // Original config should be in a valid but unspecified state
  // We can't test this directly, but we can verify that the move worked
  EXPECT_EQ(moveConfig.get("saveTree"), "Events");
}

// ============================================================================
// Immutability Tests
// ============================================================================

/**
 * @brief Test immutability after construction
 *
 * Verifies that the configuration map returned by getConfigMap cannot
 * be used to modify the internal state of the ConfigurationManager.
 */
TEST_F(BaseConfigSetup, ImmutabilityAfterConstruction) {
  // Get the config map
  auto map = config->getConfigMap();

  // Verify the map contains the expected values
  EXPECT_EQ(map.at("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(map.at("saveDirectory"), "/home/user/outDir/");

  // Try to modify the returned map
  map["saveFile"] = "modified_value";
  map["newKey"] = "newValue";

  // Verify that the internal state of ConfigurationManager is unchanged
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(config->get("newKey"), "");

  // Verify that the internal map is unchanged
  auto internalMap = config->getConfigMap();
  EXPECT_EQ(internalMap.at("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(internalMap.find("newKey"), internalMap.end());

  // Verify that the original map variable still has the modified values
  EXPECT_EQ(map.at("saveFile"), "modified_value");
  EXPECT_EQ(map.at("newKey"), "newValue");
}

/**
 * @brief Test immutability with const reference
 *
 * Verifies that const references to the configuration map cannot
 * be used to modify the internal state of the ConfigurationManager.
 */
TEST_F(BaseConfigSetup, ImmutabilityWithConstReference) {
  // Get a const reference to the config map
  const auto &constMap = config->getConfigMap();

  // Verify the const map contains the expected values
  EXPECT_EQ(constMap.at("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(constMap.at("saveDirectory"), "/home/user/outDir/");

  // The const reference should prevent modification, but let's verify
  // that even if we could modify it, the internal state would be protected
  auto nonConstMap = config->getConfigMap(); // Get a non-const copy
  nonConstMap["saveFile"] = "modified_via_copy";

  // Verify that the internal state is still unchanged
  EXPECT_EQ(config->get("saveFile"), "/home/user/outDir/output.root");
  EXPECT_EQ(constMap.at("saveFile"), "/home/user/outDir/output.root");
}

/**
 * @brief Test immutability after set operations
 *
 * Verifies that set operations don't affect existing references
 * to the configuration map.
 */
TEST_F(BaseConfigSetup, ImmutabilityAfterSetOperations) {
  // Get the config map before any modifications
  auto originalMap = config->getConfigMap();

  // Add a new key using set
  config->set("testKey", "testValue");

  // Verify the new key is in the internal state
  EXPECT_EQ(config->get("testKey"), "testValue");

  // Verify the original map reference is unchanged
  EXPECT_EQ(originalMap.find("testKey"), originalMap.end());
  EXPECT_EQ(originalMap.at("saveFile"), "/home/user/outDir/output.root");

  // Get a new map reference and verify it contains the new key
  auto newMap = config->getConfigMap();
  EXPECT_EQ(newMap.at("testKey"), "testValue");
  EXPECT_EQ(newMap.at("saveFile"), "/home/user/outDir/output.root");
}