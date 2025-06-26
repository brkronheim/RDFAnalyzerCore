#include <gtest/gtest.h>
#include "ConfigurationManager.h"
#include <fstream>

// Helper: Create a temporary config file for testing
static std::string createTempConfigFile() {
    std::string filename = "temp_test_config.cfg";
    std::ofstream file(filename);
    file << "key1=value1\n";
    file << "key2=value2\n";
    file.close();
    return filename;
}

TEST(ConfigurationManagerTest, GetReturnsCorrectValue) {
    std::string configFile = createTempConfigFile();
    ConfigurationManager config(configFile);
    EXPECT_EQ(config.get("key1"), "value1");
    EXPECT_EQ(config.get("key2"), "value2");
    EXPECT_EQ(config.get("nonexistent"), "");
    std::remove(configFile.c_str());
}

TEST(ConfigurationManagerTest, SetAndGetWorks) {
    ConfigurationManager config("nonexistent.cfg");
    config.set("foo", "bar");
    EXPECT_EQ(config.get("foo"), "bar");
}