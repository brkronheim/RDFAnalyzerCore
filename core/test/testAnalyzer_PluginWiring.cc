#include <gtest/gtest.h>
#include <analyzer.h>
#include <NDHistogramManager.h>
#include <test_util.h>

TEST(AnalyzerPluginWiring, AddPluginAfterConfigCtor) {
    ChangeToTestSourceDir();

    // Use the lightweight test config for speed and determinism
    std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.txt";
    Analyzer analyzer(cfgPath);

    // Construct a histogram manager using the analyzer's config provider
    auto histManager = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());

    // Register the plugin after Analyzer construction
    analyzer.addPlugin("histogramManager", std::move(histManager));

    // The plugin should now be retrievable and the correct type
    auto *retrieved = analyzer.getPlugin<NDHistogramManager>("histogramManager");
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->type(), std::string("NDHistogramManager"));
}
