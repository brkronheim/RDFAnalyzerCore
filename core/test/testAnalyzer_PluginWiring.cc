#include <gtest/gtest.h>
#include <analyzer.h>
#include <NDHistogramManager.h>

TEST(AnalyzerPluginWiring, AddPluginAfterConfigCtor) {
    // Use the example analysis config file (bundled in the repo) to exercise the config-file-based ctor
    std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/../../analyses/ExampleAnalysis/cfg.txt";
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
