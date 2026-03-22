#include <gtest/gtest.h>
#include <analyzer.h>
#include <NDHistogramManager.h>
#include <test_util.h>

TEST(AnalyzerPluginWiring, AddPluginAfterConfigCtor) {
    ChangeToTestSourceDir();

    // Use the lightweight test config for speed and determinism
    std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.txt";
    Analyzer analyzer(cfgPath);

    // Use the helper to create, register, and get the plugin as shared_ptr
    auto histManager = makeNDHistogramManager(analyzer);

    // The plugin should now be retrievable and the correct type
    auto retrieved = analyzer.getPlugin<NDHistogramManager>("histogramManager");
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->type(), std::string("NDHistogramManager"));
}
