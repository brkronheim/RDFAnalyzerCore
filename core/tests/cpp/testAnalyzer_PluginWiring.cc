#include <gtest/gtest.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <api/IPluggableManager.h>
#include <api/ManagerContext.h>
#include <NDHistogramManager.h>
#include <test_util.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

class RecordingOutputSink : public IOutputSink {
public:
    void writeDataFrame(ROOT::RDF::RNode&, const OutputSpec&) override {}

    void writeDataFrame(ROOT::RDF::RNode& df,
                        const IConfigurationProvider&,
                        const ISystematicManager*,
                        OutputChannel channel) override {
        if (channel == OutputChannel::Skim) {
            skimColumns = df.GetColumnNames();
            skimRows = *df.Count();
        }
    }

    std::string resolveOutputFile(const IConfigurationProvider&,
                                  OutputChannel) override {
        return {};
    }

    std::vector<std::string> skimColumns;
    std::size_t skimRows = 0;
};

class ExecuteColumnPlugin : public IPluggableManager {
public:
    void setContext(ManagerContext& context) override { context_m = &context; }
    std::string type() const override { return "ExecuteColumnPlugin"; }
    void setupFromConfigFile() override {}

    void execute() override {
        ASSERT_NE(context_m, nullptr);
        context_m->data.Define("plugin_output", []() { return 42; }, {},
                               context_m->systematics);
    }

private:
    ManagerContext* context_m = nullptr;
};

} // namespace

TEST(AnalyzerPluginWiring, AddPluginAfterConfigCtor) {
    ChangeToTestSourceDir();

    // Use the lightweight test config for speed and determinism
    std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.txt";
    Analyzer analyzer(cfgPath);

    // Use the helper to create, register, and get the plugin as shared_ptr
    auto histManager = NDHistogramManager::create(analyzer);

    // The plugin should now be retrievable and the correct type
    auto retrieved = analyzer.getPlugin<NDHistogramManager>("histogramManager");
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->type(), std::string("NDHistogramManager"));
}

TEST(AnalyzerPluginWiring, SkimUsesDataFrameUpdatedByPluginExecute) {
    ChangeToTestSourceDir();

    auto config = std::make_unique<ConfigurationManager>(
        std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.txt");
    config->set("enableSkim", "true");
    config->set("skimFilter", "plugin_output == 0");

    auto skimSink = std::make_unique<RecordingOutputSink>();
    auto* recordingSink = skimSink.get();

    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    Analyzer analyzer(std::move(config),
                      std::make_unique<DataManager>(1),
                      std::move(plugins),
                      std::make_unique<SystematicManager>(),
                      std::make_unique<DefaultLogger>(),
                      std::move(skimSink),
                      std::make_unique<NullOutputSink>());
    analyzer.addPlugin("columnProducer", std::make_unique<ExecuteColumnPlugin>());

    ASSERT_NO_THROW(analyzer.run());
    EXPECT_NE(std::find(recordingSink->skimColumns.begin(),
                        recordingSink->skimColumns.end(),
                        "plugin_output"),
              recordingSink->skimColumns.end());
    EXPECT_EQ(recordingSink->skimRows, 0u);
}

TEST(AnalyzerPluginWiring, ConfigCanDisableSystematicPropagation) {
    ChangeToTestSourceDir();

    auto config = std::make_unique<ConfigurationManager>(
        std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.txt");
    config->set("enableSystematics", "false");

    Analyzer analyzer(std::move(config),
                      std::make_unique<DataManager>(1),
                      std::unordered_map<std::string,
                                         std::shared_ptr<IPluggableManager>>{},
                      std::make_unique<SystematicManager>(),
                      std::make_unique<DefaultLogger>(),
                      std::make_unique<NullOutputSink>(),
                      std::make_unique<NullOutputSink>());

    auto &systematics = analyzer.getSystematicManager();
    EXPECT_FALSE(systematics.isEnabled());
    systematics.registerSystematic("jes", {"pt"});
    EXPECT_TRUE(systematics.getSystematics().empty());
    ASSERT_NO_THROW(analyzer.Filter("nominal_filter", []() { return true; }));
}
