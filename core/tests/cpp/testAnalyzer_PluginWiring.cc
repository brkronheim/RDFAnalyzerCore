#include <gtest/gtest.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <api/IPluggableManager.h>
#include <api/ManagerContext.h>
#include <api/IDataFrameProvider.h>
#include <NDHistogramManager.h>
#include <test_util.h>
#include <ROOT/RDataFrame.hxx>

#include <algorithm>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <cstdio>

static_assert(!std::is_move_constructible<Analyzer>::value,
              "Analyzer owns address-bound manager contexts and must not move");
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

class ProbeDataFrameProvider : public IDataFrameProvider {
public:
    explicit ProbeDataFrameProvider(std::size_t entries)
        : source_m(entries), node_m(source_m) {}

    ROOT::RDF::RNode getDataFrame() override { return node_m; }
    void setDataFrame(const ROOT::RDF::RNode& node) override { node_m = node; }
    void DefineVector(std::string, const std::vector<std::string>&, std::string,
                      ISystematicManager&) override {}
    void DefineVectorNominal(std::string, const std::vector<std::string>&,
                             std::string, ISystematicManager&) override {}
    unsigned int runCount() const { return source_m.GetNRuns(); }

private:
    ROOT::RDataFrame source_m;
    ROOT::RDF::RNode node_m;
};

class ThrowingExecutePlugin : public IPluggableManager {
public:
    void setContext(ManagerContext&) override {}
    std::string type() const override { return "ThrowingExecutePlugin"; }
    void setupFromConfigFile() override {}
    void execute() override { throw std::runtime_error("injected execute failure"); }
    void finalize() override { ++finalizeCalls; }
    void reportMetadata() override { ++metadataCalls; }

    int finalizeCalls = 0;
    int metadataCalls = 0;
};

class ThrowingSetupPlugin : public IPluggableManager {
public:
    void setContext(ManagerContext&) override {}
    std::string type() const override { return "ThrowingSetupPlugin"; }
    void setupFromConfigFile() override {
        throw std::runtime_error("injected setup failure");
    }
};

class NoOpPlugin : public IPluggableManager {
public:
    void setContext(ManagerContext&) override {}
    std::string type() const override { return "NoOpPlugin"; }
    void setupFromConfigFile() override {}
};

std::unique_ptr<Analyzer> makeProbeAnalyzer(
    std::unique_ptr<IDataFrameProvider> data,
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins = {}) {
    auto config = std::make_unique<ConfigurationManager>(
        std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.txt");
    return std::make_unique<Analyzer>(
        std::move(config), std::move(data), std::move(plugins),
        std::make_unique<SystematicManager>(), std::make_unique<DefaultLogger>(),
        std::make_unique<NullOutputSink>(), std::make_unique<NullOutputSink>());
}

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

TEST(AnalyzerPluginWiring, DependencyInjectedConstructorsRejectNullCoreDependencies) {
    auto data = [] { return std::make_unique<DataManager>(1); };
    auto systematics = [] { return std::make_unique<SystematicManager>(); };
    auto logger = [] { return std::make_unique<DefaultLogger>(); };
    auto sink = [] { return std::make_unique<NullOutputSink>(); };

    EXPECT_THROW(
        (Analyzer(std::unique_ptr<IConfigurationProvider>{}, data(),
                  std::unordered_map<std::string,
                                     std::shared_ptr<IPluggableManager>>{},
                  systematics(), logger(), sink(), sink())),
        std::invalid_argument);

    EXPECT_THROW(
        (Analyzer(std::unique_ptr<IConfigurationProvider>{}, data(),
                  std::unordered_map<std::string,
                                     std::unique_ptr<IPluggableManager>>{},
                  systematics(), logger(), sink(), sink())),
        std::invalid_argument);
}

TEST(AnalyzerLifecycle, NoActionRunConsumesAnalyzerWithoutEventLoop) {
    ChangeToTestSourceDir();

    int predicateCalls = 0;
    auto data = std::make_unique<ProbeDataFrameProvider>(10);
    auto* observation = data.get();
    auto analyzer = makeProbeAnalyzer(std::move(data));
    analyzer->Filter("audit_lazy_filter", [&predicateCalls]() {
        ++predicateCalls;
        return true;
    });

    ASSERT_NO_THROW(analyzer->run());
    EXPECT_EQ(observation->runCount(), 0u);
    EXPECT_EQ(predicateCalls, 0);
    EXPECT_THROW(analyzer->run(), std::runtime_error);
}

TEST(AnalyzerLifecycle, ExecuteFailureIsTerminalAndSkipsPostExecutionHooks) {
    ChangeToTestSourceDir();

    auto plugin = std::make_shared<ThrowingExecutePlugin>();
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    plugins.emplace("thrower", plugin);
    auto data = std::make_unique<ProbeDataFrameProvider>(10);
    auto* observation = data.get();
    auto analyzer = makeProbeAnalyzer(std::move(data), std::move(plugins));

    try {
        analyzer->run();
        FAIL() << "expected execute-hook failure";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "injected execute failure");
    }
    EXPECT_THROW(analyzer->run(), std::runtime_error);
    EXPECT_EQ(plugin->finalizeCalls, 0);
    EXPECT_EQ(plugin->metadataCalls, 0);
    EXPECT_EQ(observation->runCount(), 0u);
}

TEST(AnalyzerPluginWiring, FailedSingleRegistrationCanBeRetriedWithSameRole) {
    ChangeToTestSourceDir();
    auto analyzer = makeProbeAnalyzer(
        std::make_unique<ProbeDataFrameProvider>(1));

    EXPECT_THROW(analyzer->addPlugin("candidate",
                                     std::make_unique<ThrowingSetupPlugin>()),
                 std::runtime_error);
    EXPECT_NO_THROW(analyzer->addPlugin("candidate",
                                       std::make_unique<NoOpPlugin>()));
}

TEST(AnalyzerPluginWiring, FailedBatchDoesNotLeaveEarlierBatchRolesRegistered) {
    ChangeToTestSourceDir();
    auto analyzer = makeProbeAnalyzer(
        std::make_unique<ProbeDataFrameProvider>(1));

    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> batch;
    batch.emplace("first", std::make_unique<NoOpPlugin>());
    batch.emplace("second", std::make_unique<ThrowingSetupPlugin>());
    EXPECT_THROW(analyzer->addPlugins(std::move(batch)), std::runtime_error);

    // Both names can be registered again if addPlugins leaves no partial map
    // or dependency order behind after a lifecycle hook throws.
    EXPECT_NO_THROW(analyzer->addPlugin("first", std::make_unique<NoOpPlugin>()));
    EXPECT_NO_THROW(analyzer->addPlugin("second", std::make_unique<NoOpPlugin>()));
}
