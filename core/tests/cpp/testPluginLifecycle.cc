#include <gtest/gtest.h>

#include <api/IPluggableManager.h>
#include <api/ManagerContext.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <RootOutputSink.h>
#include <WeightManager.h>
#include <RegionManager.h>
#include <TFile.h>
#include <TObjString.h>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <fstream>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// Helpers: minimal mock plugin that records lifecycle calls
// ---------------------------------------------------------------------------

struct CallLog {
    std::vector<std::string> calls;
    void record(const std::string& name) { calls.push_back(name); }
};

/**
 * A minimal plugin that records every lifecycle method call into a shared
 * CallLog and supports configurable dependency/column advertisement.
 */
class MockPlugin : public IPluggableManager {
public:
    explicit MockPlugin(const std::string& typeName,
                        CallLog& log,
                        std::vector<std::string> deps = {},
                        std::vector<std::string> requiredCols = {},
                        std::vector<std::string> producedCols = {})
        : typeName_(typeName),
          log_(log),
          deps_(std::move(deps)),
          requiredCols_(std::move(requiredCols)),
          producedCols_(std::move(producedCols)) {}

    // IContextAware
    void setContext(ManagerContext&) override { log_.record(typeName_ + "::setContext"); }

    // IPluggableManager – identification
    std::string type() const override { return typeName_; }

    // IPluggableManager – configuration
    void setupFromConfigFile() override { log_.record(typeName_ + "::setupFromConfigFile"); }

    // Dependency / resource advertisement
    std::vector<std::string> getDependencies() const override { return deps_; }
    std::vector<std::string> getRequiredColumns() const override { return requiredCols_; }
    std::vector<std::string> getProducedColumns() const override { return producedCols_; }

    // Lifecycle hooks
    void initialize() override { log_.record(typeName_ + "::initialize"); }
    void execute()    override { log_.record(typeName_ + "::execute"); }
    void finalize()   override { log_.record(typeName_ + "::finalize"); }
    void reportMetadata() override { log_.record(typeName_ + "::reportMetadata"); }

private:
    std::string typeName_;
    CallLog& log_;
    std::vector<std::string> deps_;
    std::vector<std::string> requiredCols_;
    std::vector<std::string> producedCols_;
};

// ---------------------------------------------------------------------------
// Helper: build a minimal Analyzer using the small test_data_config
// ---------------------------------------------------------------------------
static std::string exampleCfg() {
    // The test configuration lives under cfg in the native test source directory.
    return std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config.yaml";
}

static std::unique_ptr<Analyzer> makeTestAnalyzer(
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins = {}) {
    return std::make_unique<Analyzer>(
        std::make_unique<ConfigurationManager>(exampleCfg()),
        std::make_unique<DataManager>(1),
        std::move(plugins),
        std::make_unique<SystematicManager>(),
        std::make_unique<DefaultLogger>(),
        std::make_unique<NullOutputSink>(),
        std::make_unique<NullOutputSink>());
}

// ---------------------------------------------------------------------------
// 1. Default method implementations
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, DefaultMethodsReturnEmptyVectors) {
    // A concrete subclass that only implements the two pure-virtual methods
    struct MinimalPlugin : public IPluggableManager {
        void setContext(ManagerContext&) override {}
        std::string type() const override { return "Minimal"; }
        void setupFromConfigFile() override {}
    };

    MinimalPlugin p;
    EXPECT_TRUE(p.getDependencies().empty());
    EXPECT_TRUE(p.getRequiredColumns().empty());
    EXPECT_TRUE(p.getProducedColumns().empty());
    // Lifecycle hooks should be callable without error
    EXPECT_NO_THROW(p.initialize());
    EXPECT_NO_THROW(p.execute());
    EXPECT_NO_THROW(p.finalize());
    EXPECT_NO_THROW(p.reportMetadata());
}

// ---------------------------------------------------------------------------
// 2. Resource and output advertisement round-trip
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, AdvertisedColumnsAreReturned) {
    CallLog log;
    MockPlugin p("A", log, {}, {"col_x", "col_y"}, {"out_z"});
    EXPECT_EQ(p.getRequiredColumns(), (std::vector<std::string>{"col_x", "col_y"}));
    EXPECT_EQ(p.getProducedColumns(), (std::vector<std::string>{"out_z"}));
}

// ---------------------------------------------------------------------------
// 3. Lifecycle hooks are called by Analyzer in the correct order
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, HooksCalledInOrder_SetupThenInitialize) {
    CallLog log;
    auto analyzer = makeTestAnalyzer();

    auto plugin = std::make_unique<MockPlugin>("P", log);
    analyzer->addPlugin("p", std::move(plugin));

    // After addPlugin the setup + initialize hooks must have been called
    const auto& c = log.calls;
    auto it_setup = std::find(c.begin(), c.end(), "P::setupFromConfigFile");
    auto it_init  = std::find(c.begin(), c.end(), "P::initialize");

    ASSERT_NE(it_setup, c.end()) << "setupFromConfigFile not called";
    ASSERT_NE(it_init,  c.end()) << "initialize not called";
    EXPECT_LT(it_setup, it_init) << "setupFromConfigFile must precede initialize";
}

// ---------------------------------------------------------------------------
// 4. Dependency ordering: dependency plugin must be initialized before
//    the plugin that declares it
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, DependencyInitializedBeforeDependent) {
    CallLog log;

    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    plugins["base"] = std::make_shared<MockPlugin>("Base", log);
    plugins["derived"] = std::make_shared<MockPlugin>("Derived", log,
                                                       std::vector<std::string>{"base"});

    // Should not throw
    ASSERT_NO_THROW(makeTestAnalyzer(std::move(plugins)));

    const auto& c = log.calls;
    auto it_base    = std::find(c.begin(), c.end(), "Base::initialize");
    auto it_derived = std::find(c.begin(), c.end(), "Derived::initialize");

    ASSERT_NE(it_base,    c.end()) << "Base::initialize not called";
    ASSERT_NE(it_derived, c.end()) << "Derived::initialize not called";
    EXPECT_LT(it_base, it_derived)
        << "Base must be initialized before Derived";
}

TEST(PluginLifecycle, DependencyExecutedBeforeDependentDuringRun) {
    CallLog log;

    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    plugins["base"] = std::make_shared<MockPlugin>("Base", log);
    plugins["derived"] = std::make_shared<MockPlugin>("Derived", log,
                                                        std::vector<std::string>{"base"});

    auto analyzer = makeTestAnalyzer(std::move(plugins));
    log.calls.clear();

    ASSERT_NO_THROW(analyzer->run());

    const auto& c = log.calls;
    auto it_base = std::find(c.begin(), c.end(), "Base::execute");
    auto it_derived = std::find(c.begin(), c.end(), "Derived::execute");

    ASSERT_NE(it_base, c.end()) << "Base::execute not called";
    ASSERT_NE(it_derived, c.end()) << "Derived::execute not called";
    EXPECT_LT(it_base, it_derived)
        << "Base must execute before Derived";
}

TEST(PluginLifecycle, AddPluginsResolvesDependenciesInDeterministicOrder) {
    CallLog log;
    auto analyzer = makeTestAnalyzer();

    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    plugins.emplace("z_derived", std::make_shared<MockPlugin>(
        "Derived", log, std::vector<std::string>{"a_base"}));
    plugins.emplace("m_independent", std::make_shared<MockPlugin>(
        "Independent", log));
    plugins.emplace("a_base", std::make_shared<MockPlugin>("Base", log));

    ASSERT_NO_THROW(analyzer->addPlugins(std::move(plugins)));

    std::vector<std::string> initialized;
    for (const auto& call : log.calls) {
        if (call.find("::initialize") != std::string::npos) {
            initialized.push_back(call);
        }
    }
    EXPECT_EQ(initialized, (std::vector<std::string>{
        "Base::initialize",
        "Independent::initialize",
        "Derived::initialize"}));
}

TEST(PluginLifecycle, DuplicateRoleIsRejectedWithoutReinitializingExistingPlugin) {
    CallLog log;
    auto analyzer = makeTestAnalyzer();

    analyzer->addPlugin("shared", std::make_unique<MockPlugin>("First", log));
    auto duplicate = std::make_unique<MockPlugin>("Second", log);

    EXPECT_THROW(analyzer->addPlugin("shared", std::move(duplicate)),
                 std::runtime_error);
    EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "First::initialize"), 1);
    EXPECT_EQ(std::count(log.calls.begin(), log.calls.end(), "Second::initialize"), 0);
}

TEST(PluginLifecycle, InvalidBulkDependenciesFailBeforeInitializingBatch) {
    CallLog log;
    auto analyzer = makeTestAnalyzer();
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    plugins.emplace("orphan", std::make_shared<MockPlugin>(
        "Orphan", log, std::vector<std::string>{"missing"}));

    EXPECT_THROW(analyzer->addPlugins(std::move(plugins)), std::runtime_error);
    EXPECT_TRUE(log.calls.empty());
    EXPECT_EQ(analyzer->findPlugin<MockPlugin>("orphan"), nullptr);
}

// ---------------------------------------------------------------------------
// 5. Missing dependency throws at construction / addPlugin time
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, MissingDependencyThrows_AtConstruction) {
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    CallLog log;
    plugins["orphan"] = std::make_shared<MockPlugin>("Orphan", log,
                                                      std::vector<std::string>{"missing"});

    EXPECT_THROW(makeTestAnalyzer(std::move(plugins)), std::runtime_error);
}

TEST(PluginLifecycle, MissingDependencyThrows_AtAddPlugin) {
    CallLog log;
    auto analyzer = makeTestAnalyzer();

    auto plugin = std::make_unique<MockPlugin>("Orphan", log,
                                               std::vector<std::string>{"missing"});
    EXPECT_THROW(analyzer->addPlugin("orphan", std::move(plugin)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// 6. Circular dependency throws at construction
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, CircularDependencyThrows) {
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    CallLog log;
    plugins["a"] = std::make_shared<MockPlugin>("A", log, std::vector<std::string>{"b"});
    plugins["b"] = std::make_shared<MockPlugin>("B", log, std::vector<std::string>{"a"});

    EXPECT_THROW(makeTestAnalyzer(std::move(plugins)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// 7. getDependencies round-trip
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, GetDependenciesRoundTrip) {
    CallLog log;
    MockPlugin p("P", log, {"dep1", "dep2"});
    EXPECT_EQ(p.getDependencies(), (std::vector<std::string>{"dep1", "dep2"}));
}

// ---------------------------------------------------------------------------
// 8. collectProvenanceEntries() – default returns empty map
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, DefaultCollectProvenanceEntriesIsEmpty) {
    struct MinimalPlugin : public IPluggableManager {
        void setContext(ManagerContext&) override {}
        std::string type() const override { return "Minimal"; }
        void setupFromConfigFile() override {}
    };

    MinimalPlugin p;
    EXPECT_TRUE(p.collectProvenanceEntries().empty());
}

// ---------------------------------------------------------------------------
// 9. collectProvenanceEntries() – override returns custom entries
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, OverriddenCollectProvenanceEntriesReturnsEntries) {
    class ProvenancePlugin : public IPluggableManager {
    public:
        void setContext(ManagerContext&) override {}
        std::string type() const override { return "Provenance"; }
        void setupFromConfigFile() override {}
        std::unordered_map<std::string, std::string>
        collectProvenanceEntries() const override {
            return {{"mykey", "myvalue"}, {"setting_x", "42"}};
        }
    };

    ProvenancePlugin p;
    const auto entries = p.collectProvenanceEntries();
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries.at("mykey"),     "myvalue");
    EXPECT_EQ(entries.at("setting_x"), "42");
}

// ---------------------------------------------------------------------------
// 10. setTaskMetadata() stores entries in the Analyzer's provenance map and
//     they are written as "task.<key>" to the meta ROOT file.
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, SetTaskMetadataStoresEntries) {
    // Just verify chaining and no crash (no ROOT output in this unit test)
    auto analyzer = makeTestAnalyzer();
    Analyzer* ret = analyzer->setTaskMetadata("job_id", "42");
    EXPECT_EQ(ret, analyzer.get()) << "setTaskMetadata() must return this for chaining";

    // Double-call should overwrite
    analyzer->setTaskMetadata("job_id", "99");
    // No assertion on the stored value here; integration tests cover writing
}

TEST(PluginLifecycle, AnalyzerWritesOneDocumentAfterPluginFinalizers) {
    char pattern[] = "/tmp/rdf_metadata_analyzer_XXXXXX";
    const auto* directory = mkdtemp(pattern);
    ASSERT_NE(directory, nullptr);
    const auto base = std::filesystem::path(directory);
    const auto cfg = base / "config.txt";
    const auto meta = base / "meta.root";
    {
        std::ofstream out(cfg);
        out << "sample=synthetic\nmetaFile=" << meta.string()
            << "\nenableCounters=true\n";
    }
    auto data = std::make_unique<DataManager>(5);
    auto systematics = std::make_unique<SystematicManager>();
    data->Define("sf", [](ULong64_t i) { return double(i)-2.; },
                 {"rdfentry_"}, *systematics);
    data->Define("pass", [](ULong64_t) { return true; },
                 {"rdfentry_"}, *systematics);
    auto weights = std::make_shared<WeightManager>();
    auto regions = std::make_shared<RegionManager>();
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins{
        {"weights", weights}, {"regions", regions}};
    Analyzer analyzer(std::make_unique<ConfigurationManager>(cfg.string()),
                      std::move(data), std::move(plugins), std::move(systematics),
                      std::make_unique<DefaultLogger>(),
                      std::make_unique<NullOutputSink>(), std::make_unique<RootOutputSink>());
    weights->addScaleFactor("sf", "sf");
    weights->defineNominalWeight("weight");
    regions->declareRegion("signal", "pass");
    analyzer.run();
    TFile file(meta.c_str(), "READ");
    auto* object = dynamic_cast<TObjString*>(file.Get(rdf::jobMetadataKey));
    ASSERT_NE(object, nullptr);
    const auto document = YAML::Load(object->GetString().Data());
    EXPECT_EQ(document["weight_audits"]["weight_audit_weight"]["n"].as<int>(), 5);
    EXPECT_EQ(document["weight_audits"]["weight_audit_weight"]["n_negative"].as<int>(), 2);
    EXPECT_EQ(document["regions"]["signal"].as<std::string>(), "pass");
    EXPECT_EQ(file.Get("weight_audit_weight"), nullptr);
    EXPECT_EQ(file.Get("regions"), nullptr);
    file.Close();
    std::filesystem::remove_all(base);
}
