#include <gtest/gtest.h>

#include <api/IPluggableManager.h>
#include <api/ManagerContext.h>
#include <analyzer.h>

#include <memory>
#include <stdexcept>
#include <string>
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
// Helper: build a minimal Analyzer using the ExampleAnalysis config
// ---------------------------------------------------------------------------
static std::string exampleCfg() {
    return std::string(TEST_SOURCE_DIR) + "/../../analyses/ExampleAnalysis/cfg.txt";
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
    Analyzer analyzer(exampleCfg());

    auto plugin = std::make_unique<MockPlugin>("P", log);
    analyzer.addPlugin("p", std::move(plugin));

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

    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
    plugins["base"] = std::make_unique<MockPlugin>("Base", log);
    plugins["derived"] = std::make_unique<MockPlugin>("Derived", log,
                                                       std::vector<std::string>{"base"});

    // Should not throw
    ASSERT_NO_THROW(Analyzer(exampleCfg(), std::move(plugins)));

    const auto& c = log.calls;
    auto it_base    = std::find(c.begin(), c.end(), "Base::initialize");
    auto it_derived = std::find(c.begin(), c.end(), "Derived::initialize");

    ASSERT_NE(it_base,    c.end()) << "Base::initialize not called";
    ASSERT_NE(it_derived, c.end()) << "Derived::initialize not called";
    EXPECT_LT(it_base, it_derived)
        << "Base must be initialized before Derived";
}

// ---------------------------------------------------------------------------
// 5. Missing dependency throws at construction / addPlugin time
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, MissingDependencyThrows_AtConstruction) {
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
    CallLog log;
    plugins["orphan"] = std::make_unique<MockPlugin>("Orphan", log,
                                                      std::vector<std::string>{"missing"});

    EXPECT_THROW(Analyzer(exampleCfg(), std::move(plugins)), std::runtime_error);
}

TEST(PluginLifecycle, MissingDependencyThrows_AtAddPlugin) {
    CallLog log;
    Analyzer analyzer(exampleCfg());

    auto plugin = std::make_unique<MockPlugin>("Orphan", log,
                                               std::vector<std::string>{"missing"});
    EXPECT_THROW(analyzer.addPlugin("orphan", std::move(plugin)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// 6. Circular dependency throws at construction
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, CircularDependencyThrows) {
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
    CallLog log;
    plugins["a"] = std::make_unique<MockPlugin>("A", log, std::vector<std::string>{"b"});
    plugins["b"] = std::make_unique<MockPlugin>("B", log, std::vector<std::string>{"a"});

    EXPECT_THROW(Analyzer(exampleCfg(), std::move(plugins)), std::runtime_error);
}

// ---------------------------------------------------------------------------
// 7. getDependencies round-trip
// ---------------------------------------------------------------------------
TEST(PluginLifecycle, GetDependenciesRoundTrip) {
    CallLog log;
    MockPlugin p("P", log, {"dep1", "dep2"});
    EXPECT_EQ(p.getDependencies(), (std::vector<std::string>{"dep1", "dep2"}));
}
