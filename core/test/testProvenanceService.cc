/**
 * @file testProvenanceService.cc
 * @brief Unit tests for ProvenanceService – verifies that provenance metadata
 *        is written correctly to the meta ROOT output file.
 */

#include <gtest/gtest.h>

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <ProvenanceService.h>
#include <RootOutputSink.h>
#include <SystematicManager.h>

#include <TDirectory.h>
#include <TFile.h>
#include <TNamed.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {

std::string writeMinimalConfig(const std::string& cfgPath,
                                const std::string& metaPath) {
    std::ofstream out(cfgPath);
    out << "sample=ProvenanceTest\n";
    out << "metaFile=" << metaPath << "\n";
    out << "fileList=\n";
    out.close();
    return cfgPath;
}

} // namespace

class ProvenanceServiceTest : public ::testing::Test {
protected:
    std::string cfgPath;
    std::string metaPath;

    void SetUp() override {
        const std::string base =
            std::string(TEST_SOURCE_DIR) + "/aux/provenance_test_";
        cfgPath  = base + "config.txt";
        metaPath = base + "meta.root";
        cleanup();
    }
    void TearDown() override { cleanup(); }
    void cleanup() {
        std::remove(cfgPath.c_str());
        std::remove(metaPath.c_str());
    }
};

// ---------------------------------------------------------------------------
// Test: required provenance keys are present and non-empty in the output file
// ---------------------------------------------------------------------------
TEST_F(ProvenanceServiceTest, RequiredKeysWrittenToMetaFile) {
    writeMinimalConfig(cfgPath, metaPath);

    ConfigurationManager config(cfgPath);
    DataManager dataManager(3);
    SystematicManager systematicManager;
    DefaultLogger logger;
    NullOutputSink skimSink;
    RootOutputSink metaSink;

    ManagerContext ctx{config, dataManager, systematicManager, logger,
                       skimSink, metaSink};

    ProvenanceService svc;
    svc.initialize(ctx);

    auto df = dataManager.getDataFrame();
    svc.finalize(df);

    // Verify the ROOT file was created and has the expected entries
    TFile f(metaPath.c_str(), "READ");
    ASSERT_FALSE(f.IsZombie()) << "Meta output file was not created";

    TDirectory* provDir = dynamic_cast<TDirectory*>(f.Get("provenance"));
    ASSERT_NE(provDir, nullptr)
        << "'provenance' directory missing from meta file";

    // Check mandatory keys are present and non-empty
    const std::vector<std::string> requiredKeys = {
        "framework.git_hash",
        "framework.build_timestamp",
        "framework.compiler",
        "root.version",
        "config.hash",
        "executor.num_threads",
    };

    for (const auto& key : requiredKeys) {
        auto* obj = dynamic_cast<TNamed*>(provDir->Get(key.c_str()));
        ASSERT_NE(obj, nullptr) << "Missing provenance key: " << key;
        EXPECT_FALSE(std::string(obj->GetTitle()).empty())
            << "Provenance key has empty value: " << key;
    }

    f.Close();
}

// ---------------------------------------------------------------------------
// Test: addEntry() injects custom key-value pairs
// ---------------------------------------------------------------------------
TEST_F(ProvenanceServiceTest, AddEntryStoresCustomValues) {
    writeMinimalConfig(cfgPath, metaPath);

    ConfigurationManager config(cfgPath);
    DataManager dataManager(3);
    SystematicManager systematicManager;
    DefaultLogger logger;
    NullOutputSink skimSink;
    RootOutputSink metaSink;

    ManagerContext ctx{config, dataManager, systematicManager, logger,
                       skimSink, metaSink};

    ProvenanceService svc;
    svc.initialize(ctx);
    svc.addEntry("plugin.myPlugin", "MyPluginType");
    svc.addEntry("custom.key",      "hello_world");

    auto df = dataManager.getDataFrame();
    svc.finalize(df);

    TFile f(metaPath.c_str(), "READ");
    ASSERT_FALSE(f.IsZombie());

    auto* provDir = dynamic_cast<TDirectory*>(f.Get("provenance"));
    ASSERT_NE(provDir, nullptr);

    auto* pluginEntry = dynamic_cast<TNamed*>(provDir->Get("plugin.myPlugin"));
    ASSERT_NE(pluginEntry, nullptr);
    EXPECT_EQ(std::string(pluginEntry->GetTitle()), "MyPluginType");

    auto* customEntry = dynamic_cast<TNamed*>(provDir->Get("custom.key"));
    ASSERT_NE(customEntry, nullptr);
    EXPECT_EQ(std::string(customEntry->GetTitle()), "hello_world");

    f.Close();
}

// ---------------------------------------------------------------------------
// Test: getProvenance() returns all collected entries in-memory
// ---------------------------------------------------------------------------
TEST_F(ProvenanceServiceTest, GetProvenanceReturnsAllEntries) {
    writeMinimalConfig(cfgPath, metaPath);

    ConfigurationManager config(cfgPath);
    DataManager dataManager(3);
    SystematicManager systematicManager;
    DefaultLogger logger;
    NullOutputSink skimSink;
    RootOutputSink metaSink;

    ManagerContext ctx{config, dataManager, systematicManager, logger,
                       skimSink, metaSink};

    ProvenanceService svc;
    svc.initialize(ctx);
    svc.addEntry("test.key", "test_value");

    const auto& prov = svc.getProvenance();

    EXPECT_NE(prov.find("framework.git_hash"), prov.end());
    EXPECT_NE(prov.find("root.version"),        prov.end());
    EXPECT_NE(prov.find("config.hash"),         prov.end());

    auto it = prov.find("test.key");
    ASSERT_NE(it, prov.end());
    EXPECT_EQ(it->second, "test_value");

    // Still need to finalize so the test output file is created
    auto df = dataManager.getDataFrame();
    svc.finalize(df);
}

// ---------------------------------------------------------------------------
// Test: finalize() is graceful when no meta output file is configured
// ---------------------------------------------------------------------------
TEST_F(ProvenanceServiceTest, GracefulWhenNoMetaOutputConfigured) {
    // Write a config with neither metaFile nor saveFile, so resolveOutputFile
    // returns "" and ProvenanceService skips writing gracefully.
    {
        std::ofstream out(cfgPath);
        out << "sample=NoOutput\n";
        out << "fileList=\n";
    }

    ConfigurationManager config(cfgPath);
    DataManager dataManager(3);
    SystematicManager systematicManager;
    DefaultLogger logger;
    NullOutputSink skimSink;
    NullOutputSink nullMetaSink;

    ManagerContext ctx{config, dataManager, systematicManager, logger,
                       skimSink, nullMetaSink};

    ProvenanceService svc;
    svc.initialize(ctx);

    auto df = dataManager.getDataFrame();
    ASSERT_NO_THROW(svc.finalize(df))
        << "finalize() must not throw when no meta output is configured";
}
