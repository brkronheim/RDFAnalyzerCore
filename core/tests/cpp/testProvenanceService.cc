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
#include <TObjString.h>
#include <yaml-cpp/yaml.h>

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

    auto* object = dynamic_cast<TObjString*>(f.Get(rdf::jobMetadataKey));
    ASSERT_NE(object, nullptr);
    const auto document = YAML::Load(object->GetString().Data());
    EXPECT_EQ(document["schema_version"].as<int>(), 1);
    EXPECT_EQ(f.Get("provenance"), nullptr);
    const auto provenance = document["provenance"];
    // Check mandatory keys are present and non-empty
    const std::vector<std::string> requiredKeys = {
        "framework.git_hash",
        "framework.build_timestamp",
        "framework.compiler",
        "root.version",
        "root.registry_optimization",
        "root.registry_definitions",
        "root.registry_input_bindings_before",
        "root.registry_input_bindings_after",
        "config.hash",
        "executor.num_threads",
    };

    for (const auto& key : requiredKeys) {
        ASSERT_TRUE(provenance[key]) << "Missing provenance key: " << key;
        EXPECT_FALSE(provenance[key].as<std::string>().empty());
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

    auto* object = dynamic_cast<TObjString*>(f.Get(rdf::jobMetadataKey));
    ASSERT_NE(object, nullptr);
    const auto document = YAML::Load(object->GetString().Data());
    EXPECT_EQ(document["provenance"]["plugin.myPlugin"].as<std::string>(), "MyPluginType");
    EXPECT_EQ(document["provenance"]["custom.key"].as<std::string>(), "hello_world");

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

// ---------------------------------------------------------------------------
// Test: hashString() is accessible as a public static and is deterministic
// ---------------------------------------------------------------------------
TEST(ProvenanceService, HashStringIsPublicStaticAndDeterministic) {
    const std::string hash1 = ProvenanceService::hashString("hello world");
    const std::string hash2 = ProvenanceService::hashString("hello world");
    const std::string hash3 = ProvenanceService::hashString("different");

    EXPECT_FALSE(hash1.empty());
    EXPECT_EQ(hash1, hash2) << "hashString must be deterministic";
    EXPECT_NE(hash1, hash3) << "different inputs must produce different hashes";
    // MD5 hex digest is 32 characters
    EXPECT_EQ(hash1.size(), 32u);
}

// ---------------------------------------------------------------------------
// Test: recordDatasetManifestProvenance stores all three fields
// ---------------------------------------------------------------------------
TEST_F(ProvenanceServiceTest, RecordDatasetManifestProvenance) {
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
    svc.recordDatasetManifestProvenance("abc123", "{\"year\":2023}", "ttbar,wjets");

    const auto& prov = svc.getProvenance();

    auto it_hash    = prov.find("dataset_manifest.file_hash");
    auto it_params  = prov.find("dataset_manifest.query_params");
    auto it_entries = prov.find("dataset_manifest.resolved_entries");

    ASSERT_NE(it_hash,    prov.end());
    ASSERT_NE(it_params,  prov.end());
    ASSERT_NE(it_entries, prov.end());

    EXPECT_EQ(it_hash->second,    "abc123");
    EXPECT_EQ(it_params->second,  "{\"year\":2023}");
    EXPECT_EQ(it_entries->second, "ttbar,wjets");

    auto df = dataManager.getDataFrame();
    svc.finalize(df);
}

TEST_F(ProvenanceServiceTest, TypedMetadataWrittenOnceWithEscaping) {
    writeMinimalConfig(cfgPath, metaPath);
    ConfigurationManager config(cfgPath);
    DataManager dataManager(3);
    SystematicManager systematicManager;
    DefaultLogger logger;
    NullOutputSink skimSink;
    RootOutputSink metaSink;
    ManagerContext ctx{config, dataManager, systematicManager, logger, skimSink, metaSink};
    ProvenanceService svc;
    svc.initialize(ctx);
    svc.addEntry("quoted", "a\"b\\c\n");
    rdf::OutputMetadata metadata;
    metadata.regions["signal"] = "pass:preselection";
    metadata.normalizations["weight_norm_total"] = 2.5;
    metadata.weightAudits["weight_audit_w"] = {5, 1, 0, 14., -1., 5.};
    svc.addOutputMetadata(metadata);
    auto df = dataManager.getDataFrame();
    svc.finalize(df);
    svc.finalize(df);
    TFile f(metaPath.c_str(), "READ");
    auto* object = dynamic_cast<TObjString*>(f.Get(rdf::jobMetadataKey));
    ASSERT_NE(object, nullptr);
    const auto document = YAML::Load(object->GetString().Data());
    EXPECT_EQ(document["provenance"]["quoted"].as<std::string>(), "a\"b\\c\n");
    EXPECT_EQ(document["weight_audits"]["weight_audit_w"]["n"].as<int>(), 5);
    EXPECT_EQ(document["normalizations"]["weight_norm_total"].as<double>(), 2.5);
    EXPECT_EQ(f.GetListOfKeys()->GetSize(), 1);
}

TEST_F(ProvenanceServiceTest, SemanticConfigIgnoresOnlyJobRouting) {
    writeMinimalConfig(cfgPath, metaPath);
    ConfigurationManager firstConfig(cfgPath);
    DataManager data(1);
    SystematicManager systematic;
    DefaultLogger logger;
    NullOutputSink skim;
    RootOutputSink meta;
    ManagerContext firstContext{firstConfig, data, systematic, logger, skim, meta};
    ProvenanceService first;
    first.initialize(firstContext);
    writeMinimalConfig(cfgPath, metaPath + ".other");
    {
        std::ofstream append(cfgPath, std::ios::app);
        append << "metadataJobId=other-job\nmetadataAttemptId=retry\n";
    }
    ConfigurationManager secondConfig(cfgPath);
    ManagerContext secondContext{secondConfig, data, systematic, logger, skim, meta};
    ProvenanceService second;
    second.initialize(secondContext);
    EXPECT_EQ(first.getProvenance().at("config.semantic_hash"),
              second.getProvenance().at("config.semantic_hash"));
    EXPECT_NE(first.getProvenance().at("config.hash"),
              second.getProvenance().at("config.hash"));
}

TEST_F(ProvenanceServiceTest, ConfiguredMetadataWriteFailureIsFatal) {
    writeMinimalConfig(cfgPath, metaPath);
    { std::ofstream corrupt(metaPath); corrupt << "not a ROOT file"; }
    ConfigurationManager config(cfgPath);
    DataManager data(1);
    SystematicManager systematic;
    DefaultLogger logger;
    NullOutputSink skim;
    RootOutputSink meta;
    ManagerContext context{config, data, systematic, logger, skim, meta};
    ProvenanceService service;
    service.initialize(context);
    auto df = data.getDataFrame();
    EXPECT_THROW(service.finalize(df), std::runtime_error);
}
