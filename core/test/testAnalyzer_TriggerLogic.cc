#include <gtest/gtest.h>
#include <ManagerFactory.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <TriggerManager.h>
#include <test_util.h>
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <api/IPluggableManager.h>
#include <ManagerRegistry.h>

/**
 * @file testAnalyzer_TriggerLogic.cc
 * @brief Unit tests for Analyzer::ApplyAllTriggers event logic.
 */

class AnalyzerTriggerLogicTest : public ::testing::Test {
protected:
    void SetUp() override {
        ChangeToTestSourceDir();
        // Use the test_data_config.txt and triggers.txt for config
        configManager = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
        dataManager = ManagerFactory::createDataManager(*configManager);
        
        auto tmgr = ManagerRegistry::instance().create("TriggerManager", {configManager.get()});
        triggerManager = std::unique_ptr<ITriggerManager>(dynamic_cast<ITriggerManager*>(tmgr.release()));
        
        systematicManager = ManagerFactory::createSystematicManager();
        auto bmgr = ManagerRegistry::instance().create("BDTManager", {configManager.get()});
        bdtManager = std::unique_ptr<IBDTManager>(dynamic_cast<IBDTManager*>(bmgr.release()));
        auto cmgr = ManagerRegistry::instance().create("CorrectionManager", {configManager.get()});
        correctionManager = std::unique_ptr<ICorrectionManager>(dynamic_cast<ICorrectionManager*>(cmgr.release()));
        auto ndmgr = ManagerRegistry::instance().create("NDHistogramManager", {configManager.get()});
        ndHistManager = std::unique_ptr<INDHistogramManager>(dynamic_cast<INDHistogramManager*>(ndmgr.release()));
    }
    std::unique_ptr<IConfigurationProvider> configManager;
    std::unique_ptr<IDataFrameProvider> dataManager;
    std::unique_ptr<ITriggerManager> triggerManager;
    std::unique_ptr<ISystematicManager> systematicManager;
    std::unique_ptr<IBDTManager> bdtManager;
    std::unique_ptr<ICorrectionManager> correctionManager;
    std::unique_ptr<INDHistogramManager> ndHistManager;
};

// Helper to build plugin map for Analyzer
std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> makeTestPluginMap(
    std::unique_ptr<IBDTManager> bdt,
    std::unique_ptr<ICorrectionManager> corr,
    std::unique_ptr<ITriggerManager> trig,
    std::unique_ptr<INDHistogramManager> ndh) {
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
    plugins["bdt"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(bdt.release()));
    plugins["correction"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(corr.release()));
    plugins["trigger"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(trig.release()));
    plugins["ndhist"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(ndh.release()));
    return plugins;
}

TEST_F(AnalyzerTriggerLogicTest, DataTriggersAndVetoes) {
    // Set up Analyzer for a data sample
    auto config1 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data1 = ManagerFactory::createDataManager(*config1);
    auto bmgr1 = ManagerRegistry::instance().create("BDTManager", {config1.get()});
    auto bdt1 = std::unique_ptr<IBDTManager>(dynamic_cast<IBDTManager*>(bmgr1.release()));
    auto cmgr1 = ManagerRegistry::instance().create("CorrectionManager", {config1.get()});
    auto corr1 = std::unique_ptr<ICorrectionManager>(dynamic_cast<ICorrectionManager*>(cmgr1.release()));
    auto tmgr1 = ManagerRegistry::instance().create("TriggerManager", {config1.get()});
    auto trig1 = std::unique_ptr<ITriggerManager>(dynamic_cast<ITriggerManager*>(tmgr1.release()));
    auto ndmgr1 = ManagerRegistry::instance().create("NDHistogramManager", {config1.get()});
    auto ndh1 = std::unique_ptr<INDHistogramManager>(dynamic_cast<INDHistogramManager*>(ndmgr1.release()));
    auto syst1 = ManagerFactory::createSystematicManager();
    config1->set("type", "test_sample");
    Analyzer analyzer(
        std::move(config1),
        std::move(data1),
        makeTestPluginMap(std::move(bdt1), std::move(corr1), std::move(trig1), std::move(ndh1)),
        std::move(syst1));

    // Define dummy trigger columns (simulate trigger firing)
    analyzer.Define("trigger1", []() { return true; });
    analyzer.Define("trigger2", []() { return false; });
    analyzer.Define("trigger3", []() { return false; });
    analyzer.Define("veto1", []() { return false; });
    analyzer.Define("veto2", []() { return false; });

    analyzer.ApplyAllTriggers();
    auto df = analyzer.getDF();
    auto result = df.Count();
    EXPECT_EQ(result.GetValue(), 1UL);

    // Now set a veto to true, should fail
    auto config2 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data2 = ManagerFactory::createDataManager(*config2);
    auto bmgr2 = ManagerRegistry::instance().create("BDTManager", {config2.get()});
    auto bdt2 = std::unique_ptr<IBDTManager>(dynamic_cast<IBDTManager*>(bmgr2.release()));
    auto cmgr2 = ManagerRegistry::instance().create("CorrectionManager", {config2.get()});
    auto corr2 = std::unique_ptr<ICorrectionManager>(dynamic_cast<ICorrectionManager*>(cmgr2.release()));
    auto tmgr2 = ManagerRegistry::instance().create("TriggerManager", {config2.get()});
    auto trig2 = std::unique_ptr<ITriggerManager>(dynamic_cast<ITriggerManager*>(tmgr2.release()));
    auto ndmgr2 = ManagerRegistry::instance().create("NDHistogramManager", {config2.get()});
    auto ndh2 = std::unique_ptr<INDHistogramManager>(dynamic_cast<INDHistogramManager*>(ndmgr2.release()));
    auto syst2 = ManagerFactory::createSystematicManager();
    config2->set("type", "test_sample");
    Analyzer analyzer2(
        std::move(config2),
        std::move(data2),
        makeTestPluginMap(std::move(bdt2), std::move(corr2), std::move(trig2), std::move(ndh2)),
        std::move(syst2));
    analyzer2.Define("trigger1", []() { return true; });
    analyzer2.Define("trigger2", []() { return false; });
    analyzer2.Define("trigger3", []() { return false; });
    analyzer2.Define("veto1", []() { return true; });
    analyzer2.Define("veto2", []() { return false; });
    analyzer2.ApplyAllTriggers();
    auto df2 = analyzer2.getDF();
    auto result2 = df2.Count();
    EXPECT_EQ(result2.GetValue(), 0UL);
}

TEST_F(AnalyzerTriggerLogicTest, MCTriggers) {
    // Set up Analyzer for MC (no group for sample type)
    auto config3 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data3 = ManagerFactory::createDataManager(*config3);
    auto bmgr3 = ManagerRegistry::instance().create("BDTManager", {config3.get()});
    auto bdt3 = std::unique_ptr<IBDTManager>(dynamic_cast<IBDTManager*>(bmgr3.release()));
    auto cmgr3 = ManagerRegistry::instance().create("CorrectionManager", {config3.get()});
    auto corr3 = std::unique_ptr<ICorrectionManager>(dynamic_cast<ICorrectionManager*>(cmgr3.release()));
    auto tmgr3 = ManagerRegistry::instance().create("TriggerManager", {config3.get()});
    auto trig3 = std::unique_ptr<ITriggerManager>(dynamic_cast<ITriggerManager*>(tmgr3.release()));
    auto ndmgr3 = ManagerRegistry::instance().create("NDHistogramManager", {config3.get()});
    auto ndh3 = std::unique_ptr<INDHistogramManager>(dynamic_cast<INDHistogramManager*>(ndmgr3.release()));
    auto syst3 = ManagerFactory::createSystematicManager();
    config3->set("type", "MC");
    Analyzer analyzer(
        std::move(config3),
        std::move(data3),
        makeTestPluginMap(std::move(bdt3), std::move(corr3), std::move(trig3), std::move(ndh3)),
        std::move(syst3));
    analyzer.Define("trigger1", []() { return false; });
    analyzer.Define("trigger2", []() { return true; });
    analyzer.Define("trigger3", []() { return false; });
    analyzer.ApplyAllTriggers();
    auto df = analyzer.getDF();
    auto result = df.Count();
    EXPECT_EQ(result.GetValue(), 1UL);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 