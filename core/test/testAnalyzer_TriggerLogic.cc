#include <gtest/gtest.h>
#include <ManagerFactory.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <TriggerManager.h>
#include <BDTManager.h>
#include <CorrectionManager.h>
//#include <NDHistogramManager.h>
#include <test_util.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <api/IPluggableManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>

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
        
            triggerManager = std::make_unique<TriggerManager>(*configManager);
        
        systematicManager = ManagerFactory::createSystematicManager();
            bdtManager = std::make_unique<BDTManager>(*configManager);
            correctionManager = std::make_unique<CorrectionManager>(*configManager);
        //auto ndmgr = ManagerRegistry::instance().create("NDHistogramManager", {configManager.get()});
        //ndHistManager = std::unique_ptr<NDHistogramManager>(dynamic_cast<NDHistogramManager*>(ndmgr.release()));
    }
    std::unique_ptr<IConfigurationProvider> configManager;
    std::unique_ptr<IDataFrameProvider> dataManager;
    std::unique_ptr<TriggerManager> triggerManager;
    std::unique_ptr<ISystematicManager> systematicManager;
    std::unique_ptr<BDTManager> bdtManager;
    std::unique_ptr<CorrectionManager> correctionManager;
    //std::unique_ptr<NDHistogramManager> ndHistManager;
};

// Helper to build plugin map for Analyzer
std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> makeTestPluginMap(
    std::unique_ptr<BDTManager> bdt,
    std::unique_ptr<CorrectionManager> corr,
    std::unique_ptr<TriggerManager> trig
    //std::unique_ptr<NDHistogramManager> ndh
    ) {
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
    plugins["bdt"] = std::move(bdt);
    plugins["correction"] = std::move(corr);
    plugins["trigger"] = std::move(trig);
    //plugins["ndhist"] = std::unique_ptr<IPluggableManager>(dynamic_cast<IPluggableManager*>(ndh.release()));
    return plugins;
}

TEST_F(AnalyzerTriggerLogicTest, DataTriggersAndVetoes) {
    // Set up Analyzer for a data sample
    auto config1 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data1 = ManagerFactory::createDataManager(*config1);
    auto bdt1 = std::make_unique<BDTManager>(*config1);
    auto corr1 = std::make_unique<CorrectionManager>(*config1);
    auto trig1 = std::make_unique<TriggerManager>(*config1);
    //auto ndmgr1 = ManagerRegistry::instance().create("NDHistogramManager", {config1.get()});
    //auto ndh1 = std::unique_ptr<NDHistogramManager>(dynamic_cast<NDHistogramManager*>(ndmgr1.release()));
    auto syst1 = ManagerFactory::createSystematicManager();
    config1->set("type", "test_sample");
    auto logger1 = std::make_unique<DefaultLogger>();
    auto skimSink1 = std::make_unique<NullOutputSink>();
    auto metaSink1 = std::make_unique<NullOutputSink>();
    Analyzer analyzer(
        std::move(config1),
        std::move(data1),
        makeTestPluginMap(std::move(bdt1), std::move(corr1), std::move(trig1)),
        //, std::move(ndh1)),
        std::move(syst1),
        std::move(logger1),
        std::move(skimSink1),
        std::move(metaSink1));

    // Define dummy trigger columns (simulate trigger firing)
    analyzer.Define("trigger1", []() { return true; });
    analyzer.Define("trigger2", []() { return false; });
    analyzer.Define("trigger3", []() { return false; });
    analyzer.Define("veto1", []() { return false; });
    analyzer.Define("veto2", []() { return false; });

    // Get the trigger manager and apply triggers
    auto triggerPlugin = analyzer.getPlugin<TriggerManager>("trigger");
    triggerPlugin->applyAllTriggers();
    auto df = analyzer.getDF();
    auto result = df.Count();
    EXPECT_EQ(result.GetValue(), 1UL);

    // Now set a veto to true, should fail
    auto config2 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data2 = ManagerFactory::createDataManager(*config2);
    auto bdt2 = std::make_unique<BDTManager>(*config2);
    auto corr2 = std::make_unique<CorrectionManager>(*config2);
    auto trig2 = std::make_unique<TriggerManager>(*config2);
    //auto ndmgr2 = ManagerRegistry::instance().create("NDHistogramManager", {config2.get()});
    //auto ndh2 = std::unique_ptr<NDHistogramManager>(dynamic_cast<NDHistogramManager*>(ndmgr2.release()));
    auto syst2 = ManagerFactory::createSystematicManager();
    config2->set("type", "test_sample");
    auto logger2 = std::make_unique<DefaultLogger>();
    auto skimSink2 = std::make_unique<NullOutputSink>();
    auto metaSink2 = std::make_unique<NullOutputSink>();
    Analyzer analyzer2(
        std::move(config2),
        std::move(data2),
        makeTestPluginMap(std::move(bdt2), std::move(corr2), std::move(trig2)),
        //, std::move(ndh2)),
        std::move(syst2),
        std::move(logger2),
        std::move(skimSink2),
        std::move(metaSink2));
    analyzer2.Define("trigger1", []() { return true; });
    analyzer2.Define("trigger2", []() { return false; });
    analyzer2.Define("trigger3", []() { return false; });
    analyzer2.Define("veto1", []() { return true; });
    analyzer2.Define("veto2", []() { return false; });
    
    // Get the trigger manager and apply triggers
    auto triggerPlugin2 = analyzer2.getPlugin<TriggerManager>("trigger");
    triggerPlugin2->applyAllTriggers();
    auto df2 = analyzer2.getDF();
    auto result2 = df2.Count();
    EXPECT_EQ(result2.GetValue(), 0UL);
}

TEST_F(AnalyzerTriggerLogicTest, MCTriggers) {
    // Set up Analyzer for MC (no group for sample type)
    auto config3 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data3 = ManagerFactory::createDataManager(*config3);
    auto bdt3 = std::make_unique<BDTManager>(*config3);
    auto corr3 = std::make_unique<CorrectionManager>(*config3);
    auto trig3 = std::make_unique<TriggerManager>(*config3);
    //auto ndmgr3 = ManagerRegistry::instance().create("NDHistogramManager", {config3.get()});
    //auto ndh3 = std::unique_ptr<NDHistogramManager>(dynamic_cast<NDHistogramManager*>(ndmgr3.release()));
    auto syst3 = ManagerFactory::createSystematicManager();
    config3->set("type", "MC");
    auto logger3 = std::make_unique<DefaultLogger>();
    auto skimSink3 = std::make_unique<NullOutputSink>();
    auto metaSink3 = std::make_unique<NullOutputSink>();
    Analyzer analyzer(
        std::move(config3),
        std::move(data3),
        makeTestPluginMap(std::move(bdt3), std::move(corr3), std::move(trig3)),
        //, std::move(ndh3)),
        std::move(syst3),
        std::move(logger3),
        std::move(skimSink3),
        std::move(metaSink3));
    analyzer.Define("trigger1", []() { return false; });
    analyzer.Define("trigger2", []() { return true; });
    analyzer.Define("trigger3", []() { return false; });
    
    // Get the trigger manager and apply triggers
    auto triggerPlugin = analyzer.getPlugin<TriggerManager>("trigger");
    triggerPlugin->applyAllTriggers();
    auto df = analyzer.getDF();
    auto result = df.Count();
    EXPECT_EQ(result.GetValue(), 1UL);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 