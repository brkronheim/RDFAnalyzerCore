#include <gtest/gtest.h>
#include <ManagerFactory.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <TriggerManager.h>
#include "test_util.h"
#include <memory>
#include <vector>
#include <string>

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
        triggerManager = ManagerFactory::createTriggerManager(*configManager);
        systematicManager = ManagerFactory::createSystematicManager();
        bdtManager = ManagerFactory::createBDTManager(*configManager);
        correctionManager = ManagerFactory::createCorrectionManager(*configManager);
        ndHistManager = ManagerFactory::createNDHistogramManager(*dataManager, *configManager, *systematicManager);
    }
    std::unique_ptr<IConfigurationProvider> configManager;
    std::unique_ptr<IDataFrameProvider> dataManager;
    std::unique_ptr<ITriggerManager> triggerManager;
    std::unique_ptr<ISystematicManager> systematicManager;
    std::unique_ptr<IBDTManager> bdtManager;
    std::unique_ptr<ICorrectionManager> correctionManager;
    std::unique_ptr<INDHistogramManager> ndHistManager;
};

TEST_F(AnalyzerTriggerLogicTest, DataTriggersAndVetoes) {
    // Set up Analyzer for a data sample
    auto config1 = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto data1 = ManagerFactory::createDataManager(*config1);
    auto bdt1 = ManagerFactory::createBDTManager(*config1);
    auto corr1 = ManagerFactory::createCorrectionManager(*config1);
    auto trig1 = ManagerFactory::createTriggerManager(*config1);
    auto ndh1 = ManagerFactory::createNDHistogramManager(*data1, *config1, *systematicManager);
    auto syst1 = ManagerFactory::createSystematicManager();
    config1->set("type", "test_sample");
    Analyzer analyzer(
        std::move(config1),
        std::move(data1),
        std::move(bdt1),
        std::move(corr1),
        std::move(trig1),
        std::move(ndh1),
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
    auto bdt2 = ManagerFactory::createBDTManager(*config2);
    auto corr2 = ManagerFactory::createCorrectionManager(*config2);
    auto trig2 = ManagerFactory::createTriggerManager(*config2);
    auto ndh2 = ManagerFactory::createNDHistogramManager(*data2, *config2, *systematicManager);
    auto syst2 = ManagerFactory::createSystematicManager();
    config2->set("type", "test_sample");
    Analyzer analyzer2(
        std::move(config2),
        std::move(data2),
        std::move(bdt2),
        std::move(corr2),
        std::move(trig2),
        std::move(ndh2),
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
    auto bdt3 = ManagerFactory::createBDTManager(*config3);
    auto corr3 = ManagerFactory::createCorrectionManager(*config3);
    auto trig3 = ManagerFactory::createTriggerManager(*config3);
    auto ndh3 = ManagerFactory::createNDHistogramManager(*data3, *config3, *systematicManager);
    auto syst3 = ManagerFactory::createSystematicManager();
    config3->set("type", "MC");
    Analyzer analyzer(
        std::move(config3),
        std::move(data3),
        std::move(bdt3),
        std::move(corr3),
        std::move(trig3),
        std::move(ndh3),
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