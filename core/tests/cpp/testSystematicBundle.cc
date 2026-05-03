#include <SystematicBundle.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <ManagerFactory.h>
#include <SystematicManager.h>
#include <test_util.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <gtest/gtest.h>
#include <memory>
#include <set>
#include <string>
#include <vector>

class SystematicBundleTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    configManager = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
    systematicManager = std::make_unique<SystematicManager>();
    dataManager = std::make_unique<DataManager>(4);
  }
  void registerSystematic(const std::string &systName, const std::vector<std::string> &affected) {
    systematicManager->registerSystematic(systName, std::set<std::string>(affected.begin(), affected.end()));
  }
  void defineScalar(const std::string &name, std::function<float(ULong64_t)> gen) {
    dataManager->Define(name, gen, {"rdfentry_"}, *systematicManager);
  }
  void defineBool(const std::string &name, std::function<bool(ULong64_t)> gen) {
    dataManager->Define(name, gen, {"rdfentry_"}, *systematicManager);
  }
  void defineVariation(const std::string &base, const std::string &label, std::function<float(ULong64_t)> gen) {
    dataManager->Define(systematicManager->getVariationColumnName(base, label), gen, {"rdfentry_"}, *systematicManager);
  }
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DataManager> dataManager;
};

TEST_F(SystematicBundleTest, VariationMajorLayoutIsDefault) {
  defineScalar("f1", [](ULong64_t) { return 1.0f; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto spec = definePackedInputBundle(*dataManager, *systematicManager, {"f1"}, labels, "__syst_layout", false);
  EXPECT_EQ(spec.layout, SystematicBundleLayout::VariationMajor);
}

TEST_F(SystematicBundleTest, PackedBundleNominalOnly) {
  defineScalar("a", [](ULong64_t i) { return static_cast<float>(i); });
  defineScalar("b", [](ULong64_t i) { return static_cast<float>(i*10); });
  defineScalar("c", [](ULong64_t i) { return static_cast<float>(i*100); });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto spec = definePackedInputBundle(*dataManager, *systematicManager, {"a","b","c"}, labels, "__syst_packed", false);
  EXPECT_EQ(spec.variationLabels.size(), 1);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<float>>("__syst_packed");
  EXPECT_FLOAT_EQ((*result)[0][0], 0.0f);
  EXPECT_FLOAT_EQ((*result)[0][1], 0.0f);
  EXPECT_FLOAT_EQ((*result)[0][2], 0.0f);
  EXPECT_FLOAT_EQ((*result)[1][0], 1.0f);
  EXPECT_FLOAT_EQ((*result)[1][1], 10.0f);
  EXPECT_FLOAT_EQ((*result)[1][2], 100.0f);
}

TEST_F(SystematicBundleTest, PackedBundleMultipleSystematics) {
  registerSystematic("JES", {"a","b"});
  defineScalar("a", [](ULong64_t i) { return static_cast<float>(i); });
  defineScalar("b", [](ULong64_t i) { return static_cast<float>(i*10); });
  defineVariation("a","JESUp", [](ULong64_t i) { return static_cast<float>(i)+100.0f; });
  defineVariation("a","JESDown", [](ULong64_t i) { return static_cast<float>(i)-100.0f; });
  defineVariation("b","JESUp", [](ULong64_t i) { return static_cast<float>(i*10)+200.0f; });
  defineVariation("b","JESDown", [](ULong64_t i) { return static_cast<float>(i*10)-200.0f; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  ASSERT_EQ(labels.size(), 3);
  EXPECT_EQ(labels[1], "JESUp");
  auto spec = definePackedInputBundle(*dataManager, *systematicManager, {"a","b"}, labels, "__syst_multi", true);
  EXPECT_EQ(spec.resolvedColumnNames.size(), 6);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<float>>("__syst_multi");
  EXPECT_FLOAT_EQ((*result)[0][0], 0.0f);
  EXPECT_FLOAT_EQ((*result)[0][1], 0.0f);
  EXPECT_FLOAT_EQ((*result)[0][2], 100.0f);
  EXPECT_FLOAT_EQ((*result)[0][3], 200.0f);
  EXPECT_FLOAT_EQ((*result)[0][4], -100.0f);
  EXPECT_FLOAT_EQ((*result)[0][5], -200.0f);
}

TEST_F(SystematicBundleTest, ScalarBundleLayout) {
  registerSystematic("JES", {"x"});
  defineScalar("x", [](ULong64_t i) { return static_cast<float>(i); });
  defineVariation("x","JESUp", [](ULong64_t i) { return static_cast<float>(i)+10.0f; });
  defineVariation("x","JESDown", [](ULong64_t i) { return static_cast<float>(i)-10.0f; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto spec = defineScalarBundle(*dataManager, *systematicManager, "x", labels, "__syst_scalar", true);
  EXPECT_EQ(spec.layout, SystematicBundleLayout::VariationMajor);
  EXPECT_EQ(spec.variationLabels.size(), 3);
}

TEST_F(SystematicBundleTest, FanOutScalarMatchesVariationOrder) {
  registerSystematic("JES", {"x"});
  defineScalar("x", [](ULong64_t i) { return static_cast<float>(i); });
  defineVariation("x","JESUp", [](ULong64_t i) { return static_cast<float>(i)+10.0f; });
  defineVariation("x","JESDown", [](ULong64_t i) { return static_cast<float>(i)-10.0f; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  defineScalarBundle(*dataManager, *systematicManager, "x", labels, "__syst_fanout", true);
  auto out = fanOutScalarResultBundle(*dataManager, *systematicManager, "res", labels, "__syst_fanout", false);
  EXPECT_EQ(out.outputColumnNames[0], "res");
  EXPECT_EQ(out.outputColumnNames[1], "res_JESUp");
  EXPECT_EQ(out.outputColumnNames[2], "res_JESDown");
  auto df = dataManager->getDataFrame();
  auto nom = df.Take<float>("res");
  auto up = df.Take<float>("res_JESUp");
  auto dn = df.Take<float>("res_JESDown");
  EXPECT_FLOAT_EQ((*nom)[0], 0.0f);
  EXPECT_FLOAT_EQ((*up)[0], 10.0f);
  EXPECT_FLOAT_EQ((*dn)[0], -10.0f);
}

TEST_F(SystematicBundleTest, SelectionMaskNominalOnly) {
  defineBool("sel", [](ULong64_t i) { return i%2==0; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto spec = defineSelectionMaskBundle(*dataManager, *systematicManager, "sel", labels, "__syst_mask", false);
  EXPECT_EQ(spec.selectionMaskColumnName, "__syst_mask");
  EXPECT_EQ(spec.variationLabels.size(), 1);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<bool>>("__syst_mask");
  EXPECT_TRUE((*result)[0][0]);
  EXPECT_FALSE((*result)[1][0]);
}

TEST_F(SystematicBundleTest, VariationOrdering) {
  registerSystematic("BTag", {"b"});
  registerSystematic("JES", {"a"});
  defineScalar("a", [](ULong64_t i) { return static_cast<float>(i); });
  defineScalar("b", [](ULong64_t i) { return static_cast<float>(i*2); });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  ASSERT_EQ(labels.size(), 5);
  EXPECT_EQ(labels[0], "Nominal");
  EXPECT_EQ(labels[1], "BTagUp");
  EXPECT_EQ(labels[2], "BTagDown");
  EXPECT_EQ(labels[3], "JESUp");
  EXPECT_EQ(labels[4], "JESDown");
}

TEST_F(SystematicBundleTest, SelectionMaskWithExistingVariationColumns) {
  registerSystematic("JES", {"sel"});
  defineBool("sel", [](ULong64_t i) { return i%2==0; });
  dataManager->Define("sel_JESUp", [](ULong64_t i)->bool{return i%3==0;}, {"rdfentry_"}, *systematicManager);
  dataManager->Define("sel_JESDown", [](ULong64_t i)->bool{return (i+1)%3==0;}, {"rdfentry_"}, *systematicManager);
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  bool hasSyst = hasUsableSystematicColumns(*dataManager, *systematicManager, "sel", labels);
  EXPECT_TRUE(hasSyst);
  auto spec = defineSelectionMaskBundle(*dataManager, *systematicManager, "sel", labels, "__syst_mask_vars", hasSyst);
  EXPECT_EQ(spec.variationLabels.size(), 3);
  EXPECT_EQ(spec.selectionMaskColumnName, "__syst_mask_vars");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<bool>>("__syst_mask_vars");
  EXPECT_TRUE((*result)[0][0]);
  EXPECT_TRUE((*result)[0][1]);
  EXPECT_FALSE((*result)[0][2]);
}

TEST_F(SystematicBundleTest, PackedBundleFourFeatures) {
  defineScalar("a", [](ULong64_t i) { return static_cast<float>(i); });
  defineScalar("b", [](ULong64_t i) { return static_cast<float>(i*10); });
  defineScalar("c", [](ULong64_t i) { return static_cast<float>(i*100); });
  defineScalar("d", [](ULong64_t i) { return static_cast<float>(i*1000); });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto spec = definePackedInputBundle(*dataManager, *systematicManager, {"a","b","c","d"}, labels, "__syst_four", false);
  EXPECT_EQ(spec.resolvedColumnNames.size(), 4);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<float>>("__syst_four");
  EXPECT_FLOAT_EQ((*result)[1][0], 1.0f);
  EXPECT_FLOAT_EQ((*result)[1][1], 10.0f);
  EXPECT_FLOAT_EQ((*result)[1][2], 100.0f);
  EXPECT_FLOAT_EQ((*result)[1][3], 1000.0f);
}

TEST_F(SystematicBundleTest, DefineScalarBundlesMultipleVars) {
  registerSystematic("JES", {"x","y"});
  defineScalar("x", [](ULong64_t i) { return static_cast<float>(i); });
  defineScalar("y", [](ULong64_t i) { return static_cast<float>(i*2); });
  defineVariation("x","JESUp", [](ULong64_t i) { return static_cast<float>(i)+10.0f; });
  defineVariation("x","JESDown", [](ULong64_t i) { return static_cast<float>(i)-10.0f; });
  defineVariation("y","JESUp", [](ULong64_t i) { return static_cast<float>(i*2)+20.0f; });
  defineVariation("y","JESDown", [](ULong64_t i) { return static_cast<float>(i*2)-20.0f; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto specs = defineScalarBundles(*dataManager, *systematicManager, {"x","y"}, labels, "stest");
  ASSERT_EQ(specs.size(), 2);
  for (const auto &s : specs) EXPECT_EQ(s.layout, SystematicBundleLayout::VariationMajor);
}

TEST_F(SystematicBundleTest, SelectionMaskSystematicAware) {
  registerSystematic("JES", {"other"});
  defineBool("sel", [](ULong64_t i) { return i%2==0; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  bool hasSyst = hasUsableSystematicColumns(*dataManager, *systematicManager, "sel", labels);
  EXPECT_FALSE(hasSyst);
  auto spec = defineSelectionMaskBundle(*dataManager, *systematicManager, "sel", labels, "__syst_mask_ua", false);
  EXPECT_EQ(spec.selectionMaskColumnName, "__syst_mask_ua");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<bool>>("__syst_mask_ua");
  ASSERT_EQ(result->size(), 4);
}

TEST_F(SystematicBundleTest, FanOutMultiOutput) {
  registerSystematic("JES", {"x"});
  defineScalar("x", [](ULong64_t i) { return static_cast<float>(i); });
  defineVariation("x","JESUp", [](ULong64_t i) { return static_cast<float>(i)+10.0f; });
  defineVariation("x","JESDown", [](ULong64_t i) { return static_cast<float>(i)-10.0f; });
  auto labels = resolveVariationLabels(*systematicManager, *dataManager, ISystematicManager::CANONICAL_SYST_BRANCH_NAME);
  auto df = dataManager->getDataFrame();
  const size_t nOut=2, nVar=3;
  df = df.Define("__syst_multi_r", [](float x, float xUp, float xDown)->ROOT::VecOps::RVec<float> {
    return {x, xUp, xDown, x*2, xUp*2, xDown*2};
  }, {"x","x_JESUp","x_JESDown"});
  dataManager->setDataFrame(df);
  auto specs = fanOutMultiOutputResultBundle(*dataManager, *systematicManager, {"outA","outB"}, labels, "__syst_multi_r", false);
  ASSERT_EQ(specs.size(), 2);
  EXPECT_EQ(specs[0].outputColumnNames[0], "outA");
  EXPECT_EQ(specs[1].outputColumnNames[0], "outB");
  auto df2 = dataManager->getDataFrame();
  auto a = df2.Take<float>("outA");
  auto b = df2.Take<float>("outB");
  EXPECT_FLOAT_EQ((*a)[0], 0.0f);
  EXPECT_FLOAT_EQ((*b)[0], 0.0f);
  EXPECT_FLOAT_EQ((*a)[1], 1.0f);
  EXPECT_FLOAT_EQ((*b)[1], 2.0f);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
