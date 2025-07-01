#include "SystematicManager.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <set>
#include <string>
#include <vector>

class SystematicManagerTest : public ::testing::Test {
protected:
  void SetUp() override { ChangeToTestSourceDir(); systematicManager = new SystematicManager(); }

  void TearDown() override { delete systematicManager; }

  SystematicManager *systematicManager = nullptr;
};

TEST_F(SystematicManagerTest, ConstructorCreatesEmptyManager) {
  SystematicManager manager;

  EXPECT_TRUE(manager.getSystematics().empty());
}

TEST_F(SystematicManagerTest, RegisterSystematicBasic) {
  std::set<std::string> affectedVars = {"var1", "var2", "var3"};
  systematicManager->registerSystematic("test_syst", affectedVars);

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 1);
  EXPECT_TRUE(systematics.find("test_syst") != systematics.end());

  const auto &varsForSyst =
      systematicManager->getVariablesForSystematic("test_syst");
  EXPECT_EQ(varsForSyst.size(), 3);
  EXPECT_TRUE(varsForSyst.find("var1") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var2") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var3") != varsForSyst.end());
}

TEST_F(SystematicManagerTest, RegisterMultipleSystematics) {
  std::set<std::string> vars1 = {"var1", "var2"};
  std::set<std::string> vars2 = {"var2", "var3"};
  std::set<std::string> vars3 = {"var1", "var3"};

  systematicManager->registerSystematic("syst1", vars1);
  systematicManager->registerSystematic("syst2", vars2);
  systematicManager->registerSystematic("syst3", vars3);

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 3);
  EXPECT_TRUE(systematics.find("syst1") != systematics.end());
  EXPECT_TRUE(systematics.find("syst2") != systematics.end());
  EXPECT_TRUE(systematics.find("syst3") != systematics.end());
}

TEST_F(SystematicManagerTest, GetVariablesForSystematic) {
  std::set<std::string> affectedVars = {"var1", "var2", "var3"};
  systematicManager->registerSystematic("test_syst", affectedVars);

  const auto &varsForSyst =
      systematicManager->getVariablesForSystematic("test_syst");
  EXPECT_EQ(varsForSyst.size(), 3);
  EXPECT_TRUE(varsForSyst.find("var1") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var2") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var3") != varsForSyst.end());
}

TEST_F(SystematicManagerTest, GetVariablesForNonexistentSystematic) {
  const auto &varsForSyst =
      systematicManager->getVariablesForSystematic("nonexistent");
  EXPECT_TRUE(varsForSyst.empty());
}

TEST_F(SystematicManagerTest, GetSystematicsForVariable) {
  std::set<std::string> vars1 = {"var1", "var2"};
  std::set<std::string> vars2 = {"var2", "var3"};
  std::set<std::string> vars3 = {"var1", "var3"};

  systematicManager->registerSystematic("syst1", vars1);
  systematicManager->registerSystematic("syst2", vars2);
  systematicManager->registerSystematic("syst3", vars3);

  // var1 is affected by syst1 and syst3
  const auto &systForVar1 =
      systematicManager->getSystematicsForVariable("var1");
  EXPECT_EQ(systForVar1.size(), 2);
  EXPECT_TRUE(systForVar1.find("syst1") != systForVar1.end());
  EXPECT_TRUE(systForVar1.find("syst3") != systForVar1.end());

  // var2 is affected by syst1 and syst2
  const auto &systForVar2 =
      systematicManager->getSystematicsForVariable("var2");
  EXPECT_EQ(systForVar2.size(), 2);
  EXPECT_TRUE(systForVar2.find("syst1") != systForVar2.end());
  EXPECT_TRUE(systForVar2.find("syst2") != systForVar2.end());

  // var3 is affected by syst2 and syst3
  const auto &systForVar3 =
      systematicManager->getSystematicsForVariable("var3");
  EXPECT_EQ(systForVar3.size(), 2);
  EXPECT_TRUE(systForVar3.find("syst2") != systForVar3.end());
  EXPECT_TRUE(systForVar3.find("syst3") != systForVar3.end());
}

TEST_F(SystematicManagerTest, GetSystematicsForNonexistentVariable) {
  const auto &systForVar =
      systematicManager->getSystematicsForVariable("nonexistent");
  EXPECT_TRUE(systForVar.empty());
}

TEST_F(SystematicManagerTest, RegisterSystematicWithEmptyVariables) {
  std::set<std::string> emptyVars;
  systematicManager->registerSystematic("empty_syst", emptyVars);

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 1);
  EXPECT_TRUE(systematics.find("empty_syst") != systematics.end());

  const auto &varsForSyst =
      systematicManager->getVariablesForSystematic("empty_syst");
  EXPECT_TRUE(varsForSyst.empty());
}

TEST_F(SystematicManagerTest, RegisterSystematicWithDuplicateVariables) {
  std::set<std::string> vars = {"var1", "var1", "var2"}; // var1 appears twice
  systematicManager->registerSystematic("duplicate_syst", vars);

  const auto &varsForSyst =
      systematicManager->getVariablesForSystematic("duplicate_syst");
  EXPECT_EQ(varsForSyst.size(), 2); // Should only contain unique variables
  EXPECT_TRUE(varsForSyst.find("var1") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var2") != varsForSyst.end());
}

TEST_F(SystematicManagerTest, RegisterExistingSystematics) {
  std::vector<std::string> systConfig = {"syst1", "syst2", "syst3"};
  std::vector<std::string> columnList = {"var1_syst1Up", "var2_syst2Up",
                                         "var3_syst3Up"};

  systematicManager->registerExistingSystematics(systConfig, columnList);

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 3);
  EXPECT_TRUE(systematics.find("syst1") != systematics.end());
  EXPECT_TRUE(systematics.find("syst2") != systematics.end());
  EXPECT_TRUE(systematics.find("syst3") != systematics.end());

  // Each systematic should affect its corresponding variable
  const auto &varsForSyst1 =
      systematicManager->getVariablesForSystematic("syst1");
  EXPECT_EQ(varsForSyst1.size(), 1);
  EXPECT_TRUE(varsForSyst1.find("var1") != varsForSyst1.end());

  const auto &varsForSyst2 =
      systematicManager->getVariablesForSystematic("syst2");
  EXPECT_EQ(varsForSyst2.size(), 1);
  EXPECT_TRUE(varsForSyst2.find("var2") != varsForSyst2.end());

  const auto &varsForSyst3 =
      systematicManager->getVariablesForSystematic("syst3");
  EXPECT_EQ(varsForSyst3.size(), 1);
  EXPECT_TRUE(varsForSyst3.find("var3") != varsForSyst3.end());
}

TEST_F(SystematicManagerTest, RegisterExistingSystematicsWithEmptyConfig) {
  std::vector<std::string> emptySystConfig;
  std::vector<std::string> columnList = {"var1", "var2"};

  systematicManager->registerExistingSystematics(emptySystConfig, columnList);

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_TRUE(systematics.empty());
}

TEST_F(SystematicManagerTest, RegisterExistingSystematicsWithEmptyColumns) {
  std::vector<std::string> systConfig = {"syst1", "syst2"};
  std::vector<std::string> emptyColumnList;

  systematicManager->registerExistingSystematics(systConfig, emptyColumnList);

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 0); // No variables match the pattern
}

TEST_F(SystematicManagerTest, ComplexSystematicRelationships) {
  // Create a complex scenario with overlapping systematics
  std::set<std::string> vars1 = {"var1", "var2", "var3"};
  std::set<std::string> vars2 = {"var2", "var4"};
  std::set<std::string> vars3 = {"var1", "var5"};
  std::set<std::string> vars4 = {"var3", "var4", "var5"};

  systematicManager->registerSystematic("syst1", vars1);
  systematicManager->registerSystematic("syst2", vars2);
  systematicManager->registerSystematic("syst3", vars3);
  systematicManager->registerSystematic("syst4", vars4);

  // Test all systematics
  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 4);

  // Test variable relationships
  const auto &systForVar1 =
      systematicManager->getSystematicsForVariable("var1");
  EXPECT_EQ(systForVar1.size(), 2);
  EXPECT_TRUE(systForVar1.find("syst1") != systForVar1.end());
  EXPECT_TRUE(systForVar1.find("syst3") != systForVar1.end());

  const auto &systForVar2 =
      systematicManager->getSystematicsForVariable("var2");
  EXPECT_EQ(systForVar2.size(), 2);
  EXPECT_TRUE(systForVar2.find("syst1") != systForVar2.end());
  EXPECT_TRUE(systForVar2.find("syst2") != systForVar2.end());

  const auto &systForVar3 =
      systematicManager->getSystematicsForVariable("var3");
  EXPECT_EQ(systForVar3.size(), 2);
  EXPECT_TRUE(systForVar3.find("syst1") != systForVar3.end());
  EXPECT_TRUE(systForVar3.find("syst4") != systForVar3.end());

  const auto &systForVar4 =
      systematicManager->getSystematicsForVariable("var4");
  EXPECT_EQ(systForVar4.size(), 2);
  EXPECT_TRUE(systForVar4.find("syst2") != systForVar4.end());
  EXPECT_TRUE(systForVar4.find("syst4") != systForVar4.end());

  const auto &systForVar5 =
      systematicManager->getSystematicsForVariable("var5");
  EXPECT_EQ(systForVar5.size(), 2);
  EXPECT_TRUE(systForVar5.find("syst3") != systForVar5.end());
  EXPECT_TRUE(systForVar5.find("syst4") != systForVar5.end());
}

TEST_F(SystematicManagerTest, OverwriteSystematic) {
  std::set<std::string> vars1 = {"var1", "var2"};
  std::set<std::string> vars2 = {"var3", "var4"};

  systematicManager->registerSystematic("test_syst", vars1);
  systematicManager->registerSystematic("test_syst", vars2); // Overwrite

  const auto &systematics = systematicManager->getSystematics();
  EXPECT_EQ(systematics.size(), 1);

  const auto &varsForSyst =
      systematicManager->getVariablesForSystematic("test_syst");
  EXPECT_EQ(varsForSyst.size(), 4); // Both sets are added, not overwritten
  EXPECT_TRUE(varsForSyst.find("var1") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var2") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var3") != varsForSyst.end());
  EXPECT_TRUE(varsForSyst.find("var4") != varsForSyst.end());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}