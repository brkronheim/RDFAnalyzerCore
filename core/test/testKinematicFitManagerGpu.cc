#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <KinematicFitManager.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>
#include <gtest/gtest.h>
#include <test_util.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace {

bool IsSkippableCudaRuntimeError(const std::runtime_error &error) {
  const std::string message = error.what();
  return message.find("CUDA") != std::string::npos ||
         message.find("cuda") != std::string::npos;
}

void DefineFitInputs(DataManager &dataManager,
                     SystematicManager &systematicManager) {
  dataManager.Define("lep1_pt", [](ULong64_t) -> float { return 46.0f; }, {"rdfentry_"}, systematicManager);
  dataManager.Define("lep1_eta", [](ULong64_t) -> float { return 0.5f; }, {"rdfentry_"}, systematicManager);
  dataManager.Define("lep1_phi", [](ULong64_t) -> float { return 1.0f; }, {"rdfentry_"}, systematicManager);
  dataManager.Define("lep1_mass", [](ULong64_t) -> float { return 0.106f; }, {"rdfentry_"}, systematicManager);
  dataManager.Define("met_pt", [](ULong64_t) -> float { return 38.0f; }, {"rdfentry_"}, systematicManager);
  dataManager.Define("met_phi", [](ULong64_t) -> float { return -0.8f; }, {"rdfentry_"}, systematicManager);
}

ManagerContext MakeContext(IConfigurationProvider &config,
                           DataManager &dataManager,
                           SystematicManager &systematicManager,
                           DefaultLogger &logger,
                           NullOutputSink &skimSink,
                           NullOutputSink &metaSink) {
  return ManagerContext{config, dataManager, systematicManager,
                        logger, skimSink, metaSink};
}

} // namespace

TEST(KinematicFitManagerGpuIntegrationTest, GpuFitMatchesCpuReference) {
  ChangeToTestSourceDir();

  auto cpuConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto gpuConfig = ManagerFactory::createConfigurationManager("cfg/test_gpu_config.txt");
  auto cpuManager = std::make_unique<KinematicFitManager>(*cpuConfig);
  auto gpuManager = std::make_unique<KinematicFitManager>(*gpuConfig);

  auto cpuSystematics = std::make_unique<SystematicManager>();
  auto gpuSystematics = std::make_unique<SystematicManager>();
  auto cpuDataManager = std::make_unique<DataManager>(4);
  auto gpuDataManager = std::make_unique<DataManager>(4);
  auto logger = std::make_unique<DefaultLogger>();
  auto skimSink = std::make_unique<NullOutputSink>();
  auto metaSink = std::make_unique<NullOutputSink>();

  auto cpuContext = MakeContext(*cpuConfig, *cpuDataManager, *cpuSystematics,
                                *logger, *skimSink, *metaSink);
  auto gpuContext = MakeContext(*gpuConfig, *gpuDataManager, *gpuSystematics,
                                *logger, *skimSink, *metaSink);
  cpuManager->setContext(cpuContext);
  gpuManager->setContext(gpuContext);

  DefineFitInputs(*cpuDataManager, *cpuSystematics);
  DefineFitInputs(*gpuDataManager, *gpuSystematics);

  try {
    cpuManager->applyFit("wjFit");
    gpuManager->applyFit("wjFitGPU");

    auto cpuDf = cpuDataManager->getDataFrame();
    auto gpuDf = gpuDataManager->getDataFrame();

    auto cpuChi2 = cpuDf.Take<Float_t>("wjFit_chi2");
    auto gpuChi2 = gpuDf.Take<Float_t>("wjFitGPU_chi2");
    auto cpuConv = cpuDf.Take<bool>("wjFit_converged");
    auto gpuConv = gpuDf.Take<bool>("wjFitGPU_converged");
    auto cpuLepPt = cpuDf.Take<Float_t>("wjFit_lep_pt_fitted");
    auto gpuLepPt = gpuDf.Take<Float_t>("wjFitGPU_lep_pt_fitted");
    auto cpuNuPt = cpuDf.Take<Float_t>("wjFit_nu_pt_fitted");
    auto gpuNuPt = gpuDf.Take<Float_t>("wjFitGPU_nu_pt_fitted");
    auto cpuNuPhi = cpuDf.Take<Float_t>("wjFit_nu_phi_fitted");
    auto gpuNuPhi = gpuDf.Take<Float_t>("wjFitGPU_nu_phi_fitted");

    ASSERT_EQ(cpuChi2->size(), gpuChi2->size());
    ASSERT_EQ(cpuConv->size(), gpuConv->size());
    for (std::size_t index = 0; index < cpuChi2->size(); ++index) {
      EXPECT_TRUE(cpuConv->at(index));
      EXPECT_TRUE(gpuConv->at(index));
      EXPECT_NEAR(cpuChi2->at(index), gpuChi2->at(index), 1e-3f);
      EXPECT_NEAR(cpuLepPt->at(index), gpuLepPt->at(index), 1e-3f);
      EXPECT_NEAR(cpuNuPt->at(index), gpuNuPt->at(index), 1e-3f);
      EXPECT_NEAR(cpuNuPhi->at(index), gpuNuPhi->at(index), 1e-3f);
    }
  } catch (const std::runtime_error &error) {
    if (IsSkippableCudaRuntimeError(error)) {
      GTEST_SKIP() << "Skipping kinematic-fit GPU test because the CUDA runtime is unavailable: "
                   << error.what();
    }
    throw;
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}