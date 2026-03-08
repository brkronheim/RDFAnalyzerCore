#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <OnnxManager.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>
#include <gtest/gtest.h>
#include <test_util.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace {

bool IsSkippableCudaRuntimeError(const std::exception &error) {
  std::string message = error.what();
  std::transform(message.begin(), message.end(), message.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return message.find("cuda") != std::string::npos ||
         message.find("provider") != std::string::npos ||
         message.find("shared library") != std::string::npos ||
         message.find("cublas") != std::string::npos ||
         message.find("onnxruntime_providers_cuda") != std::string::npos;
}

void DefineOnnxInputs(DataManager &dataManager,
                      SystematicManager &systematicManager,
                      bool runValue = true) {
  dataManager.Define("feature1", [](ULong64_t entry) -> float {
    return entry == 0 ? 1.0f : 2.0f;
  }, {"rdfentry_"}, systematicManager);
  dataManager.Define("feature2", [](ULong64_t) -> float {
    return 2.0f;
  }, {"rdfentry_"}, systematicManager);
  dataManager.Define("feature3", [](ULong64_t) -> float {
    return 3.0f;
  }, {"rdfentry_"}, systematicManager);
  dataManager.Define("run_number", [runValue](ULong64_t) -> bool {
    return runValue;
  }, {"rdfentry_"}, systematicManager);
}

} // namespace

class OnnxManagerGpuIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
  }

  ManagerContext makeContext(IConfigurationProvider &config,
                             DataManager &dataManager,
                             SystematicManager &systematicManager,
                             DefaultLogger &logger,
                             NullOutputSink &skimSink,
                             NullOutputSink &metaSink) {
    return ManagerContext{config, dataManager, systematicManager,
                          logger, skimSink, metaSink};
  }
};

TEST_F(OnnxManagerGpuIntegrationTest, GpuConfiguredModelReportsCudaEnabled) {
  auto config = ManagerFactory::createConfigurationManager(
      "cfg/test_onnx_gpu_config.txt");

  try {
    OnnxManager manager(*config);
    EXPECT_TRUE(manager.getUseCuda("test_model_gpu"));
    EXPECT_EQ(manager.getRunVar("test_model_gpu"), "run_number");
    const auto &inputNames = manager.getModelInputNames("test_model_gpu");
    const auto &outputNames = manager.getModelOutputNames("test_model_gpu");
    EXPECT_EQ(inputNames.size(), 1u);
    EXPECT_EQ(outputNames.size(), 1u);
  } catch (const std::exception &error) {
    if (IsSkippableCudaRuntimeError(error)) {
      GTEST_SKIP() << "CUDA-enabled ONNX Runtime is built, but the runtime environment is not usable: "
                   << error.what();
    }
    throw;
  }
}

TEST_F(OnnxManagerGpuIntegrationTest, GpuInferenceMatchesCpuReference) {
  auto cpuConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto gpuConfig = ManagerFactory::createConfigurationManager("cfg/test_onnx_gpu_config.txt");

  auto cpuSystematics = std::make_unique<SystematicManager>();
  auto gpuSystematics = std::make_unique<SystematicManager>();
  auto cpuDataManager = std::make_unique<DataManager>(2);
  auto gpuDataManager = std::make_unique<DataManager>(2);
  auto logger = std::make_unique<DefaultLogger>();
  auto skimSink = std::make_unique<NullOutputSink>();
  auto metaSink = std::make_unique<NullOutputSink>();

  try {
    auto cpuManager = std::make_unique<OnnxManager>(*cpuConfig);
    auto gpuManager = std::make_unique<OnnxManager>(*gpuConfig);

    auto cpuContext = makeContext(*cpuConfig, *cpuDataManager, *cpuSystematics,
                                  *logger, *skimSink, *metaSink);
    auto gpuContext = makeContext(*gpuConfig, *gpuDataManager, *gpuSystematics,
                                  *logger, *skimSink, *metaSink);
    cpuManager->setContext(cpuContext);
    gpuManager->setContext(gpuContext);

    DefineOnnxInputs(*cpuDataManager, *cpuSystematics);
    DefineOnnxInputs(*gpuDataManager, *gpuSystematics);

    cpuManager->applyModel("test_model");
    gpuManager->applyModel("test_model_gpu");

    auto cpuResult = cpuDataManager->getDataFrame().Take<float>("test_model");
    auto gpuResult = gpuDataManager->getDataFrame().Take<float>("test_model_gpu");

    ASSERT_EQ(cpuResult->size(), gpuResult->size());
    for (std::size_t index = 0; index < cpuResult->size(); ++index) {
      EXPECT_NEAR(cpuResult->at(index), gpuResult->at(index), 1e-5f);
    }
  } catch (const std::exception &error) {
    if (IsSkippableCudaRuntimeError(error)) {
      GTEST_SKIP() << "Skipping ONNX GPU inference comparison because the CUDA execution provider is unavailable at runtime: "
                   << error.what();
    }
    throw;
  }
}

TEST_F(OnnxManagerGpuIntegrationTest, NegativeCudaDeviceIdIsRejected) {
  auto invalidConfig = ManagerFactory::createConfigurationManager(
      "cfg/test_onnx_gpu_invalid_device_config.txt");
  EXPECT_THROW({ OnnxManager manager(*invalidConfig); }, std::runtime_error);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}