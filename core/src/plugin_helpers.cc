// plugin_helpers.cc — implements factory helpers for all plugin types.
// This file lives in core/src so it has access to all include paths.

#include <analyzer.h>

#include <NDHistogramManager.h>
#include <CutflowManager.h>
#include <RegionManager.h>
#include <TriggerManager.h>
#include <WeightManager.h>
#include <CorrectionManager.h>
#include <BDTManager.h>
#include <JetEnergyScaleManager.h>
#include <GoldenJsonManager.h>
#include <OnnxManager.h>
#include <ElectronEnergyScaleManager.h>
#include <PhotonEnergyScaleManager.h>
#include <TauEnergyScaleManager.h>
#include <MuonRochesterManager.h>
#include <SofieManager.h>
#include <KinematicFitManager.h>

std::shared_ptr<NDHistogramManager> makeNDHistogramManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<NDHistogramManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<CutflowManager> makeCutflowManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<CutflowManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<RegionManager> makeRegionManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<RegionManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<TriggerManager> makeTriggerManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<TriggerManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<WeightManager> makeWeightManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<WeightManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<CorrectionManager> makeCorrectionManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<CorrectionManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<BDTManager> makeBDTManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<BDTManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<JetEnergyScaleManager> makeJetEnergyScaleManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<JetEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<GoldenJsonManager> makeGoldenJsonManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<GoldenJsonManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<OnnxManager> makeOnnxManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<OnnxManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<ElectronEnergyScaleManager> makeElectronEnergyScaleManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<ElectronEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<PhotonEnergyScaleManager> makePhotonEnergyScaleManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<PhotonEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<TauEnergyScaleManager> makeTauEnergyScaleManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<TauEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<MuonRochesterManager> makeMuonRochesterManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<MuonRochesterManager>();
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<SofieManager> makeSofieManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<SofieManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

std::shared_ptr<KinematicFitManager> makeKinematicFitManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<KinematicFitManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}

