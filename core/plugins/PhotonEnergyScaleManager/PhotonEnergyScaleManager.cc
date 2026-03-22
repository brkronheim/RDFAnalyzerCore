// All implementation is in ObjectEnergyManagerBase.cc.
// This translation unit is required so that CMake compiles the derived class
// and the linker emits its vtable.
#include <PhotonEnergyScaleManager.h>
#include <analyzer.h>

std::shared_ptr<PhotonEnergyScaleManager> PhotonEnergyScaleManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<PhotonEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
