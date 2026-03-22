// All implementation is in ObjectEnergyManagerBase.cc.
// This translation unit is required so that CMake compiles the derived class
// and the linker emits its vtable.
#include <TauEnergyScaleManager.h>
#include <analyzer.h>

std::shared_ptr<TauEnergyScaleManager> TauEnergyScaleManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<TauEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
