// All implementation is in ObjectEnergyManagerBase.cc.
// This translation unit is required so that CMake compiles the derived class
// and the linker emits its vtable.
#include <ElectronEnergyScaleManager.h>

// ---------------------------------------------------------------------------
// Plugin helper function
// ---------------------------------------------------------------------------
#include <analyzer.h>

std::shared_ptr<ElectronEnergyScaleManager> makeElectronEnergyScaleManager(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<ElectronEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
