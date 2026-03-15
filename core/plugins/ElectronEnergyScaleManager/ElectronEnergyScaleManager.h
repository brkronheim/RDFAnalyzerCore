#ifndef ELECTRONENERGYMANAGER_H_INCLUDED
#define ELECTRONENERGYMANAGER_H_INCLUDED

#include <ObjectEnergyManagerBase.h>

/**
 * @class ElectronEnergyScaleManager
 * @brief Concrete plugin for CMS Electron energy scale and resolution corrections.
 *
 * All implementation lives in ObjectEnergyManagerBase.  This class provides
 * the plugin type string ("ElectronEnergyScaleManager") and the physics-object name ("Electron").
 *
 * @see ObjectEnergyManagerBase for the full API reference.
 * @see docs/ELECTRON_ENERGY_CORRECTIONS.md  [or MUON_ROCHESTER_CORRECTIONS.md]
 */
class ElectronEnergyScaleManager : public ObjectEnergyManagerBase {
public:
  std::string type() const override { return "ElectronEnergyScaleManager"; }
protected:
  std::string objectName() const override { return "Electron"; }
};

#endif // ELECTRONENERGYMANAGER_H_INCLUDED
