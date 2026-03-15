#ifndef MUONROCHESTERMANAGER_H_INCLUDED
#define MUONROCHESTERMANAGER_H_INCLUDED

#include <ObjectEnergyManagerBase.h>

/**
 * @class MuonRochesterManager
 * @brief Concrete plugin for CMS Muon energy scale and resolution corrections.
 *
 * All implementation lives in ObjectEnergyManagerBase.  This class provides
 * the plugin type string ("MuonRochesterManager") and the physics-object name ("Muon").
 *
 * @see ObjectEnergyManagerBase for the full API reference.
 * @see docs/MUON_ENERGY_CORRECTIONS.md  [or MUON_ROCHESTER_CORRECTIONS.md]
 */
class MuonRochesterManager : public ObjectEnergyManagerBase {
public:
  std::string type() const override { return "MuonRochesterManager"; }
protected:
  std::string objectName() const override { return "Muon"; }
};

#endif // MUONROCHESTERMANAGER_H_INCLUDED
