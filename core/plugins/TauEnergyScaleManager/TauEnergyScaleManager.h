#ifndef TAUENERGYMANAGER_H_INCLUDED
#define TAUENERGYMANAGER_H_INCLUDED

#include <ObjectEnergyManagerBase.h>

class Analyzer;

/**
 * @class TauEnergyScaleManager
 * @brief Concrete plugin for CMS Tau energy scale and resolution corrections.
 *
 * All implementation lives in ObjectEnergyManagerBase.  This class provides
 * the plugin type string ("TauEnergyScaleManager") and the physics-object name ("Tau").
 *
 * @see ObjectEnergyManagerBase for the full API reference.
 * @see docs/TAU_ENERGY_CORRECTIONS.md
 */
class TauEnergyScaleManager : public ObjectEnergyManagerBase {
public:
  std::string type() const override { return "TauEnergyScaleManager"; }
protected:
  std::string objectName() const override { return "Tau"; }

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<TauEnergyScaleManager> create(
      Analyzer& an, const std::string& role = "tauEnergyScaleManager");
};



#endif // TAUENERGYMANAGER_H_INCLUDED
