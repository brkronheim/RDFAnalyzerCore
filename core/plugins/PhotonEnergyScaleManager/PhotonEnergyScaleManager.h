#ifndef PHOTONENERGYMANAGER_H_INCLUDED
#define PHOTONENERGYMANAGER_H_INCLUDED

#include <ObjectEnergyManagerBase.h>
#include <memory>

class Analyzer;

/**
 * @class PhotonEnergyScaleManager
 * @brief Concrete plugin for CMS Photon energy scale and resolution corrections.
 *
 * All implementation lives in ObjectEnergyManagerBase.  This class provides
 * the plugin type string ("PhotonEnergyScaleManager") and the physics-object name ("Photon").
 *
 * @see ObjectEnergyManagerBase for the full API reference.
 * @see docs/PHOTON_ENERGY_CORRECTIONS.md
 */
class PhotonEnergyScaleManager : public ObjectEnergyManagerBase {
public:

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<PhotonEnergyScaleManager> create(
      Analyzer& an, const std::string& role = "photonEnergyScaleManager");

  std::string type() const override { return "PhotonEnergyScaleManager"; }
protected:
  std::string objectName() const override { return "Photon"; }
};



#endif // PHOTONENERGYMANAGER_H_INCLUDED
