#ifndef JETVETOMAPMANAGER_H_INCLUDED
#define JETVETOMAPMANAGER_H_INCLUDED

//==============================================================================
// JetVetoMapManager — CMS JetVetoMap event filter
//
// Implements CMS JetVetoMap recommendations for Run 2 and Run 3:
//
// Run 3:
//   Veto events where ANY jet with minimal selection lies in vetoed regions.
//   Minimal selection: pT > 15 GeV, tightLepVeto jet ID (bit 2 of jetId),
//   (charged EM fraction + neutral EM fraction) < 0.9.
//   Maps per era (e.g., RunCD and RunEFG for 2022).
//
// Run 2:
//   Veto jets with axis aligned with veto regions.
//   Selection: pT > 15 GeV, tight jet ID (bit 2), PU jet ID for pT < 50 GeV,
//   (charged EM fraction + neutral EM fraction) < 0.9,
//   no overlap with PF muon (dR < 0.2).
//
// Maps are correctionlib JSON files under:
//   /cvmfs/cms-griddata.cern.ch/cat/metadata/JME/<era>/latest/jetvetomaps.json.gz
//
// Configuration (via jetVetoMapConfig text file, one path per line):
//   /path/to/jetvetomaps.json.gz
//
// Uses CorrectionManager to evaluate the correctionlib payload, then defines
// a filter column vetoing events where any selected jet falls in a veto region.
//==============================================================================

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>

#include <string>
#include <vector>
#include <memory>

class Analyzer;

class JetVetoMapManager : public IPluggableManager {
public:
  static std::shared_ptr<JetVetoMapManager> create(
      Analyzer& an, const std::string& role = "jetVetoMapManager");

  JetVetoMapManager() = default;

  /// Apply the JetVetoMap filter.
  /// Defines a "jet_veto_pass" column and filters events with vetoed jets.
  /// Applied to both data and MC.
  void applyJetVetoMap();

  std::string type() const override { return "JetVetoMapManager"; }
  void setContext(ManagerContext &ctx) override;
  void setupFromConfigFile() override;
  void initialize() override;
  void reportMetadata() override;

private:
  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  ILogger *logger_m = nullptr;

  std::vector<std::string> vetoMapFiles_m;
  std::string vetoMapType_m = "jetvetomap";
  bool active_m = false;

  void loadConfigFile(const std::string &path);
};

#endif // JETVETOMAPMANAGER_H_INCLUDED
