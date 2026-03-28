#ifndef CORRECTEDOBJECTCOLLECTIONMANAGERS_H_INCLUDED
#define CORRECTEDOBJECTCOLLECTIONMANAGERS_H_INCLUDED

#include <CorrectionManager.h>
#include <ElectronEnergyScaleManager.h>
#include <JetEnergyScaleManager.h>
#include <MuonRochesterManager.h>
#include <PhotonEnergyScaleManager.h>
#include <TauEnergyScaleManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IPluggableManager.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class Analyzer;

struct CorrectedCollectionSpec {
  std::string configFile;
  std::string workflowConfig;
  std::string inputCollection;
  std::string ptColumn;
  std::string etaColumn;
  std::string phiColumn;
  std::string massColumn;
  std::string correctedPtColumn;
  std::string correctedMassColumn;
  std::string outputCollection;
  std::string variationMapColumn;
  bool autoBuildInputCollection = false;
};

class CorrectedCollectionManagerBase : public IPluggableManager {
public:
  void setContext(ManagerContext &ctx) override;
  void setupFromConfigFile() override;
  void execute() override;
  void reportMetadata() override;
  std::unordered_map<std::string, std::string> collectProvenanceEntries() const override;
  std::vector<std::string> getRequiredColumns() const override;
  std::vector<std::string> getProducedColumns() const override;

protected:
  explicit CorrectedCollectionManagerBase(std::string configKey,
                                          CorrectionManager *correctionManager = nullptr);

  virtual void bindCollectionSpec(const CorrectedCollectionSpec &spec) = 0;
  virtual std::vector<std::string> getVariationNames() const = 0;
  virtual std::string objectLabel() const = 0;
  virtual void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) = 0;

  IDataFrameProvider &dataManager() const { return *dataManager_m; }
  IConfigurationProvider &configManager() const { return *configManager_m; }
  ISystematicManager &systematicManager() const { return *systematicManager_m; }
  CorrectionManager *correctionManager() const { return correctionManager_m; }

private:
  CorrectedCollectionSpec parseSpec(const std::string &configFile) const;
  void defineAutoInputCollection(const CorrectedCollectionSpec &spec);
  void applyWorkflowConfig(const CorrectedCollectionSpec &spec);

  std::string configKey_m;
  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  ILogger *logger_m = nullptr;
  CorrectionManager *correctionManager_m = nullptr;

  CorrectedCollectionSpec spec_m;
  bool configured_m = false;
  bool systematicsRegistered_m = false;
};

class CorrectedJetCollectionManager : public CorrectedCollectionManagerBase {
public:
  explicit CorrectedJetCollectionManager(JetEnergyScaleManager &manager,
                                         CorrectionManager *correctionManager = nullptr);

  static std::shared_ptr<CorrectedJetCollectionManager> create(
      Analyzer &an,
      const std::string &role = "correctedJetCollectionManager",
      const std::string &jetManagerRole = "jetEnergyScaleManager",
      const std::string &correctionManagerRole = "correctionManager");

  std::string type() const override { return "CorrectedJetCollectionManager"; }

protected:
  void bindCollectionSpec(const CorrectedCollectionSpec &spec) override;
  std::vector<std::string> getVariationNames() const override;
  std::string objectLabel() const override { return "jet"; }
  void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) override;

private:
  JetEnergyScaleManager &manager_m;
};

class CorrectedFatJetCollectionManager : public CorrectedCollectionManagerBase {
public:
  explicit CorrectedFatJetCollectionManager(JetEnergyScaleManager &manager,
                                            CorrectionManager *correctionManager = nullptr);

  static std::shared_ptr<CorrectedFatJetCollectionManager> create(
      Analyzer &an,
      const std::string &role = "correctedFatJetCollectionManager",
      const std::string &jetManagerRole = "fatJetEnergyScaleManager",
      const std::string &correctionManagerRole = "correctionManager");

  std::string type() const override { return "CorrectedFatJetCollectionManager"; }

protected:
  void bindCollectionSpec(const CorrectedCollectionSpec &spec) override;
  std::vector<std::string> getVariationNames() const override;
  std::string objectLabel() const override { return "fatjet"; }
  void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) override;

private:
  JetEnergyScaleManager &manager_m;
};

class CorrectedElectronCollectionManager : public CorrectedCollectionManagerBase {
public:
  explicit CorrectedElectronCollectionManager(ElectronEnergyScaleManager &manager,
                                              CorrectionManager *correctionManager = nullptr);

  static std::shared_ptr<CorrectedElectronCollectionManager> create(
      Analyzer &an,
      const std::string &role = "correctedElectronCollectionManager",
      const std::string &managerRole = "electronEnergyScaleManager",
      const std::string &correctionManagerRole = "correctionManager");

  std::string type() const override { return "CorrectedElectronCollectionManager"; }

protected:
  void bindCollectionSpec(const CorrectedCollectionSpec &spec) override;
  std::vector<std::string> getVariationNames() const override;
  std::string objectLabel() const override { return "electron"; }
  void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) override;

private:
  ElectronEnergyScaleManager &manager_m;
};

class CorrectedMuonCollectionManager : public CorrectedCollectionManagerBase {
public:
  explicit CorrectedMuonCollectionManager(MuonRochesterManager &manager,
                                          CorrectionManager *correctionManager = nullptr);

  static std::shared_ptr<CorrectedMuonCollectionManager> create(
      Analyzer &an,
      const std::string &role = "correctedMuonCollectionManager",
      const std::string &managerRole = "muonRochesterManager",
      const std::string &correctionManagerRole = "correctionManager");

  std::string type() const override { return "CorrectedMuonCollectionManager"; }

protected:
  void bindCollectionSpec(const CorrectedCollectionSpec &spec) override;
  std::vector<std::string> getVariationNames() const override;
  std::string objectLabel() const override { return "muon"; }
  void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) override;

private:
  MuonRochesterManager &manager_m;
};

class CorrectedTauCollectionManager : public CorrectedCollectionManagerBase {
public:
  explicit CorrectedTauCollectionManager(TauEnergyScaleManager &manager,
                                         CorrectionManager *correctionManager = nullptr);

  static std::shared_ptr<CorrectedTauCollectionManager> create(
      Analyzer &an,
      const std::string &role = "correctedTauCollectionManager",
      const std::string &managerRole = "tauEnergyScaleManager",
      const std::string &correctionManagerRole = "correctionManager");

  std::string type() const override { return "CorrectedTauCollectionManager"; }

protected:
  void bindCollectionSpec(const CorrectedCollectionSpec &spec) override;
  std::vector<std::string> getVariationNames() const override;
  std::string objectLabel() const override { return "tau"; }
  void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) override;

private:
  TauEnergyScaleManager &manager_m;
};

class CorrectedPhotonCollectionManager : public CorrectedCollectionManagerBase {
public:
  explicit CorrectedPhotonCollectionManager(PhotonEnergyScaleManager &manager,
                                            CorrectionManager *correctionManager = nullptr);

  static std::shared_ptr<CorrectedPhotonCollectionManager> create(
      Analyzer &an,
      const std::string &role = "correctedPhotonCollectionManager",
      const std::string &managerRole = "photonEnergyScaleManager",
      const std::string &correctionManagerRole = "correctionManager");

  std::string type() const override { return "CorrectedPhotonCollectionManager"; }

protected:
  void bindCollectionSpec(const CorrectedCollectionSpec &spec) override;
  std::vector<std::string> getVariationNames() const override;
  std::string objectLabel() const override { return "photon"; }
  void applyWorkflowAction(
      const std::unordered_map<std::string, std::string> &action,
      const std::unordered_map<std::string, std::string> &localValues) override;

private:
  PhotonEnergyScaleManager &manager_m;
};

#endif