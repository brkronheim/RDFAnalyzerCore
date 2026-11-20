#ifndef GENWEIGHTMANAGER_H_INCLUDED
#define GENWEIGHTMANAGER_H_INCLUDED

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

struct GenWeightVariationSpec {
  std::string label;
  std::string column;
  std::string source;
  int index = -1;
};

class GenWeightManager : public IPluggableManager {
public:
  static std::shared_ptr<GenWeightManager> create(
      Analyzer &an, const std::string &role = "genWeightManager");

  std::string type() const override { return "GenWeightManager"; }

  void setContext(ManagerContext &ctx) override;
  void setupFromConfigFile() override;
  void execute() override;

  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

  void defineVariationWeights();

  const std::string &nominalColumn() const { return nominalColumn_m; }
  const std::vector<GenWeightVariationSpec> &variations() const {
    return variations_m;
  }

private:
  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  ILogger *logger_m = nullptr;

  bool enableLheScale_m = true;
  bool enablePs_m = true;
  bool enableIsr_m = true;
  bool enableFsr_m = true;
  bool enablePdf_m = true;
  bool enablePdfAs_m = true;
  std::string lheScaleBranch_m = "LHEScaleWeight";
  std::string psBranch_m = "PSWeight";
  std::string pdfBranch_m = "LHEPdfWeight";
  std::string baseWeightColumn_m = "genWeight";
  std::string nominalColumn_m = "genWeight_nominal";
  int pdfFirstLhaId_m = 325300;
  int pdfCount_m = 103;
  int pdfAsUpOffset_m = -1;    ///< αₛ up offset within PDF weights (-1 = auto)
  int pdfAsDownOffset_m = -1;  ///< αₛ down offset within PDF weights (-1 = auto)
  std::string weightMode_m = "sign";

  bool materialized_m = false;
  std::vector<GenWeightVariationSpec> variations_m;

  bool hasColumn(const std::string &name) const;
  bool configBool(const std::string &key, bool fallback) const;
  int configInt(const std::string &key, int fallback) const;
  std::string configString(const std::string &key,
                           const std::string &fallback) const;
  void defineLheScaleWeights(const std::string &baseWeightCol,
                             bool useDoubleBase);
  void definePsWeights(const std::string &baseWeightCol,
                       bool useDoubleBase);
  void definePdfWeights(const std::string &baseWeightCol,
                        bool useDoubleBase);
  void definePdfAsWeights(const std::string &baseWeightCol,
                          bool useDoubleBase);
  void registerVariation(const std::string &label,
                         const std::string &column,
                         const std::string &source,
                         int index);
};

#endif // GENWEIGHTMANAGER_H_INCLUDED
