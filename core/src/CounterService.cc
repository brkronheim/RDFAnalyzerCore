#include <CounterService.h>
#include <api/IConfigurationProvider.h>
#include <api/ILogger.h>
#include <algorithm>
#include <TFile.h>
#include <TH1D.h>
#include <TObject.h>

void CounterService::initialize(ManagerContext& ctx) {
  ctx_m = &ctx;

  const auto& configMap = ctx.config.getConfigMap();

  if (configMap.find("sample") != configMap.end()) {
    sampleName_m = configMap.at("sample");
  } else if (configMap.find("type") != configMap.end()) {
    sampleName_m = configMap.at("type");
  } else {
    sampleName_m = "unknown";
  }

  if (configMap.find("counterWeightBranch") != configMap.end()) {
    weightBranch_m = configMap.at("counterWeightBranch");
  }

  if (configMap.find("counterIntWeightBranch") != configMap.end()) {
    intWeightBranch_m = configMap.at("counterIntWeightBranch");
  }
}

void CounterService::onPreFilter(ROOT::RDF::RNode& df) {
  preFilterDf_m = df;
}

void CounterService::finalize(ROOT::RDF::RNode& df) {
  if (!ctx_m) {
    return;
  }

  ROOT::RDF::RNode targetDf = preFilterDf_m ? preFilterDf_m.value() : df;

  const auto columnNames = targetDf.GetColumnNames();
  const auto hasColumn = [&columnNames](const std::string& name) {
    return std::find(columnNames.begin(), columnNames.end(), name) != columnNames.end();
  };

  auto countResult = targetDf.Count();

  ROOT::RDF::RResultPtr<double> weightSumResult;
  bool hasWeightSum = false;
  ROOT::RDF::RResultPtr<long long> weightSignSumResult;
  bool hasWeightSignSum = false;
  ROOT::RDF::RNode signWeightDf = targetDf;
  if (!weightBranch_m.empty()) {
    if (hasColumn(weightBranch_m)) {
      weightSumResult = targetDf.Sum<double>(weightBranch_m);
      hasWeightSum = true;

      signWeightDf = targetDf.Define("__counter_sign_weight", [](double w) {
        return static_cast<long long>((w > 0.0) - (w < 0.0));
      }, {weightBranch_m});
      weightSignSumResult = signWeightDf.Sum<long long>("__counter_sign_weight");
      hasWeightSignSum = true;
    } else {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: weight branch not found: " + weightBranch_m);
    }
  }

  ROOT::RDF::RResultPtr<long long> intWeightSumResult;
  bool hasIntWeightSum = false;
  if (!intWeightBranch_m.empty()) {
    if (hasColumn(intWeightBranch_m)) {
      intWeightSumResult = targetDf.Sum<long long>(intWeightBranch_m);
      hasIntWeightSum = true;
    } else {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: integer weight branch not found: " + intWeightBranch_m);
    }
  }

  auto countValue = countResult.GetValue();

  ctx_m->logger.log(ILogger::Level::Info,
                    "CounterService: sample=" + sampleName_m +
                    " entries=" + std::to_string(countValue));

  if (hasWeightSum) {
    auto weightValue = weightSumResult.GetValue();
    ctx_m->logger.log(ILogger::Level::Info,
                      "CounterService: sample=" + sampleName_m +
                      " weightSum(" + weightBranch_m + ")=" + std::to_string(weightValue));
  }

  if (hasWeightSignSum) {
    auto signWeightValue = weightSignSumResult.GetValue();
    ctx_m->logger.log(ILogger::Level::Info,
                      "CounterService: sample=" + sampleName_m +
                      " weightSignSum(" + weightBranch_m + ")=" + std::to_string(signWeightValue));
  }

  if (hasIntWeightSum) {
    auto intWeightValue = intWeightSumResult.GetValue();
    ctx_m->logger.log(ILogger::Level::Info,
                      "CounterService: sample=" + sampleName_m +
                      " intWeightSum(" + intWeightBranch_m + ")=" + std::to_string(intWeightValue));
  }

  if (!intWeightBranch_m.empty() && hasColumn(intWeightBranch_m)) {
    if (countValue == 0) {
      ctx_m->logger.log(ILogger::Level::Info,
                        "CounterService: sample=" + sampleName_m +
                        " no entries for intWeight histogram");
      return;
    }

    const auto minVal = targetDf.Min<long long>(intWeightBranch_m).GetValue();
    const auto maxVal = targetDf.Max<long long>(intWeightBranch_m).GetValue();

    if (maxVal < minVal) {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: invalid intWeight range for " + intWeightBranch_m);
      return;
    }

    const long long binCount = maxVal - minVal + 1;
    if (binCount <= 0) {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: empty intWeight histogram for " + intWeightBranch_m);
      return;
    }

    std::string fileName = ctx_m->config.get("metaFile");
    if (fileName.empty()) {
      fileName = ctx_m->config.get("saveFile");
    }
    if (fileName.empty()) {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: metaFile/saveFile not set, skipping intWeight histogram");
      return;
    }

    const std::string histName = "counter_intWeightSum_" + sampleName_m;
    const std::string histTitle = "Counter intWeightSum;" + intWeightBranch_m + ";sumWeights";
    ROOT::RDF::TH1DModel model(histName.c_str(), histTitle.c_str(),
                               static_cast<int>(binCount),
                               static_cast<double>(minVal) - 0.5,
                               static_cast<double>(maxVal) + 0.5);

    ROOT::RDF::RResultPtr<TH1D> histResult;
    if (!weightBranch_m.empty() && hasColumn(weightBranch_m)) {
      histResult = targetDf.Histo1D(model, intWeightBranch_m, weightBranch_m);
    } else {
      if (!weightBranch_m.empty() && !hasColumn(weightBranch_m)) {
        ctx_m->logger.log(ILogger::Level::Warn,
                          "CounterService: weight branch missing, using unit weights for intWeight histogram");
      }
      histResult = targetDf.Histo1D(model, intWeightBranch_m);
    }

    auto histPtr = histResult.GetPtr();
    TFile outFile(fileName.c_str(), "UPDATE");
    if (outFile.IsZombie()) {
      ctx_m->logger.log(ILogger::Level::Error,
                        "CounterService: failed to open meta output file: " + fileName);
      return;
    }
    histPtr->SetDirectory(&outFile);
    histPtr->Write(histName.c_str(), TObject::kOverwrite);

    if (hasWeightSignSum) {
      const std::string signHistName = "counter_intWeightSignSum_" + sampleName_m;
      const std::string signHistTitle = "Counter intWeightSignSum;" + intWeightBranch_m + ";sumSignWeights";
      ROOT::RDF::TH1DModel signModel(signHistName.c_str(), signHistTitle.c_str(),
                                     static_cast<int>(binCount),
                                     static_cast<double>(minVal) - 0.5,
                                     static_cast<double>(maxVal) + 0.5);
      auto signHistResult = signWeightDf.Histo1D(signModel, intWeightBranch_m, "__counter_sign_weight");
      auto signHistPtr = signHistResult.GetPtr();
      signHistPtr->SetDirectory(&outFile);
      signHistPtr->Write(signHistName.c_str(), TObject::kOverwrite);
    }
    outFile.Close();
  }
}
