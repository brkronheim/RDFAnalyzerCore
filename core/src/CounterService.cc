#include <CounterService.h>
#include <RtypesCore.h>
#include <api/IConfigurationProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <NullOutputSink.h>
#include <algorithm>
#include <TFile.h>
#include <TH1D.h>
#include <TObject.h>
#include <chrono>
#include <sstream>
#include <iomanip>

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

  // record start time for processing-speed measurement
  startTime_m = std::chrono::steady_clock::now();
}

void CounterService::onPreFilter(ROOT::RDF::RNode& df) {
  preFilterDf_m = df;
}

void CounterService::finalize(ROOT::RDF::RNode& df) {
  if (!ctx_m) {
    return;
  }

  if (dynamic_cast<NullOutputSink*>(&ctx_m->metaSink) != nullptr) {
    throw std::runtime_error("CounterService: meta output sink is null");
  }

  ROOT::RDF::RNode targetDf = preFilterDf_m ? preFilterDf_m.value() : df;

  const auto columnNames = targetDf.GetColumnNames();
  const auto hasColumn = [&columnNames](const std::string& name) {
    return std::find(columnNames.begin(), columnNames.end(), name) != columnNames.end();
  };

  auto countResult = targetDf.Count();

  ROOT::RDF::RResultPtr<Float_t> weightSumResult;
  bool hasWeightSum = false;
  ROOT::RDF::RResultPtr<Int_t> weightSignSumResult;
  bool hasWeightSignSum = false;
  ROOT::RDF::RNode signWeightDf = targetDf;
  if (!weightBranch_m.empty()) {
    if (hasColumn(weightBranch_m)) {
      weightSumResult = targetDf.Sum<Float_t>(weightBranch_m);
      hasWeightSum = true;

      signWeightDf = targetDf.Define("__counter_sign_weight", [](Float_t w) {
        return Int_t((w > 0.0) - (w < 0.0));
      }, {weightBranch_m});
      weightSignSumResult = signWeightDf.Sum<Int_t>("__counter_sign_weight");
      hasWeightSignSum = true;
    } else {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: weight branch not found: " + weightBranch_m);
    }
  }

  ROOT::RDF::RResultPtr<Int_t> intWeightSumResult;
  bool hasIntWeightSum = false;
  if (!intWeightBranch_m.empty()) {
    if (hasColumn(intWeightBranch_m)) {
      intWeightSumResult = targetDf.Sum<Int_t>(intWeightBranch_m);
      hasIntWeightSum = true;
    } else {
      ctx_m->logger.log(ILogger::Level::Warn,
                        "CounterService: integer weight branch not found: " + intWeightBranch_m);
    }
  }

  // selected entries (pre-filter) — fall back to the provided df if no pre-filter was recorded
  unsigned long long selectedCountValue = 0;
  if (preFilterDf_m) {
    selectedCountValue = preFilterDf_m->Count().GetValue();
  } else {
    selectedCountValue = countResult.GetValue();
  }

  // plotted / final entries (the DataFrame passed to finalize)
  unsigned long long plottedCountValue = df.Count().GetValue();

  // compute elapsed time from initialize() -> finalize()
  double elapsed_s = 0.0;
  {
    const auto endTime = std::chrono::steady_clock::now();
    elapsed_s = std::chrono::duration<double>(endTime - startTime_m).count();
  }

  auto format_khz = [&](unsigned long long n) -> std::string {
    double khz = 0.0;
    if (elapsed_s > 0.0) khz = (static_cast<double>(n) / elapsed_s) / 1000.0;
    std::ostringstream sso;
    sso << std::fixed << std::setprecision(3) << khz;
    return sso.str();
  };

  std::ostringstream ss_elapsed;
  ss_elapsed << std::fixed << std::setprecision(3) << elapsed_s;

  // log counts and speeds
  ctx_m->logger.log(ILogger::Level::Info,
                    "CounterService: sample=" + sampleName_m +
                    " entries=" + std::to_string(selectedCountValue));

  ctx_m->logger.log(ILogger::Level::Info,
                    "CounterService: sample=" + sampleName_m +
                    " processingSpeed_selected=" + format_khz(selectedCountValue) + " kHz (elapsed=" + ss_elapsed.str() + " s)");

  ctx_m->logger.log(ILogger::Level::Info,
                    "CounterService: sample=" + sampleName_m +
                    " plottedEntries=" + std::to_string(plottedCountValue) +
                    " processingSpeed_plotted=" + format_khz(plottedCountValue) + " kHz (elapsed=" + ss_elapsed.str() + " s)");

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

  std::string fileName = ctx_m->metaSink.resolveOutputFile(ctx_m->config, OutputChannel::Meta);
  if (!fileName.empty()) {
    TFile outFile(fileName.c_str(), "UPDATE");
    if (outFile.IsZombie()) {
      ctx_m->logger.log(ILogger::Level::Error,
                        "CounterService: failed to open meta output file: " + fileName);
      return;
    }

    if (hasWeightSum) {
      auto weightValue = weightSumResult.GetValue();
      TH1D weightSumHist(("counter_weightSum_" + sampleName_m).c_str(), ("Counter weightSum;" + weightBranch_m + ";sumWeights").c_str(), 1, 0, 1);
      weightSumHist.SetBinContent(1, weightValue);
      weightSumHist.SetDirectory(&outFile);
      weightSumHist.Write("", TObject::kOverwrite);
    }

    if (hasWeightSignSum) {
      auto signWeightValue = weightSignSumResult.GetValue();
      TH1D weightSignSumHist(("counter_weightSignSum_" + sampleName_m).c_str(), ("Counter weightSignSum;" + weightBranch_m + ";sumSignWeights").c_str(), 1, 0, 1);
      weightSignSumHist.SetBinContent(1, signWeightValue);
      weightSignSumHist.SetDirectory(&outFile);
      weightSignSumHist.Write("", TObject::kOverwrite);
    }

    if (hasIntWeightSum) {
      if (selectedCountValue == 0) {
        ctx_m->logger.log(ILogger::Level::Info,
                          "CounterService: sample=" + sampleName_m +
                          " no entries for intWeight histogram");
      } else {
        const auto minVal = targetDf.Min<Int_t>(intWeightBranch_m).GetValue();
        const auto maxVal = targetDf.Max<Int_t>(intWeightBranch_m).GetValue();

        if (maxVal < minVal) {
          ctx_m->logger.log(ILogger::Level::Warn,
                            "CounterService: invalid intWeight range for " + intWeightBranch_m);
        } else {
          const Int_t binCount = maxVal - minVal + 1;
          if (binCount <= 0) {
            ctx_m->logger.log(ILogger::Level::Warn,
                              "CounterService: empty intWeight histogram for " + intWeightBranch_m);
          } else {
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
          }
        }
      }
    }
    outFile.Close();
  }
}
