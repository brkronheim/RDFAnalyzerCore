#include <CounterService.h>
#include <RtypesCore.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <NullOutputSink.h>
#include <TFile.h>
#include <TH1D.h>
#include <TObject.h>
#include <chrono>
#include <iostream>
#include <sstream>
#include <iomanip>

void CounterService::initialize(ManagerContext& ctx) {
  ctx_m = &ctx;

  const auto& configMap = ctx.config.getConfigMap();

  auto get_config_value = [&](std::initializer_list<const char*> keys) -> std::string {
    for (const char* key : keys) {
      auto it = configMap.find(key);
      if (it != configMap.end() && !it->second.empty()) {
        return it->second;
      }
    }
    return "";
  };

  sampleName_m = get_config_value({"sample", "name", "sample_type", "stitch_id", "type", "dtype", "process", "group"});
  if (sampleName_m.empty()) {
    sampleName_m = "unknown";
  }

  if (configMap.find("counterWeightBranch") != configMap.end()) {
    weightBranch_m = configMap.at("counterWeightBranch");
  }

  if (configMap.find("counterIntWeightBranch") != configMap.end()) {
    intWeightBranch_m = configMap.at("counterIntWeightBranch");
  }
  preFilterDf_m = ctx.data.getDataFrame();
  countResult = preFilterDf_m->Count();

  if (!weightBranch_m.empty()) {
    weightSumResult = preFilterDf_m->Sum<Float_t>(weightBranch_m);
    weightSignSumResult = preFilterDf_m->Define("__counter_sign_weight", [](Float_t w) {
      return Int_t((w > 0.0) - (w < 0.0));
    }, {weightBranch_m}).Sum<Int_t>("__counter_sign_weight");
  }


  // record start time for processing-speed measurement
  startTime_m = std::chrono::steady_clock::now();
}

void CounterService::bookIntWeightHistogram(ROOT::RDF::RNode df,
                                            const std::string& branch,
                                            int nBins, double low, double high) {
  if (filtersApplied_m) {
    const std::string msg =
        "CounterService: bookIntWeightHistogram('" + branch +
        "') called after filters have already been applied. "
        "Counter histograms are intended to run over all events. "
        "Proceed with caution.";
    if (ctx_m) {
      ctx_m->logger.log(ILogger::Level::Warn, msg);
    } else {
      std::cerr << "[WARNING] " << msg << std::endl;
    }
  }

  intWeightHistBranch_m = branch;

  const std::string histName  = "counter_intWeightSum_"     + sampleName_m;
  const std::string histTitle = "Counter intWeightSum;"     + branch + ";sumWeights";
  ROOT::RDF::TH1DModel model(histName.c_str(), histTitle.c_str(), nBins, low, high);

  if (!weightBranch_m.empty()) {
    intWeightHistResult_m = df.Histo1D(model, branch, weightBranch_m);
  } else {
    intWeightHistResult_m = df.Histo1D(model, branch);
  }

  // Book companion sign-weight-per-bin histogram when a weight branch is available
  if (!weightBranch_m.empty()) {
    const std::string signHistName  = "counter_intWeightSignSum_" + sampleName_m;
    const std::string signHistTitle = "Counter intWeightSignSum;" + branch + ";sumSignWeights";
    ROOT::RDF::TH1DModel signModel(signHistName.c_str(), signHistTitle.c_str(), nBins, low, high);

    // Define the sign-weight column on the df that already has the int branch
    auto signDf = df.Define("__counter_int_sign_weight", [](Float_t w) {
      return Int_t((w > 0.0) - (w < 0.0));
    }, {weightBranch_m});
    intWeightSignHistResult_m = signDf.Histo1D(signModel, branch, "__counter_int_sign_weight");
  }
}

void CounterService::onPreFilter(ROOT::RDF::RNode& df) {
  // Auto-book int-weight histogram when counterIntWeightBranch is configured
  // and the histogram has not been booked already via bookIntWeightHistogram().
  // Must happen before filtersApplied_m is set to avoid the "called after
  // filters" warning inside bookIntWeightHistogram.
  if (!intWeightBranch_m.empty() && !intWeightHistResult_m.has_value()) {
    int    nBins = 10;
    double low   = -0.5;
    double high  = 9.5;

    if (ctx_m) {
      const auto& configMap = ctx_m->config.getConfigMap();
      auto it_bins = configMap.find("counterIntWeightBranchNBins");
      auto it_low  = configMap.find("counterIntWeightBranchMin");
      auto it_high = configMap.find("counterIntWeightBranchMax");
      if (it_bins != configMap.end()) {
        try { nBins = std::stoi(it_bins->second); } catch (...) {}
      }
      if (it_low != configMap.end()) {
        try { low = std::stod(it_low->second); } catch (...) {}
      }
      if (it_high != configMap.end()) {
        try { high = std::stod(it_high->second); } catch (...) {}
      }
    }

    bookIntWeightHistogram(df, intWeightBranch_m, nBins, low, high);
  }

  filtersApplied_m = true;
}

void CounterService::finalize(ROOT::RDF::RNode& df) {
  
  if (!ctx_m) {
    return;
  }
  
  if (dynamic_cast<NullOutputSink*>(&ctx_m->metaSink) != nullptr) {
    throw std::runtime_error("CounterService: meta output sink is null");
  }

  unsigned long long selectedCountValue = countResult.GetValue();

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
                    " processingSpeed=" + format_khz(selectedCountValue) + " kHz (elapsed=" + ss_elapsed.str() + " s)");

  if (!weightBranch_m.empty() && weightSignSumResult) {
    auto signWeightValue = weightSignSumResult.GetValue();
    ctx_m->logger.log(ILogger::Level::Info,
                      "CounterService: sample=" + sampleName_m +
                      " weightSignSum(" + weightBranch_m + ")=" + std::to_string(signWeightValue));
  }

  if (weightSumResult) {
    auto weightSumValue = weightSumResult.GetValue();
    ctx_m->logger.log(ILogger::Level::Info,
                      "CounterService: sample=" + sampleName_m +
                      " weightSum(" + weightBranch_m + ")=" + std::to_string(weightSumValue));
  }

  std::string fileName = ctx_m->metaSink.resolveOutputFile(ctx_m->config, OutputChannel::Meta);
  if (!fileName.empty()) {
    TFile outFile(fileName.c_str(), "UPDATE");
    if (outFile.IsZombie()) {
      ctx_m->logger.log(ILogger::Level::Error,
                        "CounterService: failed to open meta output file: " + fileName);
      return;
    }

    if (!weightBranch_m.empty() && weightSignSumResult) {
      auto signWeightValue = weightSignSumResult.GetValue();
      TH1D weightSignSumHist(("counter_weightSignSum_" + sampleName_m).c_str(), ("Counter weightSignSum;" + weightBranch_m + ";sumSignWeights").c_str(), 1, 0, 1);
      weightSignSumHist.SetBinContent(1, signWeightValue);
      weightSignSumHist.SetDirectory(&outFile);
      weightSignSumHist.Write("", TObject::kOverwrite);
    }

    if (weightSumResult) {
      auto weightSumValue = weightSumResult.GetValue();
      TH1D weightSumHist(("counter_weightSum_" + sampleName_m).c_str(),
                         ("Counter weightSum;" + weightBranch_m + ";sumWeights").c_str(),
                         1, 0, 1);
      weightSumHist.SetBinContent(1, weightSumValue);
      weightSumHist.SetDirectory(&outFile);
      weightSumHist.Write("", TObject::kOverwrite);
    }

    // Write pre-booked int-weight histograms (booked via bookIntWeightHistogram)
    if (intWeightHistResult_m.has_value()) {
      auto histPtr = (*intWeightHistResult_m).GetPtr();
      histPtr->SetDirectory(&outFile);
      histPtr->Write(histPtr->GetName(), TObject::kOverwrite);
      ctx_m->logger.log(ILogger::Level::Info,
                        "CounterService: wrote intWeightSum histogram '" +
                        std::string(histPtr->GetName()) + "'");
    }
    if (intWeightSignHistResult_m.has_value()) {
      auto signHistPtr = (*intWeightSignHistResult_m).GetPtr();
      signHistPtr->SetDirectory(&outFile);
      signHistPtr->Write(signHistPtr->GetName(), TObject::kOverwrite);
      ctx_m->logger.log(ILogger::Level::Info,
                        "CounterService: wrote intWeightSignSum histogram '" +
                        std::string(signHistPtr->GetName()) + "'");
    }
    outFile.Close();
  }
  
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries()
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string>
CounterService::collectProvenanceEntries() const {
  return {
      {"service.counter.sample",        sampleName_m},
      {"service.counter.weight_branch", weightBranch_m},
  };
}
