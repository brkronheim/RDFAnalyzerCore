#include <PlottingUtility.h>

#include <TCanvas.h>
#include <TDirectory.h>
#include <TFile.h>
#include <TH1D.h>
#include <THStack.h>
#include <TLegend.h>
#include <TPad.h>

#include <future>

namespace {

TH1* getHistogram(TFile& file, const PlotProcessConfig& process) {
  if (process.directory.empty()) {
    return dynamic_cast<TH1*>(file.Get(process.histogramName.c_str()));
  }

  TDirectory* directory = file.GetDirectory(process.directory.c_str());
  if (!directory) {
    return nullptr;
  }
  return dynamic_cast<TH1*>(directory->Get(process.histogramName.c_str()));
}

double getNormalizationScale(TFile& file, const PlotProcessConfig& process) {
  if (process.normalizationHistogram.empty()) {
    return process.scale;
  }
  auto* normHist = dynamic_cast<TH1*>(file.Get(process.normalizationHistogram.c_str()));
  if (!normHist) {
    return process.scale;
  }

  const double norm = normHist->GetBinContent(1);
  if (norm == 0.0) {
    return process.scale;
  }
  return process.scale / norm;
}

} // namespace

std::unique_ptr<TH1D> PlottingUtility::computeRatioHistogram(const TH1& numerator,
                                                             const TH1& denominator,
                                                             const std::string& name) {
  auto ratio = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(numerator.Clone(name.c_str())));
  if (!ratio) {
    return nullptr;
  }

  ratio->SetDirectory(nullptr);
  const int binCount = ratio->GetNbinsX();
  for (int i = 1; i <= binCount; ++i) {
    const double denomValue = denominator.GetBinContent(i);
    if (denomValue == 0.0) {
      ratio->SetBinContent(i, 0.0);
      ratio->SetBinError(i, 0.0);
      continue;
    }
    ratio->SetBinContent(i, ratio->GetBinContent(i) / denomValue);
    ratio->SetBinError(i, ratio->GetBinError(i) / denomValue);
  }
  return ratio;
}

PlotResult PlottingUtility::makeStackPlot(const PlotRequest& request) const {
  PlotResult result;
  TFile file(request.metaFile.c_str(), "READ");
  if (file.IsZombie()) {
    result.message = "Unable to open meta file: " + request.metaFile;
    return result;
  }

  THStack stack("stack", request.title.c_str());
  std::vector<std::unique_ptr<TH1D>> mcHists;
  std::unique_ptr<TH1D> dataHist;
  std::unique_ptr<TH1D> mcSum;
  TLegend legend(0.65, 0.68, 0.88, 0.88);

  for (const auto& process : request.processes) {
    TH1* source = getHistogram(file, process);
    if (!source) {
      result.message = "Missing histogram '" + process.histogramName + "'";
      return result;
    }

    auto hist = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(source->Clone()));
    if (!hist) {
      result.message = "Histogram is not TH1D: '" + process.histogramName + "'";
      return result;
    }
    hist->SetDirectory(nullptr);
    hist->Scale(getNormalizationScale(file, process));
    hist->SetLineColor(process.color);
    hist->SetMarkerColor(process.color);

    if (process.isData) {
      hist->SetMarkerStyle(20);
      dataHist = std::move(hist);
      legend.AddEntry(dataHist.get(), process.legendLabel.c_str(), "lep");
      continue;
    }

    hist->SetFillColor(process.color);
    hist->SetLineWidth(1);

    if (!mcSum) {
      mcSum = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(hist->Clone("mc_sum")));
      if (!mcSum) {
        result.message = "Failed to create MC sum histogram";
        return result;
      }
      mcSum->SetDirectory(nullptr);
    } else {
      mcSum->Add(hist.get());
    }

    legend.AddEntry(hist.get(), process.legendLabel.c_str(), "f");
    mcHists.push_back(std::move(hist));
    stack.Add(mcHists.back().get(), "hist");
  }

  if (request.processes.empty()) {
    result.message = "No processes provided";
    return result;
  }

  TCanvas canvas("canvas", "canvas", 900, request.drawRatio ? 900 : 700);
  std::unique_ptr<TPad> topPad;
  std::unique_ptr<TPad> ratioPad;
  if (request.drawRatio && dataHist && mcSum) {
    topPad = std::make_unique<TPad>("topPad", "topPad", 0.0, 0.3, 1.0, 1.0);
    ratioPad = std::make_unique<TPad>("ratioPad", "ratioPad", 0.0, 0.0, 1.0, 0.3);
    topPad->SetBottomMargin(0.02);
    ratioPad->SetTopMargin(0.03);
    ratioPad->SetBottomMargin(0.30);
    topPad->Draw();
    ratioPad->Draw();
    topPad->cd();
  }

  stack.Draw("hist");
  stack.GetXaxis()->SetTitle(request.xAxisTitle.c_str());
  stack.GetYaxis()->SetTitle(request.yAxisTitle.c_str());
  stack.SetTitle(request.title.c_str());

  if (request.logY) {
    gPad->SetLogy(true);
  }

  if (dataHist) {
    dataHist->Draw("same e1");
  }
  legend.Draw();

  if (ratioPad && dataHist && mcSum) {
    ratioPad->cd();
    auto ratio = computeRatioHistogram(*dataHist, *mcSum, "ratio_hist");
    if (ratio) {
      ratio->SetStats(false);
      ratio->GetYaxis()->SetTitle("Data/MC");
      ratio->GetXaxis()->SetTitle(request.xAxisTitle.c_str());
      ratio->SetMinimum(0.0);
      ratio->SetMaximum(2.0);
      ratio->Draw("e1");
    }
  }

  canvas.SaveAs(request.outputFile.c_str());
  result.success = true;
  return result;
}

std::vector<PlotResult>
PlottingUtility::makeStackPlots(const std::vector<PlotRequest>& requests,
                                bool parallel) const {
  std::vector<PlotResult> results(requests.size());
  if (!parallel || requests.size() < 2) {
    for (size_t i = 0; i < requests.size(); ++i) {
      results[i] = makeStackPlot(requests[i]);
    }
    return results;
  }

  std::vector<std::future<PlotResult>> futures;
  futures.reserve(requests.size());
  for (const auto& request : requests) {
    futures.emplace_back(std::async(std::launch::async, [this, request]() {
      return makeStackPlot(request);
    }));
  }
  for (size_t i = 0; i < futures.size(); ++i) {
    results[i] = futures[i].get();
  }
  return results;
}
