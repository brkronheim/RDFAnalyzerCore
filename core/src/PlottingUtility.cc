#include <PlottingUtility.h>

#include <TCanvas.h>
#include <TDirectory.h>
#include <TFile.h>
#include <TH1D.h>
#include <THStack.h>
#include <TLegend.h>
#include <TMatrixD.h>
#include <TMatrixDSym.h>
#include <TMatrixDSymEigen.h>
#include <TPad.h>
#include <TLine.h>
#include <TROOT.h>

#include <functional>
#include <future>
#include <mutex>
#include <cmath>

namespace {
constexpr double kMinDenominator = 1e-12;
constexpr int kCanvasWidth = 900;
constexpr int kCanvasHeightWithRatio = 900;
constexpr int kCanvasHeightNoRatio = 700;
constexpr double kLegendX1 = 0.65;
constexpr double kLegendY1 = 0.68;
constexpr double kLegendX2 = 0.88;
constexpr double kLegendY2 = 0.88;

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
  if (std::abs(norm) < kMinDenominator) {
    return process.scale;
  }
  return process.scale / norm;
}

// Mutex to protect ROOT GUI/IO (TCanvas/TPad/SaveAs) which is not safe to run concurrently
static std::mutex gCanvasMutex;

} // namespace

std::unique_ptr<TH1D> PlottingUtility::computeRatioHistogram(const TH1D& numerator,
                                                             const TH1D& denominator,
                                                             const std::string& name) {
  auto ratio = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(numerator.Clone(name.c_str())));
  if (!ratio) {
    return nullptr;
  }

  ratio->SetDirectory(nullptr);
  const int binCount = ratio->GetNbinsX();
  for (int i = 1; i <= binCount; ++i) {
    const double denomValue = denominator.GetBinContent(i);
    if (std::abs(denomValue) < kMinDenominator) {
      ratio->SetBinContent(i, 0.0);
      ratio->SetBinError(i, 0.0);
      continue;
    }
    ratio->SetBinContent(i, ratio->GetBinContent(i) / denomValue);
    ratio->SetBinError(i, ratio->GetBinError(i) / denomValue);
  }
  return ratio;
}

RatioSummary PlottingUtility::computeRatioSummary(const TH1D& loHist,
                                                  const TH1D& nloHist,
                                                  const TH1D* covariance,
                                                  const TH1D* systematic) {
  RatioSummary summary;
  const int binCount = loHist.GetNbinsX();
  summary.ratio.resize(binCount, 0.0);
  summary.error.resize(binCount, 0.0);
  summary.pull.resize(binCount, 0.0);

  for (int i = 1; i <= binCount; ++i) {
    const double lo = loHist.GetBinContent(i);
    const double nlo = nloHist.GetBinContent(i);
    const double loVar = std::pow(loHist.GetBinError(i), 2.0);
    const double nloVar = std::pow(nloHist.GetBinError(i), 2.0);
    const double cov = covariance ? covariance->GetBinContent(i) : 0.0;
    const double systVar = systematic ? std::pow(systematic->GetBinContent(i), 2.0) : 0.0;

    if (std::abs(nlo) >= kMinDenominator) {
      const double ratio = lo / nlo;
      summary.ratio[i - 1] = ratio;
      const bool loValid = std::abs(lo) >= kMinDenominator;
      const double relLo = loValid ? loVar / (lo * lo) : 0.0;
      const double relNlo = nloVar / (nlo * nlo);
      const double relCov = loValid ? (2.0 * cov / (nlo * lo)) : 0.0;
      const double err2 = std::max(0.0, relLo + relNlo - relCov);
      summary.error[i - 1] = std::abs(ratio) * std::sqrt(err2);
    }

    const double denom2 = std::max(0.0, loVar + nloVar - 2.0 * cov + systVar);
    if (denom2 >= kMinDenominator) {
      summary.pull[i - 1] = (lo - nlo) / std::sqrt(denom2);
    }
  }

  return summary;
}

PCAResult PlottingUtility::computePCAEnvelope(const TH1D& nominal,
                                              const std::vector<const TH1D*>& variations,
                                              const std::string& baseName) {
  PCAResult result;
  const int binCount = nominal.GetNbinsX();
  if (variations.empty() || binCount <= 0) {
    return result;
  }

  const int nVariations = static_cast<int>(variations.size());
  TMatrixD counts(nVariations, binCount);
  for (int v = 0; v < nVariations; ++v) {
    if (!variations[v] || variations[v]->GetNbinsX() != binCount) {
      return result;
    }
    for (int b = 0; b < binCount; ++b) {
      counts(v, b) = variations[v]->GetBinContent(b + 1);
    }
  }

  std::vector<double> means(binCount, 0.0);
  std::vector<double> stddev(binCount, 0.0);
  for (int b = 0; b < binCount; ++b) {
    for (int v = 0; v < nVariations; ++v) {
      means[b] += counts(v, b);
    }
    means[b] /= static_cast<double>(nVariations);
    for (int v = 0; v < nVariations; ++v) {
      const double delta = counts(v, b) - means[b];
      stddev[b] += delta * delta;
    }
    if (nVariations > 1) {
      stddev[b] = std::sqrt(stddev[b] / static_cast<double>(nVariations - 1));
    } else {
      stddev[b] = 0.0;
    }
  }

  TMatrixD standardized(nVariations, binCount);
  for (int v = 0; v < nVariations; ++v) {
    for (int b = 0; b < binCount; ++b) {
      if (stddev[b] < kMinDenominator) {
        standardized(v, b) = 0.0;
      } else {
        standardized(v, b) = (counts(v, b) - means[b]) / stddev[b];
      }
    }
  }

  TMatrixDSym covariance(binCount);
  covariance.Zero();
  if (nVariations > 1) {
    for (int i = 0; i < binCount; ++i) {
      for (int j = 0; j < binCount; ++j) {
        double accum = 0.0;
        for (int v = 0; v < nVariations; ++v) {
          accum += standardized(v, i) * standardized(v, j);
        }
        covariance(i, j) = accum / static_cast<double>(nVariations - 1);
      }
    }
  }

  TMatrixDSymEigen eigen(covariance);
  const TVectorD eigenValues = eigen.GetEigenValues();
  const TMatrixD eigenVectors = eigen.GetEigenVectors();

  result.explainedVariance.resize(binCount, 0.0);
  double totalEigen = 0.0;
  for (int i = 0; i < binCount; ++i) {
    totalEigen += std::max(0.0, eigenValues[i]);
  }
  if (totalEigen >= kMinDenominator) {
    for (int i = 0; i < binCount; ++i) {
      result.explainedVariance[i] = std::max(0.0, eigenValues[i]) / totalEigen;
    }
  }

  std::vector<double> uncert(binCount, 0.0);
  for (int b = 0; b < binCount; ++b) {
    double variance = 0.0;
    for (int i = 0; i < binCount; ++i) {
      const double lambda = std::max(0.0, eigenValues[i]);
      variance += eigenVectors(b, i) * eigenVectors(b, i) * lambda;
    }
    uncert[b] = stddev[b] * std::sqrt(std::max(0.0, variance));
  }

  result.mean = std::unique_ptr<TH1D>(static_cast<TH1D*>(nominal.Clone((baseName + "_mean").c_str())));
  result.up = std::unique_ptr<TH1D>(static_cast<TH1D*>(nominal.Clone((baseName + "_up").c_str())));
  result.down = std::unique_ptr<TH1D>(static_cast<TH1D*>(nominal.Clone((baseName + "_down").c_str())));
  if (!result.mean || !result.up || !result.down) {
    result = PCAResult{};
    return result;
  }
  result.mean->SetDirectory(nullptr);
  result.up->SetDirectory(nullptr);
  result.down->SetDirectory(nullptr);

  for (int b = 0; b < binCount; ++b) {
    result.mean->SetBinContent(b + 1, means[b]);
    result.up->SetBinContent(b + 1, means[b] + uncert[b]);
    result.down->SetBinContent(b + 1, means[b] - uncert[b]);
    result.mean->SetBinError(b + 1, 0.0);
    result.up->SetBinError(b + 1, 0.0);
    result.down->SetBinError(b + 1, 0.0);
  }

  return result;
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
  TLegend legend(kLegendX1, kLegendY1, kLegendX2, kLegendY2);

  for (size_t processIndex = 0; processIndex < request.processes.size(); ++processIndex) {
    const auto& process = request.processes[processIndex];
    TH1* source = getHistogram(file, process);
    if (!source) {
      result.message = "Missing histogram '" + process.histogramName + "'";
      return result;
    }

    const std::string cloneName = "plot_hist_" + std::to_string(processIndex);
    auto hist = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(source->Clone(cloneName.c_str())));
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

  if (mcSum) {
    result.mcIntegral = mcSum->Integral();
  }
  if (dataHist) {
    result.dataIntegral = dataHist->Integral();
  }

  // Serialize all ROOT GUI/IO operations (TCanvas/TPad/SaveAs) because they are not
  // safe to run concurrently even when ROOT thread-safety is enabled.
  {
    std::lock_guard<std::mutex> lock(gCanvasMutex);

    // create canvas and stacked layout (top: data+MC, bottom: ratio)
    TCanvas canvas("canvas", "canvas", kCanvasWidth,
                   request.drawRatio ? kCanvasHeightWithRatio : kCanvasHeightNoRatio);

    std::unique_ptr<TPad> topPad;
    std::unique_ptr<TPad> ratioPad;
    if (request.drawRatio && dataHist && mcSum) {
      // 60% / 40% split (top / ratio) similar to the python layout
      topPad = std::make_unique<TPad>("topPad", "topPad", 0.0, 0.35, 1.0, 1.0);
      ratioPad = std::make_unique<TPad>("ratioPad", "ratioPad", 0.0, 0.0, 1.0, 0.35);
      topPad->SetBottomMargin(0.02);
      topPad->SetLeftMargin(0.12);
      topPad->SetRightMargin(0.05);
      ratioPad->SetTopMargin(0.03);
      ratioPad->SetBottomMargin(0.30);
      ratioPad->SetLeftMargin(0.12);
      ratioPad->SetRightMargin(0.05);
      topPad->Draw();
      ratioPad->Draw();
      topPad->cd();
    } else {
      canvas.SetLeftMargin(0.12);
      canvas.SetRightMargin(0.05);
    }

    // Draw stacked MC and customize appearance
    stack.Draw("hist");
    stack.GetXaxis()->SetTitle(request.xAxisTitle.c_str());
    stack.GetYaxis()->SetTitle(request.yAxisTitle.c_str());
    stack.SetTitle(request.title.c_str());
    stack.GetXaxis()->SetTitleSize(0.045);
    stack.GetYaxis()->SetTitleSize(0.045);
    stack.GetYaxis()->SetTitleOffset(1.15);
    stack.GetXaxis()->SetLabelSize(request.drawRatio && dataHist && mcSum ? 0.0 : 0.035);

    // draw MC uncertainty band (use mcSum's bin errors)
    if (mcSum) {
      mcSum->Sumw2();
      // create a copy for the uncertainty band
      auto mcUnc = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(mcSum->Clone("mc_unc")));
      if (mcUnc) {
        mcUnc->SetDirectory(nullptr);
        mcUnc->SetMarkerSize(0);
        mcUnc->SetFillColor(kGray+2);
        mcUnc->SetFillStyle(3354);
        // draw as uncertainty band on top of stack
        mcUnc->Draw("E2 same");
      }
    }

    // draw data points on top
    if (dataHist) {
      dataHist->SetMarkerStyle(20);
      dataHist->SetMarkerSize(0.9);
      dataHist->Draw("same e1");
    }

    // refine legend
    legend.SetBorderSize(0);
    legend.SetFillStyle(0);
    legend.SetTextSize(0.035);
    legend.Draw();

    // log-scale handling
    if (request.logY) {
      topPad ? topPad->cd() : canvas.cd();
      gPad->SetLogy(true);
      // ensure a positive minimum for log scale
      auto ymin = stack.GetHistogram() ? stack.GetHistogram()->GetMinimum(0.0) : 0.0;
      if (ymin <= 0.0) {
        stack.SetMinimum(1e-2);
      }
    }

    // Ratio pad + Pull pad: draw data/MC points with MC uncertainty band and a pull histogram
    std::unique_ptr<TPad> pullPad;
    if (request.drawRatio && dataHist && mcSum) {
      // create an extra pull pad beneath the ratio pad
      pullPad = std::make_unique<TPad>("pullPad", "pullPad", 0.0, 0.0, 1.0, 0.18);
      pullPad->SetTopMargin(0.05);
      pullPad->SetBottomMargin(0.30);
      pullPad->SetLeftMargin(0.12);
      pullPad->SetRightMargin(0.05);
      pullPad->Draw();

      // RATIO PAD
      ratioPad->cd();
      // relative MC uncertainty band (content = 1, error = rel error)
      auto mcRel = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(mcSum->Clone("mc_rel")));
      if (mcRel) {
        const int nBins = mcRel->GetNbinsX();
        for (int b = 1; b <= nBins; ++b) {
          const double mcVal = mcSum->GetBinContent(b);
          const double mcErr = mcSum->GetBinError(b);
          if (std::abs(mcVal) < kMinDenominator) {
            mcRel->SetBinContent(b, 1.0);
            mcRel->SetBinError(b, 0.0);
          } else {
            mcRel->SetBinContent(b, 1.0);
            mcRel->SetBinError(b, mcErr / mcVal);
          }
        }
        mcRel->SetDirectory(nullptr);
        mcRel->SetFillColor(kGray+2);
        mcRel->SetFillStyle(3354); // hatched-like pattern
        mcRel->SetLineColor(kGray+2);
        mcRel->Draw("E2");
      }

      auto ratio = computeRatioHistogram(*dataHist, *mcSum, "ratio_hist");
      if (ratio) {
        ratio->SetStats(false);
        ratio->GetYaxis()->SetTitle("Data/MC");
        ratio->GetXaxis()->SetTitle(request.xAxisTitle.c_str());
        ratio->GetYaxis()->SetTitleSize(0.12);
        ratio->GetXaxis()->SetTitleSize(0.12);
        ratio->GetYaxis()->SetLabelSize(0.10);
        ratio->GetXaxis()->SetLabelSize(0.10);
        ratio->GetXaxis()->SetTitleOffset(1.0);
        ratio->GetYaxis()->SetTitleOffset(0.35);
        // set sensible default range (close to python behaviour)
        ratio->SetMinimum(0.59);
        ratio->SetMaximum(1.41);
        ratio->Draw("e1 same");

        // horizontal line at 1
        TLine line(ratio->GetXaxis()->GetXmin(), 1.0, ratio->GetXaxis()->GetXmax(), 1.0);
        line.SetLineStyle(2);
        line.SetLineWidth(1);
        line.Draw("same");
      }

      ratioPad->SetGridy(true);

      // PULL PAD
      pullPad->cd();
      const int nBins = mcSum->GetNbinsX();
      auto pullHist = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(mcSum->Clone("pull_hist")));
      if (pullHist) {
        pullHist->SetDirectory(nullptr);
        for (int b = 1; b <= nBins; ++b) {
          const double d = dataHist->GetBinContent(b);
          const double dErr = dataHist->GetBinError(b);
          const double m = mcSum->GetBinContent(b);
          const double mErr = mcSum->GetBinError(b);
          const double denom = std::sqrt(dErr * dErr + mErr * mErr);
          const double pull = (std::abs(denom) < kMinDenominator) ? 0.0 : (d - m) / denom;
          pullHist->SetBinContent(b, pull);
          pullHist->SetBinError(b, 0.0);
        }

        // split into positive / negative for hatched bars
        auto posPull = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(pullHist->Clone("pos_pull")));
        auto negPull = std::unique_ptr<TH1D>(dynamic_cast<TH1D*>(pullHist->Clone("neg_pull")));
        posPull->SetDirectory(nullptr);
        negPull->SetDirectory(nullptr);
        for (int b = 1; b <= nBins; ++b) {
          const double v = pullHist->GetBinContent(b);
          posPull->SetBinContent(b, v > 0.0 ? v : 0.0);
          negPull->SetBinContent(b, v < 0.0 ? -v : 0.0);
        }

        posPull->SetFillColor(kRed+1);
        posPull->SetFillStyle(3354);
        posPull->SetLineColor(kRed+1);
        negPull->SetFillColor(kBlue+1);
        negPull->SetFillStyle(3354);
        negPull->SetLineColor(kBlue+1);

        // determine y-range for pulls
        double maxPull = 1.5;
        for (int b = 1; b <= nBins; ++b) {
          maxPull = std::max(maxPull, std::abs(pullHist->GetBinContent(b)) * 1.2);
        }
        pullHist->SetMaximum(maxPull);
        pullHist->SetMinimum(-maxPull);

        // draw axes and bars
        pullHist->GetYaxis()->SetTitle("(Data - MC)/Unc");
        pullHist->GetYaxis()->CenterTitle(true);
        pullHist->GetYaxis()->SetTitleSize(0.10);
        pullHist->GetYaxis()->SetLabelSize(0.09);
        pullHist->GetXaxis()->SetLabelSize(0.10);
        pullHist->Draw("axis");
        posPull->Draw("hist same");
        negPull->Draw("hist same");

        // zero line
        TLine zline(pullHist->GetXaxis()->GetXmin(), 0.0, pullHist->GetXaxis()->GetXmax(), 0.0);
        zline.SetLineWidth(1);
        zline.Draw("same");
      }
    }

    canvas.SaveAs(request.outputFile.c_str());
  }

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

  static const bool threadSafetyEnabled = []() {
    ROOT::EnableThreadSafety();
    return true;
  }();
  (void)threadSafetyEnabled;
  std::vector<std::future<PlotResult>> futures;
  futures.reserve(requests.size());
  for (size_t i = 0; i < requests.size(); ++i) {
    futures.emplace_back(std::async(std::launch::async, [this, request = std::cref(requests[i])]() {
      return makeStackPlot(request.get());
    }));
  }
  for (size_t i = 0; i < futures.size(); ++i) {
    results[i] = futures[i].get();
  }
  return results;
}
