#include <PlottingUtility.h>
#include <TAxis.h>
#include <TMatrixD.h>
#include <TMatrixDSym.h>
#include <TMatrixDSymEigen.h>
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace {
constexpr double kMinDenominator = 1e-12;
void validateAxes(const TH1 &a, const TH1 &b) {
  if (a.GetDimension() != 1 || b.GetDimension() != 1 ||
      a.GetNbinsX() != b.GetNbinsX())
    throw std::invalid_argument("Histogram dimensions/binning disagree");
  for (int i = 1; i <= a.GetNbinsX() + 1; ++i) {
    const double x = a.GetXaxis()->GetBinLowEdge(i),
                 y = b.GetXaxis()->GetBinLowEdge(i);
    if (std::abs(x - y) > 1e-9 * std::max({1.0, std::abs(x), std::abs(y)}))
      throw std::invalid_argument("Histogram bin edges disagree");
  }
}
} // namespace

std::unique_ptr<TH1D> PlottingUtility::computeRatioHistogram(
    const TH1D &numerator, const TH1D &denominator, const std::string &name) {
  validateAxes(numerator, denominator);
  auto ratio = std::unique_ptr<TH1D>(
      dynamic_cast<TH1D *>(numerator.Clone(name.c_str())));
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
    ratio->SetBinError(i, ratio->GetBinError(i) / std::abs(denomValue));
  }
  return ratio;
}

RatioSummary PlottingUtility::computeRatioSummary(const TH1D &loHist,
                                                  const TH1D &nloHist,
                                                  const TH1D *covariance,
                                                  const TH1D *systematic) {
  validateAxes(loHist, nloHist);
  if (covariance)
    validateAxes(loHist, *covariance);
  if (systematic)
    validateAxes(loHist, *systematic);
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
    const double systVar =
        systematic ? std::pow(systematic->GetBinContent(i), 2.0) : 0.0;
    if (!std::isfinite(lo) || !std::isfinite(nlo) || !std::isfinite(loVar) ||
        !std::isfinite(nloVar) || !std::isfinite(cov) ||
        !std::isfinite(systVar) ||
        std::abs(cov) > std::sqrt(loVar * nloVar) + 1e-9)
      throw std::invalid_argument("Invalid ratio inputs or covariance");

    if (std::abs(nlo) >= kMinDenominator) {
      const double ratio = lo / nlo;
      summary.ratio[i - 1] = ratio;
      const double d2 = nlo * nlo;
      const double variance = loVar / d2 + lo * lo * nloVar / (d2 * d2) -
                              2.0 * lo * cov / (d2 * nlo);
      summary.error[i - 1] = std::sqrt(std::max(0.0, variance));
    }

    const double denom2 = std::max(0.0, loVar + nloVar - 2.0 * cov + systVar);
    if (denom2 >= kMinDenominator) {
      summary.pull[i - 1] = (lo - nlo) / std::sqrt(denom2);
    }
  }

  return summary;
}

PCAResult
PlottingUtility::computePCAEnvelope(const TH1D &nominal,
                                    const std::vector<const TH1D *> &variations,
                                    const std::string &baseName) {
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

  result.mean = std::unique_ptr<TH1D>(
      static_cast<TH1D *>(nominal.Clone((baseName + "_mean").c_str())));
  result.up = std::unique_ptr<TH1D>(
      static_cast<TH1D *>(nominal.Clone((baseName + "_up").c_str())));
  result.down = std::unique_ptr<TH1D>(
      static_cast<TH1D *>(nominal.Clone((baseName + "_down").c_str())));
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

PlotResult PlottingUtility::makeStackPlot(const PlotRequest &request) const {
  Session session;
  return session.render(request);
}

std::vector<PlotResult>
PlottingUtility::makeStackPlots(const std::vector<PlotRequest> &requests,
                                bool parallel) const {
  PlotBatchOptions options;
  options.workers = parallel ? 4 : 1;
  Session session(options);
  return session.renderBatch(requests);
}
