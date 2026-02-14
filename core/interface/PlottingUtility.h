#ifndef PLOTTINGUTILITY_H_INCLUDED
#define PLOTTINGUTILITY_H_INCLUDED

#include <TH1.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct PlotProcessConfig {
  std::string directory;
  std::string histogramName;
  std::string legendLabel;
  Color_t color = kBlack;
  double scale = 1.0;
  std::string normalizationHistogram;
  bool isData = false;
};

struct PlotRequest {
  std::string metaFile;
  std::string outputFile;
  std::string title;
  std::string xAxisTitle;
  std::string yAxisTitle = "Counts";
  bool logY = false;
  bool drawRatio = true;
  std::vector<PlotProcessConfig> processes;
};

struct PlotResult {
  bool success = false;
  std::string message;
  double mcIntegral = 0.0;
  double dataIntegral = 0.0;
};

struct RatioSummary {
  std::vector<double> ratio;
  std::vector<double> error;
  std::vector<double> pull;
};

struct PCAResult {
  std::unique_ptr<TH1D> mean;
  std::unique_ptr<TH1D> up;
  std::unique_ptr<TH1D> down;
  std::vector<double> explainedVariance;
};

class PlottingUtility {
public:
  PlotResult makeStackPlot(const PlotRequest& request) const;
  std::vector<PlotResult> makeStackPlots(const std::vector<PlotRequest>& requests,
                                         bool parallel = false) const;

  static std::unique_ptr<TH1D> computeRatioHistogram(const TH1D& numerator,
                                                     const TH1D& denominator,
                                                     const std::string& name = "ratio");
  static RatioSummary computeRatioSummary(const TH1D& loHist,
                                          const TH1D& nloHist,
                                          const TH1D* covariance = nullptr,
                                          const TH1D* systematic = nullptr);
  static PCAResult computePCAEnvelope(const TH1D& nominal,
                                      const std::vector<const TH1D*>& variations,
                                      const std::string& baseName = "pca");
};

#endif // PLOTTINGUTILITY_H_INCLUDED
