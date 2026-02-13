#ifndef PLOTTINGUTILITY_H_INCLUDED
#define PLOTTINGUTILITY_H_INCLUDED

#include <TH1.h>
#include <memory>
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
};

class PlottingUtility {
public:
  PlotResult makeStackPlot(const PlotRequest& request) const;
  std::vector<PlotResult> makeStackPlots(const std::vector<PlotRequest>& requests,
                                         bool parallel = false) const;

  static std::unique_ptr<TH1D> computeRatioHistogram(const TH1& numerator,
                                                     const TH1& denominator,
                                                     const std::string& name = "ratio");
};

#endif // PLOTTINGUTILITY_H_INCLUDED
