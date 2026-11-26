#ifndef PLOTTINGUTILITY_H_INCLUDED
#define PLOTTINGUTILITY_H_INCLUDED

#include <TH1.h>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

enum class PlotKind { Stack, Overlay, Heatmap };
enum class PlotPanel { Ratio, Pull, SignalBackground, Significance };
enum class PlotFlow { Drop, Fold };
enum class PlotExperiment { CMS, ATLAS, LHCb, ALICE, Custom };

struct PlotVariation {
  // Equal names denote correlated nuisances. Empty up/down means nominal.
  std::string name;
  std::vector<double> up;
  std::vector<double> down;
};

struct PlotPanelConfig {
  PlotPanel type = PlotPanel::Ratio;
  double minimum = 0.5;
  double maximum = 1.5;
  bool autoRange = true;
};

struct PlotStyle {
  int width = 1000;
  int height =
      0; // Automatic: preserve readable panels and header/legend space.
  // Zero selects readable sizes scaled to canvas width (24/28/22 px at
  // 1000 px). Positive values are explicit pixels, consistent across pads.
  double labelSize = 0;
  double titleSize = 0;
  double legendSize = 0;
  double leftMargin = 0.14;
  double rightMargin = 0.045;
  int legendColumns = 2;
  bool grid = false;
  bool legendInside = true;
  double legendWidth = 0.82;        // Fraction of the main frame's width.
  double annotationBoxWidth = 0.72; // Selection/title box, as a frame fraction.
};

struct PlotProcessConfig {
  std::string directory;
  std::string histogramName;
  std::string legendLabel;
  Color_t color = kBlack;
  double scale = 1.0;
  std::string normalizationHistogram;
  bool isData = false;
  std::vector<std::string> files; // Empty: use request.metaFile.
  bool isSignal = false;
  bool overlay =
      false; // Excluded from the stack/denominator, drawn as a curve.
  double displayScale = 1.0;  // Signal outline/overlay only; never changes S/B.
  std::string group;          // Nonempty equal groups aggregate before drawing.
  std::vector<double> values; // In-memory input, with request.binEdges.
  std::vector<double> variances;
  std::vector<double> errorLow; // Optional asymmetric absolute data errors.
  std::vector<double> errorHigh;
  std::vector<PlotVariation> variations;
  // Embedded-schema axis role -> selected category labels. Empty selects all.
  std::map<std::string, std::vector<std::string>> selections;
  bool readSystematics = false;
  std::string colorHex; // Optional #RRGGBB, preferred over a ROOT color index.
  // Absolute covariance with the overlay reference, after scale, before
  // density.
  std::vector<double> referenceCovariance;
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
  PlotKind kind = PlotKind::Stack;
  std::vector<double> binEdges;
  std::vector<std::string> binLabels;
  std::vector<PlotPanelConfig>
      panels; // Empty: legacy drawRatio selects ratio+pull.
  std::vector<bool> blindBins;
  std::vector<double> systematicLow; // Additional absolute prediction errors.
  std::vector<double> systematicHigh;
  std::string experimentLabel;
  std::string luminosityLabel;
  std::vector<std::string> annotations;
  PlotStyle style;
  bool density = false;
  bool strictNormalization = true;
  bool signalOverlay = true;
  PlotFlow flow = PlotFlow::Drop;
  std::vector<std::string> additionalOutputs;
  // Heatmap, row-major with x varying fastest. Also accepts a TH2 file input
  // or a sparse observable x heatmapYRole projection.
  std::vector<double> matrix;
  std::vector<double> yBinEdges;
  std::vector<std::string> yBinLabels;
  std::string heatmapYRole = "control";
  std::string zAxisTitle = "Weighted events";
  std::vector<double> horizontalSeparators;
  bool symmetricColorRange = true;
  bool logZ = false;
  std::size_t referenceProcess =
      0; // Display-group index for overlay ratio/pull.
  PlotExperiment experiment = PlotExperiment::CMS;
  std::string experimentExtra; // e.g. Preliminary, Simulation, or Internal.
  bool rebin =
      false; // File inputs: explicit aligned-edge coarsening to binEdges.
};

struct PlotResult {
  bool success = false;
  std::string message;
  double mcIntegral = 0.0;
  double dataIntegral = 0.0;
  double mcVariance = 0.0;
  std::vector<std::string> warnings;
  std::vector<std::string> outputs;
};

struct PlotBatchOptions {
  unsigned int workers = 1; // Preparation only; ROOT graphics stay serialized.
  std::size_t maxPreparedBytes = 256 * 1024 * 1024;
  std::size_t maxOpenFiles = 8; // Across preparation workers.
};

struct PlotSessionStats {
  std::size_t plots = 0;
  std::size_t workspaces = 0;
  std::size_t drawableObjects = 0;
  std::size_t bufferGrowths = 0;
  std::size_t fileOpens = 0;
  double preparationSeconds = 0;
  double renderingSeconds = 0; // Excludes export.
  double exportSeconds = 0;
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
  // Thread-affine, noncopyable session. Reuse it across batches; construct it
  // on the main/caller thread. All sessions coordinate ROOT graphics
  // in-process.
  class Session {
  public:
    explicit Session(const PlotBatchOptions &options = {});
    ~Session();
    Session(const Session &) = delete;
    Session &operator=(const Session &) = delete;
    PlotResult render(const PlotRequest &request);
    std::vector<PlotResult>
    renderBatch(const std::vector<PlotRequest> &requests);
    PlotSessionStats stats() const;

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
  };

  PlotResult makeStackPlot(const PlotRequest &request) const;
  std::vector<PlotResult>
  makeStackPlots(const std::vector<PlotRequest> &requests,
                 bool parallel = false) const;

  // Shared YAML/JSON parser used by the compiled CLI and Python/LAW callers.
  // Accepts a sequence, or {schema_version: 1, plots: [...]}.
  static std::vector<PlotRequest> readRequests(const std::string &configFile);
  static std::vector<PlotRequest> parseRequests(const std::string &yamlOrJson);

  static std::unique_ptr<TH1D>
  computeRatioHistogram(const TH1D &numerator, const TH1D &denominator,
                        const std::string &name = "ratio");
  static RatioSummary computeRatioSummary(const TH1D &loHist,
                                          const TH1D &nloHist,
                                          const TH1D *covariance = nullptr,
                                          const TH1D *systematic = nullptr);
  static PCAResult
  computePCAEnvelope(const TH1D &nominal,
                     const std::vector<const TH1D *> &variations,
                     const std::string &baseName = "pca");
};

#endif // PLOTTINGUTILITY_H_INCLUDED
