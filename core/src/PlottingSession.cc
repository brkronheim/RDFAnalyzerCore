#include "PlottingInternal.h"
#include <TBox.h>
#include <TCanvas.h>
#include <TColor.h>
#include <TGaxis.h>
#include <TGraphAsymmErrors.h>
#include <TH1D.h>
#include <TH2D.h>
#include <THashList.h>
#include <TLatex.h>
#include <TLine.h>
#include <TList.h>
#include <TPad.h>
#include <TPaletteAxis.h>
#include <TROOT.h>
#include <TStyle.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

namespace rdfplot {
namespace {
// Includes construction/destruction, palette changes, painting and export.
std::recursive_mutex graphicsMutex;
std::atomic<unsigned long> nextId{0};
std::once_flag rootInitialization;
std::vector<int> palette;
std::vector<int> heatPalette;
// Active only while saving a macro. Ordinary exports paint the persistent
// objects directly; serialization emits standard ROOT objects, not our scenes.
thread_local std::ostream *primitiveOutput = nullptr;
void renderPrimitive(TObject &object, Option_t *option = "") {
  if (primitiveOutput) {
    if (auto *colorbar = dynamic_cast<TPaletteAxis *>(&object)) {
      // ROOT's palette serializer names its histogram by GetName(), whereas
      // histogram serializers generate unique C++ variable names. Resolve the
      // histogram explicitly so the macro also works without Cling lookup.
      const auto *histogram = colorbar->GetHistogram();
      *primitiveOutput
          << "{\nauto *" << histogram->GetName()
          << " = dynamic_cast<TH1*>(gPad->GetListOfPrimitives()->FindObject(\""
          << histogram->GetName() << "\"));\n";
      object.SavePrimitive(*primitiveOutput, option);
      *primitiveOutput << "palette->Draw();\n}\n";
      return;
    }
    object.SavePrimitive(*primitiveOutput, option);
  } else
    object.Paint(option);
}
void initializeRoot() {
  ROOT::EnableThreadSafety();
  for (const char *color : {"#4477AA", "#EEAA55", "#66AA88", "#CC6677",
                            "#AA88BB", "#88CCEE", "#BBBB77", "#999999"})
    palette.push_back(TColor::GetColor(color));
  double stops[]{0., 0.5, 1.}, red[]{0.18, 0.98, 0.70},
      green[]{0.37, 0.98, 0.12}, blue[]{0.66, 0.98, 0.22};
  // CreateGradientColorTable changes gStyle's palette. Restore the caller's.
  std::vector<int> previous;
  for (int i = 0; i < gStyle->GetNumberOfColors(); ++i)
    previous.push_back(gStyle->GetColorPalette(i));
  int first = TColor::CreateGradientColorTable(3, stops, red, green, blue, 80);
  for (int i = 0; i < 80; ++i)
    heatPalette.push_back(first + i);
  if (!previous.empty())
    gStyle->SetPalette(previous.size(), previous.data());
}

// TGraph::Set(n) reallocates even when shrinking. These subclasses change only
// the logical point count when the existing (including error) arrays suffice.
class Graph : public TGraphAsymmErrors {
public:
  void size(std::size_t n, PlotSessionStats &stats) {
    if (n > static_cast<std::size_t>(fMaxSize)) {
      TGraphAsymmErrors::Set(
          std::max<std::size_t>(n, std::max(16, 2 * fMaxSize)));
      ++stats.bufferGrowths;
    }
    fNpoints = n;
  }
};
class Scene : public TObject {
public:
  std::function<void()> paint;
  void Paint(Option_t *) override {
    if (paint)
      paint();
  }
  void SavePrimitive(std::ostream &out, Option_t *) override {
    struct Restore {
      std::ostream *previous = primitiveOutput;
      ~Restore() { primitiveOutput = previous; }
    } restore;
    primitiveOutput = &out;
    if (paint)
      paint();
  }
};
class Colorbar : public TPaletteAxis {
public:
  explicit Colorbar(TH1 *histogram) : TPaletteAxis(0, 0, 1, 1, histogram) {
    // The normal constructor defers world -> NDC conversion to first paint.
    // This renderer supplies NDC coordinates explicitly instead.
    fInit = 1;
  }
};
struct Panel {
  TPad pad;
  TH1D frame;
  Graph stat, syst, points, curve;
  std::vector<std::pair<int, int>> curveSegments;
  std::vector<std::unique_ptr<Graph>> comparisons;
  TLine reference;
  Scene scene;
  explicit Panel(const std::string &name, TCanvas &canvas)
      : pad(name.c_str(), "", 0, 0, 1, 1),
        frame((name + "_frame").c_str(), "", 1, 0, 1) {
    frame.SetDirectory(nullptr);
    frame.SetStats(false);
    pad.SetBorderMode(0);
    pad.SetFillColor(kWhite);
    pad.SetTicks(1, 1);
    canvas.cd();
    pad.Draw();
    pad.ResetBit(TObject::kCanDelete);
    pad.GetListOfPrimitives()->Add(&scene);
  }
  ~Panel() { pad.GetListOfPrimitives()->Remove(&scene); }
};
struct ProcessDrawing {
  Graph positive, negative, outline;
};
struct LegendDrawing {
  TBox box;
  TLine line;
  Graph marker;
  std::vector<std::unique_ptr<TLatex>> lines;
};

void paintText(TLatex &text, double x, double y, const std::string &label,
               double size, int align = 13, int font = 43) {
  text.SetNDC();
  text.SetTextFont(font);
  text.SetTextSize(size);
  text.SetTextAlign(align);
  text.SetTextColor(kBlack);
  text.SetX(x);
  text.SetY(y);
  text.SetTitle(label.c_str());
  renderPrimitive(text);
}
std::vector<std::string> wrap(const std::string &text, std::size_t width) {
  std::vector<std::string> lines;
  std::istringstream words(text);
  std::string word, line;
  while (words >> word) {
    if (!line.empty() && line.size() + word.size() + 1 > width) {
      lines.push_back(line);
      line.clear();
    }
    if (!line.empty())
      line += ' ';
    line += word;
  }
  if (!line.empty())
    lines.push_back(line);
  if (lines.empty())
    lines.push_back("");
  return lines;
}
void configureAxis(TAxis &axis, const PlotStyle &style,
                   const std::string &title, bool visible, bool y) {
  axis.SetTitle(title.c_str());
  axis.SetTitleFont(43);
  axis.SetLabelFont(43);
  axis.SetTitleSize(visible ? style.labelSize + 4 : 0);
  axis.SetLabelSize(visible ? style.labelSize : 0);
  axis.SetTitleOffset(y ? 2.5 : 1.25);
  axis.SetLabelOffset(y ? 0.012 : 0.015);
  axis.SetNdivisions(y ? 505 : 510);
  axis.SetTickLength(y ? 0.015 : 0.025);
  axis.CenterTitle(y);
}
bool hasLabels(const std::vector<std::string> &labels) {
  return std::any_of(labels.begin(), labels.end(),
                     [](const std::string &label) { return !label.empty(); });
}
bool blinded(const PlotRequest &req, std::size_t b) {
  return !req.blindBins.empty() && req.blindBins[b];
}
std::string experimentName(const PlotRequest &req) {
  if (!req.experimentLabel.empty())
    return req.experimentLabel;
  switch (req.experiment) {
  case PlotExperiment::CMS:
    return "CMS";
  case PlotExperiment::ATLAS:
    return "ATLAS";
  case PlotExperiment::LHCb:
    return "LHCb";
  case PlotExperiment::ALICE:
    return "ALICE";
  case PlotExperiment::Custom:
    return "";
  }
  return "";
}
void setPoint(Graph &graph, int i, double x, double y, double ex, double low,
              double high) {
  graph.SetPoint(i, x, y);
  graph.SetPointError(i, ex, ex, low, high);
}
void fillArea(Graph &graph, const std::vector<double> &edges,
              const std::vector<double> &low, const std::vector<double> &high,
              double floor, bool log, PlotSessionStats &stats) {
  const std::size_t n = low.size();
  graph.size(4 * n + 1, stats);
  auto value = [&](double y) { return log ? std::max(floor, y) : y; };
  for (std::size_t b = 0; b < n; ++b) {
    graph.SetPoint(2 * b, edges[b], value(high[b]));
    graph.SetPoint(2 * b + 1, edges[b + 1], value(high[b]));
    graph.SetPoint(2 * n + 2 * b, edges[n - b], value(low[n - b - 1]));
    graph.SetPoint(2 * n + 2 * b + 1, edges[n - b - 1], value(low[n - b - 1]));
  }
  graph.SetPoint(4 * n, edges.front(), value(high.front()));
}

class Workspace {
public:
  PlotSessionStats &stats;
  std::unique_ptr<TCanvas> canvas;
  std::unique_ptr<Panel> header;
  std::vector<std::unique_ptr<Panel>> panels;
  std::vector<std::unique_ptr<ProcessDrawing>> processes;
  std::vector<std::unique_ptr<LegendDrawing>> legend;
  std::vector<std::unique_ptr<TLatex>> text;
  std::vector<double> positive, negative, bottom, top;
  TH2D heat;
  std::unique_ptr<Colorbar> colorbar;
  std::vector<std::unique_ptr<TLine>> separators;
  PlotStyle style; // Resolved pixel sizes; the caller's request stays unchanged.
  const PlotRequest *request = nullptr;
  const Prepared *prepared = nullptr;
  std::vector<PlotPanelConfig> panelConfigs;
  std::vector<std::vector<std::string>> legendLabels;
  std::vector<int> legendColors;
  std::vector<int> legendTypes; // 0 fill, 1 line, 2 points, 3 stat, 4 syst
  std::vector<std::vector<std::string>> captionLines;
  std::vector<std::string> titleLines;
  double headerPixels = 0, legendPixels = 0, infoPixels = 0, footerPixels = 0;
  double height = 0, ymin = 0, ymax = 1;
  bool heatmap;

  Workspace(std::size_t count, bool matrix, PlotSessionStats &statistics)
      : stats(statistics),
        heat(("rdf_heat_" + std::to_string(nextId++)).c_str(), "", 1, 0, 1, 1,
             0, 1),
        heatmap(matrix) {
    const auto name = "rdf_plot_" + std::to_string(nextId++);
    // Batch canvases work without a display and do not change gROOT batch mode.
    const bool wasBatch = gROOT->IsBatch();
    gROOT->SetBatch(true);
    canvas = std::make_unique<TCanvas>(name.c_str(), "", 0, 0, 1000, 800);
    canvas->SetBatch(true);
    gROOT->SetBatch(wasBatch);
    canvas->SetFillColor(kWhite);
    canvas->SetBorderMode(0);
    header = std::make_unique<Panel>(name + "_header", *canvas);
    header->scene.paint = [this] {
      if (request && prepared)
        paintHeader();
    };
    heat.SetDirectory(nullptr);
    heat.SetStats(false);
    heat.SetContour(80);
    for (std::size_t i = 0; i <= count; ++i)
      panels.emplace_back(std::make_unique<Panel>(
          name + "_panel" + std::to_string(i), *canvas));
    if (matrix) {
      panels[0]->pad.cd();
      colorbar = std::make_unique<Colorbar>(&heat);
    }
    panels[0]->scene.paint = [this] {
      if (request && prepared) {
        if (heatmap)
          paintHeatmap();
        else
          paintMain();
      }
    };
    for (std::size_t i = 1; i < panels.size(); ++i)
      panels[i]->scene.paint = [this, i] {
        if (request && prepared)
          paintDiagnostic(i);
      };
    ++stats.workspaces;
    // Canvas + TH2 + optional palette, and eight owned objects per pad.
    stats.drawableObjects += 3 + (matrix ? 1 : 0) + 8 * (count + 2);
  }
  ~Workspace() {
    for (const auto &panel : panels)
      canvas->GetListOfPrimitives()->Remove(&panel->pad);
    canvas->GetListOfPrimitives()->Remove(&header->pad);
    panels.clear();
    header.reset();
    canvas.reset();
  }
  TLatex &label(std::size_t i) {
    while (text.size() <= i) {
      text.emplace_back(std::make_unique<TLatex>());
      ++stats.drawableObjects;
    }
    return *text[i];
  }
  int color(std::size_t i) const {
    const auto &config = *prepared->series[i].config;
    if (!config.colorHex.empty())
      return TColor::GetColor(config.colorHex.c_str());
    auto c = config.color;
    return c == kBlack ? palette[i % palette.size()] : c;
  }
  void addLegend(const std::string &name, int color, int type) {
    const int columns = style.legendColumns;
    const double pixels =
        style.width * (1 - style.leftMargin - style.rightMargin) *
            (style.legendInside ? style.legendWidth : 1) / columns -
        50;
    legendLabels.push_back(
        wrap(name, std::max(12., pixels / (0.58 * style.legendSize))));
    legendColors.push_back(color);
    legendTypes.push_back(type);
  }
  void configure(const PlotRequest &req, const Prepared &data,
                 const std::vector<PlotPanelConfig> &configs) {
    canvas->cd();
    request = &req;
    prepared = &data;
    panelConfigs = configs;
    style = req.style;
    require(style.width >= 600 && style.width <= 6000 &&
                style.height <= 10000 && style.height >= 0,
            "Invalid canvas size");
    const auto validTextSize = [](double size) {
      return size == 0 || (size >= 10 && size <= 240);
    };
    require(validTextSize(style.labelSize) && validTextSize(style.titleSize) &&
                validTextSize(style.legendSize),
            "Invalid text size");
    const double textScale = style.width / 1000.;
    if (style.labelSize == 0)
      style.labelSize = 24 * textScale;
    if (style.titleSize == 0)
      style.titleSize = 28 * textScale;
    if (style.legendSize == 0)
      style.legendSize = 22 * textScale;
    require(style.leftMargin >= 0.08 && style.rightMargin >= 0.02 &&
                style.leftMargin + style.rightMargin < 0.65,
            "Invalid plot margins");
    require(style.legendColumns >= 1 && style.legendColumns <= 4,
            "Legend columns must be between one and four");
    require(style.legendWidth >= 0.3 && style.legendWidth <= 1,
            "Legend width must be in [0.3,1]");
    require(style.annotationBoxWidth >= 0.3 && style.annotationBoxWidth <= 0.95,
            "Annotation box width must be in [0.3,0.95]");
    legendLabels.clear();
    legendColors.clear();
    legendTypes.clear();
    captionLines.clear();
    if (!heatmap) {
      for (std::size_t i = 0; i < data.count; ++i) {
        const auto &config = *data.series[i].config;
        std::ostringstream title;
        title << config.legendLabel;
        if ((config.overlay || (config.isSignal && req.signalOverlay)) &&
            config.displayScale != 1)
          title << " (#times" << config.displayScale << " outline)";
        addLegend(title.str(), color(i),
                  (config.overlay || req.kind == PlotKind::Overlay) ? 1 : 0);
      }
      if (data.hasData)
        addLegend("Data", kBlack, 2);
      if (data.hasMC && req.kind == PlotKind::Stack) {
        addLegend("MC stat. uncertainty", kGray + 2, 3);
        if (std::any_of(data.systLow.begin(), data.systLow.end(),
                        [](double v) { return v > 0; }) ||
            std::any_of(data.systHigh.begin(), data.systHigh.end(),
                        [](double v) { return v > 0; }))
          addLegend("MC syst. uncertainty", palette.front(), 4);
      }
    }
    const double frameWidth =
        style.width * (1 - style.leftMargin - style.rightMargin);
    const double maxBoxWidth =
        (heatmap ? 0.54 * style.width : frameWidth) * style.annotationBoxWidth;
    const double horizontalPadding = 12;
    const double boxWidth = maxBoxWidth - horizontalPadding;
    titleLines =
        wrap(req.title, std::max(12., boxWidth / (0.55 * style.titleSize)));
    if (req.title.empty())
      titleLines.clear();
    headerPixels =
        (!experimentName(req).empty() || !req.luminosityLabel.empty()) ? 38
                                                                       : 10;
    headerPixels = std::max(headerPixels, style.titleSize * 1.3 + 15);
    infoPixels = titleLines.size() * (style.titleSize + 3);
    auto captions = req.annotations;
    if (std::any_of(req.blindBins.begin(), req.blindBins.end(),
                    [](bool v) { return v; }))
      captions.emplace_back("Data blinded in selected bins");
    if (req.logY && !heatmap)
      captions.emplace_back(
          "Log scale: nonpositive values clipped for display");
    for (const auto &caption : captions) {
      captionLines.push_back(
          wrap(caption, std::max(12., boxWidth / (0.55 * style.legendSize))));
      infoPixels += captionLines.back().size() * (style.legendSize + 2);
    }
    if (infoPixels) {
      infoPixels += 10;
    }
    legendPixels = 0;
    for (std::size_t i = 0; i < legendLabels.size(); i += style.legendColumns) {
      std::size_t lines = 1;
      for (int c = 0; c < style.legendColumns && i + c < legendLabels.size();
           ++c)
        lines = std::max(lines, legendLabels[i + c].size());
      legendPixels += lines * (style.legendSize + 5) + 7;
    }
    // Outside legends live below the axes, leaving the top strip exclusively
    // for experiment/status/luminosity/energy. Selection boxes stay in-frame.
    footerPixels = !style.legendInside && legendPixels ? legendPixels + 16 : 0;
    const double bottomPixels =
        (hasLabels(data.labels) ? 125 : 75) *
            std::max(1., style.labelSize / 18.) + footerPixels;
    const double preferredDiagnostic = 145 * std::max(1., style.labelSize / 24.);
    // Give the data frame most of the canvas, even with large labels.
    // Reserve annotation/legend space in addition to a useful data area.
    const double preferredMain = std::max(
        720. * std::max(1., style.width / 1000.),
        (infoPixels + (style.legendInside ? legendPixels : 0)) * 3.5);
    height = style.height ? style.height
                          : headerPixels + preferredMain +
                                configs.size() * preferredDiagnostic +
                                bottomPixels;
    require(height <= 10000, "Title/legend exceeds maximum canvas height; "
                             "shorten labels or increase legendColumns");
    require(height - headerPixels - bottomPixels >= 240 + configs.size() * 85,
            "Canvas height is too small for the title, legend, and panels");
    canvas->SetCanvasSize(style.width, height);
    header->pad.SetPad(0, 1 - headerPixels / height, 1, 1);
    header->pad.SetMargin(0, 0, 0, 0);
    header->pad.Range(0, 0, 1, 1);
    header->pad.ResizePad();
    const double available = height - headerPixels - bottomPixels;
    const double mainPixels = configs.empty()
                                  ? available
                                  : available * preferredMain /
                                        (preferredMain + configs.size() *
                                                             preferredDiagnostic);
    const double diagnosticPixels =
        configs.empty() ? 0 : (available - mainPixels) / configs.size();
    double upper = height - headerPixels;
    for (std::size_t i = 0; i < panels.size(); ++i) {
      auto &panel = *panels[i];
      const bool last = i + 1 == panels.size();
      double pixels =
          (i == 0 ? mainPixels : diagnosticPixels) + (last ? bottomPixels : 0);
      panel.pad.SetPad(0, (upper - pixels) / height, 1, upper / height);
      upper -= pixels;
      double left = style.leftMargin, right = style.rightMargin;
      if (heatmap) {
        left = std::max(left, 0.30);
        right = std::max(right, 0.16);
      }
      // Half a y-axis glyph must fit on both sides of the frame. Fractional
      // margins were only 3-4 px on a diagnostic pad and clipped endpoint 0s.
      const double endpointPadding = std::ceil(style.labelSize * 0.7 + 4);
      panel.pad.SetMargin(left, right,
                          (last ? bottomPixels : endpointPadding) / pixels,
                          endpointPadding / pixels);
      panel.pad.ResizePad();
      panel.pad.SetLogy(!heatmap && i == 0 && req.logY);
      panel.pad.SetLogz(heatmap && req.logZ);
      panel.pad.SetGrid(0, style.grid && !heatmap);
      const bool categorical = hasLabels(data.labels);
      const auto bins =
          categorical ? static_cast<int>(data.edges.size() - 1) : 1;
      if (panel.frame.GetNbinsX() != bins) {
        panel.frame.SetBins(bins, data.edges.data());
        ++stats.bufferGrowths;
      } else if (categorical) {
        const auto *old = panel.frame.GetXaxis()->GetXbins();
        bool same =
            old && old->GetSize() == static_cast<int>(data.edges.size());
        if (same)
          for (int b = 0; b < old->GetSize(); ++b)
            same &= old->At(b) == data.edges[b];
        if (!same) {
          panel.frame.SetBins(bins, data.edges.data());
          ++stats.bufferGrowths;
        }
      }
      if (!categorical) {
        // SetLimits does not clear variable-bin storage left by categorical
        // axes. THistPainter would keep using that first variable bin's range.
        if (panel.frame.GetXaxis()->GetXbins()->GetSize()) {
          panel.frame.SetBins(1, data.edges.front(), data.edges.back());
          ++stats.bufferGrowths;
        } else {
          panel.frame.GetXaxis()->SetLimits(data.edges.front(),
                                            data.edges.back());
        }
      }
      if (!categorical && panel.frame.GetXaxis()->GetLabels()) {
        panel.frame.GetXaxis()->GetLabels()->Delete();
        panel.frame.GetXaxis()->SetAlphanumeric(false);
        panel.frame.GetXaxis()->LabelsOption("h");
      }
      configureAxis(*panel.frame.GetXaxis(), style, last ? req.xAxisTitle : "",
                    last, false);
      configureAxis(*panel.frame.GetYaxis(), style, "", true, true);
      panel.frame.GetXaxis()->SetRange(0, 0);
      if (categorical) {
        for (int b = 1; b <= bins; ++b)
          panel.frame.GetXaxis()->SetBinLabel(b, data.labels[b - 1].c_str());
        panel.frame.GetXaxis()->LabelsOption("v");
        panel.frame.GetXaxis()->SetLabelSize(
            last ? style.labelSize : 0);
        panel.frame.GetXaxis()->SetTitleOffset(2.8);
      }
      panel.pad.Modified();
    }
    while (processes.size() < data.count) {
      processes.emplace_back(std::make_unique<ProcessDrawing>());
      stats.drawableObjects += 3;
    }
    while (legend.size() < legendLabels.size()) {
      legend.emplace_back(std::make_unique<LegendDrawing>());
      stats.drawableObjects += 3;
    }
    for (std::size_t i = 0; i < legendLabels.size(); ++i)
      while (legend[i]->lines.size() < legendLabels[i].size()) {
        legend[i]->lines.emplace_back(std::make_unique<TLatex>());
        ++stats.drawableObjects;
      }
    // Reserve text slots before painting so a second export does not allocate
    // drawables.
    std::size_t labels = 5 + titleLines.size();
    for (const auto &lines : captionLines)
      labels += lines.size();
    label(std::max<std::size_t>(32, labels));
    if (heatmap)
      configureHeatmap();
    else
      configureMain();
    canvas->Modified();
  }

  void paintHeader() {
    header->pad.Range(0, 0, 1, 1);
    const auto &req = *request;
    double y = headerPixels - 5;
    std::size_t slot = 0;
    if (!experimentName(req).empty() || !req.luminosityLabel.empty()) {
      const auto name = experimentName(req);
      const int font =
          req.experiment == PlotExperiment::ATLAS && req.experimentLabel.empty()
              ? 73
              : 63;
      paintText(label(slot++), style.leftMargin, y / headerPixels, name,
                style.titleSize * 1.3, 13, font);
      if (!req.experimentExtra.empty()) {
        const double offset =
            (name.size() * style.titleSize * 0.85 + 12) / style.width;
        paintText(label(slot++), style.leftMargin + offset,
                  (y - 4) / headerPixels, req.experimentExtra, style.titleSize,
                  13, 53);
      }
      paintText(label(slot++), 1 - style.rightMargin, y / headerPixels,
                req.luminosityLabel, style.legendSize, 33);
      y -= 32;
      y -= 6;
    } else
      y -= 5;
  }
  void paintAnnotations(double top, double pixels, double left) {
    if (!infoPixels)
      return;
    double y = top - 5;
    std::size_t slot = 4;
    for (const auto &line : titleLines) {
      paintText(label(slot++), left + 6. / style.width, y / pixels, line,
                style.titleSize);
      y -= style.titleSize + 3;
    }
    for (const auto &lines : captionLines)
      for (const auto &line : lines) {
        paintText(label(slot++), left + 6. / style.width, y / pixels, line,
                  style.legendSize);
        y -= style.legendSize + 2;
      }
  }
  void paintFooter(Panel &panel) {
    if (!footerPixels)
      return;
    const double pixels = panel.pad.GetAbsHNDC() * height;
    const double left = panel.pad.GetLeftMargin() + 0.012;
    const double right = 1 - panel.pad.GetRightMargin() - 0.012;
    if (!style.legendInside)
      paintLegend(footerPixels - 8, pixels, left, right);
  }
  void paintLegend(double y, double pixels, double left, double right) {
    const double column = (right - left) / style.legendColumns;
    auto worldX = [](double x) {
      return gPad->PadtoX(gPad->GetX1() + x * (gPad->GetX2() - gPad->GetX1()));
    };
    auto worldY = [](double y) {
      return gPad->PadtoY(gPad->GetY1() + y * (gPad->GetY2() - gPad->GetY1()));
    };
    for (std::size_t i = 0; i < legendLabels.size(); i += style.legendColumns) {
      std::size_t maxLines = 1;
      for (int col = 0;
           col < style.legendColumns && i + col < legendLabels.size(); ++col) {
        const auto k = i + col;
        auto &entry = *legend[k];
        const double x = left + col * column,
                     mid = (y - style.legendSize * 0.55) / pixels;
        const double boxWidth = 25. / style.width,
                     boxHeight = (style.legendSize - 3) / pixels;
        if (legendTypes[k] == 0 || legendTypes[k] >= 3) {
          entry.box.SetX1(worldX(x));
          entry.box.SetX2(worldX(x + boxWidth));
          entry.box.SetY1(worldY(mid - boxHeight / 2));
          entry.box.SetY2(worldY(mid + boxHeight / 2));
          entry.box.SetFillColor(legendColors[k]);
          if (legendTypes[k] == 3)
            entry.box.SetFillColorAlpha(kBlack, 0.16);
          entry.box.SetLineColor(legendColors[k]);
          entry.box.SetLineWidth(0);
          entry.box.SetFillStyle(legendTypes[k] == 4 ? 3354 : 1001);
          renderPrimitive(entry.box);
        } else if (legendTypes[k] == 1) {
          entry.line.SetX1(worldX(x));
          entry.line.SetX2(worldX(x + boxWidth));
          entry.line.SetY1(worldY(mid));
          entry.line.SetY2(worldY(mid));
          entry.line.SetLineWidth(2);
          entry.line.SetLineColor(legendColors[k]);
          renderPrimitive(entry.line);
        } else {
          entry.marker.size(1, stats);
          setPoint(entry.marker, 0, worldX(x + boxWidth / 2), worldY(mid), 0,
                   worldY(mid) - worldY(mid - boxHeight * 0.65),
                   worldY(mid + boxHeight * 0.65) - worldY(mid));
          entry.marker.SetMarkerStyle(20);
          entry.marker.SetMarkerSize(0.85);
          entry.marker.SetMarkerColor(kBlack);
          entry.marker.SetLineColor(kBlack);
          renderPrimitive(entry.marker, "PZ");
        }
        for (std::size_t line = 0; line < legendLabels[k].size(); ++line)
          paintText(*entry.lines[line], x + 36. / style.width,
                    (y - line * (style.legendSize + 5)) / pixels,
                    legendLabels[k][line], style.legendSize);
        maxLines = std::max(maxLines, legendLabels[k].size());
      }
      y -= maxLines * (style.legendSize + 5) + 7;
    }
  }

  void configureMain() {
    const auto &req = *request;
    const auto &data = *prepared;
    const auto n = data.edges.size() - 1;
    for (auto *v : {&positive, &negative, &bottom, &top}) {
      if (v->capacity() < n)
        ++stats.bufferGrowths;
      v->assign(n, 0);
    }
    ymin = 0;
    ymax = 0;
    double minPositive = std::numeric_limits<double>::infinity();
    auto range = [&](double value) {
      ymin = std::min(ymin, value);
      ymax = std::max(ymax, value);
      if (value > 0)
        minPositive = std::min(minPositive, value);
    };
    for (std::size_t i = 0; i < data.count; ++i)
      for (std::size_t b = 0; b < n; ++b) {
        const auto &config = *data.series[i].config;
        const double value = data.series[i].values[b];
        if (req.kind == PlotKind::Overlay || config.overlay)
          range(value * config.displayScale);
        else {
          if (value >= 0)
            positive[b] += value;
          else
            negative[b] += value;
          range(positive[b]);
          range(negative[b]);
        }
        if (config.isSignal && req.signalOverlay &&
            (!req.logY || value * config.displayScale > ymax))
          range(value * config.displayScale);
      }
    for (std::size_t b = 0; b < n; ++b) {
      if (data.hasMC) {
        range(data.total[b] +
              std::max(std::sqrt(data.variance[b]), data.systHigh[b]));
        range(data.total[b] -
              std::max(std::sqrt(data.variance[b]), data.systLow[b]));
      }
      if (data.hasData && !blinded(req, b)) {
        range(data.data[b] + std::sqrt(data.dataHigh2[b]));
        range(data.data[b] - std::sqrt(data.dataLow2[b]));
      }
    }
    if (req.logY) {
      ymin = std::isfinite(minPositive) ? std::max(minPositive * 0.25, 1e-12)
                                        : 0.1;
      ymax = std::max(ymin * 10, ymax * 3.0);
    } else {
      double span = std::max(ymax - ymin, 1.);
      ymax += 0.15 * span;
      if (ymin < 0)
        ymin -= 0.10 * span;
    }
    if (infoPixels > 0 || (req.style.legendInside && legendPixels > 0)) {
      const auto &pad = panels[0]->pad;
      const double plotPixels =
          pad.GetAbsHNDC() * height *
          (1 - pad.GetTopMargin() - pad.GetBottomMargin());
      const double fraction =
          (infoPixels + (req.style.legendInside ? legendPixels : 0) + 30) /
          plotPixels;
      require(fraction < 0.65,
              "Legend leaves too little room for data; increase canvas height, "
              "use more columns, or set legendInside: false");
      if (req.logY)
        ymax = std::exp(std::log(ymin) +
                        (std::log(ymax) - std::log(ymin)) / (1 - fraction));
      else
        ymax = ymin + (ymax - ymin) / (1 - fraction);
    }
    auto &main = *panels[0];
    main.frame.SetMinimum(ymin);
    main.frame.SetMaximum(ymax);
    main.frame.GetYaxis()->SetTitle(req.yAxisTitle.c_str());
    std::fill(positive.begin(), positive.end(), 0);
    std::fill(negative.begin(), negative.end(), 0);
    for (std::size_t i = 0; i < data.count; ++i) {
      const auto &config = *data.series[i].config;
      auto &drawing = *processes[i];
      drawing.positive.SetFillColor(color(i));
      drawing.positive.SetLineColor(color(i));
      drawing.positive.SetFillStyle(1001);
      drawing.negative.SetFillColor(color(i));
      drawing.negative.SetLineColor(color(i));
      drawing.negative.SetFillStyle(1001);
      for (std::size_t b = 0; b < n; ++b) {
        bottom[b] = positive[b];
        if (!config.overlay)
          positive[b] += std::max(0., data.series[i].values[b]);
        top[b] = positive[b];
      }
      fillArea(drawing.positive, data.edges, bottom, top, ymin, req.logY,
               stats);
      for (std::size_t b = 0; b < n; ++b) {
        top[b] = negative[b];
        if (!config.overlay)
          negative[b] += std::min(0., data.series[i].values[b]);
        bottom[b] = negative[b];
      }
      fillArea(drawing.negative, data.edges, bottom, top, ymin, req.logY,
               stats);
      drawing.outline.size(2 * n, stats);
      drawing.outline.SetLineWidth(2);
      drawing.outline.SetLineColor(color(i));
      for (std::size_t b = 0; b < n; ++b) {
        double value = data.series[i].values[b] * config.displayScale;
        if (req.logY)
          value = std::max(ymin, value);
        drawing.outline.SetPoint(2 * b, data.edges[b], value);
        drawing.outline.SetPoint(2 * b + 1, data.edges[b + 1], value);
      }
    }
    for (auto *graph : {&main.stat, &main.syst, &main.points})
      graph->size(n, stats);
    int points = 0;
    for (std::size_t b = 0; b < n; ++b) {
      const double x = (data.edges[b] + data.edges[b + 1]) / 2,
                   ex = (data.edges[b + 1] - data.edges[b]) / 2;
      const double low = req.logY ? std::min(std::sqrt(data.variance[b]),
                                             std::max(0., data.total[b] - ymin))
                                  : std::sqrt(data.variance[b]);
      setPoint(
          main.stat, b, x,
          std::max(req.logY ? ymin : -std::numeric_limits<double>::infinity(),
                   data.total[b]),
          ex, low, std::sqrt(data.variance[b]));
      setPoint(
          main.syst, b, x,
          std::max(req.logY ? ymin : -std::numeric_limits<double>::infinity(),
                   data.total[b]),
          ex,
          req.logY
              ? std::min(data.systLow[b], std::max(0., data.total[b] - ymin))
              : data.systLow[b],
          data.systHigh[b]);
      if (data.hasData && !blinded(req, b) && (!req.logY || data.data[b] > 0))
        setPoint(main.points, points++, x, data.data[b], 0,
                 std::sqrt(data.dataLow2[b]), std::sqrt(data.dataHigh2[b]));
    }
    main.points.size(points, stats);
    main.points.SetMarkerStyle(20);
    main.points.SetMarkerSize(0.85);
    main.points.SetLineWidth(1);
    main.points.SetLineColor(kBlack);
    main.points.SetMarkerColor(kBlack);
    main.stat.SetFillColorAlpha(kBlack, 0.16);
    main.stat.SetFillStyle(1001);
    main.stat.SetLineWidth(0);
    main.syst.SetFillColor(palette.front());
    main.syst.SetFillStyle(3354);
    main.syst.SetLineWidth(0);
    for (std::size_t i = 1; i < panels.size(); ++i)
      configureDiagnostic(i);
  }
  void paintMain() {
    const auto &req = *request;
    const auto &data = *prepared;
    auto &main = *panels[0];
    renderPrimitive(main.frame, "AXIS");
    for (std::size_t i = 0; i < data.count; ++i) {
      const auto &config = *data.series[i].config;
      if (req.kind == PlotKind::Stack && !config.overlay) {
        renderPrimitive(processes[i]->positive, "F");
        if (!req.logY)
          renderPrimitive(processes[i]->negative, "F");
      }
    }
    if (req.kind == PlotKind::Stack && data.hasMC) {
      renderPrimitive(main.syst, "2");
      renderPrimitive(main.stat, "2");
    }
    for (std::size_t i = 0; i < data.count; ++i) {
      const auto &config = *data.series[i].config;
      if (req.kind == PlotKind::Overlay || config.overlay ||
          (config.isSignal && req.signalOverlay))
        renderPrimitive(processes[i]->outline, "L");
    }
    if (main.points.GetN())
      renderPrimitive(main.points, "PZ");
    renderPrimitive(main.frame, "AXIS SAME");
    const double pixels = main.pad.GetAbsHNDC() * height;
    paintAnnotations(
        (1 - main.pad.GetTopMargin()) * pixels - 10, pixels,
        main.pad.GetLeftMargin() + 0.012);
    if (req.style.legendInside) {
      const double right = 1 - main.pad.GetRightMargin() - 0.015;
      const double left =
          right - req.style.legendWidth * (1 - main.pad.GetLeftMargin() -
                                           main.pad.GetRightMargin());
      paintLegend((1 - main.pad.GetTopMargin()) * pixels - 12 -
                      (infoPixels ? infoPixels + 12 : 0),
                  pixels, left, right);
    }
    if (panels.size() == 1)
      paintFooter(main);
  }
  void configureDiagnostic(std::size_t i) {
    auto &panel = *panels[i];
    const auto &data = *prepared;
    const auto &req = *request;
    const auto &config = panelConfigs[i - 1];
    require(config.autoRange || (std::isfinite(config.minimum) &&
                                 std::isfinite(config.maximum) &&
                                 config.minimum < config.maximum),
            "Invalid panel range");
    const auto n = data.total.size();
    for (auto *g : {&panel.stat, &panel.syst, &panel.points})
      g->size(n, stats);
    panel.curve.size(2 * n, stats);
    panel.curveSegments.clear();
    int segmentStart = -1;
    for (auto &graph : panel.comparisons)
      graph->size(0, stats);
    int points = 0, bands = 0, curves = 0;
    double low = 0, high = 0;
    const char *title = "";
    if (config.type == PlotPanel::Ratio)
      title = "Data / MC";
    if (config.type == PlotPanel::Pull)
      title = "Pull";
    if (config.type == PlotPanel::SignalBackground)
      title = "S / B";
    if (config.type == PlotPanel::Significance)
      title = "S / #sqrt{S+B}";
    if (req.kind == PlotKind::Overlay &&
        (config.type == PlotPanel::Ratio || config.type == PlotPanel::Pull)) {
      while (panel.comparisons.size() < data.count) {
        panel.comparisons.emplace_back(std::make_unique<Graph>());
        ++stats.drawableObjects;
      }
      const auto &reference = data.series[req.referenceProcess];
      title = config.type == PlotPanel::Ratio ? "Ratio" : "Pull";
      for (std::size_t series = 0; series < data.count; ++series) {
        auto &graph = *panel.comparisons[series];
        graph.size(n, stats);
        int active = 0;
        if (series == req.referenceProcess) {
          graph.size(0, stats);
          continue;
        }
        for (std::size_t b = 0; b < n; ++b) {
          const double numerator = data.series[series].values[b],
                       denominator = reference.values[b];
          const double nv = data.series[series].variance[b],
                       dv = reference.variance[b];
          const auto &covariances =
              data.series[series].config->referenceCovariance;
          double covariance = covariances.empty() ? 0 : covariances[b];
          if (req.density)
            covariance /= std::pow(data.edges[b + 1] - data.edges[b], 2);
          require(std::abs(covariance) <= std::sqrt(nv * dv) + 1e-9,
                  "Reference covariance is inconsistent with variances");
          double value = 0, error = 0;
          if (config.type == PlotPanel::Ratio) {
            if (denominator == 0)
              continue;
            value = numerator / denominator;
            const double d2 = denominator * denominator;
            error = std::sqrt(std::max(
                0., nv / d2 + numerator * numerator * dv / (d2 * d2) -
                        2 * numerator * covariance / (d2 * denominator)));
          } else {
            const double variance = nv + dv - 2 * covariance;
            if (variance <= 0)
              continue;
            value = (numerator - denominator) / std::sqrt(variance);
          }
          setPoint(graph, active++, (data.edges[b] + data.edges[b + 1]) / 2,
                   value, 0, error, error);
          low = std::min(low, value);
          high = std::max(high, value);
        }
        graph.size(active, stats);
        graph.SetMarkerStyle(20 + series % 5);
        graph.SetMarkerSize(0.7);
        graph.SetMarkerColor(color(series));
        graph.SetLineColor(color(series));
      }
    } else
      for (std::size_t b = 0; b < n; ++b) {
        const double x = (data.edges[b] + data.edges[b + 1]) / 2,
                     ex = (data.edges[b + 1] - data.edges[b]) / 2,
                     m = data.total[b], d = data.data[b];
        if (config.type == PlotPanel::Ratio && m > 0) {
          setPoint(panel.stat, bands, x, 1, ex, std::sqrt(data.variance[b]) / m,
                   std::sqrt(data.variance[b]) / m);
          setPoint(panel.syst, bands++, x, 1, ex, data.systLow[b] / m,
                   data.systHigh[b] / m);
          if (data.hasData && !blinded(req, b)) {
            const double r = d / m;
            setPoint(panel.points, points++, x, r, 0,
                     std::sqrt(data.dataLow2[b]) / m,
                     std::sqrt(data.dataHigh2[b]) / m);
            low = std::min(low, r);
            high = std::max(high, r);
          }
        } else if (config.type == PlotPanel::Pull && data.hasData &&
                   !blinded(req, b)) {
          const double variance =
              (d >= m ? data.dataLow2[b] : data.dataHigh2[b]) +
              data.variance[b] +
              std::pow(d >= m ? data.systHigh[b] : data.systLow[b], 2);
          if (variance > 0) {
            double value = (d - m) / std::sqrt(variance);
            setPoint(panel.points, points++, x, value, 0, 0, 0);
            low = std::min(low, value);
            high = std::max(high, value);
          }
        } else if (config.type == PlotPanel::SignalBackground ||
                   config.type == PlotPanel::Significance) {
          const double s = data.signal[b], background = m - s;
          const double denominator = config.type == PlotPanel::Significance
                                         ? (m > 0 ? std::sqrt(m) : 0)
                                         : background;
          if (denominator > 0) {
            const double value =
                s / denominator *
                ((req.density && config.type == PlotPanel::Significance)
                     ? std::sqrt(data.edges[b + 1] - data.edges[b])
                     : 1.);
            if (segmentStart < 0)
              segmentStart = curves;
            panel.curve.SetPoint(curves++, data.edges[b], value);
            panel.curve.SetPoint(curves++, data.edges[b + 1], value);
            low = std::min(low, value);
            high = std::max(high, value);
          } else if (segmentStart >= 0) {
            panel.curveSegments.emplace_back(segmentStart,
                                             curves - segmentStart);
            segmentStart = -1;
          }
        }
      }
    if (segmentStart >= 0)
      panel.curveSegments.emplace_back(segmentStart, curves - segmentStart);
    panel.stat.size(bands, stats);
    panel.syst.size(bands, stats);
    panel.points.size(points, stats);
    panel.curve.size(curves, stats);
    panel.stat.SetFillColorAlpha(kBlack, 0.16);
    panel.stat.SetFillStyle(1001);
    panel.stat.SetLineWidth(0);
    panel.syst.SetFillColor(palette[0]);
    panel.syst.SetFillStyle(3354);
    panel.syst.SetLineWidth(0);
    panel.points.SetMarkerStyle(20);
    panel.points.SetMarkerSize(0.75);
    panel.points.SetMarkerColor(kBlack);
    panel.points.SetLineColor(kBlack);
    panel.curve.SetLineColor(
        config.type == PlotPanel::Significance ? palette[0] : palette[4]);
    panel.curve.SetLineWidth(2);
    panel.curve.SetMarkerStyle(1);
    if (config.autoRange) {
      if (config.type == PlotPanel::Ratio) {
        if (req.kind == PlotKind::Overlay) {
          low = std::min(0.8, low * 0.9);
          high = std::max(1.2, high * 1.1);
        } else {
          low = 0.5;
          high = 1.5;
        }
      } else if (config.type == PlotPanel::Pull) {
        high = std::max({2.5, std::abs(low) * 1.2, std::abs(high) * 1.2});
        low = -high;
      } else {
        high = std::max(0.1, high * 1.25);
        low = std::min(0., low * 1.25);
      }
    } else {
      low = config.minimum;
      high = config.maximum;
    }
    panel.frame.SetMinimum(low);
    panel.frame.SetMaximum(high);
    panel.frame.GetYaxis()->SetTitle(title);
    panel.reference.SetX1(data.edges.front());
    panel.reference.SetX2(data.edges.back());
    panel.reference.SetY1(config.type == PlotPanel::Ratio ? 1 : 0);
    panel.reference.SetY2(panel.reference.GetY1());
    panel.reference.SetLineColor(kGray + 2);
    panel.reference.SetLineStyle(2);
  }
  void paintDiagnostic(std::size_t i) {
    auto &panel = *panels[i];
    renderPrimitive(panel.frame, "AXIS");
    if (panel.syst.GetN())
      renderPrimitive(panel.syst, "2");
    if (panel.stat.GetN())
      renderPrimitive(panel.stat, "2");
    renderPrimitive(panel.reference);
    if (panel.points.GetN())
      renderPrimitive(panel.points, "PZ");
    for (const auto &segment : panel.curveSegments) {
      if (primitiveOutput) {
        TGraph graph(segment.second, panel.curve.GetX() + segment.first,
                     panel.curve.GetY() + segment.first);
        panel.curve.TAttLine::Copy(graph);
        renderPrimitive(graph, "L");
      } else {
        panel.curve.PaintGraph(segment.second,
                               panel.curve.GetX() + segment.first,
                               panel.curve.GetY() + segment.first, "L");
      }
    }
    for (const auto &graph : panel.comparisons)
      if (graph->GetN())
        renderPrimitive(*graph, "PZ");
    renderPrimitive(panel.frame, "AXIS SAME");
    if (i + 1 == panels.size())
      paintFooter(panel);
  }
  void configureHeatmap() {
    const auto &data = *prepared;
    const auto &req = *request;
    const int nx = data.edges.size() - 1, ny = data.yEdges.size() - 1;
    bool same = heat.GetNbinsX() == nx && heat.GetNbinsY() == ny;
    if (same) {
      for (int b = 0; b <= nx; ++b)
        same &= heat.GetXaxis()->GetBinLowEdge(b + 1) == data.edges[b];
      for (int b = 0; b <= ny; ++b)
        same &= heat.GetYaxis()->GetBinLowEdge(b + 1) == data.yEdges[b];
    }
    if (!same) {
      heat.SetBins(nx, data.edges.data(), ny, data.yEdges.data());
      ++stats.bufferGrowths;
    }
    double limit = 0, minPositive = std::numeric_limits<double>::infinity(),
           minimum = 0;
    for (int y = 1; y <= ny; ++y)
      for (int x = 1; x <= nx; ++x) {
        double v = data.matrix[(y - 1) * nx + x - 1];
        heat.SetBinContent(x, y, v);
        limit = std::max(limit, std::abs(v));
        minimum = std::min(minimum, v);
        if (v > 0)
          minPositive = std::min(minPositive, v);
      }
    if (req.logZ)
      require(std::isfinite(minPositive),
              "Log-z heatmap has no positive cells");
    heat.SetMinimum(req.logZ                  ? minPositive * 0.5
                    : req.symmetricColorRange ? -std::max(limit, 1e-12)
                                              : minimum);
    heat.SetMaximum(req.logZ ? std::max(limit, minPositive * 2)
                             : std::max(limit, 1e-12));
    configureAxis(*heat.GetXaxis(), style, req.xAxisTitle, true, false);
    configureAxis(*heat.GetYaxis(), style, req.yAxisTitle, true, true);
    // Place the title at the outer margin so large fonts do not push it
    // off the canvas or into long category labels.
    heat.GetYaxis()->SetTitle("");
    for (int b = 1; b <= nx; ++b)
      heat.GetXaxis()->SetBinLabel(
          b, hasLabels(data.labels) ? data.labels[b - 1].c_str() : "");
    for (int b = 1; b <= ny; ++b)
      heat.GetYaxis()->SetBinLabel(
          b, hasLabels(data.yLabels) ? data.yLabels[b - 1].c_str() : "");
    // Reset endpoints left by the initial 1x1 histogram/palette setup. ROOT's
    // source serializer otherwise turns those stale endpoints into a zoom.
    heat.GetXaxis()->SetRange(0, 0);
    heat.GetYaxis()->SetRange(0, 0);
    heat.GetXaxis()->SetLabelSize(style.labelSize);
    heat.GetYaxis()->SetLabelSize(
        std::max(8., std::min(style.labelSize,
                              0.7 * (height - headerPixels - 125) / ny)));
    heat.GetZaxis()->SetTitle(req.zAxisTitle.c_str());
    heat.GetZaxis()->SetLabelFont(43);
    heat.GetZaxis()->SetLabelSize(style.labelSize);
    heat.GetZaxis()->SetTitleFont(43);
    heat.GetZaxis()->SetTitleSize(style.labelSize + 4);
    heat.GetZaxis()->SetTitleOffset(1.4);
    auto &pad = panels[0]->pad;
    colorbar->SetX1NDC(1 - pad.GetRightMargin() + 0.025);
    colorbar->SetX2NDC(1 - pad.GetRightMargin() + 0.055);
    colorbar->SetY1NDC(pad.GetBottomMargin());
    colorbar->SetY2NDC(1 - pad.GetTopMargin());
    while (separators.size() < req.horizontalSeparators.size()) {
      separators.emplace_back(std::make_unique<TLine>());
      ++stats.drawableObjects;
    }
    for (std::size_t i = 0; i < req.horizontalSeparators.size(); ++i) {
      auto &line = *separators[i];
      line.SetX1(data.edges.front());
      line.SetX2(data.edges.back());
      line.SetY1(req.horizontalSeparators[i]);
      line.SetY2(req.horizontalSeparators[i]);
      line.SetLineColor(kWhite);
      line.SetLineWidth(1);
    }
  }
  void paintHeatmap() {
    renderPrimitive(heat, "COL");
    for (std::size_t i = 0; i < request->horizontalSeparators.size(); ++i)
      renderPrimitive(*separators[i]);
    renderPrimitive(*colorbar);
    renderPrimitive(heat, "AXIS SAME");
    auto &main = *panels[0];
    const double pixels = main.pad.GetAbsHNDC() * height;
    paintAnnotations(
        (1 - main.pad.GetTopMargin()) * pixels - 10, pixels,
        main.pad.GetLeftMargin() + 0.012);
    auto &yTitle = label(3); // Header uses slots 0-2; annotations start at 4.
    yTitle.SetTextAngle(90);
    paintText(yTitle, (style.labelSize + 4) / style.width,
              (1 - main.pad.GetTopMargin() + main.pad.GetBottomMargin()) / 2,
              request->yAxisTitle, style.labelSize + 4, 22);
    paintFooter(*panels[0]);
  }
  void saveMacro(const std::filesystem::path &path) {
    std::ofstream out(path);
    require(bool(out), "Cannot open macro output: " + path.string());
    out.precision(17);
    out << "// Standalone ROOT macro exported by PlottingUtility.\n"
           "#include <TCanvas.h>\n#include <TPad.h>\n#include "
           "<TH1D.h>\n#include <TH2D.h>\n"
           "#include <TGraph.h>\n#include <TGraphAsymmErrors.h>\n#include "
           "<TLatex.h>\n"
           "#include <TLine.h>\n#include <TBox.h>\n#include <TPaletteAxis.h>\n"
           "#include <TColor.h>\n#include <TStyle.h>\n#include "
           "<TROOT.h>\n#include <vector>\n{\n";
    gROOT->ResetClassSaved();
    if (heatmap) {
      out << "Int_t rdfPalette[] = {";
      for (std::size_t i = 0; i < heatPalette.size(); ++i) {
        if (i)
          out << ',';
        out << TColor::SavePrimitiveColor(heatPalette[i]);
      }
      out << "};\ngStyle->SetPalette(" << heatPalette.size()
          << ", rdfPalette);\n";
    }
    out << "auto *" << canvas->GetName() << " = new TCanvas(\""
        << canvas->GetName() << "\", \"\", 0, 0, " << request->style.width
        << ", " << static_cast<int>(height) << ");\n"
        << canvas->GetName() << "->SetCanvasSize(" << request->style.width
        << ", " << static_cast<int>(height) << ");\n";
    canvas->SavePrimitive(out, "toplevel");
    out << canvas->GetName() << "->Modified();\n"
        << canvas->GetName() << "->Update();\n";
    out << "\n}\n";
    out.close();
    require(bool(out), "Failed writing ROOT macro: " + path.string());
  }
};

struct Lane {
  Reader reader;
  Prepared prepared;
  std::mutex mutex;
  std::condition_variable cv;
  const PlotRequest *request = nullptr;
  bool stop = false, done = true;
  std::thread thread;
  double preparationTime = 0;
  Lane(std::size_t files, std::size_t bytes, bool threaded)
      : reader(files, bytes) {
    if (threaded)
      thread = std::thread([this] {
        std::unique_lock<std::mutex> lock(mutex);
        for (;;) {
          cv.wait(lock, [this] { return stop || !done; });
          if (stop)
            return;
          auto *input = request;
          lock.unlock();
          prepare(*input);
          lock.lock();
          done = true;
          cv.notify_all();
        }
      });
  }
  ~Lane() {
    if (thread.joinable()) {
      {
        std::lock_guard<std::mutex> lock(mutex);
        stop = true;
        cv.notify_all();
      }
      thread.join();
    }
  }
  void prepare(const PlotRequest &input) {
    const auto start = Clock::now();
    try {
      reader.prepare(input, prepared);
    } catch (const std::exception &error) {
      prepared.result = {};
      prepared.result.message = error.what();
    }
    preparationTime += seconds(start);
  }
  void submit(const PlotRequest &input) {
    std::lock_guard<std::mutex> lock(mutex);
    request = &input;
    done = false;
    cv.notify_one();
  }
  void wait() {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this] { return done; });
  }
};
} // namespace
} // namespace rdfplot

class PlottingUtility::Session::Impl {
public:
  PlotBatchOptions options;
  PlotSessionStats statistics;
  std::thread::id owner = std::this_thread::get_id();
  std::vector<std::unique_ptr<rdfplot::Lane>> lanes;
  std::map<std::pair<bool, std::size_t>, std::unique_ptr<rdfplot::Workspace>>
      workspaces;
  explicit Impl(const PlotBatchOptions &opts) : options(opts) {
    rdfplot::require(options.workers > 0 && options.workers <= 64,
                     "Preparation workers must be in [1,64]");
    rdfplot::require(options.maxOpenFiles > 0 &&
                         options.maxPreparedBytes >= 1024 * 1024,
                     "Invalid plotting resource budget");
    options.workers =
        std::min<std::size_t>(options.workers, options.maxOpenFiles);
    std::lock_guard<std::recursive_mutex> lock(rdfplot::graphicsMutex);
    TVirtualPad::TContext padContext;
    TDirectory::TContext directoryContext(nullptr);
    std::call_once(rdfplot::rootInitialization, rdfplot::initializeRoot);
    for (unsigned i = 0; i < options.workers; ++i)
      lanes.emplace_back(std::make_unique<rdfplot::Lane>(
          options.maxOpenFiles / options.workers,
          options.maxPreparedBytes / options.workers, options.workers > 1));
  }
  ~Impl() {
    lanes.clear();
    std::lock_guard<std::recursive_mutex> lock(rdfplot::graphicsMutex);
    TVirtualPad::TContext context;
    workspaces.clear();
  }
  void checkThread() const {
    rdfplot::require(
        owner == std::this_thread::get_id(),
        "Plotting sessions must be used on their constructing thread");
  }
  PlotResult draw(const PlotRequest &request, rdfplot::Prepared &prepared) {
    auto result = prepared.result;
    if (!result.message.empty())
      return result;
    std::lock_guard<std::recursive_mutex> lock(rdfplot::graphicsMutex);
    TVirtualPad::TContext padContext;
    TDirectory::TContext directoryContext(nullptr);
    const auto start = rdfplot::Clock::now();
    std::vector<PlotPanelConfig> configs = request.panels;
    if (configs.empty() && request.drawRatio && prepared.hasData &&
        prepared.hasMC && request.kind != PlotKind::Heatmap)
      configs = {{PlotPanel::Ratio}, {PlotPanel::Pull}};
    if (configs.empty() && request.drawRatio &&
        request.kind == PlotKind::Overlay && prepared.count > 1)
      configs = {{PlotPanel::Ratio}};
    if (request.kind == PlotKind::Heatmap)
      configs.clear();
    rdfplot::require(configs.size() <= 6,
                     "At most six diagnostic panels are supported");
    auto key =
        std::make_pair(request.kind == PlotKind::Heatmap, configs.size());
    auto &workspace = workspaces[key];
    if (!workspace)
      workspace = std::make_unique<rdfplot::Workspace>(configs.size(),
                                                       key.first, statistics);
    struct ReleaseInputs {
      rdfplot::Workspace &workspace;
      ~ReleaseInputs() {
        workspace.request = nullptr;
        workspace.prepared = nullptr;
      }
    } release{*workspace};
    workspace->configure(request, prepared, configs);
    statistics.renderingSeconds += rdfplot::seconds(start);
    std::vector<int> oldPalette;
    if (key.first) {
      for (int i = 0; i < gStyle->GetNumberOfColors(); ++i)
        oldPalette.push_back(gStyle->GetColorPalette(i));
      gStyle->SetPalette(rdfplot::heatPalette.size(),
                         rdfplot::heatPalette.data());
    }
    struct RestorePalette {
      std::vector<int> &colors;
      ~RestorePalette() {
        if (!colors.empty())
          gStyle->SetPalette(colors.size(), colors.data());
      }
    } restore{oldPalette};
    std::vector<std::string> outputs{request.outputFile};
    outputs.insert(outputs.end(), request.additionalOutputs.begin(),
                   request.additionalOutputs.end());
    std::set<std::string> unique;
    for (const auto &path : outputs) {
      rdfplot::require(!path.empty(), "Plot output path is empty");
      const auto extension = std::filesystem::path(path).extension().string();
      rdfplot::require(
          extension == ".png" || extension == ".pdf" || extension == ".svg" ||
              extension == ".C",
          "Plot outputs must be PNG, PDF, SVG, or ROOT macros (.C)");
      rdfplot::require(
          unique
              .insert(
                  std::filesystem::absolute(path).lexically_normal().string())
              .second,
          "Duplicate plot output: " + path);
    }
    const auto exportStart = rdfplot::Clock::now();
    for (const auto &path : outputs) {
      const std::filesystem::path destination(path);
      if (destination.has_parent_path())
        std::filesystem::create_directories(destination.parent_path());
      // ROOT SaveAs reports failures through its error handler, not a return
      // code. A fresh sibling temp file prevents stale output implying success.
      static const auto exportNonce =
          rdfplot::Clock::now().time_since_epoch().count();
      const auto temp = destination.parent_path() /
                        (destination.stem().string() + ".rdfplot-" +
                         std::to_string(exportNonce) + "-" +
                         std::to_string(rdfplot::nextId++) +
                         destination.extension().string());
      rdfplot::require(!std::filesystem::exists(temp),
                       "Temporary export path already exists");
      struct RemoveTemporary {
        std::filesystem::path path;
        ~RemoveTemporary() {
          std::error_code error;
          std::filesystem::remove(path, error);
        }
      } removeTemporary{temp};
      workspace->canvas->cd();
      workspace->canvas->Modified();
      workspace->canvas->Update();
      if (destination.extension() == ".C")
        workspace->saveMacro(temp);
      else
        workspace->canvas->SaveAs(temp.string().c_str());
      if (!std::filesystem::exists(temp) || !std::filesystem::file_size(temp)) {
        std::filesystem::remove(temp);
        throw std::runtime_error("ROOT export failed: " + path);
      }
      std::filesystem::rename(temp, destination);
      result.outputs.push_back(path);
    }
    statistics.exportSeconds += rdfplot::seconds(exportStart);
    ++statistics.plots;
    result.success = true;
    return result;
  }
};

PlottingUtility::Session::Session(const PlotBatchOptions &options)
    : impl_(std::make_unique<Impl>(options)) {}
PlottingUtility::Session::~Session() = default;
PlotResult PlottingUtility::Session::render(const PlotRequest &request) {
  impl_->checkThread();
  auto &lane = *impl_->lanes[0];
  if (impl_->options.workers > 1) {
    lane.submit(request);
    lane.wait();
  } else
    lane.prepare(request);
  try {
    return impl_->draw(request, lane.prepared);
  } catch (const std::exception &error) {
    auto result = lane.prepared.result;
    result.success = false;
    result.message = error.what();
    return result;
  }
}
std::vector<PlotResult> PlottingUtility::Session::renderBatch(
    const std::vector<PlotRequest> &requests) {
  impl_->checkThread();
  std::vector<PlotResult> results(requests.size());
  // Fixed windows bound the prepared queue and guarantee progress in input
  // order.
  for (std::size_t first = 0; first < requests.size();
       first += impl_->lanes.size()) {
    const auto count = std::min(impl_->lanes.size(), requests.size() - first);
    for (std::size_t i = 0; i < count; ++i) {
      auto &lane = *impl_->lanes[i];
      if (impl_->options.workers > 1)
        lane.submit(requests[first + i]);
      else
        lane.prepare(requests[first + i]);
    }
    for (std::size_t i = 0; i < count; ++i) {
      auto &lane = *impl_->lanes[i];
      if (impl_->options.workers > 1)
        lane.wait();
      try {
        results[first + i] = impl_->draw(requests[first + i], lane.prepared);
      } catch (const std::exception &error) {
        results[first + i] = lane.prepared.result;
        results[first + i].success = false;
        results[first + i].message = error.what();
      }
    }
  }
  return results;
}
PlotSessionStats PlottingUtility::Session::stats() const {
  impl_->checkThread();
  auto out = impl_->statistics;
  for (const auto &lane : impl_->lanes) {
    out.fileOpens += lane->reader.opens;
    out.preparationSeconds += lane->preparationTime;
  }
  return out;
}
