#include <PlottingUtility.h>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

namespace {
template <class T>
void get(const YAML::Node &node, const char *key, T &target) {
  if (node[key])
    target = node[key].as<T>();
}
PlotPanel parsePanel(const std::string &name) {
  if (name == "ratio")
    return PlotPanel::Ratio;
  if (name == "pull")
    return PlotPanel::Pull;
  if (name == "signal_background")
    return PlotPanel::SignalBackground;
  if (name == "significance")
    return PlotPanel::Significance;
  throw std::invalid_argument("Unknown plot panel: " + name);
}
} // namespace

std::vector<PlotRequest>
PlottingUtility::readRequests(const std::string &configFile) {
  std::ifstream stream(configFile);
  if (!stream)
    throw std::invalid_argument("Cannot open plot configuration: " +
                                configFile);
  std::ostringstream text;
  text << stream.rdbuf();
  return parseRequests(text.str());
}

std::vector<PlotRequest>
PlottingUtility::parseRequests(const std::string &yamlOrJson) {
  auto root = YAML::Load(yamlOrJson);
  if (root.IsMap()) {
    if (!root["schema_version"] || root["schema_version"].as<int>() != 1)
      throw std::invalid_argument("Plot config requires schema_version: 1");
    root = root["plots"];
  }
  if (!root.IsSequence())
    throw std::invalid_argument("Plot config must be a sequence of requests");
  std::vector<PlotRequest> requests;
  for (const auto &node : root) {
    if (!node.IsMap())
      throw std::invalid_argument("Plot request must be a mapping");
    PlotRequest req;
#define FIELD(name) get(node, #name, req.name)
    FIELD(metaFile);
    FIELD(outputFile);
    FIELD(title);
    FIELD(xAxisTitle);
    FIELD(yAxisTitle);
    FIELD(logY);
    FIELD(drawRatio);
    FIELD(binEdges);
    FIELD(binLabels);
    FIELD(blindBins);
    FIELD(systematicLow);
    FIELD(systematicHigh);
    FIELD(experimentLabel);
    FIELD(luminosityLabel);
    FIELD(annotations);
    FIELD(density);
    FIELD(strictNormalization);
    FIELD(signalOverlay);
    FIELD(additionalOutputs);
    FIELD(matrix);
    FIELD(yBinEdges);
    FIELD(yBinLabels);
    FIELD(heatmapYRole);
    FIELD(zAxisTitle);
    FIELD(horizontalSeparators);
    FIELD(symmetricColorRange);
    FIELD(logZ);
    FIELD(referenceProcess);
    FIELD(experimentExtra);
    FIELD(rebin);
#undef FIELD
    if (node["kind"]) {
      auto kind = node["kind"].as<std::string>();
      if (kind == "stack")
        req.kind = PlotKind::Stack;
      else if (kind == "overlay")
        req.kind = PlotKind::Overlay;
      else if (kind == "heatmap")
        req.kind = PlotKind::Heatmap;
      else
        throw std::invalid_argument("Unknown plot kind: " + kind);
    }
    if (node["flow"]) {
      auto flow = node["flow"].as<std::string>();
      if (flow == "drop")
        req.flow = PlotFlow::Drop;
      else if (flow == "fold")
        req.flow = PlotFlow::Fold;
      else
        throw std::invalid_argument("Unknown flow policy: " + flow);
    }
    if (node["experiment"]) {
      const auto experiment = node["experiment"].as<std::string>();
      if (experiment == "CMS" || experiment == "cms")
        req.experiment = PlotExperiment::CMS;
      else if (experiment == "ATLAS" || experiment == "atlas")
        req.experiment = PlotExperiment::ATLAS;
      else if (experiment == "LHCb" || experiment == "lhcb")
        req.experiment = PlotExperiment::LHCb;
      else if (experiment == "ALICE" || experiment == "alice")
        req.experiment = PlotExperiment::ALICE;
      else if (experiment == "custom")
        req.experiment = PlotExperiment::Custom;
      else
        throw std::invalid_argument("Unknown experiment header: " + experiment);
    }
    if (auto style = node["style"]) {
#define FIELD(name) get(style, #name, req.style.name)
      FIELD(width);
      FIELD(height);
      FIELD(labelSize);
      FIELD(titleSize);
      FIELD(legendSize);
      FIELD(leftMargin);
      FIELD(rightMargin);
      FIELD(legendColumns);
      FIELD(grid);
      FIELD(legendInside);
      FIELD(legendWidth);
      FIELD(annotationBoxWidth);
#undef FIELD
    }
    if (auto panels = node["panels"])
      for (const auto &panel : panels) {
        PlotPanelConfig config;
        config.type =
            parsePanel(panel.IsScalar() ? panel.as<std::string>()
                                        : panel["type"].as<std::string>());
        if (panel.IsMap()) {
          get(panel, "minimum", config.minimum);
          get(panel, "maximum", config.maximum);
          get(panel, "autoRange", config.autoRange);
        }
        req.panels.push_back(config);
      }
    if (auto processes = node["processes"])
      for (const auto &proc : processes) {
        PlotProcessConfig config;
#define FIELD(name) get(proc, #name, config.name)
        FIELD(directory);
        FIELD(histogramName);
        FIELD(legendLabel);
        FIELD(scale);
        FIELD(normalizationHistogram);
        FIELD(isData);
        FIELD(files);
        FIELD(isSignal);
        FIELD(overlay);
        FIELD(displayScale);
        FIELD(group);
        FIELD(values);
        FIELD(variances);
        FIELD(errorLow);
        FIELD(errorHigh);
        FIELD(selections);
        FIELD(readSystematics);
        FIELD(colorHex);
        FIELD(referenceCovariance);
#undef FIELD
        if (proc["color"]) {
          const auto text = proc["color"].as<std::string>();
          if (!text.empty() && text[0] == '#')
            config.colorHex = text;
          else
            config.color = proc["color"].as<int>();
        }
        if (auto variations = proc["variations"])
          for (const auto &variation : variations) {
            PlotVariation configVariation;
            get(variation, "name", configVariation.name);
            get(variation, "up", configVariation.up);
            get(variation, "down", configVariation.down);
            config.variations.push_back(std::move(configVariation));
          }
        req.processes.push_back(std::move(config));
      }
    requests.push_back(std::move(req));
  }
  return requests;
}
