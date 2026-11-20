#include "PlottingInternal.h"
#include <TDirectory.h>
#include <TH2.h>
#include <THnSparse.h>
#include <TKey.h>
#include <TObjString.h>
#include <algorithm>
#include <cmath>
#include <set>

namespace rdfplot {
namespace {
void checkEdges(const std::vector<double> &edges) {
  require(edges.size() >= 2, "At least two bin edges are required");
  for (std::size_t i = 0; i < edges.size(); ++i)
    require(std::isfinite(edges[i]) && (!i || edges[i] > edges[i - 1]),
            "Bin edges must be finite and strictly increasing");
}
bool equalEdges(const std::vector<double> &a, const std::vector<double> &b) {
  if (a.size() != b.size())
    return false;
  for (std::size_t i = 0; i < a.size(); ++i)
    if (std::abs(a[i] - b[i]) >
        1e-9 * std::max({1., std::abs(a[i]), std::abs(b[i])}))
      return false;
  return true;
}
std::vector<double> edgesOf(const TAxis &axis) {
  std::vector<double> out(axis.GetNbins() + 1);
  for (int i = 0; i <= axis.GetNbins(); ++i)
    out[i] = axis.GetBinLowEdge(i + 1);
  return out;
}
// ReadObj avoids retaining every requested histogram in TFile's object cache.
std::unique_ptr<TObject> readObject(TFile &file, const std::string &path) {
  const auto slash = path.find_last_of('/');
  TDirectory *dir = slash == std::string::npos
                        ? &file
                        : file.GetDirectory(path.substr(0, slash).c_str());
  auto *key =
      dir ? dir->GetKey(
                path.substr(slash == std::string::npos ? 0 : slash + 1).c_str())
          : nullptr;
  TDirectory::TContext context(nullptr);
  return std::unique_ptr<TObject>(key ? key->ReadObj() : nullptr);
}
std::string keyOf(const PlotProcessConfig &process) {
  return process.directory.empty()
             ? process.histogramName
             : process.directory + "/" + process.histogramName;
}
void checkArray(const std::vector<double> &a, std::size_t n,
                const std::string &name, bool nonnegative = false) {
  require(a.size() == n, name + " has incompatible bin count");
  for (double v : a)
    require(std::isfinite(v) && (!nonnegative || v >= 0),
            name + " contains invalid values");
}
struct Input {
  std::vector<double> edges, yEdges, values, variance;
  std::vector<std::string> labels, yLabels;
  std::map<std::string, std::vector<double>> variations;
};
void rebinInput(Input &input, const std::vector<double> &edges) {
  checkEdges(edges);
  require(edges.front() == input.edges.front() &&
              edges.back() == input.edges.back(),
          "Rebinning must preserve the full axis range");
  std::vector<std::size_t> offsets;
  for (double edge : edges) {
    auto it = std::find_if(
        input.edges.begin(), input.edges.end(), [edge](double source) {
          return std::abs(edge - source) <=
                 1e-9 * std::max({1., std::abs(edge), std::abs(source)});
        });
    require(it != input.edges.end(),
            "Rebin edges must coincide with source edges");
    offsets.push_back(std::distance(input.edges.begin(), it));
  }
  auto coarsen = [&](std::vector<double> &values) {
    std::vector<double> output(edges.size() - 1, 0);
    for (std::size_t b = 0; b < output.size(); ++b)
      for (auto source = offsets[b]; source < offsets[b + 1]; ++source)
        output[b] += values[source];
    values.swap(output);
  };
  coarsen(input.values);
  coarsen(input.variance);
  for (auto &variation : input.variations)
    coarsen(variation.second);
  input.edges = edges;
  input.labels.clear();
}

Input readInput(Reader::File &file, const PlotProcessConfig &process,
                const PlotRequest &request, std::size_t byteLimit) {
  Input in;
  YAML::Node metadata;
  if (file.schema && !file.schema.IsNull()) {
    require(file.schema["schema_version"].as<int>() == 1,
            "Unsupported embedded histogram schema");
    for (const auto &entry : file.schema["histograms"])
      if (entry["name"].as<std::string>() == process.histogramName) {
        metadata.reset(entry);
        break;
      }
  }
  require(process.directory.empty() || process.selections.empty(),
          "Use either a legacy directory or sparse selections, not both");
  if (metadata && !metadata.IsNull() && metadata["axes"] &&
      process.directory.empty()) {
    const auto axes = metadata["axes"];
    int xAxis = -1, yAxis = -1, systematicAxis = -1;
    std::vector<std::set<int>> selected(axes.size());
    std::vector<int> bins(axes.size());
    for (std::size_t d = 0; d < axes.size(); ++d) {
      const std::string role = axes[d]["role"].as<std::string>();
      bins[d] = axes[d]["bins"].as<int>();
      if (role == "observable")
        xAxis = d;
      if (role == request.heatmapYRole)
        yAxis = d;
      if (role == "systematic")
        systematicAxis = d;
      auto selection = process.selections.find(role);
      if (selection != process.selections.end() && !selection->second.empty()) {
        const auto labels = axes[d]["labels"].as<std::vector<std::string>>();
        for (const auto &label : selection->second) {
          const auto it = std::find(labels.begin(), labels.end(), label);
          require(it != labels.end(),
                  "Unknown sparse category " + role + ": " + label);
          selected[d].insert(1 + std::distance(labels.begin(), it));
        }
      }
    }
    for (const auto &selection : process.selections) {
      bool found = false;
      for (const auto &axis : axes)
        found |= axis["role"].as<std::string>() == selection.first;
      require(found, "Unknown sparse axis role: " + selection.first);
      require(selection.first != "systematic",
              "Use readSystematics to select variations");
    }
    require(xAxis >= 0, "Sparse schema has no observable axis");
    const bool heat = request.kind == PlotKind::Heatmap;
    require(!heat || (yAxis >= 0 && yAxis != xAxis && yAxis != systematicAxis),
            "Invalid sparse heatmap Y axis");
    in.edges = axes[xAxis]["edges"].as<std::vector<double>>();
    if (heat) {
      in.yEdges = axes[yAxis]["edges"].as<std::vector<double>>();
      in.yLabels = axes[yAxis]["labels"].as<std::vector<std::string>>();
    }
    in.labels = axes[xAxis]["labels"].as<std::vector<std::string>>();
    const auto cells =
        static_cast<std::size_t>(bins[xAxis]) * (heat ? bins[yAxis] : 1);
    require(cells <= byteLimit / (2 * sizeof(double)),
            "Sparse projection exceeds prepared-memory budget");
    in.values.assign(cells, 0);
    in.variance.assign(cells, 0);
    std::vector<std::string> systematicLabels;
    int nominal = 1;
    if (systematicAxis >= 0) {
      systematicLabels =
          axes[systematicAxis]["labels"].as<std::vector<std::string>>();
      const auto it = std::find(systematicLabels.begin(),
                                systematicLabels.end(), "Nominal");
      require(it != systematicLabels.end(),
              "Sparse schema has no Nominal variation");
      nominal = 1 + std::distance(systematicLabels.begin(), it);
      if (process.readSystematics && !process.isData && !heat) {
        require(cells <= byteLimit /
                             (sizeof(double) * (2 + systematicLabels.size())),
                "Sparse variations exceed prepared-memory budget");
        for (const auto &label : systematicLabels)
          if (label != "Nominal")
            in.variations[label].assign(cells, 0);
      }
    }
    auto object =
        readObject(*file.file, file.schema["directory"].as<std::string>() +
                                   "/" + metadata["key"].as<std::string>());
    auto *hist = dynamic_cast<THnBase *>(object.get());
    require(hist && hist->GetNdimensions() == static_cast<int>(axes.size()),
            "Sparse histogram/schema mismatch");
    for (std::size_t d = 0; d < axes.size(); ++d)
      require(equalEdges(edgesOf(*hist->GetAxis(d)),
                         axes[d]["edges"].as<std::vector<double>>()),
              "Sparse axis edges disagree with schema");
    std::vector<int> coords(axes.size());
    for (Long64_t b = 0; b < hist->GetNbins(); ++b) {
      const double value = hist->GetBinContent(b, coords.data());
      bool accept = true;
      for (std::size_t d = 0; d < axes.size(); ++d) {
        if (static_cast<int>(d) == systematicAxis)
          continue;
        const bool flow =
            static_cast<int>(d) == xAxis && request.flow == PlotFlow::Fold;
        if ((!flow && (coords[d] < 1 || coords[d] > bins[d])) ||
            (!selected[d].empty() && !selected[d].count(coords[d]))) {
          accept = false;
          break;
        }
      }
      if (!accept)
        continue;
      const auto x = std::clamp(coords[xAxis], 1, bins[xAxis]) - 1;
      const auto index = x + (heat ? (coords[yAxis] - 1) * bins[xAxis] : 0);
      const int systematic =
          systematicAxis < 0 ? nominal : coords[systematicAxis];
      if (systematic == nominal) {
        in.values[index] += value;
        in.variance[index] += hist->GetBinError2(b);
      } else if (systematic > 0 &&
                 systematic <= static_cast<int>(systematicLabels.size())) {
        auto it = in.variations.find(systematicLabels[systematic - 1]);
        if (it != in.variations.end())
          it->second[index] += value;
      }
    }
    return in;
  }

  require(process.selections.empty(),
          "Category selections require an embedded sparse schema; specify an "
          "explicit legacy directory");
  auto object = readObject(*file.file, keyOf(process));
  auto *hist = dynamic_cast<TH1 *>(object.get());
  const bool heat = request.kind == PlotKind::Heatmap;
  require(hist && hist->GetDimension() == (heat ? 2 : 1),
          "Missing histogram or wrong dimension: " + keyOf(process));
  in.edges = edgesOf(*hist->GetXaxis());
  const auto nx = hist->GetNbinsX(), ny = heat ? hist->GetNbinsY() : 1;
  require(static_cast<std::size_t>(nx) * ny <= byteLimit / (2 * sizeof(double)),
          "Histogram exceeds prepared-memory budget");
  in.values.assign(nx * ny, 0);
  in.variance.assign(nx * ny, 0);
  for (int x = 1; x <= nx; ++x)
    in.labels.emplace_back(hist->GetXaxis()->GetBinLabel(x));
  if (heat) {
    in.yEdges = edgesOf(*hist->GetYaxis());
    for (int y = 1; y <= ny; ++y)
      in.yLabels.emplace_back(hist->GetYaxis()->GetBinLabel(y));
  }
  for (int y = 1; y <= ny; ++y)
    for (int x = 1; x <= nx; ++x) {
      const int bin = heat ? hist->GetBin(x, y) : x;
      const int index = (y - 1) * nx + x - 1;
      in.values[index] = hist->GetBinContent(bin);
      in.variance[index] = std::pow(hist->GetBinError(bin), 2);
    }
  if (!heat && request.flow == PlotFlow::Fold) {
    in.values.front() += hist->GetBinContent(0);
    in.variance.front() += std::pow(hist->GetBinError(0), 2);
    in.values.back() += hist->GetBinContent(nx + 1);
    in.variance.back() += std::pow(hist->GetBinError(nx + 1), 2);
  }
  if (process.readSystematics && !heat && !process.isData) {
    // Schema gives an unambiguous list even for extracted histograms. Without
    // it, select only exact <observable>_<nuisance>Up/Down names in this
    // directory.
    const auto slash = keyOf(process).find_last_of('/');
    auto *dir = slash == std::string::npos
                    ? file.file.get()
                    : file.file->GetDirectory(process.directory.c_str());
    TIter next(dir->GetListOfKeys());
    std::set<std::string> names;
    while (auto *key = static_cast<TKey *>(next()))
      names.insert(key->GetName());
    for (const auto &name : names) {
      const auto prefix = process.histogramName + "_";
      if (name.compare(0, prefix.size(), prefix) != 0)
        continue;
      auto label = name.substr(prefix.size());
      if (!(label.size() > 2 &&
            label.compare(label.size() - 2, 2, "Up") == 0) &&
          !(label.size() > 4 &&
            label.compare(label.size() - 4, 4, "Down") == 0))
        continue;
      auto shifted = process;
      shifted.histogramName = name;
      shifted.readSystematics = false;
      auto variation = readInput(file, shifted, request, byteLimit);
      require(equalEdges(in.edges, variation.edges),
              "Variation bin edges disagree");
      in.variations.emplace(label, std::move(variation.values));
    }
  }
  return in;
}
} // namespace

Reader::~Reader() = default;
Reader::File &Reader::open(const std::string &path) {
  for (auto it = files.begin(); it != files.end(); ++it)
    if (it->path == path) {
      files.splice(files.end(), files, it);
      return files.back();
    }
  if (files.size() == maxFiles)
    files.pop_front();
  TDirectory::TContext context(nullptr);
  auto file = std::unique_ptr<TFile>(TFile::Open(path.c_str(), "READ"));
  require(file && !file->IsZombie(), "Unable to open input file: " + path);
  YAML::Node schema;
  auto schemaObject = readObject(*file, "__rdf_histogram_schema_v1");
  if (auto *text = dynamic_cast<TObjString *>(schemaObject.get()))
    schema = YAML::Load(text->GetString().Data());
  files.push_back({path, std::move(file), schema});
  ++opens;
  return files.back();
}

void Reader::prepare(const PlotRequest &request, Prepared &out) {
  out.result = {};
  out.count = 0;
  out.hasData = out.hasMC = false;
  out.edges = request.binEdges;
  out.yEdges = request.yBinEdges;
  out.labels = request.binLabels;
  out.yLabels = request.yBinLabels;
  out.matrix = request.matrix;
  const bool heat = request.kind == PlotKind::Heatmap;
  require(!request.processes.empty() || (heat && !request.matrix.empty()),
          "No input processes provided");
  std::map<std::string, std::pair<std::vector<double>, std::vector<double>>>
      shifts;
  std::size_t n = out.edges.empty() ? 0 : out.edges.size() - 1;
  auto initialize = [&] {
    checkEdges(out.edges);
    require(n <= maxBytes /
                     (sizeof(double) * (16 + 2 * request.processes.size())),
            "Plot exceeds prepared-memory budget");
    for (auto *array :
         {&out.data, &out.dataLow2, &out.dataHigh2, &out.total, &out.variance,
          &out.signal, &out.systLow, &out.systHigh})
      array->assign(n, 0);
  };
  bool initialized = false;
  if (n && !heat) {
    initialize();
    initialized = true;
  }
  if (heat && !out.matrix.empty()) {
    checkEdges(out.edges);
    checkEdges(out.yEdges);
    checkArray(out.matrix, n * (out.yEdges.size() - 1), "Heatmap");
    require(out.matrix.size() <= maxBytes / sizeof(double),
            "Heatmap exceeds prepared-memory budget");
    require(request.processes.empty(),
            "Specify a matrix or file processes, not both");
  }
  for (const auto &process : request.processes) {
    require(std::isfinite(process.scale) && std::isfinite(process.displayScale),
            "Nonfinite process scale");
    require(!process.isData || !process.overlay,
            "Data cannot be an overlay process");
    std::size_t slot = out.count;
    if (!heat && !process.isData && !process.group.empty())
      for (std::size_t i = 0; i < out.count; ++i)
        if (out.series[i].config->group == process.group) {
          slot = i;
          break;
        }
    if (!heat && !process.isData && slot == out.count) {
      if (out.series.size() == out.count)
        out.series.emplace_back();
      out.series[slot].config = &process;
      out.series[slot].values.clear();
      out.series[slot].variance.clear();
      ++out.count;
    } else if (!heat && !process.isData) {
      const auto &prior = *out.series[slot].config;
      require(prior.isSignal == process.isSignal &&
                  prior.overlay == process.overlay &&
                  prior.displayScale == process.displayScale,
              "Grouped processes must agree on signal/overlay/display scale");
    }
    auto paths = process.files;
    const bool memory = !process.values.empty();
    if (!memory && paths.empty())
      paths.push_back(request.metaFile);
    if (memory)
      paths = {""};
    double normalizationScale = process.scale;
    if (!memory && !process.normalizationHistogram.empty()) {
      double norm = 0;
      bool valid = true;
      for (const auto &path : paths) {
        auto &file = open(path);
        auto object = readObject(*file.file, process.normalizationHistogram);
        auto *hist = dynamic_cast<TH1 *>(object.get());
        valid &= hist && hist->GetDimension() == 1;
        if (hist)
          norm += hist->GetBinContent(1);
      }
      if (!valid || !std::isfinite(norm) || norm == 0) {
        require(!request.strictNormalization,
                "Missing/invalid normalization counter: " +
                    process.normalizationHistogram);
        out.result.warnings.push_back("Using manual scale: invalid counter " +
                                      process.normalizationHistogram);
      } else
        normalizationScale /= norm;
    }
    // Shared nuisance identity: add each sample's variation-minus-nominal.
    // A genuinely absent variation contributes zero shift, an empty stored
    // variation contributes minus nominal (migration out of the region).
    for (const auto &path : paths) {
      Input in;
      const double scale = normalizationScale;
      if (memory) {
        require(!heat, "Use matrix for in-memory heatmaps");
        in.edges = request.binEdges;
        in.values = process.values;
        in.variance = process.variances;
        require(!in.variance.empty(),
                "In-memory inputs require variances (sumw2)");
      } else {
        auto &file = open(path);
        in = readInput(file, process, request, maxBytes);
      }
      if (!heat && request.rebin && !out.edges.empty() &&
          !equalEdges(out.edges, in.edges))
        rebinInput(in, out.edges);
      if (out.edges.empty()) {
        out.edges = in.edges;
        n = out.edges.size() - 1;
      }
      require(equalEdges(out.edges, in.edges),
              "Input histogram bin edges disagree");
      if (out.labels.empty())
        out.labels = in.labels;
      if (heat) {
        if (out.yEdges.empty())
          out.yEdges = in.yEdges;
        require(equalEdges(out.yEdges, in.yEdges), "Input Y edges disagree");
        if (out.yLabels.empty())
          out.yLabels = in.yLabels;
        if (out.matrix.empty())
          out.matrix.assign(in.values.size(), 0);
        checkArray(in.values, out.matrix.size(), "Heatmap");
        for (std::size_t b = 0; b < in.values.size(); ++b)
          out.matrix[b] += in.values[b] * scale;
        continue;
      }
      if (!initialized) {
        initialize();
        initialized = true;
      }
      checkArray(in.values, n, "Yields");
      checkArray(in.variance, n, "Variances", true);
      if (!process.referenceCovariance.empty())
        checkArray(process.referenceCovariance, n, "Reference covariance");
      if (!process.errorLow.empty() || !process.errorHigh.empty()) {
        require(memory && process.isData,
                "Asymmetric errors apply to in-memory data");
        checkArray(process.errorLow, n, "Lower errors", true);
        checkArray(process.errorHigh, n, "Upper errors", true);
      }
      if (!process.isData) {
        auto &series = out.series[slot];
        if (series.values.empty()) {
          series.values.assign(n, 0);
          series.variance.assign(n, 0);
        }
        for (std::size_t b = 0; b < n; ++b) {
          series.values[b] += in.values[b] * scale;
          series.variance[b] += in.variance[b] * scale * scale;
        }
        out.hasMC |= !process.overlay;
      }
      for (std::size_t b = 0; b < n; ++b) {
        if (process.isData) {
          out.hasData = true;
          out.data[b] += in.values[b] * scale;
          const double low = process.errorLow.empty()
                                 ? std::sqrt(in.variance[b])
                                 : process.errorLow[b];
          const double high = process.errorHigh.empty()
                                  ? std::sqrt(in.variance[b])
                                  : process.errorHigh[b];
          out.dataLow2[b] += low * low * scale * scale;
          out.dataHigh2[b] += high * high * scale * scale;
        } else if (!process.overlay) {
          out.total[b] += in.values[b] * scale;
          out.variance[b] += in.variance[b] * scale * scale;
          if (process.isSignal)
            out.signal[b] += in.values[b] * scale;
        }
      }
      if (!process.isData && !process.overlay) {
        for (const auto &variation : in.variations) {
          std::string name = variation.first;
          bool up;
          if (name.size() > 2 && name.compare(name.size() - 2, 2, "Up") == 0) {
            name.resize(name.size() - 2);
            up = true;
          } else if (name.size() > 4 &&
                     name.compare(name.size() - 4, 4, "Down") == 0) {
            name.resize(name.size() - 4);
            up = false;
          } else
            continue;
          auto &pair = shifts[name];
          auto &values = up ? pair.first : pair.second;
          if (values.empty())
            values.assign(n, 0);
          for (std::size_t b = 0; b < n; ++b)
            values[b] += (variation.second[b] - in.values[b]) * scale;
        }
      }
    }
    if (!heat && !process.isData && !process.overlay)
      for (const auto &variation : process.variations) {
        require(!variation.name.empty(), "Nuisance name must not be empty");
        require(memory, "Explicit variations accompany in-memory values; file "
                        "inputs use readSystematics");
        auto &pair = shifts[variation.name];
        for (int direction = 0; direction < 2; ++direction) {
          const auto &values = direction == 0 ? variation.up : variation.down;
          if (values.empty())
            continue;
          checkArray(values, n, "Variation");
          auto &target = direction == 0 ? pair.first : pair.second;
          if (target.empty())
            target.assign(n, 0);
          for (std::size_t b = 0; b < n; ++b)
            target[b] += (values[b] - process.values[b]) * process.scale;
        }
      }
    require(n <= maxBytes /
                     (sizeof(double) *
                      (16 + 2 * request.processes.size() + 2 * shifts.size())),
            "Nuisance arrays exceed prepared-memory budget");
  }
  checkEdges(out.edges);
  require(out.labels.empty() || out.labels.size() == n,
          "Bin labels disagree with bin count");
  if (heat) {
    checkEdges(out.yEdges);
    require(out.yLabels.empty() || out.yLabels.size() == out.yEdges.size() - 1,
            "Y labels disagree with bin count");
    checkArray(out.matrix, n * (out.yEdges.size() - 1), "Heatmap");
    return;
  }
  require(request.blindBins.empty() || request.blindBins.size() == n,
          "Blinding mask disagrees with bin count");
  if (!request.systematicLow.empty() || !request.systematicHigh.empty()) {
    checkArray(request.systematicLow, n, "Lower systematic errors", true);
    checkArray(request.systematicHigh, n, "Upper systematic errors", true);
  }
  for (std::size_t b = 0; b < n; ++b) {
    for (const auto *values : {&out.total, &out.variance, &out.signal,
                               &out.data, &out.dataLow2, &out.dataHigh2})
      require(std::isfinite((*values)[b]),
              "Nonfinite value after scaling/aggregation");
    double low2 = request.systematicLow.empty()
                      ? 0
                      : std::pow(request.systematicLow[b], 2);
    double high2 = request.systematicHigh.empty()
                       ? 0
                       : std::pow(request.systematicHigh[b], 2);
    for (const auto &nuisance : shifts) {
      const double up =
          nuisance.second.first.empty() ? 0 : nuisance.second.first[b];
      const double down =
          nuisance.second.second.empty() ? 0 : nuisance.second.second[b];
      low2 += std::pow(std::max({0., -up, -down}), 2);
      high2 += std::pow(std::max({0., up, down}), 2);
    }
    out.systLow[b] = std::sqrt(low2);
    out.systHigh[b] = std::sqrt(high2);
    require(std::isfinite(low2) && std::isfinite(high2),
            "Nonfinite systematic variance");
    out.result.mcIntegral += out.total[b];
    out.result.mcVariance += out.variance[b];
    if (request.blindBins.empty() || !request.blindBins[b])
      out.result.dataIntegral += out.data[b];
    if (request.density) {
      const double width = out.edges[b + 1] - out.edges[b];
      for (auto *values :
           {&out.total, &out.data, &out.signal, &out.systLow, &out.systHigh})
        (*values)[b] /= width;
      for (auto *values : {&out.variance, &out.dataLow2, &out.dataHigh2})
        (*values)[b] /= width * width;
      for (std::size_t i = 0; i < out.count; ++i) {
        out.series[i].values[b] /= width;
        out.series[i].variance[b] /= width * width;
      }
    }
  }
  if (request.kind == PlotKind::Overlay)
    require(request.referenceProcess < out.count,
            "Overlay reference process is out of range");
}
} // namespace rdfplot
