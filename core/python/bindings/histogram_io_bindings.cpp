#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <TFile.h>
#include <TAxis.h>
#include <TDirectory.h>
#include <THnSparse.h>
#include <TH1F.h>
#include <TList.h>
#include <TROOT.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace py = pybind11;

namespace {
void enableRootThreadSafety() {
  static std::once_flag initialized;
  std::call_once(initialized, [] { ROOT::EnableThreadSafety(); });
}

struct SparseProjectionRequest {
  std::string histogramPath;
  std::vector<int> outputAxes;
  std::vector<std::vector<std::int64_t>> axisMaps;
  std::vector<py::ssize_t> outputShape;
  bool includeSumw2 = true;
};

SparseProjectionRequest parseProjectionRequest(py::handle value) {
  const auto fields = py::reinterpret_borrow<py::sequence>(value);
  if (fields.size() != 5)
    throw std::invalid_argument(
        "A sparse projection request must contain five fields");

  SparseProjectionRequest request;
  request.histogramPath = py::cast<std::string>(fields[0]);
  request.outputAxes = py::cast<std::vector<int>>(fields[1]);
  request.outputShape = py::cast<std::vector<py::ssize_t>>(fields[3]);
  request.includeSumw2 = py::cast<bool>(fields[4]);

  const auto maps = py::reinterpret_borrow<py::sequence>(fields[2]);
  request.axisMaps.reserve(maps.size());
  using MapArray = py::array_t<std::int64_t,
                               py::array::c_style | py::array::forcecast>;
  for (const auto mapValue : maps) {
    auto map = MapArray::ensure(mapValue);
    if (!map)
      throw py::type_error("Sparse projection axis maps must be NumPy arrays");
    const auto info = map.request();
    if (info.ndim != 1)
      throw std::invalid_argument("Sparse projection axis maps must be 1D");
    const auto *data = static_cast<const std::int64_t *>(info.ptr);
    request.axisMaps.emplace_back(data, data + info.shape[0]);
  }
  return request;
}

std::vector<SparseProjectionRequest>
parseProjectionRequests(const py::sequence &requests) {
  std::vector<SparseProjectionRequest> parsed;
  parsed.reserve(requests.size());
  for (const auto request : requests)
    parsed.push_back(parseProjectionRequest(request));
  return parsed;
}

py::tuple projectSparseHistogram(THnSparse *histogram,
                                const SparseProjectionRequest &request,
                                const std::string &fileName) {
  if (!histogram)
    throw std::runtime_error("Missing THnSparse histogram " +
                             request.histogramPath + " in " + fileName);

  const auto dimensions = histogram->GetNdimensions();
  if (request.outputAxes.size() != static_cast<std::size_t>(dimensions) ||
      request.axisMaps.size() != static_cast<std::size_t>(dimensions) ||
      request.outputShape.empty())
    throw std::invalid_argument("Sparse projection mapping has wrong dimensions");

  py::ssize_t outputSize = 1;
  for (const auto bins : request.outputShape) {
    if (bins <= 0 ||
        outputSize > std::numeric_limits<py::ssize_t>::max() / bins)
      throw std::invalid_argument("Sparse projection output shape is invalid");
    outputSize *= bins;
  }
  std::vector<py::ssize_t> strides(request.outputShape.size(), 1);
  for (std::size_t axis = request.outputShape.size(); axis-- > 1;)
    strides[axis - 1] = strides[axis] * request.outputShape[axis];

  std::vector<int> sourceBins;
  sourceBins.reserve(dimensions);
  for (int axis = 0; axis < dimensions; ++axis) {
    const int outputAxis = request.outputAxes[axis];
    if (outputAxis < 0 ||
        static_cast<std::size_t>(outputAxis) >= request.outputShape.size())
      throw std::invalid_argument("Sparse projection output axis is invalid");
    const auto bins = histogram->GetAxis(axis)->GetNbins();
    const auto &destinations = request.axisMaps[axis];
    if (destinations.size() != static_cast<std::size_t>(bins))
      throw std::invalid_argument("Sparse projection axis map has wrong size");
    for (const auto destination : destinations) {
      if (destination < -1 ||
          destination >= request.outputShape[static_cast<std::size_t>(outputAxis)])
        throw std::invalid_argument("Sparse projection destination is out of range");
    }
    sourceBins.push_back(bins);
  }

  py::array_t<double> contents(request.outputShape);
  py::array_t<double> sumw2(request.outputShape);
  auto *contentData = contents.mutable_data();
  auto *sumw2Data = sumw2.mutable_data();
  std::fill_n(contentData, outputSize, 0.0);
  std::fill_n(sumw2Data, outputSize, 0.0);

  const auto entries = histogram->GetNbins();
  {
    py::gil_scoped_release release;
    std::vector<Int_t> coordinates(static_cast<std::size_t>(dimensions));
    for (Long64_t entry = 0; entry < entries; ++entry) {
      const double value = histogram->GetBinContent(entry, coordinates.data());
      double variance = request.includeSumw2
                           ? histogram->GetBinError2(entry)
                           : 0.0;
      if (std::isfinite(variance) && variance < 0.0)
        variance = 0.0;
      py::ssize_t flatOutput = 0;
      bool selected = true;
      for (int axis = 0; axis < dimensions; ++axis) {
        const auto coordinate = coordinates[axis];
        if (coordinate < 1 || coordinate > sourceBins[axis]) {
          selected = false;
          break;
        }
        const auto destination = request.axisMaps[axis][coordinate - 1];
        if (destination < 0) {
          selected = false;
          break;
        }
        flatOutput += static_cast<py::ssize_t>(destination) *
                      strides[request.outputAxes[axis]];
      }
      if (selected) {
        contentData[flatOutput] += value;
        sumw2Data[flatOutput] += variance;
      }
    }
  }
  return py::make_tuple(std::move(contents), std::move(sumw2));
}

std::pair<TDirectory *, std::string>
directoryAndObjectName(TFile &file, const std::string &path) {
  const auto separator = path.rfind('/');
  if (separator == std::string::npos)
    return {&file, path};
  const auto directoryPath = path.substr(0, separator);
  const auto objectName = path.substr(separator + 1);
  auto *directory = file.GetDirectory(directoryPath.c_str());
  if (!directory || objectName.empty())
    throw std::runtime_error("Missing ROOT directory for sparse histogram " +
                             path);
  return {directory, objectName};
}

struct DirectoryObjectGuard {
  TDirectory *directory;
  TObject *object;

  void reset() {
    if (!object)
      return;
    if (auto *objects = directory->GetList())
      objects->Remove(object);
    delete object;
    object = nullptr;
    directory = nullptr;
  }

  ~DirectoryObjectGuard() { reset(); }
};

py::tuple readSparseBins(const std::string &fileName,
                         const std::string &histogramPath) {
  enableRootThreadSafety();
  std::unique_ptr<TFile> file(TFile::Open(fileName.c_str(), "READ"));
  if (!file || file->IsZombie())
    throw std::runtime_error("Cannot open sparse histogram file: " + fileName);
  auto *histogram = dynamic_cast<THnSparse *>(file->Get(histogramPath.c_str()));
  if (!histogram)
    throw std::runtime_error("Missing THnSparse histogram " + histogramPath +
                             " in " + fileName);

  const auto entries = static_cast<py::ssize_t>(histogram->GetNbins());
  const auto dimensions = static_cast<py::ssize_t>(histogram->GetNdimensions());
  if (entries < 0 || dimensions <= 0 ||
      entries > std::numeric_limits<py::ssize_t>::max() / dimensions)
    throw std::runtime_error("Invalid THnSparse dimensions in " + fileName +
                             ":" + histogramPath);

  py::array_t<std::int32_t> coordinates({entries, dimensions});
  py::array_t<double> contents(entries);
  py::array_t<double> sumw2(entries);
  auto *coordinateData = coordinates.mutable_data();
  auto *contentData = contents.mutable_data();
  auto *sumw2Data = sumw2.mutable_data();

  {
    py::gil_scoped_release release;
    std::vector<Int_t> coordinateBuffer(static_cast<std::size_t>(dimensions));
    for (py::ssize_t entry = 0; entry < entries; ++entry) {
      contentData[entry] = histogram->GetBinContent(
          static_cast<Long64_t>(entry), coordinateBuffer.data());
      sumw2Data[entry] = histogram->GetBinError2(static_cast<Long64_t>(entry));
      auto *row = coordinateData + entry * dimensions;
      for (py::ssize_t axis = 0; axis < dimensions; ++axis)
        row[axis] = static_cast<std::int32_t>(coordinateBuffer[axis]);
    }
  }
  return py::make_tuple(std::move(coordinates), std::move(contents),
                        std::move(sumw2));
}

py::tuple projectSparseBins(
    const std::string &fileName, const std::string &histogramPath,
    const std::vector<int> &outputAxes,
    const std::vector<py::array_t<std::int64_t, py::array::c_style>> &axisMaps,
    const std::vector<py::ssize_t> &outputShape, bool includeSumw2) {
  enableRootThreadSafety();
  std::unique_ptr<TFile> file(TFile::Open(fileName.c_str(), "READ"));
  if (!file || file->IsZombie())
    throw std::runtime_error("Cannot open sparse histogram file: " + fileName);
  auto *histogram = dynamic_cast<THnSparse *>(file->Get(histogramPath.c_str()));
  if (!histogram)
    throw std::runtime_error("Missing THnSparse histogram " + histogramPath +
                             " in " + fileName);

  SparseProjectionRequest request;
  request.histogramPath = histogramPath;
  request.outputAxes = outputAxes;
  request.outputShape = outputShape;
  request.includeSumw2 = includeSumw2;
  request.axisMaps.reserve(axisMaps.size());
  for (const auto &map : axisMaps) {
    const auto info = map.request();
    if (info.ndim != 1)
      throw std::invalid_argument("Sparse projection axis maps must be 1D");
    const auto *data = static_cast<const std::int64_t *>(info.ptr);
    request.axisMaps.emplace_back(data, data + info.shape[0]);
  }
  return projectSparseHistogram(histogram, request, fileName);
}

py::list projectSparseBinsBatch(const std::string &fileName,
                                const py::sequence &requests) {
  enableRootThreadSafety();
  const auto parsed = parseProjectionRequests(requests);
  py::list results;
  if (parsed.empty())
    return results;

  std::unique_ptr<TFile> file;
  {
    py::gil_scoped_release release;
    file.reset(TFile::Open(fileName.c_str(), "READ"));
  }
  if (!file || file->IsZombie())
    throw std::runtime_error("Cannot open sparse histogram file: " + fileName);

  for (const auto &request : parsed) {
    TDirectory *directory = nullptr;
    TObject *object = nullptr;
    std::string objectName;
    {
      py::gil_scoped_release release;
      auto location = directoryAndObjectName(*file, request.histogramPath);
      directory = location.first;
      objectName = std::move(location.second);
      object = directory->Get(objectName.c_str());
    }
    if (!object)
      throw std::runtime_error("Missing sparse histogram " +
                               request.histogramPath + " in " + fileName);
    DirectoryObjectGuard cleanup{directory, object};
    auto *histogram = dynamic_cast<THnSparse *>(object);
    if (!histogram)
      throw std::runtime_error("Object " + request.histogramPath +
                               " is not a THnSparse in " + fileName);
    results.append(projectSparseHistogram(histogram, request, fileName));
    {
      py::gil_scoped_release release;
      cleanup.reset();
    }
  }
  {
    py::gil_scoped_release release;
    file.reset();
  }
  return results;
}

struct SparseSliceRequest {
  std::string path, name;
  std::vector<int> axes; // channel, control, sample, systematic, observable
  std::vector<std::vector<std::string>> labels;
  std::vector<double> edges;
};

// Called only after the Python schema/axis validation and repair pass. Keep
// histogram objects in C++, retain empty category combinations, and bound
// resident input/output objects to one source histogram at a time.
std::size_t extractSparseSlices(const std::string &fileName,
                               const py::sequence &specifications) {
  std::vector<SparseSliceRequest> requests;
  for (const auto item : specifications) {
    const auto fields = py::cast<py::tuple>(item);
    if (fields.size() != 5) throw std::invalid_argument("Invalid sparse slice request");
    SparseSliceRequest request{py::cast<std::string>(fields[0]),
        py::cast<std::string>(fields[1]), py::cast<std::vector<int>>(fields[2]),
        py::cast<std::vector<std::vector<std::string>>>(fields[3]),
        py::cast<std::vector<double>>(fields[4])};
    if (request.axes.size() != 5 || request.labels.size() != 4 || request.edges.size() < 2)
      throw std::invalid_argument("Invalid sparse slice dimensions");
    requests.push_back(std::move(request));
  }
  enableRootThreadSafety();
  py::gil_scoped_release release;
  TDirectory::TContext context;
  std::unique_ptr<TFile> file(TFile::Open(fileName.c_str(), "UPDATE"));
  if (!file || file->IsZombie() || !file->IsWritable())
    throw std::runtime_error("Cannot update histogram file " + fileName);
  auto load = [&](const SparseSliceRequest &request) {
    auto location = directoryAndObjectName(*file, request.path);
    auto *object = location.first->Get(location.second.c_str());
    auto *histogram = dynamic_cast<THnSparse *>(object);
    if (!histogram) throw std::runtime_error("Missing THnSparse " + request.path);
    if (histogram->GetNdimensions() != 5)
      throw std::invalid_argument("Sparse slice extraction requires five axes");
    std::set<int> roles;
    for (auto axis : request.axes) {
      if (axis < 0 || axis >= 5 || !roles.insert(axis).second)
        throw std::invalid_argument("Invalid sparse slice axis roles");
    }
    return std::make_pair(location.first, histogram);
  };
  std::set<int> samples;
  std::array<Int_t, 5> coordinates;
  for (const auto &request : requests) {
    const auto loaded = load(request);
    DirectoryObjectGuard guard{loaded.first, loaded.second};
    for (Long64_t bin = 0; bin < loaded.second->GetNbins(); ++bin) {
      const auto value = loaded.second->GetBinContent(bin, coordinates.data());
      if ((value != 0 || loaded.second->GetBinError2(bin) != 0) &&
          coordinates[request.axes[2]] > 0)
        samples.insert(coordinates[request.axes[2]]);
    }
  }
  std::size_t written = 0;
  std::map<std::string, TDirectory *> directories;
  for (const auto &request : requests) {
    const auto loaded = load(request);
    auto *source = loaded.second;
    DirectoryObjectGuard guard{loaded.first, source};
    struct SliceBin { int observable; double content, variance; };
    std::map<std::array<int, 4>, std::vector<SliceBin>> populated;
    const auto &labels = request.labels;
    for (Long64_t bin = 0; bin < source->GetNbins(); ++bin) {
      const auto value = source->GetBinContent(bin, coordinates.data());
      const auto variance = source->GetBinError2(bin);
      if (value == 0 && variance == 0) continue;
      std::array<int, 4> key;
      bool valid = true;
      for (int axis = 0; axis < 4; ++axis) {
        key[axis] = coordinates[request.axes[axis]];
        valid &= key[axis] > 0 && key[axis] <= static_cast<int>(labels[axis].size());
      }
      if (valid) populated[key].push_back({coordinates[request.axes[4]], value, variance});
    }
    // Write one slice at a time. Most combinations are empty; retaining all
    // TH1 objects until the end needlessly multiplies resident memory.
    for (std::size_t ch = 0; ch < labels[0].size(); ++ch)
      for (std::size_t cr = 0; cr < labels[1].size(); ++cr)
        for (const auto sample : samples) {
          if (sample > static_cast<int>(labels[2].size())) continue;
          const std::string directoryPath = labels[0][ch] + "_" + labels[1][cr] +
              "/" + labels[2][sample-1];
          auto &directory = directories[directoryPath];
          if (!directory) {
            directory = file.get();
            std::size_t begin = 0;
            while (begin < directoryPath.size()) {
              auto end = directoryPath.find('/', begin);
              if (end == std::string::npos) end = directoryPath.size();
              const auto part = directoryPath.substr(begin, end-begin);
              auto *next = directory->GetDirectory(part.c_str());
              if (!next) next = directory->mkdir(part.c_str());
              if (!next) throw std::runtime_error("Cannot create " + directoryPath);
              directory = next;
              begin = end + 1;
            }
          }
          for (std::size_t syst = 0; syst < labels[3].size(); ++syst) {
            if (syst >= static_cast<std::size_t>(source->GetAxis(request.axes[3])->GetNbins())) continue;
            auto name = request.name;
            if (labels[3][syst] != "Nominal") name += "_" + labels[3][syst];
            const auto title = request.name + ";" + request.name + ";Counts";
            TDirectory::TContext detached(nullptr);
            TH1F histogram(name.c_str(), title.c_str(), request.edges.size()-1, request.edges.data());
            histogram.Sumw2();
            histogram.SetDirectory(nullptr);
            const std::array<int, 4> key{static_cast<int>(ch+1), static_cast<int>(cr+1),
                                       sample, static_cast<int>(syst+1)};
            if (const auto slice = populated.find(key); slice != populated.end())
              for (const auto &bin : slice->second) {
                histogram.SetBinContent(bin.observable, bin.content);
                histogram.SetBinError(bin.observable, std::sqrt(std::max(0., bin.variance)));
              }
            TDirectory::TContext destination(directory);
            if (histogram.Write(name.c_str(), TObject::kOverwrite) <= 0)
              throw std::runtime_error("Cannot write histogram slice in " + fileName);
            ++written;
          }
        }
  }

  return written;
}

class SystematicAccumulator {
public:
  SystematicAccumulator(std::size_t channels, std::size_t bins)
      : channels_(channels), bins_(bins) {
    if (channels_ == 0 || bins_ == 0 ||
        channels_ > std::numeric_limits<std::size_t>::max() / bins_)
      throw std::invalid_argument("Systematic accumulator shape is invalid");
  }

  void add(
      const py::array_t<double,
                        py::array::c_style | py::array::forcecast> &values,
      const std::vector<std::pair<std::string, std::string>> &variationKeys,
      double scale) {
    if (!std::isfinite(scale))
      throw std::invalid_argument("Systematic scale must be finite");
    const auto info = values.request();
    if (info.ndim != 3 ||
        static_cast<std::size_t>(info.shape[1]) != channels_ ||
        static_cast<std::size_t>(info.shape[2]) != bins_ ||
        variationKeys.size() != static_cast<std::size_t>(info.shape[0]))
      throw std::invalid_argument(
          "Systematic values must have shape [variations, channels, bins]");

    std::size_t nominalIndex = variationKeys.size();
    for (std::size_t variation = 0; variation < variationKeys.size();
         ++variation) {
      if (variationKeys[variation].first.empty() &&
          variationKeys[variation].second.empty()) {
        nominalIndex = variation;
        break;
      }
    }
    if (nominalIndex == variationKeys.size())
      throw std::invalid_argument("Systematic values have no nominal slice");

    const auto *source = static_cast<const double *>(info.ptr);
    const std::size_t planeSize = channels_ * bins_;
    py::gil_scoped_release release;
    std::lock_guard<std::mutex> lock(mutex_);
    for (std::size_t variation = 0; variation < variationKeys.size();
         ++variation) {
      const auto &key = variationKeys[variation];
      if (key.first.empty())
        continue;
      if (key.second != "up" && key.second != "down")
        throw std::invalid_argument(
            "Systematic direction must be 'up' or 'down'");
      auto entry = shifts_.try_emplace(key, planeSize, 0.0).first;
      auto &total = entry->second;
      const auto *shifted = source + variation * planeSize;
      const auto *nominal = source + nominalIndex * planeSize;
      for (std::size_t bin = 0; bin < planeSize; ++bin)
        total[bin] += (shifted[bin] - nominal[bin]) * scale;
    }
  }

  py::dict shifts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    py::dict result;
    for (const auto &[key, values] : shifts_) {
      py::array_t<double> output(
          {static_cast<py::ssize_t>(channels_),
           static_cast<py::ssize_t>(bins_)});
      std::copy(values.begin(), values.end(), output.mutable_data());
      result[py::make_tuple(key.first, key.second)] = std::move(output);
    }
    return result;
  }

private:
  std::size_t channels_;
  std::size_t bins_;
  mutable std::mutex mutex_;
  std::map<std::pair<std::string, std::string>, std::vector<double>> shifts_;
};
} // namespace

void bindHistogramIO(py::module_ &module) {
  module.def("extract_sparse_slices", &extractSparseSlices,
             py::arg("file_name"), py::arg("specifications"),
             "Write legacy TH1F slices after schema validation; intended for extract_nd_histograms.");
  module.def("read_sparse_bins", &readSparseBins,
             py::arg("file_name"), py::arg("histogram_path"),
             R"doc(Read allocated THnSparse bins into NumPy arrays.

Returns a C-contiguous int32 coordinate matrix and float64 content and sumw2
vectors. Coordinates retain ROOT's one-based bin numbering.)doc");
  module.def("project_sparse_bins", &projectSparseBins,
             py::arg("file_name"), py::arg("histogram_path"),
             py::arg("output_axes"), py::arg("axis_maps"),
             py::arg("output_shape"), py::arg("include_sumw2") = true,
             R"doc(Project and sum allocated THnSparse bins into dense NumPy arrays.

Each input axis maps ROOT's one-based bin numbers to an output coordinate.
Use -1 in an axis map to filter a bin. Multiple axes may target the same
singleton output axis to combine filters.)doc");
  module.def("project_sparse_bins_batch", &projectSparseBinsBatch,
             py::arg("file_name"), py::arg("requests"),
             R"doc(Project several THnSparse objects from one ROOT file handle.

Each request is (histogram_path, output_axes, axis_maps, output_shape,
include_sumw2). Results are returned in request order as (contents, sumw2)
arrays.)doc");
  py::class_<SystematicAccumulator>(module, "SystematicAccumulator")
      .def(py::init<std::size_t, std::size_t>(), py::arg("channels"),
           py::arg("bins"))
      .def("add", &SystematicAccumulator::add, py::arg("values"),
           py::arg("variation_keys"), py::arg("scale"),
           R"doc(Add [variation, channel, bin] values to correlated shifts.

Use an empty variation key for the nominal slice. Other keys are
(nuisance, 'up' or 'down'); missing variations contribute no shift.)doc")
      .def("shifts", &SystematicAccumulator::shifts);
}
