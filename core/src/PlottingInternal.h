#pragma once
#include <PlottingUtility.h>
#include <TFile.h>
#include <chrono>
#include <functional>
#include <list>
#include <numeric>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

namespace rdfplot {
using Clock = std::chrono::steady_clock;
inline double seconds(Clock::time_point start) {
  return std::chrono::duration<double>(Clock::now() - start).count();
}
inline void require(bool condition, const std::string &message) {
  if (!condition)
    throw std::invalid_argument(message);
}
struct Series {
  const PlotProcessConfig *config = nullptr;
  std::vector<double> values, variance;
};
struct Prepared {
  std::vector<double> edges, yEdges, matrix;
  std::vector<std::string> labels, yLabels;
  std::vector<Series> series; // High-water size; only [0, count) is active.
  std::size_t count = 0;
  std::vector<double> data, dataLow2, dataHigh2, total, variance, signal;
  std::vector<double> systLow, systHigh;
  bool hasData = false, hasMC = false;
  PlotResult result;
};

struct Reader {
  struct File {
    std::string path;
    std::unique_ptr<TFile> file;
    YAML::Node schema;
  };
  explicit Reader(std::size_t fileLimit, std::size_t byteLimit)
      : maxFiles(fileLimit), maxBytes(byteLimit) {}
  ~Reader();
  void prepare(const PlotRequest &request, Prepared &output);
  File &open(const std::string &path);
  std::list<File> files;
  std::size_t maxFiles, maxBytes, opens = 0;
};
} // namespace rdfplot
