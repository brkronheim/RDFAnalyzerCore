#ifndef RDF_ANALYZER_CARTESIAN_BINNING_H_INCLUDED
#define RDF_ANALYZER_CARTESIAN_BINNING_H_INCLUDED

#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>
#include <algorithm>

namespace RDFAnalyzer {

/**
 * @brief Map a tuple of axis-bin coordinates to a stable integer bin ID.
 *
 * The last axis changes fastest. Axis values are deliberately represented by
 * bin indices so analyses can use numeric edges, categories, or custom
 * classifiers without coupling this utility to a particular event model.
 */
class CartesianBinning {
public:
  explicit CartesianBinning(std::vector<std::size_t> axisBinCounts);

  std::size_t axisCount() const noexcept { return axisBinCounts_m.size(); }
  std::size_t numberOfBins() const noexcept { return numberOfBins_m; }

  std::size_t flatten(const std::vector<std::size_t>& coordinates) const;
  std::optional<std::size_t> flattenOptional(
      const std::vector<std::optional<std::size_t>>& coordinates) const;
  std::vector<std::size_t> unflatten(std::size_t bin) const;

private:
  std::vector<std::size_t> axisBinCounts_m;
  std::vector<std::size_t> strides_m;
  std::size_t numberOfBins_m = 0;
};

/**
 * @brief Convert finite numeric values to half-open bins with an open final bin.
 *
 * The supplied edges are lower bin boundaries. Values below the first edge
 * and non-finite values are invalid. Values equal to an edge enter the bin
 * starting at that edge; values at or above the last edge enter the final bin.
 */
class NumericBinningAxis {
public:
  explicit NumericBinningAxis(std::vector<double> edges);

  template <typename Numeric,
            typename = std::enable_if_t<std::is_arithmetic_v<Numeric>>>
  explicit NumericBinningAxis(const std::vector<Numeric>& edges)
      : NumericBinningAxis(
            std::vector<double>(edges.begin(), edges.end())) {}

  std::size_t numberOfBins() const noexcept { return edges_m.size(); }
  const std::vector<double>& edges() const noexcept { return edges_m; }
  std::optional<std::size_t> findBin(double value) const noexcept;

private:
  std::vector<double> edges_m;
};

inline CartesianBinning::CartesianBinning(
    std::vector<std::size_t> axisBinCounts)
    : axisBinCounts_m(std::move(axisBinCounts)),
      strides_m(axisBinCounts_m.size(), 1), numberOfBins_m(1) {
  if (axisBinCounts_m.empty()) {
    throw std::invalid_argument("CartesianBinning requires at least one axis");
  }

  for (const auto binCount : axisBinCounts_m) {
    if (binCount == 0) {
      throw std::invalid_argument(
          "CartesianBinning axes must contain at least one bin");
    }
    if (numberOfBins_m > std::numeric_limits<std::size_t>::max() / binCount) {
      throw std::overflow_error("CartesianBinning bin count overflows size_t");
    }
    numberOfBins_m *= binCount;
  }

  for (std::size_t axis = axisBinCounts_m.size(); axis-- > 0;) {
    if (axis + 1 < axisBinCounts_m.size()) {
      strides_m[axis] = strides_m[axis + 1] * axisBinCounts_m[axis + 1];
    }
  }
}

inline std::size_t CartesianBinning::flatten(
    const std::vector<std::size_t>& coordinates) const {
  if (coordinates.size() != axisBinCounts_m.size()) {
    throw std::invalid_argument(
        "CartesianBinning coordinate count does not match axis count");
  }

  std::size_t bin = 0;
  for (std::size_t axis = 0; axis < coordinates.size(); ++axis) {
    if (coordinates[axis] >= axisBinCounts_m[axis]) {
      throw std::out_of_range("CartesianBinning coordinate is outside its axis");
    }
    bin += coordinates[axis] * strides_m[axis];
  }
  return bin;
}

inline std::optional<std::size_t> CartesianBinning::flattenOptional(
    const std::vector<std::optional<std::size_t>>& coordinates) const {
  if (coordinates.size() != axisBinCounts_m.size()) {
    return std::nullopt;
  }

  std::vector<std::size_t> resolved;
  resolved.reserve(coordinates.size());
  for (const auto& coordinate : coordinates) {
    if (!coordinate.has_value()) {
      return std::nullopt;
    }
    resolved.push_back(*coordinate);
  }
  try {
    return flatten(resolved);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

inline std::vector<std::size_t> CartesianBinning::unflatten(
    std::size_t bin) const {
  if (bin >= numberOfBins_m) {
    throw std::out_of_range("CartesianBinning bin is outside the valid range");
  }

  std::vector<std::size_t> coordinates(axisBinCounts_m.size(), 0);
  for (std::size_t axis = 0; axis < axisBinCounts_m.size(); ++axis) {
    coordinates[axis] = (bin / strides_m[axis]) % axisBinCounts_m[axis];
  }
  return coordinates;
}

inline NumericBinningAxis::NumericBinningAxis(std::vector<double> edges)
    : edges_m(std::move(edges)) {
  if (edges_m.size() < 2) {
    throw std::invalid_argument(
        "NumericBinningAxis requires at least two edges");
  }
  for (std::size_t index = 0; index < edges_m.size(); ++index) {
    if (!std::isfinite(edges_m[index]) ||
        (index > 0 && edges_m[index] <= edges_m[index - 1])) {
      throw std::invalid_argument(
          "NumericBinningAxis edges must be finite and strictly increasing");
    }
  }
}

inline std::optional<std::size_t> NumericBinningAxis::findBin(
    double value) const noexcept {
  if (!std::isfinite(value) || value < edges_m.front()) {
    return std::nullopt;
  }
  const auto upperEdge = std::upper_bound(edges_m.begin(), edges_m.end(), value);
  return std::min(static_cast<std::size_t>(upperEdge - edges_m.begin() - 1),
                  edges_m.size() - 1);
}

}  // namespace RDFAnalyzer

#endif  // RDF_ANALYZER_CARTESIAN_BINNING_H_INCLUDED