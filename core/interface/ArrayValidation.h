#pragma once

#include <cstddef>
#include <cmath>
#include <limits>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace rdf {
inline int checkedIntegerInput(double value, std::string_view context) {
  if (!std::isfinite(value) || value < std::numeric_limits<int>::min() ||
      value > std::numeric_limits<int>::max() || std::trunc(value) != value)
    throw std::runtime_error(std::string(context) + ": value " + std::to_string(value) +
                             " cannot be represented as an integer input");
  return static_cast<int>(value);
}

inline std::size_t checkedIndex(std::string_view context, int index, std::size_t size) {
  if (index < 0 || static_cast<std::size_t>(index) >= size)
    throw std::runtime_error(std::string(context) + ": index " + std::to_string(index) +
                             " is outside array size " + std::to_string(size));
  return static_cast<std::size_t>(index);
}

// Check once before indexing aligned per-object arrays. Scalars must be
// broadcast explicitly by the caller; an empty array is not missing data.
inline void requireAlignedSizes(
    std::string_view context, std::size_t expected,
    std::initializer_list<std::pair<const char *, std::size_t>> arrays) {
  for (const auto &array : arrays) {
    if (array.second != expected)
      throw std::runtime_error(std::string(context) + ": array '" + array.first + "' has size " +
          std::to_string(array.second) + ", expected " + std::to_string(expected));
  }
}
} // namespace rdf
