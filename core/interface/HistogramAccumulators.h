#ifndef RDF_HISTOGRAM_ACCUMULATORS_H
#define RDF_HISTOGRAM_ACCUMULATORS_H

#include <boost/histogram/weight.hpp>
#include <boost/histogram/axis/option.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <TAxis.h>
#include <cmath>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace rdfhistogram {

// One counter per slot-local container. Only allocation/deallocation touches
// it; regular fills of existing cells incur no diagnostic work or atomics.
struct AllocationBytes {
  std::size_t current = 0;
};

template <class T> class CountingAllocator {
public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;
  std::shared_ptr<AllocationBytes> bytes = std::make_shared<AllocationBytes>();

  CountingAllocator() = default;
  template <class U>
  CountingAllocator(const CountingAllocator<U> &other) noexcept : bytes(other.bytes) {}

  T *allocate(std::size_t count) {
    auto *pointer = std::allocator<T>{}.allocate(count);
    bytes->current += count * sizeof(T);
    return pointer;
  }
  void deallocate(T *pointer, std::size_t count) noexcept {
    bytes->current -= count * sizeof(T);
    std::allocator<T>{}.deallocate(pointer, count);
  }
  template <class U> bool operator==(const CountingAllocator<U> &other) const noexcept {
    return bytes == other.bytes;
  }
  template <class U> bool operator!=(const CountingAllocator<U> &other) const noexcept {
    return !(*this == other);
  }
};

// Survives RDF action finalization so saveHists can report sparse storage cost
// after the slot-local containers have been destroyed. This measures requested
// container allocations, not malloc overhead, ROOT output storage or process RSS.
struct StorageMemoryUsage {
  std::size_t denseBytes = 0;
  std::size_t sparseBytes = 0;
  unsigned slots = 0;
  bool sparse = false;
  std::string storage;
};

// Boost and ROOT regular axes have different edge-rounding rules. Use the
// installed ROOT's own lookup for continuous/variable bins. The bounded
// half-integer fast path is exact for the common unit-width category axes.
class RootAxis {
public:
  RootAxis() : RootAxis(1, 0, 1) {}
  RootAxis(int bins, double lower, double upper,
           const std::vector<double> &edges = {}) : axis_(bins, lower, upper) {
    if (!edges.empty()) axis_.Set(bins, edges.data());
    unit_ = edges.empty() && upper-lower == bins &&
        std::abs(lower) <= 1000000 && std::abs(upper) <= 1000000 &&
        std::floor(2*lower) == 2*lower;
  }
  int size() const { return axis_.GetNbins(); }
  int index(double value) const {
    if (value < axis_.GetXmin()) return -1;
    if (!(value < axis_.GetXmax())) return size(); // includes NaN
    if (unit_) {
      const double twice = 2*value;
      if (twice == static_cast<int>(twice))
        return static_cast<int>(value-axis_.GetXmin());
    }
    return axis_.FindFixBin(value)-1;
  }
  double value(double index) const {
    if (index < 0) return -std::numeric_limits<double>::infinity();
    if (index > size()) return std::numeric_limits<double>::infinity();
    if (std::isnan(index)) return index;
    const int bin = static_cast<int>(std::floor(index));
    return axis_.GetBinLowEdge(bin+1) + (index-bin)*axis_.GetBinWidth(bin+1);
  }
  static constexpr unsigned options() {
    return decltype(boost::histogram::axis::option::underflow |
                    boost::histogram::axis::option::overflow)::value;
  }
  bool operator==(const RootAxis &other) const {
    if (size() != other.size()) return false;
    for (int bin = 1; bin <= size()+1; ++bin)
      if (axis_.GetBinLowEdge(bin) != other.axis_.GetBinLowEdge(bin)) return false;
    return true;
  }
  bool operator!=(const RootAxis &other) const { return !(*this == other); }
private:
  TAxis axis_;
  bool unit_ = false;
};

// weighted_sum<float> also rounds the variance to float. Keep the same
// float-content/double-variance arithmetic as ROOT's THnF instead.
class FloatWeightedSum {
public:
  FloatWeightedSum() = default;
  FloatWeightedSum(float value, double variance) : value_(value), variance_(variance) {}
  template <class T>
  FloatWeightedSum(const boost::histogram::weight_type<T> &weight) {
    *this += weight;
  }
  template <class T>
  FloatWeightedSum &operator+=(const boost::histogram::weight_type<T> &weight) {
    return *this += static_cast<double>(weight.value);
  }
  // Boost.Histogram 1.74 detects weight support via `accumulator += 0`.
  FloatWeightedSum &operator+=(double w) {
    value_ = static_cast<float>(static_cast<double>(value_) + w);
    variance_ += w * w;
    return *this;
  }
  FloatWeightedSum &operator+=(const FloatWeightedSum &other) {
    value_ = static_cast<float>(static_cast<double>(value_) + other.value_);
    variance_ += other.variance_;
    return *this;
  }
  FloatWeightedSum &operator++() { return *this += boost::histogram::weight(1.); }
  float value() const { return value_; }
  double variance() const { return variance_; }
  bool operator==(const FloatWeightedSum &other) const {
    return value_ == other.value_ && variance_ == other.variance_;
  }
  bool operator!=(const FloatWeightedSum &other) const { return !(*this == other); }
private:
  float value_ = 0;
  double variance_ = 0;
};

// Separate arrays avoid the padding of {float,double} structs (usually 16 B).
// This models Boost's Storage concept and uses exactly 12 bytes per cell in
// the two arrays. Slot-local ownership supplies the required thread isolation.
class FloatWeightedStorage {
public:
  using value_type = FloatWeightedSum;
  static constexpr bool has_threading_support = false;
  class Reference {
  public:
    Reference(float &value, double &variance) : value_(value), variance_(variance) {}
    operator value_type() const { return {value_, variance_}; }
    Reference &operator=(const value_type &value) {
      value_ = value.value(); variance_ = value.variance(); return *this;
    }
    Reference &operator=(const Reference &other) { return *this = value_type(other); }
    template <class T>
    Reference &operator+=(const boost::histogram::weight_type<T> &weight) {
      const double w = weight.value;
      value_ = static_cast<float>(static_cast<double>(value_) + w);
      variance_ += w * w;
      return *this;
    }
    Reference &operator+=(const value_type &other) {
      value_ = static_cast<float>(static_cast<double>(value_) + other.value());
      variance_ += other.variance(); return *this;
    }
    Reference &operator++() { return *this += boost::histogram::weight(1.); }
    float value() const { return value_; }
    double variance() const { return variance_; }
  private:
    float &value_;
    double &variance_;
  };
  using reference = Reference;
  using const_reference = value_type;
  template <bool Const>
  class Iterator : public boost::iterator_facade<Iterator<Const>, value_type,
      boost::random_access_traversal_tag, std::conditional_t<Const, value_type, Reference>> {
    friend class boost::iterator_core_access;
    using Storage = std::conditional_t<Const, const FloatWeightedStorage, FloatWeightedStorage>;
    Storage *storage_ = nullptr;
    std::ptrdiff_t index_ = 0;
    auto dereference() const { return (*storage_)[index_]; }
    bool equal(const Iterator &other) const { return storage_ == other.storage_ && index_ == other.index_; }
    void increment() { ++index_; }
    void decrement() { --index_; }
    void advance(std::ptrdiff_t amount) { index_ += amount; }
    std::ptrdiff_t distance_to(const Iterator &other) const { return other.index_ - index_; }
  public:
    Iterator() = default;
    Iterator(Storage *storage, std::ptrdiff_t index) : storage_(storage), index_(index) {}
  };
  using iterator = Iterator<false>;
  using const_iterator = Iterator<true>;
  void reset(std::size_t size) { values_.assign(size, 0); variances_.assign(size, 0); }
  std::size_t size() const { return values_.size(); }
  reference operator[](std::size_t index) { return {values_[index], variances_[index]}; }
  value_type operator[](std::size_t index) const { return {values_[index], variances_[index]}; }
  iterator begin() { return {this, 0}; }
  iterator end() { return {this, static_cast<std::ptrdiff_t>(size())}; }
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, static_cast<std::ptrdiff_t>(size())}; }
  bool operator==(const FloatWeightedStorage &other) const {
    return values_ == other.values_ && variances_ == other.variances_;
  }
  bool operator!=(const FloatWeightedStorage &other) const { return !(*this == other); }
private:
  std::vector<float> values_;
  std::vector<double> variances_;
};

} // namespace rdfhistogram
#endif
