#ifndef RDFANALYZER_XRD_READ_MEMORY_H
#define RDFANALYZER_XRD_READ_MEMORY_H

#include <cstddef>
#include <memory>
#include <mutex>

namespace rdf::xrd::detail {

// Shared by all managers, including callbacks that outlive their reader.
class ReadMemoryBudget {
public:
  static constexpr std::size_t kLimit = 64ULL * 1024 * 1024;
  static constexpr std::size_t kWindow = 8ULL * 1024 * 1024;

  class Reservation {
  public:
    ~Reservation() {
      std::lock_guard<std::mutex> lock(budget_.mutex_);
      budget_.used_ -= bytes_;
    }
    Reservation(const Reservation &) = delete;
    Reservation &operator=(const Reservation &) = delete;

  private:
    friend class ReadMemoryBudget;
    Reservation(ReadMemoryBudget &budget, std::size_t bytes)
        : budget_(budget), bytes_(bytes) {}
    ReadMemoryBudget &budget_;
    std::size_t bytes_;
  };

  std::unique_ptr<Reservation> tryReserve(std::size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes > kLimit - used_) return {};
    auto reservation = std::unique_ptr<Reservation>(new Reservation(*this, bytes));
    used_ += bytes;
    return reservation;
  }

private:
  std::mutex mutex_;
  std::size_t used_ = 0;
};

} // namespace rdf::xrd::detail
#endif
