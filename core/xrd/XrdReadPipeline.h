#ifndef RDF_XRD_READ_PIPELINE_H
#define RDF_XRD_READ_PIPELINE_H
#include <algorithm>
#include <deque>
#include <exception>
#include <future>

namespace rdf::xrd::detail {
// Drain every submitted task before returning or throwing: tasks may still
// write the caller's buffers. Depth is independent of payload reservations.
template <class Windows, class Read>
bool runReadWindows(const Windows &windows, unsigned depth, Read read) {
  if (depth <= 1 || windows.size() <= 1) {
    for (std::size_t i = 0; i < windows.size(); ++i)
      if (!read(windows[i], i)) return false;
    return true;
  }
  std::deque<std::future<bool>> pending;
  std::size_t next = 0;
  bool success = true;
  std::exception_ptr error;
  while (next < windows.size() || !pending.empty()) {
    while (success && !error && next < windows.size() && pending.size() < depth) {
      const auto i = next++;
      pending.push_back(std::async(std::launch::async, [&, i] { return read(windows[i], i); }));
    }
    if (pending.empty()) break;
    try { success = pending.front().get() && success; }
    catch (...) { if (!error) error = std::current_exception(); }
    pending.pop_front();
    if (!success || error) next = windows.size();
  }
  if (error) std::rethrow_exception(error);
  return success;
}
}
#endif
