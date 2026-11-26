#ifndef RDF_NATIVE_RESULTS_EXAMPLE_ANALYSIS_H
#define RDF_NATIVE_RESULTS_EXAMPLE_ANALYSIS_H

#include <RunResult.h>
#include <memory>
#include <string>

namespace native_results_example {

// The executable sees this interface; graph implementation is compiled once
// into the static analysis library. No ROOT action handles cross this boundary.
class Analysis {
public:
  Analysis();
  ~Analysis();
  Analysis(const Analysis&) = delete;
  Analysis& operator=(const Analysis&) = delete;
  std::string describe() const;
  rdfauthoring::RunResult run();
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace native_results_example
#endif
