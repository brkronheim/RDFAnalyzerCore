// Optional Linux LD_PRELOAD helper, built against the worker's ROOT release.
// Observe RDF's own loop boundaries without adding an action to the graph.
#include <ROOT/RDF/Utils.hxx>
#include <ROOT/RLogger.hxx>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <memory>
#include <sys/resource.h>

namespace {
double seconds(clockid_t clock) {
  timespec stamp{};
  clock_gettime(clock, &stamp);
  return stamp.tv_sec + stamp.tv_nsec * 1.e-9;
}

double cpuSeconds() {
  rusage self{}, children{};
  getrusage(RUSAGE_SELF, &self);
  getrusage(RUSAGE_CHILDREN, &children);
  double total = 0.;
  for (auto time : {self.ru_utime, self.ru_stime, children.ru_utime, children.ru_stime})
    total += time.tv_sec + time.tv_usec * 1.e-6;
  return total;
}

class LoopLog final : public ROOT::RLogHandler {
public:
  bool Emit(const ROOT::RLogEntry &entry) override {
    if (entry.fChannel != &ROOT::Detail::RDF::RDFLogChannel()) return true;
    const char *phase;
    if (entry.fMessage.rfind("Starting event loop number ", 0) == 0)
      phase = "start";
    else if (entry.fMessage.rfind("Finished event loop number ", 0) == 0)
      phase = "finish";
    else
      return true;
    std::fprintf(stderr, "RDF_BENCHMARK_LOOP %s %.9f %.9f\n", phase,
                 seconds(CLOCK_MONOTONIC), cpuSeconds());
    return true;
  }
};

struct InstallLoopLog {
  ROOT::RLogScopedVerbosity verbosity{ROOT::Detail::RDF::RDFLogChannel(), ROOT::ELogLevel::kInfo};
  InstallLoopLog() {
    // Keep the profiler out of worker subprocesses, e.g. git provenance tools.
    if (const auto previous = std::getenv("RDF_BENCHMARK_CHILD_LD_PRELOAD")) {
      if (*previous) setenv("LD_PRELOAD", previous, 1);
      else unsetenv("LD_PRELOAD");
      unsetenv("RDF_BENCHMARK_CHILD_LD_PRELOAD");
    }
    ROOT::RLogManager::Get().PushFront(std::make_unique<LoopLog>());
  }
};
InstallLoopLog install;
} // namespace
