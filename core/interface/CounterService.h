#ifndef COUNTERSERVICE_H_INCLUDED
#define COUNTERSERVICE_H_INCLUDED

#include "api/IAnalysisService.h"
#include <ROOT/RResultPtr.hxx>
#include <optional>
#include <string>
#include <chrono>

/**
 * @brief Analysis service for logging per-sample event counts.
 */
class CounterService : public IAnalysisService {
public:
  void initialize(ManagerContext& ctx) override;
  void finalize(ROOT::RDF::RNode& df) override;
  void onPreFilter(ROOT::RDF::RNode& df) override;

private:
  ManagerContext* ctx_m = nullptr;
  std::string sampleName_m;
  std::string weightBranch_m;
  std::string intWeightBranch_m;
  std::optional<ROOT::RDF::RNode> preFilterDf_m;

  // start time recorded at initialize(); used to compute processing speed in finalize()
  std::chrono::steady_clock::time_point startTime_m{};
};

#endif // COUNTERSERVICE_H_INCLUDED
