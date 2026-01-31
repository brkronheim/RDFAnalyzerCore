#ifndef COUNTERSERVICE_H_INCLUDED
#define COUNTERSERVICE_H_INCLUDED

#include "api/IAnalysisService.h"
#include <ROOT/RResultPtr.hxx>
#include <string>

/**
 * @brief Analysis service for logging per-sample event counts.
 */
class CounterService : public IAnalysisService {
public:
  void initialize(ManagerContext& ctx) override;
  void finalize(ROOT::RDF::RNode& df) override;

private:
  ManagerContext* ctx_m = nullptr;
  std::string sampleName_m;
};

#endif // COUNTERSERVICE_H_INCLUDED
