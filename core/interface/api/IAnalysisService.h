#ifndef IANALYSISSERVICE_H_INCLUDED
#define IANALYSISSERVICE_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <string>
#include <unordered_map>
#include "ManagerContext.h"

/**
 * @brief Optional analysis service interface.
 */
class IAnalysisService {
public:
  virtual ~IAnalysisService() = default;

  /**
   * @brief Initialize the service with shared context.
   */
  virtual void initialize(ManagerContext& ctx) = 0;

  /**
   * @brief Finalize the service using the current dataframe.
   */
  virtual void finalize(ROOT::RDF::RNode& df) = 0;

  /**
   * @brief Hook called before the first filter is applied.
   *
   * Default implementation is a no-op.
   */
  virtual void onPreFilter(ROOT::RDF::RNode&) {}

  /**
   * @brief Contribute structured provenance metadata for this service.
   *
   * The framework calls this after all plugins have been finalized and have
   * reported metadata.  The returned key-value pairs are stored directly in
   * the ProvenanceService (without any prefix, allowing services to choose
   * their own namespacing, e.g. "service.counter.sample").
   *
   * Services that do not override this method contribute no custom provenance
   * entries.
   *
   * @return Map of provenance key-value pairs.
   */
  virtual std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const { return {}; }
};

#endif // IANALYSISSERVICE_H_INCLUDED
