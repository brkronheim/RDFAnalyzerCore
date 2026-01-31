#ifndef IANALYSISSERVICE_H_INCLUDED
#define IANALYSISSERVICE_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
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
};

#endif // IANALYSISSERVICE_H_INCLUDED
