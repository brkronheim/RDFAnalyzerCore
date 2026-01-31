#ifndef ROOTOUTPUTSINK_H_INCLUDED
#define ROOTOUTPUTSINK_H_INCLUDED

#include "api/IOutputSink.h"

/**
 * @brief ROOT-based output sink for writing skims.
 */
class RootOutputSink : public IOutputSink {
public:
  void writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) override;
};

#endif // ROOTOUTPUTSINK_H_INCLUDED
