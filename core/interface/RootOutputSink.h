#ifndef ROOTOUTPUTSINK_H_INCLUDED
#define ROOTOUTPUTSINK_H_INCLUDED

#include "api/IOutputSink.h"

/**
 * @brief ROOT-based output sink for writing skims.
 */
class RootOutputSink : public IOutputSink {
public:
  void writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) override;
  void writeDataFrame(ROOT::RDF::RNode& df,
                      const IConfigurationProvider& configProvider,
                      const IDataFrameProvider* dataFrameProvider,
                      const ISystematicManager* systematicManager,
                      OutputChannel channel) override;
  std::string resolveOutputFile(const IConfigurationProvider& configProvider,
                                OutputChannel channel) override;
};

#endif // ROOTOUTPUTSINK_H_INCLUDED
