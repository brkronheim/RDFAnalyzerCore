#ifndef ROOTOUTPUTSINK_H_INCLUDED
#define ROOTOUTPUTSINK_H_INCLUDED

#include "api/IOutputSink.h"

/** ROOT snapshot sink. Paths and columns are resolved per request. */
class RootOutputSink : public IOutputSink {
public:
  void writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) override;
  void writeDataFrame(ROOT::RDF::RNode& df,
                      const IConfigurationProvider& configProvider,
                      const ISystematicManager* systematicManager,
                      OutputChannel channel) override;
  std::string resolveOutputFile(const IConfigurationProvider& configProvider,
                                OutputChannel channel) override;

private:
  // Reserved for binary compatibility.
  std::string cachedSkimFile_;
  std::string cachedMetaFile_;

  // Reserved for binary compatibility with previously compiled output sinks.
  std::vector<std::string> cachedColumns_;
  bool columnsCached_ = false;
};

#endif // ROOTOUTPUTSINK_H_INCLUDED
