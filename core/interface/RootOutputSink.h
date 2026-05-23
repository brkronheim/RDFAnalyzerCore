#ifndef ROOTOUTPUTSINK_H_INCLUDED
#define ROOTOUTPUTSINK_H_INCLUDED

#include "api/IOutputSink.h"

/**
 * @brief ROOT-based output sink for writing skims.
 *
 * The config-based writeDataFrame() overload resolves output files and columns
 * using the configuration provider.  Resolved file paths and column lists are
 * cached per OutputChannel so that repeated calls (should not happen in normal
 * single-run usage) avoid redundant lookups and GetColumnNames() calls.
 */
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
  // Cached resolved file paths per channel.
  std::string cachedSkimFile_;
  std::string cachedMetaFile_;

  // Cache for column names resolved from saveConfig + systematics.
  // Invalidated each time writeDataFrame(config, ...) is called with a
  // different dataframe — but in practice it's called at most once.
  std::vector<std::string> cachedColumns_;
  bool columnsCached_ = false;
};

#endif // ROOTOUTPUTSINK_H_INCLUDED
