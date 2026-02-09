#ifndef IOUTPUTSINK_H_INCLUDED
#define IOUTPUTSINK_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <string>
#include <vector>

class IConfigurationProvider;
class IDataFrameProvider;
class ISystematicManager;

/**
 * @brief Output channel selection for sink behavior.
 */
enum class OutputChannel {
  Skim,
  Meta
};

/**
 * @brief Output specification for writing a dataframe.
 */
struct OutputSpec {
  std::string outputFile;
  std::string treeName;
  std::vector<std::string> columns; // empty = all
};

/**
 * @brief Output sink interface for writing skims and metadata/histograms.
 */
class IOutputSink {
public:
  virtual ~IOutputSink() = default;

  virtual void writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) = 0;

  virtual void writeDataFrame(ROOT::RDF::RNode& df,
                              const IConfigurationProvider& configProvider,
                              const IDataFrameProvider* dataFrameProvider,
                              const ISystematicManager* systematicManager,
                              OutputChannel channel) = 0;

  virtual std::string resolveOutputFile(const IConfigurationProvider& configProvider,
                                        OutputChannel channel) = 0;
};

#endif // IOUTPUTSINK_H_INCLUDED
