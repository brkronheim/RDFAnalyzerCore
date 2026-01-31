#ifndef IOUTPUTSINK_H_INCLUDED
#define IOUTPUTSINK_H_INCLUDED

#include <ROOT/RDataFrame.hxx>
#include <string>
#include <vector>

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
};

#endif // IOUTPUTSINK_H_INCLUDED
