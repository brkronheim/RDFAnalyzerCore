#include <RootOutputSink.h>
#include <iostream>

void RootOutputSink::writeDataFrame(ROOT::RDF::RNode& df, const OutputSpec& spec) {
  if (spec.outputFile.empty()) {
    throw std::runtime_error("RootOutputSink: outputFile is empty");
  }
  if (spec.treeName.empty()) {
    throw std::runtime_error("RootOutputSink: treeName is empty");
  }

  std::cout << "Executing Snapshot" << std::endl;
  std::cout << "Tree: " << spec.treeName << std::endl;
  std::cout << "SaveFile: " << spec.outputFile << std::endl;

  if (spec.columns.empty()) {
    df.Snapshot(spec.treeName, spec.outputFile);
  } else {
    df.Snapshot(spec.treeName, spec.outputFile, spec.columns);
  }

  std::cout << "Done Saving" << std::endl;
}
