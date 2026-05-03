#ifndef WORKINGPOINTRECIPE_H_INCLUDED
#define WORKINGPOINTRECIPE_H_INCLUDED

#include "analyzer.h"

#include <string>
#include <vector>

namespace rdfanalysis::weights {

template <typename ValueT>
void defineConstantColumns(Analyzer &analyzer,
                           const std::vector<std::string> &columns,
                           ValueT value) {
  for (const auto &column : columns) {
    analyzer.Define(column, [value]() { return value; }, {});
  }
}

template <typename ApplyFn>
std::vector<std::string> createWorkingPointColumns(
    const std::vector<std::string> &workingPoints,
    const std::string &outputPrefix,
    ApplyFn &&applyFn) {
  std::vector<std::string> outputColumns;
  outputColumns.reserve(workingPoints.size());
  for (const auto &workingPoint : workingPoints) {
    const std::string outputColumn = outputPrefix + "_" + workingPoint;
    applyFn(workingPoint, outputColumn);
    outputColumns.push_back(outputColumn);
  }
  return outputColumns;
}

template <typename TaggerManagerT, typename ColumnBuilderT>
std::vector<std::string> defineFixedWorkingPointVariation(
    TaggerManagerT &taggerManager,
    const std::vector<std::string> &workingPoints,
    const std::vector<std::string> &efficiencyColumns,
    const std::string &outputColumn,
    ColumnBuilderT &&columnBuilder) {
  std::vector<std::string> scaleFactorColumns;
  scaleFactorColumns.reserve(workingPoints.size());
  for (const auto &workingPoint : workingPoints) {
    scaleFactorColumns.push_back(columnBuilder(workingPoint));
  }
  taggerManager.defineFixedWorkingPointWeight(scaleFactorColumns,
                                              efficiencyColumns,
                                              outputColumn);
  return scaleFactorColumns;
}

} // namespace rdfanalysis::weights

#endif // WORKINGPOINTRECIPE_H_INCLUDED