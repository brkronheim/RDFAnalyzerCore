#include "inspection.h"
#include <ROOT/RDataFrame.hxx>
#include <exception>

InputInspection inspectInput(const std::string &path) {
  InputInspection result;
  result.report = rdf::validation::checkAssets({{path, "input", rdf::validation::AssetKind::File, {}}});
  if (!result.report.passed()) return result;
  try {
    ROOT::RDataFrame frame("Events", path);
    std::vector<rdf::validation::ColumnInfo> observed;
    for (const auto &name : frame.GetColumnNames())
      observed.push_back({name, frame.GetColumnType(name)});
    // Presence-only jet requirement avoids guessing ROOT typedef spellings.
    // A typed requirement must use the caller's explicit exact spelling.
    result.report = rdf::validation::checkSchema({{"Jet_pt", {}, "inputs.jets"},
                                                {"event", "ULong64_t", "inputs.event"}}, observed);
    result.eventLoops = frame.GetNRuns();
    if (result.eventLoops != 0)
      result.report.addIssue("schema.event_execution", "Unexpected event loop during preflight", "input");
  } catch (const std::exception &error) {
    result.report.level = "schema";
    result.report.supportedLevels = {"schema"};
    result.report.addIssue("schema.inspect", error.what(), "input");
  }
  return result;
}
