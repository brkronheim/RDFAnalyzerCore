#ifndef RDF_DETAIL_DEFINE_VARIATIONS_H
#define RDF_DETAIL_DEFINE_VARIATIONS_H

#include <api/ISystematicManager.h>
#include <ROOT/RDataFrame.hxx>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace rdf::detail {

// Internal adapter shared by typed C++ callables and Python JIT expressions.
// The callback defines one column on the supplied node. Keep public Analyzer
// signatures independent of the expression language used by a frontend.
template <typename DefineColumn>
ROOT::RDF::RNode defineWithVariations(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, ISystematicManager &systematics,
    DefineColumn defineColumn) {
  if (df.HasColumn(name)) return df;

  const auto resolve = [&](const std::string &label) {
    std::vector<std::string> inputs;
    inputs.reserve(columns.size());
    for (const auto &column : columns)
      inputs.push_back(systematics.getVariationColumnName(column, label));
    return inputs;
  };
  // Snapshot registrations before registering derived outputs.
  const auto paired = systematics.getSystematics();
  const auto explicitLabels = systematics.getExplicitVariationLabels();
  // Validate this expansion before attaching nodes or registering its outputs.
  // An existing external column retains the legacy first-definition behavior;
  // its producer cannot be inferred from an input tuple here. New requests in
  // this call, however, must not silently share a physical name when their
  // resolved inputs differ (including paired/explicit-label collisions).
  struct Request {
    std::vector<std::string> inputs;
    std::string label;
  };
  std::map<std::string, Request> requests;
  const auto check = [&](const std::string &output,
                         const std::vector<std::string> &inputs,
                         const std::string &label) {
    if (df.HasColumn(output)) return;
    const auto result = requests.emplace(output, Request{inputs, label});
    if (result.second || result.first->second.inputs == inputs) return;
    const auto describe = [](const std::vector<std::string> &names) {
      std::ostringstream text;
      for (std::size_t i = 0; i < names.size(); ++i) {
        if (i) text << ", ";
        text << names[i];
      }
      return text.str();
    };
    const auto &previous = result.first->second;
    throw std::invalid_argument(
        "Define '" + name + "': variation output '" + output +
        "' has conflicting requests: '" + previous.label + "' inputs [" +
        describe(previous.inputs) + "] and '" + label + "' inputs [" +
        describe(inputs) + "]; use distinct physical variation columns");
  };
  for (const auto &family : paired) {
    const auto up = resolve(family + "Up");
    const auto down = resolve(family + "Down");
    if (up == columns && down == columns) continue;
    check(name + "_" + family + "Up", up, family + "Up");
    check(name + "_" + family + "Down", down, family + "Down");
  }
  for (const auto &label : explicitLabels) {
    const auto inputs = resolve(label);
    if (inputs == columns) continue;
    const auto mapped = systematics.getVariationColumnName(name, label);
    check(mapped == name ? name + "_" + label : mapped, inputs, label);
  }
  for (const auto &family : paired) {
    const auto up = resolve(family + "Up");
    const auto down = resolve(family + "Down");
    if (up == columns && down == columns) continue;
    const auto upName = name + "_" + family + "Up";
    const auto downName = name + "_" + family + "Down";
    if (!df.HasColumn(upName)) df = defineColumn(df, upName, up);
    if (!df.HasColumn(downName)) df = defineColumn(df, downName, down);
    systematics.registerSystematic(family, {name});
  }
  for (const auto &label : explicitLabels) {
    const auto inputs = resolve(label);
    if (inputs == columns) continue;
    const auto mapped = systematics.getVariationColumnName(name, label);
    const auto output = mapped == name ? name + "_" + label : mapped;
    if (!df.HasColumn(output)) df = defineColumn(df, output, inputs);
    systematics.registerVariationColumn(name, label, output);
  }
  return defineColumn(df, name, columns);
}

} // namespace rdf::detail
#endif
