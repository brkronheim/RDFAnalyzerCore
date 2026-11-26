#ifndef RDF_DETAIL_DEFINE_VARIATIONS_H
#define RDF_DETAIL_DEFINE_VARIATIONS_H

#include <api/ISystematicManager.h>
#include <ROOT/RDataFrame.hxx>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace rdf::detail {

struct VariationRequest {
  std::vector<std::string> inputs;
  std::string label;
};

// Internal adapter shared by typed C++ callables and Python JIT expressions.
// The callback defines one column on the supplied node. Keep public Analyzer
// signatures independent of the expression language used by a frontend.
struct NoVariationPreparation {};

struct CheckedVariationDefinition {
  const std::string &role;
};

inline std::string describeVariationInputs(const std::vector<std::string> &columns) {
  std::ostringstream text;
  for (std::size_t i = 0; i < columns.size(); ++i) {
    if (i) text << ", ";
    text << columns[i];
  }
  return text.str();
}

inline std::string checkedDefinitionContext(const std::string &name, const std::string &role) {
  return "DefineChecked '" + name + "'" + (role.empty() ? "" : " (role '" + role + "')");
}

template <typename DefineColumn, typename Prepare = NoVariationPreparation>
ROOT::RDF::RNode defineWithVariations(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns, ISystematicManager &systematics,
    DefineColumn defineColumn, Prepare prepare = {}) {
  constexpr bool checked = std::is_same_v<Prepare, CheckedVariationDefinition>;
  if (df.HasColumn(name)) {
    if constexpr (checked)
      throw std::invalid_argument(checkedDefinitionContext(name, prepare.role) +
          ": nominal column '" + name + "' already exists; requested inputs [" +
          describeVariationInputs(columns) + "]");
    return df;
  }
  if constexpr (std::is_same_v<Prepare, NoVariationPreparation> || checked) {
    if (columns.empty()) return defineColumn(df, name, columns);
  }

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
  std::map<std::string, VariationRequest> requests;
  std::vector<std::string> affectedFamilies;
  std::vector<std::pair<std::string, std::string>> explicitOutputs;
  const auto check = [&](const std::string &output,
                         const std::vector<std::string> &inputs,
                         const std::string &label) {
    if (df.HasColumn(output)) {
      if constexpr (checked)
        throw std::invalid_argument(checkedDefinitionContext(name, prepare.role) +
            ": physical column '" + output + "' already exists; variation '" +
            label + "' requested inputs [" + describeVariationInputs(inputs) + "]");
      return;
    }
    const auto result = requests.emplace(output, VariationRequest{inputs, label});
    if (result.second) return;
    const auto &previous = result.first->second;
    if constexpr (!checked) {
      if (previous.inputs == inputs) return;
    }
    const auto context = [&]() {
      if constexpr (checked) return checkedDefinitionContext(name, prepare.role);
      else return "Define '" + name + "'";
    };
    throw std::invalid_argument(
        context() + ": variation output '" + output +
        "' has conflicting requests: '" + previous.label + "' inputs [" +
        describeVariationInputs(previous.inputs) + "] and '" + label + "' inputs [" +
        describeVariationInputs(inputs) + "]; use distinct physical variation columns");
  };
  for (const auto &family : paired) {
    const auto up = resolve(family + "Up");
    const auto down = resolve(family + "Down");
    if (up == columns && down == columns) continue;
    check(name + "_" + family + "Up", up, family + "Up");
    check(name + "_" + family + "Down", down, family + "Down");
    affectedFamilies.push_back(family);
  }
  for (const auto &label : explicitLabels) {
    const auto inputs = resolve(label);
    if (inputs == columns) continue;
    const auto mapped = systematics.getVariationColumnName(name, label);
    const auto output = mapped == name ? name + "_" + label : mapped;
    check(output, inputs, label);
    explicitOutputs.emplace_back(label, output);
  }
  if constexpr (!std::is_same_v<Prepare, NoVariationPreparation> && !checked)
    df = prepare(std::move(df), requests);

  // Materialize the validated input tuples in the original registration order.
  // Distinct names may denote stateful callables even with identical inputs.
  const auto book = [&](const std::string &output) {
    const auto request = requests.find(output);
    if (request != requests.end() && !df.HasColumn(output))
      df = defineColumn(df, output, request->second.inputs);
  };
  for (const auto &family : affectedFamilies) {
    book(name + "_" + family + "Up");
    book(name + "_" + family + "Down");
    systematics.registerSystematic(family, {name});
  }
  for (const auto &[label, output] : explicitOutputs) {
    book(output);
    systematics.registerVariationColumn(name, label, output);
  }
  return defineColumn(df, name, columns);
}

} // namespace rdf::detail
#endif
