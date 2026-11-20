#pragma once
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <TH2D.h>
#include <algorithm>
#include <cmath>
#include <map>
#include <string>
#include <stdexcept>
#include <vector>

namespace rdf {
struct EfficiencyMeasurement {
  std::string prefix, indices, flavors, scores, pt, eta, weight;
  std::vector<int> flavorCodes;
  std::map<std::string, float> workingPoints;
  std::vector<double> ptEdges, etaEdges;
  bool foldPtOverflow = true;
};

// Call at the desired measurement stage. The caller supplies a pretag selected
// collection and an explicit weight; the utility never chooses physics cuts.
inline std::vector<ROOT::RDF::RResultPtr<TH2D>> bookEfficiencyMeasurement(
    ROOT::RDF::RNode df, const EfficiencyMeasurement &spec) {
  const auto validate = [](const std::vector<double> &edges) {
    return edges.size() >= 2 && std::all_of(edges.begin(), edges.end(),
        [](double value) { return std::isfinite(value); }) &&
        std::adjacent_find(edges.begin(), edges.end(), std::greater_equal<double>()) == edges.end();
  };
  if (!validate(spec.ptEdges) || !validate(spec.etaEdges))
    throw std::invalid_argument("efficiency axes must be finite and strictly increasing");
  using Ints = ROOT::VecOps::RVec<int>;
  using Floats = ROOT::VecOps::RVec<float>;
  std::vector<ROOT::RDF::RResultPtr<TH2D>> result;
  for (const auto flavor : spec.flavorCodes)
    for (const auto &[wp, threshold] : spec.workingPoints)
      for (const auto pass : {false, true}) {
        const auto name = spec.prefix + "_" + std::to_string(flavor) + "_" + wp +
                          (pass ? "_pass" : "_fail");
        auto node = df.Define(name + "_indices", [flavor, threshold, pass, spec](
            const Ints &indices, const Ints &flavors, const Floats &scores) {
            Ints selected;
            for (const auto index : indices) {
              if (std::find(spec.flavorCodes.begin(), spec.flavorCodes.end(), flavors.at(index)) == spec.flavorCodes.end())
                throw std::runtime_error("unsupported efficiency flavor");
              if (flavors.at(index) == flavor && (scores.at(index) >= threshold) == pass)
                selected.push_back(index);
            }
            return selected;
          }, {spec.indices, spec.flavors, spec.scores})
          .Define(name + "_pt", [spec](const Ints &indices, const Floats &pt) {
            Floats values;
            for (const auto index : indices) values.push_back(spec.foldPtOverflow ?
                std::min(pt.at(index), std::nextafter(float(spec.ptEdges.back()), 0.f)) : pt.at(index));
            return values;
          }, {name + "_indices", spec.pt})
          .Define(name + "_eta", [](const Ints &indices, const Floats &eta) {
            Floats values;
            for (const auto index : indices) values.push_back(std::abs(eta.at(index)));
            return values;
          }, {name + "_indices", spec.eta});
        result.push_back(node.Histo2D({name.c_str(), ";jet pT;|eta|",
            int(spec.ptEdges.size()-1), spec.ptEdges.data(),
            int(spec.etaEdges.size()-1), spec.etaEdges.data()},
            name + "_pt", name + "_eta", spec.weight));
      }
  return result;
}
} // namespace rdf
