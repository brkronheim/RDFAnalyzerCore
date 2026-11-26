#ifndef RDF_CHECKED_DEFINE_H
#define RDF_CHECKED_DEFINE_H

#include <analyzer.h>
#include <detail/DefineVariations.h>

namespace rdf {

/// Book a typed definition only if it owns each requested physical output.
/// Reports collisions before attaching graph nodes or registering variations.
/// The optional role identifies the analysis or plugin stage in diagnostics.
template <typename F>
Analyzer *DefineChecked(Analyzer &analyzer, const std::string &name, F function,
                        const std::vector<std::string> &columns = {},
                        const std::string &role = {}) {
  auto &provider = analyzer.getDataFrameProvider();
  provider.setDataFrame(detail::defineWithVariations(
      provider.getDataFrame(), name, columns, analyzer.getSystematicManager(),
      [&function](ROOT::RDF::RNode node, const std::string &output,
                  const std::vector<std::string> &inputs) {
        return node.Define(output, function, inputs);
      }, detail::CheckedVariationDefinition{role}));
  return &analyzer;
}

} // namespace rdf
#endif
