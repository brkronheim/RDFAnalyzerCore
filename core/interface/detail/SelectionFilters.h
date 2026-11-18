#ifndef RDF_DETAIL_SELECTION_FILTERS_H
#define RDF_DETAIL_SELECTION_FILTERS_H
#include <ROOT/RDataFrame.hxx>
#include <string>
#include <vector>
namespace rdf::detail {
// Internal row-union filter. Common small masks use compiled callables;
// arbitrary systematic inventories retain the expression fallback.
ROOT::RDF::RNode filterAnyMask(ROOT::RDF::RNode df,
                             const std::vector<std::string> &masks,
                             const std::string &label);
}
#endif
