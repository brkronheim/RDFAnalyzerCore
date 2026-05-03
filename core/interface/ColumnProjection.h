#ifndef COLUMNPROJECTION_H_INCLUDED
#define COLUMNPROJECTION_H_INCLUDED

#include "analyzer.h"

#include <cstddef>
#include <string>
#include <type_traits>
#include <utility>

namespace rdfanalysis::column {

template <typename SourceT, typename ProjectionT>
struct ProjectionSpec {
  std::string name;
  ProjectionT projection;
};

template <typename SourceT, typename ProjectionT>
auto projectedColumn(std::string name, ProjectionT &&projection)
    -> ProjectionSpec<SourceT, std::decay_t<ProjectionT>> {
  return {std::move(name), std::forward<ProjectionT>(projection)};
}

template <typename SourceT, typename MemberT>
auto memberProjection(std::string name, MemberT SourceT::*member) {
  return projectedColumn<SourceT>(
      std::move(name),
      [member](const SourceT &source) -> MemberT { return source.*member; });
}

template <typename VectorT>
auto indexProjection(std::string name, std::size_t index) {
  using ValueT = typename VectorT::value_type;
  return projectedColumn<VectorT>(
      std::move(name),
      [index](const VectorT &values) -> ValueT { return values.at(index); });
}

template <typename CollectionT>
auto sizeProjection(std::string name) {
  return projectedColumn<CollectionT>(
      std::move(name), [](const CollectionT &values) {
        return static_cast<int>(values.size());
      });
}

namespace detail {

template <typename SourceT, typename ProjectionT>
void defineProjectedColumn(Analyzer &analyzer, const std::string &sourceColumn,
                           ProjectionSpec<SourceT, ProjectionT> spec) {
  analyzer.Define(
      std::move(spec.name),
      [projection = std::move(spec.projection)](const SourceT &source) {
        return projection(source);
      },
      {sourceColumn});
}

} // namespace detail

template <typename SourceT, typename... Specs>
Analyzer *defineProjectedColumns(Analyzer &analyzer,
                                 const std::string &sourceColumn,
                                 Specs &&...specs) {
  (detail::defineProjectedColumn<SourceT>(analyzer, sourceColumn,
                                          std::forward<Specs>(specs)),
   ...);
  return &analyzer;
}

} // namespace rdfanalysis::column

#endif // COLUMNPROJECTION_H_INCLUDED