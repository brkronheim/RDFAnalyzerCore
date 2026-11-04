// ROOT 6.40.04 compatibility patch. Constructor/AddDefine adapted from ROOT:
// RDefineBase: Copyright (C) 1995-2018, Rene Brun and Fons Rademakers.
// Authors: Enrico Guiraud, Danilo Piparo (CERN).
// RColumnRegister: Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.
// All rights reserved.
// ROOT licensing: https://root.cern/about/license/ (LGPL 2.1).
// CMake checks the exact private headers. Never compile against another ABI.
#include <ROOT/RDF/RColumnRegister.hxx>
#include <ROOT/RDF/RDefineBase.hxx>
#include <ROOT/RDF/RDefineReader.hxx>
#include <ROOT/RDF/RLoopManager.hxx>
#include <ROOT/RDF/Utils.hxx>
#include <TROOT.h>
#include <RVersion.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#if defined(_GLIBCXX_DEBUG) || !defined(__GLIBCXX__) || !_GLIBCXX_USE_CXX11_ABI
#error "The audited ROOT registry patch requires the ordinary libstdc++ ABI"
#endif

namespace {
static_assert(ROOT_VERSION_CODE == ROOT_VERSION(6,40,4), "ROOT registry ABI mismatch");
static_assert(sizeof(ROOT::Internal::RDF::RColumnRegister) == 56, "ROOT registry layout mismatch");
std::atomic<std::uint64_t> constructions{0}, bindingsBefore{0}, bindingsAfter{0};
bool enabled() {
  static const bool result = [] {
    // TROOT uses decimal version numbers; ROOT_VERSION_CODE is bit-packed.
    if (gROOT->GetVersionInt() != 64004)
      throw std::runtime_error("ROOT registry optimization requires the audited ROOT 6.40.04 runtime");
    const char *value = std::getenv("RDF_ROOT_REGISTRY_OPTIMIZATION");
    const std::string mode = value ? value : "1";
    if (mode == "0" || mode == "off" || mode == "OFF" || mode == "false") return false;
    if (mode != "1" && mode != "on" && mode != "ON" && mode != "true")
      throw std::runtime_error("RDF_ROOT_REGISTRY_OPTIMIZATION must be 0/off or 1/on");
    return true;
  }();
  return result;
}
std::string_view checkedName(std::string_view name) {
  // Check before the first member initializer accesses the pinned ROOT ABI.
  enabled();
  return name;
}
}

// Small C ABI used by provenance/tests. These are cumulative construction
// counts, not a measurement of live memory. No per-event atomic operations.
extern "C" void rdf_root_registry_optimization_status_v1(std::uint64_t *values) {
  values[0] = enabled();
  values[1] = constructions.load(std::memory_order_relaxed);
  values[2] = bindingsBefore.load(std::memory_order_relaxed);
  values[3] = bindingsAfter.load(std::memory_order_relaxed);
}

namespace ROOT { namespace Internal { namespace RDF {
void RColumnRegister::KeepDirectInputs(const std::vector<std::string> &columns) {
  const auto before = fDefines->size();
  if (!fDefines->empty()) {
    auto inputs = std::make_shared<DefinesMap_t>();
    inputs->reserve(std::min(columns.size(), before));
    std::vector<std::string_view> resolved;
    resolved.reserve(columns.size());
    for (const auto &column : columns) resolved.push_back(ResolveAlias(column));
    for (const auto &binding : *fDefines)
      if (std::find(resolved.begin(), resolved.end(), binding.first) != resolved.end())
        inputs->push_back(binding);
    fDefines = std::move(inputs);
  }
  bindingsBefore.fetch_add(before, std::memory_order_relaxed);
  bindingsAfter.fetch_add(fDefines->size(), std::memory_order_relaxed);
}

void RColumnRegister::AddDefine(std::shared_ptr<RDFDetail::RDefineBase> define) {
  const bool compact = enabled();
  const auto colIt = fLoopManager->GetColumnNamesCache().Insert(define->GetName());
  auto insertion = fLoopManager->GetUniqueDefinesWithReaders().insert(
      {*colIt, std::make_unique<RDefinesWithReaders>(define, fLoopManager->GetNSlots(),
                                                   fLoopManager->GetColumnNamesCache())});
  std::shared_ptr<DefinesMap_t> next;
  if (compact) {
    next = std::make_shared<DefinesMap_t>();
    next->reserve(fDefines->size()+1);
    next->insert(next->end(), fDefines->begin(), fDefines->end());
  } else {
    next = std::make_shared<DefinesMap_t>(*fDefines);
  }
  const auto &entry = insertion.first;
  auto previous = std::find_if(next->begin(), next->end(),
      [&entry](const auto &binding) { return binding.first == entry->first; });
  if (previous != next->end()) previous->second = entry->second.get();
  else next->push_back({entry->first, entry->second.get()});
  fDefines = std::move(next);
}
} } }

using ROOT::Detail::RDF::RDefineBase;
namespace RDFInternal = ROOT::Internal::RDF;
RDefineBase::RDefineBase(std::string_view name, std::string_view type,
    const RDFInternal::RColumnRegister &colRegister, ROOT::Detail::RDF::RLoopManager &lm,
    const ROOT::RDF::ColumnNames_t &columnNames, const std::string &variationName)
    : fName(checkedName(name)), fType(type),
      fLastCheckedEntry(lm.GetNSlots()*RDFInternal::CacheLineStep<Long64_t>(), -1),
      fColRegister(colRegister), fLoopManager(&lm), fColumnNames(columnNames),
      fIsDefine(columnNames.size()), fVariationDeps(fColRegister.GetVariationDeps(fColumnNames)),
      fVariation(variationName) {
  for (auto i=0u; i<fColumnNames.size(); ++i) {
    fIsDefine[i] = fColRegister.IsDefineOrAlias(fColumnNames[i]);
    if (fVariation != "nominal" && fIsDefine[i])
      fColRegister.GetDefine(fColumnNames[i])->MakeVariations({fVariation});
  }
  constructions.fetch_add(1, std::memory_order_relaxed);
  if (enabled()) fColRegister.KeepDirectInputs(fColumnNames);
}
