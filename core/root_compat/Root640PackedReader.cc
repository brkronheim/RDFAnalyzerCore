/*************************************************************************
 * Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include "ROOT/RDF/RColumnRegister.hxx"
#include "ROOT/RDF/RDefineBase.hxx"
#include "ROOT/RDF/RDefineReader.hxx"
#include "ROOT/RDF/RLoopManager.hxx"
#include "ROOT/RDF/RVariationBase.hxx"
#include "ROOT/RDF/RVariationsDescription.hxx"
#include "ROOT/RDF/RVariationReader.hxx"
#include "ROOT/RDF/Utils.hxx" // IsStrInVec

#include "RootRuntimeVersion.hxx"

#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <cstdlib>

// Called during per-slot reader binding. The existing ROOT reader retains all
// variation routing and the lifetime of the original returned value pointer.
namespace {
using Base = ROOT::Detail::RDF::RDefineBase;
using ColumnReader = ROOT::Detail::RDF::RColumnReaderBase;
using Stock = ROOT::Internal::RDF::RDefineReader;

void checkRelease()
{
   if (!rdf::root_compat::runtimeVersionMatches())
      throw std::runtime_error("Packed RDF readers require the exact ROOT version used at build time");
}

// Addresses are stable for the lifetime of ROOT's final native Define. Two
// checked displacements leave room in a 32-byte record for the bound Update.

class ZeroReader final : public ColumnReader {
   using UpdateFn = void (*)(Base *, unsigned, Long64_t);
   Base *producer;
   UpdateFn update;
   int32_t stampOffset;
   int32_t valueOffset;

   static bool displacement(const Base &p, const void *field, int32_t &out) {
      const auto a = reinterpret_cast<uintptr_t>(&p);
      const auto b = reinterpret_cast<uintptr_t>(field);
      const auto distance = a > b ? a - b : b - a;
      if (distance > INT32_MAX)
         return false;
      out = static_cast<int32_t>(distance);
      if (b < a)
         out = -out;
      return true;
   }
   uintptr_t address(int32_t offset) const {
      return reinterpret_cast<uintptr_t>(producer) + static_cast<uintptr_t>(offset);
   }
public:
   static bool supports(Base &p, const Long64_t *s, void *v) {
      int32_t offset;
      return displacement(p,s,offset) && displacement(p,v,offset);
   }
   ZeroReader(Base &p, const Long64_t *s, void *v) : producer(&p),
      update((UpdateFn)(p.*static_cast<void (Base::*)(unsigned,Long64_t)>(&Base::Update))) {
      if (!displacement(p,s,stampOffset) || !displacement(p,v,valueOffset)) std::abort();
   }
private:
   void *GetImpl(Long64_t entry) final {
      if (entry < 0 || *reinterpret_cast<const Long64_t*>(address(stampOffset)) != entry)
         update(producer,0,entry);
      return reinterpret_cast<void*>(address(valueOffset));
   }
};
static_assert(sizeof(ZeroReader)==32);
// Bindings own these batches. The thread-local construction cursor owns none.
struct Batch {
   std::vector<ZeroReader> entries;
   Batch() { entries.reserve(128); }
};
struct BoundReader {
   std::shared_ptr<Stock> original;
   std::shared_ptr<Batch> batch;
   ZeroReader *fast;
   // The original shared pointer owns Stock; keep both it and the batch alive.
   void operator()(Stock *) const noexcept {}
};
BoundReader bindZero(std::shared_ptr<Stock> original,Base &producer,const Long64_t *stamp,void *value) {
   static thread_local std::weak_ptr<Batch> current;
   auto batch=current.lock();
   if (!batch || batch->entries.size()==batch->entries.capacity()) {
      batch=std::make_shared<Batch>();
      current=batch;
   }
   auto &reader=batch->entries.emplace_back(producer,stamp,value);
   return {std::move(original),std::move(batch),&reader};
}
std::atomic<unsigned long> fastBindings{0};
}
extern "C" unsigned long rdf_root_packed_reader_bindings_v1() { return fastBindings.load(std::memory_order_relaxed); }

ROOT::Detail::RDF::RColumnReaderBase &
ROOT::Internal::RDF::RDefinesWithReaders::GetLocalityReader(unsigned slot, const std::string &variation) {
   auto &original=GetReader(slot,variation);
   if (slot!=0) return original;
   auto &binding=fReadersPerVariation.at(slot).at(variation);
   if (auto existing=std::get_deleter<BoundReader>(binding)) return *existing->fast;
   auto &producer=original.LocalityProducer();
   // Pinned Itanium ABI. Only ROOT's final RDefine template has the audited
   // fixed-length stamp allocation. Unknown/custom producers use stock logic.
   const std::string_view type(typeid(producer).name());
   if (type.find("N4ROOT6Detail3RDF7RDefineI")!=0 && type.find("N4ROOT6Detail3RDF17InlineInputDefineI")!=0) return original;
   if (!ZeroReader::supports(producer,producer.LocalityStamp(slot),original.LocalityValue())) return original;
   auto owner=bindZero(binding,producer,producer.LocalityStamp(slot),original.LocalityValue());
   binding=std::shared_ptr<Stock>(&original,std::move(owner));
   fastBindings.fetch_add(1, std::memory_order_relaxed);
   return *std::get_deleter<BoundReader>(binding)->fast;
}

namespace ROOT { namespace Internal { namespace RDF {
RDFDetail::RColumnReaderBase *RColumnRegister::GetReader(unsigned int slot, const std::string &colName,
                                                         const std::string &variationName,
                                                         const std::type_info &requestedType)
{
   checkRelease();
   // try variations first
   if (variationName != "nominal") {
      auto *variationAndReaders = FindVariationAndReaders(colName, variationName);
      if (variationAndReaders != nullptr) {
         const auto &actualType = variationAndReaders->GetVariation().GetTypeId();
         CheckReaderTypeMatches(actualType, requestedType, colName);
         return &variationAndReaders->GetReader(slot, colName, variationName);
      }
   }

   // otherwise try defines
   auto it = std::find_if(fDefines->begin(), fDefines->end(),
                          [&colName](const DefinesMap_t::value_type &kv) { return kv.first == colName; });
   if (it != fDefines->end()) {
      const auto &actualType = it->second->GetDefine().GetTypeId();
      CheckReaderTypeMatches(actualType, requestedType, colName);
      return &it->second->GetLocalityReader(slot, variationName);
   }

   return nullptr;
}

/// Return a RDefineReader or a RVariationReader, or nullptr if not available.
/// No type checking is done on the requested reader.
RDFDetail::RColumnReaderBase *
RColumnRegister::GetReaderUnchecked(unsigned int slot, const std::string &colName, const std::string &variationName)
{
   checkRelease();
   // try variations first
   if (variationName != "nominal") {
      if (auto *variationAndReaders = FindVariationAndReaders(colName, variationName)) {
         return &variationAndReaders->GetReader(slot, colName, variationName);
      }
   }

   // otherwise try defines
   if (auto it =
          std::find_if(fDefines->begin(), fDefines->end(), [&colName](const auto &kv) { return kv.first == colName; });
       it != fDefines->end()) {
      return &it->second->GetLocalityReader(slot, variationName);
   }

   return nullptr;
}


}}}
