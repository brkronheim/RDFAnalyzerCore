// Adapted from ROOT 6.40.04 RDefine.hxx, Copyright CERN, LGPL-2.1.
#pragma once
#include "ROOT/RDF/RDefine.hxx"
namespace ROOT::Detail::RDF {
template <typename F, typename ExtraArgsTag = ExtraArgsForDefine::None>
class R__CLING_PTRCHECK(off) InlineInputDefine final : public RDefineBase {
   // shortcuts
   using NoneTag = ExtraArgsForDefine::None;
   using SlotTag = ExtraArgsForDefine::Slot;
   using SlotAndEntryTag = ExtraArgsForDefine::SlotAndEntry;
   // other types
   using FunParamTypes_t = typename CallableTraits<F>::arg_types;
   using ColumnTypesTmp_t =
      RDFInternal::RemoveFirstParameterIf_t<std::is_same<ExtraArgsTag, SlotTag>::value, FunParamTypes_t>;
   using ColumnTypes_t =
      RDFInternal::RemoveFirstTwoParametersIf_t<std::is_same<ExtraArgsTag, SlotAndEntryTag>::value, ColumnTypesTmp_t>;
   using TypeInd_t = std::make_index_sequence<ColumnTypes_t::list_size>;
   using ret_type = typename CallableTraits<F>::ret_type;
   // Avoid instantiating vector<bool> as `operator[]` returns temporaries in that case. Use std::deque instead.
   using ValuesPerSlot_t =
      std::conditional_t<std::is_same<ret_type, bool>::value, std::deque<ret_type>, std::vector<ret_type>>;

   F fExpression;
   ValuesPerSlot_t fLastResults;

   /// Column readers per slot and per input column
   using Readers = std::array<RColumnReaderBase *, ColumnTypes_t::list_size>;
   Readers fFirst{};
   std::vector<Readers> fOtherSlots;
   Readers &InputReaders(unsigned slot) { return slot == 0 ? fFirst : fOtherSlots[slot-1]; }

   /// Define objects corresponding to systematic variations other than nominal for this defined column.
   /// The map key is the full variation name, e.g. "pt:up".
   std::unordered_map<std::string, std::unique_ptr<RDefineBase>> fVariedDefines;

   template <typename ColType>
   __attribute__((always_inline)) ColType &GetValueChecked(const Readers &inputs, std::size_t readerIdx, Long64_t entry)
   {
      if (auto *val = inputs[readerIdx]->template TryGet<ColType>(entry)) return *val;
      MissingInput(readerIdx,entry);
   }
   [[noreturn]] __attribute__((cold,noinline)) void MissingInput(std::size_t readerIdx,Long64_t entry)
   {
      throw std::out_of_range{"RDataFrame: Define could not retrieve value for column '" + fColumnNames[readerIdx] +
                              "' for entry " + std::to_string(entry) +
                              ". You can use the DefaultValueFor operation to provide a default value, or "
                              "FilterAvailable/FilterMissing to discard/keep entries with missing values instead."};
   }

   template <typename... ColTypes, std::size_t... S>
   void UpdateHelper(unsigned int slot, Long64_t entry, TypeList<ColTypes...>, std::index_sequence<S...>, NoneTag)
   {
      const auto &inputs = InputReaders(slot);
      fLastResults[slot * RDFInternal::CacheLineStep<ret_type>()] =
         fExpression(GetValueChecked<ColTypes>(inputs, S, entry)...);
      (void)entry; // avoid unused parameter warning (gcc 12.1)
   }

   template <typename... ColTypes, std::size_t... S>
   void UpdateHelper(unsigned int slot, Long64_t entry, TypeList<ColTypes...>, std::index_sequence<S...>, SlotTag)
   {
      const auto &inputs = InputReaders(slot);
      fLastResults[slot * RDFInternal::CacheLineStep<ret_type>()] =
         fExpression(slot, GetValueChecked<ColTypes>(inputs, S, entry)...);
      (void)entry; // avoid unused parameter warning (gcc 12.1)
   }

   template <typename... ColTypes, std::size_t... S>
   void
   UpdateHelper(unsigned int slot, Long64_t entry, TypeList<ColTypes...>, std::index_sequence<S...>, SlotAndEntryTag)
   {
      const auto &inputs = InputReaders(slot);
      fLastResults[slot * RDFInternal::CacheLineStep<ret_type>()] =
         fExpression(slot, entry, GetValueChecked<ColTypes>(inputs, S, entry)...);
   }

public:
   InlineInputDefine(std::string_view name, std::string_view type, F expression, const ROOT::RDF::ColumnNames_t &columns,
           const RDFInternal::RColumnRegister &colRegister, RLoopManager &lm,
           const std::string &variationName = "nominal")
      : RDefineBase(name, type, colRegister, lm, columns, variationName), fExpression(std::move(expression)),
        fLastResults(lm.GetNSlots() * RDFInternal::CacheLineStep<ret_type>()), fOtherSlots(lm.GetNSlots()-1)
   {
      fLoopManager->Register(this);
   }

   InlineInputDefine(const InlineInputDefine &) = delete;
   InlineInputDefine &operator=(const InlineInputDefine &) = delete;
   ~InlineInputDefine() { fLoopManager->Deregister(this); }

   void InitSlot(TTreeReader *r, unsigned int slot) final
   {
      RDFInternal::RColumnReadersInfo info{fColumnNames, fColRegister, fIsDefine.data(), *fLoopManager};
      InputReaders(slot) = RDFInternal::GetColumnReaders(slot, r, ColumnTypes_t{}, info, fVariation);
      fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()] = -1;
   }

   /// Return the (type-erased) address of the Define'd value for the given processing slot.
   void *GetValuePtr(unsigned int slot) final
   {
      return static_cast<void *>(&fLastResults[slot * RDFInternal::CacheLineStep<ret_type>()]);
   }

   /// Update the value at the address returned by GetValuePtr with the content corresponding to the given entry
   void Update(unsigned int slot, Long64_t entry) final
   {
      if (entry != fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()]) {
         // evaluate this define expression, cache the result
         UpdateHelper(slot, entry, ColumnTypes_t{}, TypeInd_t{}, ExtraArgsTag{});
         fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()] = entry;
      }
   }

   void Update(unsigned int /*slot*/, const ROOT::RDF::RSampleInfo &/*id*/) final {}

   const std::type_info &GetTypeId() const final { return typeid(ret_type); }

   /// Clean-up operations to be performed at the end of a task.
   void FinalizeSlot(unsigned int slot) final
   {
      InputReaders(slot).fill(nullptr);

      for (auto &e : fVariedDefines)
         e.second->FinalizeSlot(slot);
   }

   /// Create clones of this Define that work with values in varied "universes".
   void MakeVariations(const std::vector<std::string> &variations) final
   {
      for (const auto &variation : variations) {
         if (std::find(fVariationDeps.begin(), fVariationDeps.end(), variation) == fVariationDeps.end()) {
            // this Defined quantity does not depend on this variation, so no need to create a varied InlineInputDefine
            continue;
         }
         if (fVariedDefines.find(variation) != fVariedDefines.end())
            continue; // we already have this variation stored

         // the varied defines get a copy of the callable object.
         // TODO document this
         auto variedDefine = std::unique_ptr<RDefineBase>(
            new InlineInputDefine(fName, fType, fExpression, fColumnNames, fColRegister, *fLoopManager, variation));
         // TODO switch to fVariedDefines.insert({variationName, std::move(variedDefine)}) when we drop gcc 5
         fVariedDefines[variation] = std::move(variedDefine);
      }
   }

   /// Return a clone of this Define that works with values in the variationName "universe".
   RDefineBase &GetVariedDefine(const std::string &variationName) final
   {
      auto it = fVariedDefines.find(variationName);
      if (it == fVariedDefines.end()) {
         // We don't have a varied InlineInputDefine for this variation.
         // This means we don't depend on it and we can return ourselves, i.e. the InlineInputDefine for the nominal universe.
         assert(std::find(fVariationDeps.begin(), fVariationDeps.end(), variationName) == fVariationDeps.end());
         return *this;
      }

      return *(it->second);
   }
};

}
