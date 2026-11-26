#pragma once

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDF/ColumnReaderUtils.hxx>
#include <ROOT/RDF/RDefineBase.hxx>
#include <algorithm>
#include <cassert>
#include <map>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace rdf::experimental {

// Reader bindings have the usual RDF task lifetime. A kernel can request
// individual values for the current entry after inspecting a gate.
class DeferredInputs {
  using Reader = ROOT::Detail::RDF::RColumnReaderBase;
  const std::vector<Reader *> &readers_;
  const ROOT::RDF::ColumnNames_t &names_;
  const std::vector<const std::type_info *> &types_;
  Long64_t entry_;

public:
  DeferredInputs(const std::vector<Reader *> &readers, const ROOT::RDF::ColumnNames_t &names,
                 const std::vector<const std::type_info *> &types, Long64_t entry)
      : readers_(readers), names_(names), types_(types), entry_(entry) {}

  template <typename T>
  const T &get(std::size_t index) const {
    assert(index < readers_.size() && *types_[index] == typeid(T));
    if (const auto *value = readers_[index]->template TryGet<T>(entry_))
      return *value;
    throw std::out_of_range("Deferred RDF Define: missing column '" + names_[index] +
                            "' at entry " + std::to_string(entry_));
  }
};

namespace detail {
namespace RootDetail = ROOT::Detail::RDF;
namespace RootInternal = ROOT::Internal::RDF;

template <typename F>
class DeferredDefine final : public RootDetail::RDefineBase {
  using Result = std::decay_t<std::invoke_result_t<F, unsigned, const DeferredInputs &>>;
  using Reader = RootDetail::RColumnReaderBase;

  struct Slot {
    Result result{};
    std::vector<Reader *> readers;
  };
  F kernel_;
  std::vector<const std::type_info *> types_;
  std::vector<Slot> slots_;
  std::map<std::string, std::unique_ptr<DeferredDefine>> varied_;

public:
  DeferredDefine(const std::string &name, F kernel, const ROOT::RDF::ColumnNames_t &columns,
                 std::vector<const std::type_info *> types,
                 const RootInternal::RColumnRegister &registry, RootDetail::RLoopManager &loop,
                 const std::string &variation = "nominal")
      : RDefineBase(name, RootInternal::TypeID2TypeName(typeid(Result)), registry, loop, columns,
                    variation),
        kernel_(std::move(kernel)), types_(std::move(types)), slots_(loop.GetNSlots()) {
    fLoopManager->Register(this);
  }

  ~DeferredDefine() override { fLoopManager->Deregister(this); }

  void InitSlot(TTreeReader *treeReader, unsigned slot) override {
    auto &readers = slots_[slot].readers;
    readers.resize(types_.size());
    for (std::size_t i = 0; i < types_.size(); ++i) {
      const auto &name = fColumnNames[i];
      const auto &type = *types_[i];
      auto *defined = fColRegister.GetReader(slot, name, fVariation, type);
      readers[i] = RootInternal::GetColumnReader(slot, defined, *fLoopManager, treeReader, name, type);
    }
    fLastCheckedEntry[slot * RootInternal::CacheLineStep<Long64_t>()] = -1;
  }

  void FinalizeSlot(unsigned slot) override {
    slots_[slot].readers.clear();
    for (auto &item : varied_)
      item.second->FinalizeSlot(slot);
  }

  void *GetValuePtr(unsigned slot) override { return &slots_[slot].result; }
  const std::type_info &GetTypeId() const override { return typeid(Result); }

  void Update(unsigned slot, Long64_t entry) override {
    auto &stamp = fLastCheckedEntry[slot * RootInternal::CacheLineStep<Long64_t>()];
    if (stamp == entry)
      return;
    auto &state = slots_[slot];
    state.result = kernel_(slot, DeferredInputs(state.readers, fColumnNames, types_, entry));
    stamp = entry;
  }

  void MakeVariations(const std::vector<std::string> &variations) override {
    for (const auto &variation : variations) {
      if (std::find(fVariationDeps.begin(), fVariationDeps.end(), variation) == fVariationDeps.end() ||
          varied_.count(variation))
        continue;
      varied_.emplace(variation, std::make_unique<DeferredDefine>(
          fName, kernel_, fColumnNames, types_, fColRegister, *fLoopManager, variation));
    }
  }

  RootDetail::RDefineBase &GetVariedDefine(const std::string &variation) override {
    const auto it = varied_.find(variation);
    return it == varied_.end() ? *this : *it->second;
  }
};

// Use RInterface's protected graph-construction interface to preserve the
// parent filter and ROOT's own per-slot reader lifecycle.
class DeferredNode : public ROOT::RDF::RNode {
  using Base = ROOT::RDF::RNode;
  DeferredNode(const std::shared_ptr<RootDetail::RNodeBase> &parent, RootDetail::RLoopManager &loop,
               const RootInternal::RColumnRegister &registry)
      : Base(parent, loop, registry) {}

public:
  explicit DeferredNode(ROOT::RDF::RNode node) : Base(std::move(node)) {}

  template <typename F>
  ROOT::RDF::RNode bind(const std::string &name, F kernel, const ROOT::RDF::ColumnNames_t &columns,
                       const std::vector<const std::type_info *> &types) {
    if (columns.size() != types.size() || HasColumn(name))
      throw std::invalid_argument("Invalid deferred RDF definition '" + name + "'");
    const auto validated = GetValidatedColumnNames(columns.size(), columns);
    CheckAndFillDSColumns(validated, types);
    auto registry = fColRegister;
    registry.AddDefine(std::make_shared<DeferredDefine<F>>(
        name, std::move(kernel), validated, types, fColRegister, *fLoopManager));
    return DeferredNode(GetProxiedPtr(), *fLoopManager, registry);
  }
};
} // namespace detail

template <typename F>
ROOT::RDF::RNode defineDeferred(ROOT::RDF::RNode node, const std::string &name, F kernel,
                               const ROOT::RDF::ColumnNames_t &columns,
                               const std::vector<const std::type_info *> &types) {
  return detail::DeferredNode(std::move(node)).bind(name, std::move(kernel), columns, types);
}

} // namespace rdf::experimental
