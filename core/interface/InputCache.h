#ifndef RDF_INPUT_CACHE_H
#define RDF_INPUT_CACHE_H

#include <TChain.h>
#include <api/IConfigurationProvider.h>
#include <memory>
#include <chrono>
#include <string>

/** Single-threaded RDF input policy. ROOT's TTreeReader supplies the physical
 * branch dependencies, after JIT and before the first event is evaluated.
 * Never infer dependencies by parsing C++ expressions or subtracting names.
 */
class InputCacheChain : public TChain {
public:
  /// Reporting-only chain, for friends whose cache policy must remain ROOT's.
  explicit InputCacheChain(const std::string &name);
  InputCacheChain(const std::string &name, const IConfigurationProvider &config,
                  bool optimize = true);
  InputCacheChain(const std::string &name, const IConfigurationProvider &config,
                  bool optimize, bool asyncOpenNextFile);
  ~InputCacheChain() override;
  Long64_t LoadTree(Long64_t entry) override;
  Int_t AddBranchToCache(const char *name, bool subbranches = false) override;
  Int_t AddBranchToCache(TBranch *branch, bool subbranches = false) override;
  Int_t StopCacheLearningPhase() override;

  /// Call before running a custom action that reads undeclared tree branches.
  /// Ordinary RDF actions (including raw RNodes) need no such notification.
  void markDependenciesIncomplete(const std::string &reason);
  void reportCache(const char *reason = "snapshot") const;
  bool statisticsEnabled() const { return statistics_; }

private:
  void configureCurrentTree();
  void scheduleNextFileOpen();
  bool asyncPrefetch_ = true;
  bool asyncOpenNextFile_ = false;
  bool explicitBranches_ = true;
  bool statistics_ = true;
  bool optimize_ = true;
  bool dependenciesResolved_ = false;
  int verbosity_ = 0;
  Long64_t first_ = 0;
  Long64_t last_ = -1;
  int configuredTree_ = -1;
  int pendingAsyncOpenTree_ = -1;
  std::string fallbackReason_;
  std::chrono::steady_clock::time_point fileStarted_ = std::chrono::steady_clock::now();
  mutable std::string lastReport_;
};

/// Statistics are enabled by default independently of optimization. Returns
/// an ordinary TChain only when neither statistics nor optimization is needed.
/// allowOptimization=false is used for friends. Never changes ROOT threading.
std::unique_ptr<TChain> makeInputCacheChain(
    const std::string &name, const IConfigurationProvider &config,
    bool allowOptimization = true);

#endif
