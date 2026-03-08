#ifndef COUNTERSERVICE_H_INCLUDED
#define COUNTERSERVICE_H_INCLUDED

#include "api/IAnalysisService.h"
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RResultPtr.hxx>
#include <TH1D.h>
#include <optional>
#include <string>
#include <chrono>

/**
 * @brief Analysis service for logging per-sample event counts.
 *
 * Total event count and sign-weighted event count are booked during initialize().
 * An integer-branch histogram (e.g. a stitching code) can optionally be booked
 * after the analysis branch is defined by calling bookIntWeightHistogram() before
 * the event loop runs. This ensures the event loop executes only once.
 */
class CounterService : public IAnalysisService {
public:
  void initialize(ManagerContext& ctx) override;
  void finalize(ROOT::RDF::RNode& df) override;
  void onPreFilter(ROOT::RDF::RNode& df) override;

  /**
   * @brief Book an integer-branch counter histogram before the event loop.
   *
   * Must be called after the relevant branch has been defined on the dataframe
   * and before any action triggers the event loop. The histogram range [low, high)
   * must be known in advance (use nBins bins). If a weight branch was configured,
   * it is applied; a companion sign-weight histogram is also booked.
   *
   * A warning is emitted if this is called after filters have been applied,
   * because the counter histograms are intended to run over all events.
   *
   * @param df   The RNode that already has @p branch defined.
   * @param branch Column name of the integer code (e.g. "stitchBinNLO").
   * @param nBins  Number of histogram bins.
   * @param low    Lower edge of the histogram range.
   * @param high   Upper edge of the histogram range.
   */
  void bookIntWeightHistogram(ROOT::RDF::RNode df,
                              const std::string& branch,
                              int nBins, double low, double high);

private:
  ManagerContext* ctx_m = nullptr;
  std::string sampleName_m;
  std::string weightBranch_m;
  std::string intWeightBranch_m;
  std::optional<ROOT::RDF::RNode> preFilterDf_m;
  ROOT::RDF::RResultPtr<ULong64_t> countResult;
  ROOT::RDF::RResultPtr<Float_t> weightSumResult;
  ROOT::RDF::RResultPtr<Int_t> weightSignSumResult;
  bool filtersApplied_m = false; // set by onPreFilter; used to warn in bookIntWeightHistogram

  // Optional pre-booked int-weight histograms (booked via bookIntWeightHistogram)
  std::optional<ROOT::RDF::RResultPtr<TH1D>> intWeightHistResult_m;
  std::optional<ROOT::RDF::RResultPtr<TH1D>> intWeightSignHistResult_m;
  std::string intWeightHistBranch_m;

  // start time recorded at initialize(); used to compute processing speed in finalize()
  std::chrono::steady_clock::time_point startTime_m{};
};

#endif // COUNTERSERVICE_H_INCLUDED
