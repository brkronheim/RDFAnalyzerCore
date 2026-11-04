/**
 * @file plots.h
 * @brief Classes and utilities for histogram and plot management in the
 * analysis framework.
 *
 * This header defines classes for managing histograms, selections, and plotting
 * utilities, including THnMulti, histInfo, and selectionInfo.
 */
#ifndef PLOTS_H_INCLUDED
#define PLOTS_H_INCLUDED

#include <ROOT/RDF/HistoModels.hxx>
#include <algorithm>
#include <array>
#include <cmath>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <Math/MinimizerOptions.h>
#include <cstdlib>
#include <iostream>
#include <ROOT/RDataFrame.hxx>
#include <TAxis.h>
#include <TCanvas.h>
#include <TF1.h>
#include <TFile.h>
#include <TFitResult.h>
#include <TGraphErrors.h>
#include <TH1.h>
#include <TH1D.h>
#include <TH1F.h>
#include <TH1I.h>
#include <THn.h>
#include <THnSparse.h>
#include <TLegend.h>
#include <TLine.h>
#include <TPad.h>
#include <TRatioPlot.h>
#include <TText.h>
#include <TVirtualFitter.h>

#include <TBox.h>
#include <TGraph.h>
#include <TROOT.h>
#include <TStyle.h>

#include <boost/histogram.hpp>
#include <boost/histogram/unsafe_access.hpp>
#include <HistogramAccumulators.h>
#if __has_include(<boost/unordered/unordered_flat_map.hpp>)
#include <boost/unordered/unordered_flat_map.hpp>
#define RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE 1
#endif
#include <unordered_map>
#include <variant>

#include <limits>


/// @brief Estimates the total memory in bytes required for dense per-thread histogram storage.
///
/// Returns std::numeric_limits<std::size_t>::max() on numeric overflow so that
/// the caller always treats an uncomputable estimate as exceeding any threshold.
///
/// @param nbins   Number of bins along each axis (overflow excluded from the parameter;
///                two overflow bins per axis are added internally).
/// @param nSlots  Number of parallel threads/slots.
/// @param bytesPerBin  Memory consumed by one bin (content + variance).
inline std::size_t estimateDenseMemoryBytes(const std::vector<Int_t>& nbins,
                                             unsigned int nSlots,
                                             std::size_t bytesPerBin) {
    constexpr std::size_t kMax = std::numeric_limits<std::size_t>::max();
    std::size_t total = 1;
    for (int n : nbins) {
        const std::size_t axBins = static_cast<std::size_t>(n) + 2; // include over/underflow
        if (axBins != 0 && total > kMax / axBins) return kMax;
        total *= axBins;
    }
    if (nSlots != 0 && total > kMax / nSlots) return kMax;
    total *= nSlots;
    if (bytesPerBin != 0 && total > kMax / bytesPerBin) return kMax;
    return total * bytesPerBin;
}

/// Maximum total bytes for dense per-thread storage before falling back to sparse storage.
static constexpr std::size_t kDenseMemoryThresholdBytes = 64ULL * 1024 * 1024; // 64 MiB


struct histFillInfo {
  Bool_t forceSparse = false;
  std::size_t denseMemoryLimitBytes = kDenseMemoryThresholdBytes;
  bool boostFloatContents = false;
  std::string boostSparseBackend = "map";
  std::string name = "";
  std::string title = "";
  Int_t dim = 5;
  Int_t nSlots = 0;
  std::vector<Int_t> nbins = {};
  std::vector<Double_t> xmin = {};
  std::vector<Double_t> xmax = {};
  std::vector<std::vector<Double_t>> binEdges = {};
  Bool_t channel_hasSystematic = false;
  Bool_t controlRegion_hasSystematic = false;
  Bool_t sampleCategory_hasSystematic = false;
  Bool_t weight_hasSystematic = false;
  Bool_t hasSystematic = false;
  Bool_t channel_hasMultiFill = false;
  Bool_t controlRegion_hasMultiFill = false;
  Bool_t sampleCategory_hasMultiFill = false;
  Bool_t systematic_hasMultiFill = false;
  Bool_t weight_hasMultiFill = false;
  Bool_t hasMultiFill = false;
};

// Opt-in per-action diagnostics. No instrumentation in the event fill path.
class HistogramFinalizeProfile {
public:
  HistogramFinalizeProfile(const char *backend, const char *storage,
                           const std::string &name, std::size_t denseBytes,
                           const THnSparseF *result)
      : backend_(backend), storage_(storage), name_(name), denseBytes_(denseBytes),
        result_(result), enabled_(std::getenv("RDF_HISTOGRAM_PROFILE") != nullptr) {
    if (enabled_) { wall_ = std::chrono::steady_clock::now(); cpu_ = std::clock(); }
  }
  ~HistogramFinalizeProfile() {
    if (enabled_)
      std::fprintf(stderr, "histogram profile: backend=%s storage=%s name=%s dense_bytes=%zu bins=%lld wall_s=%.9f cpu_s=%.9f\n",
          backend_, storage_, name_.c_str(), denseBytes_, result_->GetNbins(),
          std::chrono::duration<double>(std::chrono::steady_clock::now()-wall_).count(),
          double(std::clock()-cpu_)/CLOCKS_PER_SEC);
  }
private:
  const char *backend_, *storage_;
  const std::string &name_;
  std::size_t denseBytes_;
  const THnSparseF *result_;
  bool enabled_;
  std::chrono::steady_clock::time_point wall_;
  std::clock_t cpu_{};
};

/**
 * @class THnMulti
 * @brief Multi-threaded N-dimensional histogram action for ROOT RDataFrame.
 *
 * This class manages a set of THnSparseF histograms, one per thread, and merges
 * them at the end of processing. It is used as a custom action in ROOT's
 * RDataFrame for efficient multi-threaded histogramming.
 */
class THnMulti : public ROOT::Detail::RDF::RActionImpl<THnMulti> {
  // Keep ROOT's statistics bookkeeping in the accumulator that owns it.
  class DenseAccumulator final : public THnF {
  public:
    using THnF::THnF;
    void FillIndexed(const Double_t* coordinates, Long64_t bin, Double_t weight) {
      UpdateXStat(coordinates, weight);
      FillBin(bin, weight);
    }
  };

public:
  /**
   * @brief Type alias for the result type (THnSparseF)
   */
  using Result_t = THnSparseF;

  // Define a function pointer type for fill functions
  using FillFuncType = void (THnMulti::*)(unsigned int,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Bool_t>&,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Int_t>&);

  /**
   * @brief Construct a new THnMulti object
   * @param fillInfo Histogram fill information
   */
  THnMulti(histFillInfo &fillInfo)
    : nSlots_m(fillInfo.nSlots), dim_m(fillInfo.dim), nbins_m(fillInfo.nbins), xmin_m(fillInfo.xmin), xmax_m(fillInfo.xmax), channel_hasSystematic_m(fillInfo.channel_hasSystematic), controlRegion_hasSystematic_m(fillInfo.controlRegion_hasSystematic), 
      sampleCategory_hasSystematic_m(fillInfo.sampleCategory_hasSystematic), weight_hasSystematic_m(fillInfo.weight_hasSystematic), hasSystematic_m(fillInfo.hasSystematic), channel_hasMultiFill_m(fillInfo.channel_hasMultiFill), 
      controlRegion_hasMultiFill_m(fillInfo.controlRegion_hasMultiFill), sampleCategory_hasMultiFill_m(fillInfo.sampleCategory_hasMultiFill), systematic_hasMultiFill_m(fillInfo.systematic_hasMultiFill), weight_hasMultiFill_m(fillInfo.weight_hasMultiFill), 
      hasMultiFill_m(fillInfo.hasMultiFill), name_m(fillInfo.name), title_m(fillInfo.title) {

    binEdges_m = fillInfo.binEdges;

    // Auto-select dense (THnF) vs sparse (THnSparseF) per-thread accumulators.
    // THnF uses direct array indexing (O(1)) which is faster than THnSparseF's
    // hash-based lookup, but consumes memory proportional to all bins.
    // Float_t content (4 B) + Double_t sumw2 (8 B) = 12 bytes per bin (with Sumw2).
    useDense_m = !fillInfo.forceSparse &&
        estimateDenseMemoryBytes(nbins_m, nSlots_m, 12) <= fillInfo.denseMemoryLimitBytes;

    const auto setConfiguredBinEdges = [this](auto &histogram) {
      const auto nAxes = std::min(binEdges_m.size(),
                                  static_cast<std::size_t>(dim_m));
      for (std::size_t axis = 0; axis < nAxes; ++axis) {
        if (!binEdges_m[axis].empty()) {
          histogram->SetBinEdges(static_cast<Int_t>(axis),
                                 binEdges_m[axis].data());
        }
      }
    };

    for (unsigned int i = 0; i < nSlots_m; i++) {
      if (useDense_m) {
        fPerThreadDense_m.push_back(std::make_shared<DenseAccumulator>(
            (name_m + "_" + std::to_string(i)).c_str(), title_m.c_str(), dim_m,
            nbins_m.data(), xmin_m.data(), xmax_m.data()));
        setConfiguredBinEdges(fPerThreadDense_m[i]);
        fPerThreadDense_m[i]->Sumw2();
      } else {
        fPerThreadResults.push_back(std::make_shared<THnSparseF>(
            (name_m + "_" + std::to_string(i)).c_str(), title_m.c_str(), dim_m,
            nbins_m.data(), xmin_m.data(), xmax_m.data()));
        setConfiguredBinEdges(fPerThreadResults[i]);
        fPerThreadResults[i]->Sumw2();
      }
    }
    if (!useDense_m && !fPerThreadResults.empty()) {
      // Publish slot zero as the result from the start. Its address stays
      // stable for RResultPtr, and finalization can merge into it without
      // copying an entire occupied sparse histogram (especially costly with
      // one slot, where no merge is needed at all).
      fFinalResult = fPerThreadResults.front();
      fFinalResult->SetName(name_m.c_str());
    } else {
      fFinalResult = std::make_shared<Result_t>(
          name_m.c_str(), title_m.c_str(), dim_m,
          nbins_m.data(), xmin_m.data(), xmax_m.data());
      setConfiguredBinEdges(fFinalResult);
    }

    // Select the per-bin fill helper: dense uses THnF (direct array), sparse uses THnSparseF (hash).
    fillHistFunc_ = useDense_m ? &THnMulti::fillHistDense_ : &THnMulti::fillHistSparse_;
    // The first four axes are normally integer category/variation labels in
    // unit-width bins. Retain ROOT lookup for other layouts and noninteger
    // values. Half/integer edges in this bounded range have exact arithmetic.
    bool indexedCategories = useDense_m && !fPerThreadDense_m.empty() && dim_m == 5;
    for (int d = 0; indexedCategories && d < 4; ++d) {
      indexedCategories = xmax_m[d] - xmin_m[d] == nbins_m[d] &&
          std::abs(xmin_m[d]) <= 1000000 && std::abs(xmax_m[d]) <= 1000000 &&
          std::floor(2 * xmin_m[d]) == 2 * xmin_m[d] &&
          (binEdges_m.size() <= static_cast<std::size_t>(d) || binEdges_m[d].empty());
    }
    if (indexedCategories) {
      for (int d = 0; d < 5; ++d) {
        denseAxes_m[d] = fPerThreadDense_m[0]->GetAxis(d);
        denseStrides_m[d] = fPerThreadDense_m[0]->GetArray().GetCellSize(d);
        if (d < 4) categoryOffsets_m[d] = 1 - static_cast<int>(std::ceil(xmin_m[d]));
      }
      fillHistFunc_ = &THnMulti::fillHistDenseIndexed_;
    }

    // Set the fill function pointer based on configuration
    // (This logic can be expanded for more fast paths as needed)
    if (!hasSystematic_m && !channel_hasSystematic_m && !controlRegion_hasSystematic_m && !sampleCategory_hasSystematic_m && !weight_hasSystematic_m &&
        !hasMultiFill_m && !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m && !systematic_hasMultiFill_m) {
      fillFunc_ = &THnMulti::SingleNoSystematicFill;
    } else if (hasSystematic_m && !hasMultiFill_m && !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m && !systematic_hasMultiFill_m) {
      fillFunc_ = &THnMulti::SingleSystematicFill;
    } else if (!hasSystematic_m && (hasMultiFill_m || channel_hasMultiFill_m || controlRegion_hasMultiFill_m || sampleCategory_hasMultiFill_m || weight_hasMultiFill_m || systematic_hasMultiFill_m)) {
      fillFunc_ = &THnMulti::MultiNoSystematicFill;
    } else {
      fillFunc_ = &THnMulti::GeneralFill;
    }
  }

  /**
   * @brief Move constructor
   */
  THnMulti(THnMulti &&) = default;

  /**
   * @brief Get a shared pointer to the final merged result histogram
   * @return Shared pointer to THnSparseD
   */
  std::shared_ptr<Result_t> GetResultPtr() { return (fFinalResult); }

  /**
   * @brief Called before the event loop to retrieve the result pointer
   * @return Shared pointer to THnSparseD
   */
  std::shared_ptr<THnSparseF> GetResultPtr() const { return fFinalResult; }

  /**
   * @brief Initialize the action (called at the beginning of the event loop)
   */
  void Initialize() {}

  /**
   * @brief Initialize a processing task (called at the beginning of each task)
   * @param reader TTreeReader pointer
   * @param slot Slot index
   */
  void InitTask(TTreeReader *, int) {}

  /// Called at every entry.
  /**
   * @brief Fill the per-thread histogram for one entry (hot loop).
   *
   * This function dispatches to the appropriate fast path fill function based on configuration.
   * selectionMask uses the same systematic-slot ordering as systematicVariation.
   */
  void Exec(unsigned int slot,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
            const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
            const ROOT::VecOps::RVec<Int_t> &__restrict__ nFills) {
    if (selectionMask.size() != systematicVariation.size()) {
      throw std::invalid_argument(
          "THnMulti: selection mask length must match systematic variation length");
    }
    if (baseHistogramValues.empty() || baseHistogramWeights.empty() || systematicVariation.empty() ||
        sampleCategory.empty() || controlRegion.empty() || channel.empty() || nFills.empty()) {
      LogSizes("empty input vector", slot, baseHistogramValues, baseHistogramWeights,
               systematicVariation, sampleCategory, controlRegion, channel, nFills);
      return;
    }
    // Dispatch to the selected fill function for optimal performance
    (this->*fillFunc_)(slot, baseHistogramValues, baseHistogramWeights,
                      systematicVariation, selectionMask, sampleCategory,
                      controlRegion, channel, nFills);
  }

  void SingleNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation, // unused
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory, 
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion, const ROOT::VecOps::RVec<Float_t> &__restrict__ channel, 
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

      if (baseHistogramWeights.size() < 1 || selectionMask.empty() || channel.size() < 1 || controlRegion.size() < 1 ||
          sampleCategory.size() < 1 || systematicVariation.size() < 1) {
        LogSizes("single fill size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                 systematicVariation, sampleCategory, controlRegion, channel, nFills);
        return;
      }

      const Double_t weight = baseHistogramWeights[0];

      // Skip zero-weight entries for efficiency
      if (weight != 0.0 && selectionMask[0]) { // Skip zero-weight or rejected contributions.
        (this->*fillHistFunc_)(slot, channel[0], controlRegion[0], sampleCategory[0], systematicVariation[0], baseHistogramValues[0], weight);
      }
    
  }


  void SingleSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory, 
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion, const ROOT::VecOps::RVec<Float_t> &__restrict__ channel, 
    const ROOT::VecOps::RVec<Int_t> &__restrict__ nFills) {

      int weightCounter = 0;
      int baseFillCounter = 0;
      int controlRegionCounter = 0;
      int channelCounter = 0;
      int sampleCategoryCounter = 0;

      // Main fill loop: iterates over all base histogram values for this event
      while(baseFillCounter < baseHistogramValues.size()) {
        if (weightCounter >= static_cast<int>(baseHistogramWeights.size()) ||
            controlRegionCounter >= static_cast<int>(controlRegion.size()) ||
            channelCounter >= static_cast<int>(channel.size()) ||
            sampleCategoryCounter >= static_cast<int>(sampleCategory.size()) ||
            baseFillCounter >= static_cast<int>(systematicVariation.size()) ||
            baseFillCounter >= static_cast<int>(selectionMask.size())) {
          LogSizes("single systematic size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                   systematicVariation, sampleCategory, controlRegion, channel, nFills);
          break;
        }
        const Double_t weight = baseHistogramWeights[weightCounter];
  
        // Skip zero-weight entries for efficiency
        if (weight != 0.0 && selectionMask[baseFillCounter]) {
          (this->*fillHistFunc_)(slot, channel[channelCounter], controlRegion[controlRegionCounter], sampleCategory[sampleCategoryCounter], systematicVariation[baseFillCounter], baseHistogramValues[baseFillCounter], weight);
        }
        
        controlRegionCounter+=controlRegion_hasSystematic_m;
        sampleCategoryCounter+=sampleCategory_hasSystematic_m;
        channelCounter+=channel_hasSystematic_m;
        weightCounter+=weight_hasSystematic_m;
        baseFillCounter++;
      }
    
  }


  void MultiNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation, // unused
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory, 
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion, const ROOT::VecOps::RVec<Float_t> &__restrict__ channel, 
    const ROOT::VecOps::RVec<Int_t> &__restrict__ nFills) {

      int weightCounter = 0;
      int baseFillCounter = 0;
      int systematicCounter = 0;
      int controlRegionCounter = 0;
      int channelCounter = 0;
      int sampleCategoryCounter = 0;

      
      // Main fill loop: iterates over all base histogram values for this event
      while(baseFillCounter < baseHistogramValues.size()) {
        if (weightCounter >= static_cast<int>(baseHistogramWeights.size()) ||
            controlRegionCounter >= static_cast<int>(controlRegion.size()) ||
            channelCounter >= static_cast<int>(channel.size()) ||
            sampleCategoryCounter >= static_cast<int>(sampleCategory.size()) ||
            systematicCounter >= static_cast<int>(systematicVariation.size()) ||
            systematicCounter >= static_cast<int>(selectionMask.size())) {
          LogSizes("multi fill size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                   systematicVariation, sampleCategory, controlRegion, channel, nFills);
          break;
        }
        const Double_t weight = baseHistogramWeights[weightCounter];
  
        // Skip zero-weight entries for efficiency
        if (weight != 0.0 && selectionMask[systematicCounter]) {
          (this->*fillHistFunc_)(slot, channel[channelCounter], controlRegion[controlRegionCounter], sampleCategory[sampleCategoryCounter], systematicVariation[systematicCounter], baseHistogramValues[baseFillCounter], weight);
        }
        
        systematicCounter+=systematic_hasMultiFill_m;
        controlRegionCounter+=controlRegion_hasMultiFill_m;
        sampleCategoryCounter+=sampleCategory_hasMultiFill_m;
        channelCounter+=channel_hasMultiFill_m;
        weightCounter+=weight_hasMultiFill_m;
        baseFillCounter++;
      }
      
    
  }



  void GeneralFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory, 
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion, const ROOT::VecOps::RVec<Float_t> &__restrict__ channel, 
    const ROOT::VecOps::RVec<Int_t> &__restrict__ nFills) {

    int weightCounter = 0;
    int baseFillCounter = 0;
    int systematicCounter = 0;
    int controlRegionCounter = 0;
    int channelCounter = 0;
    int sampleCategoryCounter = 0;
    int fillCounter = 0;
    // Main fill loop: iterates over all base histogram values for this event
    while(baseFillCounter < baseHistogramValues.size()) {
      if (weightCounter >= static_cast<int>(baseHistogramWeights.size()) ||
          controlRegionCounter >= static_cast<int>(controlRegion.size()) ||
          channelCounter >= static_cast<int>(channel.size()) ||
          sampleCategoryCounter >= static_cast<int>(sampleCategory.size()) ||
          systematicCounter >= static_cast<int>(systematicVariation.size()) ||
          systematicCounter >= static_cast<int>(selectionMask.size()) ||
          systematicCounter >= static_cast<int>(nFills.size())) {
        LogSizes("general fill size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                 systematicVariation, sampleCategory, controlRegion, channel, nFills);
        break;
      }
      const Double_t weight = baseHistogramWeights[weightCounter];

      // Skip zero-weight entries for efficiency
      if (weight != 0.0 && selectionMask[systematicCounter]) {
        (this->*fillHistFunc_)(slot, channel[channelCounter], controlRegion[controlRegionCounter], sampleCategory[sampleCategoryCounter], systematicVariation[systematicCounter], baseHistogramValues[baseFillCounter], weight);
      }

      // Update the counter for everything with multi fill
      // These counters track the current index for each axis that may require multiple fills per event.
      baseFillCounter++;
      fillCounter++;
      if(channel_hasMultiFill_m) {
        channelCounter++;
      }
      if(controlRegion_hasMultiFill_m) {
        controlRegionCounter++;
      }
      if(sampleCategory_hasMultiFill_m) {
        sampleCategoryCounter++;
      }
      if(systematic_hasMultiFill_m) {
        systematicCounter++;
      }
      if(weight_hasMultiFill_m) {
        weightCounter++;
      }
      // Check if we've completed the required number of fills for the current systematic
      if (systematicCounter >= static_cast<int>(nFills.size())) {
        LogSizes("general fill nFills index", slot, baseHistogramValues, baseHistogramWeights,
                 systematicVariation, sampleCategory, controlRegion, channel, nFills);
        break;
      }
      if(fillCounter >= nFills[systematicCounter]) { // finished fill for the first systematic
        fillCounter = 0;
        // Reset or increment counters for each axis depending on systematic/multifill configuration
        // If the axis does not depend on the systematic, reset its counter; otherwise, increment if not multi-fill
        if(!channel_hasSystematic_m) {
          channelCounter = 0;
        } else if(!channel_hasMultiFill_m) {
          channelCounter +=1;
        }
        if(!controlRegion_hasSystematic_m) {
          controlRegionCounter = 0;
        } else if(!controlRegion_hasMultiFill_m) {
          controlRegionCounter +=1;
        }
        if(!sampleCategory_hasSystematic_m) {
          sampleCategoryCounter = 0;
        } else if(!sampleCategory_hasMultiFill_m) {
          sampleCategoryCounter +=1;
        }
        if(!weight_hasSystematic_m) {
          weightCounter = 0;
        } else if(!weight_hasMultiFill_m) {
          weightCounter +=1;
        }
        // If the systematic axis is not multi-fill, increment its counter
        if(!systematic_hasMultiFill_m) {
          systematicCounter +=1;
        }
      }


    }

}

  /**
   * @brief Merge per-thread histograms at the end of the event loop.
   *
   * When using dense per-thread accumulators (THnF), all bins are iterated and
   * non-zero bins are written to the sparse final result, minimising output size.
   * When using sparse per-thread accumulators (THnSparseF), a simple Add() merge
   * is used as before.
   */
  void Finalize() {
    HistogramFinalizeProfile profile("root", useDense_m ? "dense" : "sparse",
        name_m, estimateDenseMemoryBytes(nbins_m, nSlots_m, 12), fFinalResult.get());
    if (useDense_m) {
      // Sum all per-thread dense histograms into the first one
      for (size_t slot = 1; slot < fPerThreadDense_m.size(); slot++) {
        fPerThreadDense_m[0]->Add(fPerThreadDense_m[slot].get());
        fPerThreadDense_m[slot].reset();
      }
      // Convert dense result to sparse, preserving nonempty flow bins too.
      const Long64_t totalBins = fPerThreadDense_m[0]->GetNbins();
      std::vector<Int_t> idxBuf(dim_m);
      for (Long64_t bin = 0; bin < totalBins; ++bin) {
        const Double_t content = fPerThreadDense_m[0]->GetBinContent(bin);
        const Double_t variance = fPerThreadDense_m[0]->GetBinError2(bin);
        if (content == 0.0 && variance == 0.0) continue;
        // Decode only occupied cells; integer bin coordinates preserve flows
        // and avoid the bin -> centre -> FindBin round trip.
        fPerThreadDense_m[0]->GetBinContent(bin, idxBuf.data());
        Long64_t sparseBin = fFinalResult->GetBin(idxBuf.data(), true);
        fFinalResult->SetBinContent(sparseBin, content);
        fFinalResult->SetBinError2(sparseBin, variance);
      }
      fFinalResult->SetEntries(fPerThreadDense_m[0]->GetEntries());
      fPerThreadDense_m.clear();
    } else {
      for (std::size_t slot = 1; slot < fPerThreadResults.size(); ++slot) {
        fFinalResult->Add(fPerThreadResults[slot].get());
        fPerThreadResults[slot].reset();
      }
      fPerThreadResults.clear();
    }
  }

  /**
   * @brief Get the name of this action for RDataFrame
   * @return Action name string
   */
  std::string GetActionName() const { return "THnMulti"; }

private:
  void LogSizes(const char* reason,
                unsigned int slot,
                const ROOT::VecOps::RVec<Float_t> &baseHistogramValues,
                const ROOT::VecOps::RVec<Float_t> &baseHistogramWeights,
                const ROOT::VecOps::RVec<Float_t> &systematicVariation,
                const ROOT::VecOps::RVec<Float_t> &sampleCategory,
                const ROOT::VecOps::RVec<Float_t> &controlRegion,
                const ROOT::VecOps::RVec<Float_t> &channel,
                const ROOT::VecOps::RVec<Int_t> &nFills) const {
    static const bool enabled = (std::getenv("RDF_NDHIST_DEBUG") != nullptr);
    if (!enabled) {
      return;
    }
    std::cerr << "[THnMulti] " << reason
              << " slot=" << slot
              << " baseVals=" << baseHistogramValues.size()
              << " baseWts=" << baseHistogramWeights.size()
              << " syst=" << systematicVariation.size()
              << " sampleCat=" << sampleCategory.size()
              << " control=" << controlRegion.size()
              << " channel=" << channel.size()
              << " nFills=" << nFills.size()
              << std::endl;
  }

  /// Fill one bin in the sparse per-thread accumulator.
  void fillHistSparse_(unsigned int slot, Double_t ch, Double_t cr, Double_t sc,
                       Double_t sv, Double_t bv, Double_t w) {
    fPerThreadResults[slot]->Fill(ch, cr, sc, sv, bv, w);
  }

  /// Fill one bin in the dense per-thread accumulator (faster: O(1) direct array indexing).
  void fillHistDense_(unsigned int slot, Double_t ch, Double_t cr, Double_t sc,
                      Double_t sv, Double_t bv, Double_t w) {
    fPerThreadDense_m[slot]->Fill(ch, cr, sc, sv, bv, w);
  }

  void fillHistDenseIndexed_(unsigned int slot, Double_t ch, Double_t cr,
                            Double_t sc, Double_t sv, Double_t bv, Double_t w) {
    const Double_t coordinates[5] = {ch, cr, sc, sv, bv};
    Long64_t bin = denseAxes_m[4]->FindFixBin(bv);
    for (int d = 0; d < 4; ++d) {
      const auto value = coordinates[d];
      int index;
      if (value < xmin_m[d]) index = 0;
      else if (!(value < xmax_m[d])) index = nbins_m[d] + 1; // includes NaN
      else {
        const int integer = static_cast<int>(value); // bounded by construction
        index = value == integer ? integer + categoryOffsets_m[d]
                                 : denseAxes_m[d]->FindFixBin(value);
      }
      bin += denseStrides_m[d] * index;
    }
    auto& histogram = *fPerThreadDense_m[slot];
    histogram.FillIndexed(coordinates, bin, w); // ROOT accumulation and Sumw2
  }

  std::array<const TAxis*, 5> denseAxes_m{};
  std::array<Long64_t, 5> denseStrides_m{};
  std::array<int, 4> categoryOffsets_m{};

  /** @brief Shared pointer to the final merged THnSparseD result. */
  std::shared_ptr<THnSparseF> fFinalResult = std::make_shared<THnSparseF>();
  /** @brief Vector of per-thread THnSparseF histogram pointers (used when useDense_m == false). */
  std::vector<std::shared_ptr<THnSparseF>> fPerThreadResults;
  /** @brief Vector of per-thread THnF histogram pointers (used when useDense_m == true). */
  std::vector<std::shared_ptr<DenseAccumulator>> fPerThreadDense_m;
  /** @brief True when dense (THnF) per-thread accumulators are used instead of sparse. */
  bool useDense_m = false;
  /** @brief Number of threads/slots. */
  const unsigned int nSlots_m;
  /** @brief Number of dimensions. */
  const Int_t dim_m;
  /** @brief Number of bins per dimension. */
  const std::vector<Int_t> nbins_m;
  /** @brief Minimum value per dimension. */
  const std::vector<Double_t> xmin_m;
  /** @brief Maximum value per dimension. */
  const std::vector<Double_t> xmax_m;
  std::vector<std::vector<Double_t>> binEdges_m;

  const std::string name_m;
  const std::string title_m;
  
  const Bool_t channel_hasSystematic_m;
  const Bool_t controlRegion_hasSystematic_m;
  const Bool_t sampleCategory_hasSystematic_m;
  const Bool_t weight_hasSystematic_m;
  const Bool_t hasSystematic_m;

  const Bool_t channel_hasMultiFill_m;
  const Bool_t controlRegion_hasMultiFill_m;
  const Bool_t sampleCategory_hasMultiFill_m;
  const Bool_t systematic_hasMultiFill_m;
  const Bool_t weight_hasMultiFill_m;
  const Bool_t hasMultiFill_m;
  
  // Pointer to the per-bin fill helper (sparse or dense path), set once in constructor.
  using FillHistFuncType = void (THnMulti::*)(unsigned int, Double_t, Double_t, Double_t, Double_t, Double_t, Double_t);
  FillHistFuncType fillHistFunc_ = nullptr;

  // Pointer to the selected fill function for fast path dispatch
  FillFuncType fillFunc_ = nullptr;
};

/**
 * @class BHnMulti
 * @brief Multi-threaded N-dimensional histogram action using Boost.Histogram.
 *
 * This class provides an alternative to THnMulti, using Boost.Histogram as the
 * per-thread accumulator. The filling interface is identical to THnMulti.
 * In Finalize(), the per-thread Boost histograms are merged and converted to
 * a THnSparseF, preserving full compatibility with the rest of the framework.
 *
 * Storage is chosen automatically at construction time based on the estimated
 * total memory for dense per-thread histograms:
 * - **Dense** (`weight_storage`): pre-allocates all bins, O(1) fills.
 *   Selected when the dense estimate fits within the configured all-slot budget.
 * - **Sparse** (map-adapted weighted storage): hash-based, only filled bins consume
 *   memory.  Selected when the dense estimate exceeds the threshold.
 *   Uses Boost storage_adaptor with a weighted unordered_map.
 *
 * The final result is always a sparse THnSparseF.
 *
 * Select this backend by setting histogramBackend=boost in the configuration.
 */
class BHnMulti : public ROOT::Detail::RDF::RActionImpl<BHnMulti> {
public:
  /** @brief Result type: ROOT sparse histogram (same as THnMulti) */
  using Result_t = THnSparseF;

  // 5D Boost.Histogram type with dense weight_storage (O(1) fills, pre-allocates all bins)
  template <class Storage>
  using Histogram = decltype(boost::histogram::make_histogram_with(
      Storage{},
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0)));
  template <class Storage>
  using FloatHistogram = decltype(boost::histogram::make_histogram_with(
      Storage{}, rdfhistogram::RootAxis{}, rdfhistogram::RootAxis{},
      rdfhistogram::RootAxis{}, rdfhistogram::RootAxis{}, rdfhistogram::RootAxis{}));
  using BH5DDense = Histogram<boost::histogram::weight_storage>;
  using BH5DFloatDense = FloatHistogram<rdfhistogram::FloatWeightedStorage>;

  // storage_adaptor constructs the mapped value from weight_type on insertion.
  struct SparseWeight : boost::histogram::accumulators::weighted_sum<> {
    using Base = boost::histogram::accumulators::weighted_sum<>;
    using Base::Base;
    SparseWeight() = default;
    template <class T>
    SparseWeight(const boost::histogram::weight_type<T> &weight)
        : Base(weight.value, weight.value * weight.value) {}
  };
  template <class Weight>
  using SparseAllocator = rdfhistogram::CountingAllocator<std::pair<const std::size_t, Weight>>;
  using SparseMap = std::unordered_map<std::size_t, SparseWeight,
      std::hash<std::size_t>, std::equal_to<std::size_t>, SparseAllocator<SparseWeight>>;
  using FloatSparseMap = std::unordered_map<std::size_t, rdfhistogram::FloatWeightedSum,
      std::hash<std::size_t>, std::equal_to<std::size_t>, SparseAllocator<rdfhistogram::FloatWeightedSum>>;
  using BH5DSparse = Histogram<SparseMap>;
  using BH5DFloatSparse = FloatHistogram<FloatSparseMap>;
#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
  using FlatMap = boost::unordered_flat_map<std::size_t, SparseWeight,
      std::hash<std::size_t>, std::equal_to<std::size_t>, SparseAllocator<SparseWeight>>;
  using FloatFlatMap = boost::unordered_flat_map<std::size_t, rdfhistogram::FloatWeightedSum,
      std::hash<std::size_t>, std::equal_to<std::size_t>, SparseAllocator<rdfhistogram::FloatWeightedSum>>;
  using BH5DFlat = Histogram<FlatMap>;
  using BH5DFloatFlat = FloatHistogram<FloatFlatMap>;
  using BHStorage = std::variant<std::vector<BH5DDense>, std::vector<BH5DFloatDense>,
      std::vector<BH5DSparse>, std::vector<BH5DFloatSparse>,
      std::vector<BH5DFlat>, std::vector<BH5DFloatFlat>>;
#else
  using BHStorage = std::variant<std::vector<BH5DDense>, std::vector<BH5DFloatDense>,
      std::vector<BH5DSparse>, std::vector<BH5DFloatSparse>>;
#endif

  // Define a function pointer type for fill functions (same signature as THnMulti)
  using FillFuncType = void (BHnMulti::*)(unsigned int,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Bool_t>&,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Int_t>&);

  /**
   * @brief Construct a new BHnMulti object
   * @param fillInfo Histogram fill information (same struct as used by THnMulti)
   */
  BHnMulti(histFillInfo &fillInfo)
    : nSlots_m(fillInfo.nSlots), dim_m(fillInfo.dim), nbins_m(fillInfo.nbins), xmin_m(fillInfo.xmin), xmax_m(fillInfo.xmax),
      channel_hasSystematic_m(fillInfo.channel_hasSystematic), controlRegion_hasSystematic_m(fillInfo.controlRegion_hasSystematic),
      sampleCategory_hasSystematic_m(fillInfo.sampleCategory_hasSystematic), weight_hasSystematic_m(fillInfo.weight_hasSystematic),
      hasSystematic_m(fillInfo.hasSystematic), channel_hasMultiFill_m(fillInfo.channel_hasMultiFill),
      controlRegion_hasMultiFill_m(fillInfo.controlRegion_hasMultiFill), sampleCategory_hasMultiFill_m(fillInfo.sampleCategory_hasMultiFill),
      systematic_hasMultiFill_m(fillInfo.systematic_hasMultiFill), weight_hasMultiFill_m(fillInfo.weight_hasMultiFill),
      hasMultiFill_m(fillInfo.hasMultiFill), name_m(fillInfo.name), title_m(fillInfo.title) {

    namespace bh = boost::histogram;

    floatContents_m = fillInfo.boostFloatContents;
    denseBytesPerCell_m = floatContents_m ? 12 : 16;
    const bool useSparse = fillInfo.forceSparse ||
        estimateDenseMemoryBytes(nbins_m, nSlots_m, denseBytesPerCell_m) > fillInfo.denseMemoryLimitBytes;
    memoryUsage_m->denseBytes = estimateDenseMemoryBytes(nbins_m, nSlots_m, denseBytesPerCell_m);
    memoryUsage_m->slots = nSlots_m;
    memoryUsage_m->sparse = useSparse;
    auto book = [&](auto storage) {
      using Storage = decltype(storage);
      constexpr bool isFloat = std::is_same_v<Storage, rdfhistogram::FloatWeightedStorage> ||
          std::is_same_v<Storage, FloatSparseMap>
#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
          || std::is_same_v<Storage, FloatFlatMap>
#endif
          ;
      using H = std::conditional_t<isFloat, FloatHistogram<Storage>, Histogram<Storage>>;
      auto &hists = fPerThreadHists.emplace<std::vector<H>>();
      hists.reserve(nSlots_m);
      auto axis = [&](int dimension) {
        if constexpr (isFloat)
          return rdfhistogram::RootAxis(nbins_m[dimension], xmin_m[dimension], xmax_m[dimension],
              fillInfo.binEdges.size() > static_cast<std::size_t>(dimension)
                  ? fillInfo.binEdges[dimension] : std::vector<double>{});
        else return bh::axis::regular<>(nbins_m[dimension], xmin_m[dimension], xmax_m[dimension]);
      };
      for (unsigned slot = 0; slot < nSlots_m; ++slot)
        hists.push_back(bh::make_histogram_with(Storage{}, axis(0), axis(1), axis(2), axis(3), axis(4)));
      // Vector move transfers its element buffer, so this address survives
      // moving the RDF action into Book(). Dispatch is once per Exec, not fill.
      perThreadHistograms_m = hists.data();
      selectFillFunction<H>();
    };
    if (!useSparse) {
      storageName_m = "dense";
      if (floatContents_m) book(rdfhistogram::FloatWeightedStorage{});
      else book(bh::weight_storage{});
    } else if (fillInfo.boostSparseBackend == "flat") {
#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
      storageName_m = "sparse_flat";
      if (floatContents_m) book(FloatFlatMap{});
      else book(FlatMap{});
#else
      throw std::runtime_error("Boost flat histogram storage requires boost/unordered/unordered_flat_map.hpp");
#endif
    } else if (fillInfo.boostSparseBackend == "map") {
      storageName_m = "sparse_map";
      if (floatContents_m) book(FloatSparseMap{});
      else book(SparseMap{});
    } else throw std::invalid_argument("Unknown Boost sparse histogram backend");
    memoryUsage_m->storage = storageName_m;
    fFinalResult = std::make_shared<THnSparseF>(
        name_m.c_str(), title_m.c_str(), dim_m,
        nbins_m.data(), xmin_m.data(), xmax_m.data());
    for (std::size_t axis = 0; axis < fillInfo.binEdges.size(); ++axis)
      if (!fillInfo.binEdges[axis].empty())
        fFinalResult->SetBinEdges(axis, fillInfo.binEdges[axis].data());
    fFinalResult->Sumw2();

  }

  /** @brief Move constructor */
  BHnMulti(BHnMulti &&) = default;

  std::shared_ptr<const rdfhistogram::StorageMemoryUsage> GetStorageMemoryUsage() const {
    return memoryUsage_m;
  }

  /** @brief Get result pointer */
  std::shared_ptr<Result_t> GetResultPtr() { return fFinalResult; }
  std::shared_ptr<Result_t> GetResultPtr() const { return fFinalResult; }

  void Initialize() {}
  void InitTask(TTreeReader *, int) {}

  /**
   * @brief Fill the per-thread Boost histogram for one entry.
   *
   * selectionMask uses the same systematic-slot ordering as systematicVariation.
   */
  void Exec(unsigned int slot,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
            const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
            const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {
    if (selectionMask.size() != systematicVariation.size()) {
      throw std::invalid_argument(
          "BHnMulti: selection mask length must match systematic variation length");
    }
    if (baseHistogramValues.empty() || baseHistogramWeights.empty() || systematicVariation.empty() ||
        sampleCategory.empty() || controlRegion.empty() || channel.empty() || nFills.empty()) {
      return;
    }
    (this->*fillFunc_)(slot, baseHistogramValues, baseHistogramWeights,
                       systematicVariation, selectionMask, sampleCategory,
                       controlRegion, channel, nFills);
  }

  template <class H>
  void SingleNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

    if (baseHistogramWeights.size() < 1 || selectionMask.empty() || channel.size() < 1 || controlRegion.size() < 1 ||
        sampleCategory.size() < 1 || systematicVariation.size() < 1) {
      return;
    }
    const double weight = static_cast<double>(baseHistogramWeights[0]);
    if (weight != 0.0 && selectionMask[0]) {
      fillHist_<H>(slot, weight,
          static_cast<double>(channel[0]),
          static_cast<double>(controlRegion[0]),
          static_cast<double>(sampleCategory[0]),
          static_cast<double>(systematicVariation[0]),
          static_cast<double>(baseHistogramValues[0]));
    }
  }

  template <class H>
  void SingleSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

    int weightCounter = 0, baseFillCounter = 0;
    int controlRegionCounter = 0, channelCounter = 0, sampleCategoryCounter = 0;

    while (baseFillCounter < static_cast<int>(baseHistogramValues.size())) {
      if (weightCounter >= static_cast<int>(baseHistogramWeights.size()) ||
          controlRegionCounter >= static_cast<int>(controlRegion.size()) ||
          channelCounter >= static_cast<int>(channel.size()) ||
          sampleCategoryCounter >= static_cast<int>(sampleCategory.size()) ||
          baseFillCounter >= static_cast<int>(systematicVariation.size()) ||
          baseFillCounter >= static_cast<int>(selectionMask.size())) {
        break;
      }

      const double weight = static_cast<double>(baseHistogramWeights[weightCounter]);
      if (weight != 0.0 && selectionMask[baseFillCounter]) {
        fillHist_<H>(slot, weight,
            static_cast<double>(channel[channelCounter]),
            static_cast<double>(controlRegion[controlRegionCounter]),
            static_cast<double>(sampleCategory[sampleCategoryCounter]),
            static_cast<double>(systematicVariation[baseFillCounter]),
            static_cast<double>(baseHistogramValues[baseFillCounter]));
      }
      controlRegionCounter += controlRegion_hasSystematic_m;
      sampleCategoryCounter += sampleCategory_hasSystematic_m;
      channelCounter += channel_hasSystematic_m;
      weightCounter += weight_hasSystematic_m;
      baseFillCounter++;
    }
  }

  template <class H>
  void MultiNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

    int weightCounter = 0, baseFillCounter = 0, systematicCounter = 0;
    int controlRegionCounter = 0, channelCounter = 0, sampleCategoryCounter = 0;

    while (baseFillCounter < static_cast<int>(baseHistogramValues.size())) {
      if (weightCounter >= static_cast<int>(baseHistogramWeights.size()) ||
          controlRegionCounter >= static_cast<int>(controlRegion.size()) ||
          channelCounter >= static_cast<int>(channel.size()) ||
          sampleCategoryCounter >= static_cast<int>(sampleCategory.size()) ||
          systematicCounter >= static_cast<int>(systematicVariation.size()) ||
          systematicCounter >= static_cast<int>(selectionMask.size())) {
        break;
      }
      const double weight = static_cast<double>(baseHistogramWeights[weightCounter]);
      if (weight != 0.0 && selectionMask[systematicCounter]) {
        fillHist_<H>(slot, weight,
            static_cast<double>(channel[channelCounter]),
            static_cast<double>(controlRegion[controlRegionCounter]),
            static_cast<double>(sampleCategory[sampleCategoryCounter]),
            static_cast<double>(systematicVariation[systematicCounter]),
            static_cast<double>(baseHistogramValues[baseFillCounter]));
      }
      systematicCounter += systematic_hasMultiFill_m;
      controlRegionCounter += controlRegion_hasMultiFill_m;
      sampleCategoryCounter += sampleCategory_hasMultiFill_m;
      channelCounter += channel_hasMultiFill_m;
      weightCounter += weight_hasMultiFill_m;
      baseFillCounter++;
    }
  }

  template <class H>
  void GeneralFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Bool_t> &__restrict__ selectionMask,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

    int weightCounter = 0, baseFillCounter = 0, systematicCounter = 0;
    int controlRegionCounter = 0, channelCounter = 0, sampleCategoryCounter = 0;
    int fillCounter = 0;

    while (baseFillCounter < static_cast<int>(baseHistogramValues.size())) {
      if (weightCounter >= static_cast<int>(baseHistogramWeights.size()) ||
          controlRegionCounter >= static_cast<int>(controlRegion.size()) ||
          channelCounter >= static_cast<int>(channel.size()) ||
          sampleCategoryCounter >= static_cast<int>(sampleCategory.size()) ||
          systematicCounter >= static_cast<int>(systematicVariation.size()) ||
          systematicCounter >= static_cast<int>(selectionMask.size()) ||
          systematicCounter >= static_cast<int>(nFills.size())) {
        break;
      }
      const double weight = static_cast<double>(baseHistogramWeights[weightCounter]);
      if (weight != 0.0 && selectionMask[systematicCounter]) {
        fillHist_<H>(slot, weight,
            static_cast<double>(channel[channelCounter]),
            static_cast<double>(controlRegion[controlRegionCounter]),
            static_cast<double>(sampleCategory[sampleCategoryCounter]),
            static_cast<double>(systematicVariation[systematicCounter]),
            static_cast<double>(baseHistogramValues[baseFillCounter]));
      }
      baseFillCounter++;
      fillCounter++;
      if (channel_hasMultiFill_m) channelCounter++;
      if (controlRegion_hasMultiFill_m) controlRegionCounter++;
      if (sampleCategory_hasMultiFill_m) sampleCategoryCounter++;
      if (systematic_hasMultiFill_m) systematicCounter++;
      if (weight_hasMultiFill_m) weightCounter++;
      if (systematicCounter >= static_cast<int>(nFills.size())) break;
      if (fillCounter >= nFills[systematicCounter]) {
        fillCounter = 0;
        if (!channel_hasSystematic_m) { channelCounter = 0; }
        else if (!channel_hasMultiFill_m) { channelCounter += 1; }
        if (!controlRegion_hasSystematic_m) { controlRegionCounter = 0; }
        else if (!controlRegion_hasMultiFill_m) { controlRegionCounter += 1; }
        if (!sampleCategory_hasSystematic_m) { sampleCategoryCounter = 0; }
        else if (!sampleCategory_hasMultiFill_m) { sampleCategoryCounter += 1; }
        if (!weight_hasSystematic_m) { weightCounter = 0; }
        else if (!weight_hasMultiFill_m) { weightCounter += 1; }
        if (!systematic_hasMultiFill_m) { systematicCounter += 1; }
      }
    }
  }

  /**
   * @brief Merge per-thread Boost histograms and convert to THnSparseF.
   *
   * Dispatches via std::visit to handle dense arrays and sparse weighted maps.
   * Sparse merging visits allocated entries. Cells with non-zero content or
   * variance, including flow bins, are written to the result.
   */
  void Finalize() {
    namespace bh = boost::histogram;
    HistogramFinalizeProfile profile(floatContents_m ? "boost_float" : "boost", storageName_m,
        name_m, estimateDenseMemoryBytes(nbins_m, nSlots_m, denseBytesPerCell_m), fFinalResult.get());
    auto writeBin = [&](const Int_t *indices, double content, double variance) {
      if (content == 0.0 && variance == 0.0) return;
      const auto bin = fFinalResult->GetBin(indices, true);
      fFinalResult->SetBinContent(bin, content);
      fFinalResult->SetBinError2(bin, variance);
    };
    std::visit([&](auto &hists) {
      if (hists.empty()) return;
      using H = typename std::decay_t<decltype(hists)>::value_type;
      if constexpr (!std::is_same_v<H, BH5DDense> && !std::is_same_v<H, BH5DFloatDense>) {
        // Library-level access is needed here: indexed() and operator+= walk
        // the whole logical grid even for map storage. Only visit allocations.
        auto map = [](auto &histogram) -> auto& {
          auto &storage = bh::unsafe_access::storage(histogram);
          if constexpr (std::is_same_v<H, BH5DSparse>) return static_cast<SparseMap&>(storage);
          else if constexpr (std::is_same_v<H, BH5DFloatSparse>) return static_cast<FloatSparseMap&>(storage);
#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
          else if constexpr (std::is_same_v<H, BH5DFlat>) return static_cast<FlatMap&>(storage);
          else return static_cast<FloatFlatMap&>(storage);
#endif
        };
        auto recordMemory = [&] {
          std::size_t bytes = 0;
          for (auto &histogram : hists)
            bytes += map(histogram).get_allocator().bytes->current;
          memoryUsage_m->sparseBytes = std::max(memoryUsage_m->sparseBytes, bytes);
        };
        // Read counters only after event processing has joined all RDF slots.
        // Also observe the merged container before export; clear() can retain
        // bucket capacity, so occupied-cell counts alone would undercount it.
        recordMemory();
        auto &merged = map(hists[0]);
        for (std::size_t slot = 1; slot < hists.size(); ++slot) {
          for (const auto &entry : map(hists[slot])) merged[entry.first] += entry.second;
          recordMemory();
          map(hists[slot]).clear();
        }
        for (const auto &entry : merged) {
          auto linear = entry.first;
          std::array<Int_t, 5> indices;
          // Boost stores the first axis fastest; ROOT bin indices include flows.
          for (int axis = 0; axis < 5; ++axis) {
            indices[axis] = linear % (nbins_m[axis] + 2);
            linear /= nbins_m[axis] + 2;
          }
          writeBin(indices.data(), entry.second.value(), entry.second.variance());
        }
      } else {
        auto &merged = hists[0];
        for (std::size_t slot = 1; slot < hists.size(); ++slot) merged += hists[slot];
        for (auto &&cell : bh::indexed(merged, bh::coverage::all)) {
          const auto &weight = *cell;
          if (weight.value() == 0.0 && weight.variance() == 0.0) continue;
          std::array<Int_t, 5> indices;
          for (int axis = 0; axis < 5; ++axis) indices[axis] = cell.index(axis) + 1;
          writeBin(indices.data(), weight.value(), weight.variance());
        }
      }
      hists.clear();
    }, fPerThreadHists);
  }

  std::string GetActionName() const { return "BHnMulti"; }

private:
  template <class H>
  void selectFillFunction() {
    if (!hasSystematic_m && !channel_hasSystematic_m && !controlRegion_hasSystematic_m && !sampleCategory_hasSystematic_m && !weight_hasSystematic_m &&
        !hasMultiFill_m && !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m && !systematic_hasMultiFill_m) {
      fillFunc_ = &BHnMulti::SingleNoSystematicFill<H>;
    } else if (hasSystematic_m && !hasMultiFill_m && !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m && !systematic_hasMultiFill_m) {
      fillFunc_ = &BHnMulti::SingleSystematicFill<H>;
    } else if (!hasSystematic_m && (hasMultiFill_m || channel_hasMultiFill_m || controlRegion_hasMultiFill_m || sampleCategory_hasMultiFill_m || weight_hasMultiFill_m || systematic_hasMultiFill_m)) {
      fillFunc_ = &BHnMulti::MultiNoSystematicFill<H>;
    } else {
      fillFunc_ = &BHnMulti::GeneralFill<H>;
    }
  }
  template <class H>
  void fillHist_(unsigned slot, double weight, double ch, double cr,
                 double sc, double sv, double bv) {
    // Native regular Boost axes need the counter at the bin centre. ROOT
    // axes accept its original integer coordinate and preserve exact edges.
    constexpr bool rootAxes = std::is_same_v<typename H::value_type,
                                            rdfhistogram::FloatWeightedSum>;
    static_cast<H*>(perThreadHistograms_m)[slot](boost::histogram::weight(weight),
        ch, cr, sc, sv + (rootAxes ? 0.0 : 0.5), bv);
  }
  void *perThreadHistograms_m = nullptr;
  const char *storageName_m = "dense";
  bool floatContents_m = false;
  std::size_t denseBytesPerCell_m = 16;
  std::shared_ptr<rdfhistogram::StorageMemoryUsage> memoryUsage_m =
      std::make_shared<rdfhistogram::StorageMemoryUsage>();

  std::shared_ptr<THnSparseF> fFinalResult = std::make_shared<THnSparseF>();
  /// Per-thread Boost histograms.
  /// Dense weight_storage or sparse weighted map, selected from the
  /// configured all-slot memory limit. Output is always THnSparseF.
  BHStorage fPerThreadHists;

  const unsigned int nSlots_m;
  const Int_t dim_m;
  const std::vector<Int_t> nbins_m;
  const std::vector<Double_t> xmin_m;
  const std::vector<Double_t> xmax_m;
  const std::string name_m;
  const std::string title_m;

  const Bool_t channel_hasSystematic_m;
  const Bool_t controlRegion_hasSystematic_m;
  const Bool_t sampleCategory_hasSystematic_m;
  const Bool_t weight_hasSystematic_m;
  const Bool_t hasSystematic_m;

  const Bool_t channel_hasMultiFill_m;
  const Bool_t controlRegion_hasMultiFill_m;
  const Bool_t sampleCategory_hasMultiFill_m;
  const Bool_t systematic_hasMultiFill_m;
  const Bool_t weight_hasMultiFill_m;
  const Bool_t hasMultiFill_m;

  FillFuncType fillFunc_ = nullptr;
};

/**
 * @class histInfo
 * @brief Stores metadata for a histogram, including name, variable, label,
 * weight, binning, and bounds.
 *
 * This class encapsulates the information needed to define a histogram, such as
 * its name, the variable it represents, its label, weight expression, number of
 * bins, and the lower and upper bounds of the histogram.
 */
class histInfo {
public:
  /**
   * @brief Construct a new histInfo object.
   * @param name Name of the histogram.
   * @param variable Variable to be histogrammed.
   * @param label Label for the histogram axis.
   * @param weight Weight expression for the histogram.
   * @param bins Number of bins.
   * @param lowerBound Lower bound of the histogram.
   * @param upperBound Upper bound of the histogram.
   */
  histInfo(const char name[], const char variable[], const char label[],
           const char weight[], int bins, float lowerBound, float upperBound,
           std::vector<Double_t> binEdges = {}, bool forceSparse = false)
      : name_m(name), variable_m(variable), label_m(label), weight_m(weight),
        bins_m(bins), lowerBound_m(lowerBound), upperBound_m(upperBound),
        binEdges_m(std::move(binEdges)), forceSparse_m(forceSparse) {}

  /**
   * @brief Get the name of the histogram.
   * @return Reference to the histogram name string.
   */
  constexpr const std::string &name() const { return (name_m); }

  /**
   * @brief Get the weight expression for the histogram.
   * @return Reference to the weight string.
   */
  constexpr const std::string &weight() const { return (weight_m); }

  /**
   * @brief Get the variable name for the histogram.
   * @return Reference to the variable string.
   */
  constexpr const std::string &variable() const { return (variable_m); }

  /**
   * @brief Get the label for the histogram axis.
   * @return Reference to the label string.
   */
  constexpr const std::string &label() const { return (label_m); }

  /**
   * @brief Get the number of bins for the histogram.
   * @return Reference to the number of bins.
   */
  constexpr const int &bins() const { return (bins_m); }

  /**
   * @brief Get the lower bound of the histogram.
   * @return Reference to the lower bound value.
   */
  constexpr const float &lowerBound() const { return (lowerBound_m); }

  /**
   * @brief Get the upper bound of the histogram.
   * @return Reference to the upper bound value.
   */
  constexpr const float &upperBound() const { return (upperBound_m); }

  /** @brief Get optional explicit edges for a variable-width axis. */
  constexpr const std::vector<Double_t> &binEdges() const {
    return (binEdges_m);
  }

  /// Force sparse ROOT accumulation for a histogram with low expected occupancy.
  constexpr bool forceSparse() const { return forceSparse_m; }

private:
  /** @brief Name of the histogram. */
  const std::string name_m;
  /** @brief Variable to be histogrammed. */
  const std::string variable_m;
  /** @brief Label for the histogram axis. */
  const std::string label_m;
  /** @brief Weight expression for the histogram. */
  const std::string weight_m;
  /** @brief Number of bins. */
  const int bins_m;
  /** @brief Lower bound of the histogram. */
  const float lowerBound_m;
  /** @brief Upper bound of the histogram. */
  const float upperBound_m;
  /** @brief Optional variable-width edges for the histogram axis. */
  const std::vector<Double_t> binEdges_m;
  const bool forceSparse_m;
};

/**
 * @class selectionInfo
 * @brief Holds selection information for applying cuts to histograms.
 *
 * This class is used to define a set of selection criteria (cuts) that can be
 * applied to a predefined set of histograms. It stores the variable, binning,
 * and bounds for the selection.
 */
class selectionInfo {
public:

  selectionInfo() : variable_m("zero__"), bins_m(1), lowerBound_m(0), upperBound_m(1), regions_m({"Default"}) {}


  /**
   * @brief Construct a new selectionInfo object.
   * @param variable Variable to apply the selection on.
   * @param bins Number of bins for the selection.
   * @param lowerBound Lower bound for the selection.
   * @param upperBound Upper bound for the selection.
   */
  selectionInfo(std::string variable, int bins, double lowerBound,
                double upperBound, std::vector<std::string> regions)
      : variable_m(variable), bins_m(bins), lowerBound_m(lowerBound),
        upperBound_m(upperBound), regions_m(regions) {}

  /**
   * @brief Construct a new selectionInfo object with default regions.
   * @param variable Variable to apply the selection on.
   * @param bins Number of bins for the selection.
   * @param lowerBound Lower bound for the selection.
   * @param upperBound Upper bound for the selection.
   */
  selectionInfo(std::string variable, int bins, double lowerBound,
                double upperBound)
      : selectionInfo(std::move(variable), bins, lowerBound, upperBound, {"Default"}) {}

  /**
   * @brief Get the variable name for the selection.
   * @return Reference to the variable string.
   */
  constexpr const std::string &variable() const { return (variable_m); }

  /**
   * @brief Get the number of bins for the selection.
   * @return Reference to the number of bins.
   */
  constexpr const int &bins() const { return (bins_m); }

  /**
   * @brief Get the lower bound for the selection.
   * @return Reference to the lower bound value.
   */
  constexpr const float &lowerBound() const { return (lowerBound_m); }

  /**
   * @brief Get the upper bound for the selection.
   * @return Reference to the upper bound value.
   */
  constexpr const float &upperBound() const { return (upperBound_m); }

  /**
   * @brief Get the regions for the selection.
   * @return Reference to the regions vector.
   */
  constexpr const std::vector<std::string> &regions() const { return (regions_m); }

private:
  /** @brief Variable to apply the selection on. */
  const std::string variable_m;
  /** @brief Number of bins for the selection. */
  const int bins_m;
  /** @brief Lower bound for the selection. */
  const float lowerBound_m;
  /** @brief Upper bound for the selection. */
  const float upperBound_m;
  /** @brief Regions for the selection. */
  const std::vector<std::string> regions_m;
};

/**
 * @class histHolder
 * @brief Holds and manages multiple histograms, providing booking and saving
 * utilities.
 *
 * This class manages a collection of histograms, allowing for booking
 * N-dimensional histograms and saving them to file.
 */
class histHolder {
public:
  /**
   * @brief Default constructor for histHolder.
   */
  histHolder() {}

  /**
   * @brief Book N-dimensional histograms based on provided info and selection.
   * @tparam types Types of the variables to be histogrammed.
   * @param infos Vector of histogram info objects.
   * @param selection Vector of selection info objects.
   * @param df ROOT RDataFrame node to use for histogramming.
   * @param suffix Suffix to append to histogram names.
   */
  template <typename... types>
  void bookND(std::vector<histInfo> &infos,
              std::vector<selectionInfo> &selection, ROOT::RDF::RNode df,
              std::string suffix) {

    // Store the selection info in some vectors
    std::vector<int> binVectorBase;
    std::vector<double> lowerBoundVectorBase;
    std::vector<double> upperBoundVectorBase;
    std::vector<std::string> varVectorBase;

    for (auto const &selectionInfo : selection) {
      binVectorBase.push_back(selectionInfo.bins());
      lowerBoundVectorBase.push_back(selectionInfo.lowerBound());
      upperBoundVectorBase.push_back(selectionInfo.upperBound());
      varVectorBase.push_back(selectionInfo.variable());
    }

    for (auto const &info : infos) {
      std::string newName = info.name() + "." + suffix; // name of hist info
      std::vector<int> binVector(binVectorBase); // vectors from selectionInfos
      std::vector<double> lowerBoundVector(lowerBoundVectorBase);
      std::vector<double> upperBoundVector(upperBoundVectorBase);
      std::vector<std::string> varVector(varVectorBase);
      // add selection info from the histInfos object
      binVector.push_back(info.bins());
      lowerBoundVector.push_back(info.lowerBound());
      upperBoundVector.push_back(info.upperBound());
      varVector.push_back(info.variable());
      varVector.push_back(info.weight());

      // Book the THnSparseD histo
      const ROOT::RDF::THnDModel tempModel(
          newName.c_str(), newName.c_str(), selection.size() + 1,
          binVector.data(), lowerBoundVector.data(), upperBoundVector.data());

      // histos_m.push_back(tempModel.GetResultPtr());
      histos_m.emplace(histos_m.end(),
                       df.HistoND<types..., float>(tempModel, varVector));
    }
  }

  /**
   * @brief Save all booked histograms to file.
   *
   * Iterates over all histograms and writes them to the output file.
   * This triggers execution of the RDataFrame.
   */
  void save() {
    for (auto &histo_m : histos_m) {
      auto ptr = histo_m.GetPtr();
      ptr->Write();
      // histo_m->Write();
    }
  }

private:
  /** @brief Vector of histogram result pointers. */
  std::vector<ROOT::RDF::RResultPtr<THnSparseD>> histos_m;
};

#endif
