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
#include <memory>
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
  std::string name = "";
  std::string title = "";
  Int_t dim = 5;
  Int_t nSlots = 0;
  std::vector<Int_t> nbins = {};
  std::vector<Double_t> xmin = {};
  std::vector<Double_t> xmax = {};
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

/**
 * @class THnMulti
 * @brief Multi-threaded N-dimensional histogram action for ROOT RDataFrame.
 *
 * This class manages a set of THnSparseF histograms, one per thread, and merges
 * them at the end of processing. It is used as a custom action in ROOT's
 * RDataFrame for efficient multi-threaded histogramming.
 */
class THnMulti : public ROOT::Detail::RDF::RActionImpl<THnMulti> {

public:
  /**
   * @brief Type alias for the result type (THnSparseF)
   */
  using Result_t = THnSparseF;

  // Define a function pointer type for fill functions
  using FillFuncType = void (THnMulti::*)(unsigned int,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
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

    // Auto-select dense (THnF) vs sparse (THnSparseF) per-thread accumulators.
    // THnF uses direct array indexing (O(1)) which is faster than THnSparseF's
    // hash-based lookup, but consumes memory proportional to all bins.
    // Float_t content (4 B) + Double_t sumw2 (8 B) = 12 bytes per bin (with Sumw2).
    useDense_m = estimateDenseMemoryBytes(nbins_m, nSlots_m, 12) <= kDenseMemoryThresholdBytes;

    for (unsigned int i = 0; i < nSlots_m; i++) {
      if (useDense_m) {
        fPerThreadDense_m.push_back(std::make_shared<THnF>(
            (name_m + "_" + std::to_string(i)).c_str(), title_m.c_str(), dim_m,
            nbins_m.data(), xmin_m.data(), xmax_m.data()));
        fPerThreadDense_m[i]->Sumw2();
      } else {
        fPerThreadResults.push_back(std::make_shared<THnSparseF>(
            (name_m + "_" + std::to_string(i)).c_str(), title_m.c_str(), dim_m,
            nbins_m.data(), xmin_m.data(), xmax_m.data()));
        fPerThreadResults[i]->Sumw2();
      }
    }
    fFinalResult =
        std::make_shared<Result_t>((name_m).c_str(), title_m.c_str(), dim_m,
                                   nbins_m.data(), xmin_m.data(), xmax_m.data());

    // Select the per-bin fill helper: dense uses THnF (direct array), sparse uses THnSparseF (hash).
    fillHistFunc_ = useDense_m ? &THnMulti::fillHistDense_ : &THnMulti::fillHistSparse_;

    // Set the fill function pointer based on configuration
    // (This logic can be expanded for more fast paths as needed)
    if (!channel_hasSystematic_m && !controlRegion_hasSystematic_m && !sampleCategory_hasSystematic_m && !weight_hasSystematic_m && !systematic_hasMultiFill_m &&
        !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m) {
      // Fast path: single fill, no systematic
      fillFunc_ = &THnMulti::SingleNoSystematicFill;
    } else if (systematic_hasMultiFill_m && !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m) {
      // Fast path: single systematic fill
      fillFunc_ = &THnMulti::SingleSystematicFill;
    } else if (!systematic_hasMultiFill_m && (channel_hasMultiFill_m || controlRegion_hasMultiFill_m || sampleCategory_hasMultiFill_m || weight_hasMultiFill_m)) {
      // Fast path: multi fill, no systematic
      fillFunc_ = &THnMulti::MultiNoSystematicFill;
    } else {
      // General case
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
   */
  void Exec(unsigned int slot,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation, const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory, 
            const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion, const ROOT::VecOps::RVec<Float_t> &__restrict__ channel, 
            const ROOT::VecOps::RVec<Int_t> &__restrict__ nFills) {
    if (baseHistogramValues.empty() || baseHistogramWeights.empty() || systematicVariation.empty() ||
        sampleCategory.empty() || controlRegion.empty() || channel.empty() || nFills.empty()) {
      LogSizes("empty input vector", slot, baseHistogramValues, baseHistogramWeights,
               systematicVariation, sampleCategory, controlRegion, channel, nFills);
      return;
    }
    // Dispatch to the selected fill function for optimal performance
    (this->*fillFunc_)(slot, baseHistogramValues, baseHistogramWeights,
                      systematicVariation, sampleCategory, controlRegion, channel, nFills);
  }

  void SingleNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation, // unused
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory, 
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion, const ROOT::VecOps::RVec<Float_t> &__restrict__ channel, 
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

      if (baseHistogramWeights.size() < 1 || channel.size() < 1 || controlRegion.size() < 1 ||
          sampleCategory.size() < 1 || systematicVariation.size() < 1) {
        LogSizes("single fill size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                 systematicVariation, sampleCategory, controlRegion, channel, nFills);
        return;
      }

      const Double_t weight = baseHistogramWeights[0];

      // Skip zero-weight entries for efficiency
      if (weight != 0.0) { // Skip zero-weight contributions.
        (this->*fillHistFunc_)(slot, channel[0], controlRegion[0], sampleCategory[0], systematicVariation[0], baseHistogramValues[0], weight);
      }
    
  }


  void SingleSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues, const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
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
            baseFillCounter >= static_cast<int>(systematicVariation.size())) {
          LogSizes("single systematic size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                   systematicVariation, sampleCategory, controlRegion, channel, nFills);
          break;
        }
        const Double_t weight = baseHistogramWeights[weightCounter];
  
        // Skip zero-weight entries for efficiency
        if (weight != 0.0) { // Skip zero-weight contributions.
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
            systematicCounter >= static_cast<int>(systematicVariation.size())) {
          LogSizes("multi fill size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                   systematicVariation, sampleCategory, controlRegion, channel, nFills);
          break;
        }
        const Double_t weight = baseHistogramWeights[weightCounter];
  
        // Skip zero-weight entries for efficiency
        if (weight != 0.0) { // Skip zero-weight contributions.
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
          systematicCounter >= static_cast<int>(nFills.size())) {
        LogSizes("general fill size mismatch", slot, baseHistogramValues, baseHistogramWeights,
                 systematicVariation, sampleCategory, controlRegion, channel, nFills);
        break;
      }
      const Double_t weight = baseHistogramWeights[weightCounter];

      // Skip zero-weight entries for efficiency
      if (weight != 0.0) { // Skip zero-weight contributions.
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
    if (useDense_m) {
      // Sum all per-thread dense histograms into the first one
      for (size_t slot = 1; slot < fPerThreadDense_m.size(); slot++) {
        fPerThreadDense_m[0]->Add(fPerThreadDense_m[slot].get());
      }
      // Convert dense result to sparse: iterate all inner bins and copy non-zero ones
      const Long64_t totalBins = fPerThreadDense_m[0]->GetNbins();
      std::vector<Int_t> idxBuf(dim_m);
      std::vector<Double_t> coordBuf(dim_m);
      for (Long64_t bin = 0; bin < totalBins; ++bin) {
        const Double_t content = fPerThreadDense_m[0]->GetBinContent(bin, idxBuf.data());
        if (content == 0.0) continue;
        // Skip overflow/underflow bins
        bool isOverflow = false;
        for (int d = 0; d < dim_m; ++d) {
          if (idxBuf[d] == 0 || idxBuf[d] > fPerThreadDense_m[0]->GetAxis(d)->GetNbins()) {
            isOverflow = true;
            break;
          }
        }
        if (isOverflow) continue;
        for (int d = 0; d < dim_m; ++d) {
          coordBuf[d] = fPerThreadDense_m[0]->GetAxis(d)->GetBinCenter(idxBuf[d]);
        }
        Long64_t sparseBin = fFinalResult->GetBin(coordBuf.data(), true);
        fFinalResult->SetBinContent(sparseBin, content);
        fFinalResult->SetBinError(sparseBin, fPerThreadDense_m[0]->GetBinError(bin));
      }
    } else {
      for (auto hist : fPerThreadResults) {
        fFinalResult->Add(hist.get());
      }
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

  /** @brief Shared pointer to the final merged THnSparseD result. */
  std::shared_ptr<THnSparseF> fFinalResult = std::make_shared<THnSparseF>();
  /** @brief Vector of per-thread THnSparseF histogram pointers (used when useDense_m == false). */
  std::vector<std::shared_ptr<THnSparseF>> fPerThreadResults;
  /** @brief Vector of per-thread THnF histogram pointers (used when useDense_m == true). */
  std::vector<std::shared_ptr<THnF>> fPerThreadDense_m;
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
 * Storage used for per-thread histograms:
 * - On Boost >= 1.76: `sparse_storage<weighted_sum<>>` (hash-based, memory-proportional
 *   to filled bins only), matching the memory characteristics of THnMulti's sparse path.
 * - On older Boost: `weight_storage` (dense), same as THnMulti's dense path.
 *
 * The final result is always a sparse THnSparseF.
 *
 * Select this backend by setting histogramBackend=boost in the configuration.
 */
class BHnMulti : public ROOT::Detail::RDF::RActionImpl<BHnMulti> {
public:
  /** @brief Result type: ROOT sparse histogram (same as THnMulti) */
  using Result_t = THnSparseF;

#if BOOST_VERSION >= 107600
  // Boost 1.76+: sparse_storage<weighted_sum<>> is available.
  // Only filled bins consume memory (same memory characteristics as THnSparseF).
  using BH5D = decltype(boost::histogram::make_histogram_with(
      boost::histogram::sparse_storage<boost::histogram::accumulators::weighted_sum<>>(),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0)));
#else
  // Boost < 1.76: sparse_storage not available; fall back to weight_storage (dense).
  // The output result is still a sparse THnSparseF.
  using BH5D = decltype(boost::histogram::make_histogram_with(
      boost::histogram::weight_storage(),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0),
      boost::histogram::axis::regular<>(1, 0.0, 1.0)));
#endif

  // Define a function pointer type for fill functions (same signature as THnMulti)
  using FillFuncType = void (BHnMulti::*)(unsigned int,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
    const ROOT::VecOps::RVec<Float_t>&, const ROOT::VecOps::RVec<Float_t>&,
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

    fPerThreadHists.reserve(nSlots_m);
    for (unsigned int i = 0; i < nSlots_m; i++) {
#if BOOST_VERSION >= 107600
      fPerThreadHists.push_back(bh::make_histogram_with(
          bh::sparse_storage<bh::accumulators::weighted_sum<>>(),
          bh::axis::regular<>(nbins_m[0], xmin_m[0], xmax_m[0]),
          bh::axis::regular<>(nbins_m[1], xmin_m[1], xmax_m[1]),
          bh::axis::regular<>(nbins_m[2], xmin_m[2], xmax_m[2]),
          bh::axis::regular<>(nbins_m[3], xmin_m[3], xmax_m[3]),
          bh::axis::regular<>(nbins_m[4], xmin_m[4], xmax_m[4])));
#else
      fPerThreadHists.push_back(bh::make_histogram_with(
          bh::weight_storage(),
          bh::axis::regular<>(nbins_m[0], xmin_m[0], xmax_m[0]),
          bh::axis::regular<>(nbins_m[1], xmin_m[1], xmax_m[1]),
          bh::axis::regular<>(nbins_m[2], xmin_m[2], xmax_m[2]),
          bh::axis::regular<>(nbins_m[3], xmin_m[3], xmax_m[3]),
          bh::axis::regular<>(nbins_m[4], xmin_m[4], xmax_m[4])));
#endif
    }

    fFinalResult = std::make_shared<THnSparseF>(
        name_m.c_str(), title_m.c_str(), dim_m,
        nbins_m.data(), xmin_m.data(), xmax_m.data());
    fFinalResult->Sumw2();

    // Select fill function fast path (same logic as THnMulti)
    if (!channel_hasSystematic_m && !controlRegion_hasSystematic_m && !sampleCategory_hasSystematic_m && !weight_hasSystematic_m && !systematic_hasMultiFill_m &&
        !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m) {
      fillFunc_ = &BHnMulti::SingleNoSystematicFill;
    } else if (systematic_hasMultiFill_m && !channel_hasMultiFill_m && !controlRegion_hasMultiFill_m && !sampleCategory_hasMultiFill_m && !weight_hasMultiFill_m) {
      fillFunc_ = &BHnMulti::SingleSystematicFill;
    } else if (!systematic_hasMultiFill_m && (channel_hasMultiFill_m || controlRegion_hasMultiFill_m || sampleCategory_hasMultiFill_m || weight_hasMultiFill_m)) {
      fillFunc_ = &BHnMulti::MultiNoSystematicFill;
    } else {
      fillFunc_ = &BHnMulti::GeneralFill;
    }
  }

  /** @brief Move constructor */
  BHnMulti(BHnMulti &&) = default;

  /** @brief Get result pointer */
  std::shared_ptr<Result_t> GetResultPtr() { return fFinalResult; }
  std::shared_ptr<Result_t> GetResultPtr() const { return fFinalResult; }

  void Initialize() {}
  void InitTask(TTreeReader *, int) {}

  /**
   * @brief Fill the per-thread Boost histogram for one entry.
   */
  void Exec(unsigned int slot,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
            const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
            const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {
    if (baseHistogramValues.empty() || baseHistogramWeights.empty() || systematicVariation.empty() ||
        sampleCategory.empty() || controlRegion.empty() || channel.empty() || nFills.empty()) {
      return;
    }
    (this->*fillFunc_)(slot, baseHistogramValues, baseHistogramWeights,
                       systematicVariation, sampleCategory, controlRegion, channel, nFills);
  }

  void SingleNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ sampleCategory,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ controlRegion,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ channel,
    const ROOT::VecOps::RVec<Int_t>   &__restrict__ nFills) {

    if (baseHistogramWeights.size() < 1 || channel.size() < 1 || controlRegion.size() < 1 ||
        sampleCategory.size() < 1 || systematicVariation.size() < 1) {
      return;
    }
    const double weight = static_cast<double>(baseHistogramWeights[0]);
    if (weight != 0.0) {
      fillHist_(slot, weight,
          static_cast<double>(channel[0]),
          static_cast<double>(controlRegion[0]),
          static_cast<double>(sampleCategory[0]),
          static_cast<double>(systematicVariation[0]),
          static_cast<double>(baseHistogramValues[0]));
    }
  }

  void SingleSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
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
          baseFillCounter >= static_cast<int>(systematicVariation.size())) {
        break;
      }
      const double weight = static_cast<double>(baseHistogramWeights[weightCounter]);
      if (weight != 0.0) {
        fillHist_(slot, weight,
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

  void MultiNoSystematicFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
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
          systematicCounter >= static_cast<int>(systematicVariation.size())) {
        break;
      }
      const double weight = static_cast<double>(baseHistogramWeights[weightCounter]);
      if (weight != 0.0) {
        fillHist_(slot, weight,
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

  void GeneralFill(unsigned int slot,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramValues,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ baseHistogramWeights,
    const ROOT::VecOps::RVec<Float_t> &__restrict__ systematicVariation,
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
          systematicCounter >= static_cast<int>(nFills.size())) {
        break;
      }
      const double weight = static_cast<double>(baseHistogramWeights[weightCounter]);
      if (weight != 0.0) {
        fillHist_(slot, weight,
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
   * Only bins with non-zero content are written to the result, keeping the output sparse.
   */
  void Finalize() {
    namespace bh = boost::histogram;

    if (fPerThreadHists.empty()) return;

    // Merge all per-thread histograms into the first one
    auto& merged = fPerThreadHists[0];
    for (size_t slot = 1; slot < fPerThreadHists.size(); slot++) {
      merged += fPerThreadHists[slot];
    }

    // Convert to THnSparseF: iterate only filled (inner) bins
    for (auto&& x : bh::indexed(merged, bh::coverage::inner)) {
      const auto& w = *x;
      const double content = w.value();
      const double variance = w.variance();
      if (content == 0.0 && variance == 0.0) {
        continue;  // skip empty bins — sparse output
      }

      // Compute bin-centre coordinates for each axis
      std::vector<Double_t> coords(dim_m);
      for (int d = 0; d < dim_m; d++) {
        coords[d] = merged.axis(d).bin(x.index(d)).center();
      }

      // Create bin in THnSparseF and set content/error
      Long64_t globalBin = fFinalResult->GetBin(coords.data(), true);
      fFinalResult->SetBinContent(globalBin, content);
      fFinalResult->SetBinError2(globalBin, variance);
    }
  }

  std::string GetActionName() const { return "BHnMulti"; }

private:
  /// Fill one bin in the per-thread Boost histogram.
  void fillHist_(unsigned int slot, double weight,
                 double ch, double cr, double sc, double sv, double bv) {
    fPerThreadHists[slot](boost::histogram::weight(weight), ch, cr, sc, sv, bv);
  }

  std::shared_ptr<THnSparseF> fFinalResult = std::make_shared<THnSparseF>();
  /// Per-thread Boost histograms.  On Boost >= 1.76 uses sparse_storage<weighted_sum<>>;
  /// on older Boost falls back to weight_storage (dense).  Output is always THnSparseF.
  std::vector<BH5D> fPerThreadHists;

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
           const char weight[], int bins, float lowerBound, float upperBound)
      : name_m(name), variable_m(variable), label_m(label), weight_m(weight),
        bins_m(bins), lowerBound_m(lowerBound), upperBound_m(upperBound) {}

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
