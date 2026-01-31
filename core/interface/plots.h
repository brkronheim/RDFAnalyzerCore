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

    


    for (unsigned int i = 0; i < nSlots_m; i++) {

      fPerThreadResults.push_back(std::make_shared<THnSparseF>(
          (name_m + "_" + std::to_string(i)).c_str(), title_m.c_str(), dim_m,
          nbins_m.data(), xmin_m.data(), xmax_m.data()));
      fPerThreadResults[i]->Sumw2();
    }
    fFinalResult =
        std::make_shared<Result_t>((name_m).c_str(), title_m.c_str(), dim_m,
                                   nbins_m.data(), xmin_m.data(), xmax_m.data());
    // Define all the histograms

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

      const Double_t weight = baseHistogramWeights[0];

      // Skip zero-weight entries for efficiency
      if (weight != 0.0) { // Skip zero-weight contributions.
        // Prepare the array of values to fill into the histogram.
        // The array size and contents should match the histogram dimensionality and axis order.
    
        fPerThreadResults[slot]->Fill(channel[0], controlRegion[0], sampleCategory[0], systematicVariation[0], baseHistogramValues[0], weight);
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
        const Double_t weight = baseHistogramWeights[weightCounter];
  
        // Skip zero-weight entries for efficiency
        if (weight != 0.0) { // Skip zero-weight contributions.
          // Prepare the array of values to fill into the histogram.
          // The array size and contents should match the histogram dimensionality and axis order.
  
          fPerThreadResults[slot]->Fill(channel[channelCounter], controlRegion[controlRegionCounter], sampleCategory[sampleCategoryCounter], systematicVariation[baseFillCounter], baseHistogramValues[baseFillCounter], weight);
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
        const Double_t weight = baseHistogramWeights[weightCounter];
  
        // Skip zero-weight entries for efficiency
        if (weight != 0.0) { // Skip zero-weight contributions.
          // Prepare the array of values to fill into the histogram.
          // The array size and contents should match the histogram dimensionality and axis order.
  
  
          fPerThreadResults[slot]->Fill(channel[channelCounter], controlRegion[controlRegionCounter], sampleCategory[sampleCategoryCounter], systematicVariation[systematicCounter], baseHistogramValues[baseFillCounter], weight);
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
      const Double_t weight = baseHistogramWeights[weightCounter];

      // Skip zero-weight entries for efficiency
      if (weight != 0.0) { // Skip zero-weight contributions.
        // Prepare the array of values to fill into the histogram.
        // The array size and contents should match the histogram dimensionality and axis order.

        fPerThreadResults[slot]->Fill(channel[channelCounter], controlRegion[controlRegionCounter], sampleCategory[sampleCategoryCounter], systematicVariation[systematicCounter], baseHistogramValues[baseFillCounter], weight);
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
   * @brief Merge per-thread histograms at the end of the event loop
   */
  void Finalize() {
    for (auto hist : fPerThreadResults) {
      // auto rawPtr = ;
      fFinalResult->Add(hist.get());
    }
  }

  /**
   * @brief Get the name of this action for RDataFrame
   * @return Action name string
   */
  std::string GetActionName() const { return "THnMulti"; }

private:
  /** @brief Shared pointer to the final merged THnSparseD result. */
  std::shared_ptr<THnSparseF> fFinalResult = std::make_shared<THnSparseF>();
  /** @brief Vector of per-thread THnSparseD histogram pointers. */
  std::vector<std::shared_ptr<THnSparseF>> fPerThreadResults;
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
  
  // Pointer to the selected fill function for fast path dispatch
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
