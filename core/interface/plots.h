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

/**
 * @class THnMulti
 * @brief Multi-threaded N-dimensional histogram action for ROOT RDataFrame.
 *
 * This class manages a set of THnSparseD histograms, one per thread, and merges
 * them at the end of processing. It is used as a custom action in ROOT's
 * RDataFrame for efficient multi-threaded histogramming.
 */
class THnMulti : public ROOT::Detail::RDF::RActionImpl<THnMulti> {

public:
  /**
   * @brief Type alias for the result type (THnSparseD)
   */
  using Result_t = THnSparseD;

  /**
   * @brief Construct a new THnMulti object
   * @param nSlots Number of threads/slots
   * @param name Histogram name
   * @param title Histogram title
   * @param dim Number of dimensions
   * @param nFills Number of fills per event
   * @param nbins Number of bins per dimension
   * @param xmin Minimum value per dimension
   * @param xmax Maximum value per dimension
   */
  THnMulti(unsigned int nSlots, std::string name, std::string title, Int_t dim,
           Int_t nFills, std::vector<Int_t> nbins, std::vector<Double_t> xmin,
           std::vector<Double_t> xmax) {
    nSlots_m = nSlots;
    dim_m = dim;
    nbins_m = nbins;
    xmin_m = xmin;
    xmax_m = xmax;
    nFills_m = nFills;
    for (unsigned int i = 0; i < nSlots; i++) {

      fPerThreadResults.push_back(std::make_shared<THnSparseD>(
          (name + "_" + std::to_string(i)).c_str(), title.c_str(), dim,
          nbins.data(), xmin.data(), xmax.data()));
      fPerThreadResults[i]->Sumw2();
    }
    fFinalResult =
        std::make_shared<Result_t>((name).c_str(), title.c_str(), dim,
                                   nbins.data(), xmin.data(), xmax.data());
    // Define all the histograms
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
  std::shared_ptr<THnSparseD> GetResultPtr() const { return fFinalResult; }

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
  void Exec(unsigned int slot, ROOT::VecOps::RVec<Double_t> &val) {
    Double_t *array;
    Double_t weight;
    for (int i = 0; i < nFills_m; i++) {
      array = val.data() + i * (dim_m + 1);
      weight = array[dim_m];
      if (weight == 0.0) {
        continue;
      }

      fPerThreadResults[slot]->Fill(array, weight); // Hits
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
  std::shared_ptr<THnSparseD> fFinalResult = std::make_shared<THnSparseD>();
  /** @brief Vector of per-thread THnSparseD histogram pointers. */
  std::vector<std::shared_ptr<THnSparseD>> fPerThreadResults;
  /** @brief Number of threads/slots. */
  unsigned int nSlots_m;
  /** @brief Number of dimensions. */
  Int_t dim_m;
  /** @brief Number of bins per dimension. */
  std::vector<Int_t> nbins_m;
  /** @brief Minimum value per dimension. */
  std::vector<Double_t> xmin_m;
  /** @brief Maximum value per dimension. */
  std::vector<Double_t> xmax_m;
  /** @brief Number of fills per event. */
  Int_t nFills_m;
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
  /**
   * @brief Construct a new selectionInfo object.
   * @param variable Variable to apply the selection on.
   * @param bins Number of bins for the selection.
   * @param lowerBound Lower bound for the selection.
   * @param upperBound Upper bound for the selection.
   */
  selectionInfo(std::string variable, int bins, double lowerBound,
                double upperBound)
      : variable_m(variable), bins_m(bins), lowerBound_m(lowerBound),
        upperBound_m(upperBound) {}

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

private:
  /** @brief Variable to apply the selection on. */
  const std::string variable_m;
  /** @brief Number of bins for the selection. */
  const int bins_m;
  /** @brief Lower bound for the selection. */
  const float lowerBound_m;
  /** @brief Upper bound for the selection. */
  const float upperBound_m;
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
