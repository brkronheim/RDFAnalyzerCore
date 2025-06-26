/**
 * @file analyzer.h
 * @brief Declaration of the Analyzer class for event analysis.
 *
 The Analyzer class provides an interface for configuring, processing, and
 analyzing event data
 * using ROOT's RDataFrame. It supports defining variables, applying filters,
 handling systematics,
 * and managing histograms and corrections.
 */
#ifndef ANALYZER_H_INCLUDED
#define ANALYZER_H_INCLUDED

#include <ROOT/RResultPtr.hxx>

#include <RtypesCore.h>
#include <correction.h>
#include <fastforest.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>

#include <BDTManager.h>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <TChain.h>
#include <TriggerManager.h>
#include <functions.h>

/**
 * @class Analyzer
 * @brief Main analysis class for event processing using ROOT's RDataFrame.
 *
 * The Analyzer class manages configuration, data loading, event selection,
 * histogramming, application of corrections, BDTs, and systematics. It provides
 * a high-level interface for defining variables, applying filters, and managing
 * the analysis workflow.
 */
class Analyzer {
public:
  /**
   * @brief Construct a new Analyzer object
   *
   * @param configFile File containing the configuration information for this
   * analyzer
   */
  Analyzer(std::string configFile);

  /**
   * @brief Define a new variable in the dataframe. Systematics are handled
   * automatically.
   * @tparam F Callable type for the variable definition
   * @param name Name of the variable
   * @param f Callable to compute the variable
   * @param columns Input columns
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F>
  Analyzer *Define(std::string name, F f,
                   const std::vector<std::string> &columns = {}) {
    dataManager_m.Define(name, f, columns);
    return this;
  }

  /**
   * @brief Define a filter (selection) in the dataframe. Systematics are
   * handled automatically.
   * @tparam F Callable type for the filter
   * @param name Name of the filter
   * @param f Callable to compute the filter
   * @param columns Input columns for the callable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F>
  Analyzer *Filter(std::string name, F f,
                   const std::vector<std::string> &columns = {}) {
    name = "pass_" + name;
    Define(name, f, columns);
    dataManager_m.Filter_m(passCut, {name});
    return this;
  }

  /**
   * @brief Define a variable per sample (e.g., for storing constants).
   * @tparam F Callable type for the variable definition
   * @param name Name of the variable
   * @param f Callable to compute the variable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F> Analyzer *DefinePerSample(std::string name, F f) {
    dataManager_m.DefinePerSample_m<F>(name, f);
    return (this);
  }

  /**
   * @brief Save a constant variable per sample.
   * @tparam T Type of the variable
   * @param var Value to store
   * @param name Name of the variable
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename T> Analyzer *SaveVar(T var, std::string name) {
    auto storeVar = [var](unsigned int, const ROOT::RDF::RSampleInfo) -> T {
      return (var);
    };

    std::cout << "Defining variable " << name << " to be " << var << std::endl;
    dataManager_m.DefinePerSample_m(name, storeVar);

    return (this);
  }

  /**
   * @brief Define a vector variable in the dataframe. Systematics are handled
   * automatically.
   * @param name Name of the variable
   * @param columns Input columns
   * @param type Data type (default: Float_t)
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *DefineVector(std::string name,
                         const std::vector<std::string> &columns = {},
                         std::string type = "Float_t");

  /**
   * @brief Apply a Boosted Decision Tree (BDT) to the dataframe.
   *
   * This method defines the input vector for the BDT, creates a lambda for BDT
   * evaluation, and defines the BDT output variable in the dataframe.
   *
   * @param BDTName Name of the BDT to apply
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyBDT(std::string BDTName);

  /**
   * @brief Apply a correction using correctionlib.
   * @param correctionName Name of the correction
   * @param stringArguments Arguments for the correction
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyCorrection(std::string correctionName,
                            std::vector<std::string> stringArguments);

  /**
   * @brief Apply all registered BDTs to the dataframe.
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyAllBDTs();

  /**
   * @brief Apply all registered triggers to the dataframe.
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *ApplyAllTriggers();

  /**
   * @brief Save the configured branches to the output file and trigger the
   * computation of the dataframe.
   *
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *save();

  /**
   * @brief Get the underlying RDataFrame node.
   * @return The current RNode
   */
  ROOT::RDF::RNode getDF();

  /**
   * @brief Get a configuration value by key.
   * @param key Configuration key
   * @return Configuration value
   */
  std::string configMap(std::string key);

  /**
   * @brief Get a correction object by key.
   * @param key Correction key
   * @return Correction reference
   */
  correction::Correction::Ref correctionMap(std::string key);

  /**
   * @brief Get a reference to the DataManager.
   * @return Reference to DataManager
   */
  DataManager &getDataManager() { return dataManager_m; }
  const DataManager &getDataManager() const { return dataManager_m; }
  // TODO: why are there two of these?

  /**
   * @brief Access the NDHistogramManager for booking and saving histograms.
   * @return Reference to NDHistogramManager
   */
  NDHistogramManager &getNDHistogramManager() { return ndHistManager_m; }
  const NDHistogramManager &getNDHistogramManager() const {
    return ndHistManager_m;
  }

private:
  /// Verbosity level for logging and debug output (higher = more verbose)
  UInt_t verbosityLevel_m;

  // Managers
  ConfigurationManager configManager_m;
  DataManager dataManager_m;
  CorrectionManager correctionManager_m;
  BDTManager bdtManager_m;
  TriggerManager triggerManager_m;

  /// ND histogram manager
  NDHistogramManager ndHistManager_m;
};

#endif
