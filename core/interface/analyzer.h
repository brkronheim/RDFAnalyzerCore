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

#include <api/IBDTManager.h>
#include <api/IConfigurationProvider.h>
#include <api/ICorrectionManager.h>
#include <api/IDataFrameProvider.h>
#include <api/INDHistogramManager.h>
#include <api/ITriggerManager.h>
#include <DataManager.h>
#include <TChain.h>
#include <functions.h>

/**
 * @class Analyzer
 * @brief Main analysis class for event processing using ROOT's RDataFrame with dependency injection support.
 *
 * The Analyzer class manages configuration, data loading, event selection,
 * histogramming, application of corrections, BDTs, and systematics. It provides
 * a high-level interface for defining variables, applying filters, and managing
 * the analysis workflow. Supports dependency injection for improved testability and flexibility.
 */
class Analyzer {
public:
  /**
   * @brief Construct a new Analyzer object with dependency injection
   *
   * @param configProvider Unique pointer to the configuration provider
   * @param dataFrameProvider Unique pointer to the dataframe provider
   * @param bdtManager Unique pointer to the BDT manager interface
   * @param correctionManager Unique pointer to the correction manager interface
   * @param triggerManager Unique pointer to the trigger manager interface
   * @param ndHistManager Unique pointer to the ND histogram manager interface
   * @param systematicManager Unique pointer to the systematic manager interface
   */
  Analyzer(std::unique_ptr<IConfigurationProvider> configProvider,
           std::unique_ptr<IDataFrameProvider> dataFrameProvider,
           std::unique_ptr<IBDTManager> bdtManager,
           std::unique_ptr<ICorrectionManager> correctionManager,
           std::unique_ptr<ITriggerManager> triggerManager,
           std::unique_ptr<INDHistogramManager> ndHistManager,
           std::unique_ptr<ISystematicManager> systematicManager);

  /**
   * @brief Construct a new Analyzer object with backward compatibility
   *
   * @param configFile File containing the configuration information for this analyzer
   */
  Analyzer(std::string configFile);

  /**
   * @brief Define a new variable in the dataframe. Systematics are handled automatically.
   * @tparam F Callable type for the variable definition
   * @param name Name of the variable
   * @param f Callable to compute the variable
   * @param columns Input columns
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F>
  Analyzer *Define(std::string name, F f,
                  const std::vector<std::string> &columns = {}) {
    dataFrameProvider_m->Define(name, f, columns, getSystematicManager());
    return this;
  }

  /**
   * @brief Define a filter (selection) in the dataframe. Systematics are handled automatically.
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
    dataFrameProvider_m->Filter(passCut, {name}, getSystematicManager());
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
    dataFrameProvider_m->DefinePerSample(name, f);
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
    dataFrameProvider_m->DefinePerSample(name, storeVar);
    return (this);
  }

  /**
   * @brief Define a vector variable in the dataframe. Systematics are handled automatically.
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
   * @brief Save the configured branches to the output file and trigger the computation of the dataframe.
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
   * @brief Access the NDHistogramManager for booking and saving histograms.
   * @return Reference to NDHistogramManager
   */
  INDHistogramManager &getNDHistogramManager() { return *ndHistManager_m; }
  const INDHistogramManager &getNDHistogramManager() const { return *ndHistManager_m; }

  /**
   * @brief Get the BDT manager interface
   * @return Reference to the BDT manager interface
   */
  IBDTManager &getBDTManager() const { return *bdtManager_m; }

  /**
   * @brief Get the correction manager interface
   * @return Reference to the correction manager interface
   */
  ICorrectionManager &getCorrectionManager() const { return *correctionManager_m; }

  /**
   * @brief Get the trigger manager interface
   * @return Reference to the trigger manager interface
   */
  ITriggerManager &getTriggerManager() const { return *triggerManager_m; }

  /**
   * @brief Get the configuration provider interface
   * @return Reference to the configuration provider interface
   */
  IConfigurationProvider &getConfigurationProvider() const { return *configProvider_m; }

  /**
   * @brief Get the dataframe provider interface
   * @return Reference to the dataframe provider interface
   */
  IDataFrameProvider &getDataFrameProvider() const { return *dataFrameProvider_m; }

  /**
   * @brief Get the systematic manager interface
   * @return Reference to the systematic manager interface
   */
  ISystematicManager &getSystematicManager() const { return *systematicManager_m; }


private:
  /**
   * @brief Verbosity level for logging and debug output (higher = more verbose)
   */
  UInt_t verbosityLevel_m;
  /**
   * @brief Unique pointer to the configuration provider.
   */
  std::unique_ptr<IConfigurationProvider> configProvider_m;
  /**
   * @brief Unique pointer to the dataframe provider.
   */
  std::unique_ptr<IDataFrameProvider> dataFrameProvider_m;
  /**
   * @brief Unique pointer to the BDT manager interface.
   */
  std::unique_ptr<IBDTManager> bdtManager_m;
  /**
   * @brief Unique pointer to the correction manager interface.
   */
  std::unique_ptr<ICorrectionManager> correctionManager_m;
  /**
   * @brief Unique pointer to the trigger manager interface.
   */
  std::unique_ptr<ITriggerManager> triggerManager_m;
  /**
   * @brief Unique pointer to the ND histogram manager interface.
   */
  std::unique_ptr<INDHistogramManager> ndHistManager_m;
  /**
   * @brief Unique pointer to the systematic manager interface.
   */
  std::unique_ptr<ISystematicManager> systematicManager_m;
  /**
   * @brief Initialize the analyzer with the provided dependencies
   */
  void initialize();
};

#endif // ANALYZER_H_INCLUDED
