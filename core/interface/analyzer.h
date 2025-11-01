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
#include <vector>

#include <ROOT/RDFHelpers.hxx>
#include <ROOT/RDataFrame.hxx>


#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <TChain.h>
#include <functions.h>
#include <api/IPluggableManager.h>

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
   * @param plugins Rvalue reference to a map of plugin role names to pluggable manager instances
   * @param systematicManager Unique pointer to the systematic manager interface
   */
  Analyzer(std::unique_ptr<IConfigurationProvider> configProvider,
           std::unique_ptr<IDataFrameProvider> dataFrameProvider,
           std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins,
           std::unique_ptr<ISystematicManager> systematicManager);

  /**
   * @brief Construct a new Analyzer object with backward compatibility
   *
   * @param configFile File containing the configuration information for this analyzer
   * @param pluginSpecs Map of plugin role names to (type, args) for instantiation
   */
  Analyzer(std::string configFile,
           const std::unordered_map<std::string, std::pair<std::string, std::vector<void*>>>& pluginSpecs);

  /**
   * @brief Construct a new Analyzer object with a config file and optional plugins.
   *
   * This constructor creates default configuration, data, and systematic managers using the provided config file.
   * Plugins can be optionally provided as a map; if omitted, no plugins are set.
   *
   * @param configFile Path to the configuration file.
   * @param plugins Map of plugin role names to pluggable manager instances (default: empty).
   */
  Analyzer(std::string configFile,
           std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins = {});

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
    dataFrameProvider_m->Filter(passCut, {name});
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

  ///**
  // * @brief Get a correction object by key.
  // * @param key Correction key
  // * @return Correction reference
  // */
  //correction::Correction::Ref correctionMap(std::string key);


  /**
   * @brief Get the systematic manager interface
   * @return Reference to the systematic manager interface
   */
  ISystematicManager &getSystematicManager() const { return *systematicManager_m; }

  /**
   * @brief Get a plugin by role name and cast to the desired interface.
   * @tparam T Interface type to cast to
   * @param key Role name of the plugin
   * @return Pointer to the plugin as T, or nullptr if not found or wrong type
   */
  template<typename T>
  T* getPlugin(const std::string& key) const {
    auto it = plugins.find(key);
    if (it != plugins.end()) {
      return dynamic_cast<T*>(it->second.get());
    }
    return nullptr;
  }

  /**
   * @brief Get the dataframe provider interface
   * @return Reference to the dataframe provider interface
   */
  IDataFrameProvider &getDataFrameProvider() const { return *dataFrameProvider_m; }

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
   * @brief Unique pointer to the systematic manager interface.
   */
  std::unique_ptr<ISystematicManager> systematicManager_m;
  /**
   * @brief Map of plugin role names to pluggable manager instances.
   */
  std::unordered_map<std::string, std::unique_ptr<IPluggableManager>> plugins;
  ///**
  // * @brief Initialize the analyzer with the provided dependencies
  // */
  //void initialize();
  /**
   * @brief Wire up plugin manager pointers to core managers
   */
  void wirePluginManagers();
};

#endif // ANALYZER_H_INCLUDED
