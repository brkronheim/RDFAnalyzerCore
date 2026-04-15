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
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/IAnalysisService.h>
#include <api/ManagerContext.h> // needed for wiring plugins and services

class ProvenanceService; // forward declare to avoid header pollution


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
           std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins,
           std::unique_ptr<ISystematicManager> systematicManager,
           std::unique_ptr<ILogger> logger,
           std::unique_ptr<IOutputSink> skimSink,
           std::unique_ptr<IOutputSink> metaSink);

  /// @brief Dependency-injected constructor (unique_ptr plugin map — backward compat).
  Analyzer(std::unique_ptr<IConfigurationProvider> configProvider,
           std::unique_ptr<IDataFrameProvider> dataFrameProvider,
           std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins,
           std::unique_ptr<ISystematicManager> systematicManager,
           std::unique_ptr<ILogger> logger,
           std::unique_ptr<IOutputSink> skimSink,
           std::unique_ptr<IOutputSink> metaSink);

  /**
   * @brief Construct a new Analyzer object with a config file and optional plugins.
   *
   * This constructor creates default configuration, data, systematic, logger, and output sinks.
   * Plugins can be optionally provided as a map; if omitted, no plugins are set.
   *
   * @param configFile Path to the configuration file.
   * @param plugins Map of plugin role names to pluggable manager instances (default: empty).
   */
  Analyzer(std::string configFile,
           std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins = {});

  /// @brief Config-file constructor (unique_ptr plugin map — backward compat).
  Analyzer(std::string configFile,
           std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& plugins);

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
   * @brief Redefine an existing variable in the dataframe.
   * @tparam F Callable type for the variable redefinition
   * @param name Name of the variable to redefine
   * @param f Callable to compute the new variable value
   * @param columns Input columns
   * @return Pointer to this Analyzer (for chaining)
   */
  template <typename F>
  Analyzer *Redefine(std::string name, F f,
                    const std::vector<std::string> &columns = {}) {
    dataFrameProvider_m->Redefine(name, f, columns);
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
    if (!preFilterNotified_m) {
      auto df = dataFrameProvider_m->getDataFrame();
      for (auto& service : services_m) {
        service->onPreFilter(df);
      }
      preFilterNotified_m = true;
    }
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
   * @brief Book histograms defined in config file
   * 
   * This method should be called after all Define and Filter operations
   * are complete, but before save(). It triggers the NDHistogramManager
   * to book all histograms that were loaded from the config file.
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *bookConfigHistograms();

  /**
   * @brief Book an integer-branch counter histogram on the CounterService.
   *
   * Call this after the integer branch (e.g. a stitching code) has been defined
   * via Define(), and before the event loop runs (before save()). The histogram
   * range must be known in advance. This forwards to CounterService and keeps
   * all result pointers alive so the event loop executes only once.
   *
   * @param branch Column name of the integer branch (e.g. "stitchBinNLO").
   * @param nBins  Number of histogram bins.
   * @param low    Lower edge of the histogram x-axis.
   * @param high   Upper edge of the histogram x-axis.
   * @return Pointer to this Analyzer (for chaining).
   */
  Analyzer *bookCounterIntHistogram(const std::string& branch, int nBins,
                                    double low, double high);

  /**
   * @brief Save the configured branches to the output file and trigger the computation of the dataframe.
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *save();

  /**
   * @brief Unified run/save entry point.
   *
   * Triggers the event loop exactly once. This method:
   *  - Writes a skim only when @c enableSkim=1 (or @c true/@c True) is present in the config.
   *  - Saves all histograms booked on the NDHistogramManager (if one is registered).
   *  - Finalizes all analysis services (e.g. CounterService).
   *
   * Use this instead of manually calling save() followed by a separate
   * NDHistogramManager::saveHists() call.
   *
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *run();

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
  std::shared_ptr<T> getPlugin(const std::string& key) const {
    auto it = plugins.find(key);
    if (it != plugins.end()) {
      return std::dynamic_pointer_cast<T>(it->second);
    }
    return nullptr;
  }

  /**
   * @brief Get the dataframe provider interface
   * @return Reference to the dataframe provider interface
   */
  IDataFrameProvider &getDataFrameProvider() const { return *dataFrameProvider_m; }

  /**
   * @brief Register a pluggable manager after construction and immediately wire it.
   * This will set the manager context and call setupFromConfigFile() for the plugin.
   * @param role Role name for the plugin (e.g. "histogramManager")
   * @param plugin Unique pointer to the plugin manager to register
   * @return Pointer to this Analyzer (for chaining)
   */
  Analyzer *addPlugin(const std::string &role, std::shared_ptr<IPluggableManager> plugin);
  /// Accept any unique_ptr<T> (T must derive from IPluggableManager).
  /// Template avoids ambiguity with the shared_ptr overload when T is a derived type.
  template<typename T>
  Analyzer *addPlugin(const std::string &role, std::unique_ptr<T> plugin) {
    return addPlugin(role, std::shared_ptr<IPluggableManager>(std::move(plugin)));
  }

  Analyzer *addPlugins(std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& newPlugins);
  Analyzer *addPlugins(std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& newPlugins);

  /**
   * @brief Expose the configuration provider used by this Analyzer.
   * Useful for constructing plugins that require IConfigurationProvider at construction.
   * @return Reference to the configuration provider
   */
  IConfigurationProvider &getConfigurationProvider() const { return *configProvider_m; }

  /**
   * @brief Inject task-level provenance metadata.
   *
   * Stores a key-value pair that is contributed to the ProvenanceService at
   * finalize time under the namespace "task.<key>".  This allows workflow
   * orchestration layers (e.g. LAW tasks) to attach task-specific information
   * (job ID, dataset query hash, submission timestamp, etc.) to the provenance
   * record without bypassing the provenance interface.
   *
   * May be called at any time before save() / run().
   *
   * @param key   Metadata key (will be stored as "task.<key>").
   * @param value Metadata value.
   * @return Pointer to this Analyzer (for chaining).
   */
  Analyzer *setTaskMetadata(const std::string& key, const std::string& value);

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
   * @brief Unique pointer to the logger.
   */
  std::unique_ptr<ILogger> logger_m;
  /**
   * @brief Unique pointer to the skim output sink.
   */
  std::unique_ptr<IOutputSink> skimSink_m;
  /**
   * @brief Unique pointer to the metadata/hist output sink.
   */
  std::unique_ptr<IOutputSink> metaSink_m;
  /**
   * @brief Persisted ManagerContext for wiring plugins/services. Stored here so
   * services can keep a pointer/reference to a context that outlives stack
   * temporaries.
   */
  ManagerContext managerContext_m;

  /**
   * @brief Map of plugin role names to pluggable manager instances.
   */
  std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
  /**
   * @brief Optional analysis services (internal only for now).
   */
  std::vector<std::unique_ptr<IAnalysisService>> services_m;
  /**
   * @brief Non-owning pointer to the ProvenanceService (owned by services_m).
   * Kept separately so addPlugin() can register plugin provenance entries
   * after initializeServices() has run.
   */
  ProvenanceService* provenanceService_m = nullptr;
  /**
   * @brief Task-level provenance metadata contributed via setTaskMetadata().
   * Entries are stored here until finalize time, then forwarded to the
   * ProvenanceService under the "task.<key>" namespace.
   */
  std::unordered_map<std::string, std::string> taskMetadata_m;
  bool preFilterNotified_m = false;
  ///**
  // * @brief Initialize the analyzer with the provided dependencies
  // */
  //void initialize();
  /**
   * @brief Wire up plugin manager pointers to core managers
   */
  void wirePluginManagers();
  void initializeServices(ManagerContext& ctx);
  /**
   * @brief Collect structured provenance contributions from all plugins and
   * services, inject task metadata, then finalize the ProvenanceService.
   *
   * Called at the end of save() and run() to ensure all plugin/service
   * provenance contributions are captured before the provenance record is
   * written to disk.
   */
  void collectAndRegisterProvenance(ROOT::RDF::RNode& df);
};

#endif // ANALYZER_H_INCLUDED
