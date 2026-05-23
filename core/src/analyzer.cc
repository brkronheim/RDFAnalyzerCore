#include <analyzer.h>
#include <ManagerFactory.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <RootOutputSink.h>
#include <CounterService.h>
#include <ProvenanceService.h>
#include <NDHistogramManager.h>
#include <functions.h>
#include <iostream>
#include <util.h>
#include <algorithm>
#include <memory>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <SystematicManager.h>
#include <api/ManagerContext.h> // for wiring plugins and services

// Forward declaration of helper defined at file scope below.
static void registerPluginProvenance(
    ProvenanceService* svc,
    const std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>& plugins);

// Dependency-injected constructor (shared_ptr plugin map)
Analyzer::Analyzer(
    std::unique_ptr<IConfigurationProvider> configProvider,
    std::unique_ptr<IDataFrameProvider> dataFrameProvider,
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins,
    std::unique_ptr<ISystematicManager> systematicManager,
    std::unique_ptr<ILogger> logger,
    std::unique_ptr<IOutputSink> skimSink,
    std::unique_ptr<IOutputSink> metaSink)
    : configProvider_m(std::move(configProvider)),
      dataFrameProvider_m(std::move(dataFrameProvider)),
      systematicManager_m(std::move(systematicManager)),
      logger_m(std::move(logger)),
      skimSink_m(std::move(skimSink)),
      metaSink_m(std::move(metaSink)),
      managerContext_m{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m},
      plugins(std::move(plugins))
{
    if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m || !logger_m || !skimSink_m || !metaSink_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    if (auto* dataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get())) {
        dataManager->finalizeSetup(*configProvider_m);
    }
    initializeServices(managerContext_m);
    wirePluginManagers();
    registerPluginProvenance(provenanceService_m.get(), plugins);
}

// Dependency-injected constructor (unique_ptr plugin map — backward compat)
Analyzer::Analyzer(
    std::unique_ptr<IConfigurationProvider> configProvider,
    std::unique_ptr<IDataFrameProvider> dataFrameProvider,
    std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& uniquePlugins,
    std::unique_ptr<ISystematicManager> systematicManager,
    std::unique_ptr<ILogger> logger,
    std::unique_ptr<IOutputSink> skimSink,
    std::unique_ptr<IOutputSink> metaSink)
    : configProvider_m(std::move(configProvider)),
      dataFrameProvider_m(std::move(dataFrameProvider)),
      systematicManager_m(std::move(systematicManager)),
      logger_m(std::move(logger)),
      skimSink_m(std::move(skimSink)),
      metaSink_m(std::move(metaSink)),
      managerContext_m{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m}
{
    if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m || !logger_m || !skimSink_m || !metaSink_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    for (auto& kv : uniquePlugins) { plugins.emplace(kv.first, std::move(kv.second)); }
    verbosityLevel_m = 1;
    if (auto* dataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get())) {
        dataManager->finalizeSetup(*configProvider_m);
    }
    initializeServices(managerContext_m);
    wirePluginManagers();
    registerPluginProvenance(provenanceService_m.get(), plugins);
}

Analyzer::Analyzer(std::string configFile,
                                     std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& plugins)
        : configProvider_m(ManagerFactory::createConfigurationManager(configFile)),
            dataFrameProvider_m(ManagerFactory::createDataManager(*configProvider_m)),
            systematicManager_m(ManagerFactory::createSystematicManager()),
            logger_m(std::make_unique<DefaultLogger>()),
    skimSink_m(std::make_unique<RootOutputSink>()),
            metaSink_m(std::make_unique<RootOutputSink>()),            managerContext_m{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m},            plugins(std::move(plugins))
{
        if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    verbosityLevel_m = 1;
    if (auto* dataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get())) {
        dataManager->finalizeSetup(*configProvider_m);
    }
    initializeServices(managerContext_m);
    wirePluginManagers();
    registerPluginProvenance(provenanceService_m.get(), plugins);
}

// Config-file constructor (unique_ptr plugin map — backward compat)
Analyzer::Analyzer(std::string configFile,
                                     std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& uniquePlugins)
        : configProvider_m(ManagerFactory::createConfigurationManager(configFile)),
            dataFrameProvider_m(ManagerFactory::createDataManager(*configProvider_m)),
            systematicManager_m(ManagerFactory::createSystematicManager()),
            logger_m(std::make_unique<DefaultLogger>()),
    skimSink_m(std::make_unique<RootOutputSink>()),
            metaSink_m(std::make_unique<RootOutputSink>()),            managerContext_m{*configProvider_m, *dataFrameProvider_m, *systematicManager_m, *logger_m, *skimSink_m, *metaSink_m}
{
        if (!configProvider_m || !dataFrameProvider_m || !systematicManager_m) {
        throw std::invalid_argument("Analyzer: Core dependencies must be non-null");
    }
    for (auto& kv : uniquePlugins) { plugins.emplace(kv.first, std::move(kv.second)); }
    verbosityLevel_m = 1;
    if (auto* dataManager = dynamic_cast<DataManager*>(dataFrameProvider_m.get())) {
        dataManager->finalizeSetup(*configProvider_m);
    }
    initializeServices(managerContext_m);
    wirePluginManagers();
    registerPluginProvenance(provenanceService_m.get(), plugins);
}

/*
void Analyzer::initialize() {
    if (verbosityLevel_m >= 1) {
        std::cout << "Analyzer initialized." << std::endl;
    }
}
*/

void Analyzer::wirePluginManagers() {
    // Validate + topological sort + lifecycle hooks all in one pass.
    // Dependency validation.
    for (auto& [role, plugin] : plugins) {
        if (!plugin) continue;
        for (const auto& dep : plugin->getDependencies()) {
            if (plugins.find(dep) == plugins.end()) {
                throw std::runtime_error(
                    "Plugin '" + role + "' declares dependency '" + dep +
                    "' which is not registered.");
            }
        }
    }

    // Topological sort via Kahn's algorithm.
    std::unordered_map<std::string, int> inDegree;
    std::unordered_map<std::string, std::vector<std::string>> dependents;
    for (auto& [role, plugin] : plugins) {
        if (!inDegree.count(role)) inDegree[role] = 0;
        if (!plugin) continue;
        for (const auto& dep : plugin->getDependencies()) {
            inDegree[role]++;
            dependents[dep].push_back(role);
        }
    }

    std::queue<std::string> ready;
    for (auto& [role, deg] : inDegree) {
        if (deg == 0) ready.push(role);
    }

    std::vector<std::string> order;
    while (!ready.empty()) {
        std::string cur = ready.front();
        ready.pop();
        order.push_back(cur);
        for (const auto& dep : dependents[cur]) {
            if (--inDegree[dep] == 0) ready.push(dep);
        }
    }

    if (order.size() != plugins.size()) {
        throw std::runtime_error(
            "Analyzer: circular dependency detected among registered plugins.");
    }
    pluginOrder_m = order;

    // setContext + setupFromConfigFile in topological order.
    for (const auto& role : order) {
        auto& plugin = plugins.at(role);
        if (!plugin) continue;
        plugin->setContext(managerContext_m);
        plugin->setupFromConfigFile();
    }

    // initialize() in topological order.
    for (const auto& role : order) {
        auto& plugin = plugins.at(role);
        if (!plugin) continue;
        plugin->initialize();
    }
}

void Analyzer::reorderPlugins() {
    // Re-run Kahn's algorithm over the current plugin set.
    std::unordered_map<std::string, int> inDegree;
    std::unordered_map<std::string, std::vector<std::string>> dependents;
    for (auto& [role, plugin] : plugins) {
        if (!inDegree.count(role)) inDegree[role] = 0;
        if (!plugin) continue;
        for (const auto& dep : plugin->getDependencies()) {
            if (plugins.find(dep) == plugins.end()) {
                throw std::runtime_error(
                    "Plugin '" + role + "' declares dependency '" + dep +
                    "' which is not registered.");
            }
            inDegree[role]++;
            dependents[dep].push_back(role);
        }
    }

    std::queue<std::string> ready;
    for (auto& [role, deg] : inDegree) {
        if (deg == 0) ready.push(role);
    }

    std::vector<std::string> order;
    while (!ready.empty()) {
        std::string cur = ready.front();
        ready.pop();
        order.push_back(cur);
        for (const auto& dep : dependents[cur]) {
            if (--inDegree[dep] == 0) ready.push(dep);
        }
    }

    if (order.size() != plugins.size()) {
        throw std::runtime_error(
            "Analyzer: circular dependency detected among registered plugins.");
    }
    pluginOrder_m = std::move(order);
}

Analyzer *Analyzer::addPlugin(const std::string &role, std::shared_ptr<IPluggableManager> plugin) {
    if (!plugin) return this;
    // Validate that the new plugin's dependencies exist.
    for (const auto& dep : plugin->getDependencies()) {
        if (plugins.find(dep) == plugins.end()) {
            throw std::runtime_error(
                "Plugin '" + role + "' declares dependency '" + dep +
                "' which is not registered.");
        }
    }

    // Insert the new plugin.
    plugins.emplace(role, std::move(plugin));
    pluginOrder_m.push_back(role);

    // Re-run topological sort over the full set to ensure order is valid.
    // (Existing plugins that depend on the new role will now be ordered correctly.)
    reorderPlugins();

    // Lifecycle hooks for the new plugin only.
    auto& inserted = plugins.at(role);
    inserted->setContext(managerContext_m);
    inserted->setupFromConfigFile();
    inserted->initialize();

    if (provenanceService_m) {
        provenanceService_m->addEntry("plugin." + role, inserted->type());
    }
    return this;
}

Analyzer *Analyzer::addPlugins(std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>&& newPlugins) {
    for (auto &kv : newPlugins) { addPlugin(kv.first, std::move(kv.second)); }
    return this;
}

Analyzer *Analyzer::addPlugins(std::unordered_map<std::string, std::unique_ptr<IPluggableManager>>&& newPlugins) {
    for (auto &kv : newPlugins) { addPlugin(kv.first, std::move(kv.second)); }
    return this;
}

void Analyzer::initializeServices(ManagerContext& ctx) {
    // ProvenanceService: always enabled. Plugin provenance entries are
    // registered after wirePluginManagers() returns.
    // Owned via shared_ptr (not in services_m) to decouple its lifetime
    // from the services_m vector.
    {
        auto svc = std::make_shared<ProvenanceService>();
        svc->initialize(ctx);
        provenanceService_m = std::move(svc);
    }

    const auto& configMap = configProvider_m->getConfigMap();
    auto it = configMap.find("enableCounters");
    if (it != configMap.end()) {
        const auto& val = it->second;
        const bool enabled = (val == "1" || val == "true" || val == "True");
        if (enabled) {
            auto service = std::make_unique<CounterService>();
            service->initialize(ctx);
            services_m.emplace_back(std::move(service));
        }
    } 
}

// Register provenance for all wired plugins. Called after wirePluginManagers().
static void registerPluginProvenance(ProvenanceService* svc,
    const std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>& plugins) {
    if (!svc) return;
    for (const auto& [role, plugin] : plugins) {
        if (plugin) {
            svc->addEntry("plugin." + role, plugin->type());
        }
    }
}

Analyzer *Analyzer::DefineVector(std::string name, const std::vector<std::string> &columns, std::string type) {
    dataFrameProvider_m->DefineVector(name, columns, type, *systematicManager_m);
    return this;
}

Analyzer *Analyzer::bookConfigHistograms() {
    // Get the NDHistogramManager plugin if it exists
    auto histogramManager = getPlugin<NDHistogramManager>("histogramManager");
    if (histogramManager) {
        histogramManager->bookConfigHistograms();
    }
    return this;
}

Analyzer *Analyzer::bookCounterIntHistogram(const std::string& branch, int nBins,
                                             double low, double high) {
    auto df = dataFrameProvider_m->getDataFrame();
    for (auto& service : services_m) {
        if (auto* cs = dynamic_cast<CounterService*>(service.get())) {
            cs->bookIntWeightHistogram(df, branch, nBins, low, high);
        }
    }
    return this;
}


Analyzer *Analyzer::setTaskMetadata(const std::string& key, const std::string& value) {
    taskMetadata_m[key] = value;
    return this;
}

void Analyzer::collectAndRegisterProvenance(ROOT::RDF::RNode& df) {
    if (provenanceService_m) {
        // Task-level metadata (injected via setTaskMetadata())
        for (const auto& [k, v] : taskMetadata_m) {
            provenanceService_m->addEntry("task." + k, v);
        }

        // Collect structured provenance contributions from each plugin.
        // Entries are stored under "plugin.<role>.<key>".  A content hash
        // ("plugin.<role>.config_hash") is auto-computed from the returned
        // entries so that any configuration change is detectable.
        for (const auto& [role, plugin] : plugins) {
            if (!plugin) continue;
            const auto entries = plugin->collectProvenanceEntries();
            if (entries.empty()) continue;

            for (const auto& [k, v] : entries) {
                provenanceService_m->addEntry("plugin." + role + "." + k, v);
            }

            // Serialize sorted entries for deterministic hashing.
            std::vector<std::pair<std::string, std::string>> sorted(
                entries.begin(), entries.end());
            std::sort(sorted.begin(), sorted.end());
            std::ostringstream oss;
            for (const auto& [k, v] : sorted) {
                oss << k << '=' << v << '\n';
            }
            provenanceService_m->addEntry(
                "plugin." + role + ".config_hash",
                ProvenanceService::hashString(oss.str()));
        }

        // Collect structured provenance contributions from non-provenance services.
        // Services choose their own key namespacing (no prefix is added by the
        // framework to allow services like CounterService to use descriptive keys).
        // Note: ProvenanceService is NOT in services_m (owned via shared_ptr).
        for (const auto& svc : services_m) {
            for (const auto& [k, v] : svc->collectProvenanceEntries()) {
                provenanceService_m->addEntry(k, v);
            }
        }
    }

    // ProvenanceService finalizes last so it captures all contributions.
    if (provenanceService_m) {
        provenanceService_m->finalize(df);
    }
}

Analyzer *Analyzer::run() {
    if (eventLoopTriggered_m) {
        throw std::runtime_error(
            "Analyzer::run() called twice — the event loop would be "
            "re-triggered. Create a new Analyzer instance for a second run.");
    }
    eventLoopTriggered_m = true;

    auto df = dataFrameProvider_m->getDataFrame();

    // Pre-execution hook
    for (const auto& role : pluginOrder_m) {
        auto it = plugins.find(role);
        if (it != plugins.end() && it->second) {
            it->second->execute();
        }
    }

    // Conditionally write a skim when enableSkim=1/true/True is set in config
    const auto& cfgMap = configProvider_m->getConfigMap();
    const auto skimIt = cfgMap.find("enableSkim");
    if (skimIt != cfgMap.end()) {
        const auto& val = skimIt->second;
        if (val == "1" || val == "true" || val == "True") {
            skimSink_m->writeDataFrame(df,
                                       *configProvider_m,
                                       systematicManager_m.get(),
                                       OutputChannel::Skim);
        }
    }

    // Save ND histograms (uses internally tracked histInfo / regionNames)
    if (auto histogramManager = getPlugin<NDHistogramManager>("histogramManager")) {
        histogramManager->saveHists();
    }

    // Finalize all non-provenance services (e.g. CounterService).
    // ProvenanceService is finalized last so it captures contributions from
    // plugins and other services.
    // Note: ProvenanceService is NOT in services_m (owned via shared_ptr).
    for (auto& service : services_m) {
        service->finalize(df);
    }

    // Post-execution plugin hooks
    for (const auto& role : pluginOrder_m) {
        auto it = plugins.find(role);
        if (it != plugins.end() && it->second) {
            it->second->finalize();
        }
    }
    for (const auto& role : pluginOrder_m) {
        auto it = plugins.find(role);
        if (it != plugins.end() && it->second) {
            it->second->reportMetadata();
        }
    }

    // Collect structured provenance contributions from plugins and services,
    // then finalize ProvenanceService so it writes all collected entries.
    collectAndRegisterProvenance(df);

    return this;
}

std::vector<std::string> Analyzer::GetColumnNames() {
    return dataFrameProvider_m->getDataFrame().GetColumnNames();
}

bool Analyzer::HasColumn(const std::string& name) {
    const auto names = GetColumnNames();
    return std::find(names.begin(), names.end(), name) != names.end();
}

std::string Analyzer::GetColumnType(const std::string& name) {
    return dataFrameProvider_m->getDataFrame().GetColumnType(name);
}

std::string Analyzer::FirstAvailableColumn(const std::vector<std::string>& candidates) {
    for (const auto& c : candidates) {
        if (!c.empty() && HasColumn(c)) {
            return c;
        }
    }
    return {};
}

ROOT::RDF::RNode Analyzer::getDataFrameUnsafe() {
    std::cerr << "WARNING [Analyzer::getDataFrameUnsafe()]: Returning raw RNode. "
              << "This bypasses systematic expansion, provenance tracking, and "
              << "the standard lifecycle. Only use this for advanced operations "
              << "that the Analyzer API (Define/Filter/run) does not support."
              << std::endl;
    return dataFrameProvider_m->getDataFrame();
}

ROOT::RDF::RNode Analyzer::getDF() {
    // No warning — for internal (Analyzer) use only.
    return dataFrameProvider_m->getDataFrame();
}

std::string Analyzer::configMap(std::string key) {
    return configProvider_m->get(key);
}

 