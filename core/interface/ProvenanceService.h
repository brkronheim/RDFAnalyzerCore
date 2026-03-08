#ifndef PROVENANCESERVICE_H_INCLUDED
#define PROVENANCESERVICE_H_INCLUDED

#include "api/IAnalysisService.h"
#include <string>
#include <unordered_map>

/**
 * @brief Analysis service that collects and writes complete provenance metadata.
 *
 * ProvenanceService automatically records the following provenance information
 * into the meta-output ROOT file under a "provenance" TDirectory:
 *
 *  - framework.git_hash      : Git commit SHA of the RDFAnalyzerCore framework
 *                              (captured at CMake configure time).
 *  - framework.git_dirty     : Whether the framework tree had uncommitted changes
 *                              at configure time (true/false).
 *  - framework.build_timestamp : UTC timestamp when the framework was configured.
 *  - framework.compiler      : Compiler ID and version used to build the framework.
 *  - root.version            : ROOT version string (from ROOT_RELEASE macro).
 *  - analysis.git_hash       : Git commit SHA of the analysis repository (queried
 *                              at run time from the current working directory).
 *  - analysis.git_dirty      : Whether the analysis tree had uncommitted changes
 *                              at run time (true/false).
 *  - env.container_tag       : Container / runtime environment tag, taken from
 *                              CONTAINER_TAG, APPTAINER_NAME, SINGULARITY_NAME,
 *                              or DOCKER_IMAGE environment variables
 *                              (empty string if none are set).
 *  - executor.num_threads    : Number of threads in the ROOT implicit MT pool at
 *                              the time finalize() is called (0 = single-threaded).
 *  - config.hash             : MD5 digest of the serialised configuration map
 *                              (all key=value pairs sorted by key).
 *  - filelist.hash           : MD5 digest of the file referenced by the "fileList"
 *                              configuration key, if present.
 *  - plugin.<role>           : Type name of each registered plugin, keyed by
 *                              its role (e.g. plugin.histogramManager).
 *  - file.hash.<cfg_key>     : MD5 digest of any configuration value that looks
 *                              like a file path with a recognised extension
 *                              (.json, .root, .onnx, .bdt, .pt, .pb, .xml).
 *
 * Additional entries can be injected at any time before finalize() via addEntry().
 *
 * All entries are stored as ROOT TNamed objects (name = key, title = value) in
 * a TDirectory named "provenance" inside the meta output file.  They can be read
 * back from Python with either PyROOT or uproot:
 *
 * @code{.py}
 * import ROOT
 * f = ROOT.TFile("output_meta.root")
 * d = f.Get("provenance")
 * for key in d.GetListOfKeys():
 *     print(key.GetName(), "=", key.ReadObj().GetTitle())
 * @endcode
 */
class ProvenanceService : public IAnalysisService {
public:
    void initialize(ManagerContext& ctx) override;
    void finalize(ROOT::RDF::RNode& df) override;

    /**
     * @brief Manually add or overwrite a provenance entry.
     * @param key   Provenance key (e.g. "plugin.myPlugin").
     * @param value Provenance value.
     */
    void addEntry(const std::string& key, const std::string& value);

    /**
     * @brief Read-only access to the collected provenance map.
     */
    const std::unordered_map<std::string, std::string>& getProvenance() const;

private:
    ManagerContext* ctx_m = nullptr;
    std::unordered_map<std::string, std::string> provenance_m;

    void collectBuildInfo();
    void collectRuntimeInfo(const IConfigurationProvider& config);

    /// Compute MD5 hex digest of a file's contents; returns "<not found>" on error.
    static std::string hashFile(const std::string& path);
    /// Compute MD5 hex digest of an arbitrary string.
    static std::string hashString(const std::string& data);
    /// Run a shell command and return trimmed stdout; returns "" on failure.
    static std::string runCommand(const std::string& cmd);
    /// Serialise a config map deterministically (sorted keys) for hashing.
    static std::string serializeConfigMap(
        const std::unordered_map<std::string, std::string>& configMap);
    /// Return true if the value looks like a file path with a known extension.
    static bool looksLikeFilePath(const std::string& value);
};

#endif // PROVENANCESERVICE_H_INCLUDED
