#include <ProvenanceService.h>
#include <api/IConfigurationProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>

#include <GitVersion.h>

#include <TDirectory.h>
#include <TFile.h>
#include <TMD5.h>
#include <TNamed.h>
#include <TObject.h>
#include <RVersion.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

std::string ProvenanceService::runCommand(const std::string& cmd) {
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        return "";
    }
    std::string result;
    constexpr int kBufSize = 256;
    static_assert(kBufSize > 0, "fgets buffer size must be positive");
    char buf[kBufSize]{};
    while (std::fgets(buf, kBufSize, pipe) != nullptr) {
        result += buf;
    }
    pclose(pipe);
    // Strip trailing newlines / carriage returns
    while (!result.empty() &&
           (result.back() == '\n' || result.back() == '\r' || result.back() == ' ')) {
        result.pop_back();
    }
    return result;
}

std::string ProvenanceService::hashString(const std::string& data) {
    TMD5 md5;
    md5.Update(reinterpret_cast<const UChar_t*>(data.data()),
               static_cast<UInt_t>(data.size()));
    md5.Final();
    return std::string(md5.AsString());
}

std::string ProvenanceService::hashFile(const std::string& path) {
    if (path.empty()) {
        return "<empty path>";
    }
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return "<not found>";
    }
    TMD5 md5;
    std::array<char, 8192> buf{};
    while (file.read(buf.data(), static_cast<std::streamsize>(buf.size()))) {
        md5.Update(reinterpret_cast<const UChar_t*>(buf.data()),
                   static_cast<UInt_t>(file.gcount()));
    }
    if (file.gcount() > 0) {
        md5.Update(reinterpret_cast<const UChar_t*>(buf.data()),
                   static_cast<UInt_t>(file.gcount()));
    }
    md5.Final();
    return std::string(md5.AsString());
}

std::string ProvenanceService::serializeConfigMap(
    const std::unordered_map<std::string, std::string>& configMap) {
    // Sort by key for deterministic output
    std::vector<std::pair<std::string, std::string>> sorted(configMap.begin(),
                                                             configMap.end());
    std::sort(sorted.begin(), sorted.end());
    std::ostringstream oss;
    for (const auto& [k, v] : sorted) {
        oss << k << '=' << v << '\n';
    }
    return oss.str();
}

bool ProvenanceService::looksLikeFilePath(const std::string& value) {
    if (value.empty() || value.front() == '#') {
        return false;
    }
    // Multi-value strings (comma- or space-separated) are not single file paths
    if (value.find(' ') != std::string::npos ||
        value.find(',') != std::string::npos ||
        value.find(';') != std::string::npos) {
        return false;
    }
    static const std::array<const char*, 9> kExts = {
        ".json", ".root", ".onnx", ".bdt", ".pt", ".pb", ".xml", ".yaml", ".yml"};
    for (const char* ext : kExts) {
        const std::string_view sv(ext);
        if (value.size() > sv.size() &&
            value.compare(value.size() - sv.size(), sv.size(), ext) == 0) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// IAnalysisService lifecycle
// ---------------------------------------------------------------------------

void ProvenanceService::initialize(ManagerContext& ctx) {
    ctx_m = &ctx;

    collectBuildInfo();
    collectRuntimeInfo(ctx.config);
}

void ProvenanceService::finalize(ROOT::RDF::RNode& /*df*/) {
    if (!ctx_m) {
        return;
    }

    // Resolve the meta output file path
    const std::string fileName =
        ctx_m->metaSink.resolveOutputFile(ctx_m->config, OutputChannel::Meta);
    if (fileName.empty()) {
        ctx_m->logger.log(ILogger::Level::Warn,
            "ProvenanceService: no meta output file configured – "
            "provenance will not be written.");
        return;
    }

    TFile outFile(fileName.c_str(), "UPDATE");
    if (outFile.IsZombie()) {
        ctx_m->logger.log(ILogger::Level::Error,
            "ProvenanceService: failed to open meta output file: " + fileName);
        return;
    }

    // Create (or retrieve) the "provenance" sub-directory
    TDirectory* provDir = dynamic_cast<TDirectory*>(outFile.Get("provenance"));
    if (!provDir) {
        provDir = outFile.mkdir("provenance", "Provenance metadata");
    }
    if (!provDir) {
        ctx_m->logger.log(ILogger::Level::Error,
            "ProvenanceService: could not create 'provenance' directory in "
            + fileName);
        outFile.Close();
        return;
    }

    provDir->cd();
    for (const auto& [key, value] : provenance_m) {
        TNamed entry(key.c_str(), value.c_str());
        entry.Write(key.c_str(), TObject::kOverwrite);
    }

    outFile.Close();

    ctx_m->logger.log(ILogger::Level::Info,
        "ProvenanceService: wrote " + std::to_string(provenance_m.size()) +
        " provenance entries to " + fileName);
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

void ProvenanceService::addEntry(const std::string& key, const std::string& value) {
    provenance_m[key] = value;
}

void ProvenanceService::recordDatasetManifestProvenance(
    const std::string& manifestFileHash,
    const std::string& queryParamsJson,
    const std::string& resolvedEntries
) {
    provenance_m["dataset_manifest.file_hash"]        = manifestFileHash;
    provenance_m["dataset_manifest.query_params"]     = queryParamsJson;
    provenance_m["dataset_manifest.resolved_entries"] = resolvedEntries;
}

const std::unordered_map<std::string, std::string>&
ProvenanceService::getProvenance() const {
    return provenance_m;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

void ProvenanceService::collectBuildInfo() {
    // Values injected by CMake via GitVersion.h
    provenance_m["framework.git_hash"]        = RDFANALYZER_GIT_HASH;
    provenance_m["framework.git_dirty"]       = RDFANALYZER_GIT_DIRTY ? "true" : "false";
    provenance_m["framework.build_timestamp"] = RDFANALYZER_BUILD_TIMESTAMP;
    provenance_m["framework.compiler"]        = RDFANALYZER_COMPILER;

    // ROOT version is a compile-time macro
    provenance_m["root.version"] = ROOT_RELEASE;
}

void ProvenanceService::collectRuntimeInfo(const IConfigurationProvider& config) {
    // -----------------------------------------------------------------------
    // Analysis repository git info (current working directory)
    // -----------------------------------------------------------------------
    const std::string analysisHash =
        runCommand("git rev-parse HEAD 2>/dev/null");
    provenance_m["analysis.git_hash"] = analysisHash.empty() ? "unknown" : analysisHash;

    const std::string gitStatus =
        runCommand("git status --porcelain 2>/dev/null");
    provenance_m["analysis.git_dirty"] = gitStatus.empty() ? "false" : "true";

    // -----------------------------------------------------------------------
    // Runtime environment / container tag
    // -----------------------------------------------------------------------
    static const std::array<const char*, 4> kEnvVars = {
        "CONTAINER_TAG", "APPTAINER_NAME", "SINGULARITY_NAME", "DOCKER_IMAGE"};
    std::string containerTag;
    for (const char* var : kEnvVars) {
        const char* val = std::getenv(var);
        if (val && *val) {
            containerTag = val;
            break;
        }
    }
    provenance_m["env.container_tag"] = containerTag;

    // -----------------------------------------------------------------------
    // ROOT implicit MT thread pool size
    // -----------------------------------------------------------------------
    provenance_m["executor.num_threads"] =
        std::to_string(ROOT::GetThreadPoolSize());

    // -----------------------------------------------------------------------
    // Configuration hash (deterministic serialisation of the config map)
    // -----------------------------------------------------------------------
    const auto& configMap = config.getConfigMap();
    provenance_m["config.hash"] = hashString(serializeConfigMap(configMap));

    // -----------------------------------------------------------------------
    // File-list hash (the file referenced by "fileList")
    // -----------------------------------------------------------------------
    const auto fileListIt = configMap.find("fileList");
    if (fileListIt != configMap.end() && !fileListIt->second.empty()) {
        provenance_m["filelist.hash"] = hashFile(fileListIt->second);
    }

    // -----------------------------------------------------------------------
    // Hash of any config value that looks like a file path
    // (model files, correction files, etc.)
    // Skip well-known output or already-handled keys.
    // -----------------------------------------------------------------------
    static const std::array<const char*, 4> kSkipKeys = {
        "saveFile", "metaFile", "fileList", "saveConfig"};
    for (const auto& [key, value] : configMap) {
        bool skip = false;
        for (const char* sk : kSkipKeys) {
            if (key == sk) { skip = true; break; }
        }
        if (!skip && looksLikeFilePath(value)) {
            provenance_m["file.hash." + key] = hashFile(value);
        }
    }
}
