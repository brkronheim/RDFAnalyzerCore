#include <ProvenanceService.h>
#include <api/IConfigurationProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>

#include <GitVersion.h>
#include <ExtensionConfig.h>
#include <RootRegistryOptimization.h>

#include <TDirectory.h>
#include <TFile.h>
#include <TMD5.h>
#include <TObjString.h>
#include <TObject.h>
#include <RVersion.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <system_error>
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
    namespace fs = std::filesystem;

    std::error_code ec;
    const fs::path filePath(path);
    const fs::file_status status = fs::status(filePath, ec);

    std::ostringstream fingerprint;
    fingerprint << "path=" << path << '\n';

    if (!ec && fs::exists(status) && fs::is_regular_file(status)) {
        const auto fileSize = fs::file_size(filePath, ec);
        if (!ec) {
            fingerprint << "size=" << fileSize << '\n';
        }

        ec.clear();
        const auto writeTime = fs::last_write_time(filePath, ec);
        if (!ec) {
            fingerprint << "mtime=" << writeTime.time_since_epoch().count() << '\n';
        }

        fingerprint << "kind=local_regular_file";
        return hashString(fingerprint.str());
    }

    // Remote URIs and non-stat'able paths still need a stable provenance token
    // without forcing any I/O against potentially large or inaccessible files.
    fingerprint << "kind=path_only";
    return hashString(fingerprint.str());
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
        throw std::runtime_error("ProvenanceService: failed to open meta output file: " + fileName);
    }

    const auto registry = rdf::rootRegistryOptimizationStatus();
    provenance_m["root.registry_optimization"] = registry.implementation;
    provenance_m["root.registry_definitions"] = std::to_string(registry.definitions);
    provenance_m["root.registry_input_bindings_before"] = std::to_string(registry.inputBindingsBefore);
    provenance_m["root.registry_input_bindings_after"] = std::to_string(registry.inputBindingsAfter);

    // One compressed, versioned object. Definitions and measured diagnostics
    // are contributed by plugins, so no plugin needs to reopen this file.
    std::map<std::string, std::string> sorted(provenance_m.begin(), provenance_m.end());
    std::ostringstream document;
    document << "{\"schema_version\":1,\"format\":\"rdf-job-metadata\",\"provenance\":"
             << rdf::metadataJsonMap(sorted)
             << ",\"regions\":" << rdf::metadataJsonMap(outputMetadata_m.regions)
             << ",\"normalizations\":{";
    bool first = true;
    for (const auto& [key, value] : outputMetadata_m.normalizations) {
        if (!first) document << ',';
        first = false;
        document << rdf::metadataJsonString(key) << ':' << rdf::metadataJsonNumber(value);
    }
    document << "},\"weight_audits\":{";
    first = true;
    for (const auto& [key, value] : outputMetadata_m.weightAudits) {
        if (!first) document << ',';
        first = false;
        document << rdf::metadataJsonString(key) << ":{\"n\":" << value.count
                 << ",\"sum_w\":" << rdf::metadataJsonNumber(value.sum)
                 << ",\"min_w\":" << (value.count ? rdf::metadataJsonNumber(value.minimum) : "null")
                 << ",\"max_w\":" << (value.count ? rdf::metadataJsonNumber(value.maximum) : "null")
                 << ",\"n_negative\":" << value.negativeCount
                 << ",\"n_zero\":" << value.zeroCount << '}';
    }
    document << "}}";
    outFile.cd();
    // A reused file must not advertise both the old and new provenance formats.
    outFile.Delete("provenance;*");
    TObjString entry(document.str().c_str());
    if (entry.Write(rdf::jobMetadataKey, TObject::kOverwrite) <= 0)
        throw std::runtime_error("Failed to write job metadata to " + fileName);

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

void ProvenanceService::removeEntry(const std::string& key) {
    provenance_m.erase(key);
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
    std::string enabledExtensions;
    for (const auto &extension : rdf::extensionBuildInfo) {
        const auto prefix = std::string("extensions.") + extension.name;
        provenance_m[prefix + ".enabled"] = extension.enabled ? "true" : "false";
        if (extension.enabled) {
            if (!enabledExtensions.empty()) enabledExtensions += ";";
            enabledExtensions += extension.name;
            provenance_m[prefix + ".git_hash"] = extension.revision;
            provenance_m[prefix + ".git_dirty"] = extension.dirty ? "true" : "false";
        }
    }
    provenance_m["extensions.enabled"] = enabledExtensions;

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
    provenance_m["analysis.git_dirty"] = analysisHash.empty() ? "unknown" : (gitStatus.empty() ? "false" : "true");

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
    auto setting = [&](const char* key) {
        auto it = configMap.find(key);
        return it == configMap.end() ? std::string() : it->second;
    };
    std::string logicalOutput = setting("__orig_metaFile");
    if (logicalOutput.empty()) logicalOutput = setting("metaFile");
    if (logicalOutput.empty()) logicalOutput = setting("saveFile");
    if (!logicalOutput.empty() && logicalOutput.find("://") == std::string::npos)
        logicalOutput = std::filesystem::absolute(logicalOutput).lexically_normal().string();
    if (!setting("metadataJobId").empty())
        provenance_m["job.logical_id"] = setting("metadataJobId");
    else if (!logicalOutput.empty())
        provenance_m["job.logical_id"] = hashString(logicalOutput);
    if (!setting("metadataAttemptId").empty())
        provenance_m["job.attempt_id"] = setting("metadataAttemptId");
    if (!setting("runBundleId").empty())
        provenance_m["run.bundle_id"] = setting("runBundleId");
    if (!setting("metadataExecutionId").empty())
        provenance_m["run.execution_id"] = setting("metadataExecutionId");
    if (!setting("metadataExecutableSha256").empty())
        provenance_m["run.expected_executable_sha256"] = setting("metadataExecutableSha256");
    provenance_m["service.counter.sample"] = setting("sample");
    if (provenance_m["service.counter.sample"].empty())
        provenance_m["service.counter.sample"] = setting("name");
    if (provenance_m["service.counter.sample"].empty())
        provenance_m["service.counter.sample"] = "unknown";
    provenance_m["config.hash"] = hashString(serializeConfigMap(configMap));
    auto semanticConfig = configMap;
    for (const char* key : {"saveFile", "metaFile", "__orig_saveFile", "__orig_metaFile",
                            "saveDirectory", "fileList", "firstEntry", "lastEntry",
                            "metadataJobId", "metadataAttemptId"})
        semanticConfig.erase(key);
    provenance_m["config.semantic_hash"] = hashString(serializeConfigMap(semanticConfig));

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
            const auto fingerprint = hashFile(value);
            provenance_m["file.reference_fingerprint." + key] = fingerprint;
            provenance_m["file.hash." + key] = fingerprint; // legacy alias
        }
    }
}
