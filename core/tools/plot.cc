#include <GitVersion.h>
#include <PlottingUtility.h>
#include <RVersion.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <spawn.h>
#include <sstream>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>

extern char **environ;

namespace {
YAML::Node provenanceFor(const std::string &config) {
  YAML::Node info;
  info["framework_hash"] = RDFANALYZER_GIT_HASH;
  info["framework_dirty"] = RDFANALYZER_GIT_DIRTY;
  info["root_version"] = ROOT_RELEASE;
  info["compiler"] = RDFANALYZER_COMPILER;
  // Deterministic content identity; this is not a cryptographic signature.
  std::ifstream stream(config, std::ios::binary);
  std::uint64_t hash = 14695981039346656037ULL;
  for (char ch; stream.get(ch);) {
    hash ^= static_cast<unsigned char>(ch);
    hash *= 1099511628211ULL;
  }
  std::ostringstream fingerprint;
  fingerprint << "fnv1a64:" << std::hex << std::setw(16) << std::setfill('0')
              << hash;
  info["configuration_fingerprint"] = fingerprint.str();
  return info;
}
void writeReport(const std::string &path, const std::string &text) {
  if (path.empty())
    return;
  const auto parent = std::filesystem::path(path).parent_path();
  if (!parent.empty())
    std::filesystem::create_directories(parent);
  std::ofstream output(path);
  output << text << '\n';
  if (!output)
    throw std::runtime_error("Unable to write report: " + path);
}

// Fresh executable launches, not forked ROOT sessions. Each child renders a
// shard serially and keeps its session alive for the complete shard.
int renderProcesses(const std::string &executable, const std::string &config,
                    const std::string &report, const PlotBatchOptions &options,
                    unsigned repeat, unsigned count,
                    const std::vector<PlotRequest> &requests) {
  if (count > 32)
    throw std::invalid_argument("At most 32 renderer processes are supported");
  std::set<std::string> outputs;
  for (const auto &request : requests) {
    auto paths = request.additionalOutputs;
    paths.push_back(request.outputFile);
    for (const auto &path : paths)
      if (!outputs
               .insert(
                   std::filesystem::absolute(path).lexically_normal().string())
               .second)
        throw std::invalid_argument(
            "Renderer processes require unique output paths: " + path);
  }
  count = std::min<std::size_t>(count, requests.size());
  if (!count)
    return 0;
  std::string pattern =
      (std::filesystem::temp_directory_path() / "rdf-plot-XXXXXX").string();
  std::vector<char> buffer(pattern.begin(), pattern.end());
  buffer.push_back('\0');
  const char *created = mkdtemp(buffer.data());
  if (!created)
    throw std::runtime_error("Cannot create plotting worker directory");
  struct Cleanup {
    std::filesystem::path directory;
    std::vector<pid_t> children;
    ~Cleanup() {
      for (auto pid : children)
        if (pid > 0) {
          kill(pid, SIGTERM);
          int status;
          while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
          }
        }
      std::error_code error;
      std::filesystem::remove_all(directory, error);
    }
  } cleanup{created, {}};
  auto document = YAML::LoadFile(config);
  YAML::Node nodes = document.IsMap() ? document["plots"] : document;
  std::vector<std::vector<std::size_t>> indices(count);
  std::vector<std::string> reports;
  const auto start = std::chrono::steady_clock::now();
  for (unsigned worker = 0; worker < count; ++worker) {
    YAML::Emitter shard;
    shard << YAML::BeginSeq;
    for (std::size_t i = worker; i < requests.size(); i += count) {
      indices[worker].push_back(i);
      shard << nodes[i];
    }
    shard << YAML::EndSeq;
    auto base = cleanup.directory / std::to_string(worker);
    const auto input = base.string() + ".yaml",
               output = base.string() + ".report.yaml";
    reports.push_back(output);
    writeReport(input, shard.c_str());
    std::vector<std::string> args{executable,
                                  "--config",
                                  input,
                                  "--workers",
                                  std::to_string(options.workers),
                                  "--repeat",
                                  std::to_string(repeat),
                                  "--report",
                                  output};
    std::vector<char *> argv;
    for (auto &arg : args)
      argv.push_back(arg.data());
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions))
      throw std::runtime_error("Cannot initialize plotting worker");
    struct Actions {
      posix_spawn_file_actions_t &value;
      ~Actions() { posix_spawn_file_actions_destroy(&value); }
    } actionCleanup{actions};
    const auto log = base.string() + ".log";
    if (posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log.c_str(),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0600) ||
        posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO,
                                         STDERR_FILENO))
      throw std::runtime_error("Cannot redirect plotting worker output");
    pid_t pid;
    const int error = posix_spawnp(&pid, executable.c_str(), &actions, nullptr,
                                   argv.data(), environ);
    if (error)
      throw std::runtime_error("Cannot start renderer process (error " +
                               std::to_string(error) + ")");
    cleanup.children.push_back(pid);
  }
  bool success = true;
  YAML::Node combined;
  combined["schema_version"] = 1;
  combined["config"] = std::filesystem::absolute(config).string();
  combined["provenance"] = provenanceFor(config);
  combined["renderProcesses"] = count;
  std::vector<YAML::Node> results(repeat * requests.size());
  for (unsigned worker = 0; worker < count; ++worker) {
    int status = 0;
    pid_t waited;
    do {
      waited = waitpid(cleanup.children[worker], &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited < 0)
      throw std::runtime_error("Cannot wait for renderer process");
    cleanup.children[worker] = 0;
    success &= WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (!std::filesystem::exists(reports[worker])) {
      success = false;
      for (unsigned run = 0; run < repeat; ++run)
        for (auto index : indices[worker]) {
          results[run * requests.size() + index]["success"] = false;
          results[run * requests.size() + index]["message"] =
              "Renderer process failed before producing a report";
        }
      continue;
    }
    auto child = YAML::LoadFile(reports[worker]);
    for (unsigned run = 0; run < repeat; ++run)
      for (std::size_t i = 0; i < indices[worker].size(); ++i)
        results[run * requests.size() + indices[worker][i]].reset(
            child["results"][run * indices[worker].size() + i]);
    for (const auto &entry : child["stats"]) {
      const auto key = entry.first.as<std::string>();
      const auto previous = combined["stats"][key];
      combined["stats"][key] =
          (previous ? previous.as<double>() : 0) + entry.second.as<double>();
    }
  }
  for (const auto &result : results)
    combined["results"].push_back(result);
  combined["wallSeconds"] =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count();
  YAML::Emitter provenance;
  provenance << combined;
  writeReport(report, provenance.c_str());
  std::cout << requests.size() * repeat << " requests in "
            << combined["wallSeconds"].as<double>() << " s across " << count
            << " renderer processes\n";
  if (!success)
    std::cerr << "One or more renderer processes failed; inspect the report "
                 "for per-request status\n";
  return success ? 0 : 1;
}
} // namespace

int main(int argc, char **argv) {
  try {
    std::string config, report;
    PlotBatchOptions options;
    unsigned repeat = 1, processes = 1;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--help") {
        std::cout << "rdf-plot --config plots.yaml [--workers N] [--report "
                     "report.yaml] [--repeat N] [--render-processes N]\n";
        return 0;
      }
      if (i + 1 == argc)
        throw std::invalid_argument("Missing value for " + flag);
      const std::string value = argv[++i];
      if (flag == "--config")
        config = value;
      else if (flag == "--report")
        report = value;
      else if (flag == "--workers")
        options.workers = std::stoul(value);
      else if (flag == "--repeat")
        repeat = std::stoul(value);
      else if (flag == "--render-processes")
        processes = std::stoul(value);
      else
        throw std::invalid_argument("Unknown argument: " + flag);
    }
    if (config.empty() || !repeat || !processes)
      throw std::invalid_argument(
          "Specify --config and a positive repeat count");
    auto requests = PlottingUtility::readRequests(config);
    if (processes > 1)
      return renderProcesses(argv[0], config, report, options, repeat,
                             processes, requests);
    PlottingUtility::Session session(options);
    bool success = true;
    YAML::Emitter provenance;
    provenance << YAML::BeginMap << YAML::Key << "schema_version" << YAML::Value
               << 1 << YAML::Key << "config" << YAML::Value
               << std::filesystem::absolute(config).string() << YAML::Key
               << "provenance" << YAML::Value << provenanceFor(config)
               << YAML::Key << "results" << YAML::Value << YAML::BeginSeq;
    for (unsigned run = 0; run < repeat; ++run)
      for (const auto &result : session.renderBatch(requests)) {
        success &= result.success;
        provenance << YAML::BeginMap << YAML::Key << "success" << YAML::Value
                   << result.success << YAML::Key << "message" << YAML::Value
                   << result.message << YAML::Key << "outputs" << YAML::Value
                   << result.outputs << YAML::Key << "mcIntegral" << YAML::Value
                   << result.mcIntegral << YAML::Key << "mcVariance"
                   << YAML::Value << result.mcVariance << YAML::Key
                   << "dataIntegral" << YAML::Value << result.dataIntegral
                   << YAML::Key << "warnings" << YAML::Value << result.warnings
                   << YAML::EndMap;
        if (!result.success)
          std::cerr << result.message << '\n';
      }
    const auto stats = session.stats();
    provenance << YAML::EndSeq << YAML::Key << "stats" << YAML::Value
               << YAML::BeginMap;
#define STAT(name) provenance << YAML::Key << #name << YAML::Value << stats.name
    STAT(plots);
    STAT(workspaces);
    STAT(drawableObjects);
    STAT(bufferGrowths);
    STAT(fileOpens);
    STAT(preparationSeconds);
    STAT(renderingSeconds);
    STAT(exportSeconds);
#undef STAT
    provenance << YAML::EndMap << YAML::EndMap;
    writeReport(report, provenance.c_str());
    std::cout << stats.plots << " plots; " << stats.workspaces
              << " reusable layouts; " << stats.preparationSeconds
              << " s preparation, " << stats.renderingSeconds << " s layout, "
              << stats.exportSeconds << " s paint/export\n";
    return success ? 0 : 1;
  } catch (const std::exception &error) {
    std::cerr << "rdf-plot: " << error.what() << '\n';
    return 1;
  }
}
