#include <RegionManager.h>
#include <NullOutputSink.h>
#include <TFile.h>
#include <TNamed.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

// ---------------------------------------------------------------------------
// Helper: apply a boolean-column filter
// ---------------------------------------------------------------------------
namespace {
bool passRegionFilter(bool pass) { return pass; }
} // anonymous namespace

// ---------------------------------------------------------------------------
// RegionManager implementation
// ---------------------------------------------------------------------------

void RegionManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  logger_m = &ctx.logger;
  metaSink_m = &ctx.metaSink;
}

void RegionManager::declareRegion(const std::string &name,
                                   const std::string &filterColumn,
                                   const std::string &parent) {
  if (!dataManager_m) {
    throw std::runtime_error(
        "RegionManager::declareRegion(): context not set. "
        "Call setContext() before declareRegion().");
  }
  if (name.empty()) {
    throw std::runtime_error(
        "RegionManager::declareRegion(): region name must not be empty.");
  }
  if (filterColumn.empty()) {
    throw std::runtime_error(
        "RegionManager::declareRegion(): filterColumn for region '" + name +
        "' must not be empty.");
  }
  if (regions_m.count(name)) {
    throw std::runtime_error(
        "RegionManager::declareRegion(): region '" + name +
        "' is already declared.");
  }

  // Capture the base DF on the first call so all root regions share the
  // same starting node (including any global preselection applied before
  // the first declareRegion() call).
  if (!baseCaptured_m) {
    baseDf_m = dataManager_m->getDataFrame();
    baseCaptured_m = true;
  }

  // Determine the starting node for this region.
  ROOT::RDF::RNode startNode =
      parent.empty() ? baseDf_m : [&]() -> ROOT::RDF::RNode {
        auto it = regions_m.find(parent);
        if (it == regions_m.end()) {
          throw std::runtime_error(
              "RegionManager::declareRegion(): parent region '" + parent +
              "' has not been declared. Declare parent regions before children.");
        }
        return it->second.dfNode;
      }();

  // Build the filtered node for this region.
  ROOT::RDF::RNode filteredNode =
      startNode.Filter(passRegionFilter, {filterColumn});

  regionOrder_m.push_back(name);
  regions_m.emplace(name, RegionEntry{name, filterColumn, parent, filteredNode});
}

ROOT::RDF::RNode
RegionManager::getRegionDataFrame(const std::string &name) const {
  auto it = regions_m.find(name);
  if (it == regions_m.end()) {
    throw std::runtime_error(
        "RegionManager::getRegionDataFrame(): region '" + name +
        "' has not been declared.");
  }
  return it->second.dfNode;
}

const std::vector<std::string> &RegionManager::getRegionNames() const {
  return regionOrder_m;
}

std::vector<std::string> RegionManager::validate() const {
  std::vector<std::string> errors;

  for (const auto &name : regionOrder_m) {
    const auto &entry = regions_m.at(name);

    if (entry.name.empty()) {
      errors.push_back("Region has an empty name.");
    }
    if (entry.filterColumn.empty()) {
      errors.push_back("Region '" + name + "' has an empty filterColumn.");
    }

    // Check that declared parent exists (should be caught by declareRegion,
    // but we validate here as well for completeness).
    if (!entry.parent.empty() && !regions_m.count(entry.parent)) {
      errors.push_back("Region '" + name + "' declares parent '" +
                       entry.parent + "' which does not exist.");
    }
  }

  // Cycle detection using DFS colouring (white=0, grey=1, black=2).
  std::unordered_map<std::string, int> colour;
  for (const auto &name : regionOrder_m) {
    colour[name] = 0;
  }

  std::function<bool(const std::string &, std::vector<std::string> &)>
      hasCycle = [&](const std::string &node,
                     std::vector<std::string> &path) -> bool {
    colour[node] = 1; // grey = in current DFS path
    path.push_back(node);
    const auto &entry = regions_m.at(node);
    if (!entry.parent.empty() && regions_m.count(entry.parent)) {
      const auto &parentName = entry.parent;
      if (colour[parentName] == 1) {
        // Found a cycle: report the cycle members.
        std::ostringstream ss;
        ss << "Cycle detected in region hierarchy: ";
        for (const auto &p : path) {
          ss << p << " -> ";
        }
        ss << parentName;
        errors.push_back(ss.str());
        path.pop_back();
        colour[node] = 2;
        return true;
      }
      if (colour[parentName] == 0) {
        if (hasCycle(parentName, path)) {
          path.pop_back();
          colour[node] = 2;
          return true;
        }
      }
    }
    path.pop_back();
    colour[node] = 2; // black = fully explored
    return false;
  };

  for (const auto &name : regionOrder_m) {
    if (colour[name] == 0) {
      std::vector<std::string> path;
      hasCycle(name, path);
    }
  }

  return errors;
}

void RegionManager::initialize() {
  auto errors = validate();
  if (!errors.empty()) {
    std::ostringstream ss;
    ss << "RegionManager: region hierarchy validation failed:\n";
    for (const auto &e : errors) {
      ss << "  - " << e << "\n";
    }
    throw std::runtime_error(ss.str());
  }
}

void RegionManager::finalize() {
  if (regionOrder_m.empty()) return;

  // Re-validate to catch any issues introduced after initialize().
  auto errors = validate();
  if (!errors.empty() && logger_m) {
    for (const auto &e : errors) {
      logger_m->log(ILogger::Level::Error,
                    "RegionManager (finalize): " + e);
    }
  }

  if (dynamic_cast<NullOutputSink *>(metaSink_m) != nullptr) return;

  std::string fileName =
      metaSink_m->resolveOutputFile(*configManager_m, OutputChannel::Meta);
  if (fileName.empty()) return;

  TFile outFile(fileName.c_str(), "UPDATE");
  if (outFile.IsZombie()) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Error,
                    "RegionManager: failed to open meta output file: " +
                        fileName);
    }
    return;
  }

  // Write each region as a TNamed of the form
  //   name → "<filter_column>[:<parent>]"
  // stored in a "regions" TDirectory.
  TDirectory *regDir = outFile.GetDirectory("regions");
  if (!regDir) {
    regDir = outFile.mkdir("regions");
  }
  regDir->cd();

  for (const auto &name : regionOrder_m) {
    const auto &entry = regions_m.at(name);
    std::string value = entry.filterColumn;
    if (!entry.parent.empty()) {
      value += ":" + entry.parent;
    }
    TNamed obj(name.c_str(), value.c_str());
    obj.Write(name.c_str(), TObject::kOverwrite);
  }

  outFile.Close();
}

void RegionManager::reportMetadata() {
  if (regionOrder_m.empty() || !logger_m) return;

  std::ostringstream ss;
  ss << "RegionManager: declared regions (" << regionOrder_m.size() << ")\n";
  for (const auto &name : regionOrder_m) {
    const auto &entry = regions_m.at(name);
    ss << "  " << name
       << "  filter=" << entry.filterColumn;
    if (!entry.parent.empty()) {
      ss << "  parent=" << entry.parent;
    }
    ss << "\n";
  }
  logger_m->log(ILogger::Level::Info, ss.str());
}
