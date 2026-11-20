#include <NativePreflight.h>
#include <chrono>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;
using namespace rdf::validation;

namespace {
void require(bool value, const char *message) {
  if (!value) throw std::runtime_error(message);
}

struct Directory {
  fs::path path;
  Directory() {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    for (int attempt = 0; attempt < 100; ++attempt) {
      path = fs::temp_directory_path() / ("rdf-native-preflight-" + std::to_string(stamp) + "-" + std::to_string(attempt));
      if (fs::create_directory(path)) return;
    }
    throw std::runtime_error("Cannot create test directory");
  }
  ~Directory() { std::error_code error; fs::remove_all(path, error); }
};

void selfTest() {
  Directory temp;
  std::ofstream(temp.path / "model.txt") << "abc";
  std::ofstream(temp.path / "empty.txt");
  fs::create_directory(temp.path / "config");
  fs::create_symlink(temp.path / "model.txt", temp.path / "link.txt");
  fs::create_symlink(temp.path / "missing.txt", temp.path / "broken.txt");
  require(checkAssets({{"model.txt", "model", AssetKind::File, 3},
                       {"empty.txt", "empty", AssetKind::File, 0},
                       {"link.txt", "link", AssetKind::File, 3},
                       {"config", "config", AssetKind::Directory, {}}}, temp.path).passed(), "Valid assets rejected");
  const auto bad = checkAssets({{"missing.txt", "missing", AssetKind::File, {}},
                               {"model.txt", "size", AssetKind::File, 4},
                               {"config", "kind", AssetKind::File, {}},
                               {"broken.txt", "broken", AssetKind::File, {}},
                               {"root://example/file", "remote", AssetKind::File, {}},
                               {std::string("bad\0path", 8), "nul", AssetKind::File, {}},
                               {"model.txt", "directory", AssetKind::Directory, 0},
                               {"model.txt", "enum", static_cast<AssetKind>(99), {}}}, temp.path);
  require(bad.issues.size() == 8, "Expected aggregated actionable asset failures");
  require(bad.level == "assets" && bad.supportedLevels == std::vector<std::string>{"assets"}, "Asset capabilities overclaimed");
  require(bad.issues[1].code == "asset.size" && bad.issues[1].location == "size", "Size issue missing location");
  require(checkAssets({{(temp.path / "model.txt").string(), "absolute", AssetKind::File, 3}}, "/unused/base").passed(), "Absolute path rebased");
  require(checkAssets({}, temp.path / "does-not-exist").passed(), "Empty request discovered assets");
  const std::vector<ColumnInfo> available{{"pt", "ROOT::VecOps::RVec<float>"}, {"event", "ULong64_t"}, {"extra", "bool"}};
  require(checkSchema({{"pt", "ROOT::VecOps::RVec<float>", "jets.pt"}, {"event", {}, "event"}}, available).passed(), "Valid schema rejected");
  const auto schema = checkSchema({{"pt", "RVec<float>", "jets.pt"}, {"weight", {}, "weights.nominal"},
                                   {"pt", {}, "duplicate"}, {"", {}, "empty"}}, available);
  require(schema.issues.size() == 4 && schema.issues[0].code == "schema.type", "Exact types or issue aggregation lost");
  require(schema.issues[1].location == "weights.nominal", "Missing column location lost");
  require(!checkSchema({}, {{"pt", "float"}, {"pt", "float"}}).passed(), "Ambiguous available columns accepted");
  require(!checkSchema({{"event", "ULong64_t", {}}}, {{"event", ""}}).passed(), "Unknown type silently accepted");
  require(checkSchema({}, {}).passed(), "Empty schema unexpectedly failed");
  fs::remove(temp.path / "model.txt");
  require(!checkAssets({{"link.txt", {}, AssetKind::File, {}}}, temp.path).passed(), "Revalidation failed to observe missing symlink target");
}
} // namespace

int main(int argc, char **argv) {
  try {
    const std::string mode = argc > 1 ? argv[1] : "self-test";
    if (mode == "self-test") selfTest();
    else if (mode == "schema-report")
      std::cout << checkSchema({{"Jet_pt", "ROOT::VecOps::RVec<float>", "inputs.jets"},
                               {"weight", {}, "inputs.weight"}}, {{"Jet_pt", "double"}}).toJson() << '\n';
    else throw std::invalid_argument("Unknown mode");
    return 0;
  } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
