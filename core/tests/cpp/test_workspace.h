#pragma once

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

// Each test process owns its mutable fixtures, including the generated ROOT inputs.
class TestWorkspace {
public:
    TestWorkspace() {
        auto name = (std::filesystem::temp_directory_path() / "rdf-test-XXXXXX").string();
        if (!mkdtemp(name.data())) throw std::runtime_error("Cannot create test workspace " + name);
        path = name;
        try {
            for (const auto* subdir : {"cfg", "aux"}) {
                auto destination = path / subdir;
                std::filesystem::create_directory(destination);
                for (const auto& entry : std::filesystem::directory_iterator(
                         std::filesystem::path(TEST_SOURCE_DIR) / subdir)) {
                    if (entry.is_regular_file()) {
                        auto ext = entry.path().extension();
                        if (ext == ".yaml" || ext == ".yml" || ext == ".txt" ||
                            ext == ".onnx" || ext == ".json")
                            std::filesystem::copy_file(entry.path(), destination / entry.path().filename());
                    }
                }
            }
        } catch (...) {
            std::error_code error;
            std::filesystem::remove_all(path, error);
            throw;
        }
    }
    ~TestWorkspace() {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
    std::filesystem::path path;
};

inline const std::filesystem::path& testWorkspacePath() {
    static TestWorkspace workspace;
    return workspace.path;
}
