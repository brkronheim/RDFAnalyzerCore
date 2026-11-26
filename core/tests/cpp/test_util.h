#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include "test_workspace.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <string>
#include <filesystem>
#include <dlfcn.h>
#include <TFile.h>
#include <TROOT.h>
#include <TTree.h>

inline void EnsureConsistentRootTestEnvironment() {
    Dl_info info;
    if (dladdr(reinterpret_cast<void*>(&TROOT::Class), &info) == 0 || info.dli_fname == nullptr) {
        return;
    }

    std::filesystem::path libCorePath(info.dli_fname);
    std::filesystem::path rootPrefix = libCorePath.parent_path();
    if (!std::filesystem::exists(rootPrefix)) {
        return;
    }

    setenv("ROOTSYS", rootPrefix.c_str(), 1);
}

// Historical helper name retained for existing tests. Relative paths now resolve
// inside a process-private copy of the test fixtures.
inline void ChangeToTestSourceDir() {
    EnsureConsistentRootTestEnvironment();
    std::filesystem::current_path(testWorkspacePath());

    // Ensure minimal test data directories exist and contain a small ROOT file.
    // test_data_minimal gets a single-event file (used by lightweight tests).
    // test_data gets a two-event file so that trigger-logic tests, which expect
    // at least two events to survive after a passing trigger filter, behave
    // correctly.
    auto make_minimal_root = [](const std::string &dir, int nEvents = 1) {
        if (!std::filesystem::exists(dir)) {
            std::filesystem::create_directory(dir);
        }
        std::string path = dir + "/dummy.root";
        if (!std::filesystem::exists(path)) {
            TFile f(path.c_str(), "RECREATE");
            TTree t("Events", "Events");
            int dummy = 1;
            t.Branch("dummy", &dummy, "dummy/I");
            for (int i = 0; i < nEvents; ++i) {
                t.Fill();
            }
            f.Write();
            f.Close();
        }
    };

    make_minimal_root("test_data_minimal", 1);
    make_minimal_root("test_data", 2);
}

#endif // TEST_UTIL_H 