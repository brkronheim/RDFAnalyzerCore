#ifndef TEST_UTIL_H
#define TEST_UTIL_H

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

/**
 * @brief Change to the test source directory using TEST_SOURCE_DIR macro.
 *        Creates minimal test input dirs/files if they are missing to make
 *        tests robust when run in a clean environment.
 *        Fails the test if the directory change is unsuccessful.
 */
inline void ChangeToTestSourceDir() {
    EnsureConsistentRootTestEnvironment();

    if (chdir(TEST_SOURCE_DIR) != 0) {
        FAIL() << "Failed to change directory to " << TEST_SOURCE_DIR;
    }

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