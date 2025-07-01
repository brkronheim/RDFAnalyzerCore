#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include <gtest/gtest.h>
#include <unistd.h>
#include <string>

/**
 * @brief Change to the test source directory using TEST_SOURCE_DIR macro.
 *        Fails the test if the directory change is unsuccessful.
 */
inline void ChangeToTestSourceDir() {
    if (chdir(TEST_SOURCE_DIR) != 0) {
        FAIL() << "Failed to change directory to " << TEST_SOURCE_DIR;
    }
}

#endif // TEST_UTIL_H 