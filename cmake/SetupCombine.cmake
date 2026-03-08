# SetupCombine.cmake
# CMake module that provides optional downloading and building of
# the CMS Combine package and the CombineHarvester tools.
# This module is only included when the top-level CMakeLists.txt sees
# BUILD_COMBINE or BUILD_COMBINE_HARVESTER set to ON.

include(ExternalProject)

# Users can override the git tags/branches if desired.
set(COMBINE_GIT_TAG "v10.5.1" CACHE STRING "Git tag or branch to checkout for Combine")
set(COMBINEHARV_GIT_TAG "v3.0.0" CACHE STRING "Git tag or branch to checkout for CombineHarvester")

# Validate options
if(BUILD_COMBINE_HARVESTER AND NOT BUILD_COMBINE)
    message(WARNING "BUILD_COMBINE_HARVESTER is ON but BUILD_COMBINE is OFF; disabling CombineHarvester build.")
    set(BUILD_COMBINE_HARVESTER OFF CACHE BOOL "Build CombineHarvester tools" FORCE)
endif()

# Combine upstream requires Eigen3; make sure we can find it and
# pass the location through to the external project.
# If the system doesn't provide Eigen3 we fall back to downloading a copy
# using FetchContent so the build is self-contained.
find_package(Eigen3 QUIET)
if(NOT Eigen3_FOUND)
    message(STATUS "Eigen3 not found on system - fetching a copy via FetchContent")
    include(FetchContent)
    set(BUILD_TESTING OFF CACHE BOOL "Disable Eigen test targets in fetched dependency" FORCE)
    set(EIGEN_BUILD_DOC OFF CACHE BOOL "Disable Eigen documentation targets in fetched dependency" FORCE)
    set(EIGEN_BUILD_BTL OFF CACHE BOOL "Disable Eigen benchmark targets in fetched dependency" FORCE)
    set(EIGEN_BUILD_PKGCONFIG OFF CACHE BOOL "Disable Eigen pkg-config generation in fetched dependency" FORCE)
    FetchContent_Declare(
        eigen
        GIT_REPOSITORY https://gitlab.com/libeigen/eigen.git
        GIT_TAG 3.4.0
    )
    FetchContent_MakeAvailable(eigen)
    find_package(Eigen3 REQUIRED)
endif()

if(NOT BUILD_COMBINE)
    # nothing to do
    return()
endif()

message(STATUS "Configuring external projects for CMS Combine")

# Directory prefixes
set(_combine_prefix "${CMAKE_BINARY_DIR}/external/HiggsAnalysis/CombinedLimit")
set(_ch_prefix "${CMAKE_BINARY_DIR}/external/CombineHarvester")
set(_combineharvester_src_dir "${_ch_prefix}/src/CombineHarvester")
set(_combineharvester_helper_dir "${CMAKE_SOURCE_DIR}/cmake/CombineHarvesterStandalone")

# Common CMake arguments for the external projects
# We install into the same prefix to make it easier to locate binaries.
set(_common_args
    -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DEigen3_DIR=${Eigen3_DIR}
    -DBUILD_TESTS=OFF
    -DBUILD_TESTING=OFF
)

ExternalProject_Add(CombineTool
    GIT_REPOSITORY https://github.com/cms-analysis/HiggsAnalysis-CombinedLimit.git
    GIT_TAG ${COMBINE_GIT_TAG}
    PREFIX "${_combine_prefix}"
    CMAKE_ARGS ${_common_args}
    UPDATE_COMMAND ""
    TEST_COMMAND ""
)

# Expose the path to the combine executable for other projects
set(COMBINE_EXECUTABLE "${_combine_prefix}/bin/combine" CACHE FILEPATH "Path to built combine executable")
add_custom_target(CombineToolExecutable
    DEPENDS "${COMBINE_EXECUTABLE}"
)

set(COMBINE_PREFIX "${_combine_prefix}" CACHE PATH "Install prefix containing the standalone Combine checkout")
set(COMBINE_BIN_DIR "${_combine_prefix}/bin" CACHE PATH "Directory containing Combine executables")
set(COMBINE_LIBRARY_DIR "${_combine_prefix}/lib" CACHE PATH "Directory containing Combine libraries")
set(COMBINE_PYTHON_DIR "${_combine_prefix}/python" CACHE PATH "Directory containing Combine Python modules")

# Optionally build CombineHarvester after Combine is ready
if(BUILD_COMBINE_HARVESTER)
    message(STATUS "Configuring standalone build for CombineHarvester (depends on Combine)")

    set(COMBINEHARVESTER_ROOT_DIR "${_ch_prefix}" CACHE PATH "Root directory containing the installed CombineHarvester checkout and binaries" FORCE)
    set(COMBINEHARVESTER_TOOLS_DIR "${_ch_prefix}/CombineTools/bin" CACHE PATH "Directory containing CombineHarvester tools" FORCE)
    set(COMBINEHARVESTER_PYTHON_DIR "${_ch_prefix}/CombineTools/python" CACHE PATH "Directory containing CombineHarvester Python modules" FORCE)
    set(COMBINEHARVESTER_PDFS_DIR "${_ch_prefix}/CombinePdfs" CACHE PATH "Directory containing CombineHarvester PDF helpers" FORCE)
    set(COMBINEHARVESTER_LIBRARY_DIR "${_ch_prefix}/lib" CACHE PATH "Directory containing built CombineHarvester libraries" FORCE)

    ExternalProject_Add(CombineHarvester
        GIT_REPOSITORY https://github.com/cms-analysis/CombineHarvester.git
        GIT_TAG ${COMBINEHARV_GIT_TAG}
        PREFIX "${_ch_prefix}"
        DEPENDS CombineTool
        PATCH_COMMAND ${CMAKE_COMMAND} -DCH_SOURCE_DIR=<SOURCE_DIR> -P ${CMAKE_SOURCE_DIR}/cmake/PatchCombineHarvesterForRoot.cmake
        CONFIGURE_COMMAND ${CMAKE_COMMAND}
            -S ${_combineharvester_helper_dir}
            -B <BINARY_DIR>
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DCOMBINEHARVESTER_SOURCE_DIR=<SOURCE_DIR>
            -DCOMBINE_PREFIX=${_combine_prefix}
            -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
        BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --config ${CMAKE_BUILD_TYPE}
        INSTALL_COMMAND ${CMAKE_COMMAND} --install <BINARY_DIR> --config ${CMAKE_BUILD_TYPE}
        UPDATE_COMMAND ""
        TEST_COMMAND ""
    )

    # Provide convenient variables for tools and scripts.
    add_custom_target(CombineHarvesterTools
        DEPENDS CombineHarvester
    )
endif()
