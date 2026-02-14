# SetupYamlCpp.cmake
# Downloads and sets up yaml-cpp for use in the project

# Use FetchContent to download and build yaml-cpp
include(FetchContent)

# yaml-cpp version
set(YAMLCPP_VERSION "0.8.0")

message(STATUS "Configuring yaml-cpp ${YAMLCPP_VERSION}...")

FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG ${YAMLCPP_VERSION}
    GIT_SHALLOW TRUE
)

# Configure yaml-cpp build options
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "Disable yaml-cpp tests")
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "Disable yaml-cpp tools")
set(YAML_CPP_BUILD_CONTRIB OFF CACHE BOOL "Disable yaml-cpp contrib")
set(YAML_CPP_INSTALL OFF CACHE BOOL "Disable yaml-cpp install")
set(YAML_BUILD_SHARED_LIBS OFF CACHE BOOL "Build yaml-cpp as static library")

# Make yaml-cpp available
FetchContent_MakeAvailable(yaml-cpp)

# Verify the target was created
if(NOT TARGET yaml-cpp)
    message(FATAL_ERROR "yaml-cpp target not created after FetchContent_MakeAvailable")
endif()

message(STATUS "yaml-cpp configured successfully")
