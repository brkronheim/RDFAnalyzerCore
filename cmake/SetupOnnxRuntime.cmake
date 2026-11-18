# SetupOnnxRuntime.cmake
# Downloads and sets up ONNX Runtime for use in the project

# Option to download the GPU-enabled (CUDA) build of ONNX Runtime.
# The GPU package includes libonnxruntime_providers_cuda.so and
# libonnxruntime_providers_shared.so required for the CUDA execution provider.
# Only supported on Linux x64.
option(ONNXRUNTIME_USE_CUDA "Download and use the GPU-enabled (CUDA) ONNX Runtime build" OFF)

if(NOT ONNXRUNTIME_ROOT_DIR)
    # Determine the platform
    if(UNIX AND NOT APPLE)
        set(ONNX_PLATFORM "linux")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64")
            set(ONNX_ARCH "x64")
        elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
            set(ONNX_ARCH "aarch64")
        else()
            message(FATAL_ERROR "Unsupported architecture: ${CMAKE_SYSTEM_PROCESSOR}")
        endif()
    elseif(APPLE)
        set(ONNX_PLATFORM "osx")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64")
            set(ONNX_ARCH "x64")
        elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
            set(ONNX_ARCH "arm64")
        else()
            message(FATAL_ERROR "Unsupported architecture: ${CMAKE_SYSTEM_PROCESSOR}")
        endif()
    else()
        message(FATAL_ERROR "Unsupported platform")
    endif()

    # CUDA execution provider is only available in the Linux x64 GPU package
    if(ONNXRUNTIME_USE_CUDA AND NOT (ONNX_PLATFORM STREQUAL "linux" AND ONNX_ARCH STREQUAL "x64"))
        message(FATAL_ERROR "ONNXRUNTIME_USE_CUDA is only supported on Linux x64")
    endif()

    # ONNX Runtime version
    set(ONNXRUNTIME_VERSION "1.24.1")
    
    # Construct download URL and filename.
    # The GPU package (onnxruntime-linux-x64-gpu-<ver>.tgz) ships all CUDA
    # provider libraries in addition to the core library.
    if(ONNXRUNTIME_USE_CUDA)
        set(ONNX_FILENAME "onnxruntime-linux-x64-gpu-${ONNXRUNTIME_VERSION}.tgz")
    elseif(ONNX_PLATFORM STREQUAL "linux")
        set(ONNX_FILENAME "onnxruntime-linux-${ONNX_ARCH}-${ONNXRUNTIME_VERSION}.tgz")
    elseif(ONNX_PLATFORM STREQUAL "osx")
        set(ONNX_FILENAME "onnxruntime-osx-${ONNX_ARCH}-${ONNXRUNTIME_VERSION}.tgz")
    endif()

    # Derive the extracted directory name from the archive name (strip .tgz)
    string(REGEX REPLACE "\\.tgz$" "" ONNX_DIR_NAME "${ONNX_FILENAME}")
    
    set(ONNX_URL "https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/${ONNX_FILENAME}")
    set(ONNXRUNTIME_ROOT_DIR "${CMAKE_BINARY_DIR}/${ONNX_DIR_NAME}")
    
    # Publish only a completely extracted package. A failed configure can retry.
    if(NOT EXISTS "${ONNXRUNTIME_ROOT_DIR}")
        set(_onnx_stage "${CMAKE_BINARY_DIR}/${ONNX_DIR_NAME}-download")
        file(REMOVE_RECURSE "${_onnx_stage}")
        file(MAKE_DIRECTORY "${_onnx_stage}")

        message(STATUS "Downloading ONNX Runtime ${ONNXRUNTIME_VERSION} for ${ONNX_PLATFORM}-${ONNX_ARCH}...")
        file(DOWNLOAD "${ONNX_URL}" "${_onnx_stage}/${ONNX_FILENAME}"
            STATUS _onnx_release_status TLS_VERIFY ON)
        list(GET _onnx_release_status 0 _onnx_release_code)
        if(_onnx_release_code EQUAL 0)
            file(ARCHIVE_EXTRACT INPUT "${_onnx_stage}/${ONNX_FILENAME}"
                DESTINATION "${_onnx_stage}")
        elseif(ONNX_PLATFORM STREQUAL "linux" AND NOT ONNXRUNTIME_USE_CUDA)
            # Microsoft's NuGet distribution contains the same-version native
            # Linux CPU libraries and headers for both supported architectures.
            set(_onnx_nuget_name "microsoft.ml.onnxruntime.${ONNXRUNTIME_VERSION}.nupkg")
            set(_onnx_nuget_url "https://api.nuget.org/v3-flatcontainer/microsoft.ml.onnxruntime/${ONNXRUNTIME_VERSION}/${_onnx_nuget_name}")
            set(_onnx_nuget_archive "${_onnx_stage}/${_onnx_nuget_name}")
            message(STATUS "GitHub download failed (${_onnx_release_status}); trying Microsoft's NuGet package")
            file(DOWNLOAD "${_onnx_nuget_url}" "${_onnx_nuget_archive}"
                STATUS _onnx_nuget_status TLS_VERIFY ON)
            list(GET _onnx_nuget_status 0 _onnx_nuget_code)
            if(NOT _onnx_nuget_code EQUAL 0)
                message(FATAL_ERROR
                    "Could not download ONNX Runtime from ${ONNX_URL} (${_onnx_release_status}) "
                    "or ${_onnx_nuget_url} (${_onnx_nuget_status}). "
                    "Check network access or set ONNXRUNTIME_ROOT_DIR to an extracted ONNX Runtime package.")
            endif()
            # Update this digest together with ONNXRUNTIME_VERSION.
            file(SHA256 "${_onnx_nuget_archive}" _onnx_nuget_sha256)
            if(NOT _onnx_nuget_sha256 STREQUAL "5fca4421974b450711520f17a27e527fec60a78c2b6f9db3b05c159d9b5a346d")
                message(FATAL_ERROR "ONNX Runtime NuGet package SHA-256 does not match the pinned archive")
            endif()
            set(_onnx_nuget_native "runtimes/linux-${ONNX_ARCH}/native")
            file(ARCHIVE_EXTRACT INPUT "${_onnx_nuget_archive}" DESTINATION "${_onnx_stage}"
                PATTERNS "build/native/include/*" "${_onnx_nuget_native}/*"
                    "LICENSE" "ThirdPartyNotices.txt")
            set(_onnx_package "${_onnx_stage}/${ONNX_DIR_NAME}")
            file(COPY "${_onnx_stage}/build/native/include"
                "${_onnx_stage}/LICENSE" "${_onnx_stage}/ThirdPartyNotices.txt"
                DESTINATION "${_onnx_package}")
            file(MAKE_DIRECTORY "${_onnx_package}/lib")
            file(COPY "${_onnx_stage}/${_onnx_nuget_native}/libonnxruntime.so"
                "${_onnx_stage}/${_onnx_nuget_native}/libonnxruntime_providers_shared.so"
                DESTINATION "${_onnx_package}/lib")
            # NuGet ships an unversioned filename for a versioned ELF SONAME.
            file(CREATE_LINK "libonnxruntime.so" "${_onnx_package}/lib/libonnxruntime.so.1" SYMBOLIC)
        else()
            message(FATAL_ERROR
                "Could not download ONNX Runtime from ${ONNX_URL} (${_onnx_release_status}). "
                "Check network access or set ONNXRUNTIME_ROOT_DIR to an extracted ONNX Runtime package.")
        endif()
        if(NOT EXISTS "${_onnx_stage}/${ONNX_DIR_NAME}/include/onnxruntime_cxx_api.h")
            message(FATAL_ERROR "The downloaded ONNX Runtime archive is missing its C++ headers")
        endif()
        file(RENAME "${_onnx_stage}/${ONNX_DIR_NAME}" "${ONNXRUNTIME_ROOT_DIR}")
        file(REMOVE_RECURSE "${_onnx_stage}")
        message(STATUS "ONNX Runtime extracted to ${ONNXRUNTIME_ROOT_DIR}")
    else()
        message(STATUS "Using existing ONNX Runtime at ${ONNXRUNTIME_ROOT_DIR}")
    endif()
endif()

# Set up include directories and library paths
set(ONNXRUNTIME_INCLUDE_DIRS "${ONNXRUNTIME_ROOT_DIR}/include")
set(ONNXRUNTIME_LIB_DIR "${ONNXRUNTIME_ROOT_DIR}/lib")

# find_library caches its result across CMake reconfigurations. When switching
# between the CPU and CUDA ONNX Runtime packages, discard a cached library that
# came from the previously selected runtime directory.
if(ONNXRUNTIME_LIBRARY)
    get_filename_component(_ONNXRUNTIME_CACHED_LIB_DIR
        "${ONNXRUNTIME_LIBRARY}" DIRECTORY)
    get_filename_component(_ONNXRUNTIME_CACHED_LIB_DIR
        "${_ONNXRUNTIME_CACHED_LIB_DIR}" REALPATH)
    get_filename_component(_ONNXRUNTIME_SELECTED_LIB_DIR
        "${ONNXRUNTIME_LIB_DIR}" REALPATH)
    if(NOT "${_ONNXRUNTIME_CACHED_LIB_DIR}" STREQUAL
           "${_ONNXRUNTIME_SELECTED_LIB_DIR}")
        unset(ONNXRUNTIME_LIBRARY CACHE)
        unset(ONNXRUNTIME_LIBRARY)
    endif()
endif()

# Find the ONNX Runtime library
find_library(ONNXRUNTIME_LIBRARY
    NAMES onnxruntime
    PATHS "${ONNXRUNTIME_LIB_DIR}"
    NO_DEFAULT_PATH
)

if(NOT ONNXRUNTIME_LIBRARY)
    message(FATAL_ERROR "Could not find ONNX Runtime library in ${ONNXRUNTIME_LIB_DIR}")
endif()

if(ONNXRUNTIME_USE_CUDA AND
   NOT EXISTS "${ONNXRUNTIME_LIB_DIR}/libonnxruntime_providers_cuda.so")
    message(FATAL_ERROR
        "ONNXRUNTIME_USE_CUDA=ON, but the selected ONNX Runtime package has no "
        "CUDA provider at ${ONNXRUNTIME_LIB_DIR}/libonnxruntime_providers_cuda.so. "
        "Select the GPU-enabled ONNX Runtime package with ONNXRUNTIME_ROOT_DIR.")
endif()

message(STATUS "ONNX Runtime library: ${ONNXRUNTIME_LIBRARY}")
message(STATUS "ONNX Runtime include dir: ${ONNXRUNTIME_INCLUDE_DIRS}")

# Keep link-time artifacts inside this build tree. CMake can then retain
# automatic external runtime paths (ROOT, CUDA, extension dependencies, etc.)
# without appending the selected producer ONNX directory to installed RPATH.
# Stage all provider/version symlinks as well, preserving GPU provider lookup.
string(SHA256 _onnx_link_identity "${ONNXRUNTIME_ROOT_DIR}")
set(_onnx_link_dir "${CMAKE_BINARY_DIR}/_rdf_onnx_link/${_onnx_link_identity}")
file(MAKE_DIRECTORY "${_onnx_link_dir}")
file(GLOB _onnx_link_files "${ONNXRUNTIME_LIB_DIR}/*.so*" "${ONNXRUNTIME_LIB_DIR}/*.dylib")
list(APPEND _onnx_link_files "${ONNXRUNTIME_LIBRARY}")
list(REMOVE_DUPLICATES _onnx_link_files)
foreach(_onnx_link_file IN LISTS _onnx_link_files)
    get_filename_component(_onnx_link_name "${_onnx_link_file}" NAME)
    set(_onnx_link_path "${_onnx_link_dir}/${_onnx_link_name}")
    if(IS_SYMLINK "${_onnx_link_path}")
        file(READ_SYMLINK "${_onnx_link_path}" _onnx_link_previous)
        if(_onnx_link_previous STREQUAL _onnx_link_file)
            continue()
        endif()
    endif()
    file(CREATE_LINK "${_onnx_link_file}" "${_onnx_link_path}" SYMBOLIC COPY_ON_ERROR)
endforeach()
get_filename_component(_onnx_link_selected "${ONNXRUNTIME_LIBRARY}" NAME)
set(_onnx_link_library "${_onnx_link_dir}/${_onnx_link_selected}")

# Create an imported target for ONNX Runtime
if(NOT TARGET OnnxRuntime::OnnxRuntime)
    add_library(OnnxRuntime::OnnxRuntime SHARED IMPORTED)
    set_target_properties(OnnxRuntime::OnnxRuntime PROPERTIES
        IMPORTED_LOCATION "${_onnx_link_library}"
        INTERFACE_INCLUDE_DIRECTORIES "${ONNXRUNTIME_INCLUDE_DIRS}"
    )
endif()

# Set up RPATH so the library can be found at runtime
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_RPATH};${ONNXRUNTIME_LIB_DIR}")
set(CMAKE_BUILD_RPATH "${CMAKE_BUILD_RPATH};${ONNXRUNTIME_LIB_DIR}")

# Export ONNXRUNTIME_USE_CUDA as a preprocessor definition so C++ code can
# conditionally compile CUDA-specific paths (e.g., in OnnxManager.cc).
if(ONNXRUNTIME_USE_CUDA)
    add_compile_definitions(ONNXRUNTIME_USE_CUDA=1)
else()
    add_compile_definitions(ONNXRUNTIME_USE_CUDA=0)
endif()
