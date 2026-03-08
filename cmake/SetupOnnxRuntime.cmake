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
    
    # Download and extract if not already present
    if(NOT EXISTS "${ONNXRUNTIME_ROOT_DIR}")
        message(STATUS "Downloading ONNX Runtime ${ONNXRUNTIME_VERSION} for ${ONNX_PLATFORM}-${ONNX_ARCH}...")
        file(DOWNLOAD
            "${ONNX_URL}"
            "${CMAKE_BINARY_DIR}/${ONNX_FILENAME}"
            SHOW_PROGRESS
            STATUS DOWNLOAD_STATUS
        )
        
        list(GET DOWNLOAD_STATUS 0 STATUS_CODE)
        if(NOT STATUS_CODE EQUAL 0)
            list(GET DOWNLOAD_STATUS 1 ERROR_MESSAGE)
            message(FATAL_ERROR "Failed to download ONNX Runtime: ${ERROR_MESSAGE}")
        endif()
        
        message(STATUS "Extracting ONNX Runtime...")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E tar xzf "${CMAKE_BINARY_DIR}/${ONNX_FILENAME}"
            WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
            RESULT_VARIABLE EXTRACT_RESULT
        )
        
        if(NOT EXTRACT_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to extract ONNX Runtime")
        endif()
        
        # Clean up the archive
        file(REMOVE "${CMAKE_BINARY_DIR}/${ONNX_FILENAME}")
        message(STATUS "ONNX Runtime extracted to ${ONNXRUNTIME_ROOT_DIR}")
    else()
        message(STATUS "Using existing ONNX Runtime at ${ONNXRUNTIME_ROOT_DIR}")
    endif()
endif()

# Set up include directories and library paths
set(ONNXRUNTIME_INCLUDE_DIRS "${ONNXRUNTIME_ROOT_DIR}/include")
set(ONNXRUNTIME_LIB_DIR "${ONNXRUNTIME_ROOT_DIR}/lib")

# Find the ONNX Runtime library
find_library(ONNXRUNTIME_LIBRARY
    NAMES onnxruntime
    PATHS "${ONNXRUNTIME_LIB_DIR}"
    NO_DEFAULT_PATH
)

if(NOT ONNXRUNTIME_LIBRARY)
    message(FATAL_ERROR "Could not find ONNX Runtime library in ${ONNXRUNTIME_LIB_DIR}")
endif()

message(STATUS "ONNX Runtime library: ${ONNXRUNTIME_LIBRARY}")
message(STATUS "ONNX Runtime include dir: ${ONNXRUNTIME_INCLUDE_DIRS}")

# Create an imported target for ONNX Runtime
if(NOT TARGET OnnxRuntime::OnnxRuntime)
    add_library(OnnxRuntime::OnnxRuntime SHARED IMPORTED)
    set_target_properties(OnnxRuntime::OnnxRuntime PROPERTIES
        IMPORTED_LOCATION "${ONNXRUNTIME_LIBRARY}"
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
