set(_xrootd_hints "")
set(_xrootd_library_hints "")
if(DEFINED ENV{XROOTD_ROOT})
    list(APPEND _xrootd_hints "$ENV{XROOTD_ROOT}/include/xrootd" "$ENV{XROOTD_ROOT}/include")
    list(APPEND _xrootd_library_hints "$ENV{XROOTD_ROOT}/lib" "$ENV{XROOTD_ROOT}/lib64")
endif()
if(DEFINED ENV{XRD_ROOT})
    list(APPEND _xrootd_hints "$ENV{XRD_ROOT}/include/xrootd" "$ENV{XRD_ROOT}/include")
    list(APPEND _xrootd_library_hints "$ENV{XRD_ROOT}/lib" "$ENV{XRD_ROOT}/lib64")
endif()

# CMS runtime containers expose the XrdCl development package through CVMFS,
# without adding its headers to the default compiler search path.
file(GLOB _cms_xrootd_dirs LIST_DIRECTORIES TRUE
    "/cvmfs/cms.cern.ch/el9_amd64_gcc11/external/xrootd/*")
list(SORT _cms_xrootd_dirs)
list(REVERSE _cms_xrootd_dirs)
set(_cms_xrootd_package "")
foreach(_dir IN LISTS _cms_xrootd_dirs)
    list(APPEND _xrootd_hints "${_dir}/include/xrootd")
    list(APPEND _xrootd_library_hints "${_dir}/lib64" "${_dir}/lib")
    if(NOT _cms_xrootd_package
       AND EXISTS "${_dir}/include/xrootd/XrdCl/XrdClFile.hh"
       AND EXISTS "${_dir}/lib64/libXrdCl.so")
        set(_cms_xrootd_package "${_dir}")
    endif()
endforeach()

if(DEFINED ENV{XROOTD_ROOT} OR DEFINED ENV{XRD_ROOT})
    find_path(XRDCL_INCLUDE_DIR
        NAMES XrdCl/XrdClFile.hh
        HINTS ${_xrootd_hints}
        DOC "Directory containing XrdCl development headers")
    find_library(XRDCL_LIBRARY
        NAMES XrdCl
        HINTS ${_xrootd_library_hints}
        DOC "XrdCl client library")
elseif(_cms_xrootd_package)
    # Keep the headers and client library from the same CMS XRootD release.
    # On some EL9 hosts, the system XrdCl 5.9 runtime crashes during its own
    # static initialization; the matched CMS CVMFS 5.7 package is usable by
    # both the standalone build and its worker container.
    set(XRDCL_INCLUDE_DIR "${_cms_xrootd_package}/include/xrootd"
        CACHE PATH "Directory containing XrdCl development headers" FORCE)
    set(XRDCL_LIBRARY "${_cms_xrootd_package}/lib64/libXrdCl.so"
        CACHE FILEPATH "XrdCl client library" FORCE)
else()
    find_path(XRDCL_INCLUDE_DIR
        NAMES XrdCl/XrdClFile.hh
        HINTS ${_xrootd_hints}
        DOC "Directory containing XrdCl development headers")
    find_library(XRDCL_LIBRARY
        NAMES XrdCl
        HINTS ${_xrootd_library_hints}
        DOC "XrdCl client library")
endif()

if(NOT XRDCL_INCLUDE_DIR OR NOT XRDCL_LIBRARY)
    message(FATAL_ERROR
        "RDF_ENABLE_XRD_ADAPTOR=ON requires XrdCl headers and libXrdCl. "
        "Set XROOTD_ROOT or XRD_ROOT to the XRootD development package prefix.")
endif()

message(STATUS "XrdCl headers: ${XRDCL_INCLUDE_DIR}")
message(STATUS "XrdCl library: ${XRDCL_LIBRARY}")

get_filename_component(XRDCL_LIBRARY_DIR "${XRDCL_LIBRARY}" DIRECTORY)
list(APPEND CMAKE_BUILD_RPATH "${XRDCL_LIBRARY_DIR}")
list(APPEND CMAKE_INSTALL_RPATH "${XRDCL_LIBRARY_DIR}")
list(REMOVE_DUPLICATES CMAKE_BUILD_RPATH)
list(REMOVE_DUPLICATES CMAKE_INSTALL_RPATH)

ROOT_GENERATE_DICTIONARY(G__RDFXrdAdaptor
    XrdTFile.h
    LINKDEF "${CMAKE_CURRENT_SOURCE_DIR}/LinkDef.h"
    OPTIONS -I"${CMAKE_CURRENT_SOURCE_DIR}" -I"${XRDCL_INCLUDE_DIR}")

add_library(RDFXrdAdaptor SHARED
    XrdSourceManager.cc
    XrdTFile.cc
    "${CMAKE_CURRENT_BINARY_DIR}/G__RDFXrdAdaptor.cxx")
target_compile_features(RDFXrdAdaptor PUBLIC cxx_std_17)
target_include_directories(RDFXrdAdaptor PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${XRDCL_INCLUDE_DIR}")
target_link_libraries(RDFXrdAdaptor PUBLIC
    ROOT::Core
    ROOT::RIO
    ROOT::Tree
    "${XRDCL_LIBRARY}")
set_target_properties(RDFXrdAdaptor PROPERTIES OUTPUT_NAME RDFXrdAdaptor)

# ROOT's runtime dictionary uses this header when it loads the PCM. Keep a
# copy next to the library so the submission workflow can stage the complete,
# relocatable dictionary payload with the shared library.
add_custom_command(TARGET RDFXrdAdaptor POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/XrdTFile.h"
        "$<TARGET_FILE_DIR:RDFXrdAdaptor>/XrdTFile.h"
    VERBATIM)
