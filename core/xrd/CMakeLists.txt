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

find_path(XRDCL_INCLUDE_DIR NAMES XrdCl/XrdClFile.hh
    HINTS ${_xrootd_hints} DOC "Directory containing XrdCl development headers")
find_library(XRDCL_LIBRARY NAMES XrdCl HINTS ${_xrootd_library_hints}
    DOC "XrdCl client library")

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
if(RDF_XRD_OPEN_POLICY_HEADER)
    # Copy the extension policy alongside the dictionary for relocatable workers.
    configure_file("${RDF_XRD_OPEN_POLICY_HEADER}"
        "${CMAKE_CURRENT_BINARY_DIR}/RDFXrdOpenPolicy.h" COPYONLY)
    target_include_directories(RDFXrdAdaptor PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
    target_compile_definitions(RDFXrdAdaptor PUBLIC
        RDF_XRD_OPEN_POLICY_HEADER="RDFXrdOpenPolicy.h")
endif()

# ROOT's runtime dictionary uses the rootmap header when it loads the PCM.
# Keep the Xrd adaptor headers beside the library so the submission workflow
# can stage the complete, relocatable dictionary payload with the shared
# library, including headers included by the rootmap header.
file(GLOB _xrd_runtime_headers CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
add_custom_command(TARGET RDFXrdAdaptor POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${_xrd_runtime_headers}
        "$<TARGET_FILE_DIR:RDFXrdAdaptor>"
    VERBATIM)

# The Python component is a usable local prefix, including its dynamic adaptor
# and dictionary payload. External ROOT/XRootD runtimes retain their own ABI.
install(TARGETS RDFXrdAdaptor LIBRARY DESTINATION lib COMPONENT Python)
install(FILES ${_xrd_runtime_headers}
    "${CMAKE_CURRENT_BINARY_DIR}/libRDFXrdAdaptor_rdict.pcm"
    "${CMAKE_CURRENT_BINARY_DIR}/libRDFXrdAdaptor.rootmap"
    "${CMAKE_CURRENT_BINARY_DIR}/RDFXrdOpenPolicy.h"
    DESTINATION lib COMPONENT Python OPTIONAL)
