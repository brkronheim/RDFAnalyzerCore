set(CoreDirectory "${RDFAnalyzer_SOURCE_DIR}/core")

# Core and source extensions consume the generated correctionlib version header.
include_directories("${CMAKE_BINARY_DIR}/include")

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.30.00")
    add_compile_definitions(HAS_ROOT_PROGRESS_BAR)
endif()

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.34.00")
    add_compile_definitions(HAS_DEFAULT_VALUE_FOR)
endif()

add_subdirectory(extern)
if(RDF_ENABLE_XRD_ADAPTOR)
    add_subdirectory(xrd)
endif()
add_subdirectory(src)
add_executable(rdf-plot tools/plot.cc)
target_link_libraries(rdf-plot PRIVATE core)
install(TARGETS rdf-plot RUNTIME DESTINATION bin)

# Optional custom ROOT dictionary (user-provided)
if(RDF_CUSTOM_DICT_HEADERS AND RDF_CUSTOM_DICT_LINKDEF)
    if(NOT COMMAND ROOT_GENERATE_DICTIONARY)
        message(FATAL_ERROR "ROOT_GENERATE_DICTIONARY not available. Ensure ROOT CMake is properly configured.")
    endif()

    set(_dict_headers ${RDF_CUSTOM_DICT_HEADERS})
    set(_dict_linkdef ${RDF_CUSTOM_DICT_LINKDEF})
    set(_dict_includes ${RDF_CUSTOM_DICT_INCLUDE_DIRS})
    set(_dict_sources ${RDF_CUSTOM_DICT_SOURCES})

    ROOT_GENERATE_DICTIONARY(G__${RDF_CUSTOM_DICT_TARGET}
        ${_dict_headers}
        LINKDEF ${_dict_linkdef}
    )

    add_library(${RDF_CUSTOM_DICT_TARGET} SHARED
        G__${RDF_CUSTOM_DICT_TARGET}.cxx
        ${_dict_sources}
    )

    target_include_directories(${RDF_CUSTOM_DICT_TARGET} PUBLIC
        ${_dict_includes}
    )

    target_link_libraries(${RDF_CUSTOM_DICT_TARGET} PUBLIC
        ROOT::Core
        ROOT::RIO
        ROOT::Tree
    )

    target_link_libraries(core PUBLIC ${RDF_CUSTOM_DICT_TARGET})
endif()
add_subdirectory(plugins)
rdf_add_extensions()
get_property(_rdf_extension_targets GLOBAL PROPERTY RDF_EXTENSION_TARGETS)

# coreAll: interface target that links core + corePlugins inside a linker
# rescan group so the circular dependency between the two static archives
# (core→NDHistogramManager symbols, corePlugins→Analyzer::addPlugin) is
# resolved regardless of link order on single-pass GNU ld.
add_library(coreAll INTERFACE)
# ROOT's imported targets can expose bare transitive names (notably tbb).
# Propagate their resolved directories to every consumer, including tests.
if(RDF_ROOT_TRANSITIVE_RPATH_DIRS)
    target_link_directories(coreAll INTERFACE ${RDF_ROOT_TRANSITIVE_RPATH_DIRS})
    # Keep -L even when CMake considers an environment path implicit: some
    # resolved libraries are linker scripts with relative INPUT members.
    foreach(_rdf_transitive_dir IN LISTS RDF_ROOT_TRANSITIVE_RPATH_DIRS)
        target_link_options(coreAll INTERFACE "-L${_rdf_transitive_dir}")
    endforeach()
endif()
target_link_libraries(coreAll INTERFACE
    -Wl,--start-group
    core
    corePlugins
    ${_rdf_extension_targets}
    -Wl,--end-group
)
rdf_add_default_sofie_blas(coreAll)

add_subdirectory(python/bindings)

# Only build tests if requested
if(BUILD_TESTS)
    add_subdirectory(tests)
    rdf_add_extension_tests()
endif()



