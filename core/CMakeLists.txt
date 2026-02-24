set(CoreDirectory "${RDFAnalyzer_SOURCE_DIR}/core")

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.30.00")
    add_compile_definitions(HAS_ROOT_PROGRESS_BAR)
endif()

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.34.00")
    add_compile_definitions(HAS_DEFAULT_VALUE_FOR)
endif()

add_subdirectory(extern)
add_subdirectory(src)

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
# add_subdirectory(bindings)

# Only build tests if requested
if(BUILD_TESTS)
    add_subdirectory(test)
endif()




