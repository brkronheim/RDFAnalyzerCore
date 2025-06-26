set(CoreDirectory "${RDFAnalyzer_SOURCE_DIR}/core")

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.30.00")
    add_compile_definitions(HAS_ROOT_PROGRESS_BAR)
endif()

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.34.00")
    add_compile_definitions(HAS_DEFAULT_VALUE_FOR)
endif()

add_subdirectory(extern)
add_subdirectory(src)
add_subdirectory(test)

