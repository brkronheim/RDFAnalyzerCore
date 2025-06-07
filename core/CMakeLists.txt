
set(CoreDirectory "${RDFAnalyzer_SOURCE_DIR}/core")

if(ROOT_VERSION VERSION_GREATER_EQUAL "6.28.00")
    add_compile_definitions(HAS_ROOT_PROGRESS_BAR)
endif()

add_subdirectory(extern)
add_subdirectory(src)

