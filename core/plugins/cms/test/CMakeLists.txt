# core/plugins/cms/test/CMakeLists.txt
#
# The plugin-local historical test source had drifted from the implemented
# API and called undefined working-point/print helpers. Use the canonical
# framework suite so both root CTest and this plugin-owned target exercise the
# same supported behavior.
if(NOT TARGET coreAll OR NOT TARGET gtest OR NOT TARGET gtest_main)
  message(FATAL_ERROR
    "CMS infrastructure tests must be configured from the RDFAnalyzerCore root with BUILD_TESTS=ON")
endif()

set(_CMS_FRAMEWORK_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../tests/cpp")
add_executable(testCmsInfrastructure
    "${_CMS_FRAMEWORK_TEST_DIR}/testCmsInfrastructure.cc")
target_link_libraries(testCmsInfrastructure PRIVATE coreAll gtest gtest_main)
target_include_directories(testCmsInfrastructure PRIVATE
    "${_CMS_FRAMEWORK_TEST_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/..")
target_compile_definitions(testCmsInfrastructure PRIVATE
    TEST_SOURCE_DIR="${_CMS_FRAMEWORK_TEST_DIR}")
add_test(NAME CmsInfrastructureTest COMMAND testCmsInfrastructure)
