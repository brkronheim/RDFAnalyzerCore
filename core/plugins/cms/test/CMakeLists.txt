# core/plugins/cms/test/CMakeLists.txt
#
# Unit tests for the CMS analysis infrastructure.
# Works in both standalone and subdirectory mode.

add_executable(testCmsInfrastructure testCmsInfrastructure.cc)

if(CMS_STANDALONE)
  # Standalone: link against our own library + GoogleTest from framework
  # The user must provide GTEST_ROOT or have it installed system-wide.
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    # Try to find it in the framework tree
    set(_GTEST_DIR "${RDFANALYZER_CORE_DIR}/core/extern/googletest")
    if(EXISTS "${_GTEST_DIR}")
      set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
      add_subdirectory("${_GTEST_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/googletest" EXCLUDE_FROM_ALL)
      set(GTEST_BOTH_LIBRARIES GTest::gtest GTest::gtest_main)
    else()
      message(WARNING "GoogleTest not found; CMS tests will not be built.")
      return()
    endif()
  else()
    set(GTEST_BOTH_LIBRARIES GTest::gtest GTest::gtest_main)
  endif()

  target_link_libraries(testCmsInfrastructure PRIVATE
      CmsInfrastructure
      ${GTEST_BOTH_LIBRARIES}
      ROOT::Core ROOT::ROOTDataFrame ROOT::ROOTVecOps
  )
else()
  # Subdirectory mode: parent provides coreAll, gtest targets
  target_link_libraries(testCmsInfrastructure PRIVATE
      coreAll gtest gtest_main CmsInfrastructure
  )
endif()

target_include_directories(testCmsInfrastructure PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../tests/cpp
)

# TEST_SOURCE_DIR is needed by test_util.h's ChangeToTestSourceDir()
target_compile_definitions(testCmsInfrastructure PRIVATE
    TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
)

add_test(NAME CmsInfrastructureTest COMMAND testCmsInfrastructure)
