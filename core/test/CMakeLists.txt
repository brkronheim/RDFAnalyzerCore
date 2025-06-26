include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../extern/gtest/googletest/include)

add_executable(testConfigurationManager testConfigurationManager.cc)
target_link_libraries(testConfigurationManager core gtest gtest_main)
add_test(NAME ConfigurationManagerTest COMMAND testConfigurationManager)