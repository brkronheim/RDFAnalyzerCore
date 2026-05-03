find_package(Python3 COMPONENTS Interpreter QUIET)
if(NOT Python3_FOUND)
    message(STATUS "Python3 not found; law tests will be registered from test/CMakeLists.txt.")
    return()
endif()

message(STATUS "Python3 found; law tests are registered from test/CMakeLists.txt.")
