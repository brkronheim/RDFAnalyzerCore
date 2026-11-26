cmake_minimum_required(VERSION 3.19)
project(RDFNativeResultsExample LANGUAGES CXX)

find_package(RDFNativeAuthoring CONFIG REQUIRED)
find_package(ROOT CONFIG REQUIRED COMPONENTS ROOTDataFrame RIO)

# Physics and graph construction live in a separately compiled library.
add_library(native_results_analysis STATIC analysis.cc)
target_compile_features(native_results_analysis PUBLIC cxx_std_17)
target_include_directories(native_results_analysis PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(native_results_analysis PUBLIC RDFAnalyzer::NativeAuthoring
    PRIVATE ROOT::ROOTDataFrame ROOT::RIO)
add_executable(rdf-native-results main.cc)
target_link_libraries(rdf-native-results PRIVATE native_results_analysis)

install(TARGETS native_results_analysis rdf-native-results
    ARCHIVE DESTINATION lib RUNTIME DESTINATION bin)
install(FILES steer.py README.md DESTINATION share/rdfanalyzer/examples/native-results)
install(FILES tests/validate.py DESTINATION share/rdfanalyzer/examples/native-results/tests)
