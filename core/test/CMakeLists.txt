add_executable(testPlots testPlots.cc)
target_compile_features(testPlots PRIVATE cxx_std_17)
find_package(ROOT)
target_link_libraries(testPlots PRIVATE plots ${ROOT_LIBRARIES}) # ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)