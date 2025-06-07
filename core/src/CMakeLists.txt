set(INTERFACE_DIR "${CoreDirectory}/interface")

include_directories(PUBLIC "${CoreDirectory}/extern/XGBoost-FastForest/include" "${CoreDirectory}/extern/pythia8/include" "${CoreDirectory}/interface")

add_library(plots OBJECT plots.cc)
target_compile_features(plots PUBLIC cxx_std_17)
target_link_libraries(plots PRIVATE ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)

add_library(systematics OBJECT systematics.cc)
target_compile_features(systematics PUBLIC cxx_std_17)
target_link_libraries(systematics PRIVATE ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)


add_library(util OBJECT util.cc)
target_compile_features(util PUBLIC cxx_std_17)
target_link_libraries(util PRIVATE plots ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)

add_library(analyzer OBJECT analyzer.cc)
target_compile_features(analyzer PUBLIC cxx_std_17)
target_link_libraries(analyzer PRIVATE systematics fastforest correctionlib ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore) 
