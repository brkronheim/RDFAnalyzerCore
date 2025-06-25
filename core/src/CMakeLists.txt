set(INTERFACE_DIR "${CoreDirectory}/interface")

include_directories(PUBLIC "${CoreDirectory}/extern/XGBoost-FastForest/include" "${CoreDirectory}/extern/correctionlib/include" "${CoreDirectory}/interface")

add_library(plots OBJECT plots.cc)
target_compile_features(plots PUBLIC cxx_std_17)
target_link_libraries(plots PRIVATE  ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)

add_library(configParser OBJECT configParser.cc)
target_compile_features(configParser PUBLIC cxx_std_17)
target_link_libraries(configParser PRIVATE plots ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)


add_library(util OBJECT util.cc)
target_compile_features(util PUBLIC cxx_std_17)
target_link_libraries(util PRIVATE plots configParser ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore)



add_library(analyzer OBJECT analyzer.cc)
target_compile_features(analyzer PUBLIC cxx_std_17)
target_link_libraries(analyzer PRIVATE configParser plots fastforest correctionlib plots ROOT::ROOTDataFrame ROOT::ROOTVecOps ROOT::Core ROOT::Hist ROOT::MathCore) 
