add_executable(analysis analysis.cc)
target_compile_features(analysis PRIVATE cxx_std_17)

target_include_directories(analysis
    PUBLIC
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/XGBoost-FastForest/include
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/correctionlib/include
        ${RDFAnalyzer_SOURCE_DIR}/core/interface
        ${RDFAnalyzer_SOURCE_DIR}/core/interface/api
)

target_link_libraries(analysis
    PRIVATE
        core
        corePlugins
        ROOT::ROOTDataFrame
        ROOT::ROOTVecOps
        ROOT::Core
        ROOT::Hist
        ROOT::MathCore
        ROOT::RIO
)
