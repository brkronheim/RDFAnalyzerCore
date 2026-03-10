add_executable(cms_doublemu_analysis analysis.cc)
target_compile_features(cms_doublemu_analysis PRIVATE cxx_std_17)

target_include_directories(cms_doublemu_analysis
    PUBLIC
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/XGBoost-FastForest/include
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/correctionlib/include
        ${RDFAnalyzer_SOURCE_DIR}/core/interface
        ${RDFAnalyzer_SOURCE_DIR}/core/interface/api
)

target_link_libraries(cms_doublemu_analysis
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
