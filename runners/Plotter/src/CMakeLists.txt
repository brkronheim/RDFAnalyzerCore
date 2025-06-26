add_executable(makePlots makePlots.cc)
target_compile_features(makePlots PRIVATE cxx_std_17)

target_include_directories(makePlots
    PUBLIC
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/XGBoost-FastForest/include
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/correctionlib/include
        ${RDFAnalyzer_SOURCE_DIR}/core/interface
)

target_link_libraries(makePlots
    PRIVATE
        core
        ROOT::ROOTDataFrame
        ROOT::ROOTVecOps
        ROOT::Core
        ROOT::Hist
        ROOT::MathCore
        ROOT::RIO
)

ROOT_GENERATE_DICTIONARY(makePlots_dict LINKDEF LinkDef.h MODULE makePlots)
