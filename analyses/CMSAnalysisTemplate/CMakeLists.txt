add_executable(cms_analysis_template analysis.cc)
target_compile_features(cms_analysis_template PRIVATE cxx_std_17)

target_include_directories(cms_analysis_template
    PUBLIC
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/XGBoost-FastForest/include
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/correctionlib/include
        ${RDFAnalyzer_SOURCE_DIR}/core/interface
        ${RDFAnalyzer_SOURCE_DIR}/core/interface/api
)

target_link_libraries(cms_analysis_template
    PRIVATE
        coreAll
        ROOT::ROOTDataFrame
        ROOT::ROOTVecOps
        ROOT::Core
        ROOT::Hist
        ROOT::MathCore
        ROOT::RIO
)

# ---------------------------------------------------------------------------
# Python extension module: cms_analysis_template
#
# Wraps setupCMSAnalysis() from analysis_setup.h via pybind11 so that the
# same C++ analysis logic can be called from analysis_wrapper.py.
# Built into build/python/ alongside the rdfanalyzer module.
# ---------------------------------------------------------------------------
pybind11_add_module(cms_analysis_template_py analysis_bindings.cc)
set_target_properties(cms_analysis_template_py PROPERTIES
    OUTPUT_NAME cms_analysis_template
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python"
)
target_compile_features(cms_analysis_template_py PRIVATE cxx_std_17)

target_include_directories(cms_analysis_template_py
    PRIVATE
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/XGBoost-FastForest/include
        ${RDFAnalyzer_SOURCE_DIR}/core/extern/correctionlib/include
        ${RDFAnalyzer_SOURCE_DIR}/core/interface
        ${RDFAnalyzer_SOURCE_DIR}/core/interface/api
)

target_link_libraries(cms_analysis_template_py
    PRIVATE
        coreAll
        ROOT::ROOTDataFrame
        ROOT::ROOTVecOps
        ROOT::Core
        ROOT::Hist
        ROOT::MathCore
        ROOT::RIO
)
