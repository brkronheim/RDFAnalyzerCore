# pybind11 is provided by core/extern.
pybind11_add_module(rdfanalyzer python_bindings.cpp histogram_io_bindings.cpp)

# Link against the core library
target_link_libraries(rdfanalyzer PRIVATE
    coreAll
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    ROOT::Hist
    ROOT::RIO
)

# Include directories
target_include_directories(rdfanalyzer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface/api
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/OnnxManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/BDTManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/CorrectionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/TriggerManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/SofieManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/NDHistogramManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/NamedObjectManager
)

# Set output directory for the Python module
set_target_properties(rdfanalyzer PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python"
)
# Installed Python modules find their packaged runtime libraries beside the
# prefix's python directory. ROOT and compiler runtimes remain environment-owned.
get_target_property(_python_install_rpath rdfanalyzer INSTALL_RPATH)
list(REMOVE_ITEM _python_install_rpath "${ONNXRUNTIME_LIB_DIR}")
list(PREPEND _python_install_rpath "$ORIGIN/../lib")
list(REMOVE_DUPLICATES _python_install_rpath)
set_property(TARGET rdfanalyzer PROPERTY INSTALL_RPATH "${_python_install_rpath}")

# Installation rules
install(TARGETS rdfanalyzer
    LIBRARY DESTINATION python COMPONENT Python
)
include("${CMAKE_CURRENT_LIST_DIR}/../../../cmake/InstallPythonAuthoring.cmake")
rdf_install_python_authoring(COMPONENT Python)
rdf_install_python_controller(COMPONENT PythonController)
rdf_install_snakemake_controller()
if(ONNXRUNTIME_LIB_DIR)
    include("${PROJECT_SOURCE_DIR}/cmake/InstallOnnxRuntime.cmake")
    rdf_install_onnx_runtime(COMPONENT Python)
endif()

# Python binding runtime tests are registered in core/tests/cpp/CMakeLists.txt
# so they run as part of the standard test suite.
