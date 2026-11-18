cmake_minimum_required(VERSION 3.19)
# No compiler detection, native package discovery, FetchContent or event runtime.
project(RDFAnalyzerPythonController LANGUAGES NONE)
include("${CMAKE_CURRENT_LIST_DIR}/../../../cmake/InstallPythonController.cmake")
rdf_install_python_controller(COMPONENT PythonController)
