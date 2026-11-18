cmake_minimum_required(VERSION 3.19)
# Installation needs no compiler or ROOT; consumers enable their own C++ language.
project(RDFAnalyzerNativeAuthoring LANGUAGES NONE)
include("${CMAKE_CURRENT_LIST_DIR}/../../cmake/InstallNativeAuthoring.cmake")
rdf_install_native_authoring(COMPONENT NativeAuthoring)
