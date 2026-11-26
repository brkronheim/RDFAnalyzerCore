# core/src/CMakeLists.txt

# Gather all source files in this directory
file(GLOB CORE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

# The installed SDK historically exported both implicit FriendTreeSpec constructors.
# Retain the out-of-line definitions in this setup-only translation unit.
set_property(SOURCE FriendTreeSpec.cc APPEND PROPERTY COMPILE_OPTIONS
    "$<$<COMPILE_LANG_AND_ID:CXX,GNU,Clang,AppleClang>:-fno-inline>"
    "$<$<COMPILE_LANG_AND_ID:CXX,Clang,AppleClang>:-Xclang;-mno-constructor-aliases>")

add_library(core STATIC ${CORE_SRC})
if(TARGET RDFMimalloc)
    target_sources(core INTERFACE "$<$<STREQUAL:$<TARGET_PROPERTY:TYPE>,EXECUTABLE>:$<TARGET_OBJECTS:RDFMimalloc>>")
    target_link_libraries(core INTERFACE Threads::Threads ${CMAKE_DL_LIBS})
endif()
if(RDF_ROOT_REGISTRY_AVAILABLE OR RDF_ROOT_READER_AVAILABLE)
    rdf_attach_root_registry(core)
endif()

target_compile_features(core PUBLIC cxx_std_17)

target_include_directories(core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/XGBoost-FastForest/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/correctionlib/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/BDTManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/CorrectionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/NDHistogramManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/SystematicManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/TriggerManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/NamedObjectManager
    ${CMAKE_BINARY_DIR}/include  # for generated GitVersion.h
)

target_link_libraries(core PUBLIC
    ${CMAKE_DL_LIBS}
    fastforest
    correctionlib
    Boost::headers
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    ROOT::Gpad
    ROOT::Graf
    ROOT::Hist
    ROOT::HistPainter
    ROOT::MathCore
    ${RDF_ROOT_TRANSITIVE_LIBS}
    yaml-cpp
)

if(RDF_ENABLE_XRD_ADAPTOR)
    target_link_libraries(core PUBLIC RDFXrdAdaptor)
    target_compile_definitions(core PRIVATE RDF_HAS_XRD_ADAPTOR)
endif()

set(SOURCES
    # ... existing sources ...
)
