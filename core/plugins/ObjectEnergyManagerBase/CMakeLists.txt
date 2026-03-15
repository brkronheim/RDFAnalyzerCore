add_library(ObjectEnergyManagerBase OBJECT ObjectEnergyManagerBase.cc)
target_include_directories(ObjectEnergyManagerBase PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(ObjectEnergyManagerBase PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
