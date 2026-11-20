# core/plugins/StxsManager/CMakeLists.txt

add_library(StxsManager OBJECT
    StxsManager.cc
    StxsColumns.cc
    stxs/Manager.cc
)

target_include_directories(StxsManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(StxsManager PUBLIC
    ROOT::Core
    ROOT::Hist
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
)

# Ensure StxsManager is propagated to the parent scope
set(STXS_MANAGER_LIB StxsManager PARENT_SCOPE)
