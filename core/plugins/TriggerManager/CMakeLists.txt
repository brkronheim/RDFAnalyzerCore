add_library(TriggerManager OBJECT TriggerManager.cc)
target_include_directories(TriggerManager PUBLIC ${PLUGIN_SOURCE_DIRECTORIES} ${CMAKE_CURRENT_SOURCE_DIR}/../../interface) 
# Ensure ROOT headers are available when compiling this plugin
target_link_libraries(TriggerManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
) 