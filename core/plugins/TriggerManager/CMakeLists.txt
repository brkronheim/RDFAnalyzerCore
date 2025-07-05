add_library(TriggerManager OBJECT TriggerManager.cc)
target_include_directories(TriggerManager PUBLIC ${PLUGIN_SOURCE_DIRECTORIES} ${CMAKE_CURRENT_SOURCE_DIR}/../../interface) 