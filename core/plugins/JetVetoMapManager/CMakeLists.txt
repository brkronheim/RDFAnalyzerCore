add_library(JetVetoMapManager OBJECT JetVetoMapManager.cc)
target_include_directories(JetVetoMapManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(JetVetoMapManager PUBLIC
    ROOT::Core
)
