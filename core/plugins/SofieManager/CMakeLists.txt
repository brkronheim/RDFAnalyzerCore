add_library(SofieManager OBJECT SofieManager.cc)
target_include_directories(SofieManager PUBLIC 
                        ${PLUGIN_SOURCE_DIRECTORIES} 
                        ${CMAKE_CURRENT_SOURCE_DIR}/../../interface 
) 
# Ensure ROOT headers are available when compiling this plugin
target_link_libraries(SofieManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    ROOT::TMVA
)
