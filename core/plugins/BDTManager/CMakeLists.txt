add_library(BDTManager OBJECT BDTManager.cc)
target_include_directories(BDTManager PUBLIC 
                        ${PLUGIN_SOURCE_DIRECTORIES} 
                        ${CMAKE_CURRENT_SOURCE_DIR}/../../interface 
                        ${CMAKE_CURRENT_SOURCE_DIR}/../../extern/XGBoost-FastForest/include
) 