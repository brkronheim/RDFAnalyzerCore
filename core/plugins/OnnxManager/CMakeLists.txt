add_library(OnnxManager OBJECT OnnxManager.cc)
target_include_directories(OnnxManager PUBLIC 
                        ${PLUGIN_SOURCE_DIRECTORIES} 
                        ${CMAKE_CURRENT_SOURCE_DIR}/../../interface 
) 
# Ensure ROOT headers and ONNX Runtime are available when compiling this plugin
target_link_libraries(OnnxManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    OnnxRuntime::OnnxRuntime
)
