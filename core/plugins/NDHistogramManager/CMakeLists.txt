add_library(NDHistogramManager OBJECT NDHistogramManager.cc)
target_include_directories(NDHistogramManager PUBLIC ${PLUGIN_SOURCE_DIRECTORIES} ${CMAKE_CURRENT_SOURCE_DIR}/../../interface) 
# Ensure ROOT headers (e.g. ROOT/RDataFrame.hxx) are available when compiling this plugin
target_link_libraries(NDHistogramManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    Boost::headers
) 