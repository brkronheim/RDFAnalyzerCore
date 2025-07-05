add_library(NDHistogramManager OBJECT NDHistogramManager.cc)
target_include_directories(NDHistogramManager PUBLIC ${PLUGIN_SOURCE_DIRECTORIES} ${CMAKE_CURRENT_SOURCE_DIR}/../../interface) 