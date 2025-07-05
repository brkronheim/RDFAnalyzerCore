add_library(CorrectionManager OBJECT CorrectionManager.cc)
target_include_directories(CorrectionManager PUBLIC 
${PLUGIN_SOURCE_DIRECTORIES} 
${CMAKE_CURRENT_SOURCE_DIR}/../../interface 
${CMAKE_CURRENT_SOURCE_DIR}/../../extern/correctionlib/include
) 