add_library(KinematicFitManager OBJECT KinematicFitManager.cc)
target_include_directories(KinematicFitManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(KinematicFitManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
)

if(USE_CUDA)
    target_sources(KinematicFitManager PRIVATE KinematicFitGPU.cu)
    target_compile_definitions(KinematicFitManager PUBLIC USE_CUDA)
    target_link_libraries(KinematicFitManager PUBLIC CUDA::cudart)
endif()
