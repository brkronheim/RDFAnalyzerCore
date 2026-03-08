cmake_minimum_required(VERSION 3.19)

project(CombineHarvesterStandalone LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

if(NOT DEFINED COMBINEHARVESTER_SOURCE_DIR)
    message(FATAL_ERROR "COMBINEHARVESTER_SOURCE_DIR must point to the checked out CombineHarvester repository")
endif()

if(NOT DEFINED COMBINE_PREFIX)
    message(FATAL_ERROR "COMBINE_PREFIX must point to the installed Combine prefix")
endif()

set(_ch_source_dir "${COMBINEHARVESTER_SOURCE_DIR}")
set(_combine_prefix "${COMBINE_PREFIX}")

if(NOT EXISTS "${_ch_source_dir}/CombineTools")
    message(FATAL_ERROR "Invalid COMBINEHARVESTER_SOURCE_DIR: ${_ch_source_dir}")
endif()

if(NOT EXISTS "${_combine_prefix}/include/HiggsAnalysis/CombinedLimit/interface")
    message(FATAL_ERROR "Invalid COMBINE_PREFIX, missing Combine headers: ${_combine_prefix}")
endif()

find_package(ROOT REQUIRED COMPONENTS MathMore RooFitCore RooFit RooStats HistFactory)
find_package(Boost REQUIRED CONFIG COMPONENTS program_options filesystem regex)
find_package(LibXml2 REQUIRED)
find_package(Vdt REQUIRED)

find_library(COMBINE_STANDALONE_LIBRARY
    NAMES HiggsAnalysisCombinedLimit
    PATHS "${_combine_prefix}/lib"
    NO_DEFAULT_PATH
    REQUIRED
)

include(${ROOT_USE_FILE})

include_directories(${ROOT_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${LIBXML2_INCLUDE_DIR})
include_directories(BEFORE "${CMAKE_BINARY_DIR}")
include_directories(BEFORE "${_combine_prefix}/include")

file(REMOVE_RECURSE "${CMAKE_BINARY_DIR}/CombineHarvester")
execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${_ch_source_dir}" "${CMAKE_BINARY_DIR}/CombineHarvester"
)

set(_ch_compile_options
    -fno-guess-branch-probability
    -fno-devirtualize
    -fno-tree-forwprop
)

function(ch_install_root_dictionary library_name)
    install(
        FILES
            "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/lib${library_name}_rdict.pcm"
            "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/lib${library_name}.rootmap"
        DESTINATION lib
        OPTIONAL
    )
endfunction()

function(ch_add_package_library package_name dependency_libs)
    set(options)
    set(oneValueArgs)
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(CHLIB "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    set(target_name "CombineHarvester${package_name}")
    set(source_root "${_ch_source_dir}/${package_name}")

    ROOT_GENERATE_DICTIONARY(
        G__${target_name}
        "CombineHarvester/${package_name}/src/classes.h"
        LINKDEF "${source_root}/src/classes_def.xml"
        MODULE ${target_name}
        NOINSTALL
        OPTIONS ${ROOTCLING_OPTIONS}
    )

    add_library(${target_name} SHARED ${CHLIB_SOURCES} G__${target_name}.cxx)
    target_compile_options(${target_name} PRIVATE ${_ch_compile_options})
    target_include_directories(${target_name}
        PUBLIC
            "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>"
            "$<BUILD_INTERFACE:${_ch_source_dir}>"
            "$<BUILD_INTERFACE:${_combine_prefix}/include>"
            "$<INSTALL_INTERFACE:include>"
    )
    target_link_libraries(${target_name}
        PUBLIC
            ${dependency_libs}
            ${ROOT_LIBRARIES}
            ${Boost_LIBRARIES}
            ${LIBXML2_LIBRARIES}
            VDT::VDT
            "${COMBINE_STANDALONE_LIBRARY}"
    )

    install(TARGETS ${target_name}
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
    )
    ch_install_root_dictionary(${target_name})
endfunction()

function(ch_add_package_binaries package_name library_target)
    file(GLOB package_bins "${_ch_source_dir}/${package_name}/bin/*.cpp")
    foreach(bin_source IN LISTS package_bins)
        get_filename_component(bin_name "${bin_source}" NAME_WE)
        add_executable(${bin_name} "${bin_source}")
        target_compile_options(${bin_name} PRIVATE ${_ch_compile_options})
        target_include_directories(${bin_name}
            PRIVATE
                "${CMAKE_BINARY_DIR}"
                "${_ch_source_dir}"
                "${_combine_prefix}/include"
        )
        target_link_libraries(${bin_name}
            PRIVATE
                ${library_target}
                ${ROOT_LIBRARIES}
                ${Boost_LIBRARIES}
                ${LIBXML2_LIBRARIES}
                VDT::VDT
                "${COMBINE_STANDALONE_LIBRARY}"
        )
        install(TARGETS ${bin_name} RUNTIME DESTINATION "${package_name}/bin")
    endforeach()
endfunction()

file(GLOB _combine_tools_sources "${_ch_source_dir}/CombineTools/src/*.cc")
file(GLOB _combine_pdfs_sources "${_ch_source_dir}/CombinePdfs/src/*.cc")

ch_add_package_library(CombineTools "" SOURCES ${_combine_tools_sources})
ch_add_package_library(CombinePdfs "CombineHarvesterCombineTools" SOURCES ${_combine_pdfs_sources})

ch_add_package_binaries(CombineTools CombineHarvesterCombineTools)
ch_add_package_binaries(CombinePdfs CombineHarvesterCombinePdfs)

install(DIRECTORY "${_ch_source_dir}/CombineTools/" DESTINATION CombineTools)
install(DIRECTORY "${_ch_source_dir}/CombinePdfs/" DESTINATION CombinePdfs)
install(DIRECTORY "${_ch_source_dir}/docs/" DESTINATION docs)
install(FILES "${_ch_source_dir}/README.md" DESTINATION .)
install(FILES "${_ch_source_dir}/Doxyfile" DESTINATION .)
