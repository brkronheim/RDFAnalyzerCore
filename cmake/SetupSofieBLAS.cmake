include_guard(GLOBAL)

if(NOT DEFINED RDF_SOFIE_BLAS_LIBRARY)
    set(RDF_SOFIE_BLAS_LIBRARY "" CACHE FILEPATH
        "Override the default OpenBLAS library for generated SOFIE code")
endif()

function(_rdf_find_sofie_blas result)
    if(RDF_SOFIE_BLAS_LIBRARY)
        if(NOT IS_ABSOLUTE "${RDF_SOFIE_BLAS_LIBRARY}" OR
           NOT EXISTS "${RDF_SOFIE_BLAS_LIBRARY}" OR
           IS_DIRECTORY "${RDF_SOFIE_BLAS_LIBRARY}")
            message(FATAL_ERROR "RDF_SOFIE_BLAS_LIBRARY must name an existing absolute library file")
        endif()
        set(library "${RDF_SOFIE_BLAS_LIBRARY}")
    else()
        # The generated headers use the LP64 Fortran BLAS symbols. These
        # runtime SONAMEs also cover systems without a development symlink.
        find_library(RDF_SOFIE_OPENBLAS_LIBRARY
            NAMES openblas openblaso openblasp openblass
                  libopenblas.so.0 libopenblaso.so.0 libopenblasp.so.0 libopenblass.so.0
            HINTS ENV LD_LIBRARY_PATH)
        set(library "${RDF_SOFIE_OPENBLAS_LIBRARY}")
    endif()
    set(${result} "${library}" PARENT_SCOPE)
endfunction()

function(rdf_add_default_sofie_blas target)
    _rdf_find_sofie_blas(library)
    if(NOT library)
        return()
    endif()
    set(link_items "${library}")
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        # Scope linker state and retain the DSO only when a symbol needs it.
        set(link_items "-Wl,--push-state,--as-needed;${library};-Wl,--pop-state")
    endif()
    target_link_libraries(${target} INTERFACE
        "$<$<NOT:$<BOOL:$<TARGET_PROPERTY:RDF_SOFIE_BLAS_EXPLICIT>>>:${link_items}>")
endfunction()

# Require a BLAS library for one target, or use explicit per-target link
# items such as BLAS::BLAS instead of the framework-wide default.
function(rdf_target_link_sofie target)
    if(ARGC GREATER 1)
        set(libraries ${ARGN})
    else()
        _rdf_find_sofie_blas(libraries)
        if(NOT libraries)
            message(FATAL_ERROR
                "OpenBLAS is required by SOFIE target '${target}'. Install OpenBLAS, set "
                "RDF_SOFIE_BLAS_LIBRARY to a BLAS library, or pass explicit link items "
                "to rdf_target_link_sofie().")
        endif()
    endif()
    set_property(TARGET ${target} PROPERTY RDF_SOFIE_BLAS_EXPLICIT TRUE)
    # Append private link items without selecting a target_link_libraries
    # signature. Existing analysis targets use both the plain and keyword forms.
    set_property(TARGET ${target} APPEND PROPERTY LINK_LIBRARIES ${libraries})
endfunction()
