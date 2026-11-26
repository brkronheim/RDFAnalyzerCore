# Combine also uses Vdt with ROOT installations that do not export it.
find_package(Vdt CONFIG QUIET)
if(TARGET VDT::VDT)
    set(Vdt_FOUND TRUE)
    return()
endif()

find_path(VDT_INCLUDE_DIR NAMES vdt/vdtMath.h)
find_library(VDT_LIBRARY NAMES vdt)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Vdt REQUIRED_VARS VDT_INCLUDE_DIR VDT_LIBRARY)
mark_as_advanced(VDT_INCLUDE_DIR VDT_LIBRARY)

if(Vdt_FOUND)
    add_library(VDT::VDT UNKNOWN IMPORTED)
    set_target_properties(VDT::VDT PROPERTIES
        IMPORTED_LOCATION "${VDT_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${VDT_INCLUDE_DIR}")
endif()
