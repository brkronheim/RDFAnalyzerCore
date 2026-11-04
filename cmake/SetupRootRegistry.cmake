# Native executable compatibility patch for the audited ROOT 6.40.04 ABI.
# No installed ROOT file or process-wide preload is changed.
set(RDF_ROOT_REGISTRY_OPTIMIZATION "AUTO" CACHE STRING
    "Compact ROOT Define registries in native executables: AUTO, ON, OFF")
set_property(CACHE RDF_ROOT_REGISTRY_OPTIMIZATION PROPERTY STRINGS AUTO ON OFF)
string(TOUPPER "${RDF_ROOT_REGISTRY_OPTIMIZATION}" _rdf_registry_mode)
if(NOT _rdf_registry_mode MATCHES "^(AUTO|ON|OFF)$")
    message(FATAL_ERROR "RDF_ROOT_REGISTRY_OPTIMIZATION must be AUTO, ON or OFF")
endif()
set(RDF_ROOT_REGISTRY_AVAILABLE FALSE)
set(_rdf_registry_reason "disabled by configuration")
if(NOT _rdf_registry_mode STREQUAL "OFF")
    set(_rdf_registry_reason "requires ROOT 6.40.04, Linux, 64-bit GNU/Clang with the audited headers")
    if(ROOT_VERSION VERSION_EQUAL "6.40.04" AND CMAKE_SYSTEM_NAME STREQUAL "Linux"
       AND CMAKE_SIZEOF_VOID_P EQUAL 8 AND CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")
        get_target_property(_rdf_root_includes ROOT::Core INTERFACE_INCLUDE_DIRECTORIES)
        unset(_rdf_registry_include CACHE)
        unset(_rdf_registry_include)
        find_path(_rdf_registry_include ROOT/RDF/RColumnRegister.hxx HINTS ${_rdf_root_includes} NO_DEFAULT_PATH)
        set(_rdf_registry_headers
            RColumnRegister.hxx RDefineBase.hxx RDefineReader.hxx RLoopManager.hxx)
        set(_rdf_registry_hashes
            7da8b0b893cc0672f4a252c8b0df132139197620b3fe0d2afca7abd5e40996eb
            3e0d114d66a5a5dbe6483fb452504f6e137615f5c567e382c7d7c2fb42130aa3
            6afd24fa08aa505acdfc1d3ee38ce12b009fd33d65068a7d262ed41976f3f471 503c8488a1cd0fb30a20688ecfea6d5a3c270422d981ec756bc8b02b9827dc1e)
        set(RDF_ROOT_REGISTRY_AVAILABLE TRUE)
        foreach(_i RANGE 0 3)
            list(GET _rdf_registry_headers ${_i} _header)
            list(GET _rdf_registry_hashes ${_i} _expected)
            set(_file "${_rdf_registry_include}/ROOT/RDF/${_header}")
            if(NOT EXISTS "${_file}")
                set(RDF_ROOT_REGISTRY_AVAILABLE FALSE)
                set(_rdf_registry_reason "missing ROOT header ${_header}")
                break()
            endif()
            file(SHA256 "${_file}" _actual)
            if(NOT _actual STREQUAL _expected)
                set(RDF_ROOT_REGISTRY_AVAILABLE FALSE)
                set(_rdf_registry_reason "ROOT header ${_header} differs from the audited 6.40.04 source")
                break()
            endif()
        endforeach()
        if(RDF_ROOT_REGISTRY_AVAILABLE)
            get_target_property(_rdf_registry_library_type ROOT::ROOTDataFrame TYPE)
            if(NOT _rdf_registry_library_type STREQUAL "SHARED_LIBRARY")
                set(RDF_ROOT_REGISTRY_AVAILABLE FALSE)
                set(_rdf_registry_reason "requires shared ROOTDataFrame for native ELF symbol interposition")
            endif()
        endif()
        if(RDF_ROOT_REGISTRY_AVAILABLE)
            include(CheckCXXSourceCompiles)
            string(SHA256 _rdf_registry_flags_key "${CMAKE_CXX_COMPILER};${CMAKE_BUILD_TYPE};${CMAKE_TRY_COMPILE_CONFIGURATION};${CMAKE_CXX_FLAGS};${CMAKE_CXX_FLAGS_DEBUG};${CMAKE_CXX_FLAGS_RELEASE};${CMAKE_CXX_FLAGS_RELWITHDEBINFO};${CMAKE_CXX_FLAGS_MINSIZEREL}")
            string(SUBSTRING "${_rdf_registry_flags_key}" 0 12 _rdf_registry_flags_key)
            set(_rdf_registry_abi_test "_rdf_registry_abi_${_rdf_registry_flags_key}")
            check_cxx_source_compiles("#include <string>
#if defined(_GLIBCXX_DEBUG) || !defined(__GLIBCXX__) || !_GLIBCXX_USE_CXX11_ABI
#error incompatible standard library ABI
#endif
int main() { return 0; }" ${_rdf_registry_abi_test})
            if(NOT ${_rdf_registry_abi_test})
                set(RDF_ROOT_REGISTRY_AVAILABLE FALSE)
                set(_rdf_registry_reason "requires the ordinary libstdc++ C++11 ABI without debug iterators")
            endif()
        endif()
        if(RDF_ROOT_REGISTRY_AVAILABLE)
            include(CheckLinkerFlag)
            check_linker_flag(CXX "LINKER:--export-dynamic-symbol=rdf_root_registry_optimization_status_v1"
                _rdf_registry_linker_supported)
            if(NOT _rdf_registry_linker_supported)
                set(RDF_ROOT_REGISTRY_AVAILABLE FALSE)
                set(_rdf_registry_reason "linker does not support selective ELF symbol exports")
            endif()
        endif()
    endif()
endif()
if(NOT RDF_ROOT_REGISTRY_AVAILABLE)
    if(_rdf_registry_mode STREQUAL "ON")
        message(FATAL_ERROR "Cannot enable ROOT registry optimization: ${_rdf_registry_reason}")
    endif()
    message(STATUS "ROOT registry optimization: OFF (${_rdf_registry_reason})")
    return()
endif()
set(_rdf_registry_private "${CMAKE_BINARY_DIR}/root_registry_include")
file(MAKE_DIRECTORY "${_rdf_registry_private}/ROOT/RDF")
file(READ "${_rdf_registry_include}/ROOT/RDF/RColumnRegister.hxx" _header)
string(REPLACE "   void AddDefine(std::shared_ptr<RDFDetail::RDefineBase> column);"
    "   void AddDefine(std::shared_ptr<RDFDetail::RDefineBase> column);\n   void KeepDirectInputs(const std::vector<std::string> &columns);"
    _header "${_header}")
file(WRITE "${_rdf_registry_private}/ROOT/RDF/RColumnRegister.hxx" "${_header}")
add_library(RDFRootRegistryCompat OBJECT "${CMAKE_CURRENT_LIST_DIR}/../core/root_compat/Root640Registry.cc")
target_include_directories(RDFRootRegistryCompat BEFORE PRIVATE "${_rdf_registry_private}")
target_link_libraries(RDFRootRegistryCompat PRIVATE ROOT::ROOTDataFrame)
target_compile_features(RDFRootRegistryCompat PRIVATE cxx_std_17)
set_target_properties(RDFRootRegistryCompat PROPERTIES
    CXX_VISIBILITY_PRESET default VISIBILITY_INLINES_HIDDEN OFF)
message(STATUS "ROOT registry optimization: ON for native executables (audited ROOT 6.40.04)")

# Apply through core's interface, evaluated for the final consuming target.
# Shared/Python modules keep their ordinary ROOT behavior; executable symbols
# must precede ROOT in ELF lookup, including for interpreter-compiled Defines.
function(rdf_attach_root_registry target)
    set(_native "$<STREQUAL:$<TARGET_PROPERTY:TYPE>,EXECUTABLE>")
    target_sources(${target} INTERFACE "$<${_native}:$<TARGET_OBJECTS:RDFRootRegistryCompat>>")
    foreach(_symbol IN ITEMS
        _ZN4ROOT6Detail3RDF11RDefineBaseC*
        _ZN4ROOT8Internal3RDF15RColumnRegister9AddDefine*
        rdf_root_registry_optimization_status_v1)
        target_link_options(${target} INTERFACE "$<${_native}:LINKER:--export-dynamic-symbol=${_symbol}>")
    endforeach()
endfunction()
