# ROOT >=6.38 rejects out-of-range RooRealVar::setVal calls. Keep the
# finite-difference stencil inside the upper bound instead of above it.
if(NOT DEFINED COMBINE_SOURCE_DIR OR NOT IS_DIRECTORY "${COMBINE_SOURCE_DIR}")
  message(FATAL_ERROR "COMBINE_SOURCE_DIR is missing or is not a directory")
endif()
set(_hesse "${COMBINE_SOURCE_DIR}/src/RobustHesse.cc")
file(READ "${_hesse}" _contents)
string(REPLACE "valHi = boundHi + 1E-7;" "valHi = boundHi - 1E-7;"
  _contents "${_contents}")
if(_contents MATCHES "valHi = boundHi \\+ 1E-7;")
  message(FATAL_ERROR "Out-of-range RobustHesse stencil remains in ${_hesse}")
endif()
file(WRITE "${_hesse}" "${_contents}")
