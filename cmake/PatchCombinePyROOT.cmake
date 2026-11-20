# ROOT 6.40 PyROOT proxies cannot be compared to None with == or !=.
# Use their truth value to test whether the RooFit objects are null.
if(NOT DEFINED COMBINE_SOURCE_DIR OR NOT IS_DIRECTORY "${COMBINE_SOURCE_DIR}")
  message(FATAL_ERROR "COMBINE_SOURCE_DIR is missing or is not a directory")
endif()

set(_utils "${COMBINE_SOURCE_DIR}/python/tool_base/utils.py")
if(NOT EXISTS "${_utils}")
  message(FATAL_ERROR "Cannot find Combine tool_base/utils.py: ${_utils}")
endif()

file(READ "${_utils}" _contents)
string(REPLACE
  "# For pyROOT NULL test: \"pdf != None\" != \"pdf is not None\""
  "# PyROOT 6.40 null proxies must be tested by truth value."
  _contents "${_contents}")
string(REPLACE
  "if pdf != None and gobs != None:"
  "if pdf and gobs:"
  _contents "${_contents}")
string(REPLACE
  "elif pdf == None or pdf.IsA().InheritsFrom(ROOT.RooUniform.Class()):"
  "elif not pdf or pdf.IsA().InheritsFrom(ROOT.RooUniform.Class()):"
  _contents "${_contents}")

if(_contents MATCHES "pdf[ \t]*(!=|==)[ \t]*None")
  message(FATAL_ERROR "Unsafe PyROOT None comparison remains in ${_utils}")
endif()
file(WRITE "${_utils}" "${_contents}")
