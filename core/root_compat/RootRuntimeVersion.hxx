#ifndef RDF_ROOT_COMPAT_RUNTIME_VERSION_HXX
#define RDF_ROOT_COMPAT_RUNTIME_VERSION_HXX

#include <ROOT/RVersion.hxx>
#include <TROOT.h>
#include <string_view>

namespace rdf::root_compat {

static_assert(ROOT_VERSION_MAJOR * 10000 + ROOT_VERSION_MINOR * 100 + ROOT_VERSION_PATCH ==
              RDF_EXPECT_ROOT_VERSION, "ROOT ABI version mismatch");

inline bool runtimeVersionMatches()
{
   // TROOT uses decimal version numbers. ROOT_VERSION_CODE is bit-packed.
   static const bool matches = gROOT->GetVersionInt() == RDF_EXPECT_ROOT_VERSION
#ifdef RDF_EXPECT_ROOT_GIT_TAG
      && std::string_view(gROOT->GetGitCommit()) == RDF_EXPECT_ROOT_GIT_TAG
#endif
      ;
   return matches;
}

} // namespace rdf::root_compat

#endif
