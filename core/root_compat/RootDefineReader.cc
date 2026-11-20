// Adapted from ROOT RDefineReader.hxx, Copyright CERN, LGPL-2.1.
#include <ROOT/RDF/RDefineReader.hxx>
#include "RootRuntimeVersion.hxx"
#include <stdexcept>

#if defined(_GLIBCXX_DEBUG) || !defined(__GLIBCXX__) || !_GLIBCXX_USE_CXX11_ABI
#error "The audited ROOT reader patch requires the ordinary libstdc++ ABI"
#endif

void *ROOT::Internal::RDF::RDefineReader::GetImpl(Long64_t entry)
{
   if (!rdf::root_compat::runtimeVersionMatches())
      throw std::runtime_error("ROOT reader optimization requires the exact ROOT runtime used to compile it");
   if (!fDefine.HasCurrentEntry(fSlot, entry))
      fDefine.Update(fSlot, entry);
   return fValuePtr;
}
