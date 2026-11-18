#ifndef RDF_EXAMPLE_NATIVE_INSPECTION_H
#define RDF_EXAMPLE_NATIVE_INSPECTION_H
#include <NativePreflight.h>

struct InputInspection {
  rdf::validation::Report report;
  unsigned int eventLoops = 0;
};

// Implementation is compiled separately and linked into the native executable.
InputInspection inspectInput(const std::string &path);
#endif
