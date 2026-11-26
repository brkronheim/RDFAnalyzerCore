#ifndef RDF_ROOT_REGISTRY_OPTIMIZATION_H
#define RDF_ROOT_REGISTRY_OPTIMIZATION_H
#include <cstdint>
#include <string>
namespace rdf {
struct RootRegistryOptimizationStatus {
  bool linked = false;
  bool enabled = false;
  std::uint64_t definitions = 0;
  std::uint64_t inputBindingsBefore = 0;
  std::uint64_t inputBindingsAfter = 0;
  std::string implementation = "off";
};
// Reports the implementation actually present in this process. Shared/Python
// modules do not install an override into an already-running ROOT process.
RootRegistryOptimizationStatus rootRegistryOptimizationStatus();
}
#endif
