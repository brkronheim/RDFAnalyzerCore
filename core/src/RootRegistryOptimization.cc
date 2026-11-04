#include <RootRegistryOptimization.h>
#if defined(__linux__)
#include <dlfcn.h>
#endif
namespace rdf {
RootRegistryOptimizationStatus rootRegistryOptimizationStatus() {
  RootRegistryOptimizationStatus result;
#if defined(__linux__)
  using Query = void (*)(std::uint64_t*);
  const auto query = reinterpret_cast<Query>(
      dlsym(RTLD_DEFAULT, "rdf_root_registry_optimization_status_v1"));
  if (query) {
    std::uint64_t values[4]{};
    query(values);
    result.linked = true;
    result.enabled = values[0];
    result.definitions = values[1];
    result.inputBindingsBefore = values[2];
    result.inputBindingsAfter = values[3];
    result.implementation = result.enabled ? "root64004-direct-inputs-v1" : "disabled-at-runtime";
  }
#endif
  return result;
}
}
