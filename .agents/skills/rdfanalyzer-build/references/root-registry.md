# ROOT registry compatibility

Use [SetupRootRegistry.cmake](../../../../cmake/SetupRootRegistry.cmake) and
[the implementation](../../../../core/root_compat/Root640Registry.cc) for
`RDF_ROOT_REGISTRY_OPTIMIZATION=AUTO|ON|OFF`. AUTO enables only the audited
ROOT 6.40.04 internal headers on supported native builds; ON rejects unsupported
headers/platforms and OFF omits the object. Preserve the exact-header and
runtime-version guards when changing build integration.

The object reaches native executables through `core`'s interface, with explicit
ELF exports for ROOT/Cling lookup. Shared/Python modules retain stock ROOT.
There is no new worker library or preload to stage. Use runtime environment
`RDF_ROOT_REGISTRY_OPTIMIZATION=0` for a fresh-process rollback comparison.
The provenance `root.registry_optimization` and construction counters establish
whether the running binary used the patch; source/build state does not prove
worker deployment.

[The public contract](../../../../docs/ROOT_REGISTRY_OPTIMIZATION.md) covers
reader retention and rollback. Run RootRegistryOptimizationTest, its disabled
variant, provenance and affected analysis checks. For a new supported ROOT
build, compare upstream RDF tests plus full analysis/skim outputs and inspect
symbol exports. Keep installed ROOT and active productions intact.
