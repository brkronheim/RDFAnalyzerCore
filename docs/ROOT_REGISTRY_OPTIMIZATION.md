# ROOT column-registry memory optimization

Native executables linked to `core` or `coreAll` automatically use the audited
ROOT 6.40.04 registry compatibility patch on supported builds. It retains each
Define's exact direct input bindings and shares alias/variation metadata, while
leaving public dataframe, action and filter registries complete. It also avoids
excess capacity when adding a definition to a registry. This reduces metadata
memory without changing the event graph, selection, arithmetic or output schema.

The implementation is in [Root640Registry.cc](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/root_compat/Root640Registry.cc).
It adapts two ROOT implementations under ROOT's LGPL 2.1 license. It is a
repository compatibility patch, not an installed or upstream ROOT modification.
ROOT itself and the existing CVMFS installation remain unchanged.

## Build selection

`RDF_ROOT_REGISTRY_OPTIMIZATION` is a CMake string option:

| Value | Behavior |
|---|---|
| `AUTO` (default) | Enable for the audited ROOT 6.40.04 headers and supported platform; otherwise use ordinary ROOT. |
| `ON` | Require support; configuration fails if the ROOT headers, version, platform or linker do not match. |
| `OFF` | Omit the compatibility object and its exported symbols entirely. |

Support requires 64-bit Linux, a GNU/Clang compiler using the libstdc++ C++11 ABI,
shared ROOTDataFrame, an ELF linker with selective symbol exports, and exact hashes of four ROOT
internal headers. Debug iterator mode and libc++ are not compatible. The
runtime also checks ROOT's version before using the patched implementation.
The build keeps the added method declaration private to one generated header;
it does not change the framework's public ROOT include path or class layout.

```bash
# Default automatic selection; no registry option is needed.
cmake -S . -B build
cmake --build build --target vjet_njetpt vjet_znunu_inclusive -j 4

# Require the audited implementation, or explicitly build ordinary ROOT behavior.
cmake -S . -B build -DRDF_ROOT_REGISTRY_OPTIMIZATION=ON
cmake -S . -B build -DRDF_ROOT_REGISTRY_OPTIMIZATION=OFF
```

The normal build wrappers inherit this default. Supported native binaries also
enable the optimization when the runtime environment variable is unset.
An explicitly cached `OFF` remains respected; use
`-DRDF_ROOT_REGISTRY_OPTIMIZATION=AUTO` to restore automatic selection.

Use the repository runtime environment for these commands. The compiler checks
remain enabled in Debug builds; an unsupported internal-header change requires
review and validation before updating the allowlist.

## Runtime and output evidence

The compatibility object is linked directly into native executables. CMake
exports its ROOT constructor/registration symbols so ROOT and Cling-generated
code resolve them in the executable. No `LD_PRELOAD`, new shared library,
wrapper, or extra worker transfer is required. Existing input/output formats
and analysis configs continue to work.

For an isolated per-process rollback, set this environment variable **before
starting the executable**:

```bash
RDF_ROOT_REGISTRY_OPTIMIZATION=0 build/analyses/vjet-njetpt/vjet_njetpt config.yaml
```

The linked object then uses ROOT's original registry behavior. Changing the
variable after graph construction starts does not change the cached mode.
`1`/`on` enables the normal compiled default; invalid values fail explicitly.

Provenance in `__rdf_job_metadata_v1` records what the process actually used:

| Key | Meaning |
|---|---|
| `root.registry_optimization` | `root64004-direct-inputs-v1`, `disabled-at-runtime`, or `off` when no compatibility object is linked. |
| `root.registry_definitions` | Number of Define-base constructions observed by the linked implementation. |
| `root.registry_input_bindings_before` | Cumulative input-binding entries before direct-input pruning. |
| `root.registry_input_bindings_after` | Cumulative entries retained by those private registries. |

The counters describe construction operations, not live bytes or process RSS.
The binding counters remain zero when pruning is disabled. They do not run
per event. C++ callers can inspect the same information through
`rdf::rootRegistryOptimizationStatus()` in
[RootRegistryOptimization.h](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/RootRegistryOptimization.h).

Shared/Python extension modules do not inject the compatibility override into
an already-running ROOT/PyROOT process. They retain ordinary ROOT behavior and
report `off`. Native vjet executables, including `vjet_znunu_inclusive`, receive
the patch through the shared framework target. New executables carry the code
when staged; already-prepared/submitted executables retain their original bytes.

## Correctness boundary and checks

Old reader bindings remain necessary across `Redefine`, aliases and native
variations. Pruning removes unrelated bindings from each Define's private view;
the readers and computations themselves remain owned by ROOT's loop manager.
The complete public registry remains available for column discovery, graph
cloning/export, filters, actions and skim branch selection. No event values,
correction payloads or histogram accumulators are deleted by this patch.

`RootRegistryOptimizationTest` verifies linkage, public-column availability,
pruning, JIT expressions, redefinition and native variations. Its separate
`RootRegistryOptimizationDisabledTest` checks runtime rollback in a fresh
process. Provenance tests verify the output fields. The validation driver in
the enablement study
also runs selected unmodified upstream ROOT tests against stock and linked
implementations and compares full-analysis outputs. This is bounded coverage,
not the entire ROOT project's regression matrix.
