# ROOT 6.40 upgrade

The 6.40 upgrade selected ROOT **6.40.04** in the local environment and both CMake presets, using
`/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.40.04/x86_64-almalinux9.8-gcc115-opt`.
The runner Dockerfile selects the corresponding Ubuntu 22.04 binary. See the [current CVMFS setup](ROOT_6_42_UPGRADE.md) for newer builds. GCC 11.5,
C++17 and the existing Python 3.12 environment remain in use locally.

Re-source the environment and refresh cached ROOT/VDT paths before rebuilding:

```bash
RDF_CVMFS_ROOT_RELEASE=6.40.04 source env.sh
root-config --version
cmake -S . -B build-root640 -U 'ROOT_*' -U 'VDT_*' -DROOT_DIR="$ROOTSYS/cmake"
cmake --build build-root640 -j4
```

Rebuild all executables and bindings you intend to run; old binaries must not
be mixed with the new libraries. Generated job sandboxes keep their copied
environment until regenerated. External analysis repositories were not changed.

## Follow-ups worth considering

These are recommendations, not additional behavior changes in this upgrade.

| Priority | ROOT change | Repository-specific recommendation |
| --- | --- | --- |
| High | 6.40 reverts TTree Snapshot's ZSTD default after adverse results for empty collections. | Make compression algorithm/level configurable in `RootOutputSink.cc`, which currently forces ZSTD-5. Benchmark representative skims. |
| High | 6.40 separates the weight argument in `HistoND`/`HistoNSparseD`; the previous signature is deprecated. | Migrate `plots.h`'s weighted `HistoND` call with an older-ROOT guard. |
| Already available | 6.40 reduces RDF JIT setup work. | Measure startup on large systematic graphs; no wrapper API addition is needed. |
| Evaluate | 6.40 introduces experimental `RDataLoader` with cluster-aligned reads, shuffling and PyTorch batches. | Prototype a training input backend separately; preserve existing training semantics and measure CPU/memory use. |
| Optional | 6.40 adds `GetDatasetTopLevelFieldNames`. | Useful for schema inspection, but it does not identify consumed graph dependencies and should not replace the cache's reader-based branch resolution. |

These changes are documented in the [ROOT 6.40 release notes](https://root.cern/doc/v640/release-notes.html).

Changes inherited from [6.38](https://root.cern/doc/v638/release-notes.html) also
matter: Snapshot no longer requires column-type JIT, and native `HistoNSparseD`
is available. The output sink already uses untemplated Snapshot, so it receives
the startup improvement automatically. Replacing `NDHistogramManager`'s custom
`Book` actions is a larger design decision: it uses `THnSparseF` and framework
region/systematic handling, so the native double-precision action is not a
direct replacement.

## Compatibility checks

PyROOT ownership changes merit targeted tests before modifying downstream
Python workflows. The experimental ROOT 7 histogram API is not a production
replacement for this framework's persisted histograms. See the
[6.40 compatibility notes](https://root.cern/doc/v640/release-notes.html).

The existing asynchronous TTreeCache policy remains relevant to single-CPU
XRootD workloads. Its two-file, split-branch, entry-range, fallback and numerical
checks should be rerun whenever ROOT changes. No implicit multithreading is
enabled by this environment update.

## Validation performed

`root-config`, the ROOT interpreter and PyROOT report 6.40.04. After refreshing
CMake and a clean rebuild, all six selected CTest suites passed:
`InputCacheTest`, `DataManagerTest`, `RootOutputSinkTest`,
`NDHistogramManagerTest`, `SystematicBundleTest` and `PluginLifecycleTest`.
The cache suite includes actual prefetch activity and branch-pointer rebinding
across two files. No source compatibility fixes were required for these targets.
The Docker image and external analysis builds were not validated.
