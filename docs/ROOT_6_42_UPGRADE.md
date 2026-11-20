# ROOT 6.42 CVMFS upgrade

The AlmaLinux 9 environment and both CMake presets select **ROOT 6.42.00-rc1**,
the newest standalone build on CERN CVMFS as of November 20, 2026.
ROOT reports the numeric version `6.41.99` and the tag `v6-42-00-rc1`.
ROOT 6.40.04 is the latest stable release. The Ubuntu 22.04 runner image uses
6.40.04 with system Python 3.10. The Ubuntu 22.04 CVMFS candidate was built for
Python 3.12. [CVMFS release and platform list](https://root.cern/releases/release-64200-rc1/)

```bash
source env.sh
root-config --version
cmake -S . -B build-root642 -DROOT_DIR="$ROOTSYS/cmake"
cmake --build build-root642 -j4
```

For a stable build, use a separate build directory:

```bash
RDF_CVMFS_ROOT_RELEASE=6.40.04 source env.sh
cmake -S . -B build-root640 -DROOT_DIR="$ROOTSYS/cmake"
cmake --build build-root640 -j4
```

Run each executable or Python module with the ROOT installation used to build
it. Rebuild and restage job bundles when switching a production to a new ROOT
version. The input cache and TTree output formats carry forward. Opt-in
`entryRangeMode=source` supports the candidate with the existing one-thread,
signed 64-bit range contract.

The environment supplies CVMFS FFTW for RooFit's runtime dependency. The
optional Combine build discovers Vdt directly when ROOT's CMake package leaves
it private and receives the framework's selected `ROOT_DIR`.

## Compatibility patches

The eligible native executable builds use registry pruning, the cached Define
reader, and packed and inline input readers on the candidate. The release-tag
check distinguishes the candidate from development builds with the same numeric
version. The existing source reader patch applies with zero fuzz. The
column-name patch has a candidate-specific version that also covers the new
per-sample definition methods. [Patch matrix and instructions](../core/root_compat/patches/README.md)

## Features worth using

| ROOT feature | RDFAnalyzerCore use |
| --- | --- |
| `RedefinePerSample` | Use it on an `RNode` when an analysis needs to replace an existing column with a value derived from sample metadata. Registered configuration constants already use `DefinePerSample`. |
| `Median` | Use it directly for exact medians in small, local QA graphs. It retains all input values in memory and the ROOT action is local-only. |
| RNTuple processor and lazy metadata | A bounded I/O pilot can compare open latency, memory and event throughput for converted inputs before adopting RNTuple in workflows that currently require TTree chains and friends. The processor-backed RDF source is experimental. |
| RooFit parallel gradients and Minuit2 Hessian sparsity | Evaluate them on representative large fits with unchanged fit inputs. Hessian sparsity requires objective functions to identify exactly independent parameter pairs. |
| SOFIE safetensors weights | Evaluate binary weight loading when the standalone SOFIE generator used by the framework provides support. The AlmaLinux CVMFS candidate ships without `tmva-sofie`. |

[ROOT 6.42 release notes](https://root.cern/doc/v642/release-notes.html)

## Validation

The native registry, reader and packed path were exercised through the framework
tests with the registry enabled and disabled. ROOT's upstream Redefine and
per-sample suites passed. Both source patches were applied and their changed RDF
code was compiled in an isolated overlay. A saved 6,807-event Drell–Yan input
produced identical bin contents and errors in 246 nominal and shifted histogram
cells with stock 6.40.04, stock 6.42.00-rc1, and the candidate's patched paths.
The local ExampleAnalysis run wrote the expected dimuon skim and histogram
metadata. The CMS DoubleMuon example also matched all 477 occupied cells in 10
histograms from its saved 6.40.04 reference. The focused checks covered input
caching, cross-file and 64-bit source ranges, Snapshot output, systematics,
plugin lifecycle and SOFIE callable execution. PyROOT's inherited
`THnSparseF` constructor requires an explicit chunk size with this build. The
Python histogram fixtures pass the existing 16,384-entry default explicitly.

Combine 11.1.0 built with the existing patches and passed workspace creation
and a counting-card `FitDiagnostics --robustHesse 1` smoke test. The same
card's default `AsymptoticLimits` run encountered an out-of-range parameter
on both 6.40.04 and the candidate. CombineHarvester remains unverified.
