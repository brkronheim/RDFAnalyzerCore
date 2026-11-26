# Native performance patches

## RDFAnalyzerCore distribution patch

[rdf-native-performance.patch](rdf-native-performance.patch) packages the native
reader changes, vendored mimalloc, CMake integration and regression tests for
RDFAnalyzerCore. To backport the defaults, run these commands in a clean
RDFAnalyzerCore checkout at `fe4b3687f18f`, with `patches` pointing to the
distributed patch directory.

```sh
git apply --check "$patches/rdf-native-performance.patch"
git apply "$patches/rdf-native-performance.patch"
```

Build the analysis executable normally against the cluster’s ROOT installation.
Checkouts already containing the defaults can go directly to the build step.
This backport targets the audited ROOT 6.40.04 GNU x86-64 Linux configuration.
The current framework also supports ROOT 6.42.00-rc1. [Build options and compatibility](../../../docs/ROOT_REGISTRY_OPTIMIZATION.md)

## ROOT source patches

[matrix.tsv](matrix.tsv) lists the exact patches for each tested ROOT source
release. Multiple releases can share a file. The reader patch skips a virtual
update when the producer has already evaluated this slot and entry. The
column-name patch uses a reference to an empty list for the no-data-source
case in RDF definition checks and GetValidatedColumnNames. This preserves the
reference to the source's own column list when a data source exists.

Apply either patch, or both, to a clean checkout of the indicated release.
For example, to apply both to ROOT 6.36.14, run the following in its source
directory with `patches` pointing to this directory in RDFAnalyzerCore.

```sh
patch --dry-run --fuzz=0 -p1 -i "$patches/reader-6.30.12.patch"
patch --dry-run --fuzz=0 -p1 -i "$patches/column-names-6.36.14.patch"
patch --fuzz=0 -p1 -i "$patches/reader-6.30.12.patch"
patch --fuzz=0 -p1 -i "$patches/column-names-6.36.14.patch"
```

Rebuild ROOT and consumers that instantiate the modified RDF headers.
For stock Linux ROOT binaries the framework can link the reader directly into
native executables. [Framework build options](../../../docs/ROOT_REGISTRY_OPTIMIZATION.md)

Each listed version was checked with zero-fuzz patch application and compilation
of the modified RDF code. A 6,807-event Drell–Yan input produced exactly the
stock ROOT bin contents and errors in 246 nominal and shifted histogram cells
per release. The reader tests also covered repeated loops, sibling columns,
file changes, native variations, aliases, redefinition, JIT evaluation and
exceptions with one and four slots. Missing-branch defaults were covered from
6.34.10. These checks establish compatibility for the listed releases. The
earlier full-worker performance measurement was made on ROOT 6.40.04.
