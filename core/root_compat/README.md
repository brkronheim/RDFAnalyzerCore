# Audited native ROOT compatibility

This directory adapts ROOT's `RDefineBase` constructor and
`RColumnRegister::AddDefine`, and adds a private direct-input registry view.
RDefineBase: Copyright (C) 1995-2018, Rene Brun and Fons Rademakers;
authors Enrico Guiraud and Danilo Piparo (CERN).
RColumnRegister: Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.
All rights reserved.
The reader override uses the existing per-slot entry cache in `RDefineBase`.
The supported releases and exact header hashes are in [native_abis.tsv](native_abis.tsv).
Independent source patches are in [patches](patches/README.md).
The adapted portions are under [ROOT's LGPL 2.1 license](https://root.cern/about/license/).
Original sources include ROOT `tree/dataframe/src/RDefineBase.cxx`,
`tree/dataframe/src/RDFColumnRegister.cxx` and `tree/dataframe/inc/ROOT/RDF/RDefineReader.hxx` in the matched release.

[Build selection, runtime behavior and validation](../../docs/ROOT_REGISTRY_OPTIMIZATION.md)
explain the exact-header guards, native executable linkage, rollback and output
evidence. Do not extend the version/header allowlist without reviewing the
matching ROOT source, running upstream regression cases and comparing actual
analysis outputs. No installed ROOT files are edited.
