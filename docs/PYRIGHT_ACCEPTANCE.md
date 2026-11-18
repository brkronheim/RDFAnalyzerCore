# Pyright acceptance for installed Python interfaces

The installed public interfaces now pass **Pyright 1.1.414** as well as the
existing pinned mypy checks. Six clients pass strict mode, three installed
tutorials pass basic mode, and 13 deliberately invalid calls are rejected at
all expected sites. No source/stub changes or runtime wrappers were required.

The language-free installation uses the real shared authoring/controller CMake
rules and the optional CMS extension's own package rules. Checker imports resolve
the fresh installed prefix, and a separate import-only subprocess confirms that
configuration/provider imports need no ROOT, native binding, Numba, NumPy or
YAML modules. Nothing processes events or modifies the shared native build.
