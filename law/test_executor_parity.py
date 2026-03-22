#!/usr/bin/env python3
"""
Executor parity validation suite for RDFAnalyzerCore.

This suite compares local, HTCondor, and Dask executor behaviour for
representative workflows.  It verifies that:

1. **Compatible outputs** – the same inputs and configurations produce
   outputs with identical path structure and schema regardless of which
   executor is selected.

2. **Provenance parity** – all three executors invoke ``_run_analysis_job``
   with the same arguments, so the provenance recorded by the analysis
   process is executor-agnostic.

3. **Task metadata consistency** – the workflow task exposes the same
   parameters and branch-map logic for every executor mode.

4. **Explicitly allowed differences** – only the ``workflow`` parameter
   value, Dask-specific scheduler/worker parameters, and the HTCondor
   infrastructure helpers (``htcondor_output_directory``,
   ``htcondor_job_config``) are legitimately executor-specific.

Tests do **not** require a running HTCondor cluster, Dask scheduler,
CMS/ATLAS software environment, or a VOMS proxy.

Design notes
------------
* All tests are pure unit tests that exercise the task API through mocks
  and temporary directories.
* The ``_run_analysis_job`` helper is exercised end-to-end so that the
  parity of the local and Dask code paths is verified directly.
* Each test class focuses on one parity dimension and documents its scope
  in its docstring.
"""

from __future__ import annotations

import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Path setup – make law/ and core/python importable
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR   = os.path.join(_REPO_ROOT, "law")
_CORE_PY   = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PY):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Optional dependency guard
# ---------------------------------------------------------------------------
try:
    import luigi   # noqa: F401
    import law     # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _stub_rucio():
    """Ensure rucio stubs are in sys.modules (idempotent)."""
    import unittest.mock as _mock
    for mod in ("rucio", "rucio.client"):
        if mod not in sys.modules:
            sys.modules[mod] = _mock.MagicMock()


def _load_nano():
    """Return the nano_tasks module with Rucio stubbed out."""
    _stub_rucio()
    import law as _law
    _law.contrib.load("htcondor")
    import nano_tasks
    return nano_tasks


def _load_opendata():
    """Return the opendata_tasks module."""
    import law as _law
    _law.contrib.load("htcondor")
    import opendata_tasks
    return opendata_tasks


# ===========================================================================
# Schema parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestManifestSchemaParity(unittest.TestCase):
    """
    Verify that ``OutputManifest`` and its sub-schemas behave consistently
    regardless of which executor is used to produce job outputs.

    Because the manifest is written by the analysis executable (not the
    executor), its structure must be executor-agnostic.  These tests confirm
    that:

    * The same manifest round-trips correctly through YAML serialisation.
    * Schema version checks are independent of the executor.
    * The ``resolve_manifest()`` API returns consistent results for manifests
      produced under any executor.
    """

    def _make_manifest(self, framework_hash="fw_hash_abc", user_hash="user_hash_xyz"):
        from output_schema import (
            OutputManifest, SkimSchema, MetadataSchema, LawArtifactSchema,
        )
        return OutputManifest(
            skim=SkimSchema(output_file="output.root", tree_name="Events"),
            metadata=MetadataSchema(output_file="output_meta.root"),
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type="run_job",
                    path_pattern="job_outputs/job_*.done",
                    format="text",
                ),
            ],
            framework_hash=framework_hash,
            user_repo_hash=user_hash,
        )

    def test_manifest_validates_without_errors(self):
        """A well-formed manifest validates successfully."""
        manifest = self._make_manifest()
        errors = manifest.validate()
        self.assertEqual(errors, [],
                         f"Manifest validation failed: {errors}")

    def test_manifest_round_trip_yaml(self):
        """Manifest serialises to YAML and back without data loss."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = self._make_manifest()
            path = os.path.join(tmpdir, "manifest.yaml")
            from output_schema import OutputManifest
            manifest.save_yaml(path)
            loaded = OutputManifest.load_yaml(path)

            self.assertEqual(loaded.manifest_version,       manifest.manifest_version)
            self.assertEqual(loaded.framework_hash,         manifest.framework_hash)
            self.assertEqual(loaded.user_repo_hash,         manifest.user_repo_hash)
            self.assertEqual(loaded.skim.output_file,       manifest.skim.output_file)
            self.assertEqual(loaded.metadata.output_file,   manifest.metadata.output_file)
            self.assertEqual(len(loaded.law_artifacts),     len(manifest.law_artifacts))

    def test_resolve_manifest_compatible_when_provenance_matches(self):
        """
        resolve_manifest() returns COMPATIBLE for every schema when the
        manifest provenance matches the current provenance.
        """
        from output_schema import (
            ArtifactResolutionStatus, ProvenanceRecord, resolve_manifest,
        )
        manifest = self._make_manifest(framework_hash="same", user_hash="same_user")
        current = ProvenanceRecord(framework_hash="same", user_repo_hash="same_user")
        statuses = resolve_manifest(manifest, current_provenance=current)
        for role, status in statuses.items():
            self.assertEqual(
                status, ArtifactResolutionStatus.COMPATIBLE,
                f"Schema '{role}' should be COMPATIBLE but is {status}",
            )

    def test_resolve_manifest_stale_when_provenance_differs(self):
        """
        resolve_manifest() returns STALE when the manifest was produced with
        a different framework hash (simulating a code update).
        """
        from output_schema import (
            ArtifactResolutionStatus, ProvenanceRecord, resolve_manifest,
        )
        manifest = self._make_manifest(framework_hash="old_hash")
        current = ProvenanceRecord(framework_hash="new_hash")
        statuses = resolve_manifest(manifest, current_provenance=current)
        # At least the skim/metadata should be STALE
        stale = [r for r, s in statuses.items()
                 if s == ArtifactResolutionStatus.STALE]
        self.assertTrue(
            len(stale) > 0,
            "Expected at least one STALE schema when framework hash changed",
        )

    def test_check_version_compatibility_passes_for_current_manifest(self):
        """
        OutputManifest.check_version_compatibility() raises no exception for
        a manifest produced with the current schema versions.
        """
        from output_schema import OutputManifest
        manifest = self._make_manifest()
        # Should not raise
        try:
            OutputManifest.check_version_compatibility(manifest)
        except Exception as exc:
            self.fail(
                f"check_version_compatibility raised unexpectedly: {exc}"
            )

    def test_schema_registry_contains_all_schema_types(self):
        """
        SCHEMA_REGISTRY lists all known schema names – used by tooling that
        needs to enumerate schemas independently of the executor.
        """
        from output_schema import SCHEMA_REGISTRY
        for name in ("skim", "histogram", "metadata", "cutflow",
                     "law_artifact", "output_manifest"):
            self.assertIn(name, SCHEMA_REGISTRY,
                          f"SCHEMA_REGISTRY is missing entry for '{name}'")


# ===========================================================================
# 8. End-to-end execution parity (_run_analysis_job)
# ===========================================================================
