"""
Law tasks for merging and reducing partitioned analysis outputs.

Provides a first-class reduction layer for law workflows that merges outputs
from partitioned skims, histograms, metadata, cutflows, and other framework
artifacts produced by the Rucio and Open Data analysis workflows.

Merging validates inputs via :func:`~output_schema.validate_merge_inputs`,
preserves provenance via :func:`~output_schema.merge_manifests`, and is
resumable: if the output target already exists the task is considered complete
by the law task scheduler.

Systematic variations and multi-region outputs are handled automatically.
Output files are grouped by their basename (e.g. ``skim_jesUp.root`` and
``skim_jesDown.root`` are kept as separate groups) so that corresponding
per-job files for each variation or region are merged independently.

Workflow
--------
  MergeSkims      (Task)  – hadd all per-job skim ROOT files into merged skim(s)
  MergeHistograms (Task)  – hadd all per-job histogram ROOT files
  MergeCutflows   (Task)  – hadd all per-job cutflow ROOT files
  MergeMetadata   (Task)  – write merged manifest preserving full provenance
  MergeAll        (Task)  – requires all applicable merge sub-tasks

All tasks:
  - Validate inputs via :func:`~output_schema.validate_merge_inputs` before executing
  - Preserve provenance via :func:`~output_schema.merge_manifests`
  - Are resumable: existing output targets skip re-execution
  - Handle systematic variations: separate hadd per variation basename
  - Handle multi-region outputs: separate hadd per region basename
  - Record performance metrics via :class:`~performance_recorder.PerformanceRecorder`

Usage
-----
    source core/python/law/env.sh

  # After running analysis jobs with PrepareSkimJobs / RunSkimJobs:

  law run MergeHistograms \\
      --name myRun

  law run MergeSkims \\
      --name myRun

  law run MergeAll \\
      --name myRun

  # Override where to look for manifests
  law run MergeAll \\
      --name myRun \\
      --input-dir /path/to/condorSub_myRun \\
      --output-dir /path/to/merged_outputs
"""

from __future__ import annotations

import os
import sys
from typing import Optional

import law  # type: ignore
import luigi  # type: ignore

_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

from rdfproduction.manifest_merge import (  # noqa: E402, F401
    DEFAULT_MANIFEST_GLOB,
    OutputManifest,
    WORKSPACE,
    _ManifestMergeOperations,
    _derive_systematic_family,
    _find_hadd,
    _group_files_by_basename,
    _is_xrootd_url,
    _resolve_manifest_root_path,
    _run_hadd,
    write_merge_summary,
)


class MergeMixin(_ManifestMergeOperations):
    """Parameters and helpers shared by all merge tasks."""

    output_manifest_role: Optional[str] = None

    name = luigi.Parameter(
        description=(
            "Run name.  Merged outputs are written to "
            "<workspace>/mergeRun_<name>/.  Input job manifests are "
            "searched for under <workspace>/condorSub_<name>/ unless "
            "--input-dir is given."
        ),
    )
    input_dir = luigi.Parameter(
        default="",
        description=(
            "Directory to scan recursively for job output manifests "
            "(output_manifest.yaml).  Defaults to "
            "<workspace>/condorSub_<name>/ when not specified."
        ),
    )
    output_dir = luigi.Parameter(
        default="",
        description=(
            "Directory where merged output files and the merged manifest "
            "are written.  Defaults to <workspace>/mergeRun_<name>/."
        ),
    )
    manifest_glob = luigi.Parameter(
        default=DEFAULT_MANIFEST_GLOB,
        description=(
            "Glob pattern relative to --input-dir used to discover per-job "
            "output manifests.  Supports ** for recursive matching.  "
            f"Default: {DEFAULT_MANIFEST_GLOB!r}."
        ),
    )
    group_production_outputs = luigi.BoolParameter(
        default=False, description="Merge verified histograms/cutflows together and group skim trees by dataset.",
    )
    framework_hash = luigi.Parameter(
        default="",
        description=(
            "Framework git hash to record in the merged manifest provenance.  "
            "Leave blank to omit from provenance."
        ),
    )
    user_repo_hash = luigi.Parameter(
        default="",
        description=(
            "User-repository git hash to record in the merged manifest.  "
            "Leave blank to omit."
        ),
    )

    # ---- derived helpers ---------------------------------------------------

    @property
    def _effective_input_dir(self) -> str:
        """Resolved input directory (absolute path)."""
        if self.input_dir:
            return os.path.abspath(self.input_dir)
        return os.path.join(WORKSPACE, f"condorSub_{self.name}")

    @property
    def _merge_dir(self) -> str:
        """Root directory for all merged outputs."""
        if self.output_dir:
            return os.path.abspath(self.output_dir)
        return os.path.join(WORKSPACE, f"mergeRun_{self.name}")


    def complete(self) -> bool:
        if not super().complete():
            return False
        if self.output_manifest_role == "skim":
            return self._skim_merge_complete(self.output().path)
        if self.output_manifest_role is not None:
            return self._merge_complete(self.output().path, self.output_manifest_role)
        return True


class MergeSkims(MergeMixin, law.Task):
    """Merge per-job skim ROOT files into one (or more) merged skim file(s).

    For each unique skim output basename found across all job manifests
    (e.g. ``skim.root``, ``skim_jesUp.root``, ``skim_jesDown.root``) a
    separate ``hadd`` call is made.  This correctly handles systematic
    variations and multi-region outputs without any extra configuration.

    Outputs are written to ``<merge_dir>/skims/`` and a merged manifest
    is saved as ``<merge_dir>/skims/output_manifest.yaml``.
    """

    task_namespace = ""
    output_manifest_role = "skim"

    def output(self):
        return law.LocalFileTarget(self._manifest_path("skim"))

    def run(self):
        self.merge_skims()


class MergeHistograms(MergeMixin, law.Task):
    """Merge per-job histogram ROOT files into one (or more) merged file(s).

    Groups histogram output files by basename so that separate merge
    operations are performed for each systematic variation or region.

    Outputs are written to ``<merge_dir>/histograms/``.
    """

    task_namespace = ""
    output_manifest_role = "histograms"

    def output(self):
        return law.LocalFileTarget(self._manifest_path("histograms"))

    def run(self):
        self.merge_histograms()


class MergeCutflows(MergeMixin, law.Task):
    """Merge per-job cutflow ROOT files into one merged cutflow file.

    ``hadd`` is called once per unique cutflow output basename.  All
    counter objects within each merged file are summed automatically by
    ROOT.

    Outputs are written to ``<merge_dir>/cutflows/``.
    """

    task_namespace = ""
    output_manifest_role = "cutflow"

    def output(self):
        return law.LocalFileTarget(self._manifest_path("cutflow"))

    def run(self):
        self.merge_cutflows()


class MergeMetadata(MergeMixin, law.Task):
    """Aggregate provenance metadata from all per-job manifests.

    Does not merge ROOT files; instead it writes a consolidated
    ``output_manifest.yaml`` that records the provenance from all input
    jobs.  Downstream tasks (datacards, plotting, statistics) can use this
    manifest to verify the provenance chain without opening ROOT files.

    The merged manifest's ``metadata.output_file`` is set to the path of a
    ``provenance_summary.json`` file written alongside the manifest.

    Outputs are written to ``<merge_dir>/metadata/``.
    """

    task_namespace = ""
    output_manifest_role = "metadata"

    def output(self):
        return law.LocalFileTarget(self._manifest_path("metadata"))

    def run(self):
        self.merge_metadata()


class MergeAll(MergeMixin, law.Task):
    """Orchestrate all applicable merge sub-tasks for a single run.

    Determines which sub-tasks to require by inspecting the manifests found
    under ``--input-dir``.  Only tasks for schema types that are actually
    present across the discovered manifests are required; e.g. if no job
    produced skim outputs, ``MergeSkims`` is silently skipped.

    Parameters
    ----------
    skip_skims : BoolParameter
        Skip merging skim outputs even if skim manifests are found.
    skip_histograms : BoolParameter
        Skip merging histogram outputs.
    skip_cutflows : BoolParameter
        Skip merging cutflow outputs.
    skip_metadata : BoolParameter
        Skip writing the merged metadata manifest.
    """

    task_namespace = ""

    def complete(self) -> bool:
        if not super().complete():
            return False
        try:
            return all(task.complete() for task in self.requires())
        except Exception:
            return False

    skip_skims = luigi.BoolParameter(
        default=False,
        description="Skip merging skim ROOT files.",
    )
    skip_histograms = luigi.BoolParameter(
        default=False,
        description="Skip merging histogram ROOT files.",
    )
    skip_cutflows = luigi.BoolParameter(
        default=False,
        description="Skip merging cutflow ROOT files.",
    )
    skip_metadata = luigi.BoolParameter(
        default=False,
        description="Skip writing the merged provenance metadata manifest.",
    )
    metadata_manifest_glob = luigi.Parameter(
        default="", description="Restrict metadata provenance to one input pass of a composed workflow.",
    )

    def _shared_params(self):
        """Return shared MergeMixin parameter values for sub-task construction."""
        return dict(
            name=self.name,
            input_dir=self.input_dir,
            output_dir=self.output_dir,
            manifest_glob=self.manifest_glob,
            group_production_outputs=self.group_production_outputs,
            framework_hash=self.framework_hash,
            user_repo_hash=self.user_repo_hash,
        )

    def _probe_schemas(self):
        """Inspect discovered manifests to determine which schemas are present.

        Returns
        -------
        dict[str, bool]
            Mapping of schema role name to whether any manifest exposes it.
        """
        import glob as _glob

        input_dir = self._effective_input_dir
        if not os.path.isdir(input_dir):
            # Return all False so requires() stays empty until jobs are ready
            return {"skim": False, "histograms": False, "cutflow": False, "metadata": False}

        pattern = os.path.join(input_dir, self.manifest_glob)
        paths = sorted(_glob.glob(pattern, recursive=True))

        present = {"skim": False, "histograms": False, "cutflow": False, "metadata": False}
        for p in paths:
            try:
                m = OutputManifest.load_yaml(p)
            except Exception:  # noqa: BLE001
                continue
            if m.skim is not None:
                present["skim"] = True
            if m.histograms is not None:
                present["histograms"] = True
            if m.cutflow is not None:
                present["cutflow"] = True
            if m.metadata is not None:
                present["metadata"] = True
        return present

    def requires(self):
        params = self._shared_params()
        present = self._probe_schemas()
        reqs = []

        if present["skim"] and not self.skip_skims:
            reqs.append(MergeSkims(**params))
        if present["histograms"] and not self.skip_histograms:
            reqs.append(MergeHistograms(**params))
        if present["cutflow"] and not self.skip_cutflows:
            reqs.append(MergeCutflows(**params))
        if not self.skip_metadata:
            reqs.append(MergeMetadata(**{**params, "manifest_glob": self.metadata_manifest_glob or self.manifest_glob}))

        return reqs

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._merge_dir, "merge_all.done")
        )

    def run(self):
        merged_manifests = {type(req).__name__: req.output().path for req in self.requires()}
        write_merge_summary(self._merge_dir, self.name, merged_manifests)
        self.publish_message(
            f"MergeAll complete for run '{self.name}'. "
            f"Sub-tasks completed: {list(merged_manifests)}. "
            f"Summary: {os.path.join(self._merge_dir, 'merge_summary.json')}"
        )
