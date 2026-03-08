"""
Law tasks for merging and reducing partitioned analysis outputs.

Provides a first-class reduction layer for law workflows that merges outputs
from partitioned skims, histograms, metadata, cutflows, and other framework
artifacts produced by the NANO and Open Data analysis workflows.

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
  source law/env.sh

  # After running analysis jobs with PrepareNANOSample / RunNANOJobs:

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

import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import luigi  # type: ignore
import law  # type: ignore

# ---------------------------------------------------------------------------
# Make sure core/python and law/ are importable regardless of invocation path
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from output_schema import (  # noqa: E402
    OutputManifest,
    MergeInputValidationError,
    merge_manifests,
    validate_merge_inputs,
)
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))

#: Default glob pattern (relative to input_dir) for finding job manifests.
DEFAULT_MANIFEST_GLOB = "**/output_manifest.yaml"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_hadd() -> str:
    """Return the path to the ROOT ``hadd`` binary.

    Searches in:
    1. The local build directory: ``build/bin/hadd``.
    2. System PATH via :func:`shutil.which`.

    Raises
    ------
    RuntimeError
        If ``hadd`` cannot be found.
    """
    candidates = [
        os.path.join(WORKSPACE, "build", "bin", "hadd"),
        "hadd",
    ]
    for candidate in candidates:
        if os.path.isabs(candidate):
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        else:
            found = shutil.which(candidate)
            if found:
                return found
    raise RuntimeError(
        "ROOT hadd binary not found.  Either build ROOT or source a CMSSW environment."
    )


def _run_hadd(output_path: str, input_paths: List[str]) -> None:
    """Run ``hadd`` to merge *input_paths* into *output_path*.

    Parameters
    ----------
    output_path:
        Destination file.  Parent directories are created automatically.
    input_paths:
        Non-empty list of input ROOT files.

    Raises
    ------
    RuntimeError
        If ``hadd`` exits with a non-zero status.
    ValueError
        If *input_paths* is empty.
    """
    if not input_paths:
        raise ValueError("_run_hadd: no input files provided.")

    hadd_bin = _find_hadd()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    cmd = [hadd_bin, "-f", output_path] + list(input_paths)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"hadd failed (exit {result.returncode}) merging "
            f"{len(input_paths)} file(s) into {output_path!r}.\n"
            f"stderr: {result.stderr.strip()}"
        )


def _group_files_by_basename(
    file_paths: List[str],
) -> Dict[str, List[str]]:
    """Group file paths by their basename.

    This naturally handles systematic-variation and multi-region outputs:
    files named ``skim_jesUp.root`` from different job directories are
    grouped together under the key ``skim_jesUp.root``, while
    ``skim_jesDown.root`` files form a separate group.

    Parameters
    ----------
    file_paths:
        Absolute paths to output ROOT files.

    Returns
    -------
    dict[str, list[str]]
        Mapping of ``basename → sorted list of matching paths``.
    """
    groups: Dict[str, List[str]] = defaultdict(list)
    for p in file_paths:
        groups[os.path.basename(p)].append(p)
    return {k: sorted(v) for k, v in groups.items()}


# ---------------------------------------------------------------------------
# Shared parameter mixin
# ---------------------------------------------------------------------------


class MergeMixin:
    """Parameters and helpers shared by all merge tasks."""

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

    # ---- manifest discovery & validation -----------------------------------

    def _find_manifest_paths(self) -> List[str]:
        """Return sorted list of manifest YAML paths found under input_dir.

        Raises
        ------
        RuntimeError
            If the input directory does not exist or no manifests are found.
        """
        import glob as _glob

        input_dir = self._effective_input_dir
        if not os.path.isdir(input_dir):
            raise RuntimeError(
                f"Input directory not found: {input_dir!r}.  "
                "Run the analysis jobs first (e.g. RunNANOJobs or RunOpenDataJobs)."
            )
        pattern = os.path.join(input_dir, self.manifest_glob)
        paths = sorted(_glob.glob(pattern, recursive=True))
        if not paths:
            raise RuntimeError(
                f"No manifests found matching {pattern!r}.  "
                "Ensure the analysis jobs have completed and written "
                "output_manifest.yaml files."
            )
        return paths

    def _load_manifests(
        self, manifest_paths: List[str]
    ) -> List[Tuple[str, OutputManifest]]:
        """Load all manifests; raise ``RuntimeError`` on any load failure.

        Parameters
        ----------
        manifest_paths:
            Paths returned by :meth:`_find_manifest_paths`.

        Returns
        -------
        list[(path, OutputManifest)]
        """
        loaded: List[Tuple[str, OutputManifest]] = []
        errors: List[str] = []
        for p in manifest_paths:
            try:
                loaded.append((p, OutputManifest.load_yaml(p)))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"  Cannot load {p!r}: {exc}")
        if errors:
            raise RuntimeError(
                f"Failed to load {len(errors)} manifest(s):\n" + "\n".join(errors)
            )
        return loaded

    def _validate(
        self,
        manifests: List[OutputManifest],
        required_roles: Optional[List[str]] = None,
    ) -> None:
        """Validate manifests for merge compatibility.

        Parameters
        ----------
        manifests:
            Loaded :class:`OutputManifest` objects.
        required_roles:
            Role names that must be present in every manifest.

        Raises
        ------
        MergeInputValidationError
            If any validation check fails.
        """
        errors = validate_merge_inputs(manifests, required_roles=required_roles)
        if errors:
            raise MergeInputValidationError(
                "Pre-merge validation failed:\n"
                + "\n".join(f"  {e}" for e in errors)
            )

    def _build_merged_manifest(
        self, manifests: List[OutputManifest], required_roles: Optional[List[str]] = None
    ) -> OutputManifest:
        """Build merged manifest via :func:`~output_schema.merge_manifests`."""
        return merge_manifests(
            manifests,
            framework_hash=self.framework_hash or None,
            user_repo_hash=self.user_repo_hash or None,
            required_roles=required_roles,
        )


# ---------------------------------------------------------------------------
# Task 1 – MergeSkims
# ---------------------------------------------------------------------------


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

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._merge_dir, "skims", "output_manifest.yaml")
        )

    def run(self):
        out_dir = os.path.join(self._merge_dir, "skims")
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        manifest_paths = self._find_manifest_paths()
        self.publish_message(f"Found {len(manifest_paths)} manifest(s).")

        loaded = self._load_manifests(manifest_paths)
        all_manifests = [m for _, m in loaded]

        # Filter to manifests that have a skim schema
        skim_manifests = [(p, m) for p, m in loaded if m.skim is not None]
        if not skim_manifests:
            raise RuntimeError(
                "None of the discovered manifests contain a 'skim' schema.  "
                "Pass --required-roles skim to enforce this requirement."
            )
        self.publish_message(
            f"{len(skim_manifests)} manifest(s) contain a skim schema."
        )

        self._validate([m for _, m in skim_manifests], required_roles=["skim"])

        # Collect skim output file paths from manifests
        skim_files: List[str] = []
        for p, m in skim_manifests:
            job_dir = os.path.dirname(p)
            skim_path = m.skim.output_file  # type: ignore[union-attr]
            if not os.path.isabs(skim_path):
                skim_path = os.path.join(job_dir, skim_path)
            if not os.path.isfile(skim_path):
                raise RuntimeError(
                    f"Skim file not found: {skim_path!r}  (referenced in {p!r})"
                )
            skim_files.append(skim_path)

        groups = _group_files_by_basename(skim_files)
        self.publish_message(
            f"Merging {len(skim_files)} skim file(s) in "
            f"{len(groups)} group(s): {sorted(groups.keys())}"
        )

        merged_output_files: Dict[str, str] = {}
        with PerformanceRecorder("MergeSkims") as rec:
            for basename, paths in groups.items():
                out_path = os.path.join(out_dir, basename)
                self.publish_message(
                    f"hadd: {len(paths)} → {out_path}"
                )
                _run_hadd(out_path, paths)
                merged_output_files[basename] = out_path

        rec.save(os.path.join(out_dir, "merge_skims.perf.json"))

        # Build merged manifest using the first skim group as the template
        merged = self._build_merged_manifest(
            [m for _, m in skim_manifests], required_roles=["skim"]
        )
        # Update output_file to the primary (first) merged skim
        primary_basename = sorted(merged_output_files.keys())[0]
        merged.skim.output_file = merged_output_files[primary_basename]  # type: ignore[union-attr]

        # Persist a sidecar JSON listing all merged groups for downstream use
        groups_meta = {k: v for k, v in sorted(merged_output_files.items())}
        with open(os.path.join(out_dir, "merged_skim_groups.json"), "w") as fh:
            json.dump(groups_meta, fh, indent=2)

        merged.save_yaml(self.output().path)
        self.publish_message(f"Merged manifest written to: {self.output().path}")


# ---------------------------------------------------------------------------
# Task 2 – MergeHistograms
# ---------------------------------------------------------------------------


class MergeHistograms(MergeMixin, law.Task):
    """Merge per-job histogram ROOT files into one (or more) merged file(s).

    Groups histogram output files by basename so that separate merge
    operations are performed for each systematic variation or region.

    Outputs are written to ``<merge_dir>/histograms/``.
    """

    task_namespace = ""

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._merge_dir, "histograms", "output_manifest.yaml")
        )

    def run(self):
        out_dir = os.path.join(self._merge_dir, "histograms")
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        manifest_paths = self._find_manifest_paths()
        self.publish_message(f"Found {len(manifest_paths)} manifest(s).")

        loaded = self._load_manifests(manifest_paths)

        hist_manifests = [(p, m) for p, m in loaded if m.histograms is not None]
        if not hist_manifests:
            raise RuntimeError(
                "None of the discovered manifests contain a 'histograms' schema."
            )
        self.publish_message(
            f"{len(hist_manifests)} manifest(s) contain a histograms schema."
        )

        self._validate([m for _, m in hist_manifests], required_roles=["histograms"])

        hist_files: List[str] = []
        for p, m in hist_manifests:
            job_dir = os.path.dirname(p)
            hist_path = m.histograms.output_file  # type: ignore[union-attr]
            if not os.path.isabs(hist_path):
                hist_path = os.path.join(job_dir, hist_path)
            if not os.path.isfile(hist_path):
                raise RuntimeError(
                    f"Histogram file not found: {hist_path!r}  (referenced in {p!r})"
                )
            hist_files.append(hist_path)

        groups = _group_files_by_basename(hist_files)
        self.publish_message(
            f"Merging {len(hist_files)} histogram file(s) in "
            f"{len(groups)} group(s): {sorted(groups.keys())}"
        )

        merged_output_files: Dict[str, str] = {}
        with PerformanceRecorder("MergeHistograms") as rec:
            for basename, paths in groups.items():
                out_path = os.path.join(out_dir, basename)
                self.publish_message(f"hadd: {len(paths)} → {out_path}")
                _run_hadd(out_path, paths)
                merged_output_files[basename] = out_path

        rec.save(os.path.join(out_dir, "merge_histograms.perf.json"))

        merged = self._build_merged_manifest(
            [m for _, m in hist_manifests], required_roles=["histograms"]
        )
        primary_basename = sorted(merged_output_files.keys())[0]
        merged.histograms.output_file = merged_output_files[primary_basename]  # type: ignore[union-attr]

        groups_meta = {k: v for k, v in sorted(merged_output_files.items())}
        with open(os.path.join(out_dir, "merged_histogram_groups.json"), "w") as fh:
            json.dump(groups_meta, fh, indent=2)

        merged.save_yaml(self.output().path)
        self.publish_message(f"Merged manifest written to: {self.output().path}")


# ---------------------------------------------------------------------------
# Task 3 – MergeCutflows
# ---------------------------------------------------------------------------


class MergeCutflows(MergeMixin, law.Task):
    """Merge per-job cutflow ROOT files into one merged cutflow file.

    ``hadd`` is called once per unique cutflow output basename.  All
    counter objects within each merged file are summed automatically by
    ROOT.

    Outputs are written to ``<merge_dir>/cutflows/``.
    """

    task_namespace = ""

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._merge_dir, "cutflows", "output_manifest.yaml")
        )

    def run(self):
        out_dir = os.path.join(self._merge_dir, "cutflows")
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        manifest_paths = self._find_manifest_paths()
        self.publish_message(f"Found {len(manifest_paths)} manifest(s).")

        loaded = self._load_manifests(manifest_paths)

        cf_manifests = [(p, m) for p, m in loaded if m.cutflow is not None]
        if not cf_manifests:
            raise RuntimeError(
                "None of the discovered manifests contain a 'cutflow' schema."
            )
        self.publish_message(
            f"{len(cf_manifests)} manifest(s) contain a cutflow schema."
        )

        self._validate([m for _, m in cf_manifests], required_roles=["cutflow"])

        cf_files: List[str] = []
        for p, m in cf_manifests:
            job_dir = os.path.dirname(p)
            cf_path = m.cutflow.output_file  # type: ignore[union-attr]
            if not os.path.isabs(cf_path):
                cf_path = os.path.join(job_dir, cf_path)
            if not os.path.isfile(cf_path):
                raise RuntimeError(
                    f"Cutflow file not found: {cf_path!r}  (referenced in {p!r})"
                )
            cf_files.append(cf_path)

        groups = _group_files_by_basename(cf_files)
        self.publish_message(
            f"Merging {len(cf_files)} cutflow file(s) in "
            f"{len(groups)} group(s): {sorted(groups.keys())}"
        )

        merged_output_files: Dict[str, str] = {}
        with PerformanceRecorder("MergeCutflows") as rec:
            for basename, paths in groups.items():
                out_path = os.path.join(out_dir, basename)
                self.publish_message(f"hadd: {len(paths)} → {out_path}")
                _run_hadd(out_path, paths)
                merged_output_files[basename] = out_path

        rec.save(os.path.join(out_dir, "merge_cutflows.perf.json"))

        merged = self._build_merged_manifest(
            [m for _, m in cf_manifests], required_roles=["cutflow"]
        )
        primary_basename = sorted(merged_output_files.keys())[0]
        merged.cutflow.output_file = merged_output_files[primary_basename]  # type: ignore[union-attr]

        groups_meta = {k: v for k, v in sorted(merged_output_files.items())}
        with open(os.path.join(out_dir, "merged_cutflow_groups.json"), "w") as fh:
            json.dump(groups_meta, fh, indent=2)

        merged.save_yaml(self.output().path)
        self.publish_message(f"Merged manifest written to: {self.output().path}")


# ---------------------------------------------------------------------------
# Task 4 – MergeMetadata
# ---------------------------------------------------------------------------


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

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._merge_dir, "metadata", "output_manifest.yaml")
        )

    def run(self):
        out_dir = os.path.join(self._merge_dir, "metadata")
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        manifest_paths = self._find_manifest_paths()
        self.publish_message(f"Found {len(manifest_paths)} manifest(s).")

        loaded = self._load_manifests(manifest_paths)

        # For metadata we accept any manifests (schema presence is optional)
        all_manifests = [m for _, m in loaded]
        errors = validate_merge_inputs(all_manifests)
        if errors:
            raise MergeInputValidationError(
                "Pre-merge validation failed:\n"
                + "\n".join(f"  {e}" for e in errors)
            )

        with PerformanceRecorder("MergeMetadata") as rec:
            # Collect provenance from all manifests
            provenance_records = []
            for p, m in loaded:
                record = {
                    "manifest_path": p,
                    "framework_hash": m.framework_hash,
                    "user_repo_hash": m.user_repo_hash,
                    "config_mtime": m.config_mtime,
                }
                provenance_records.append(record)

            summary_path = os.path.join(out_dir, "provenance_summary.json")
            with open(summary_path, "w") as fh:
                json.dump(
                    {
                        "n_inputs": len(loaded),
                        "provenance_records": provenance_records,
                    },
                    fh,
                    indent=2,
                )

        rec.save(os.path.join(out_dir, "merge_metadata.perf.json"))

        merged = merge_manifests(
            all_manifests,
            framework_hash=self.framework_hash or None,
            user_repo_hash=self.user_repo_hash or None,
        )
        # Point metadata output_file to the provenance summary we just wrote
        if merged.metadata is not None:
            merged.metadata.output_file = summary_path

        merged.save_yaml(self.output().path)
        self.publish_message(
            f"Provenance summary written to: {summary_path}\n"
            f"Merged manifest written to: {self.output().path}"
        )


# ---------------------------------------------------------------------------
# Task 5 – MergeAll
# ---------------------------------------------------------------------------


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

    def _shared_params(self):
        """Return shared MergeMixin parameter values for sub-task construction."""
        return dict(
            name=self.name,
            input_dir=self.input_dir,
            output_dir=self.output_dir,
            manifest_glob=self.manifest_glob,
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
            reqs.append(MergeMetadata(**params))

        return reqs

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self._merge_dir, "merge_all.done")
        )

    def run(self):
        out_dir = self._merge_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        # Collect paths to merged manifests from completed sub-tasks
        merged_manifests: Dict[str, str] = {}
        for req in self.requires():
            task_name = type(req).__name__
            merged_manifests[task_name] = req.output().path

        summary = {
            "name": self.name,
            "merge_dir": out_dir,
            "merged_manifests": merged_manifests,
        }
        summary_path = os.path.join(out_dir, "merge_summary.json")
        with open(summary_path, "w") as fh:
            json.dump(summary, fh, indent=2)

        self.publish_message(
            f"MergeAll complete for run '{self.name}'.  "
            f"Sub-tasks completed: {list(merged_manifests.keys())}.  "
            f"Summary: {summary_path}"
        )

        # Mark this orchestrating task as done
        with open(self.output().path, "w") as fh:
            fh.write(json.dumps(summary, indent=2) + "\n")
