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
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from output_schema import (  # noqa: E402
    OutputManifest,
    MergeInputValidationError,
    SystematicColumnInventory,
    SystematicColumnEntry,
    merge_manifests,
    validate_merge_inputs,
)
from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402
from extract_nd_histograms import (  # noqa: E402
    extract_nd_histograms,
    read_embedded_histogram_schema,
    validate_compatible_histogram_schemas,
)

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

#: Default glob pattern (relative to input_dir) for finding job manifests.
DEFAULT_MANIFEST_GLOB = "**/output_manifest.yaml"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _derive_systematic_family(base_name: str) -> str:
    """Map a systematic base name to a family grouping key.

    Examples:
      - ``"jesTotal"``, ``"jesAbsolute"`` -> ``"jes"``
      - ``"jer"`` -> ``"jer"``
      - ``"btag"``, ``"btagHF"`` -> ``"btag"``
      - ``"ctag"`` -> ``"ctag"``
      - ``"muon_id"``, ``"muon_iso"`` -> ``"muon"``
      - ``"electron_id"``, ``"electron_reco"`` -> ``"electron"``
      - ``"pileup"`` -> ``"pileup"``
      - ``"lumi"`` -> ``"lumi"``
      - ``"stxs_acceptance"``, ``"stxs_delta1"`` -> ``"stxs"``
      - ``"vjet_"`` prefixed -> ``"vjet"``
      - Everything else -> ``"other"``
    """
    lower = base_name.lower()

    # Known family prefixes (longest-first for specificity)
    known_prefixes = [
        ("jes", "jes"),
        ("jer", "jer"),
        ("btag", "btag"),
        ("ctag", "ctag"),
        ("muon", "muon"),
        ("electron", "electron"),
        ("pileup", "pileup"),
        ("lumi", "lumi"),
        ("stxs", "stxs"),
        ("vjet", "vjet"),
    ]
    for prefix, family in known_prefixes:
        if lower.startswith(prefix):
            return family
    return "other"


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
    """Run ``hadd`` to merge local or XRootD inputs into a local output.

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
            + (
                "\nFor root:// inputs, confirm this ROOT installation has "
                "XRootD support and the current credentials can read the files."
                if any(_is_xrootd_url(path) for path in input_paths)
                else ""
            )
        )


def _is_xrootd_url(path: str) -> bool:
    """Return whether *path* is a ROOT XRootD URL."""
    return path.startswith("root://")


def _resolve_manifest_root_path(manifest_path: str, root_path: str) -> str:
    """Resolve a manifest ROOT path while preserving remote ``root://`` URLs."""
    if _is_xrootd_url(root_path) or os.path.isabs(root_path):
        return root_path
    return os.path.join(os.path.dirname(manifest_path), root_path)


def _require_local_root_input(path: str, role: str, manifest_path: str) -> None:
    """Check local inputs early; remote inputs are validated by ROOT/hadd."""
    if _is_xrootd_url(path):
        return
    if not os.path.isfile(path):
        raise RuntimeError(
            f"{role} file not found: {path!r} (referenced in {manifest_path!r})"
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
                "Run the analysis jobs first (e.g. RunSkimJobs or RunOpenDataJobs)."
            )
        pattern = os.path.join(input_dir, self.manifest_glob)
        paths = sorted(_glob.glob(pattern, recursive=True))
        # A caller may put merged output beneath the input tree. Exclude the
        # task's own output directory so a later run cannot recursively merge
        # its previous result together with original job products.
        input_real = os.path.realpath(input_dir)
        output_real = os.path.realpath(self._merge_dir)
        try:
            output_is_nested = os.path.commonpath([input_real, output_real]) == input_real
        except ValueError:
            output_is_nested = False
        if output_is_nested:
            paths = [
                path for path in paths
                if os.path.commonpath([output_real, os.path.realpath(path)]) != output_real
            ]
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

    def complete(self) -> bool:
        """Require the manifest and every merged artifact it describes.

        A manifest is written last, but it can outlive a ROOT/JSON artifact if
        an operator removes or truncates one after a completed run. Treat such
        a cache as stale so Luigi can rebuild it.
        """
        if not super().complete():
            return False
        role = self.output_manifest_role
        if role is None:
            return True

        manifest_path = self.output().path
        try:
            manifest = OutputManifest.load_yaml(manifest_path)
            schema = getattr(manifest, role)
            if schema is None or not schema.output_file:
                return False
            primary = schema.output_file
            if not os.path.isabs(primary):
                primary = os.path.join(os.path.dirname(manifest_path), primary)

            if role == "metadata":
                with open(primary, "r", encoding="utf-8") as stream:
                    summary = json.load(stream)
                return isinstance(summary, dict)

            groups_name = {
                "skim": "merged_skim_groups.json",
                "histograms": "merged_histogram_groups.json",
                "cutflow": "merged_cutflow_groups.json",
            }[role]
            with open(os.path.join(os.path.dirname(manifest_path), groups_name),
                      "r", encoding="utf-8") as stream:
                groups = json.load(stream)
            paths = list(groups.values()) if isinstance(groups, dict) else []
            if not paths or primary not in paths:
                paths.append(primary)

            import uproot  # type: ignore

            for path in paths:
                resolved = path if os.path.isabs(path) else os.path.join(
                    os.path.dirname(manifest_path), path
                )
                if not os.path.isfile(resolved) or os.path.getsize(resolved) <= 0:
                    return False
                with uproot.open(resolved) as root_file:
                    if role == "skim":
                        tree_name = getattr(schema, "tree_name", "Events")
                        obj = root_file[tree_name]
                        classname = getattr(obj, "classname", None)
                        if classname is not None and classname not in (
                            "TTree", "ROOT::RNTuple"
                        ):
                            return False
                    elif not root_file.keys():
                        return False
            return True
        except Exception:  # malformed manifests or artifacts are stale targets
            return False


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
    output_manifest_role = "skim"

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
            skim_path = m.skim.output_file  # type: ignore[union-attr]
            skim_path = _resolve_manifest_root_path(p, skim_path)
            _require_local_root_input(skim_path, "Skim", p)
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
    output_manifest_role = "histograms"

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
            hist_path = m.histograms.output_file  # type: ignore[union-attr]
            hist_path = _resolve_manifest_root_path(p, hist_path)
            _require_local_root_input(hist_path, "Histogram", p)
            hist_files.append(hist_path)

        groups = _group_files_by_basename(hist_files)
        self.publish_message(
            f"Merging {len(hist_files)} histogram file(s) in "
            f"{len(groups)} group(s): {sorted(groups.keys())}"
        )

        # A THnSparse can only be added directly when every file uses the same
        # category-to-bin mapping and exact binning. Validate every group
        # before writing any merged output.
        group_schemas: Dict[str, Optional[dict]] = {}
        for basename, paths in groups.items():
            schemas = [(path, read_embedded_histogram_schema(path)) for path in paths]
            present = [(path, schema) for path, schema in schemas if schema is not None]
            if present and len(present) != len(schemas):
                missing = [path for path, schema in schemas if schema is None]
                raise RuntimeError(
                    f"Cannot merge mixed legacy and sparse-first histogram files for "
                    f"{basename!r}; files without the embedded schema: {missing}"
                )
            group_schemas[basename] = validate_compatible_histogram_schemas(present)

        # ---- Pre-merge coverage prediction ----
        # Collect all histogram names across input manifests to predict
        # which systematic variations will be available post-merge.
        coverage_prediction: Dict[str, List[str]] = {}
        for _, m in hist_manifests:
            if m.histograms:
                names = (m.histograms.axis_labels or {}).get("systematic", [])
                if not names:
                    names = m.histograms.histogram_names or []
                for name in names:
                    lower = name.lower()
                    for suffix in ("up", "down"):
                        if lower.endswith(suffix):
                            base = name[:-len(suffix)].rstrip("_-")
                            if base:
                                coverage_prediction.setdefault(base, []).append(name)
        if coverage_prediction:
            n_complete = sum(
                1 for v in coverage_prediction.values()
                if any("up" in x.lower() for x in v) and any("down" in x.lower() for x in v)
            )
            self.publish_message(
                f"Pre-merge coverage prediction: {len(coverage_prediction)} systematic "
                f"base(s) detected, {n_complete} with both up+down variations."
            )

        merged_output_files: Dict[str, str] = {}
        with PerformanceRecorder("MergeHistograms") as rec:
            for basename, paths in groups.items():
                out_path = os.path.join(out_dir, basename)
                self.publish_message(f"hadd: {len(paths)} → {out_path}")
                _run_hadd(out_path, paths)
                if group_schemas[basename] is not None:
                    self.publish_message(
                        f"Reconstructing single histograms in {out_path}"
                    )
                    extract_nd_histograms(out_path, out_path)
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

        # ---- Build systematic column inventory from histogram names ----
        all_hist_names: List[str] = []
        for _, m in hist_manifests:
            if m.histograms:
                systematic_labels = (m.histograms.axis_labels or {}).get(
                    "systematic", []
                )
                if systematic_labels:
                    all_hist_names.extend(
                        label for label in systematic_labels if label != "Nominal"
                    )
                elif m.histograms.histogram_names:
                    all_hist_names.extend(m.histograms.histogram_names)

        if all_hist_names:
            # Deduplicate while preserving order
            seen: set = set()
            unique_names: List[str] = []
            for n in all_hist_names:
                if n not in seen:
                    seen.add(n)
                    unique_names.append(n)

            # Derive systematic variations from histogram names
            # (e.g. "h_jesUp" -> base="jes", direction="up")
            syst_entries: Dict[str, SystematicColumnEntry] = {}
            for name in unique_names:
                lower = name.lower()
                direction = None
                base = None
                for suffix, d in [("up", "up"), ("down", "down")]:
                    if lower.endswith(suffix):
                        base_candidate = name[:-len(suffix)].rstrip("_-")
                        if base_candidate:
                            base = base_candidate
                            direction = d
                            break

                if base and direction:
                    if base not in syst_entries:
                        # Determine family from common prefixes
                        family = _derive_systematic_family(base)
                        syst_entries[base] = SystematicColumnEntry(
                            family=family,
                            base_name=base,
                            nominal_column="weight_nominal",
                            producer="NDHistogramManager",
                            affected_observable="weight",
                        )
                    entry = syst_entries[base]
                    if direction == "up":
                        entry.up_column = name
                    elif direction == "down":
                        entry.down_column = name

            if syst_entries:
                merged.column_inventory = SystematicColumnInventory(
                    entries=list(syst_entries.values())
                )
                self.publish_message(
                    f"Built systematic column inventory with "
                    f"{len(syst_entries)} entries: "
                    f"{sorted(syst_entries.keys())}"
                )

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
    output_manifest_role = "cutflow"

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
            cf_path = m.cutflow.output_file  # type: ignore[union-attr]
            cf_path = _resolve_manifest_root_path(p, cf_path)
            _require_local_root_input(cf_path, "Cutflow", p)
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
    output_manifest_role = "metadata"

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
