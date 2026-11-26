"""Merge partition artifacts described by output manifests."""

from dataclasses import dataclass, replace
import fcntl
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from output_schema import (
    OutputManifest,
    MetadataSchema,
    SystematicColumnInventory,
    SystematicColumnEntry,
    merge_manifests,
)
from .performance import PerformanceRecorder, perf_path_for
from metadata_io import (merge_root_files, load_metadata_records, build_bundle,
                         validate_metadata_bundle)
from extract_nd_histograms import (
    extract_nd_histograms,
    read_embedded_histogram_schema,
    validate_compatible_histogram_schemas,
)
from run_bundle import write_json


from . import REPOSITORY_ROOT

WORKSPACE = str(REPOSITORY_ROOT)
DEFAULT_MANIFEST_GLOB = "**/output_manifest.yaml"
MERGED_SUBDIRECTORIES = {
    "skim": "skims", "histograms": "histograms", "cutflow": "cutflows", "metadata": "metadata",
}


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


class _ManifestMergeOperations:
    def _manifest_path(self, role):
        return os.path.join(self._merge_dir, MERGED_SUBDIRECTORIES[role], "output_manifest.yaml")

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

    def _merge_complete(self, manifest_path: str, role: str) -> bool:
        """Require the manifest and every merged artifact it describes.

        A manifest is written last, but it can outlive a ROOT/JSON artifact if
        an operator removes or truncates one after a completed run. Treat such
        a cache as stale so the caller can rebuild it.
        """
        try:
            manifest = OutputManifest.load_yaml(manifest_path)
            if manifest.validate():
                return False
            schema = getattr(manifest, role)
            if schema is None or not schema.output_file:
                return False
            primary = schema.output_file
            if not os.path.isabs(primary):
                primary = os.path.join(os.path.dirname(manifest_path), primary)

            if role == "metadata":
                with open(primary, "r", encoding="utf-8") as stream:
                    summary = json.load(stream)
                if not isinstance(summary, dict):
                    return False
                records = summary.get("provenance_records")
                if not isinstance(records, list) or not records or summary.get("n_inputs") != len(records):
                    return False
                if summary.get("metadata_bundle") is not None:
                    from metadata_io import expand_bundle
                    expand_bundle(summary["metadata_bundle"])
                return True

            groups_name = {
                "skim": "merged_skim_groups.json",
                "histograms": "merged_histogram_groups.json",
                "cutflow": "merged_cutflow_groups.json",
            }[role]
            with open(os.path.join(os.path.dirname(manifest_path), groups_name),
                      "r", encoding="utf-8") as stream:
                groups = json.load(stream)
            paths = list(groups.values()) if isinstance(groups, dict) else []
            paths = [os.path.abspath(os.path.join(os.path.dirname(manifest_path), path))
                     for path in paths]
            if os.path.abspath(primary) not in paths:
                return False

            import uproot  # type: ignore

            for resolved in dict.fromkeys(paths):
                if not os.path.isfile(resolved) or os.path.getsize(resolved) <= 0:
                    return False
                if role != "skim":
                    validate_metadata_bundle(resolved)
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

    def _skim_merge_complete(self, manifest_path: str) -> bool:
        # Keep the index, manifest and ROOT reads within one publication.
        try:
            directory = os.open(Path(manifest_path).parent, os.O_RDONLY | os.O_DIRECTORY)
        except FileNotFoundError:
            return False
        try:
            try:
                fcntl.flock(directory, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            return self._merge_complete(manifest_path, "skim")
        finally:
            os.close(directory)

    def merge_skims(self):
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

        merged = self._build_merged_manifest(
            [m for _, m in skim_manifests], required_roles=["skim"]
        )

        # Collect skim output file paths from manifests
        skim_files: List[str] = []
        for p, m in skim_manifests:
            skim_path = m.skim.output_file  # type: ignore[union-attr]
            skim_path = _resolve_manifest_root_path(p, skim_path)
            _require_local_root_input(skim_path, "Skim", p)
            skim_files.append(skim_path)

        if self.group_production_outputs:
            groups = {}
            for (_, entry), path in zip(skim_manifests, skim_files):
                provenance = entry.dataset_manifest_provenance
                names = provenance.resolved_entry_names if provenance is not None else []
                if len(names) != 1 or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", names[0]):
                    raise ValueError(f"Verified skim needs one valid dataset name: {path}")
                groups.setdefault(f"{names[0]}.root", []).append(path)
        else:
            groups = _group_files_by_basename(skim_files)
        self.publish_message(
            f"Merging {len(skim_files)} skim file(s) in "
            f"{len(groups)} group(s): {sorted(groups.keys())}"
        )

        out = Path(out_dir)
        manifest = Path(self._manifest_path('skim'))
        index_name = "merged_skim_groups.json"
        perf_name = "merge_skims.perf.json"
        if groups.keys() & {manifest.name, index_name, perf_name}:
            raise ValueError("Skim basename conflicts with a publication metadata filename.")
        with tempfile.TemporaryDirectory(dir=out, prefix=".skim-merge-") as staging:
            staged = Path(staging)
            with PerformanceRecorder("MergeSkims") as rec:
                for basename, paths in groups.items():
                    self.publish_message(f"hadd: {len(paths)} → {out / basename}")
                    _run_hadd(str(staged / basename), paths)

            rec.save(str(staged / perf_name))
            groups_meta = {name: str(out / name) for name in sorted(groups)}
            merged.skim.output_file = groups_meta[min(groups)]
            with (staged / index_name).open("w") as stream:
                json.dump(groups_meta, stream, indent=2)
            merged.save_yaml(str(staged / manifest.name))

            # The public manifest is the commit marker. A retry can rebuild
            # every group even after process death releases the directory lock.
            directory = os.open(out, os.O_RDONLY | os.O_DIRECTORY)
            try:
                fcntl.flock(directory, fcntl.LOCK_EX)
                manifest.unlink(missing_ok=True)
                for name in (*groups_meta, perf_name, index_name, manifest.name):
                    os.replace(staged / name, out / name)
            finally:
                os.close(directory)

        self.publish_message(f"Merged manifest written to: {self._manifest_path('skim')}")

    def merge_histograms(self):
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

        merged = self._build_merged_manifest(
            [m for _, m in hist_manifests], required_roles=["histograms"]
        )

        hist_files: List[str] = []
        for p, m in hist_manifests:
            hist_path = m.histograms.output_file  # type: ignore[union-attr]
            hist_path = _resolve_manifest_root_path(p, hist_path)
            _require_local_root_input(hist_path, "Histogram", p)
            hist_files.append(hist_path)

        groups = ({"histograms.root": hist_files} if self.group_production_outputs
                  else _group_files_by_basename(hist_files))
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
        coverage_prediction = {}
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
                                coverage_prediction.setdefault(base, set()).add(suffix)
        if coverage_prediction:
            n_complete = sum(
                1 for directions in coverage_prediction.values() if len(directions) == 2
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
                merge_root_files(
                    out_path, paths, role="histograms", hadd=_find_hadd(),
                    postprocess=(lambda path: extract_nd_histograms(path, path))
                    if group_schemas[basename] is not None else None,
                )
                merged_output_files[basename] = out_path

        rec.save(os.path.join(out_dir, "merge_histograms.perf.json"))

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

        merged.save_yaml(self._manifest_path('histograms'))
        self.publish_message(f"Merged manifest written to: {self._manifest_path('histograms')}")

    def merge_cutflows(self):
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

        merged = self._build_merged_manifest(
            [m for _, m in cf_manifests], required_roles=["cutflow"]
        )

        cf_files: List[str] = []
        for p, m in cf_manifests:
            cf_path = m.cutflow.output_file  # type: ignore[union-attr]
            cf_path = _resolve_manifest_root_path(p, cf_path)
            _require_local_root_input(cf_path, "Cutflow", p)
            cf_files.append(cf_path)

        groups = ({"cutflows.root": cf_files} if self.group_production_outputs
                  else _group_files_by_basename(cf_files))
        self.publish_message(
            f"Merging {len(cf_files)} cutflow file(s) in "
            f"{len(groups)} group(s): {sorted(groups.keys())}"
        )

        merged_output_files: Dict[str, str] = {}
        with PerformanceRecorder("MergeCutflows") as rec:
            for basename, paths in groups.items():
                out_path = os.path.join(out_dir, basename)
                self.publish_message(f"hadd: {len(paths)} → {out_path}")
                merge_root_files(out_path, paths, role="cutflow", hadd=_find_hadd())
                merged_output_files[basename] = out_path

        rec.save(os.path.join(out_dir, "merge_cutflows.perf.json"))

        primary_basename = sorted(merged_output_files.keys())[0]
        merged.cutflow.output_file = merged_output_files[primary_basename]  # type: ignore[union-attr]

        groups_meta = {k: v for k, v in sorted(merged_output_files.items())}
        with open(os.path.join(out_dir, "merged_cutflow_groups.json"), "w") as fh:
            json.dump(groups_meta, fh, indent=2)

        merged.save_yaml(self._manifest_path('cutflow'))
        self.publish_message(f"Merged manifest written to: {self._manifest_path('cutflow')}")

    def merge_metadata(self):
        out_dir = os.path.join(self._merge_dir, "metadata")
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        manifest_paths = self._find_manifest_paths()
        self.publish_message(f"Found {len(manifest_paths)} manifest(s).")

        loaded = self._load_manifests(manifest_paths)

        # For metadata we accept any manifests (schema presence is optional)
        all_manifests = [m for _, m in loaded]
        merged = self._build_merged_manifest(all_manifests)

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

            root_records = []
            seen_metadata_paths = set()
            for manifest_path, manifest in loaded:
                if manifest.metadata is None or not manifest.metadata.output_file:
                    continue
                metadata_path = _resolve_manifest_root_path(
                    manifest_path, manifest.metadata.output_file)
                if metadata_path in seen_metadata_paths:
                    raise ValueError(f"Duplicate metadata artifact: {metadata_path}")
                seen_metadata_paths.add(metadata_path)
                # Manifests may describe metadata-only tasks with no ROOT artifact.
                if metadata_path.endswith((".root", ".json", ".json.gz")):
                    root_records.extend(load_metadata_records(metadata_path))
            metadata_bundle = build_bundle(root_records) if root_records else None
            summary_path = os.path.join(out_dir, "provenance_summary.json")
            with open(summary_path, "w") as fh:
                json.dump(
                    {
                        "n_inputs": len(loaded),
                        "provenance_records": provenance_records,
                        "metadata_bundle": metadata_bundle,
                    },
                    fh,
                    indent=2,
                )

        rec.save(os.path.join(out_dir, "merge_metadata.perf.json"))

        if merged.metadata is None:
            merged.metadata = MetadataSchema(output_file=summary_path)
        else:
            merged.metadata.output_file = summary_path

        merged.save_yaml(self._manifest_path('metadata'))
        self.publish_message(
            f"Provenance summary written to: {summary_path}\n"
            f"Merged manifest written to: {self._manifest_path('metadata')}"
        )


@dataclass
class _MergeRequest(_ManifestMergeOperations):
    input_dir: str
    output_dir: str
    manifest_glob: str
    group_production_outputs: bool
    framework_hash: str
    user_repo_hash: str

    publish_message = staticmethod(print)

    @property
    def _effective_input_dir(self) -> str:
        return os.path.abspath(self.input_dir)

    @property
    def _merge_dir(self) -> str:
        return os.path.abspath(self.output_dir)


def write_merge_summary(out_dir, name, merged_manifests):
    """Publish the summary last, once the individual manifests are ready."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {"name": name, "merge_dir": str(out_dir), "merged_manifests": merged_manifests}
    write_json(out_dir / "merge_summary.json", summary)
    write_json(out_dir / "merge_all.done", summary)


def merge_manifest_outputs(input_dir, output_dir, *, manifest_glob=DEFAULT_MANIFEST_GLOB,
                           group_production_outputs=False, metadata_manifest_glob="",
                           framework_hash="", user_repo_hash="", name="manifest"):
    """Merge and validate each role present in a directory of manifests."""
    request = _MergeRequest(str(input_dir), str(output_dir), manifest_glob, group_production_outputs,
                            framework_hash, user_repo_hash)
    inventory = request._load_manifests(request._find_manifest_paths())
    merged = {}
    for task_name, role, operation in (
        ("MergeSkims", "skim", "merge_skims"),
        ("MergeHistograms", "histograms", "merge_histograms"),
        ("MergeCutflows", "cutflow", "merge_cutflows"),
        ("MergeMetadata", "metadata", "merge_metadata"),
    ):
        if role != "metadata" and not any(getattr(manifest, role) for _, manifest in inventory):
            continue
        current = request
        if role == "metadata" and metadata_manifest_glob:
            current = replace(request, manifest_glob=metadata_manifest_glob)
        getattr(current, operation)()
        manifest_path = current._manifest_path(role)
        complete = (current._skim_merge_complete(manifest_path) if role == "skim"
                    else current._merge_complete(manifest_path, role))
        if not complete:
            raise RuntimeError(f"merged {role} artifact failed verification: {manifest_path}")
        merged[task_name] = manifest_path

    write_merge_summary(request._merge_dir, name, merged)
    return merged
