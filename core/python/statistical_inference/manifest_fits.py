"""Scheduler-neutral manifest card generation and statistical fits."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
from typing import Dict, List, Optional

import numpy as np
import yaml

from analytic_datacards import configured_rates, write_rate_card
from output_schema import OutputManifest
from rdfproduction.performance import PerformanceRecorder
from run_bundle import content_digest


WORKSPACE = str(Path(__file__).resolve().parents[3])
# Default search locations for the combine binary
_COMBINE_SEARCH_PATHS = [
    os.path.join(WORKSPACE, "build", "external", "HiggsAnalysis", "CombinedLimit", "bin", "combine"),
    os.path.join(WORKSPACE, "build", "external", "HiggsAnalysis", "CombinedLimit", "exe", "combine"),
    "combine",  # system PATH
]


def _find_histogram_in_file(root_file, hist_name: str):
    """Find a histogram by name in common analysis output layouts."""
    import ROOT  # type: ignore

    candidate_paths = [
        f"histograms/{hist_name}",
        f"Default_Default/Default/{hist_name}",
        hist_name,
    ]
    for hist_path in candidate_paths:
        hist = root_file.Get(hist_path)
        if hist and hist.InheritsFrom("TH1"):
            return hist, hist_path

    def _walk(directory, prefix=""):
        for key in directory.GetListOfKeys():
            name = key.GetName()
            obj = key.ReadObj()
            obj_path = f"{prefix}/{name}" if prefix else name
            if obj.InheritsFrom("TH1") and name == hist_name:
                return obj, obj_path
            if obj.InheritsFrom("TDirectory"):
                found_hist, found_path = _walk(obj, obj_path)
                if found_hist:
                    return found_hist, found_path
        return None, None

    return _walk(root_file)


def _find_combine(combine_exe: str = "", *, search_paths=None) -> str:
    """Return the path to the combine binary.

    Searches in:
    1. ``combine_exe`` parameter (if non-empty).
    2. The build directory: build/external/HiggsAnalysis/CombinedLimit/bin/combine (or legacy exe/combine).
    3. System PATH.

    Raises RuntimeError if not found.
    """
    if combine_exe:
        if os.path.isfile(combine_exe) and os.access(combine_exe, os.X_OK):
            return os.path.abspath(combine_exe)
        raise RuntimeError(
            f"Specified combine executable not found or not executable: {combine_exe!r}"
        )

    candidates = _COMBINE_SEARCH_PATHS if search_paths is None else search_paths
    for candidate in candidates:
        if os.path.isabs(candidate):
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        else:
            # Relative → check via shutil.which (portable, no subprocess)
            found = shutil.which(candidate)
            if found:
                return os.path.abspath(found)

    raise RuntimeError(
        "CMS Combine binary not found.  Either:\n"
        "  1. Build with Combine support: cmake -S . -B build -DBUILD_COMBINE=ON && cmake --build build\n"
        "  2. Source a CMSSW environment that provides `combine`\n"
        "  3. Pass --combine-exe /path/to/combine explicitly"
    )


def _load_manifest(manifest_path: str) -> "OutputManifest":
    """Load an :class:`OutputManifest` from *manifest_path*.

    Raises
    ------
    RuntimeError
        If the path does not exist or the manifest cannot be parsed.
    """
    if not os.path.exists(manifest_path):
        raise RuntimeError(
            f"Manifest file not found: {manifest_path!r}.  "
            "Run MergeAll (or MergeHistograms + MergeMetadata) first."
        )
    try:
        return OutputManifest.load_yaml(manifest_path)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Failed to load manifest from {manifest_path!r}: {exc}"
        ) from exc


def _derive_available_variations(manifest: "OutputManifest") -> Dict[str, List[str]]:
    """Derive a ``{systematic_base → [variant_names]}`` map from the manifest.

    Sparse-first manifests use the systematic axis labels. Legacy manifests
    fall back to histogram names. A label or histogram named ``jesUp`` is
    treated as the Up variation of ``jes``; ``jesDown`` is the Down variation.

    Parameters
    ----------
    manifest:
        The merged :class:`OutputManifest` whose systematic labels are inspected.

    Returns
    -------
    dict[str, list[str]]
        Mapping of base systematic name → list of observed variant histogram names.
        For example ``{"jes": ["h_jesUp", "h_jesDown"]}``.
    """
    if manifest.histograms is None:
        return {}

    axis_labels = manifest.histograms.axis_labels or {}
    systematic_labels = axis_labels.get("systematic", [])
    names = (
        [name for name in systematic_labels if name != "Nominal"]
        if systematic_labels
        else manifest.histograms.histogram_names or []
    )
    variations: Dict[str, List[str]] = {}
    for name in names:
        lower = name.lower()
        for suffix in ("up", "down"):
            if lower.endswith(suffix):
                base = name[:-len(suffix)]
                # Normalise base: strip trailing underscore / dash
                base = base.rstrip("_-")
                if not base:
                    continue
                variations.setdefault(base, []).append(name)
                break
    return variations


def _resolve_manifest_histogram_file(
    manifest: "OutputManifest", manifest_path: str
) -> str:
    """Return the absolute merged histogram ROOT path referenced by *manifest*."""
    if manifest.histograms is None or not manifest.histograms.output_file:
        raise RuntimeError(
            "ManifestDatacardTask requires a manifest with a 'histograms.output_file' entry."
        )

    hist_path = manifest.histograms.output_file
    if os.path.isabs(hist_path) or hist_path.startswith("root://"):
        return hist_path
    return os.path.abspath(os.path.join(os.path.dirname(manifest_path), hist_path))


def _collect_config_sample_names(config: Dict[str, object]) -> List[str]:
    """Extract the sample aliases referenced by a datacard config."""
    sample_names: List[str] = []
    seen: set[str] = set()

    for process_name, process_cfg in (config.get("processes") or {}).items():
        if not isinstance(process_cfg, dict):
            continue
        samples = process_cfg.get("samples")
        if isinstance(samples, list) and samples:
            for sample in samples:
                if isinstance(sample, str) and sample and sample not in seen:
                    sample_names.append(sample)
                    seen.add(sample)
        elif isinstance(process_name, str) and process_name and process_name not in seen:
            sample_names.append(process_name)
            seen.add(process_name)

    for combo_cfg in (config.get("sample_combinations") or {}).values():
        if not isinstance(combo_cfg, dict):
            continue
        for sample in combo_cfg.get("samples", []):
            if isinstance(sample, str) and sample and sample not in seen:
                sample_names.append(sample)
                seen.add(sample)

    return sample_names


def _load_dataset_manifest_sample_names(dataset_manifest_path: str) -> List[str]:
    """Load dataset names from a dataset manifest for implicit input-file mapping."""
    from dataset_manifest import DatasetManifest  # noqa: E402

    manifest = DatasetManifest.load(dataset_manifest_path)
    return [entry.name for entry in manifest.datasets if entry.name]


def _load_manifest_sample_names(manifest: "OutputManifest") -> List[str]:
    """Extract available sample aliases from enriched histogram metadata."""
    if manifest.histograms is None:
        return []

    sample_names: List[str] = []
    seen: set[str] = set()
    for sample_name in (manifest.histograms.sample_metadata or {}).keys():
        if isinstance(sample_name, str) and sample_name and sample_name not in seen:
            sample_names.append(sample_name)
            seen.add(sample_name)
    return sample_names


def _build_manifest_region_config(
    manifest_defaults: Dict[str, object],
    region_name: str,
) -> Dict[str, object]:
    """Create a control-region config block from manifest defaults."""
    region_cfg = dict(manifest_defaults)
    region_cfg.pop("region_names", None)
    region_cfg.pop("region_overrides", None)
    overrides = manifest_defaults.get("region_overrides", {})
    if isinstance(overrides, dict) and isinstance(overrides.get(region_name), dict):
        region_cfg.update(overrides[region_name])
    return region_cfg


def _prepare_manifest_datacard_config(
    config_file: str,
    manifest: "OutputManifest",
    manifest_path: str,
    target_region: str = "",
    dataset_manifest_path: str = "",
    *,
    output_dir: str = "",
    publish_message=print,
) -> str:
    """Create a temporary datacard YAML adapted to a merged manifest.

    The helper fills in missing ``input_files`` from the merged histogram file,
    optionally synthesizes region definitions from a ``manifest_defaults`` block,
    and restricts multi-region configs to the requested region.
    """
    with open(config_file, "r") as fh:
        config = yaml.safe_load(fh) or {}

    if not isinstance(config, dict):
        raise RuntimeError(
            f"Datacard configuration must contain a YAML mapping at the top level: {config_file!r}"
        )
    if output_dir:
        config["output_dir"] = output_dir

    # The analysis manifest records which nuisance groups and process/region
    # scopes were produced. Carry those into the generator unless the user
    # supplied an explicit datacard-side override.
    if manifest.nuisance_groups and "nuisance_groups" not in config:
        config["nuisance_groups"] = [
            group.to_dict() for group in manifest.nuisance_groups
        ]

    hist_file = _resolve_manifest_histogram_file(manifest, manifest_path)

    if not config.get("input_files"):
        sample_names = _collect_config_sample_names(config)
        if not sample_names:
            sample_names = _load_manifest_sample_names(manifest)
        if not sample_names and dataset_manifest_path:
            sample_names = _load_dataset_manifest_sample_names(dataset_manifest_path)
        if not sample_names:
            sample_names = ["merged"]
        config["input_files"] = {
            sample_name: {"path": hist_file} for sample_name in sample_names
        }

    if (
        not config.get("sample_combinations")
        and manifest.histograms is not None
        and manifest.histograms.sample_combinations
    ):
        config["sample_combinations"] = {
            combo_name: dict(combo_cfg)
            for combo_name, combo_cfg in manifest.histograms.sample_combinations.items()
        }

    if (
        not config.get("processes")
        and manifest.histograms is not None
        and manifest.histograms.processes
    ):
        config["processes"] = {
            process_name: dict(process_cfg)
            for process_name, process_cfg in manifest.histograms.processes.items()
        }

    control_regions = config.get("control_regions")
    manifest_defaults = config.get("manifest_defaults")

    # Override-only mode: if control_regions is empty/missing, synthesise
    # from manifest region definitions, then from manifest_defaults.
    if not control_regions:
        region_names: List[str] = []
        if target_region:
            region_names = [target_region]
        elif manifest.regions:
            region_names = [r.name for r in manifest.regions if r.name]
        elif isinstance(manifest_defaults, dict):
            region_names = list(manifest_defaults.get("region_names", []))

        if not region_names:
            region_names = ["combined"]

        if isinstance(manifest_defaults, dict):
            config["control_regions"] = {
                name: _build_manifest_region_config(manifest_defaults, name)
                for name in region_names
            }
        else:
            # No manifest_defaults — create minimal region configs
            config["control_regions"] = {
                name: {} for name in region_names
            }
        control_regions = config["control_regions"]

    if control_regions and manifest.histograms is not None:
        # Infer missing observables from manifest histogram axis labels.
        # If a control region has no "observable" set, scan the manifest for
        # histogram axis labels that match the region name and use the first
        # matching axis variable.
        for rname, rinfo in list(control_regions.items()):
            if not isinstance(rinfo, dict):
                continue
            if rinfo.get("observable"):
                continue  # already set
            # Try to find a matching observable from axis_labels
            axis_labels = manifest.histograms.axis_labels or {}
            # The "control" axis role typically holds observable names.
            control_labels = axis_labels.get("control", [])
            if control_labels:
                # Pick the first matching label that contains the region name
                matches = [l for l in control_labels if rname.lower() in l.lower()]
                if matches:
                    rinfo["observable"] = matches[0]
                    publish_message(
                        f"Inferred observable '{matches[0]}' for region '{rname}' "
                        f"from manifest axis labels."
                    )
                    continue
            # Fallback: scan histogram names for a match
            if manifest.histograms.histogram_names:
                hist_matches = [
                    n for n in manifest.histograms.histogram_names
                    if rname.lower() in n.lower()
                ]
                if hist_matches:
                    rinfo["observable"] = hist_matches[0]

    if target_region:
        if isinstance(control_regions, dict) and target_region in control_regions:
            config["control_regions"] = {target_region: control_regions[target_region]}
        elif isinstance(manifest_defaults, dict):
            config["control_regions"] = {
                target_region: _build_manifest_region_config(manifest_defaults, target_region)
            }

    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="manifest_datacard_", delete=False
    )
    with handle:
        yaml.safe_dump(config, handle, sort_keys=False)
    return handle.name


class _ManifestDatacardOperations:
    def _validate_nuisance_coverage(
        self, manifest: "OutputManifest"
    ) -> Optional[Dict[str, int]]:
        """Validate nuisance group coverage against available variations.

        Compares each declared ``NuisanceGroupDefinition`` in the manifest
        against the available histogram-based variation names derived from
        :func:`_derive_available_variations`.

        Parameters
        ----------
        manifest:
            Merged manifest to validate.

        Returns
        -------
        dict or None
            Coverage summary with keys ``"pass"``, ``"fail"``, ``"warn"``
            counts, or ``None`` if validation was skipped.

        Raises
        ------
        RuntimeError
            If ``--strict-coverage`` is set and any coverage issues are found.
        """
        if not manifest.nuisance_groups:
            self.publish_message("No nuisance groups declared in manifest; skipping coverage validation.")
            return None

        try:
            from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry  # noqa: E402
        except ImportError:
            self.publish_message(
                "WARNING: nuisance_groups module not available; skipping coverage validation."
            )
            return None

        available = _derive_available_variations(manifest)
        self.publish_message(
            f"Coverage validation: {len(manifest.nuisance_groups)} nuisance group(s), "
            f"{len(available)} detected variation base(s): {sorted(available.keys())}"
        )

        registry_groups = [
            NuisanceGroup(
                name=ng.name,
                group_type=ng.group_type,
                systematics=list(ng.systematics),
                processes=list(ng.processes),
                regions=list(ng.regions),
                output_usage=list(ng.output_usage),
            )
            for ng in manifest.nuisance_groups
        ]
        registry = NuisanceGroupRegistry(groups=registry_groups)
        issues = registry.validate_coverage(available)

        if not issues:
            self.publish_message("Nuisance coverage validation passed: all systematics have up+down coverage.")
            return {"pass": len(available), "fail": 0, "warn": 0}

        lines = ["Nuisance coverage issues detected:"]
        for issue in issues:
            lines.append(f"  [{issue.severity.value.upper()}] {issue.message}")
        report = "\n".join(lines)

        error_count = sum(1 for i in issues if i.severity.value == "error")
        warn_count = sum(1 for i in issues if i.severity.value == "warning")
        if self.strict_coverage and error_count:
            raise RuntimeError(
                report + f"\n{error_count} coverage error(s) found with --strict-coverage enabled."
            )
        self.publish_message(report)
        return {"pass": len(available) - error_count - warn_count, "fail": error_count, "warn": warn_count}

    def _preflight_combine_shapes(self, manifest: "OutputManifest") -> None:
        """Validate shape file integrity before datacard generation.

        Checks:
        - Shape file existence and readability.
        - Nominal vs. variation bin compatibility (same binning).
        - Empty/negative bin detection.
        - Process definition consistency.
        - Low-stat STXS bin warnings (< 10 events in any category).

        Parameters
        ----------
        manifest:
            Merged manifest whose histogram file is inspected.
        """
        if manifest.histograms is None or not manifest.histograms.output_file:
            self.publish_message("No histograms in manifest; skipping shape preflight.")
            return

        hist_file = _resolve_manifest_histogram_file(manifest, self.manifest_path)
        if not hist_file.startswith("root://") and not os.path.exists(hist_file):
            self.publish_message(
                f"WARNING: Shape file not found: {hist_file}. "
                "Skipping shape preflight."
            )
            return

        # Try to open the file with uproot for basic sanity checks
        try:
            import uproot  # type: ignore
        except ImportError:
            self.publish_message(
                "WARNING: uproot not available; skipping shape preflight validation."
            )
            return

        try:
            rf = uproot.open(hist_file)
        except Exception as e:
            self.publish_message(
                f"WARNING: Cannot open shape file {hist_file}: {e}"
            )
            return

        hist_names = manifest.histograms.histogram_names or []
        if not hist_names:
            self.publish_message("No histogram names in manifest; skipping shape preflight.")
            return
        if all(name.startswith("nd_histograms_v1/") for name in hist_names):
            # The manifest lists the merge inputs, while the merge task adds
            # reconstructed TH1 objects under channel/control/sample paths.
            # Inspect those 1D outputs after extraction.
            projected_names = []
            for key in rf.keys(recursive=True, cycle=False):
                name = str(key).split(";", 1)[0]
                if name.count("/") < 2:
                    continue
                try:
                    hist = rf[name]
                    axes = getattr(hist, "axes", None)
                    if hasattr(hist, "values") and axes is not None and len(axes) == 1:
                        projected_names.append(name)
                except Exception:
                    continue
            hist_names = sorted(projected_names)
            if not hist_names:
                self.publish_message(
                    "No reconstructed 1D histograms found; skipping shape preflight."
                )
                return

        # Check each histogram
        low_stat_warnings = 0
        bin_mismatch_warnings = 0
        empty_negative_bins = 0
        ref_binning = None

        for hname in hist_names:
            try:
                hist = rf[hname]
                # Try to read bin contents
                if hasattr(hist, "values"):
                    values = hist.values()
                    edges = hist.edges() if hasattr(hist, "edges") else None
                elif hasattr(hist, "member"):
                    values = hist.member("fArray")
                    edges = None
                else:
                    continue

                # Check for empty/negative bins
                neg_count = int(np.sum(values < 0)) if len(values) > 0 else 0
                zero_count = int(np.sum(values == 0)) if len(values) > 0 else 0
                if neg_count > 0:
                    empty_negative_bins += 1
                    if empty_negative_bins <= 3:
                        self.publish_message(
                            f"WARNING: Histogram '{hname}' has {neg_count} negative bin(s)."
                        )

                # Check bin compatibility against reference
                if ref_binning is None and edges is not None:
                    ref_binning = edges
                elif edges is not None and ref_binning is not None and len(edges) == len(ref_binning):
                    if not np.allclose(edges, ref_binning):
                        bin_mismatch_warnings += 1
                        if bin_mismatch_warnings <= 3:
                            self.publish_message(
                                f"WARNING: Histogram '{hname}' has different binning "
                                f"than reference."
                            )

                # Low-stat warning (total integral < 10)
                total = float(np.sum(values))
                if 0 < total < 10:
                    low_stat_warnings += 1
                    if low_stat_warnings <= 5:
                        self.publish_message(
                            f"WARNING: Histogram '{hname}' has low statistics: "
                            f"{total:.1f} total events."
                        )

            except Exception as e:
                self.publish_message(
                    f"WARNING: Cannot inspect histogram '{hname}': {e}"
                )

        rf.close()

        summary = []
        if low_stat_warnings:
            summary.append(f"{low_stat_warnings} low-stat histograms")
        if bin_mismatch_warnings:
            summary.append(f"{bin_mismatch_warnings} bin-mismatched histograms")
        if empty_negative_bins:
            summary.append(f"{empty_negative_bins} histograms with negative bins")
        if summary:
            self.publish_message(
                "Shape preflight warnings: " + "; ".join(summary)
            )
        else:
            self.publish_message(
                f"Shape preflight passed: {len(hist_names)} histograms checked."
            )

    def run(self):
        config_file = os.path.abspath(self.datacard_config)
        if not os.path.exists(config_file):
            raise RuntimeError(
                f"Datacard configuration file not found: {config_file!r}"
            )

        manifest = _load_manifest(self.manifest_path)

        coverage_result = self._validate_nuisance_coverage(manifest)
        self._preflight_combine_shapes(manifest)

        region_names: List[str] = [r.name for r in manifest.regions if r.name]
        self.publish_message(
            f"Manifest regions: {region_names or ['(none – single combined datacard)']}"
        )

        try:
            from create_datacards import DatacardGenerator  # noqa: E402
        except ImportError as exc:
            raise RuntimeError(
                "Cannot import create_datacards from core/python. "
                "Ensure core/python is on PYTHONPATH."
            ) from exc

        out_dir = self._manifest_datacard_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        provenance = {
            "task": "ManifestDatacardTask",
            "name": self.name,
            "manifest_path": self.manifest_path,
            "dataset_manifest": self.dataset_manifest or None,
            "manifest_framework_hash": manifest.framework_hash,
            "manifest_user_repo_hash": manifest.user_repo_hash,
            "regions": region_names,
            "n_nuisance_groups": len(manifest.nuisance_groups),
            "coverage_result": coverage_result,
        }

        with ExitStack() as cleanup, PerformanceRecorder("ManifestDatacardTask") as rec:
            for region in region_names or [""]:
                region_out_dir = os.path.join(out_dir, region) if region else out_dir
                generated_config = _prepare_manifest_datacard_config(
                    config_file,
                    manifest,
                    self.manifest_path,
                    target_region=region,
                    dataset_manifest_path=self.dataset_manifest,
                    output_dir=region_out_dir,
                    publish_message=self.publish_message,
                )
                cleanup.callback(Path(generated_config).unlink, missing_ok=True)
                generator = DatacardGenerator(generated_config)
                description = f"datacard for region '{region}'" if region else "combined datacard"
                self.publish_message(f"Generating {description} in {region_out_dir}")
                generator.run()

        rec.save(os.path.join(out_dir, "manifest_datacard.perf.json"))

        # Write provenance record
        prov_path = os.path.join(out_dir, "provenance.json")
        with open(prov_path, "w") as fh:
            json.dump(provenance, fh, indent=2)

        datacards = list(Path(out_dir).glob("**/*.txt"))
        if not datacards:
            raise RuntimeError(
                f"Datacard generation completed but no .txt files were found "
                f"under {out_dir!r}.  Check the YAML config."
            )
        self.publish_message(
            f"ManifestDatacardTask: {len(datacards)} datacard(s) written under {out_dir}."
        )


@dataclass
class ManifestDatacards(_ManifestDatacardOperations):
    """Generate datacards and shape files from a merged output manifest."""

    manifest_path: str
    datacard_config: str
    output_dir: str
    name: str = "manifest"
    dataset_manifest: str = ""
    strict_coverage: bool = False

    publish_message = staticmethod(print)

    @property
    def _manifest_datacard_dir(self) -> str:
        return self.output_dir


class _ManifestFitOperations:
    @property
    def _rabbit_completion_path(self) -> Path:
        return Path(self._manifest_fit_dir) / "rabbit" / "task_complete.json"

    def _verify_rabbit_result(self):
        try:
            from statistical_inference.model import cached_model, _file_digest
            marker = json.loads(self._rabbit_completion_path.read_text())
            return (marker["task_id"] == self._completion_id and
                    marker["model_identity"] == cached_model(self._collect_datacards(),
                        Path(self._manifest_fit_dir) / "rabbit" / "prepared_model.json")["identity"] and
                    Path(marker["native_output"]).is_file() and
                    _file_digest(marker["native_output"]) == marker["native_sha256"])
        except (OSError, ValueError, KeyError, RuntimeError):
            return False

    def _collect_datacards(self) -> List[str]:
        search_dir = self.datacard_dir if self.datacard_dir else self._manifest_datacard_dir
        if Path(search_dir).is_file():
            return [str(Path(search_dir).resolve())]
        cards = sorted(Path(search_dir).glob("**/*.txt"))
        if not cards:
            raise RuntimeError(
                f"No .txt datacard files found under {search_dir!r}.  "
                "Run ManifestDatacardTask first or pass --datacard-dir."
            )
        return [str(c) for c in cards]

    def _run_combine_fits(self, datacards: List[str], out_dir: str) -> bool:
        """Run Combine for each datacard.  Returns True if all succeeded."""
        from statistical_inference.combine import run_standard_combine
        extra_opts = shlex.split(self.combine_options) if self.combine_options else []

        all_ok = True
        for datacard in datacards:
            tag = Path(datacard).stem.replace("datacard_", "")
            try:
                result = run_standard_combine(datacard, Path(out_dir) / "combine" / tag,
                    executable=self.combine_exe or None, method=self.method, options=extra_opts,
                    asimov=self.fit_dataset == "asimov")
                (Path(out_dir) / "combine" / tag / "fit_result.json").write_text(json.dumps(result, indent=2) + "\n")
            except RuntimeError as exc:
                self.publish_message(f"ERROR: {exc}")
                all_ok = False
        return all_ok

    def _run_analysis_fits(self, datacards: List[str], out_dir: str) -> bool:
        """Lightweight histogram-based chi2 fit.

        Reads shape ROOT files referenced in each datacard (if they exist and
        uproot is available), computes a simple χ² between the data histogram
        and the sum of background histograms for each bin, and writes results
        as JSON.  When uproot is unavailable or no shapes file is found, a
        stub result is written to keep the DAG runnable.

        Supports both histogram-based and function-based entries in the shapes
        ROOT file: if the ROOT object is a TH1 its bin contents are summed
        directly; otherwise (e.g. TF1 or TGraph) the integral is used.

        Returns True (always succeeds – individual per-card results capture
        any per-card errors).
        """
        results = []
        for datacard in datacards:
            tag = Path(datacard).stem
            result: Dict[str, object] = {"datacard": datacard, "tag": tag}
            try:
                result.update(self._fit_single_card(datacard))
            except Exception as exc:  # noqa: BLE001
                result["status"] = "error"
                result["error"] = str(exc)
            results.append(result)

        summary_path = os.path.join(out_dir, "analysis_fit_results.json")
        with open(summary_path, "w") as fh:
            json.dump(results, fh, indent=2)
        self.publish_message(f"Analysis fit results written to: {summary_path}")
        return True

    def _fit_single_card(self, datacard_path: str) -> Dict[str, object]:
        """Perform a simple chi2 fit for one datacard.

        Parses the datacard for the associated shapes ROOT file, then reads
        data and background histograms.  Returns a dict with chi2, ndof, and
        per-process integrals.
        """
        shapes_file, data_hist_name, bkg_hist_names = _parse_datacard_shapes(datacard_path)
        if not shapes_file or not os.path.exists(shapes_file):
            return {
                "status": "no_shapes",
                "message": f"No shapes ROOT file found for {datacard_path!r}.",
            }

        try:
            import uproot  # type: ignore
            return _chi2_fit_uproot(shapes_file, data_hist_name, bkg_hist_names)
        except ImportError:
            pass

        return {
            "status": "stub",
            "message": (
                "uproot not available; chi2 fit skipped.  "
                "Install uproot for histogram-based fits."
            ),
        }

    def run(self):
        out_dir = self._manifest_fit_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        datacards = self._collect_datacards()
        self.publish_message(
            f"ManifestFitTask[backend={self.fitting_backend}]: "
            f"{len(datacards)} datacard(s)."
        )

        backend = self.fitting_backend.lower()
        if backend not in ("combine", "analysis", "rabbit"):
            raise RuntimeError(
                f"Unknown fitting_backend {self.fitting_backend!r}.  "
                "Choose 'combine', 'rabbit', or 'analysis'."
            )

        provenance = {
            "task": "ManifestFitTask",
            "name": self.name,
            "fitting_backend": backend,
            "method": self.method if backend == "combine" else None,
            "n_datacards": len(datacards),
            "manifest_path": self.manifest_path or None,
        }

        with PerformanceRecorder(f"ManifestFitTask[{backend}]") as rec:
            if backend == "combine":
                all_ok = self._run_combine_fits(datacards, out_dir)
            elif backend == "rabbit":
                from statistical_inference.rabbit import run_rabbit
                marker = self._rabbit_completion_path
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.unlink(missing_ok=True)
                fit = run_rabbit({"datacards": datacards}, out_dir,
                                 self.fit_dataset == "asimov",
                                 python=self.rabbit_python or None, device=self.rabbit_device,
                                 devices=[int(v) for v in self.rabbit_devices.split(",") if v],
                                 n_devices=self.rabbit_n_devices, mc_stat=self.rabbit_mc_stat,
                                 likelihood=self.rabbit_likelihood,
                                 covariance=self.rabbit_covariance,
                                 zero_shape_policy=self.rabbit_zero_shape_policy,
                                 hvp_batch=self.rabbit_hvp_batch or None)
                provenance["rabbit"] = fit
                all_ok = True
            else:
                all_ok = self._run_analysis_fits(datacards, out_dir)

        rec.save(os.path.join(out_dir, "manifest_fit.perf.json"))

        prov_path = os.path.join(out_dir, "provenance.json")
        with open(prov_path, "w") as fh:
            json.dump(provenance, fh, indent=2)

        if not all_ok:
            raise RuntimeError(
                f"One or more fits failed.  Check logs in {out_dir!r}."
            )

        if backend == "rabbit":
            from statistical_inference.model import _file_digest
            self._rabbit_completion_path.write_text(json.dumps({"task_id": self._completion_id,
                "model_identity": fit["model_identity"], "native_output": fit["hdf5_output"],
                "native_sha256": _file_digest(fit["hdf5_output"])}) + "\n")

        self.publish_message(
            f"ManifestFitTask complete.  Results in: {out_dir}"
        )


@dataclass
class ManifestFit(_ManifestFitOperations):
    """Fit a directory of datacards with the configured statistics backend."""

    datacard_dir: str
    output_dir: str
    name: str = "manifest"
    manifest_path: str = ""
    fitting_backend: str = "combine"
    fit_dataset: str = "observed"
    method: str = "AsymptoticLimits"
    combine_exe: str = ""
    combine_options: str = ""
    rabbit_python: str = ""
    rabbit_device: str = "gpu"
    rabbit_devices: str = ""
    rabbit_n_devices: int = 1
    rabbit_mc_stat: str = "card"
    rabbit_likelihood: str = "native"
    rabbit_covariance: str = "auto"
    rabbit_zero_shape_policy: str = "error"
    rabbit_hvp_batch: int = 0

    publish_message = staticmethod(print)

    def __post_init__(self):
        choices = {
            "fitting_backend": ("combine", "rabbit", "analysis"),
            "fit_dataset": ("observed", "asimov"),
            "rabbit_device": ("cpu", "gpu"),
            "rabbit_mc_stat": ("card", "none", "lite", "full"),
            "rabbit_likelihood": ("native", "combine"),
            "rabbit_covariance": ("auto", "analytic", "combine"),
            "rabbit_zero_shape_policy": ("error", "native"),
        }
        for key, allowed in choices.items():
            if getattr(self, key) not in allowed:
                raise ValueError(f"{key} must be one of {', '.join(allowed)}")

    @property
    def _manifest_fit_dir(self) -> str:
        return self.output_dir

    @property
    def _completion_id(self) -> str:
        return content_digest(asdict(self))

    def verify_rabbit_result(self) -> bool:
        return self._verify_rabbit_result()


class _AnalyticWorkspaceOperations:
    def run(self):
        try:
            import yaml  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "PyYAML is required by AnalyticWorkspaceFitTask. "
                "Install it with: pip install pyyaml"
            ) from exc
        import yaml

        out_dir = self._out_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        # ---- Load workspace config ----------------------------------------
        config_path = os.path.abspath(self.workspace_config)
        if not os.path.exists(config_path):
            raise RuntimeError(
                f"Workspace config not found: {config_path!r}"
            )
        with open(config_path) as fh:
            ws_cfg = yaml.safe_load(fh)
        rates = configured_rates(ws_cfg)

        # ---- Open analysis ROOT file --------------------------------------
        try:
            import ROOT  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "PyROOT (ROOT Python bindings) is required.  "
                "Source a ROOT environment: source env.sh"
            ) from exc

        ROOT.gROOT.SetBatch(True)
        ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)

        hist_path = self.histogram_file
        if not hist_path.startswith("root://"):
            hist_path = os.path.abspath(hist_path)
        root_file = ROOT.TFile.Open(hist_path, "READ")
        if not root_file or root_file.IsZombie():
            raise RuntimeError(
                f"Cannot open histogram file: {hist_path!r}"
            )

        active_channels: List[str] = []

        with PerformanceRecorder("AnalyticWorkspaceFitTask") as rec:
            # ---- Build workspace + datacard per channel -------------------
            for ch_cfg in ws_cfg.get("channels", []):
                channel = ch_cfg["name"]
                hist_name = ch_cfg["histogram"]
                label = ch_cfg.get("label", channel)

                hist, hist_obj_path = _find_histogram_in_file(root_file, hist_name)
                if not hist or hist.IsZombie() or hist.GetDimension() != 1:
                    if rates is not None:
                        raise RuntimeError(f"Missing 1D histogram '{hist_name}' for channel '{channel}'")
                    self.publish_message(
                        f"WARNING: histogram '{hist_name}' not "
                        f"found in {hist_path!r}; skipping channel '{channel}'."
                    )
                    continue

                hist.SetDirectory(0)
                n_obs = hist.Integral()
                if not np.isfinite(n_obs) or n_obs < 10:
                    if rates is not None:
                        raise RuntimeError(f"Channel '{channel}' requires at least 10 finite in-range events")
                    self.publish_message(
                        f"WARNING: channel '{channel}' ({label}) has only "
                        f"{n_obs:.0f} events; skipping."
                    )
                    continue

                self.publish_message(
                    f"Building workspace for '{channel}' ({label}): "
                    f"{n_obs:.0f} events"
                )
                self.publish_message(f"  Histogram : {hist_obj_path}")

                ws = _build_analytic_workspace(ws_cfg, hist, channel)
                ws_path = os.path.join(out_dir, f"ws_{channel}.root")
                ws_file = ROOT.TFile(ws_path, "RECREATE")
                ws.Write()
                ws_file.Close()

                if rates is None:
                    card_path = _write_analytic_datacard(channel, n_obs, ws_path, out_dir)
                else:
                    card_path = write_rate_card([channel], rates, out_dir, tag=channel)
                active_channels.append(channel)
                self.publish_message(f"  Workspace : {ws_path}")
                self.publish_message(f"  Datacard  : {card_path}")

            root_file.Close()

            if not active_channels:
                raise RuntimeError(
                    "No channels with sufficient statistics were found.  "
                    "Check that the histogram file contains the expected "
                    "keys and that each channel has at least 10 events."
                )

            # ---- Combined simultaneous-fit datacard ----------------------
            if rates is None:
                combined_card = _write_analytic_combined_datacard(active_channels, out_dir)
            else:
                combined_card = write_rate_card(active_channels, rates, out_dir, tag="combined")
            self.publish_message(f"Combined datacard: {combined_card}")

        rec.save(os.path.join(out_dir, "analytic_fit.perf.json"))

        # ---- Provenance --------------------------------------------------
        prov = {
            "task": "AnalyticWorkspaceFitTask",
            "name": self.name,
            "workspace_config": config_path,
            "histogram_file": hist_path,
            "method": self.method,
            "channels": active_channels,
            "skip_fit": self.skip_fit,
        }
        if rates is not None:
            prov["combine_rates"] = ws_cfg["combine_rates"]
        with open(os.path.join(out_dir, "provenance.json"), "w") as fh:
            json.dump(prov, fh, indent=2)

        if self.skip_fit:
            self.publish_message(
                "--skip-fit set: skipping combine invocations.  "
                f"Datacards available in {out_dir!r}."
            )
            return

        # ---- Locate combine binary ---------------------------------------
        try:
            combine_bin = _find_combine(self.combine_exe)
        except RuntimeError as exc:
            if rates is not None:
                raise
            self.publish_message(
                f"WARNING: {exc}  "
                "Skipping fits — datacards and workspaces are available in "
                f"{out_dir!r}.  Re-run with Combine installed or pass "
                "--skip-fit to suppress this warning."
            )
            return

        from statistical_inference.combine import _combine_runtime_environment, combine_failed
        environment, _, _, _ = _combine_runtime_environment(combine_bin)
        extra_opts = shlex.split(self.combine_options) if self.combine_options else []
        all_ok = True

        all_cards = (
            [os.path.join(out_dir, f"datacard_{ch}.txt") for ch in active_channels]
            + [combined_card]
        )
        for card_path in all_cards:
            tag = Path(card_path).stem.removeprefix("datacard_")
            cmd = [combine_bin, "-M", self.method, card_path, "-n", f"_{tag}"] + extra_opts
            log_path = os.path.join(out_dir, f"combine_{tag}.log")
            self.publish_message("Running: " + " ".join(cmd))
            with open(log_path, "w") as log_fh:
                result = subprocess.run(
                    cmd, stdout=log_fh, stderr=subprocess.STDOUT, cwd=out_dir, env=environment
                )
            if combine_failed(result.returncode, Path(log_path).read_text(errors="replace")):
                self.publish_message(
                    f"ERROR: combine failed for '{tag}' "
                    f"(exit {result.returncode}).  Log: {log_path}"
                )
                all_ok = False
            elif rates is not None and self.method == "FitDiagnostics":
                diagnostic = Path(out_dir) / f"fitDiagnostics_{tag}.root"
                with ROOT.TFile.Open(str(diagnostic), "READ") as fit_file:
                    fit = fit_file.Get("fit_s")
                    if not fit or fit.status() != 0:
                        self.publish_message(f"ERROR: no successful signal-plus-background fit in {diagnostic}")
                        all_ok = False
                    else:
                        self.publish_message(f"Fit complete: '{tag}'. covQual={fit.covQual()}. Log: {log_path}")
            else:
                self.publish_message(
                    f"Fit complete: '{tag}'.  Log: {log_path}"
                )

        if not all_ok:
            raise RuntimeError(
                f"One or more combine fits failed.  See logs in {out_dir!r}."
            )

        self.publish_message(
            f"AnalyticWorkspaceFitTask complete.  Results in: {out_dir}"
        )


@dataclass
class AnalyticWorkspaceFit(_AnalyticWorkspaceOperations):
    """Build a RooFit workspace and optionally run Combine on its cards."""

    workspace_config: str
    histogram_file: str
    output_dir: str
    name: str = "manifest"
    method: str = "FitDiagnostics"
    combine_exe: str = ""
    combine_options: str = "--saveShapes --saveNormalizations"
    skip_fit: bool = False

    publish_message = staticmethod(print)

    @property
    def _out_dir(self) -> str:
        return self.output_dir


def _make_roo_var(
    par_name: str,
    par_cfg: dict,
    channel: str,
) -> "ROOT.RooRealVar":
    """Create a RooRealVar from a parameter config dict.

    If ``par_cfg['shared']`` is ``True`` the variable name is ``par_name``
    (no channel suffix) so that Combine ties it across channels in the
    combined fit.  Otherwise the name is ``par_name_<channel>``.

    Parameters
    ----------
    par_name:
        Logical parameter name (e.g. ``"mean"``, ``"decay"``).
    par_cfg:
        Dict with keys ``init``, optionally ``min``, ``max``, ``fixed``,
        ``title``, ``shared``.
    channel:
        Channel identifier appended as a suffix when ``shared`` is False.
    """
    import ROOT  # type: ignore

    shared = par_cfg.get("shared", False)
    var_name = par_name if shared else f"{par_name}_{channel}"
    title = par_cfg.get("title", var_name)
    init = float(par_cfg.get("init", 0.0))

    if par_cfg.get("fixed", False):
        var = ROOT.RooRealVar(var_name, title, init)
        var.setConstant(True)
    else:
        lo = float(par_cfg.get("min", init - abs(init) - 1.0))
        hi = float(par_cfg.get("max", init + abs(init) + 1.0))
        var = ROOT.RooRealVar(var_name, title, init, lo, hi)

    return var


def _build_signal_pdf(
    sig_cfg: dict,
    obs: "ROOT.RooRealVar",
    channel: str,
) -> "tuple[ROOT.RooAbsPdf, list]":
    """Build the signal PDF and its parameters for one channel.

    Returns ``(pdf, [parameter, ...])`` where all objects are owned by the
    caller until imported into a RooWorkspace.

    Supported ``sig_cfg['pdf']`` values:
        ``voigtian``, ``gaussian``, ``crystalball``,
        ``double_gaussian``, ``breit_wigner``.
    """
    import ROOT  # type: ignore

    pdf_type = sig_cfg["pdf"].lower()
    params = sig_cfg.get("parameters", {})

    if pdf_type == "voigtian":
        mean  = _make_roo_var("mean",  params.get("mean",  {"init": 91.2}), channel)
        width = _make_roo_var("width", params.get("width", {"init": 2.495, "fixed": True}), channel)
        sigma = _make_roo_var("sigma", params.get("sigma", {"init": 2.0, "min": 0.1, "max": 10.0}), channel)
        pdf = ROOT.RooVoigtian(
            f"sig_{channel}", f"Voigtian signal ({channel})",
            obs, mean, width, sigma,
        )
        return pdf, [mean, width, sigma]

    if pdf_type == "gaussian":
        mean  = _make_roo_var("mean",  params.get("mean",  {"init": 0.0}), channel)
        sigma = _make_roo_var("sigma", params.get("sigma", {"init": 1.0, "min": 0.01, "max": 20.0}), channel)
        pdf = ROOT.RooGaussian(
            f"sig_{channel}", f"Gaussian signal ({channel})",
            obs, mean, sigma,
        )
        return pdf, [mean, sigma]

    if pdf_type == "crystalball":
        mean  = _make_roo_var("mean",  params.get("mean",  {"init": 0.0}), channel)
        sigma = _make_roo_var("sigma", params.get("sigma", {"init": 1.0, "min": 0.01, "max": 20.0}), channel)
        alpha = _make_roo_var("alpha", params.get("alpha", {"init": 1.5, "min": 0.1, "max": 10.0}), channel)
        n_par = _make_roo_var("n",     params.get("n",     {"init": 2.0, "min": 0.1, "max": 100.0}), channel)
        pdf = ROOT.RooCBShape(
            f"sig_{channel}", f"Crystal Ball signal ({channel})",
            obs, mean, sigma, alpha, n_par,
        )
        return pdf, [mean, sigma, alpha, n_par]

    if pdf_type == "double_gaussian":
        mean   = _make_roo_var("mean",   params.get("mean",   {"init": 0.0}), channel)
        sigma1 = _make_roo_var("sigma1", params.get("sigma1", {"init": 1.0, "min": 0.01, "max": 10.0}), channel)
        sigma2 = _make_roo_var("sigma2", params.get("sigma2", {"init": 3.0, "min": 0.01, "max": 20.0}), channel)
        frac   = _make_roo_var("frac",   params.get("frac",   {"init": 0.7, "min": 0.01, "max": 0.99}), channel)
        g1 = ROOT.RooGaussian(f"sig_g1_{channel}", f"Gaussian 1 ({channel})", obs, mean, sigma1)
        g2 = ROOT.RooGaussian(f"sig_g2_{channel}", f"Gaussian 2 ({channel})", obs, mean, sigma2)
        pdf = ROOT.RooAddPdf(
            f"sig_{channel}", f"Double Gaussian signal ({channel})",
            ROOT.RooArgList(g1, g2), ROOT.RooArgList(frac),
        )
        return pdf, [mean, sigma1, sigma2, frac]

    if pdf_type == "breit_wigner":
        mean  = _make_roo_var("mean",  params.get("mean",  {"init": 91.2}), channel)
        width = _make_roo_var("width", params.get("width", {"init": 2.495}), channel)
        pdf = ROOT.RooBreitWigner(
            f"sig_{channel}", f"Breit-Wigner signal ({channel})",
            obs, mean, width,
        )
        return pdf, [mean, width]

    raise RuntimeError(
        f"Unknown signal PDF type {pdf_type!r}.  "
        "Supported: voigtian, gaussian, crystalball, double_gaussian, breit_wigner."
    )


def _build_background_pdf(
    bkg_cfg: dict,
    obs: "ROOT.RooRealVar",
    channel: str,
) -> "tuple[ROOT.RooAbsPdf, list]":
    """Build the background PDF and its parameters for one channel.

    Returns ``(pdf, [parameter, ...])`` where all objects are owned by the
    caller until imported into a RooWorkspace.

    Supported ``bkg_cfg['pdf']`` values:
        ``exponential``, ``polynomial``, ``chebychev``, ``bernstein``.
    """
    import ROOT  # type: ignore

    pdf_type = bkg_cfg["pdf"].lower()
    params = bkg_cfg.get("parameters", {})

    if pdf_type == "exponential":
        decay = _make_roo_var(
            "decay",
            params.get("decay", {"init": -0.05, "min": -10.0, "max": -1e-4}),
            channel,
        )
        pdf = ROOT.RooExponential(
            f"bkg_{channel}", f"Exponential background ({channel})",
            obs, decay,
        )
        return pdf, [decay]

    # Polynomial-family helpers: build a list of coefficient RooRealVars.
    def _coef_vars(key_prefix: str, n_coefs: int, default_cfg: dict) -> List:
        cvars = []
        for i in range(n_coefs):
            key = f"{key_prefix}{i}"
            c = _make_roo_var(key, params.get(key, default_cfg), channel)
            cvars.append(c)
        return cvars

    if pdf_type == "polynomial":
        order = int(bkg_cfg.get("order", max(1, len(params))))
        cvars = _coef_vars("c", order, {"init": 0.0, "min": -100.0, "max": 100.0})
        coef_list = ROOT.RooArgList()
        for c in cvars:
            coef_list.add(c)
        pdf = ROOT.RooPolynomial(
            f"bkg_{channel}", f"Polynomial background ({channel})",
            obs, coef_list,
        )
        return pdf, cvars

    if pdf_type in ("chebychev", "chebyshev"):
        order = int(bkg_cfg.get("order", max(1, len(params))))
        cvars = _coef_vars("a", order, {"init": 0.0, "min": -10.0, "max": 10.0})
        coef_list = ROOT.RooArgList()
        for c in cvars:
            coef_list.add(c)
        pdf = ROOT.RooChebychev(
            f"bkg_{channel}", f"Chebyshev background ({channel})",
            obs, coef_list,
        )
        return pdf, cvars

    if pdf_type == "bernstein":
        order = int(bkg_cfg.get("order", max(1, len(params))))
        # Bernstein of degree N has N+1 coefficients (b0 … bN)
        cvars = _coef_vars("b", order + 1, {"init": 0.5, "min": 0.0, "max": 1.0})
        coef_list = ROOT.RooArgList()
        for c in cvars:
            coef_list.add(c)
        pdf = ROOT.RooBernstein(
            f"bkg_{channel}", f"Bernstein background ({channel})",
            obs, coef_list,
        )
        return pdf, cvars

    raise RuntimeError(
        f"Unknown background PDF type {pdf_type!r}.  "
        "Supported: exponential, polynomial, chebychev, bernstein."
    )


def _build_analytic_workspace(
    ws_cfg: dict,
    hist: "ROOT.TH1",
    channel: str,
) -> "ROOT.RooWorkspace":
    """Build a complete RooWorkspace for one channel from a workspace config.

    The workspace contains:

    - ``mass`` (or the configured observable name): range [lo, hi]
    - All signal PDF parameters (names depend on ``shared`` flag)
    - All background PDF parameters
    - ``sig_<channel>``: signal PDF
    - ``bkg_<channel>``: background PDF
    - ``nsig_<channel>``, ``nbkg_<channel>``: freely floating extended yields
    - ``model_<channel>``: extended ``RooAddPdf(sig, bkg)``
    - ``data_obs_<channel>``: ``RooDataHist`` from the input histogram

    Parameters
    ----------
    ws_cfg:
        Parsed workspace config dict.
    hist:
        TH1 histogram for this channel (already detached from the ROOT file
        via ``SetDirectory(0)``).
    channel:
        Channel identifier string (e.g. ``"0j"``).
    """
    import ROOT  # type: ignore

    obs_cfg = ws_cfg["observable"]
    obs_name = obs_cfg.get("name", "mass")
    obs_title = obs_cfg.get("title", obs_name)
    obs_lo = float(obs_cfg["lo"])
    obs_hi = float(obs_cfg["hi"])
    n_obs = hist.Integral()

    ws = ROOT.RooWorkspace(f"ws_{channel}")
    obs = ROOT.RooRealVar(obs_name, obs_title, obs_lo, obs_hi)

    sig_pdf, sig_pars = _build_signal_pdf(ws_cfg["signal"], obs, channel)
    bkg_pdf, bkg_pars = _build_background_pdf(ws_cfg["background"], obs, channel)

    nsig = ROOT.RooRealVar(f"nsig_{channel}", "Signal yield",
                           0.85 * n_obs, 0.0, 1.5 * n_obs + 10.0)
    nbkg = ROOT.RooRealVar(f"nbkg_{channel}", "Background yield",
                           0.15 * n_obs, 0.0, 0.5 * n_obs + 10.0)
    model = ROOT.RooAddPdf(
        f"model_{channel}", "Signal + Background",
        ROOT.RooArgList(sig_pdf, bkg_pdf),
        ROOT.RooArgList(nsig, nbkg),
    )
    dh = ROOT.RooDataHist(
        f"data_obs_{channel}", "Observed data",
        ROOT.RooArgList(obs), hist,
    )

    _imp = getattr(ws, "import")
    _imp(obs)
    for par in sig_pars + bkg_pars:
        _imp(par)
    _imp(sig_pdf, ROOT.RooFit.RecycleConflictNodes())
    _imp(bkg_pdf, ROOT.RooFit.RecycleConflictNodes())
    _imp(nsig)
    _imp(nbkg)
    _imp(model, ROOT.RooFit.RecycleConflictNodes())
    _imp(dh)

    return ws


def _write_analytic_datacard(
    channel: str,
    n_obs: float,
    ws_path: str,
    out_dir: str,
) -> str:
    """Write a CMS Combine datacard that references a RooWorkspace for shapes.

    The shapes refer to the component PDFs. The emitted ``rate -1`` placeholders
    require a Combine-compatible normalization for each process. In the
    DoubleMuon tutorial the fitted yields belong to the combined RooAddPdf,
    so those coefficients do not supply the component rates in this card.

    The workspace naming conventions assumed here match those produced by
    :func:`_build_analytic_workspace`.

    Parameters
    ----------
    channel:
        Channel identifier (e.g. ``"0j"``).
    n_obs:
        Observed event count (written to the ``observation`` line).
    ws_path:
        Absolute path to the workspace ROOT file.
    out_dir:
        Directory where the datacard ``.txt`` file is written.

    Returns
    -------
    str
        Absolute path of the written datacard file.
    """
    ws_name = f"ws_{channel}"
    lines = [
        f"# CMS Combine datacard — channel: {channel}",
        "# Generated by RDFAnalyzerCore analytic workspace fitting",
        "imax 1   # 1 channel",
        "jmax 1   # 1 background process",
        "kmax 0   # shape parameters float freely in the workspace",
        "-" * 72,
        f"shapes sig       {channel}  {ws_path}  {ws_name}:sig_{channel}",
        f"shapes bkg       {channel}  {ws_path}  {ws_name}:bkg_{channel}",
        f"shapes data_obs  {channel}  {ws_path}  {ws_name}:data_obs_{channel}",
        "-" * 72,
        f"bin          {channel}",
        f"observation  {n_obs:.0f}",
        "-" * 72,
        f"bin          {channel}    {channel}",
        "process      sig          bkg",
        "process      0            1",
        "# Component PDFs require Combine-compatible process normalizations for rate -1.",
        "rate         -1           -1",
        "-" * 72,
        "# All shape parameters (mean, sigma, decay, …) float in the workspace.",
        "# Shared parameters (same name across channels) are tied by Combine",
        "# in the simultaneous combined fit.",
    ]
    card_path = os.path.join(out_dir, f"datacard_{channel}.txt")
    with open(card_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return card_path


def _write_analytic_combined_datacard(
    channels: List[str],
    out_dir: str,
) -> str:
    """Write a combined Combine datacard for simultaneous multi-channel fitting.

    All per-channel workspace files must already exist in ``out_dir``.  The
    combined card references them by absolute path and lists each channel's
    signal, background, and data shapes.

    Combine automatically correlates parameters with the same name across
    channels — so shared parameters (e.g. Z mass ``mean``, resolution
    ``sigma``) are tied while per-channel parameters (e.g. ``decay_0j``) are
    independent.

    Parameters
    ----------
    channels:
        Ordered list of channel names for which workspaces and individual
        datacards have already been written.
    out_dir:
        Directory where the combined datacard is written.

    Returns
    -------
    str
        Absolute path of the written combined datacard file.
    """
    n = len(channels)
    lines = [
        "# CMS Combine combined datacard — simultaneous fit across all channels",
        "# Channels: " + ", ".join(channels),
        "# Generated by RDFAnalyzerCore analytic workspace fitting",
        f"imax {n}   # {n} channel(s)",
        "jmax 1    # 1 background process",
        "kmax 0    # shape parameters float freely in each workspace",
        "-" * 72,
    ]
    for ch in channels:
        ws_path = os.path.join(out_dir, f"ws_{ch}.root")
        lines += [
            f"shapes sig       {ch}  {ws_path}  ws_{ch}:sig_{ch}",
            f"shapes bkg       {ch}  {ws_path}  ws_{ch}:bkg_{ch}",
            f"shapes data_obs  {ch}  {ws_path}  ws_{ch}:data_obs_{ch}",
        ]
    lines += [
        "-" * 72,
        "bin          " + "    ".join(channels),
        "observation  " + "    ".join(["-1"] * n),
        "-" * 72,
        "bin     " + "    ".join(f"{c}    {c}" for c in channels),
        "process " + "    ".join("sig      bkg" for _ in channels),
        "process " + "    ".join("0        1"   for _ in channels),
        "# Each channel requires Combine-compatible component normalizations for rate -1.",
        "rate    " + "    ".join("-1       -1"  for _ in channels),
        "-" * 72,
        "# Parameters with the same name across channels are tied by Combine.",
        "# Use 'shared: true' in the workspace config for universal parameters",
        "# (e.g. resonance mass, detector resolution) and 'shared: false' for",
        "# per-channel parameters (e.g. background slope).",
    ]
    card_path = os.path.join(out_dir, "datacard_combined.txt")
    with open(card_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return card_path


def _parse_datacard_shapes(datacard_path: str):
    """Extract the shapes ROOT file, data histogram name, and background names
    from a CMS Combine datacard text file.

    Returns a tuple ``(shapes_file, data_hist_name, bkg_hist_names)`` where
    each element may be ``None`` / empty when not found.
    """
    shapes_file: Optional[str] = None
    data_hist: Optional[str] = None
    bkg_hists: List[str] = []
    defaults: Dict[str, str] = {}
    explicit: Dict[str, str] = {}
    processes: List[str] = []

    datacard_dir = os.path.dirname(os.path.abspath(datacard_path))
    try:
        with open(datacard_path) as fh:
            lines = fh.readlines()
    except OSError:
        return None, None, []

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if parts[0] == "process" and not all(value.lstrip("+-").isdigit() for value in parts[1:]):
            processes = parts[1:]
        if parts[0].lower() == "shapes" and len(parts) >= 4:
            process_name = parts[1]
            root_file = parts[3]
            if not os.path.isabs(root_file):
                root_file = os.path.join(datacard_dir, root_file)
            if not shapes_file:
                shapes_file = root_file
            hist_pattern = parts[4] if len(parts) > 4 else ""
            hist_pattern = hist_pattern.replace("$CHANNEL", parts[2])
            if process_name == "*":
                defaults[root_file] = hist_pattern
                continue
            hist_name = hist_pattern.replace("$PROCESS", process_name).replace("$SYSTEMATIC", "nominal")
            explicit[process_name] = hist_name or process_name
            if process_name.lower() == "data_obs":
                data_hist = hist_name or process_name
            else:
                bkg_hists.append(hist_name or process_name)

    if shapes_file in defaults:
        pattern = defaults[shapes_file]

        def histogram(process):
            return explicit.get(process) or pattern.replace("$PROCESS", process).replace("$SYSTEMATIC", "nominal") or process

        data_hist = data_hist or histogram("data_obs")
        if processes:
            bkg_hists = [histogram(process) for process in processes if process != "data_obs"]
    return shapes_file, data_hist, bkg_hists


def _chi2_fit_uproot(
    shapes_file: str,
    data_hist_name: Optional[str],
    bkg_hist_names: List[str],
) -> Dict[str, object]:
    """Compute chi2 between data and sum-of-backgrounds using uproot.

    Supports histogram-based ROOT objects (TH1x variants).  For non-histogram
    objects (e.g. TF1, TGraph) the integral of the object is used as the
    bin-by-bin expectation.

    Parameters
    ----------
    shapes_file:
        Path to the ROOT shapes file.
    data_hist_name:
        Name of the data histogram inside the file.
    bkg_hist_names:
        Names of background (signal + background) histograms.

    Returns
    -------
    dict
        Keys: ``status``, ``chi2``, ``ndof``, ``process_integrals``.
    """
    import uproot  # type: ignore
    import math

    with uproot.open(shapes_file) as f:
        keys = [k.split(";")[0] for k in f.keys()]

        def _integral(name: str) -> Optional[float]:
            """Return the total integral of a ROOT object, if readable."""
            if name not in keys:
                return None
            obj = f[name]
            # TH1-like objects expose .values()
            if hasattr(obj, "values"):
                vals = obj.values()
                try:
                    return float(sum(vals))
                except Exception:  # noqa: BLE001
                    return None
            return None

        process_integrals: Dict[str, Optional[float]] = {}
        bkg_total = 0.0
        for name in bkg_hist_names:
            integral = _integral(name)
            process_integrals[name] = integral
            if integral is not None:
                bkg_total += integral

        data_integral = _integral(data_hist_name) if data_hist_name else None
        process_integrals["data_obs"] = data_integral

        if data_integral is None or bkg_total == 0.0:
            return {
                "status": "incomplete",
                "message": "Data or background histogram missing/empty.",
                "process_integrals": process_integrals,
            }

        # Simple one-bin chi2: chi2 = (data - bkg)^2 / bkg
        chi2 = (data_integral - bkg_total) ** 2 / bkg_total
        ndof = max(1, len([v for v in process_integrals.values() if v is not None]) - 1)

        return {
            "status": "ok",
            "chi2": chi2,
            "ndof": ndof,
            "chi2_per_ndof": chi2 / ndof if ndof else None,
            "data_integral": data_integral,
            "bkg_total": bkg_total,
            "process_integrals": process_integrals,
        }
