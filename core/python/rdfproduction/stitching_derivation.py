"""Derive stitching weights and cross sections from the verified counter metadata."""

from __future__ import annotations

from native_config_io import job_config_path
from dataclasses import dataclass
import json
import math
import os
import re
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Union
import yaml
from .performance import PerformanceRecorder
from dataset_manifest import DatasetManifest
from derivation_correctionlib import _build_correctionlib_cset as _shared_build_correctionlib_cset
from derivation_correctionlib import _derive_stitch_cross_sections as _shared_derive_stitch_cross_sections
from derivation_correctionlib import _derive_stitch_weights as _shared_derive_stitch_weights
from derivation_correctionlib import _normalize_sample_names as _shared_normalize_sample_names
from derivation_correctionlib import _read_intweight_sign_hist as _shared_read_intweight_sign_hist
from derivation_correctionlib import _read_weight_sign_sum as _shared_read_weight_sign_sum
from submission_backend import read_config
from rdfproduction.jobs.paths import WORKSPACE
from rdfproduction.jobs.staging import _resolve_analysis_config_path
from rdfproduction.jobs.condor_files import _safe_dataset_submission_name
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import correctionlib.schemav2


def _read_intweight_sign_hist(
    meta_file: str,
    sample_name: str,
) -> list[float]:
    return _shared_read_intweight_sign_hist(meta_file, sample_name)


def _read_weight_sign_sum(meta_file: str, sample_name: str) -> float:
    return _shared_read_weight_sign_sum(meta_file, sample_name)


def _derive_stitch_weights(
    group_name: str,
    meta_files: dict[str, Union[str, list[str]]],
) -> dict[str, list[float]]:
    return _shared_derive_stitch_weights(
        group_name,
        meta_files,
        _read_intweight_sign_hist,
    )


def _derive_stitch_cross_sections(
    group_name: str,
    meta_files: dict[str, Union[str, list[str]]],
    inclusive_sample: Union[str, list[str]],
    inclusive_cross_section: float,
    *,
    inclusive_sample_fractions: Optional[dict[str, float]] = None,
) -> list[float]:
    if inclusive_sample_fractions is not None:
        anchor_bins = [
            _shared_derive_stitch_cross_sections(
                group_name, meta_files, sample, inclusive_cross_section * fraction,
                _read_intweight_sign_hist,
            )
            for sample, fraction in inclusive_sample_fractions.items()
        ]
        if len({len(bins) for bins in anchor_bins}) != 1:
            raise RuntimeError(f"Inclusive anchors in stitching group '{group_name}' have different binning.")
        return [math.fsum(values) for values in zip(*anchor_bins)]
    return _shared_derive_stitch_cross_sections(
        group_name,
        meta_files,
        inclusive_sample,
        inclusive_cross_section,
        _read_intweight_sign_hist,
    )


def _normalize_stitch_cross_section_era(value: object) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    if normalized in {"run2", "2", "2016", "2017", "2018"}:
        return "run2"
    if normalized in {"24", "2024", "run3_24", "run3_2024"}:
        return "run3_24"
    if normalized in {
        "22_23",
        "2022_2023",
        "run3",
        "run3_22_23",
        "run3_2022_2023",
    }:
        return "run3_22_23"
    raise ValueError(
        f"Unknown stitching cross-section era '{value}'; expected run2, "
        "22/23, or 24"
    )


def _resolve_stitch_cross_section_era(
    group_cfg: dict[str, Any],
    sample_names: list[str],
    source_dataset_manifest: str = "",
    source_skim_name: str = "",
) -> str:
    configured = group_cfg.get("cross_section_era")
    if configured is not None:
        return _normalize_stitch_cross_section_era(configured)

    if source_dataset_manifest and os.path.isfile(source_dataset_manifest):
        manifest = DatasetManifest.load(source_dataset_manifest)
        selected = set(sample_names)
        years = {
            int(entry.year)
            for entry in manifest.datasets
            if entry.name in selected and entry.year is not None
        }
        run2_years = years & {16, 17, 18, 2016, 2017, 2018}
        run3_years = years & {22, 23, 24, 2022, 2023, 2024}
        if run2_years and run3_years:
            raise RuntimeError(
                "Binned cross sections cannot combine Run 2 and Run 3 "
                "samples in one stitching derivation"
            )
        if years and years <= {2016, 2017, 2018, 16, 17, 18}:
            return "run2"
        if years and years & {24, 2024}:
            if years - {24, 2024}:
                raise RuntimeError(
                    "Binned cross sections cannot combine Run 3 2024 samples "
                    "with 2022/23 samples in one stitching derivation"
                )
            return "run3_24"
        if years and years <= {2022, 2023, 22, 23}:
            return "run3_22_23"

    if source_skim_name:
        lowered = source_skim_name.lower()
        if "run2" in lowered:
            return "run2"
        if re.search(r"(?:^|[^0-9])24(?:[^0-9]|$)", lowered):
            return "run3_24"
    return "run3_22_23"


def _resolve_stitch_inclusive_sample(
    group_name: str,
    group_cfg: dict[str, Any],
    sample_names: list[str],
    cross_section_era: str = "",
) -> Optional[Union[str, list[str]]]:
    configured = group_cfg.get("inclusive_samples")
    if configured is None:
        configured = group_cfg.get("inclusive_sample")
    if configured is None:
        return None

    if isinstance(configured, dict):
        era_keys = [cross_section_era, "default"] if cross_section_era else ["default"]
        selected = next(
            (configured[key] for key in era_keys if key in configured), None
        )
        if selected is None:
            raise RuntimeError(
                f"Inclusive samples for group '{group_name}' have no entry for "
                f"cross-section era '{cross_section_era}'"
            )
        configured = selected

    try:
        configured_samples = _shared_normalize_sample_names(
            configured, "inclusive_sample"
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid inclusive samples for group '{group_name}': {exc}"
        ) from exc

    missing_samples = [
        sample_name
        for sample_name in configured_samples
        if sample_name not in sample_names
    ]
    if missing_samples:
        raise RuntimeError(
            f"Inclusive sample(s) {missing_samples} for group '{group_name}' "
            "are not present in the resolved stitch samples"
        )
    return (
        configured_samples[0]
        if len(configured_samples) == 1
        else configured_samples
    )


def _resolve_stitch_inclusive_sample_fractions(
    group_name: str,
    group_cfg: dict[str, Any],
    inclusive_sample: Optional[Union[str, list[str]]],
    era: str,
) -> Optional[dict[str, float]]:
    configured = group_cfg.get("inclusive_sample_fractions")
    if configured is None:
        return None
    label = f"Stitch group '{group_name}' inclusive_sample_fractions"
    if not isinstance(configured, dict):
        raise RuntimeError(f"{label} must be an era-to-fractions mapping.")
    if era not in configured:
        return None
    anchors = (
        _shared_normalize_sample_names(inclusive_sample, "inclusive_sample")
        if inclusive_sample
        else []
    )
    fractions = configured[era]
    if (
        not anchors
        or len(set(anchors)) != len(anchors)
        or not isinstance(fractions, dict)
        or set(fractions) != set(anchors)
    ):
        raise RuntimeError(f"{label} for '{era}' must assign a fraction to each inclusive sample.")
    if any(
        type(value) not in (int, float) or not math.isfinite(value) or value <= 0.0
        for value in fractions.values()
    ):
        raise RuntimeError(f"{label} for '{era}' must have finite, positive fractions.")
    if not math.isclose(math.fsum(fractions.values()), 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise RuntimeError(f"{label} for '{era}' must sum to one.")
    return {sample: float(fractions[sample]) for sample in anchors}


def _resolve_stitch_inclusive_cross_section(
    group_name: str,
    group_cfg: dict[str, Any],
    config: dict[str, Any],
    era: str,
) -> Optional[float]:
    configured = group_cfg.get(
        "inclusive_cross_section_pb",
        group_cfg.get("inclusive_cross_section"),
    )
    if configured is None:
        configured_by_group = config.get("inclusive_cross_sections", {})
        if isinstance(configured_by_group, dict):
            configured = configured_by_group.get(group_name)
            if isinstance(configured, dict):
                configured = configured.get(era)

    if configured is None:
        return None
    try:
        return float(configured)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Invalid inclusive cross section '{configured}' for group '{group_name}'"
        ) from exc


def _build_correctionlib_cset(
    all_scale_factors: dict[str, dict[str, list[float]]],
    description: str = "Stitching scale factors derived from counter_intWeightSignSum histograms",
) -> "correctionlib.schemav2.CorrectionSet":
    return _shared_build_correctionlib_cset(all_scale_factors, description)


def _stitch_meta_read_path(path: str) -> str:
    """Convert EOS stage-out paths into worker-independent XRootD URLs."""
    if path.startswith("/eos/"):
        return f"root://eosuser.cern.ch/{path}"
    return path


def _stitch_meta_files_for_sample(
    run_name: str,
    sample_name: str,
) -> list[str]:
    """Return local metadata files or staged Condor metadata destinations."""
    output_dir = Path(WORKSPACE) / f"skimRun_{run_name}" / "outputs" / sample_name
    local_paths = sorted(
        str(path)
        for path in output_dir.rglob("meta.root")
        if path.is_file()
    )
    if local_paths:
        return local_paths

    submission_dir = (
        Path(WORKSPACE)
        / f"skimRun_{run_name}"
        / "condor_submissions"
        / _safe_dataset_submission_name(sample_name)
    )
    staged_paths: list[str] = []
    for job_dir in sorted(submission_dir.glob("job_*/")):
        config_path = job_config_path(job_dir)
        if not config_path.is_file():
            continue
        config = read_config(str(config_path))
        meta_path = str(
            config.get("__orig_metaFile", config.get("metaFile", "")) or ""
        ).strip()
        if not meta_path:
            continue
        if (
            os.path.isfile(meta_path)
            or meta_path.startswith("root://")
            or meta_path.startswith(("/eos/", "/store/"))
        ):
            staged_paths.append(_stitch_meta_read_path(meta_path))
    return sorted(dict.fromkeys(staged_paths))


def _resolve_stitch_meta_files(
    group_name: str,
    group_cfg: dict[str, Any],
    stitch_config_path: str,
    source_skim_name: str = "",
    source_dataset_manifest: str = "",
    verified_meta_files: Optional[dict[str, list[str]]] = None,
) -> dict[str, Union[str, list[str]]]:
    """Resolve one stitch group to sample names and metadata ROOT files."""
    raw_meta_files = group_cfg.get("meta_files")
    if isinstance(raw_meta_files, dict):
        if verified_meta_files is not None:
            raise ValueError(f"Stitch group {group_name!r} must use samples or select with a verified counter pass")
        resolved: dict[str, Union[str, list[str]]] = {}
        for sample_name, raw_paths in raw_meta_files.items():
            paths = raw_paths if isinstance(raw_paths, list) else [raw_paths]
            resolved_paths = [
                _resolve_analysis_config_path(stitch_config_path, str(path))
                for path in paths
            ]
            resolved[str(sample_name)] = (
                resolved_paths[0]
                if len(resolved_paths) == 1
                else resolved_paths
            )
        return resolved

    if not source_skim_name and verified_meta_files is None:
        raise RuntimeError(
            f"Group '{group_name}' must contain 'meta_files' unless "
            "source_skim_name is configured."
        )
    if not source_dataset_manifest:
        raise RuntimeError(
            "source_dataset_manifest is required when resolving stitch metadata "
            "from a source skim run."
        )

    manifest = DatasetManifest.load(source_dataset_manifest)
    sample_names = group_cfg.get("samples", [])
    if sample_names and not isinstance(sample_names, list):
        raise RuntimeError(
            f"Group '{group_name}' has a 'samples' value that is not a list."
        )
    if not sample_names:
        selector = group_cfg.get("select")
        if selector is None:
            selector = {"group": group_name}
        if not isinstance(selector, dict):
            raise RuntimeError(
                f"Group '{group_name}' has a 'select' value that is not a mapping."
            )
        try:
            sample_names = [entry.name for entry in manifest.query(**selector)]
        except TypeError as exc:
            raise RuntimeError(
                f"Invalid dataset selector for stitch group '{group_name}': {selector}"
            ) from exc

    if not sample_names:
        raise RuntimeError(
            f"Stitch group '{group_name}' did not resolve to any dataset samples."
        )

    resolved = {}
    for sample_name in sample_names:
        sample = str(sample_name)
        meta_paths = (verified_meta_files.get(sample, []) if verified_meta_files is not None
                      else _stitch_meta_files_for_sample(source_skim_name, sample))
        if verified_meta_files is not None and not meta_paths:
            raise ValueError(f"Verified counter outputs contain no metadata for stitch sample {sample!r}")
        if not meta_paths:
            raise RuntimeError(
                f"No meta.root files found for stitch sample '{sample}' under "
                f"'skimRun_{source_skim_name}/outputs/{sample}' or its Condor "
                "submission metadata."
            )
        resolved[sample] = meta_paths[0] if len(meta_paths) == 1 else meta_paths
    return resolved


class StitchingDerivationOperations(object):
    @property
    def _output_dir(self) -> str:
        return os.path.join(WORKSPACE, f"stitchWeights_{self.name}")

    def derive(self, *, verified_meta_files=None, output_dir=None):
        import yaml  # type: ignore

        publish = self.publish_message if output_dir is None else print
        config_path = os.path.abspath(self.stitch_config)
        if not os.path.isfile(config_path):
            raise RuntimeError(
                f"Stitch configuration file not found: '{config_path}'"
            )

        with open(config_path) as fh:
            config = yaml.safe_load(fh)

        if "groups" not in config or not isinstance(config["groups"], dict):
            raise RuntimeError(
                f"Stitch config '{config_path}' must contain a top-level "
                "'groups' mapping.  See the task docstring for the format."
            )

        all_scale_factors: dict[str, dict[str, list[float]]] = {}
        all_binned_cross_sections: dict[str, dict[str, Any]] = {}

        with PerformanceRecorder("StitchingDerivationTask") as rec:
            for group_name, group_cfg in config["groups"].items():
                if not isinstance(group_cfg, dict):
                    raise RuntimeError(
                        f"Group '{group_name}' in '{config_path}' must be a mapping."
                    )

                meta_files = _resolve_stitch_meta_files(
                    str(group_name),
                    group_cfg,
                    config_path,
                    source_skim_name=self.source_skim_name,
                    source_dataset_manifest=self.source_dataset_manifest,
                    verified_meta_files=verified_meta_files,
                )

                publish(
                    f"Processing group '{group_name}' "
                    f"({len(meta_files)} sample(s): {list(meta_files)})…"
                )

                scale_factors = _derive_stitch_weights(group_name, meta_files)
                all_scale_factors[group_name] = scale_factors

                cross_section_era = _resolve_stitch_cross_section_era(
                    group_cfg,
                    list(meta_files),
                    source_dataset_manifest=self.source_dataset_manifest,
                    source_skim_name=self.source_skim_name,
                )
                inclusive_sample = _resolve_stitch_inclusive_sample(
                    str(group_name),
                    group_cfg,
                    list(meta_files),
                    cross_section_era,
                )
                inclusive_cross_section = _resolve_stitch_inclusive_cross_section(
                    str(group_name), group_cfg, config, cross_section_era
                )
                sample_fractions = _resolve_stitch_inclusive_sample_fractions(
                    str(group_name), group_cfg, inclusive_sample, cross_section_era
                )
                if sample_fractions is not None and inclusive_cross_section is None:
                    raise RuntimeError(
                        f"Inclusive sample fractions for '{group_name}' require a total cross section."
                    )
                if inclusive_sample and inclusive_cross_section is not None:
                    binned_cross_sections = _derive_stitch_cross_sections(
                        str(group_name),
                        meta_files,
                        inclusive_sample,
                        inclusive_cross_section,
                        inclusive_sample_fractions=sample_fractions,
                    )
                    all_binned_cross_sections[str(group_name)] = {
                        "era": cross_section_era,
                        "inclusive_sample": inclusive_sample,
                        "inclusive_samples": _shared_normalize_sample_names(
                            inclusive_sample, "inclusive_sample"
                        ),
                        "inclusive_cross_section_pb": inclusive_cross_section,
                        "bin_fractions": [
                            value / inclusive_cross_section
                            for value in binned_cross_sections
                        ],
                        "binned_cross_sections_pb": binned_cross_sections,
                    }
                    if sample_fractions is not None:
                        all_binned_cross_sections[str(group_name)]["inclusive_sample_fractions"] = (
                            sample_fractions
                        )
                    publish(
                        f"  group '{group_name}': binned cross sections computed from "
                        f"inclusive sample '{inclusive_sample}' for {cross_section_era}."
                    )
                elif inclusive_cross_section is not None:
                    publish(
                        f"  group '{group_name}': no unambiguous inclusive sample; "
                        "binned cross sections were not written for this group."
                    )

                publish(
                    f"  group '{group_name}': scale factors computed for "
                    f"{len(scale_factors)} sample(s)."
                )

        # ---- build and write correctionlib JSON ----------------------------
        output_dir = Path(output_dir or self._output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        cset = _build_correctionlib_cset(all_scale_factors)
        json_path = output_dir / "stitch_weights.json"
        with open(json_path, "w") as fh:
            fh.write(cset.model_dump_json(indent=2))

        cross_sections_path = output_dir / "stitch_cross_sections.json"
        with open(cross_sections_path, "w") as fh:
            json.dump(
                {
                    "schema_version": 1,
                    "description": (
                        "Cross sections split into stitch bins using the "
                        "nonnegative net signed event fractions of each "
                        "inclusive sample."
                    ),
                    "groups": all_binned_cross_sections,
                },
                fh,
                indent=2,
            )

        rec.save(str(output_dir / "stitch_derivation.perf.json"))
        publish(
            f"Stitch weights (correctionlib) written to: {json_path}; "
            f"binned cross sections written to: {cross_sections_path}"
        )


@dataclass
class StitchingDerivation(StitchingDerivationOperations):
    name: str
    stitch_config: str
    source_dataset_manifest: str = ""
    source_skim_name: str = ""

    publish_message = staticmethod(print)
