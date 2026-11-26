"""Render plots from a merged histogram manifest."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Dict, List

from output_schema import OutputManifest
from .performance import PerformanceRecorder


def _build_plot_request_from_dict(
    meta_file: str,
    output_file: str,
    title: str,
    x_axis_title: str,
    y_axis_title: str,
    log_y: bool,
    draw_ratio: bool,
    processes_cfg: list,
    rdfanalyzer,
):
    """Build a :class:`rdfanalyzer.PlotRequest` from plain Python objects.

    """
    req = rdfanalyzer.PlotRequest()
    req.metaFile = meta_file
    req.outputFile = output_file
    req.title = title
    req.xAxisTitle = x_axis_title
    req.yAxisTitle = y_axis_title
    req.logY = log_y
    req.drawRatio = draw_ratio

    procs = []
    for cfg in processes_cfg:
        proc = rdfanalyzer.PlotProcessConfig()
        proc.directory = cfg.get("directory", "")
        proc.histogramName = cfg.get("histogramName", "")
        proc.legendLabel = cfg.get("legendLabel", "")
        proc.color = int(cfg.get("color", 1))
        proc.scale = float(cfg.get("scale", 1.0))
        proc.normalizationHistogram = cfg.get("normalizationHistogram", "")
        proc.isData = bool(cfg.get("isData", False))
        procs.append(proc)

    req.processes = procs
    return req


class _ManifestPlotOperations:
    def _load_plot_configs(self) -> List[Dict]:
        if Path(self.plot_config).suffix.lower() in (".yaml", ".yml"):
            from native_config_io import load_yaml_document
            configs = load_yaml_document(self.plot_config, typed=True)
        else:
            with open(self.plot_config) as fh:
                configs = json.load(fh)
        if not isinstance(configs, list):
            raise ValueError(
                f"plot_config must be a YAML sequence or JSON array of plots: {self.plot_config}"
            )
        return configs

    def _load_manifest(self) -> OutputManifest:
        if not os.path.exists(self.manifest_path):
            raise RuntimeError(
                f"Manifest file not found: {self.manifest_path!r}.  "
                "Run MergeAll (or MergeHistograms) first."
            )
        try:
            return OutputManifest.load_yaml(self.manifest_path)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to load manifest from {self.manifest_path!r}: {exc}"
            ) from exc

    def run(self):
        manifest = self._load_manifest()
        plot_cfgs = self._load_plot_configs()

        meta_file = ""
        if manifest.histograms is not None:
            meta_file = manifest.histograms.output_file
        if meta_file and not os.path.isabs(meta_file) and "://" not in meta_file:
            meta_file = os.path.abspath(os.path.join(os.path.dirname(self.manifest_path), meta_file))
        has_input = bool(meta_file) and (meta_file.startswith("root://") or os.path.exists(meta_file))
        if not has_input:
            self.publish_message(
                f"WARNING: Histogram ROOT file not found at {meta_file!r}.  "
                "Plots will be attempted but PlottingUtility may fail."
            )

        region_names: List[str] = [r.name for r in manifest.regions if r.name]
        self.publish_message(
            f"ManifestPlotTask: {len(plot_cfgs)} plot config(s), "
            f"{len(region_names) or 1} region(s), "
            f"histogram file: {meta_file!r}"
        )

        Path(self._plots_dir).mkdir(parents=True, exist_ok=True)

        provenance = {
            "task": "ManifestPlotTask",
            "name": self.name,
            "manifest_path": self.manifest_path,
            "manifest_framework_hash": manifest.framework_hash,
            "manifest_user_repo_hash": manifest.user_repo_hash,
            "regions": region_names,
            "n_plot_configs": len(plot_cfgs),
        }

        plots_created: List[str] = []
        errors: List[str] = []

        with PerformanceRecorder("ManifestPlotTask") as rec:
            try:
                import rdfanalyzer  # type: ignore
                pu = rdfanalyzer.PlottingUtility()
                _rdfanalyzer_available = True
            except ImportError:
                pu = None
                _rdfanalyzer_available = False
                rdfanalyzer = None
                self.publish_message(
                    "WARNING: rdfanalyzer module not available.  "
                    "Plot stubs will be written instead of real plots."
                )

            from plot_config import apply_plot_options
            pending = []
            contexts = region_names if region_names else [""]
            for cfg in plot_cfgs:
                for region in contexts:
                    out_file = cfg.get("outputFile", "plot.pdf")
                    if region:
                        out_file = os.path.join(region, out_file)
                    out_path = os.path.join(self._plots_dir, out_file)
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

                    if not _rdfanalyzer_available or not has_input:
                        # Write a stub so the output target is satisfied
                        with open(out_path + ".stub.json", "w") as fh:
                            json.dump(
                                {
                                    "stub": True,
                                    "region": region,
                                    "config": cfg,
                                    "meta_file": meta_file,
                                },
                                fh,
                                indent=2,
                            )
                        plots_created.append(out_path + ".stub.json")
                        continue

                    try:
                        req = _build_plot_request_from_dict(
                            meta_file=meta_file,
                            output_file=out_path,
                            title=cfg.get("title", region or ""),
                            x_axis_title=cfg.get("xAxisTitle", ""),
                            y_axis_title=cfg.get("yAxisTitle", "Counts"),
                            log_y=bool(cfg.get("logY", False)),
                            draw_ratio=bool(cfg.get("drawRatio", True)),
                            processes_cfg=cfg.get("processes", []),
                            rdfanalyzer=rdfanalyzer,
                        )
                        # Explicit placeholders also work in sparse selections.
                        def expand(value):
                            if isinstance(value, str):
                                return value.replace("{region}", region)
                            if isinstance(value, list):
                                return [expand(item) for item in value]
                            if isinstance(value, dict):
                                return {key: expand(item) for key, item in value.items()}
                            return value
                        expanded = expand(cfg)
                        req = apply_plot_options(req, expanded, rdfanalyzer)
                        pending.append(req)
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"Error plotting {out_file!r}: {exc}")

            if pending:
                results = pu.makeStackPlots(pending)
                for req, result in zip(pending, results):
                    if result.success:
                        plots_created.append(req.outputFile)
                    else:
                        errors.append(f"PlottingUtility failed for {req.outputFile!r}: {result.message}")
                provenance["warnings"] = [warning for result in results for warning in result.warnings]

        rec.save(os.path.join(self._plots_dir, "manifest_plot.perf.json"))

        provenance["plots_created"] = plots_created
        provenance["errors"] = errors
        prov_path = os.path.join(self._plots_dir, "provenance.json")
        with open(prov_path, "w") as fh:
            json.dump(provenance, fh, indent=2)

        if errors:
            msg = f"ManifestPlotTask completed with {len(errors)} error(s):\n" + "\n".join(
                f"  {e}" for e in errors
            )
            raise RuntimeError(msg)
        else:
            self.publish_message(
                f"ManifestPlotTask complete.  {len(plots_created)} plot(s) written to {self._plots_dir}."
            )


@dataclass
class ManifestPlotter(_ManifestPlotOperations):
    """Plot a merged histogram manifest with a checked-in JSON request."""

    manifest_path: str
    plot_config: str
    output_dir: str
    name: str = "manifest"

    publish_message = staticmethod(print)

    @property
    def _plots_dir(self) -> str:
        return self.output_dir
