"""Artifact-based adapters for downstream Snakemake rules."""

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import sys

from native_config_io import load_yaml_document

from dataset_manifest import DatasetManifest
from output_schema import DatasetManifestProvenance, OutputManifest, SkimSchema
from .controller import _credential_environment, run_directory
from .specs import inspect_spec, load_spec_document, normalize_spec_paths
from run_bundle import (
    atomic_directory, derived_artifact, file_digest, find_bundle, load_artifact_receipt, seal_bundle,
    verify_artifact, verify_bundle, write_json,
)
from submission_backend import ensure_xrootd_redirector, read_config
from statistical_inference.diagnostic_plots import PLOT_TYPES
from .output_index import _execution_plan, load_verified_output_index, verify_output_signatures


_BOOL = {"strict_coverage", "skip_fit"}
_FIT_OPTIONS = {
    "fitting_backend", "fit_dataset", "method", "combine_exe", "combine_options",
    "rabbit_python", "rabbit_device", "rabbit_devices", "rabbit_n_devices", "rabbit_mc_stat",
    "rabbit_likelihood", "rabbit_covariance", "rabbit_zero_shape_policy", "rabbit_hvp_batch",
}
CONSUMER_STAGES = ("plots", "datacards", "fit", "analytic_fit", "fit_diagnostics")
MANIFEST_CONSUMERS = {"plots", "datacards", "analytic_fit"}


def downstream_paths(root, era):
    directory = Path(root) / era / "downstream"
    paths = {key: directory / name for key, name in {
        "refill_binding": "refill-inputs", "refill_index": "refill_index.json",
        "input_manifests": "job-manifests", "manifest_merge": "merged", "plots": "plots",
        **{stage: stage for stage in CONSUMER_STAGES},
    }.items()}
    for key in ("manifest_merge", *CONSUMER_STAGES):
        paths[f"{key}_receipt"] = paths[key].with_suffix(".json")
    return paths


def needs_manifest_merge(settings):
    if "manifest" in settings:
        return False
    manifest_stages = MANIFEST_CONSUMERS & settings.keys()
    if settings.get("datacards", {}).get("raw_inputs"):
        manifest_stages.discard("datacards")
    return bool(manifest_stages or {"job_manifests", "refill", "saved_production"} & settings.keys()
                or not {"saved_datacards", "input_card", "saved_fit", "datacards"} & settings.keys())


def downstream_targets(plans, root):
    targets = []
    for era, settings in plans.items():
        paths = downstream_paths(root, era)
        if needs_manifest_merge(settings):
            targets.append(str(paths["manifest_merge_receipt"]))
        targets.extend(str(paths[f"{key}_receipt"]) for key in CONSUMER_STAGES if key in settings)
    return targets


def saved_manifest_files(sources, *, roles=("skim", "histograms", "cutflow", "metadata")):
    """Return local artifacts to track as dependencies of saved manifests."""
    files = []
    for source in sources:
        manifest = OutputManifest.load_yaml(source)
        for role in roles:
            schema = getattr(manifest, role)
            if schema and schema.output_file and "://" not in _root_path(schema.output_file):
                files.append(str((Path(source).parent / schema.output_file).resolve()))
    return list(dict.fromkeys(files))


def card_shapes(card):
    """Local shape files referenced by a completed Combine datacard."""
    paths = []
    for line in Path(card).read_text().splitlines():
        fields = line.split("#", 1)[0].split()
        # Combine accepts extArg NAME file.root:workspace and external
        # rateParams. Include local referenced workspaces as DAG inputs.
        if fields[1:2] in (["extArg"], ["rateParam"]):
            for field in fields[2:]:
                if ".root:" in field:
                    filename = field.split(".root:", 1)[0] + ".root"
                    workspace = (Path(card).parent / filename).resolve()
                    if not workspace.is_file():
                        raise ValueError(f"Missing local workspace {filename!r} referenced by {card}")
                    paths.append(str(workspace))
        if fields[:1] == ["shapes"]:
            if len(fields) < 4:
                raise ValueError(f"Malformed shapes declaration in {card}: {line}")
            if fields[3] != "FAKE":
                shape = (Path(card).parent / fields[3]).resolve()
                if not shape.is_file():
                    raise ValueError(f"Missing local shapes file {fields[3]!r} referenced by {card}")
                paths.append(str(shape))
    return list(dict.fromkeys(paths))


def raw_card_inputs(config):
    """Declare local input ROOT files using the datacard generator's working-directory convention."""
    document = load_yaml_document(config, typed=True)
    samples = document.get("input_files") if isinstance(document, dict) else None
    if not isinstance(samples, dict) or not samples:
        raise ValueError(f"{config}: raw datacards require a nonempty input_files mapping")
    paths = []
    for sample, settings in samples.items():
        value = settings.get("path") if isinstance(settings, dict) else None
        if not isinstance(value, str) or not value:
            raise ValueError(f"{config}: input_files.{sample}.path must be a path")
        if not value.startswith("root://"):
            paths.append(str(Path(value).resolve()))
    return list(dict.fromkeys(paths))


def describe_fit_diagnostics(options, fits, label):
    if not isinstance(options, dict) or options.keys() - {"source", "required", "max_parameters"}:
        raise ValueError(f"{label} has unknown options")
    options = dict(options)
    if "source" not in options and len(fits) == 1:
        options["source"] = fits[0]
    if options.get("source") not in fits:
        raise ValueError(f"{label}.source must select a configured fit or analytic_fit that runs a fit")
    required = options.get("required", [])
    if not isinstance(required, list) or any(name not in PLOT_TYPES for name in required):
        raise ValueError(f"{label}.required must list diagnostic types: {', '.join(PLOT_TYPES)}")
    limit = options.get("max_parameters", 24)
    if type(limit) is not int or limit < 1:
        raise ValueError(f"{label}.max_parameters must be a positive integer")
    return options


def describe_downstream(settings, eras, *, base=Path(".")):
    """Resolve configured downstream stages from their declared inputs, before opening any event files."""
    if not isinstance(settings, dict) or not settings:
        raise ValueError("downstream must map eras to nonempty stage configurations")
    plans = {str(era): value for era, value in settings.items()}
    if len(plans) != len(settings) or plans.keys() - set(eras):
        raise ValueError("downstream must identify distinct selected eras")

    def path(value, label):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be a path")
        resolved = (base / Path(value).expanduser()).resolve()
        if not resolved.is_file():
            raise ValueError(f"{label} does not exist: {resolved}")
        return str(resolved)

    def saved_products(value, label, allowed):
        if not isinstance(value, dict) or set(value) != {"directory", "receipt"}:
            raise ValueError(f"{label} requires directory and receipt")
        directory_name = value["directory"]
        if not isinstance(directory_name, str) or not directory_name.strip():
            raise ValueError(f"{label}.directory must be a path")
        directory = (base / Path(directory_name).expanduser()).resolve()
        receipt = path(value["receipt"], f"{label}.receipt")
        record = load_artifact_receipt(receipt)
        if (not directory.is_dir() or record["kind"] not in allowed or not record["files"]
                or any(not Path(product).resolve().is_relative_to(directory)
                       for product in record["files"])):
            raise ValueError(f"{label} receipt does not describe {directory}")
        return {"directory": str(directory), "receipt": receipt, "kind": record["kind"]}

    for era, plan in plans.items():
        if not isinstance(plan, dict) or plan.keys() - {
            "refill", "job_manifests", "manifest", "saved_production", "saved_datacards", "input_card", "saved_fit", *CONSUMER_STAGES,
        }:
            raise ValueError(f"{era} has unknown downstream settings")
        if len(plan.keys() & {"refill", "job_manifests", "manifest"}) > 1:
            raise ValueError(f"{era} must choose at most one of refill, job_manifests or manifest")
        card_sources = plan.keys() & {"datacards", "saved_datacards", "input_card"}
        if len(card_sources) > 1:
            raise ValueError(f"{era} must choose one datacard source for fit")
        if "input_card" in plan and "fit" not in plan:
            raise ValueError(f"{era}.input_card requires fit")
        if "fit" in plan and not card_sources:
            raise ValueError(f"{era} fit requires datacards")
        if "saved_datacards" in plan and ("fit" not in plan or "datacards" in plan):
            raise ValueError(f"{era} saved_datacards requires fit and excludes generated datacards")
        if "saved_fit" in plan and "fit_diagnostics" not in plan:
            raise ValueError(f"{era} saved_fit requires fit_diagnostics")
        result = {key: path(plan[key], f"{era}.{key}")
                  for key in ("refill", "manifest", "plots", "input_card") if key in plan}
        if "saved_production" in plan:
            saved = plan["saved_production"]
            if not isinstance(saved, dict) or set(saved) != {"spec", "index"}:
                raise ValueError(f"{era}.saved_production requires spec and index")
            if plan.keys() & {"job_manifests", "manifest"}:
                raise ValueError(f"{era}.saved_production conflicts with supplied manifests")
            result["saved_production"] = {key: path(value, f"{era}.saved_production.{key}")
                                          for key, value in saved.items()}
        if "saved_datacards" in plan:
            result["saved_datacards"] = saved_products(plan["saved_datacards"], f"{era}.saved_datacards",
                                                        {"datacards"})
        if "saved_fit" in plan:
            saved_fit = saved_products(plan["saved_fit"], f"{era}.saved_fit", {"fit", "analytic_fit"})
            if saved_fit["kind"] in plan:
                raise ValueError(f"{era} cannot both generate and reuse {saved_fit['kind']}")
            result["saved_fit"] = saved_fit
        if "job_manifests" in plan:
            sources = plan["job_manifests"]
            if not isinstance(sources, list) or not sources:
                raise ValueError(f"{era}.job_manifests must list individual manifest files")
            result["job_manifests"] = [path(source, f"{era}.job_manifests") for source in sources]
            if len(result["job_manifests"]) != len(set(result["job_manifests"])):
                raise ValueError(f"{era}.job_manifests contains duplicate inputs")
        allowed = {
            "datacards": {"config", "strict_coverage", "dataset_manifest", "raw_inputs"},
            "fit": _FIT_OPTIONS,
            "analytic_fit": {"config", "method", "combine_exe", "combine_options", "skip_fit"},
        }
        for stage in allowed.keys() & plan.keys():
            options = plan[stage]
            if not isinstance(options, dict) or options.keys() - allowed[stage]:
                raise ValueError(f"{era}.{stage} has unknown options")
            options = dict(options)
            for key in options.keys() & (_BOOL | {"raw_inputs"}):
                if type(options[key]) is not bool:
                    raise ValueError(f"{era}.{stage}.{key} must be a boolean")
            if stage in ("datacards", "analytic_fit"):
                options["config"] = path(options.get("config"), f"{era}.{stage}.config")
            if "dataset_manifest" in options:
                options["dataset_manifest"] = path(options["dataset_manifest"], f"{era}.{stage}.dataset_manifest")
            if options.get("raw_inputs"):
                if options.keys() & {"strict_coverage", "dataset_manifest"}:
                    raise ValueError(f"{era}.datacards.raw_inputs excludes manifest-based options")
                raw_card_inputs(options["config"])
            result[stage] = options
        if "fit_diagnostics" in plan:
            label = f"{era}.fit_diagnostics"
            fits = [stage for stage in ("fit", "analytic_fit") if stage in result
                    and not result[stage].get("skip_fit")]
            if "saved_fit" in result:
                fits.append(result["saved_fit"]["kind"])
            result["fit_diagnostics"] = describe_fit_diagnostics(plan["fit_diagnostics"], fits, label)
        plans[era] = result
    return plans


def _root_path(path):
    return ensure_xrootd_redirector(path, "root://eosuser.cern.ch/")


@contextmanager
def _verified_production(spec_path, index_path):
    from rdfproduction.jobs.output_validation import _output_file_info_batch

    spec = inspect_spec(spec_path)
    parameters = spec["task_parameters"]
    run = run_directory(parameters["name"], workspace=spec.get("state_root"))
    if json.loads((run / "rdf_production.json").read_text()) != spec:
        raise ValueError(f"Production spec changed after the run was claimed: {run}")
    _, identity, plan = _execution_plan(run)
    index = load_verified_output_index(index_path)
    if (index["execution_id"], index["plan_id"]) != (identity["execution_id"], plan["execution_id"]):
        raise ValueError(f"Verified index does not belong to {run}")
    with _credential_environment(parameters.get("x509")):
        verify_output_signatures(index, _output_file_info_batch)
        yield parameters, index


def bind_refill(spec_path, index_path, template, destination):
    """Freeze the exact terminal skim files as the input manifest of a new run."""
    template = Path(template)
    raw = normalize_spec_paths(load_spec_document(template), template.parent)
    plan = inspect_spec(template)
    settings = plan["task_parameters"]
    source_bundle = find_bundle(settings["submit_config"])
    sealed = verify_bundle(source_bundle)
    dataset_path = Path(settings["dataset_manifest"])
    dataset_relative = dataset_path.relative_to(source_bundle)
    submit_relative = Path(settings["submit_config"]).relative_to(source_bundle)
    if dataset_relative == submit_relative:
        raise ValueError("refill requires a separate dataset manifest")
    datasets = DatasetManifest.load(str(dataset_path))
    if not datasets.datasets:
        raise ValueError("refill dataset manifest is empty")
    with _verified_production(spec_path, index_path) as (upstream, index):
        check_refill_settings(settings, upstream)
        for entry in datasets.datasets:
            jobs = index["datasets"].get(entry.name, {}).get("jobs", {})
            if not jobs or any("save" not in job["outputs"] for job in jobs.values()):
                raise ValueError(f"refill requires verified skim outputs for {entry.name!r}")
            entry.files = [_root_path(job["outputs"]["save"])
                           for _, job in sorted(jobs.items(), key=lambda entry: int(entry[0]))]
            entry.das = None
        final_bundle = destination / "bundle"
        with atomic_directory(destination) as staging:
            bundle = staging / "bundle"
            for relative in sealed["files"]:
                target = bundle / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_bundle / relative, target)
            datasets.save_yaml(str(bundle / dataset_relative))
            seal_bundle(bundle, assets=sealed["assets"], entrypoints=sealed["entrypoints"],
                        metadata={**sealed["metadata"], "refill_index_id": index["index_id"],
                                  "refill_template_bundle_id": sealed["bundle_id"]})
            raw.update(submit_config=str(final_bundle / submit_relative),
                       dataset_manifest=str(final_bundle / dataset_relative))
            write_json(staging / "spec.json", raw)


def check_refill_settings(settings, upstream):
    config = read_config(settings["submit_config"])
    if (settings.get("file_source") not in (None, "", "manifest")
            or settings.get("file_source_name") or settings["sizing_pilots"]):
        raise ValueError("refill uses its own verified skim inventory. Use file_source: manifest for partitioned refill")
    if str(config.get("enableSkim", "")).lower() in {"1", "yes", "true", "on"}:
        raise ValueError("set enableSkim: false in the histogram refill bundle")
    if any(int(config.get(key, 0)) for key in ("firstEntry", "lastEntry")):
        raise ValueError("refill ranges are assigned by the partition planner; leave firstEntry and lastEntry unset")
    upstream_tree = read_config(upstream["submit_config"]).get("saveTree", "Events")
    if config.get("treeList", "Events") != upstream_tree:
        raise ValueError(f"refill treeList must match upstream saveTree {upstream_tree!r}")


def _production_manifests(spec, index_path, directory, *, roles):
    from rdfproduction.jobs.provenance import _build_histfill_manifest

    with _verified_production(spec, index_path) as (parameters, index):
        config_path = parameters["submit_config"]
        dataset_path = parameters["dataset_manifest"]
        datasets = DatasetManifest.load(dataset_path)
        tree = read_config(config_path).get("saveTree", "Events")
        directory.mkdir(parents=True, exist_ok=True)
        for sample_id, (sample, entry) in enumerate(index["datasets"].items()):
            dataset = datasets.by_name(sample)
            if dataset is None:
                raise ValueError(f"Verified output dataset absent from the run manifest: {sample}")
            for partition, job in entry["jobs"].items():
                outputs = job["outputs"]
                if roles == "skim" and "save" not in outputs:
                    continue
                if roles == "skim":
                    manifest = OutputManifest(dataset_manifest_provenance=DatasetManifestProvenance(
                        manifest_path=dataset_path, manifest_hash=DatasetManifest.file_hash(dataset_path),
                        resolved_entry_names=[sample]))
                else:
                    manifest = _build_histfill_manifest(
                        dataset, config_path, dataset_path, config_path, meta_file=_root_path(outputs["meta"]))
                if roles != "histograms" and "save" in outputs:
                    manifest.skim = SkimSchema(output_file=_root_path(outputs["save"]), tree_name=tree)
                manifest.save_yaml(str(directory / f"{sample_id}-{partition}.yaml"))


def publish_manifests(spec, index, destination, *, refill_spec=None, refill_index=None):
    with atomic_directory(destination) as staging:
        _production_manifests(spec, index, staging / "primary",
                              roles="skim" if refill_spec else "all")
        if refill_spec:
            _production_manifests(refill_spec, refill_index, staging / "refill", roles="histograms")


def merge_manifests(destination, *, production=None, supplied=(), receipt=None):
    from .manifest_merge import merge_manifest_outputs, _resolve_manifest_root_path

    if not (production or supplied):
        raise ValueError("manifest merge needs a production inventory or saved job manifests")
    if supplied:
        # Preserve the exact rebased inputs referenced by merged provenance.
        source_dir = destination / "source-manifests"
        source_dir.mkdir(parents=True)
        for slot, source in enumerate(supplied):
            manifest = OutputManifest.load_yaml(str(source))
            for role in ("skim", "histograms", "cutflow", "metadata"):
                schema = getattr(manifest, role)
                if schema and schema.output_file:
                    schema.output_file = _resolve_manifest_root_path(str(source), schema.output_file)
            manifest.save_yaml(str(source_dir / f"{slot}.yaml"))
    else:
        source_dir = production
    merge_manifest_outputs(
        source_dir, destination, name="snakemake", manifest_glob="**/*.yaml",
        group_production_outputs=bool(production),
        metadata_manifest_glob="refill/*.yaml" if production and (source_dir / "refill").is_dir() else "",
    )
    _publish_receipt("manifest_merge", destination, {"input": str(source_dir)}, {}, receipt)


def _manifest_path(source):
    return source / "histograms" / "output_manifest.yaml" if source.is_dir() else source


def _check_products(paths, label):
    if not paths or any(not path.is_file() or path.stat().st_size == 0 for path in paths):
        raise RuntimeError(f"missing or empty {label} products")


def _publish_receipt(stage, destination, parents, options, receipt):
    receipt = receipt or destination / "receipt.json"
    products = [path for path in destination.rglob("*") if path.is_file() and path != receipt]
    write_json(receipt,
               derived_artifact(kind=stage, parents=parents, configuration=options, files=products))


def run_consumer(stage, source, destination, options, *, receipt=None, source_receipt=None):
    if (receipt or destination / "receipt.json").exists():
        raise ValueError(f"downstream destination already has a published receipt: {destination}")
    parent = {"input": str(source)}
    if source.is_file():
        parent["input_sha256"] = file_digest(source)
    if stage == "fit_diagnostics" or source.is_dir():
        recorded = verify_artifact(source_receipt or source / "receipt.json")
        expected = {"fit": "datacards", "fit_diagnostics": options.get("source")}.get(stage, "manifest_merge")
        if recorded["kind"] != expected or any(not Path(path).resolve().is_relative_to(source)
                                                  for path in recorded["files"]):
            raise ValueError(f"downstream receipt does not describe {source}")
        parent["artifact_id"] = recorded["artifact_id"]
    if stage == "fit_diagnostics":
        from statistical_inference.diagnostic_plots import render_fit_diagnostics
        with atomic_directory(destination) as staging:
            render_fit_diagnostics(source, staging, recorded, options)
        _publish_receipt(stage, destination, parent, options, receipt)
        return

    raw_cards = stage == "datacards" and options.get("raw_inputs", False)
    manifest_path = _manifest_path(source) if stage != "fit" and not raw_cards else None
    settings = dict(options)
    common = {"name": "snakemake", "output_dir": str(destination)}
    if stage == "plots":
        from .manifest_plotting import ManifestPlotter

        task = ManifestPlotter(**common, manifest_path=str(manifest_path), plot_config=settings["config"])
        task.run()
        report = json.loads((destination / "provenance.json").read_text())
        plots = [Path(path) for path in report["plots_created"]]
        if any(path.name.endswith(".stub.json") for path in plots):
            raise RuntimeError("manifest plot stage produced stubs")
        _check_products(plots, "plot")
    elif stage == "datacards":
        from statistical_inference.manifest_fits import ManifestDatacards

        if settings.pop("raw_inputs", False):
            from create_datacards import DatacardGenerator

            with atomic_directory(destination) as staging:
                generator = DatacardGenerator(str(source), output_dir=str(staging), strict_inputs=True)
                generator.run()
                expected = [staging / f"datacard_{region}.txt" for region in generator.control_regions]
                _check_products(expected, "raw datacard")
                for card in expected:
                    _check_products([Path(p) for p in card_shapes(card)], "datacard shapes")
        else:
            task = ManifestDatacards(**common, manifest_path=str(manifest_path),
                                    datacard_config=settings.pop("config"), **settings)
            task.run()
        cards = list(destination.rglob("*.txt"))
        _check_products(cards, "datacard")
        for card in cards:
            for line in card.read_text().splitlines():
                words = line.split()
                if words[:1] == ["shapes"] and len(words) >= 4 and words[3] != "FAKE":
                    _check_products([card.parent / words[3]], "datacard shapes")
    elif stage == "fit":
        from statistical_inference.manifest_fits import ManifestFit

        task = ManifestFit(**common, datacard_dir=str(source), **settings)
        if source.is_file():
            card_shapes(source)
        else:
            inputs = {str(Path(card).resolve()) for card in task._collect_datacards()}
            inputs.update(shape for card in tuple(inputs) for shape in card_shapes(card))
            if inputs - recorded["files"].keys():
                raise ValueError(f"Fit inputs absent from datacard receipt: {sorted(inputs - recorded['files'].keys())}")
        task.run()
        if task.fitting_backend == "rabbit":
            if not task.verify_rabbit_result():
                raise RuntimeError("Rabbit fit did not publish a verified result")
        elif task.fitting_backend == "analysis":
            results = json.loads((destination / "analysis_fit_results.json").read_text())
            if not results or any(result.get("status") != "ok" for result in results):
                raise RuntimeError("analysis fit is incomplete; see analysis_fit_results.json")
        else:
            results = list(destination.glob("combine/*/fit_result.json"))
            _check_products(results, "Combine fit")
            if len(results) != len(task._collect_datacards()):
                raise RuntimeError("Combine did not publish a result for each datacard")
    elif stage == "analytic_fit":
        from statistical_inference.manifest_fits import (
            AnalyticWorkspaceFit, _find_combine, _resolve_manifest_histogram_file,
        )

        manifest = OutputManifest.load_yaml(str(manifest_path))
        if manifest.histograms is None:
            raise ValueError("analytic fit requires a histogram manifest")
        histograms = _resolve_manifest_histogram_file(manifest, str(manifest_path))
        if not settings.get("skip_fit", False):
            _find_combine(settings.get("combine_exe", ""))
        task = AnalyticWorkspaceFit(**common, workspace_config=settings.pop("config"),
                                    histogram_file=histograms, **settings)
        task.run()
        _check_products(list(destination.glob("ws_*.root")), "analytic workspace")
        _check_products(list(destination.glob("datacard*.txt")), "analytic datacard")
        if not task.skip_fit:
            _check_products(list(destination.glob("higgsCombine*.root")), "analytic Combine fit")
    else:
        raise ValueError(f"unknown downstream stage: {stage}")
    _publish_receipt(stage, destination, parent, options, receipt)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for stage in ("refill", "manifests", "merge", *CONSUMER_STAGES):
        command = commands.add_parser(stage)
        command.add_argument("--destination", required=True, type=Path)
        if stage in ("refill", "manifests"):
            command.add_argument("--spec", required=True, type=Path)
            command.add_argument("--index", required=True, type=Path)
            if stage == "refill":
                command.add_argument("--template", required=True, type=Path)
            else:
                command.add_argument("--refill", nargs=2, type=Path)
        elif stage == "merge":
            command.add_argument("--receipt", type=Path)
            sources = command.add_mutually_exclusive_group(required=True)
            sources.add_argument("--production", type=Path)
            sources.add_argument("--supplied", nargs="+", type=Path)
        else:
            command.add_argument("--input", required=True, type=Path)
            command.add_argument("--options", required=True, type=json.loads)
            command.add_argument("--receipt", type=Path)
            command.add_argument("--source-receipt", type=Path)
    args = parser.parse_args(argv)
    destination = args.destination.resolve()
    try:
        if args.command == "refill":
            bind_refill(args.spec, args.index, args.template.resolve(), destination)
        elif args.command == "manifests":
            refill_spec, refill_index = args.refill or (None, None)
            publish_manifests(args.spec, args.index, destination, refill_spec=refill_spec, refill_index=refill_index)
        elif args.command == "merge":
            merge_manifests(destination, production=args.production, supplied=args.supplied or (), receipt=args.receipt)
        else:
            run_consumer(args.command, args.input.resolve(), destination, args.options,
                         receipt=args.receipt, source_receipt=args.source_receipt)
    except (ValueError, RuntimeError, OSError, KeyError, TypeError, ImportError) as exc:
        if args.command in ("fit", "analytic_fit"):
            for log in sorted(destination.rglob("combine*.log")):
                print(f"{log}\n" + "\n".join(log.read_text(errors="replace").splitlines()[-12:]),
                      file=sys.stderr)
        parser.exit(1, f"rdf-production downstream: {exc}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
