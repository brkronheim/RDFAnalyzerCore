"""Compose two production passes using verified stitching inputs and outputs."""

import argparse
import json
from pathlib import Path
import shutil

import yaml

from .campaign import describe_campaign, inspect_campaign, validate_run_ownership
from .controller import _check_build_recipes, _credential_environment, run_directory
from .specs import inspect_spec, load_spec_document, normalize_spec_paths
from job_sizing import file_key
from run_bundle import atomic_directory, find_bundle, record_execution, seal_bundle, verify_bundle
from .output_index import _execution_plan, load_verified_output_index, verify_output_signatures


def stitching_artifacts(output_root, era):
    directory = Path(output_root) / era / "stitch"
    return {"application_spec": directory / "application.json",
            "weights": directory / "stitch_weights.json",
            "cross_sections": directory / "stitch_cross_sections.json",
            "validation": directory / "stitch_validation.json"}


def counter_campaign(settings, campaign, *, base):
    source_config = settings.get("source") if isinstance(settings, dict) else None
    if not isinstance(source_config, dict) or source_config.keys() - {
        "eras", "collection", "select_eras", "production", "era_options"
    }:
        raise ValueError("stitching.source must define a campaign's eras or collection")
    return describe_campaign({**source_config, "output_root": str(campaign["output_root"] / ".stitch-source")},
                             base=base)


def describe_stitching(settings, campaign, application_plan, *, base=Path(".")):
    if not isinstance(settings, dict) or settings.keys() - {"source", "configs", "require_stitched_histograms"}:
        raise ValueError("stitching requires source and configs, with optional require_stitched_histograms")
    source = counter_campaign(settings, campaign, base=base)
    source_plan = inspect_campaign(source)
    eras = application_plan["eras"].keys()
    if source_plan["eras"].keys() != eras:
        raise ValueError("stitching.source and the application campaign must select the same eras")
    configured = settings.get("configs")
    if not isinstance(configured, dict):
        raise ValueError("stitching.configs must map each selected era to a stitching YAML")
    configs = {str(era): path for era, path in configured.items()}
    if (len(configs) != len(configured) or configs.keys() != eras
            or any(not isinstance(path, str) or not path.strip() for path in configs.values())):
        raise ValueError("stitching.configs must map each selected era to a stitching YAML")
    configs = {era: (Path(base) / Path(path).expanduser()).resolve() for era, path in configs.items()}
    required = settings.get("require_stitched_histograms", True)
    if type(required) is not bool:
        raise ValueError("stitching.require_stitched_histograms must be a boolean")
    plans = {}
    for era in eras:
        counter = source_plan["eras"][era]["inspection"]
        application = application_plan["eras"][era]["inspection"]
        configured_source = counter["task_parameters"]
        if configured_source.get("stitch_mode") != "none" or "stitch_weights" in configured_source:
            raise ValueError(f"{era} counter specification requires stitch_mode: none")
        if application["task_parameters"].keys() & {"stitch_mode", "stitch_weights"}:
            raise ValueError(f"{era} application stitching options are supplied by the counter pass")
        document = yaml.safe_load(configs[era].read_text())
        groups = document.get("groups") if isinstance(document, dict) else None
        if (not isinstance(groups, dict) or not groups or
                any(not isinstance(group, dict) or "meta_files" in group for group in groups.values())):
            raise ValueError(f"{configs[era]} must declare stitch groups using samples or dataset selectors")
        plans[f"{era} counters"] = counter
        plans[f"{era} application"] = application
    validate_run_ownership(plans)
    _check_build_recipes(plans.values())
    return {"source": source, "source_plan": source_plan, "configs": configs,
            "require_stitched_histograms": required}


def _bind_successful_files(document, index, destination):
    datasets = index["datasets"]
    if not any(group.get("failures") for group in datasets.values()):
        return
    options = document["options"]
    if options.get("file_source") != "manifest" or options.get("partition") != "file_group":
        raise ValueError("partial stitching requires explicit whole-file manifest partitioning")

    manifest = Path(document["dataset_manifest"])
    bundle = find_bundle(manifest)
    sealed = verify_bundle(bundle)
    manifest_member = manifest.relative_to(bundle)
    payload = yaml.safe_load(manifest.read_text())
    if {sample["name"] for sample in payload["datasets"]} != datasets.keys():
        raise ValueError("application and counter manifests differ in dataset membership")
    for sample in payload["datasets"]:
        group = datasets[sample["name"]]
        original = {file for job in group["originals"].values() for file in job["partition"]["files"]}
        files = sample["files"]
        if set(map(file_key, files)) != original:
            raise ValueError(f"application input inventory differs for {sample['name']}")
        jobs = group["jobs"].values()
        failures = group.get("failures", {}).values()
        if any(job["partition"]["range"] is not None for job in (*jobs, *failures)):
            raise ValueError("partial stitching cannot carry an entry-range split into a whole-file application")
        accepted = {file for job in jobs for file in job["partition"]["files"]}
        sample["files"] = [file for file in files if file_key(file) in accepted]

    filtered = Path(destination).resolve().parent / f"counter-inputs-{index['index_id']}"
    metadata = {**sealed["metadata"], "counter_index_id": index["index_id"],
                "parent_bundle_id": sealed["bundle_id"]}
    filtered_manifest = yaml.safe_dump(payload, sort_keys=False)
    if not filtered.exists():
        with atomic_directory(filtered) as staging:
            for relative in sealed["files"]:
                if Path(relative) == manifest_member:
                    continue
                path = staging / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(bundle / relative, path)
            (staging / manifest_member).write_text(filtered_manifest)
            seal_bundle(staging, metadata=metadata, assets=sealed["assets"], entrypoints=sealed["entrypoints"])
    if (verify_bundle(filtered)["metadata"] != metadata
            or (filtered / manifest_member).read_text() != filtered_manifest):
        raise ValueError(f"existing filtered application bundle differs: {filtered}")
    document["dataset_manifest"] = str(filtered / manifest_member)
    document["submit_config"] = str(filtered / Path(document["submit_config"]).relative_to(bundle))


def bind_application(spec, weights, destination, *, counter_index=None):
    """Create the effective application spec once the correction file exists."""
    spec = Path(spec).resolve()
    weights = Path(weights).resolve(strict=True)
    if not weights.is_file():
        raise ValueError(f"Expected a stitching correction file: {weights}")
    document = normalize_spec_paths(load_spec_document(spec), spec.parent)
    options = document.get("options", {})
    if options.keys() & {"stitch_mode", "stitch_weights"}:
        raise ValueError("application specification already sets stitching options")
    document["options"] = {**options, "stitch_mode": "correctionlib", "stitch_weights": str(weights)}
    if counter_index is not None:
        _bind_successful_files(document, load_verified_output_index(counter_index), destination)
    record_execution(destination, document)


def _verified_metadata(spec, index_path):
    from rdfproduction.jobs.output_validation import _output_file_info_batch
    from .stitching_derivation import _stitch_meta_read_path

    parameters = spec["task_parameters"]
    run = run_directory(parameters["name"], workspace=spec.get("state_root"))
    claim = json.loads((run / "rdf_production.json").read_text())
    if claim != spec:
        raise ValueError(f"Production spec changed after the run was claimed: {run}")
    _, identity, plan = _execution_plan(run)
    index = load_verified_output_index(index_path)
    if (index["execution_id"], index["plan_id"], index["bundle_id"]) != (
        identity["execution_id"], plan["execution_id"], plan["bundle_id"]
    ):
        raise ValueError(f"Verified output index does not belong to {run}")
    verify_output_signatures(index, _output_file_info_batch)
    return {name: [_stitch_meta_read_path(job["outputs"]["meta"])
                   for job in dataset["jobs"].values()]
            for name, dataset in index["datasets"].items()}


def derive(spec, config, index, destination):
    from .stitching_derivation import StitchingDerivation
    plan = inspect_spec(spec)
    parameters = plan["task_parameters"]
    task = StitchingDerivation(name=parameters["name"], stitch_config=str(config),
                               source_dataset_manifest=parameters["dataset_manifest"])
    with _credential_environment(parameters.get("x509")):
        metadata = _verified_metadata(plan, index)
        task.derive(verified_meta_files=metadata, output_dir=destination)


def validate(spec, config, index, weights, cross_sections, destination, *, require_stitched_histograms=True):
    from .stitching_validation import StitchingValidation
    plan = inspect_spec(spec)
    parameters = plan["task_parameters"]
    if (parameters.get("stitch_mode") != "correctionlib"
            or parameters.get("stitch_weights") != str(Path(weights).resolve())):
        raise ValueError("validation requires the correction bound to the application spec")
    task = StitchingValidation(
        name=parameters["name"], dataset_manifest=parameters["dataset_manifest"],
        submit_config=parameters["submit_config"], stitch_config=str(config),
        stitch_weights=str(weights), stitch_cross_sections=str(cross_sections),
        require_stitched_histograms=require_stitched_histograms,
    )
    with _credential_environment(parameters.get("x509")):
        metadata = _verified_metadata(plan, index)
        task.validate_outputs(metadata, output_dir=destination)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    binding = commands.add_parser("bind", help="materialize the application spec")
    binding.add_argument("spec", type=Path)
    binding.add_argument("weights", type=Path)
    binding.add_argument("destination", type=Path)
    binding.add_argument("--counter-index", type=Path)
    for action in ("derive", "validate"):
        command = commands.add_parser(action)
        command.add_argument("spec", type=Path)
        command.add_argument("config", type=Path)
        command.add_argument("index", type=Path)
        command.add_argument("destination", type=Path)
        if action == "validate":
            command.add_argument("--weights", type=Path, required=True)
            command.add_argument("--cross-sections", type=Path, required=True)
            command.add_argument("--no-stitched-histograms", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "bind":
            bind_application(args.spec, args.weights, args.destination, counter_index=args.counter_index)
        elif args.command == "derive":
            derive(args.spec, args.config, args.index, args.destination)
        else:
            validate(args.spec, args.config, args.index, args.weights, args.cross_sections, args.destination,
                     require_stitched_histograms=not args.no_stitched_histograms)
    except (ValueError, RuntimeError, OSError, ImportError, KeyError, TypeError) as exc:
        parser.exit(1, f"rdf-production stitching: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
