"""Plan and preflight a multi-era production campaign used by the Snakefile."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

from .specs import load_spec_document
from run_bundle import (
    content_digest, load_artifact_receipt, record_execution, verify_artifact, verify_bundle,
    verify_collection, write_json,
)
from run_collection import isolated_name, isolated_output


_RUN_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")


def execution_slots(plan):
    """Controller slots and local partition workers requested from Snakemake."""
    backend = plan.get("backend")
    if backend == "local":
        return plan["workers"]
    parameters = plan["task_parameters"]
    if backend == "dask" and not parameters.get("dask_scheduler"):
        return parameters.get("dask_workers", 1) * parameters.get("dask_threads_per_worker", 1)
    return 1


def era_artifacts(output_root, era):
    root = Path(output_root) / era
    return {
        "prepared": root / "prepared.json",
        "smoke": root / "smoke.json",
        "submitted": root / "submitted.json",
        "output_index": root / "output_index.json",
        "merge_receipt": root / "merged" / "merge_receipt.json",
        "plot_report": root / "plots" / "report.yaml",
    }


def _mapping(value, label):
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return value


def _path(value, base, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty path")
    return (base / Path(value).expanduser()).resolve()


def _positive(config, name, default):
    value = config.get(name, default)
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _eras(records):
    entries = {str(era): value for era, value in _mapping(records, "eras").items()}
    if len(entries) != len(records) or not entries:
        raise ValueError("eras must be distinct and nonempty")
    for era in entries:
        isolated_name("campaign", era)
    return entries


def _collection_specs(config, base):
    from .specs import normalize_spec_paths

    root = _path(config["collection"], base, "collection")
    verified = verify_collection(root)
    children = _eras(verified["children"])
    selected = config.get("select_eras", list(children))
    if not isinstance(selected, list) or not selected:
        raise ValueError("select_eras must be a nonempty list")
    eras = [str(era) for era in selected]
    if len(eras) != len(set(eras)) or set(eras) - children.keys():
        raise ValueError("select_eras must identify distinct members of the collection")
    template = _mapping(config.get("production"), "production")
    if template.keys() & {"submit_config", "dataset_manifest"}:
        raise ValueError("collection entrypoints supply submit_config and dataset_manifest")
    prefix = template.get("name")
    if not isinstance(prefix, str) or not _RUN_NAME.fullmatch(prefix):
        raise ValueError("production.name must be a valid run name prefix")
    options = _mapping(template.get("options", {}), "production.options")
    raw_overrides = _mapping(config.get("era_options", {}), "era_options")
    era_options = {str(era): _mapping(value, f"era_options.{era}") for era, value in raw_overrides.items()}
    if len(era_options) != len(raw_overrides) or era_options.keys() - children.keys():
        raise ValueError("era_options must identify distinct members of the collection")

    specs = {}
    inputs = [root / "collection_manifest.json"]
    for era in eras:
        child = (root / children[era]["bundle"]).resolve()
        record = verify_bundle(child)
        if "era" in record["metadata"] and str(record["metadata"]["era"]) != era:
            raise ValueError(f"collection child {era} has mismatched bundle era")
        entrypoints = record["entrypoints"]
        if not {"submit_config", "datasets"} <= entrypoints.keys():
            raise ValueError(f"collection child {era} needs submit_config and datasets entrypoints")
        spec = dict(template)
        spec.setdefault("version", 1)
        spec.update(
            name=isolated_name(prefix, era),
            submit_config=str(child / entrypoints["submit_config"]),
            dataset_manifest=str(child / entrypoints["datasets"]),
        )
        overrides = era_options.get(era, {})
        child_options = {**options, **overrides}
        destination = child_options.get("output_directory")
        if spec.get("backend", "htcondor") == "htcondor":
            if not isinstance(destination, str) or not destination.strip():
                raise ValueError(f"{era} needs an output_directory in production.options or era_options.{era}")
            if "output_directory" not in overrides:
                child_options["output_directory"] = isolated_output(destination, era)
        spec["options"] = child_options
        specs[era] = normalize_spec_paths(spec, base)
        inputs.append(child / "bundle_manifest.json")
    return specs, inputs, verified["collection_id"]


def describe_campaign(config, *, base=Path(".")):
    """Describe rule inputs and targets without creating files or running commands."""
    config = _mapping(config, "campaign")
    allowed = {
        "eras", "collection", "production", "select_eras", "era_options", "output_root",
        "max_parallel_eras", "data_merge_threads", "hadd_jobs", "xrdcp_jobs", "python",
        "metadata_merger", "nominal_only", "semantic_config_compatibility", "verify_storage",
        "condor_retry_limit", "condor_allow_partial", "plotting", "stitching", "downstream", "joint_fit",
    }
    unknown = config.keys() - allowed
    if unknown:
        raise ValueError("Unknown campaign settings: " + ", ".join(sorted(map(str, unknown))))
    for key in ("nominal_only", "verify_storage", "condor_allow_partial"):
        if key in config and type(config[key]) is not bool:
            raise ValueError(f"{key} must be a boolean")
    base = Path(base).resolve()
    output_root = _path(config.get("output_root", "results"), base, "output_root")
    if "collection" in config:
        if "eras" in config:
            raise ValueError("select a collection or supply explicit eras, not both")
        specs, inputs, collection_id = _collection_specs(config, base)
        identity = content_digest({"collection_id": collection_id, "specifications": specs})
        generated = {
            era: output_root / ".campaign" / "specs" / content_digest(spec) / f"{era}.json"
            for era, spec in specs.items()
        }
        era_specs = generated.copy()
    else:
        if config.keys() & {"production", "select_eras", "era_options"}:
            raise ValueError("production, select_eras and era_options require a sealed collection")
        specs, generated, collection_id = {}, {}, None
        era_specs = {
            era: _path(path, base, f"eras.{era}") for era, path in _eras(config.get("eras")).items()
        }
        inputs = list(era_specs.values())
        identity = content_digest({era: str(path) for era, path in era_specs.items()})
    return {
        "output_root": output_root,
        "era_specs": era_specs,
        "generated": generated,
        "generated_documents": specs,
        "inputs": inputs,
        "collection_id": collection_id,
        "plan_path": output_root / ".campaign" / f"plan-{identity}.json",
        "preflight_path": output_root / ".campaign" / f"ready-{identity}.json",
        "max_parallel_eras": _positive(config, "max_parallel_eras", 2),
        "data_merge_threads": _positive(config, "data_merge_threads", 1),
        "hadd_jobs": _positive(config, "hadd_jobs", 4),
        "xrdcp_jobs": _positive(config, "xrdcp_jobs", 4),
    }


def _output_destination(settings):
    from submission_backend import read_config

    path = settings.get("output_directory")
    if not path:
        path = read_config(settings["submit_config"]).get("saveDirectory", "")
    if not path:
        return None
    if path.startswith("/eos/"):
        path = "root://eosuser.cern.ch/" + path
    url = urlsplit(path)
    if url.scheme != "root" or not url.hostname or not url.path.lstrip("/"):
        raise ValueError(f"{settings['name']} needs an EOS or XRootD output directory")
    directory = PurePosixPath("/" + url.path.lstrip("/"))
    if ".." in directory.parts or url.query or url.fragment:
        raise ValueError(f"invalid output directory for {settings['name']}: {path}")
    return url.hostname.lower(), url.port or 1094, directory


def validate_run_ownership(plans):
    """Reject shared run names and overlapping Condor output directories."""
    condor_count = sum(plan.get("backend", "htcondor") == "htcondor" for plan in plans.values())
    names, destinations = {}, {}
    for label, plan in plans.items():
        parameters = plan["task_parameters"]
        name = parameters["name"]
        if name in names:
            raise ValueError(f"{label} and {names[name]} use duplicate run name {name!r}")
        names[name] = label
        if plan.get("backend", "htcondor") != "htcondor":
            continue
        destination = _output_destination(parameters)
        if destination is None:
            if condor_count > 1:
                raise ValueError(f"{label} must set a remote output directory for concurrent production")
            continue
        endpoint, port, directory = destination
        for (other_host, other_port, other_directory), previous in destinations.items():
            if (endpoint, port) == (other_host, other_port) and (
                directory.is_relative_to(other_directory) or other_directory.is_relative_to(directory)
            ):
                raise ValueError(f"{label} and {previous} use overlapping output destinations")
        destinations[destination] = label


def inspect_campaign(campaign):
    """Inspect every selected specification and reject ambiguous run ownership."""
    from .specs import inspect_spec

    records = {}
    with tempfile.TemporaryDirectory() as directory:
        for era, path in campaign["era_specs"].items():
            if era in campaign["generated_documents"]:
                path = Path(directory) / f"{era}.json"
                path.write_text(json.dumps(campaign["generated_documents"][era]))
            try:
                record = inspect_spec(path)
            except (ValueError, OSError, KeyError) as exc:
                raise ValueError(f"{era}: {exc}") from exc
            records[era] = {"specification": str(campaign["era_specs"][era]), "inspection": record}
    validate_run_ownership({era: record["inspection"] for era, record in records.items()})
    plan = {"schema": "rdf-production-campaign-v1", "collection_id": campaign["collection_id"],
            "eras": records}
    return {**plan, "plan_id": content_digest(plan)}


def publish_plan(campaign):
    plan = inspect_campaign(campaign)
    for era, path in campaign["generated"].items():
        record_execution(path, campaign["generated_documents"][era])
    write_json(campaign["plan_path"], plan)


def _artifact_record(path):
    return {"path": str(path), "present": path.is_file()}


def _phase_status(*, products, log=None, check=None, verify=False):
    records = [_artifact_record(path) for path in products]
    result = {"products": records}
    if log is not None:
        result["log"] = _artifact_record(log)
    if all(record["present"] for record in records):
        result["state"] = "recorded"
        if check is not None:
            try:
                result.update(check(), state="verified" if verify else "recorded")
            except (ValueError, OSError, KeyError, TypeError) as exc:
                result.update(state="invalid", error=str(exc))
    else:
        started = any(record["present"] for record in records) or (log is not None and log.is_file())
        result["state"] = "incomplete" if started else "pending"
    return result


def _derived_status(path, kind, log=None, *, verify):
    kinds = (kind,) if isinstance(kind, str) else kind

    def check():
        artifact = (verify_artifact if verify else load_artifact_receipt)(path)
        if artifact.get("kind") not in kinds:
            raise ValueError(f"{path} does not describe a {' or '.join(kinds)} artifact")
        return {"artifact_id": artifact["artifact_id"], "parents": artifact["parents"],
                "product_count": len(artifact["files"]), "kind": artifact["kind"]}

    return _phase_status(products=[path], log=log, check=check, verify=verify)


def _index_status(path, log, *, verify):
    from .artifacts import verify_outputs

    # Both modes read the index identity. The storage fingerprints are checked
    # separately by the explicit rdf_production verify --storage command.
    return _phase_status(products=[path], log=log, check=lambda: verify_outputs(path), verify=verify)


def _downstream_status(root, era, settings, *, base, verify):
    from .downstream import CONSUMER_STAGES, downstream_paths, needs_manifest_merge

    products = downstream_paths(root, era)
    logs = root / era / "logs"
    result = {}
    if "refill" in settings:
        result["refill_binding"] = _phase_status(products=[products["refill_binding"] / "spec.json"],
                                                 log=logs / "refill-bind.log")
        result["refill"] = _index_status(products["refill_index"], logs / "refill.log", verify=verify)
    if needs_manifest_merge(settings):
        result["manifest_merge"] = _derived_status(products["manifest_merge_receipt"], "manifest_merge",
                                                    logs / "manifest-merge.log", verify=verify)
    for kind in CONSUMER_STAGES:
        if kind in settings:
            result[kind] = _derived_status(products[f"{kind}_receipt"], kind,
                                          logs / f"downstream-{kind}.log", verify=verify)
    for source, kind in (("saved_datacards", "datacards"), ("saved_fit", ("fit", "analytic_fit"))):
        if source in settings:
            details = _mapping(settings[source], f"{era}.{source}")
            result[source] = _derived_status(_path(details.get("receipt"), base, f"{era}.{source}.receipt"),
                                             kind, verify=verify)
    return result


def campaign_status(campaign, *, settings=None, base=Path("."), verify=False):
    """Read local phase and batch receipts for each selected era."""
    from .artifacts import verify_outputs
    from .controller import run_directory, status

    settings = settings or {}
    downstream = _mapping(settings.get("downstream", {}), "downstream")
    downstream = {str(era): _mapping(value, f"downstream.{era}") for era, value in downstream.items()}
    if campaign["era_specs"].keys() and downstream.keys() - campaign["era_specs"].keys():
        raise ValueError("downstream must identify selected eras")
    output_root = campaign["output_root"]
    eras = {}
    for era in dict.fromkeys((*campaign["era_specs"], *downstream)):
        directory = output_root / era
        spec = campaign["era_specs"].get(era)
        if spec is None:
            isolated_name("downstream", era)
            eras[era] = {"stages": _downstream_status(output_root, era, downstream[era], base=base, verify=verify)}
            continue
        paths = era_artifacts(output_root, era)
        logs = directory / "logs"
        record = {
            "specification": _artifact_record(spec),
            "artifacts": {name: _artifact_record(path) for name, path in paths.items()},
            "logs": {name: _artifact_record(logs / f"{name}.log")
                     for name in ("prepare", "smoke", "submit", "production", "merge", "plots")},
            "stages": {
                "prepare": _phase_status(products=[paths["prepared"]], log=logs / "prepare.log"),
                "smoke": _phase_status(products=[paths["smoke"]], log=logs / "smoke.log"),
                "submit": _phase_status(products=[paths["submitted"]], log=logs / "submit.log"),
                "production": _index_status(paths["output_index"], logs / "production.log", verify=verify),
                "merge": _phase_status(
                    products=[paths["merge_receipt"]], log=logs / "merge.log", verify=verify,
                    check=lambda: verify_outputs(paths["output_index"], paths["merge_receipt"])
                    if verify else {},
                ),
            },
        }
        if era in downstream:
            record["stages"].update(_downstream_status(output_root, era, downstream[era], base=base, verify=verify))
        # The optional native renderer has a separate report from manifest plots.
        if era in map(str, (settings.get("plotting") or {}).get("configs", {})):
            record["stages"]["native_plots"] = _phase_status(products=[paths["plot_report"]],
                                                              log=logs / "plots.log")
        eras[era] = record
        try:
            specification = campaign["generated_documents"].get(era)
            if specification is None:
                specification = load_spec_document(spec)
            name = specification.get("name")
            if not isinstance(name, str) or not _RUN_NAME.fullmatch(name):
                raise ValueError(f"invalid run name in {spec}")
            state_root = specification.get("state_root")
            if state_root is not None:
                state_root = _path(state_root, spec.parent, "state_root")
            run_dir = run_directory(name, workspace=state_root)
            record.update(name=name, run_dir=str(run_dir))
        except (ValueError, OSError) as exc:
            record.update(state="unreadable", error=str(exc))
            continue
        try:
            record.update(state="recorded", run=status(run_dir))
            production = record["stages"]["production"]
            failed = record["run"]["recorded_partition_statuses"].get("perm_fail", 0)
            if failed and production["state"] in ("pending", "incomplete"):
                production.update(state="failed", error=f"{failed} partitions recorded as permanently failed")
        except FileNotFoundError:
            record["state"] = "unclaimed" if run_dir.is_dir() else "missing"
        except (ValueError, KeyError, OSError) as exc:
            record.update(state="unreadable", error=str(exc))
    result = {"schema": "rdf-production-campaign-status-v1", "collection_id": campaign["collection_id"],
              "eras": eras, "verification": "local_artifact_hashes" if verify else "recorded"}
    if campaign["era_specs"]:
        result.update(plan=_artifact_record(campaign["plan_path"]),
                      preflight=_artifact_record(campaign["preflight_path"]))
    if "stitching" in settings and campaign["era_specs"]:
        from .stitching import counter_campaign, stitching_artifacts

        result["counter_pass"] = campaign_status(
            counter_campaign(settings["stitching"], campaign, base=base), verify=verify)
        for era, record in eras.items():
            paths = stitching_artifacts(output_root, era)
            logs = output_root / era / "logs"
            record["artifacts"].update({name: _artifact_record(path) for name, path in paths.items()})
            record["stages"].update(
                stitch_weights=_phase_status(products=[paths["weights"], paths["cross_sections"]],
                                             log=logs / "stitch-derive.log"),
                stitch_application=_phase_status(products=[paths["application_spec"]]),
                stitch_validation=_phase_status(products=[paths["validation"]], log=logs / "stitch-validate.log"),
            )
    if "joint_fit" in settings:
        from .joint_fit import joint_paths

        paths = joint_paths(output_root)
        joint_settings = _mapping(settings["joint_fit"], "joint_fit")
        stages = ("datacards", "fit", "fit_diagnostics") if "fit_diagnostics" in joint_settings else (
            "datacards", "fit")
        result["joint_fit"] = {
            stage: _derived_status(paths[f"{stage}.json"], stage,
                                   paths["logs"] / f"{stage}.log", verify=verify)
            for stage in stages
        }
    return result


def format_campaign_status(status):
    """Summarize stage states and point to logs when a stage needs attention."""
    lines = []

    def append(label, stages):
        lines.append(f"{label}  " + "  ".join(f"{stage}={details['state']}" for stage, details in stages.items()))
        for stage, details in stages.items():
            if details["state"] not in {"failed", "invalid", "incomplete"}:
                continue
            pieces = [details["error"]] if "error" in details else []
            if details.get("log", {}).get("present"):
                pieces.append(details["log"]["path"])
            if pieces:
                lines.append(f"  {stage}  " + "  ".join(pieces))

    for era, record in status["eras"].items():
        append(era, record["stages"])
    for era, record in status.get("counter_pass", {}).get("eras", {}).items():
        append(f"{era} counters", record["stages"])
    if "joint_fit" in status:
        append("joint fit", status["joint_fit"])
    return "\n".join(lines)


def preflight_campaign(campaign, *, python, log, specifications=None):
    plan = inspect_campaign(campaign)
    if json.loads(campaign["plan_path"].read_text()) != plan:
        raise ValueError("campaign changed after inspection; refresh the plan before submission")
    environment = os.environ.copy()
    from . import PYTHON_ROOT

    paths = [str(PYTHON_ROOT)]
    if environment.get("PYTHONPATH"):
        paths.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(paths)
    log = Path(log)
    log.parent.mkdir(parents=True, exist_ok=True)
    specs = specifications if specifications is not None else [
        record["specification"] for record in plan["eras"].values()
    ]
    with log.open("w") as stream:
        print(f"Validating runtimes for {', '.join(plan['eras'])}", file=stream, flush=True)
        subprocess.run([python, "-m", "rdf_production", "preflight", *specs],
                       check=True, env=environment, stdout=stream, stderr=subprocess.STDOUT)
    write_json(campaign["preflight_path"], {"schema": "rdf-production-campaign-ready-v1",
                                          "plan_id": plan["plan_id"], "eras": list(plan["eras"])})


def record_stage(spec, receipt, *, stage):
    from .controller import run_stage
    from run_bundle import file_digest

    artifact = run_stage(spec, submit_only=stage == "submit", smoke_only=stage == "smoke")
    write_json(receipt, {"schema": "rdf-production-stage-receipt-v1",
                         "stage": stage,
                         "artifact": str(artifact), "sha256": file_digest(artifact)})


def main(argv=None):
    import yaml

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for action, description in (("inspect", "show a campaign plan without staging or submitting"),
                                ("status", "show recorded artifacts and batch state for selected eras")):
        campaign_parser = commands.add_parser(action, help=description)
        campaign_parser.add_argument("config", type=Path)
        campaign_parser.add_argument("--workdir", type=Path, default=Path.cwd())
        if action == "status":
            campaign_parser.add_argument("--verify", action="store_true",
                                         help="also verify local merge and downstream product hashes")
            campaign_parser.add_argument("--format", choices=("json", "text"), default="json")
    for action, description in (("prepare", "stage one era and write its receipt"),
                                ("smoke", "test one era locally and write its receipt"),
                                ("submit", "submit one era and write its receipt")):
        stage_parser = commands.add_parser(action, help=description)
        stage_parser.add_argument("spec", type=Path)
        stage_parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command in ("inspect", "status"):
            settings = _mapping(yaml.safe_load(args.config.read_text()), "campaign")
            if (args.command == "status" and settings.keys() & {"downstream", "joint_fit"}
                    and not settings.keys() & {"eras", "collection"}):
                campaign = {"output_root": _path(settings.get("output_root", "results"), args.workdir, "output_root"),
                            "era_specs": {}, "collection_id": None}
            else:
                campaign = describe_campaign(settings, base=args.workdir)
            if args.command == "inspect":
                result = inspect_campaign(campaign)
                if "stitching" in settings:
                    from .stitching import describe_stitching
                    stitch = describe_stitching(settings["stitching"], campaign, result, base=args.workdir)
                    result = {"application": result, "counter_pass": stitch["source_plan"],
                              "stitch_configs": {era: str(path) for era, path in stitch["configs"].items()}}
            else:
                result = campaign_status(campaign, settings=settings, base=args.workdir, verify=args.verify)
            if args.command == "status" and args.format == "text":
                print(format_campaign_status(result))
            else:
                print(json.dumps(result, indent=2, sort_keys=True))
        else:
            record_stage(args.spec, args.receipt, stage=args.command)
    except (ValueError, RuntimeError, OSError) as exc:
        parser.exit(1, f"rdf-production campaign: {exc}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
