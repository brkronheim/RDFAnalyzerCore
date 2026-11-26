"""Inspect and run a sealed RDFAnalyzerCore production stage from a JSON spec."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .controller import _output_signatures, preflight, run_directory, run_stage, status
from .specs import inspect_spec


def main(argv=None):
    parser = argparse.ArgumentParser(prog="rdfproduction", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="resolve a spec and verify its local inputs")
    inspect.add_argument("spec", type=Path)
    inspect.add_argument("--format", choices=("text", "json"), default="text")
    runtime = commands.add_parser("preflight", help="build and validate the runtime without discovery or submission")
    runtime.add_argument("spec", type=Path, nargs="+")
    prepare = commands.add_parser("prepare", help="discover inputs and stage a new batch")
    prepare.add_argument("spec", type=Path)
    smoke = commands.add_parser("smoke", help="run the staged analysis against representative inputs locally")
    smoke.add_argument("spec", type=Path)
    submit = commands.add_parser("submit", help="prepare and submit a batch without waiting for output")
    submit.add_argument("spec", type=Path)
    execute = commands.add_parser("execute", help="run or resume production and publish a verified output index")
    execute.add_argument("spec", type=Path)
    execute.add_argument("--output-index", type=Path, required=True)
    execute.add_argument("--local-workers", type=int, help="worker limit for local execution or a local Dask cluster")
    execute.add_argument("--retry-limit", type=int, help="raise the retry budget without changing the sealed spec")
    execute.add_argument("--allow-partial", action="store_true",
                         help="accept exhausted Condor partitions if every dataset has verified output")
    verify = commands.add_parser("verify", help="check artifact receipts, optionally querying storage")
    verify.add_argument("--output-index", type=Path, required=True)
    verify.add_argument("--merge-receipt", type=Path)
    verify.add_argument("--storage", action="store_true")
    recorded = commands.add_parser("status", help="show recorded state for an RDF production run")
    recorded.add_argument("run_dir", type=Path)
    merge = commands.add_parser("merge", help="merge a complete verified output index")
    merge.add_argument("--output-index", type=Path, required=True)
    merge.add_argument("--destination", type=Path, required=True)
    merge.add_argument("--receipt", type=Path, required=True)
    merge_mode = merge.add_mutually_exclusive_group()
    merge_mode.add_argument("--sparse-only", action="store_true")
    merge_mode.add_argument("--nominal-only", action="store_true")
    merge.add_argument("--metadata-merger", type=Path)
    merge.add_argument("--data-merge-threads", type=int, default=1)
    merge.add_argument("--hadd-jobs", type=int, default=4)
    merge.add_argument("--xrdcp-jobs", type=int, default=4)
    merge.add_argument("--semantic-config-compatibility", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "inspect":
            plan = inspect_spec(args.spec)
            if args.format == "json":
                print(json.dumps(plan, indent=2, sort_keys=True))
            else:
                name = plan["task_parameters"]["name"]
                print(f"{name}  {plan['spec_id']}\n"
                      f"Bundle {plan['bundle_id']}\n"
                      f"Run directory {run_directory(name, workspace=plan.get('state_root'))}\n"
                      f"Backend {plan.get('backend', 'htcondor')}\n"
                      "Prepare → execute or resume → verify → publish output index")
        elif args.command == "status":
            print(json.dumps(status(args.run_dir), indent=2, sort_keys=True))
        elif args.command == "preflight":
            results = preflight(*args.spec)
            print(json.dumps(results[0] if len(results) == 1 else results, sort_keys=True))
        elif args.command == "verify":
            from .artifacts import verify_outputs
            print(json.dumps(verify_outputs(
                args.output_index, args.merge_receipt,
                check_signatures=_output_signatures if args.storage else None,
            ), indent=2, sort_keys=True))
        elif args.command == "merge":
            from partial_merge import merge_verified_outputs
            merge_verified_outputs(
                args.output_index, args.destination, check_signatures=_output_signatures, receipt=args.receipt,
                sparse_only=args.sparse_only, metadata_merger=args.metadata_merger,
                nominal_only=args.nominal_only, semantic_config_compatibility=args.semantic_config_compatibility,
                data_merge_threads=args.data_merge_threads, hadd_jobs=args.hadd_jobs,
                xrdcp_jobs=args.xrdcp_jobs,
            )
            print(args.receipt)
        else:
            print(run_stage(args.spec, output_index=getattr(args, "output_index", None),
                            submit_only=args.command == "submit", smoke_only=args.command == "smoke",
                            local_workers=getattr(args, "local_workers", None),
                            retry_limit=getattr(args, "retry_limit", None),
                            allow_partial=getattr(args, "allow_partial", False)))
    except (ValueError, RuntimeError, OSError, ImportError, KeyError, TypeError) as exc:
        parser.exit(1, f"rdf-production: {exc}\n")
    return 0
