#!/usr/bin/env python3
"""Run a prepared binned model with Combine or native Rabbit CPU/CUDA."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

from statistical_inference import FitError
from statistical_inference.combine import run_combine
from statistical_inference.rabbit import add_rabbit_options, options_from_args, run_rabbit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datacard", type=Path, nargs="+", required=True,
                        help="one simultaneous card, or unique-channel cards to fit jointly in Rabbit")
    parser.add_argument("--poi-map", type=Path, help="optional structured analysis parameter model")
    parser.add_argument("--backend", choices=("combine", "rabbit"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dataset", choices=("observed", "asimov"), default="asimov")
    parser.add_argument("--combine-exe")
    parser.add_argument("--freeze-parameters", nargs="*", default=[])
    parser.add_argument("--scan", nargs="*", default=[])
    parser.add_argument("--impacts", action="store_true")
    add_rabbit_options(parser)
    args = parser.parse_args(argv)
    try:
        info = {"datacard": str(args.datacard[0].resolve()),
                "datacards": [str(p.resolve()) for p in args.datacard]}
        map_path = args.poi_map or args.datacard[0].parent / "poi_map.json"
        if map_path.is_file():
            info["poi_map"] = str(map_path.resolve())
        args.output_dir.mkdir(parents=True, exist_ok=True)
        if args.backend == "rabbit":
            fit = run_rabbit(info, args.output_dir, args.dataset == "asimov",
                             freeze_parameters=args.freeze_parameters, scan=args.scan,
                             impacts=args.impacts, **options_from_args(args))
        else:
            if len(args.datacard) != 1 or args.freeze_parameters or args.scan or args.impacts:
                raise FitError("central Combine fit currently requires one simultaneous card; use Combine options for scans/impacts")
            from statistical_inference.model import read_model
            model = read_model(args.datacard, poi_map=info.get("poi_map"))
            info.update(active_pois=[p["parameter"] for p in model["parameters"] if p["is_poi"]],
                        floating_parameters=[p["parameter"] for p in model["parameters"] if not p["constant"]])
            if not info.get("poi_map") and [p["parameter"] for p in model["parameters"]] == ["r"]:
                # Standard cards use Combine's common signal strength r.
                from statistical_inference.combine import run_standard_combine
                fit = run_standard_combine(info["datacard"], args.output_dir / "combine",
                                            asimov=args.dataset == "asimov", executable=args.combine_exe)
            else:
                metadata = model["metadata"]
                info.update(fit_model=metadata.get("fit_model", "binned"), systematics=metadata.get("systematics", {}))
                fit = run_combine(info, args.output_dir, args.dataset == "asimov", args.combine_exe,
                                  freeze_theory_nuisances=False)
        result = dict(schema="rdf-fit-summary-v1", backend=args.backend,
                      status=fit["status"], mode=args.dataset, fit=fit, **{args.backend: fit})
        output = args.output_dir / f"{args.backend}_fit_result.json"
        output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        print(output)
        return 0
    except (FitError, OSError, ValueError) as exc:
        print(f"Fit failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
