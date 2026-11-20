"""Recover native robustHesse nodes for an independent covariance diagnostic.

Requires a verbose log with both bisection traces for every retained parameter.
Printed nodes have the precision of that log. No covariance values are read.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

from .errors import FitError
from .parameters import free_parameters


def extract_stencils(log, model):
    searches, stencils = [], {}
    trace = re.compile(r"\[Bisect\] (\d+) cmin = ([^,]+), cmax = ([^,]+), trial = (\S+) dNll =")
    row = re.compile(r"^(\S+)\s+([-+0-9.eE]+)\s+\|")
    for line in log.splitlines():
        match = trace.search(line)
        if match:
            step, centre, _, value = match.groups()
            if step == "0":
                searches.append([float(centre), float(value)])
            elif searches:
                searches[-1][1] = float(value)
            continue
        match = row.match(line)
        if not match:
            continue
        name = match.group(1)
        if len(searches) != 2 or name in stencils:
            raise FitError(f"Cannot recover both unique robustHesse stencils for {name}")
        nodes = [searches[0][1]-searches[0][0], 0., searches[1][1]-searches[1][0]]
        if not nodes[0] < 0 < nodes[2]:
            raise FitError(f"robustHesse stencil for {name} is not an interior stencil")
        stencils[name] = nodes
        searches = []
    required = {p["parameter"] for p in free_parameters(model)} | {e["name"] for e in model["effects"]}
    missing = required-set(stencils)
    if missing:
        raise FitError(f"Native stencil log lacks parameters: {sorted(missing)}")
    return dict(schema="rdf-covariance-validation-stencils-v1", model_identity=model["identity"],
                source_sha256=hashlib.sha256(log.encode()).hexdigest(),
                precision="Combine verbose log printed precision", stencils=stencils)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model", type=Path)
    parser.add_argument("log", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    record = extract_stencils(args.log.read_text(), json.loads(args.model.read_text()))
    record["source"] = str(args.log.resolve())
    args.output.write_text(json.dumps(record, indent=2)+"\n")


if __name__ == "__main__":
    main()
