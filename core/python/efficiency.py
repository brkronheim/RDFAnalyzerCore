"""Efficiency sufficient statistics, aligned rebinning and correctionlib output."""
import json
from pathlib import Path
import numpy as np


def efficiency(passed, failed, passed_variance, failed_variance):
    p, f, vp, vf = np.broadcast_arrays(*map(np.asarray,
        (passed, failed, passed_variance, failed_variance)))
    total = p + f
    if (not all(np.all(np.isfinite(v)) for v in (p, f, vp, vf)) or
            np.any(total <= 0) or np.any(p < 0) or np.any(f < 0) or
            np.any(vp < 0) or np.any(vf < 0)):
        raise ValueError("efficiency requires positive totals and nonnegative pass/fail sums and variances")
    value = p / total
    error = np.sqrt(f*f*vp + p*p*vf) / (total*total)
    return (float(value), float(error)) if value.ndim == 0 else (value, error)


def rebin_counts(counts, pt_edges):
    requested = np.asarray(pt_edges, dtype=float)
    if len(requested) < 2 or not np.all(np.isfinite(requested)) or np.any(np.diff(requested) <= 0):
        raise ValueError("rebin edges must be finite and strictly increasing")
    result = {}
    for name, (values, variances, old_edges, eta_edges) in counts.items():
        old_edges = np.asarray(old_edges)
        if requested[0] != old_edges[0] or requested[-1] != old_edges[-1]:
            raise ValueError("rebinning must preserve the measurement domain")
        indices = []
        for edge in requested:
            matches = np.flatnonzero(old_edges == edge)
            if len(matches) != 1:
                raise ValueError("rebin edges must align with saved source boundaries")
            indices.append(int(matches[0]))
        result[name] = (np.array([values[a:b].sum(axis=0) for a,b in zip(indices, indices[1:])]),
            np.array([variances[a:b].sum(axis=0) for a,b in zip(indices, indices[1:])]),
            requested, np.asarray(eta_edges))
    return result


def read_pass_fail(inputs, names, *, directory="taggerEfficiencies"):
    import uproot
    result = {}
    for entry in inputs:
        scale = float(entry.get("scale", 1.))
        with uproot.open(entry["path"]) as source:
            for name in names:
                if f"{directory}/{name}" not in source:
                    continue
                hist = source[f"{directory}/{name}"]
                values = hist.values(flow=False) * scale
                variance = hist.variances(flow=False)
                if variance is None:
                    raise ValueError(f"efficiency input has no sumw2: {name}")
                variance = variance * scale**2
                edges = tuple(axis.edges() for axis in hist.axes)
                if name in result:
                    previous = result[name]
                    if any(not np.array_equal(a,b) for a,b in zip(previous[2:],edges)):
                        raise ValueError(f"incompatible efficiency axes for {name}")
                    result[name] = (previous[0]+values, previous[1]+variance, *edges)
                else:
                    result[name] = (values, variance, *edges)
    missing = set(names) - set(result)
    if missing:
        raise ValueError(f"missing efficiency histograms: {sorted(missing)}")
    return result


def export_pass_fail(inputs, output, *, groups, flavors, working_points,
                     correction_name, directory="taggerEfficiencies", pt_edges=None):
    names = [f"{group}_{flavor}_{wp}_{state}" for group in groups for flavor in flavors
             for wp in working_points for state in ("pass", "fail")]
    counts = read_pass_fail(inputs, names, directory=directory)
    if pt_edges is not None:
        counts = rebin_counts(counts, pt_edges)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    diagnostics = {}
    for group in groups:
        by_flavor = []
        for flavor in flavors:
            by_wp = []
            for wp in working_points:
                base = f"{group}_{flavor}_{wp}"
                passed, vp, ptedges, etaedges = counts[base+"_pass"]
                failed, vf, _, _ = counts[base+"_fail"]
                values, errors = efficiency(passed, failed, vp, vf)
                node = dict(nodetype="multibinning", inputs=["abseta", "pt"],
                    edges=[etaedges.tolist(), ptedges.tolist()], content=values.T.ravel().tolist(), flow="clamp")
                by_wp.append(dict(key=wp, value=node))
                diagnostics[base] = dict(efficiency=values.ravel().tolist(), stat_error=errors.ravel().tolist())
            by_flavor.append(dict(key=int(flavor), value=dict(nodetype="category", input="working_point", content=by_wp)))
        payload = dict(schema_version=2, corrections=[dict(name=correction_name, version=1,
            inputs=[dict(name="flavor", type="int"), dict(name="working_point", type="string"),
                    dict(name="abseta", type="real"), dict(name="pt", type="real")],
            output=dict(name="efficiency", type="real"),
            data=dict(nodetype="category", input="flavor", content=by_flavor))])
        (output/f"{group}_btag_efficiencies.json").write_text(json.dumps(payload, indent=2)+"\n")
    (output/"diagnostics.json").write_text(json.dumps(diagnostics, indent=2)+"\n")
    return diagnostics
