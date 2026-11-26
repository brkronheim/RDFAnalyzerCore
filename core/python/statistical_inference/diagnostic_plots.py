"""Render diagnostics from the artifacts of a completed statistical fit."""

import json
from pathlib import Path

from .errors import FitError
from .results import read_fit_histograms


PLOT_TYPES = ("postfit", "prefit_postfit", "correlations", "pulls", "fit_summary")


def _read_json(path):
    return json.loads(path.read_text())


def _rabbit_case(summary, certified, np):
    for key in ("model", "histograms"):
        if Path(summary[key]).resolve() not in certified:
            raise FitError(f"Rabbit {key} is absent from the fit receipt")
    _, histograms = read_fit_histograms({"backend": "rabbit", "rabbit": summary}, require_uncertainties=False)
    model = _read_json(Path(summary["model"]))
    edges = {channel["name"]: channel["data"]["edges"] for channel in model["channels"]}
    if histograms.keys() != edges.keys() or summary.get("status") != "rabbit_complete":
        raise FitError("saved Rabbit predictions do not describe a completed fit")
    channels = {}
    for name, hist in histograms.items():
        channels[name] = {
            "edges": edges[name],
            "prefit": hist["prefit"], "postfit": hist["postfit"], "data": hist["data"],
            "data_errors": np.sqrt(hist["data_variance"]),
        }
        if summary.get("histogram_errors_computed", True):
            channels[name]["postfit_error"] = np.sqrt(hist["postfit_variance"])
    # Rabbit's card lnN and shape effects use unit Gaussian constraints.
    # Free rate parameters and implicit MC-stat parameters use other conventions.
    constrained = {effect["name"] for effect in model["effects"]}
    pulls = {name: (value["value"], value["error"]) for name, value in summary["nuisance_results"].items()
             if name in constrained}
    return dict(tag=summary["mode"], method="Rabbit", dataset=summary["mode"],
                channels=channels, pulls=pulls, covariance=_saved_covariance(summary),
                estimates=summary["poi_results"],
                status={key: summary[key] for key in ("minimizer_status", "edm", "covariance_rank",
                                                      "covariance_min_eigenvalue") if key in summary})


def _saved_covariance(summary):
    for prefix in ("fit_parameter_covariance", "full_covariance", "covariance"):
        order = summary.get(f"{prefix}_order")
        if order:
            return order, summary[prefix]
    return None


def _roofit_parameters(path, *, diagnostics):
    import ROOT

    with ROOT.TFile.Open(str(path), "READ") as handle:
        fit = handle.Get("fit_s" if diagnostics else "fit_mdf")
        if not fit:
            raise FitError(f"no saved {'signal-plus-background' if diagnostics else 'MultiDimFit'} result in {path}")
        status = {"fit_status": int(fit.status()), "covariance_quality": int(fit.covQual()),
                  "edm": float(fit.edm())}
        if status["fit_status"] != 0:
            raise FitError(f"{path.name} has fit status {status['fit_status']}")
        parameters = list(fit.floatParsFinal())
        names = [str(parameter.GetName()) for parameter in parameters]
        matrix = fit.covarianceMatrix()
        covariance = [[float(matrix[i][j]) for j in range(len(names))] for i in range(len(names))]
        estimates = {name: {"value": float(parameter.getVal()), "error": float(parameter.getError())}
                     for name, parameter in zip(names, parameters)}
        prefit = handle.Get("nuisances_prefit") if diagnostics else None
        pulls = {}
        if prefit:
            for name, estimate in estimates.items():
                reference = prefit.find(name)
                if reference and reference.InheritsFrom("RooRealVar") and reference.getError() > 0:
                    sigma = float(reference.getError())
                    pulls[name] = ((estimate["value"] - float(reference.getVal())) / sigma,
                                   estimate["error"] / sigma)
    return dict(covariance=(names, covariance), estimates=estimates, pulls=pulls, status=status)


def _combine_channels(path, *, uncertainties, np):
    import uproot

    channels = {}
    with uproot.open(path) as handle:
        if "shapes_fit_s" not in handle:
            return channels
        postfit = handle["shapes_fit_s"]
        prefit = handle.get("shapes_prefit")
        channel_names = postfit.keys(recursive=False, cycle=False)
        prefit_channels = prefit.keys(recursive=False, cycle=False) if prefit is not None else []
        if prefit_channels and set(channel_names) != set(prefit_channels):
            raise FitError(f"Combine prefit and postfit channels differ in {path}")
        for channel in channel_names:
            post = postfit[channel]["total"]
            edges = post.axis().edges()
            centers = (edges[1:] + edges[:-1]) / 2
            widths = np.diff(edges)
            data = postfit[channel]["data"]
            if data.values("x").shape != centers.shape or not np.allclose(centers, data.values("x")):
                raise FitError(f"Combine data and prediction bins differ for {channel} in {path}")
            # FitDiagnostics saves the shapes and data graph as densities.
            channels[channel] = {
                "edges": edges, "postfit": post.values() * widths,
                "data": data.values("y") * widths,
                "data_errors": np.array([data.errors(side, "y") for side in ("low", "high")]) * widths,
            }
            if channel in prefit_channels:
                pre = prefit[channel]["total"]
                if not np.array_equal(edges, pre.axis().edges()):
                    raise FitError(f"Combine prefit and postfit bin edges differ for {channel} in {path}")
                channels[channel]["prefit"] = pre.values() * widths
            if uncertainties and "total_covar" in postfit[channel]:
                channels[channel]["postfit_error"] = post.errors() * widths
    return channels


def _combine_case(tag, method, dataset, artifacts, command, np, summary=None):
    case = dict(tag=tag, method=method, dataset=dataset)
    diagnostic = next((path for path in artifacts if path.name.startswith("fitDiagnostics")), None)
    if method == "FitDiagnostics":
        if diagnostic is None:
            raise FitError(f"Combine FitDiagnostics did not save a native result for {tag}")
        case.update(_roofit_parameters(diagnostic, diagnostics=True))
        saved_errors = "--saveWithUncertainties" in command and (
            case["status"]["covariance_quality"] == 3 or "--ignoreCovWarning" in command
            or "--robustHesse" in command
        )
        case["channels"] = _combine_channels(diagnostic, uncertainties=saved_errors, np=np)
    elif summary and _saved_covariance(summary) is not None:
        case.update(covariance=_saved_covariance(summary), estimates=summary["poi_results"],
                    status={"covariance_method": summary.get("covariance_method")})
    elif method == "MultiDimFit":
        native = next((path for path in artifacts if path.name.startswith("multidimfit")), None)
        if native is not None:
            case.update(_roofit_parameters(native, diagnostics=False))
    elif method in ("AsymptoticLimits", "Significance"):
        native = next((path for path in artifacts if path.name.startswith("higgsCombine")), None)
        if native is not None:
            import uproot
            with uproot.open(native) as handle:
                tree = handle["limit"]
                entries = tree.arrays(["limit", "quantileExpected"], library="np")
                case["statistic"] = _combine_statistic(method, entries, command, np)
    return case


def _combine_statistic(method, entries, command, np):
    values, quantiles = entries["limit"], entries["quantileExpected"]
    if not len(values) or not np.all(np.isfinite(values)):
        raise FitError(f"Combine {method} has no finite result")
    if method == "Significance":
        if len(values) != 1:
            raise FitError("Significance plotting requires one saved result")
        label = "p-value" if "--pvalue" in command else "Significance"
        return {"kind": label, "values": {"Fit sample": float(values[0])}}
    points = {"Observed / fit sample": -1, "Expected −2σ": .025, "Expected −1σ": .16,
              "Median expected": .5, "Expected +1σ": .84, "Expected +2σ": .975}
    selected = {}
    for label, quantile in points.items():
        matches = values[np.isclose(quantiles, quantile, atol=1e-5)]
        if len(matches) > 1:
            raise FitError(f"Combine has multiple {label} limits")
        if len(matches):
            selected[label] = float(matches[0])
    if not selected:
        raise FitError("Combine has no recognized limit quantiles")
    return {"kind": "Upper limit", "values": selected}


def _cases(source, receipt, np):
    certified = {Path(path).resolve() for path in receipt["files"]}
    if receipt["kind"] == "fit":
        provenance = source / "provenance.json"
        if provenance not in certified:
            raise FitError("fit provenance is absent from the fit receipt")
        backend = _read_json(provenance)["fitting_backend"]
        if backend == "analysis":
            summary = source / "analysis_fit_results.json"
            if summary not in certified:
                raise FitError("analysis results are absent from the fit receipt")
            return backend, [dict(tag=row["tag"], analysis=row) for row in _read_json(summary)]
        summaries = sorted(path for path in certified if path.name == "fit_result.json")
        if backend == "rabbit":
            return backend, [_rabbit_case(_read_json(path), certified, np) for path in summaries]
        if backend == "combine":
            cases = []
            for path in summaries:
                result = _read_json(path)
                artifacts = [Path(name).resolve() for name in result["root_outputs"]]
                if any(artifact not in certified for artifact in artifacts):
                    raise FitError(f"{path} refers to ROOT output outside the fit receipt")
                cases.append(_combine_case(path.parent.name, result["method"], result["mode"],
                                           artifacts, result["command"], np, summary=result))
            return backend, cases
        raise FitError(f"unsupported fit backend {backend}")

    import shlex

    provenance = source / "provenance.json"
    if provenance not in certified:
        raise FitError("analytic fit provenance is absent from the fit receipt")
    info = _read_json(provenance)
    if info["skip_fit"]:
        raise FitError("analytic workspaces were generated with skip_fit")
    method = info["method"]
    command = shlex.split(receipt["configuration"].get("combine_options", "--saveShapes --saveNormalizations"))
    dataset = "asimov" if ("--toys=-1" in command or any(
        key in ("-t", "--toys") and value == "-1" for key, value in zip(command, command[1:])
    )) else None
    cases = []
    for tag in [*info["channels"], "combined"]:
        names = {f"fitDiagnostics_{tag}.root", f"multidimfit_{tag}.root"}
        artifacts = sorted(path for path in certified if path.name in names
                           or path.name.startswith(f"higgsCombine_{tag}.{method}."))
        cases.append(_combine_case(tag, method, dataset, artifacts, command, np))
    return "analytic_combine", cases


def _shape_plot(plt, np, path, title, dataset, shape):
    edges = np.asarray(shape["edges"], dtype=float)
    post, data = (np.asarray(shape[key], dtype=float) for key in ("postfit", "data"))
    pre = shape.get("prefit")
    errors = np.asarray(shape["data_errors"])
    post_error = shape.get("postfit_error")
    arrays = [edges, post, data, errors]
    if pre is not None:
        arrays.append(pre)
    if post_error is not None:
        arrays.append(post_error)
    if (data.ndim != 1 or not len(data) or edges.shape != (len(data) + 1,) or np.any(np.diff(edges) <= 0)
            or (pre is not None and np.shape(pre) != data.shape)
            or post.shape != data.shape or errors.shape not in {data.shape, (2, len(data))}
            or (post_error is not None and np.shape(post_error) != data.shape)
            or np.any(errors < 0) or (post_error is not None and np.any(post_error < 0))
            or any(not np.all(np.isfinite(a)) for a in arrays)):
        raise FitError(f"invalid saved binning or prediction for {title}")
    fig, (ax, residual) = plt.subplots(2, 1, sharex=True, figsize=(8, 6),
                                      gridspec_kw={"height_ratios": [3, 1]}, constrained_layout=True)
    x = (edges[1:] + edges[:-1]) / 2
    if pre is not None:
        ax.stairs(pre, edges, label="Prefit", color="C1", linestyle="--")
    ax.stairs(post, edges, label="Postfit", color="C0")
    if post_error is not None:
        ax.stairs(post + post_error, edges, baseline=post - post_error, fill=True,
                  color="C0", alpha=.2, label="Saved postfit uncertainty")
        residual.stairs(post_error, edges, baseline=-post_error, fill=True, color="C0", alpha=.2)
    ax.errorbar(x, data, yerr=errors, fmt=".", color="black",
                label={"asimov": "Asimov data", "observed": "Observed data"}.get(dataset, "Fit data"))
    ax.set(title=title, ylabel="Events per bin")
    ax.legend()
    residual.axhline(0, color="C0", linewidth=1)
    residual.errorbar(x, data - post, yerr=errors, fmt=".", color="black")
    residual.set(xlabel="Observable (saved fit axis)", ylabel="Data − fit")
    _save(plt, fig, path)


def _save(plt, fig, path):
    try:
        fig.savefig(path, dpi=140)
    finally:
        plt.close(fig)


def _parameter_plot(plt, np, path, title, values, limit, *, pull=False):
    names = list(values)[:limit]
    numbers = np.asarray([values[name] for name in names], dtype=float)
    if not np.all(np.isfinite(numbers)) or np.any(numbers[:, 1] < 0):
        raise FitError(f"invalid saved parameter errors for {title}")
    y = np.arange(len(names))
    fig, ax = plt.subplots(figsize=(8, max(3, .28 * len(names) + 1.6)), constrained_layout=True)
    if pull:
        ax.axvspan(-1, 1, color="C0", alpha=.12)
        ax.axvline(0, color="C0", linewidth=1)
    ax.errorbar(numbers[:, 0], y, xerr=numbers[:, 1], fmt=".", color="black")
    ax.set(yticks=y, yticklabels=names, title=title,
           xlabel="(Postfit − prefit) / prefit σ" if pull else "Fitted value and saved uncertainty")
    ax.invert_yaxis()
    _save(plt, fig, path)
    return names


def _correlation_plot(plt, np, path, title, covariance, limit):
    names, values = covariance
    matrix = np.asarray(values, dtype=float)
    if (matrix.shape != (len(names), len(names)) or len(set(names)) != len(names)
            or not np.all(np.isfinite(matrix)) or not np.allclose(matrix, matrix.T)):
        raise FitError(f"invalid saved covariance for {title}")
    variances = np.diag(matrix)
    indices = np.flatnonzero(variances > 0)[:limit]
    if not len(indices):
        return []
    selected = matrix[np.ix_(indices, indices)]
    errors = np.sqrt(variances[indices])
    correlations = selected / np.outer(errors, errors)
    if np.max(np.abs(correlations)) > 1 + 1e-5:
        raise FitError(f"saved covariance gives invalid correlations for {title}")
    names = [names[index] for index in indices]
    fig, ax = plt.subplots(figsize=(8, 7), constrained_layout=True)
    image = ax.imshow(correlations, vmin=-1, vmax=1, cmap="RdBu_r")
    ax.set(title=title, xticks=range(len(names)), yticks=range(len(names)), yticklabels=names)
    ax.set_xticklabels(names, rotation=90, fontsize=7)
    ax.tick_params(axis="y", labelsize=7)
    fig.colorbar(image, ax=ax, label="Correlation")
    _save(plt, fig, path)
    return names


def _analysis_plot(plt, np, path, title, result):
    if result["status"] != "ok":
        raise FitError(f"analysis comparison did not complete for {title}")
    fig, ax = plt.subplots(figsize=(6, 4), constrained_layout=True)
    yields = [result["data_integral"], result["bkg_total"]]
    if not np.all(np.isfinite([*yields, result["chi2"], result["ndof"]])):
        raise FitError(f"invalid saved analysis comparison for {title}")
    ax.bar(["Data", "Saved reference"], yields)
    ax.set(title=f"{title} · aggregate comparison\nχ² = {result['chi2']:.3g}, ndof = {result['ndof']}",
           ylabel="Integrated yield")
    _save(plt, fig, path)


def render_fit_diagnostics(source, destination, receipt, options):
    """Publish plots and an inventory of diagnostics available in a fit receipt."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    source = source.resolve()
    backend, cases = _cases(source, receipt, np)
    if not cases:
        raise FitError(f"verified {backend} fit has no results to plot")
    limit = options.get("max_parameters", 24)
    required = set(options.get("required", []))
    report = {"backend": backend, "fit_artifact_id": receipt["artifact_id"], "cases": []}
    for index, case in enumerate(cases, start=1):
        tag = case["tag"]
        title = tag + (f" · {case['method']}" if case.get("method") else "")
        folder = destination / f"fit-{index:03d}"
        folder.mkdir()
        rendered = {}
        for channel_index, (name, shape) in enumerate(case.get("channels", {}).items(), start=1):
            path = folder / f"channel-{channel_index:03d}.png"
            _shape_plot(plt, np, path, f"{title} · {name}", case.get("dataset"), shape)
            rendered.setdefault("postfit", []).append({
                "channel": name, "path": str(path.relative_to(destination)),
                "bins": len(shape["data"]), "prefit": shape.get("prefit") is not None,
                "postfit_uncertainty": shape.get("postfit_error") is not None,
            })
        if case.get("covariance"):
            path = folder / "correlations.png"
            names = _correlation_plot(plt, np, path, title, case["covariance"], limit)
            if names:
                rendered["correlations"] = {"path": str(path.relative_to(destination)), "parameters": names,
                                            "saved_parameters": len(case["covariance"][0])}
        if case.get("pulls"):
            path = folder / "pulls.png"
            names = _parameter_plot(plt, np, path, title, case["pulls"], limit, pull=True)
            rendered["pulls"] = {"path": str(path.relative_to(destination)), "parameters": names,
                                 "saved_parameters": len(case["pulls"])}
        path = folder / "fit-summary.png"
        if "analysis" in case:
            _analysis_plot(plt, np, path, title, case["analysis"])
            rendered["fit_summary"] = {"path": str(path.relative_to(destination))}
        elif case.get("statistic"):
            statistic = case["statistic"]
            fig, ax = plt.subplots(figsize=(7, 4), constrained_layout=True)
            ax.barh(list(statistic["values"]), list(statistic["values"].values()))
            ax.invert_yaxis()
            ax.set(title=title, xlabel=statistic["kind"])
            _save(plt, fig, path)
            rendered["fit_summary"] = {"path": str(path.relative_to(destination)), **statistic}
        elif case.get("estimates"):
            estimates = {name: (value["value"], value["error"]) for name, value in case["estimates"].items()}
            names = _parameter_plot(plt, np, path, title, estimates, limit)
            rendered["fit_summary"] = {"path": str(path.relative_to(destination)), "parameters": names,
                                       "saved_parameters": len(estimates)}
        missing = {
            "postfit": "no saved postfit predictions; use Rabbit or Combine FitDiagnostics with --saveShapes",
            "prefit_postfit": "at least one channel lacks saved prefit or postfit predictions",
            "correlations": "no saved covariance with positive parameter variances",
            "pulls": "no fitted nuisance parameters with a saved or defined prefit constraint",
            "fit_summary": "no saved fit estimates; use a method that saves a fit result",
        }
        absent = {name: reason for name, reason in missing.items() if name not in rendered}
        if rendered.get("postfit") and all(plot["prefit"] for plot in rendered["postfit"]):
            absent.pop("prefit_postfit")
        if not rendered or required & absent.keys():
            requested = sorted((required & absent.keys()) or absent.keys())
            raise FitError(f"{tag} cannot produce diagnostics: " + "; ".join(f"{name}: {absent[name]}" for name in requested))
        report["cases"].append({
            "fit": tag, "method": case.get("method"), "dataset": case.get("dataset"),
            "status": case.get("status", {}), "plots": rendered, "unavailable": absent,
        })
    (destination / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
