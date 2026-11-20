"""Independent PyROOT reference for compat_probe output (no TensorFlow import)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np


class CombineReference:
    def __init__(self, workspace, model, probe, library):
        from .combine import _ensure_combine_stack_size
        _ensure_combine_stack_size()
        import ROOT
        if ROOT.gSystem.Load(str(library)) < 0:
            raise RuntimeError(f"Cannot load Combine: {library}")
        source = Path(library).resolve().parents[1]/"src/CombineTool"
        ROOT.gInterpreter.AddIncludePath(str(source))
        if not hasattr(ROOT, "rdfCompatVariance") and not ROOT.gInterpreter.Declare(r'''
        #include "interface/CMSHistErrorPropagator.h"
        #include "interface/CMSHistFunc.h"
        #include <vector>
        std::vector<double> rdfCompatVariance(CMSHistErrorPropagator* p, unsigned n) {
            p->getVal();
            std::vector<double> out(n, 0.);
            auto const& fs = p->funcList(); auto const& cs = p->coefList();
            for (int i=0; i<fs.getSize(); ++i) {
                auto f = dynamic_cast<CMSHistFunc*>(fs.at(i));
                double c = dynamic_cast<RooAbsReal*>(cs.at(i))->getVal();
                for (unsigned j=0; j<n; ++j) {
                    double e = f->errors()[j]*c; out[j] += e*e;
                }
            }
            return out;
        }
        '''):
            raise RuntimeError("Cannot compile native Combine reference helper")
        self.ROOT = ROOT
        self.handle = ROOT.TFile.Open(str(workspace))
        self.workspace = self.handle.Get("w")
        self.model = model
        self.nuisances = sorted({e["name"] for e in model["effects"]})
        if self.workspace.var("r") and "r" not in probe["parameter_order"]:
            self.workspace.var("r").setVal(1.)
        self.observable = self.workspace.var("CMS_th1x")
        self.normset = ROOT.RooArgSet(self.observable)
        # Use the same observable cache optimization as CachingNLL. Without
        # it, changing the common bin coordinate propagates dirty flags
        # through every shared polynomial expression in a large workspace.
        pdf = self.workspace.pdf("model_s")
        if pdf:
            pdf.optimizeCacheMode(self.normset)
        self.channels = []
        offset = 0
        ranges = probe.get("channel_ranges")
        channels = sorted(model["channels"], key=lambda c: ranges[c["name"]]["start"]) if ranges else model["channels"]
        for channel in channels:
            size = len(channel["data"]["values"])
            name = channel["name"]
            prop = self.workspace.function("prop_bin"+name)
            data = hist = None
            beta = []
            if prop:
                hist = ROOT.TH1D("shared_"+name, "", size, 0., float(size))
                hist.SetDirectory(0)
                for i, value in enumerate(probe["observations"][offset:offset+size]):
                    hist.SetBinContent(i+1, value)
                # RooDataHist adjusts the supplied observable's binning. Give
                # it a copy so channels with different lengths cannot mutate
                # the workspace's common observable or each other's range.
                observable = ROOT.RooRealVar(self.observable)
                data = ROOT.RooDataHist("shared_data_"+name, "", ROOT.RooArgList(observable), hist)
                prop.setData(data)
                beta = [self.workspace.var(f"prop_bin{name}_bin{i}") for i in range(size)]
            self.channels.append((name, size, prop, beta, data, hist))
            offset += size
        self.observed = np.asarray(probe["observations"])

    def evaluate(self, parameters):
        for name, value in parameters.items():
            var = self.workspace.var(name)
            if not var:
                raise ValueError(f"Native workspace lacks {name}")
            var.setVal(float(value))
        expected, variances, profiled = [], [], []
        penalty = .5*sum(parameters[name]**2 for name in self.nuisances)
        for name, size, prop, beta, data, hist in self.channels:
            if prop:
                prop.setAnalyticBarlowBeeston(False)
                for v in beta:
                    if v:
                        v.setVal(0.)
                mean = []
                for i in range(size):
                    self.observable.setVal(i+.5)
                    mean.append(prop.getVal())
                variance = list(self.ROOT.rdfCompatVariance(prop, size))
                prop.setAnalyticBarlowBeeston(True)
                prop.runBarlowBeeston()
                fitted = []
                for i in range(size):
                    self.observable.setVal(i+.5)
                    fitted.append(prop.getVal())
                penalty += .5*sum(v.getVal()**2 for v in beta if v)
            else:
                pdf = self.workspace.pdf("pdf_bin"+name)
                total = pdf.expectedEvents(self.normset)
                mean = []
                for i in range(size):
                    self.observable.setVal(i+.5)
                    mean.append(pdf.getVal(self.normset)*total)
                fitted, variance = mean, [0.]*size
            expected.extend(mean)
            profiled.extend(fitted)
            variances.extend(variance)
        n, y = self.observed, np.asarray(profiled)
        nll = float(np.sum(y-n-n*np.log(y/np.where(n > 0, n, 1.)))+penalty)
        return dict(expected=expected, variance=variances, profiled=profiled, nll=nll)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspace", type=Path)
    parser.add_argument("model", type=Path)
    parser.add_argument("probe", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--derivatives", action="store_true")
    args = parser.parse_args()
    model = json.loads(args.model.read_text())
    probe = json.loads(args.probe.read_text())
    reference = CombineReference(args.workspace, model, probe, args.library)
    output = dict(points=[], excluded_mc_boundary_points=probe.get("excluded_mc_boundary_points", []),
                  probe=str(args.probe.resolve()))
    for index, point in enumerate(probe["points"]):
        if index % 10 == 0:
            print(f"Native likelihood validation: {index}/{len(probe['points'])}", flush=True)
        result = reference.evaluate(point["parameters"])
        result["parameters"] = point["parameters"]
        if args.derivatives:
            values = []
            h = 1e-4
            for name in probe["parameter_order"]:
                plus, minus = dict(point["parameters"]), dict(point["parameters"])
                plus[name] += h
                minus[name] -= h
                values.append((reference.evaluate(plus)["nll"]-reference.evaluate(minus)["nll"])/(2*h))
            result["gradient"] = values
        output["points"].append(result)
    metrics = {}
    for key in ("expected", "variance", "profiled", "nll", "gradient"):
        if key not in output["points"][0]:
            continue
        a = np.asarray([p[key] for p in probe["points"]])
        b = np.asarray([p[key] for p in output["points"]])
        metrics[key] = dict(max_abs_difference=float(np.max(np.abs(a-b))),
                            max_scaled_difference=float(np.max(np.abs(a-b)/np.maximum(1., np.abs(b)))))
    output["metrics"] = metrics
    args.output.write_text(json.dumps(output, indent=2, allow_nan=False)+"\n")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
