"""Exercise the compiled plotting API, rather than mocking its bindings."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import subprocess
import shutil
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.environ["RDFANALYZER_PYTHON_MODULE_DIR"])
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
import rdfanalyzer as rdf
from plot_config import apply_plot_options


class PlottingBindingsTest(unittest.TestCase):
    def test_macros_replay_without_framework_and_keep_annotations_in_frame(self):
        root = shutil.which("root")
        if not root:
            self.skipTest("ROOT executable is unavailable")
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            configs = [
                {"outputFile": str(base / "distribution.C"),
                 "additionalOutputs": [str(base / "distribution.png")],
                 "title": "SelectionBoxTitle", "annotations": ["Region annotation"],
                 "experimentExtra": "Simulation", "luminosityLabel": "100 fb^{-1} (13.6 TeV)",
                 "binEdges": [0, 1, 2, 3], "blindBins": [False, True, False],
                 "panels": ["significance", "signal_background", "ratio"],
                 "style": {"labelSize": 24},
                 "processes": [
                     {"legendLabel": "Background", "values": [20, 15, 10], "variances": [4, 3, 2]},
                     {"legendLabel": "Signal", "isSignal": True, "values": [1, 3, 2], "variances": [1, 1, 1]},
                     {"isData": True, "values": [21, 987654321, 12], "variances": [21, 987654321, 12]},
                 ]},
                {"kind": "heatmap", "outputFile": str(base / "response.C"),
                 "additionalOutputs": [str(base / "response.png")],
                 "title": "SelectionBoxTitle", "experimentExtra": "Simulation",
                 "binEdges": [0, 1, 2], "yBinEdges": [0, 1, 2, 3],
                 "matrix": [10, -2, 0, 5, -1, 8], "yBinLabels": ["light", "heavy", "top"]},
            ]
            config = base / "plots.json"
            config.write_text(json.dumps(configs))
            result = subprocess.run([os.environ["RDF_PLOT_EXECUTABLE"], "--config", str(config)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            for spec in configs:
                macro = Path(spec["outputFile"])
                text = macro.read_text()
                self.assertNotIn("987654321", text)
                self.assertNotIn("rdfplot::", text)
                self.assertNotIn("PlottingUtility::", text)
                image = macro.with_name(macro.stem + "_replayed.png")
                wrapper = base / "replay.C"
                wrapper.write_text(r'''
#include <TROOT.h>
#include <TCanvas.h>
#include <TPad.h>
#include <TH1.h>
#include <TH2.h>
#include <TLatex.h>
#include <TPaletteAxis.h>
#include <TSystem.h>
#include <TList.h>
#include <cstring>
void replay() {
  Int_t error=0;
  gROOT->Macro(MACRO_PATH, &error);
  if(error) {gSystem->Exit(1);return;}
  auto* canvas=dynamic_cast<TCanvas*>(gROOT->GetListOfCanvases()->Last());
  if(!canvas) {gSystem->Exit(2);return;}
  bool header=false, selection=false, palette=false, heat=false;
  TIter pads(canvas->GetListOfPrimitives());
  while(auto* object=pads()) {
    auto* pad=dynamic_cast<TPad*>(object); if(!pad)continue;
    const bool isHeader=strstr(pad->GetName(),"_header");
    TIter primitives(pad->GetListOfPrimitives());
    while(auto* primitive=primitives()) {
      if(auto* text=dynamic_cast<TLatex*>(primitive)) {
        const bool isSelection=strcmp(text->GetTitle(),"SelectionBoxTitle")==0;
        if(isHeader) {
          header=true;
          if(isSelection || strcmp(text->GetTitle(),"Region annotation")==0) {gSystem->Exit(3);return;}
        } else if(isSelection) {
          selection=true;
          if(text->GetY()<=pad->GetBottomMargin() || text->GetY()>=1-pad->GetTopMargin()) {gSystem->Exit(4);return;}
        }
      }
      if(auto* hist=dynamic_cast<TH1*>(primitive)) {
        if(hist->GetDimension()==1) {
          const double pixels=canvas->GetWh()*pad->GetAbsHNDC();
          const double halfGlyph=hist->GetYaxis()->GetLabelSize()/2+3;
          if(pixels*pad->GetBottomMargin()<halfGlyph || pixels*pad->GetTopMargin()<halfGlyph) {gSystem->Exit(5);return;}
        } else if(auto* matrix=dynamic_cast<TH2*>(hist)) {
          heat=true;
          if(matrix->GetXaxis()->GetFirst()!=1 || matrix->GetXaxis()->GetLast()!=2 ||
             matrix->GetYaxis()->GetFirst()!=1 || matrix->GetYaxis()->GetLast()!=3 ||
             matrix->GetBinContent(2,1)!=-2) {gSystem->Exit(6);return;}
        }
      }
      palette |= dynamic_cast<TPaletteAxis*>(primitive)!=nullptr;
    }
  }
  if(!header || !selection || (heat && !palette)) {gSystem->Exit(7);return;}
  canvas->SaveAs(IMAGE_PATH);
}
'''.replace("MACRO_PATH", json.dumps(str(macro))).replace("IMAGE_PATH", json.dumps(str(image))))
                replay = subprocess.run([root, "-l", "-b", "-q", str(wrapper)],
                                        capture_output=True, text=True, timeout=60)
                self.assertEqual(replay.returncode, 0, replay.stdout + replay.stderr)
                self.assertGreater(image.stat().st_size, 0)
                reference = Path(spec["additionalOutputs"][0])
                if spec.get("kind") == "heatmap":
                    self.assertEqual(image.read_bytes(), reference.read_bytes())
                else:
                    try:
                        from PIL import Image, ImageChops, ImageStat
                    except ImportError:
                        continue
                    original, replayed = Image.open(reference), Image.open(image)
                    self.assertEqual(original.size, replayed.size)
                    # ROOT's saved pad transforms can round a boundary by one
                    # pixel; compare visual content rather than PNG encoding.
                    difference = ImageStat.Stat(ImageChops.difference(original, replayed))
                    self.assertLess(max(difference.mean), 1.0)

    def test_compiled_renderer_processes_report_failures_and_order(self):
        import yaml
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            configs = [
                {"outputFile": str(base / f"plot{i}.svg"), "drawRatio": False,
                 "binEdges": [0, 1, 2], "processes": [
                     {"values": [10, 20], "variances": [4, 9], "legendLabel": f"sample{i}"}
                 ]}
                for i in range(2)
            ]
            config, report = base / "plots.json", base / "report.yaml"
            config.write_text(json.dumps(configs))
            command = [os.environ["RDF_PLOT_EXECUTABLE"], "--config", str(config),
                       "--render-processes", "2", "--report", str(report)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            results = yaml.safe_load(report.read_text())["results"]
            self.assertEqual([item["outputs"][0] for item in results],
                             [cfg["outputFile"] for cfg in configs])
            configs[0]["processes"][0]["variances"][0] = -1
            config.write_text(json.dumps(configs))
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            results = yaml.safe_load(report.read_text())["results"]
            self.assertEqual([item["success"] for item in results], [False, True])
            configs[0]["outputFile"] = configs[1]["outputFile"]
            config.write_text(json.dumps(configs))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unique output paths", result.stderr)

    def test_session_reuses_objects_and_handles_extended_workflow_config(self):
        with tempfile.TemporaryDirectory() as directory:
            config = {
                "outputFile": str(Path(directory) / "plot.svg"),
                "title": "Binding test", "binEdges": [0, 1, 2],
                "panels": ["ratio"], "experimentLabel": "Example",
                "processes": [
                    {"legendLabel": "MC", "values": [10, 20], "variances": [4, 9], "color": "#4477AA"},
                    {"isData": True, "values": [11, 19], "variances": [11, 19]},
                ],
            }
            requests = rdf.PlottingUtility.parseRequests(json.dumps([config]))
            options = rdf.PlotBatchOptions()
            options.workers = 2
            session = rdf.PlotSession(options)
            result = session.render(requests[0])
            self.assertTrue(result.success, result.message)
            warm = session.stats()
            results = session.renderBatch(requests * 3)
            self.assertTrue(all(result.success for result in results))
            self.assertEqual(session.stats().drawableObjects, warm.drawableObjects)
            self.assertEqual(session.stats().bufferGrowths, warm.bufferGrowths)
            self.assertEqual(session.stats().plots, 4)
            with ThreadPoolExecutor(1) as executor:
                with self.assertRaises(ValueError):
                    executor.submit(session.render, requests[0]).result()
            request = apply_plot_options(requests[0], config, rdf)
            self.assertEqual(request.processes[0].colorHex, "#4477AA")
            self.assertTrue(session.render(request).success)
            self.assertGreater(Path(request.outputFile).stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
