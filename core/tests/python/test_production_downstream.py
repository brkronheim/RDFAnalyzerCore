import json
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from contextlib import nullcontext
import sys

from dataset_manifest import DatasetManifest
from production_downstream import bind_refill, describe_downstream, downstream_targets, run_consumer, saved_manifest_files
from production_joint_fit import _canonical_combination, describe_joint_fit, joint_targets
from output_schema import HistogramSchema, OutputManifest
from run_bundle import BundleError, derived_artifact, seal_bundle, verify_artifact, verify_bundle, write_json


class TestRefill(unittest.TestCase):
    def test_refill_reseals_only_verified_skim_files_in_partition_order(self):
        with TemporaryDirectory() as scratch:
            root = Path(scratch)
            source = root / "bundle"
            source.mkdir()
            (source / "submit.yaml").write_text("enableSkim: false\ntreeList: Events\n")
            (source / "datasets.yaml").write_text(
                "lumi: 2\ndatasets:\n- name: A\n  dtype: data\n  files: [raw.root]\n")
            original = seal_bundle(source)
            spec = root / "refill.json"
            spec.write_text(json.dumps({"version": 1, "name": "refill", "backend": "local",
                                        "exe": sys.executable, "submit_config": "bundle/submit.yaml",
                                        "dataset_manifest": "bundle/datasets.yaml"}))
            jobs = {"10": {"outputs": {"save": "/tmp/final10.root"}},
                    "2": {"outputs": {"save": "/tmp/final2.root"}}}
            index = {"datasets": {"A": {"jobs": jobs}}, "index_id": "source-index"}
            verified = ({"submit_config": str(source / "submit.yaml")}, index)
            with patch("production_downstream._verified_production", return_value=nullcontext(verified)):
                bind_refill(root / "upstream.json", root / "index.json", spec, root / "bound")
            sealed = verify_bundle(root / "bound/bundle")
            self.assertEqual(sealed["metadata"]["refill_index_id"], "source-index")
            self.assertEqual(verify_bundle(source), original)
            bound = json.loads((root / "bound/spec.json").read_text())
            manifest = DatasetManifest.load(bound["dataset_manifest"])
            self.assertEqual(manifest.by_name("A").files, ["/tmp/final2.root", "/tmp/final10.root"])
            self.assertEqual(manifest.lumi, 2)

    def test_fit_needs_cards_and_saved_sources_are_exclusive(self):
        with self.assertRaisesRegex(ValueError, "fit requires datacards"):
            describe_downstream({"2024": {"fit": {}}}, ["2024"])
        with self.assertRaisesRegex(ValueError, "at most one"):
            describe_downstream({"2024": {"manifest": "saved", "refill": "spec"}}, ["2024"])

    def test_saved_manifest_registers_relative_root_file_as_a_dependency(self):
        with TemporaryDirectory() as scratch:
            manifest = Path(scratch) / "manifest.yaml"
            OutputManifest(histograms=HistogramSchema(output_file="products/meta.root")).save_yaml(str(manifest))
            self.assertEqual(saved_manifest_files([str(manifest)]), [str(manifest.parent / "products/meta.root")])

    def test_targets_require_receipts_beside_product_directories(self):
        targets = downstream_targets({"2024": {"datacards": {}, "fit": {}, "fit_diagnostics": {}}},
                                     Path("/tmp/output"))
        self.assertEqual(targets, [f"/tmp/output/2024/downstream/{stage}.json"
                                   for stage in ("merged", "datacards", "fit", "fit_diagnostics")])


class TestFitDiagnostics(unittest.TestCase):
    def test_saved_fit_can_start_diagnostics_without_scheduling_production(self):
        with TemporaryDirectory() as scratch:
            root = Path(scratch)
            products = root / "analytic_fit"
            products.mkdir()
            summary = products / "provenance.json"
            write_json(summary, {})
            receipt = root / "analytic_fit.json"
            write_json(receipt, derived_artifact(kind="analytic_fit", parents={}, configuration={}, files=[summary]))
            settings = {"2024": {"saved_fit": {"directory": products.name, "receipt": receipt.name},
                                 "fit_diagnostics": {}}}
            plans = describe_downstream(settings, ["2024"], base=root)
            self.assertEqual(plans["2024"]["fit_diagnostics"], {"source": "analytic_fit"})
            self.assertEqual(downstream_targets(plans, root / "results"),
                             [str(root / "results/2024/downstream/fit_diagnostics.json")])
            settings["2024"]["saved_fit"]["directory"] = str(root / "not-the-products")
            with self.assertRaisesRegex(ValueError, "receipt does not describe"):
                describe_downstream(settings, ["2024"], base=root)

    def test_selects_single_runnable_fit_and_validates_options(self):
        with TemporaryDirectory() as scratch:
            config = Path(scratch) / "config.yaml"
            config.touch()
            plan = {
                "datacards": {"config": str(config)}, "fit": {},
                "analytic_fit": {"config": str(config), "skip_fit": True}, "fit_diagnostics": {},
            }
            self.assertEqual(describe_downstream({"2024": plan}, ["2024"])["2024"]["fit_diagnostics"],
                             {"source": "fit"})
            plan["analytic_fit"]["skip_fit"] = False
            with self.assertRaisesRegex(ValueError, "must select"):
                describe_downstream({"2024": plan}, ["2024"])
            plan["fit_diagnostics"] = {"source": "analytic_fit", "required": ["unknown"]}
            with self.assertRaisesRegex(ValueError, "must list diagnostic types"):
                describe_downstream({"2024": plan}, ["2024"])
            plan["fit_diagnostics"]["required"] = ["prefit_postfit"]
            self.assertEqual(describe_downstream({"2024": plan}, ["2024"])["2024"]["fit_diagnostics"],
                             plan["fit_diagnostics"])

    @unittest.skipUnless(find_spec("matplotlib"), "fit diagnostic rendering requires Matplotlib")
    def test_diagnostics_require_verified_fit_and_enforce_required_plots(self):
        with TemporaryDirectory() as scratch:
            root = Path(scratch)
            source = root / "fit"
            destination = root / "fit_diagnostics"
            source.mkdir()
            provenance = source / "provenance.json"
            summary = source / "analysis_fit_results.json"
            write_json(provenance, {"fitting_backend": "analysis"})
            result = {"tag": "signal_region", "status": "ok", "chi2": .5, "ndof": 2,
                      "data_integral": 33., "bkg_total": 30.}
            write_json(summary, [result])
            receipt = root / "fit.json"
            fit = derived_artifact(kind="fit", parents={}, configuration={}, files=[provenance, summary])
            write_json(receipt, fit)

            with self.assertRaisesRegex(RuntimeError, "cannot produce diagnostics.*prefit_postfit"):
                run_consumer("fit_diagnostics", source, destination,
                             {"source": "fit", "required": ["prefit_postfit"]}, source_receipt=receipt)
            self.assertFalse(destination.exists())
            run_consumer("fit_diagnostics", source, destination, {"source": "fit"}, source_receipt=receipt)
            artifact = verify_artifact(destination / "receipt.json")
            self.assertEqual(artifact["parents"]["artifact_id"], fit["artifact_id"])
            report = json.loads((destination / "report.json").read_text())
            case = report["cases"][0]
            image = destination / case["plots"]["fit_summary"]["path"]
            self.assertIn("prefit_postfit", case["unavailable"])
            self.assertTrue(image.read_bytes().startswith(b"\x89PNG"))

            summary.write_text("changed")
            with self.assertRaisesRegex(BundleError, "input missing or changed"):
                run_consumer("fit_diagnostics", source, root / "other", {"source": "fit"},
                             source_receipt=receipt)

    def test_joint_fit_requires_explicit_correlation_scope_and_distinct_cards(self):
        with TemporaryDirectory() as scratch:
            root = Path(scratch)
            downstream = {"2024": {"datacards": {}}, "2025": {"datacards": {}}}
            config = {"cards": {"Run2024": {"era": 2024, "card": "datacard_SR.txt"},
                                "Run2025": {"era": 2025, "card": "datacard_SR.txt"}},
                      "fit_diagnostics": {"required": ["correlations"]}}
            with self.assertRaisesRegex(ValueError, "correlations must be by_name"):
                describe_joint_fit(config, downstream, root)
            config["correlations"] = "by_name"
            plan = describe_joint_fit(config, downstream, root)
            self.assertEqual(plan["cards"]["Run2025"]["receipt"], str(root / "2025/downstream/datacards.json"))
            self.assertEqual(plan["fit_diagnostics"]["source"], "fit")
            self.assertEqual(joint_targets(plan, root), [str(root / ".joint/fit_diagnostics.json")])
            config["cards"]["Run2025"]["era"] = 2024
            with self.assertRaisesRegex(ValueError, "duplicate cards"):
                describe_joint_fit(config, downstream, root)

    def test_joint_card_preserves_mc_statistics_when_using_shared_wildcard(self):
        header = "Combination of A=a.txt B=b.txt\nbin A B\n"
        common = _canonical_combination(header + "A autoMCStats 0 1 1\nB autoMCStats 0 1 1\n")
        self.assertEqual(common, "# " + header + "* autoMCStats 0 1 1\n")
        active_only_in_a = _canonical_combination(header + "A autoMCStats 0 1 1\n")
        self.assertEqual(active_only_in_a, "# " + header + "A autoMCStats 0 1 1\n")


if __name__ == "__main__":
    unittest.main()
