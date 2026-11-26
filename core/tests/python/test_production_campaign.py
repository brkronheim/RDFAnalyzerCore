import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from production_campaign import (
    campaign_status, describe_campaign, format_campaign_status, inspect_campaign, preflight_campaign,
    publish_plan,
)
from production_stitching import bind_application, describe_stitching
from rdf_production import inspect_spec
from run_bundle import derived_artifact, seal_bundle, seal_collection, write_json


class TestProductionCampaign(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name)
        self.collection = self.work / "collection"
        for era in ("2024", "2025"):
            bundle = self.collection / era
            bundle.mkdir(parents=True)
            (bundle / "cfg.yaml").write_text("enableSkim: false\n")
            (bundle / "datasets.yaml").write_text("datasets: []\n")
            seal_bundle(bundle, metadata={"era": era},
                        entrypoints={"submit_config": "cfg.yaml", "datasets": "datasets.yaml"})
        seal_collection(self.collection, {"2024": "2024", "2025": "2025"})

    def _explicit_spec(self, era, *, name=None, output=None):
        bundle = self.collection / era
        spec = self.work / f"{era}.json"
        spec.write_text(json.dumps({
            "name": name or f"run_{era}", "version": 1, "exe": sys.executable,
            "submit_config": str(bundle / "cfg.yaml"), "dataset_manifest": str(bundle / "datasets.yaml"),
            "options": {"output_directory": output or f"root://example.invalid//campaign/{era}"}
        }))
        return spec.name

    def test_collection_plan_materializes_selected_isolated_specs(self):
        config = {
            "collection": self.collection.name,
            "select_eras": [2025, "2024"],
            "production": {"name": "production", "exe": sys.executable,
                           "options": {"output_directory": "/eos/test/runs/"}},
        }
        campaign = describe_campaign(config, base=self.work)
        plan = inspect_campaign(campaign)
        self.assertFalse(campaign["output_root"].exists())
        self.assertEqual(list(plan["eras"]), ["2025", "2024"])
        for era, record in plan["eras"].items():
            settings = record["inspection"]["task_parameters"]
            self.assertEqual(settings["name"], f"production_{era}")
            self.assertEqual(settings["output_directory"], f"/eos/test/runs/{era}")
        publish_plan(campaign)
        self.assertEqual(json.loads(campaign["plan_path"].read_text()), plan)
        for era, path in describe_campaign(config, base=self.work)["generated"].items():
            self.assertEqual(path, campaign["generated"][era])
            self.assertEqual(json.loads(path.read_text())["name"], f"production_{era}")

    def test_all_eras_checked_for_name_and_destination_collisions(self):
        era_b = self._explicit_spec("2025", name="shared", output="/eos/test/runs/2025/subdir")
        era_a = self._explicit_spec("2024", name="shared", output="root://eosuser.cern.ch:1094//eos/test/runs/2025")
        config = {"eras": {2024: era_a, "2025": era_b}}
        with self.assertRaisesRegex(ValueError, "duplicate run name"):
            inspect_campaign(describe_campaign(config, base=self.work))
        self._explicit_spec("2025", output="/eos/test/runs/2025/subdir")
        with self.assertRaisesRegex(ValueError, "overlapping output destinations"):
            inspect_campaign(describe_campaign(config, base=self.work))

    def test_era_options_override_shared_options_for_selected_children(self):
        config = {
            "collection": self.collection.name, "select_eras": ["2025"],
            "production": {"name": "production", "exe": sys.executable, "options": {
                "output_directory": "root://shared.invalid//results", "files_per_job": 20, "request_memory": 1024,
            }},
            "era_options": {
                2024: {"request_memory": 4096},
                2025: {"output_directory": "root://other.invalid//different-site/results", "request_memory": 2048},
            },
        }
        campaign = describe_campaign(config, base=self.work)
        records = inspect_campaign(campaign)["eras"]
        self.assertEqual(list(records), ["2025"])
        options = records["2025"]["inspection"]["task_parameters"]
        self.assertEqual(options["request_memory"], 2048)
        self.assertEqual(options["files_per_job"], 20)
        self.assertEqual(options["output_directory"], "root://other.invalid//different-site/results")
        del config["select_eras"]
        config["era_options"][2025]["output_directory"] = "root://shared.invalid//results/2024"
        with self.assertRaisesRegex(ValueError, "overlapping output destinations"):
            inspect_campaign(describe_campaign(config, base=self.work))
        config["era_options"]["unknown"] = {"request_memory": 10}
        with self.assertRaisesRegex(ValueError, "distinct members of the collection"):
            describe_campaign(config, base=self.work)

    def test_preflight_checks_selected_eras_together_before_publishing(self):
        specs = {era: self._explicit_spec(era) for era in ("2024", "2025")}
        campaign = describe_campaign({"eras": specs}, base=self.work)
        publish_plan(campaign)
        log = self.work / "preflight.log"
        failure = subprocess.CalledProcessError(1, ["python", "-m", "rdf_production"])
        with patch("production_campaign.subprocess.run", side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError):
                preflight_campaign(campaign, python=sys.executable, log=log)
        self.assertFalse(campaign["preflight_path"].exists())
        with patch("production_campaign.subprocess.run") as run:
            preflight_campaign(campaign, python=sys.executable, log=log)
        run.assert_called_once()
        command = run.call_args.args[0]
        self.assertEqual(command[1:4], ["-m", "rdf_production", "preflight"])
        self.assertEqual(command[4:], [str(self.work / path) for path in specs.values()])
        self.assertEqual(json.loads(campaign["preflight_path"].read_text())["eras"], ["2024", "2025"])

    def test_status_reports_artifacts_and_missing_or_unclaimed_runs(self):
        specs = {era: self._explicit_spec(era) for era in ("2024", "2025")}
        campaign = describe_campaign({"eras": specs}, base=self.work)
        spec = self.work / specs["2024"]
        settings = json.loads(spec.read_text())
        settings["exe"] = str(self.work / "unavailable-executable")
        spec.write_text(json.dumps(settings))
        run = self.work / "skimRun_run_2024"
        run.mkdir()
        with patch("rdf_production._WORKSPACE", self.work):
            unstarted = campaign_status(campaign)["eras"]
            self.assertEqual(unstarted["2024"]["state"], "unclaimed")
            self.assertEqual(unstarted["2025"]["state"], "missing")
            (run / "rdf_production.json").write_text('{"spec_id":"recorded-run"}')
            receipt = campaign["output_root"] / "2024" / "merged" / "merge_receipt.json"
            receipt.parent.mkdir(parents=True)
            receipt.write_text("{}")
            complete = campaign_status(campaign)["eras"]["2024"]
        self.assertEqual(complete["state"], "recorded")
        self.assertEqual(complete["run"]["spec_id"], "recorded-run")
        self.assertEqual(complete["artifacts"]["merge_receipt"], {"path": str(receipt), "present": True})

    def test_stitching_binds_weights_only_after_counters_and_isolates_both_passes(self):
        app_path = self.work / self._explicit_spec("2024")
        app_doc = json.loads(app_path.read_text())
        app_doc["backend"] = "local"
        app_doc["options"].pop("output_directory")
        app_path.write_text(json.dumps(app_doc))
        counter_path = self.work / "counter.json"
        counter_doc = {**app_doc, "name": "counter_2024", "options": {"stitch_mode": "none"}}
        counter_path.write_text(json.dumps(counter_doc))
        config_path = self.work / "stitch.yaml"
        config_path.write_text("groups:\n  family:\n    samples: [inclusive, exclusive]\n")
        campaign = describe_campaign({"eras": {"2024": app_path.name}}, base=self.work)
        application = inspect_campaign(campaign)
        settings = {"source": {"eras": {"2024": str(counter_path)}}, "configs": {2024: config_path.name}}
        plan = describe_stitching(settings, campaign, application, base=self.work)
        self.assertEqual(plan["source_plan"]["eras"]["2024"]["inspection"]["backend"], "local")
        self.assertFalse(campaign["output_root"].exists())

        weights = self.work / "weights.json"
        effective = self.work / "derived" / "application.json"
        with self.assertRaises(FileNotFoundError):
            bind_application(app_path, weights, effective)
        weights.write_text('{"schema_version": 2}')
        bind_application(app_path, weights, effective)
        result = inspect_spec(effective)
        self.assertEqual(result["task_parameters"]["stitch_mode"], "correctionlib")
        self.assertEqual(result["task_parameters"]["stitch_weights"], str(weights))
        self.assertIn("stitch_weights", result["file_hashes"])

        counter_doc["name"] = app_doc["name"]
        counter_path.write_text(json.dumps(counter_doc))
        with self.assertRaisesRegex(ValueError, "duplicate run name"):
            describe_stitching(settings, campaign, application, base=self.work)

    def test_status_verifies_downstream_products_and_locates_incomplete_refill(self):
        output = self.work / "results"
        settings = {"eras": {"2024": self._explicit_spec("2024")},
                    "downstream": {"2024": {"refill": "refill.json", "fit_diagnostics": {}}}}
        campaign = describe_campaign(settings, base=self.work)
        era = output / "2024"
        log = era / "logs" / "refill.log"
        log.parent.mkdir(parents=True)
        log.write_text("running refill\n")
        destination = era / "downstream"
        destination.mkdir()
        report = destination / "report.json"
        report.write_text("{}")
        receipt = destination / "fit_diagnostics.json"
        artifact = derived_artifact(kind="fit_diagnostics", parents={"fit": "fixture"},
                                    configuration={}, files=[report])
        write_json(receipt, artifact)

        status = campaign_status(campaign, settings=settings, verify=True)
        stages = status["eras"]["2024"]["stages"]
        self.assertEqual(stages["refill"]["state"], "incomplete")
        self.assertEqual(stages["refill"]["log"], {"path": str(log), "present": True})
        self.assertEqual(stages["fit_diagnostics"]["state"], "verified")
        self.assertEqual(stages["fit_diagnostics"]["artifact_id"], artifact["artifact_id"])
        self.assertIn(f"refill  {log}", format_campaign_status(status))
        report.write_text('{"changed":true}')
        recorded = campaign_status(campaign, settings=settings)["eras"]["2024"]["stages"]
        self.assertEqual(recorded["fit_diagnostics"]["state"], "recorded")
        verified = campaign_status(campaign, settings=settings, verify=True)["eras"]["2024"]["stages"]
        self.assertEqual(verified["fit_diagnostics"]["state"], "invalid")
        self.assertIn("missing or changed", verified["fit_diagnostics"]["error"])


if __name__ == "__main__":
    unittest.main()
