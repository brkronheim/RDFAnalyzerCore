"""Run standalone DAGs against completed, local products without a scheduler."""

import ast
import json
from importlib.util import find_spec
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

import yaml

from rdfproduction.campaign import describe_campaign
from rdfproduction.downstream import card_shapes, describe_downstream, downstream_targets, run_consumer
from run_bundle import compile_config_bundle, derived_artifact, seal_bundle, verify_artifact, write_json


REPO = Path(__file__).resolve().parents[3]
SNAKEMAKE = shutil.which("snakemake")
INSTALL = os.environ.get("RDF_SNAKEMAKE_TEST_PREFIX")
WORKFLOWS = Path(INSTALL) / "share/rdfanalyzer/snakemake" if INSTALL else REPO / "examples/snakemake"
MODULES = Path(INSTALL) / "python" if INSTALL else REPO / "core/python"


class TestWorkflowInputs(unittest.TestCase):
    def test_campaign_rejects_unknown_and_quoted_boolean(self):
        for config, expected in [({"verify_storag": True}, "verify_storag"),
                                 ({"verify_storage": "false"}, "boolean")]:
            with self.subTest(config=config), self.assertRaisesRegex(ValueError, expected):
                describe_campaign(config)

    def test_manifest_plot_accepts_yaml_requests(self):
        from rdfproduction.manifest_plotting import ManifestPlotter
        with TemporaryDirectory() as directory:
            cfg = Path(directory) / "plots.yaml"
            cfg.write_text("- title: Example\n  logY: false\n")
            plot = ManifestPlotter(name="plots", manifest_path="merged.yaml", output_dir=directory, plot_config=str(cfg))
            self.assertEqual(plot._load_plot_configs(), [{"title": "Example", "logY": False}])

    def test_fit_rejects_unrecorded_sibling_card_before_running(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            cards = root / "cards"
            cards.mkdir()
            card = cards / "signed.txt"
            card.write_text("imax 1\n")
            receipt = root / "cards.json"
            write_json(receipt, derived_artifact(kind="datacards", parents={}, configuration={}, files=[card]))
            (cards / "extra.txt").write_text("imax 2\n")
            with self.assertRaisesRegex(ValueError, "extra.txt"):
                run_consumer("fit", cards, root / "fit", {}, source_receipt=receipt)
            self.assertFalse((root / "fit").exists())

    def test_direct_card_tracks_shapes_and_omits_merge(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.root").touch()
            card = root / "sr.txt"
            card.write_text("shapes * * model.root x\nshapes x * FAKE\n")
            cfg = describe_downstream({"sr": {"input_card": "sr.txt", "fit": {}}}, ["sr"], base=root)
            self.assertEqual(card_shapes(card), [str(root / "model.root")])
            self.assertEqual(downstream_targets(cfg, root), [str(root / "sr/downstream/fit.json")])
            (root / "model.root").unlink()
            with self.assertRaisesRegex(ValueError, "Missing local shapes"):
                card_shapes(card)


@unittest.skipUnless(INSTALL, "set RDF_SNAKEMAKE_TEST_PREFIX to a relocated installation")
class TestWorkflowInstallation(unittest.TestCase):
    def test_rules_fixtures_and_local_python_imports_are_installed(self):
        source = REPO / "examples/snakemake"
        for pattern in ("*Snakefile", "*.smk", "fixtures/**/*"):
            for path in source.glob(pattern):
                if path.is_file():
                    installed = WORKFLOWS / path.relative_to(source)
                    self.assertTrue(installed.is_file(), installed)
                    self.assertEqual(path.read_bytes(), installed.read_bytes(), installed)
        source_python = REPO / "core/python"
        for package in ("rdfproduction", "statistical_inference"):
            for script in (source_python / package).rglob("*.py"):
                installed = MODULES / script.relative_to(source_python)
                self.assertTrue(installed.is_file(), installed)
                self.assertEqual(script.read_bytes(), installed.read_bytes(), installed)
        for name in ("requirements-workflow.txt", "requirements-snakemake.txt"):
            self.assertEqual((REPO / name).read_bytes(), (WORKFLOWS.parent / name).read_bytes())
        for script in sorted(MODULES.rglob("*.py")):
            for node in ast.walk(ast.parse(script.read_text())):
                names = []
                if isinstance(node, ast.Import):
                    names = [item.name for item in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                    names = [node.module]
                for name in names:
                    base = name.split(".")[0] + ".py"
                    if (source_python / base).is_file():
                        self.assertTrue((MODULES / base).is_file(), f"{script.name} requires {base}")
        # Legacy sample records use a lazy import. Exercise that path outside
        # the checkout so an incomplete controller install fails here.
        with TemporaryDirectory() as directory:
            (Path(directory) / "samples.txt").write_text("name=Example type=7 xsec=1\n")
            code = ("from native_config_io import read_legacy_aux; "
                    "assert read_legacy_aux('samples.txt', 'samples')['datasets'][0]['name'] == 'Example'")
            subprocess.run([sys.executable, "-c", code], cwd=directory, check=True,
                           env={**os.environ, "PYTHONPATH": str(MODULES)})
        archiver = MODULES / "setup_python_env.sh"
        subprocess.run([str(archiver), "--help"], check=True, stdout=subprocess.DEVNULL)
        # The sizing pilot's source is embedded on demand by the installed submit writer.
        from submission_backend import generate_condor_runscript
        self.assertIn("def run_measurement(", generate_condor_runscript(
            "worker", False, False, "", sizing_pilot_entries=100))
        self.assertFalse((MODULES / "law").exists())


@unittest.skipUnless(SNAKEMAKE, "snakemake executable unavailable")
class TestStandaloneWorkflow(unittest.TestCase):
    def run_workflow(self, root, config, *targets, snakefile="DownstreamSnakefile", cores=1):
        (root / "workflow.yaml").write_text(yaml.safe_dump(config))
        command = [SNAKEMAKE, "--snakefile", str(WORKFLOWS / snakefile),
                   "--cores", str(cores), "--configfile", "workflow.yaml", "--", *targets]
        # A relocated installation must work without falling back to source
        # modules. Keep third-party packages and the wrapper interpreter.
        paths = [Path(p).resolve() for p in sys.path if p]
        if INSTALL:
            interpreter_prefix = Path(sys.prefix).resolve()
            paths = [p for p in paths if not p.is_relative_to(REPO) or p.is_relative_to(interpreter_prefix)]
        env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(MODULES), *map(str, paths)])}
        result = subprocess.run(command, cwd=root, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_plot_dag_recreates_a_missing_declared_image(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "hist.root").write_text("test input")
            (root / "plot.yaml").write_text(yaml.safe_dump({"schema_version": 1, "plots": [
                {"metaFile": "hist.root", "outputFile": "main.png", "additionalOutputs": ["main.pdf"],
                 "processes": [{"histogramName": "mass"}]}]}))
            renderer = root / "render"
            renderer.write_text(f"#!{sys.executable}\n" + """from pathlib import Path
import sys
for product in ["main.png", "main.pdf", sys.argv[sys.argv.index("--report") + 1]]:
    path = Path(product)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("rendered")
""")
            renderer.chmod(0o755)
            config = {"plotting": {"executable": str(renderer), "configs": {"era": "plot.yaml"}}}
            self.run_workflow(root, config, "plots")
            (root / "main.pdf").unlink()
            self.run_workflow(root, config, "plots")
            self.assertTrue((root / "main.pdf").is_file())
            self.assertFalse((root / "results/era/merged").exists())

    @unittest.skipUnless(os.environ.get("RDF_SNAKEMAKE_TEST_NATIVE_BDT"),
                         "requires matching installed authoring bindings, BDT plugin and ROOT runtime")
    def test_native_bdt_runs_directly_on_a_saved_skim(self):
        import numpy as np
        import uproot
        from rdfproduction.output_index import load_verified_output_index

        with TemporaryDirectory() as directory:
            root = Path(directory)
            saved = root / "saved_skim.root"
            with uproot.recreate(saved) as stream:
                stream.mktree("Events", {"x": np.array([0.45, 0.55, 0.85])})
            source = root / "model-config"
            source.mkdir()
            shutil.copy(REPO / "examples/systematics-inference/toy_bdt.txt", source)
            (source / "models.yaml").write_text(yaml.safe_dump([
                {"file": "toy_bdt.txt", "name": "score", "inputVariables": "ml_feature", "runVar": "run_model"}]))
            (source / "datasets.yaml").write_text(yaml.safe_dump(
                {"datasets": [{"name": "sample", "dtype": "data", "files": [str(saved)]}]}))
            template = source / "analysis.yaml"
            template.write_text(yaml.safe_dump({"enableSkim": False, "bdtConfig": "models.yaml",
                                                 "sampleConfig": "datasets.yaml"}))
            compile_config_bundle(template, root / "bundle", asset_keys=("sampleConfig", "bdtConfig"))
            shutil.rmtree(source)
            shutil.copy(Path(__file__).parent / "fixtures/native_bdt_worker.py", root / "worker.py")
            spec = {"version": 1, "name": "bdt", "backend": "local", "state_root": "state",
                    "python": {"entrypoint": "worker.py", "interpreter": sys.executable},
                    "submit_config": "bundle/submit_config.yaml", "dataset_manifest": "bundle/dataset_manifest.yaml",
                    "options": {"make_test_job": False, "partition": "file_group"}}
            (root / "worker.yaml").write_text(yaml.safe_dump(spec))
            self.run_workflow(root, {"eras": {"saved": "worker.yaml"}}, "production", snakefile="Snakefile")
            index = load_verified_output_index(root / "results/saved/output_index.json")
            job, = index["datasets"]["sample"]["jobs"].values()
            with uproot.open(job["outputs"]["meta"]) as output:
                self.assertEqual(output["python_histograms/native_score"].values().tolist(), [2.0, 1.0])

    def test_two_era_local_stitching_uses_era_specific_corrections(self):
        import numpy as np
        import uproot
        from rdfproduction.output_index import load_verified_output_index

        with TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(Path(__file__).parent / "fixtures/stitch_worker.py", root / "worker.py")
            config = {"eras": {}, "stitching": {"source": {"eras": {}}, "configs": {},
                                                "require_stitched_histograms": True}}
            expected = {}
            for era, inclusive in [("early", [0, 0, 1]), ("late", [0, 1, 1, 1])]:
                bundle = root / era
                bundle.mkdir()
                datasets = []
                for name, bins in [("inclusive", inclusive), ("exclusive", [1, 1])]:
                    source = root / f"{era}-{name}.root"
                    with uproot.recreate(source) as stream:
                        stream["Events"] = {"bin": np.array(bins)}
                    datasets.append({"name": name, "dtype": "mc", "xsec": 10.0, "files": [str(source)]})
                (bundle / "analysis.yaml").write_text("enableSkim: false\n")
                (bundle / "datasets.yaml").write_text(yaml.safe_dump({"lumi": 1, "datasets": datasets}))
                seal_bundle(bundle)
                stitch_yaml = root / f"{era}-groups.yaml"
                stitch_yaml.write_text(yaml.safe_dump({"groups": {"toy": {
                    "samples": ["inclusive", "exclusive"], "inclusive_sample": "inclusive",
                    "inclusive_cross_section_pb": 10.0}}, "validation": {"histograms": [{"name": "observable"}]}}))
                config["stitching"]["configs"][era] = str(stitch_yaml)
                for phase in ("counters", "apply"):
                    name = f"{era}-{phase}"
                    spec = {"version": 1, "name": name, "backend": "local", "state_root": "state",
                            "python": {"entrypoint": "worker.py", "interpreter": sys.executable},
                            "submit_config": f"{era}/analysis.yaml", "dataset_manifest": f"{era}/datasets.yaml",
                            "options": {"file_source": "manifest", "partition": "file_group", "make_test_job": False}}
                    owners = config["eras"]
                    if phase == "counters":
                        spec["options"]["stitch_mode"] = "none"
                        owners = config["stitching"]["source"]["eras"]
                    specfile = root / (name + ".yaml")
                    specfile.write_text(yaml.safe_dump(spec))
                    owners[era] = str(specfile)
                count = inclusive.count(1)
                expected[era] = count * count / (count + 2)
            targets = ["stitch"]
            if find_spec("ROOT") and shutil.which("hadd"):
                targets.append("merge")
            self.run_workflow(root, config, *targets, snakefile="Snakefile", cores=2)
            mtimes = []
            for era, yield_bin in expected.items():
                out = root / "results" / era
                validation = out / "stitch/stitch_validation.json"
                self.assertTrue(validation.is_file())
                mtimes.append((validation, validation.stat().st_mtime_ns))
                index = load_verified_output_index(out / "output_index.json")
                self.assertEqual(index["partition_count"], 2)
                job, = index["datasets"]["inclusive"]["jobs"].values()
                with uproot.open(job["outputs"]["meta"]) as stream:
                    self.assertAlmostEqual(stream["observable_stitched"].values()[1], yield_bin)
                if "merge" in targets:
                    self.assertTrue((out / "merged/merge_receipt.json").is_file())
            self.run_workflow(root, config, *targets, snakefile="Snakefile", cores=2)
            self.assertEqual([p.stat().st_mtime_ns for p, _ in mtimes], [t for _, t in mtimes])

    def test_nonempty_local_skim_then_refill_in_a_new_workflow(self):
        import numpy as np
        import uproot
        from rdfproduction.output_index import load_verified_output_index

        with TemporaryDirectory() as directory:
            root = Path(directory)
            with uproot.recreate(root / "events.root") as stream:
                stream["Events"] = {"mass": np.array([0.5, 1.5])}
            worker = root / "worker.py"
            worker.write_text("""import json
import sys
from pathlib import Path
import numpy as np
import uproot
import yaml
cfg = yaml.safe_load(Path(sys.argv[1]).read_text())
source = cfg['fileList']
if isinstance(source, str):
    source = source.split(',')
with uproot.open(source[0]) as stream:
    mass = stream['Events']['mass'].array(library='np')
if str(cfg.get('enableSkim')).lower() == 'true':
    with uproot.recreate(cfg['saveFile']) as stream:
        stream['Events'] = {'mass': mass}
with uproot.recreate(cfg['metaFile']) as stream:
    stream['mass'] = np.histogram(mass, bins=[0, 1, 2])
    stream['__rdf_job_metadata_v1'] = json.dumps({
        'schema_version': 1, 'format': 'rdf-job-metadata',
        'provenance': {'job.logical_id': '0', 'service.counter.sample': cfg['sample'],
                       'framework.git_hash': 'fixture', 'config.hash': 'fixture'},
        'regions': {}, 'normalizations': {}, 'weight_audits': {}})
""")
            for name in ("skim", "refill"):
                bundle = root / name
                bundle.mkdir()
                (bundle / "analysis.yaml").write_text(yaml.safe_dump({"enableSkim": name == "skim"}))
                (bundle / "datasets.yaml").write_text(yaml.safe_dump({"lumi": 1, "datasets": [
                    {"name": "sample", "dtype": "data", "files": [str(root / "events.root")]}]}))
                seal_bundle(bundle)
                specification = {"version": 1, "name": name, "backend": "local", "state_root": "state",
                                 "python": {"entrypoint": "worker.py", "interpreter": sys.executable},
                                 "submit_config": f"{name}/analysis.yaml", "dataset_manifest": f"{name}/datasets.yaml",
                                 "options": {"partition": "file_group", "make_test_job": False}}
                (root / f"{name}.yaml").write_text(yaml.safe_dump(specification))
            self.run_workflow(root, {"eras": {"era": "skim.yaml"}, "output_root": "original"},
                              "production", snakefile="Snakefile")
            index = root / "original/era/output_index.json"
            source = load_verified_output_index(index)
            skim, = source["datasets"]["sample"]["jobs"].values()
            with uproot.open(skim["outputs"]["save"]) as stream:
                self.assertEqual(stream["Events"].num_entries, 2)
            # The independent refill has no raw-input fallback.
            (root / "events.root").unlink()
            standalone = {"output_root": "restarted", "downstream": {"era": {
                "saved_production": {"spec": "skim.yaml", "index": str(index)}, "refill": "refill.yaml"}}}
            self.run_workflow(root, standalone, "refill")
            filled = load_verified_output_index(root / "restarted/era/downstream/refill_index.json")
            output, = filled["datasets"]["sample"]["jobs"].values()
            with uproot.open(output["outputs"]["meta"]) as stream:
                self.assertEqual(stream["mass"].values().tolist(), [1.0, 1.0])
            if find_spec("ROOT") and shutil.which("hadd"):
                # Exercise the real metadata merger where the ROOT runtime is available.
                restart = {"output_root": "merged-only", "merge_indexes": {"era": str(index)}}
                self.run_workflow(root, restart, "merge")
                out = root / "merged-only/era/merged"
                self.assertTrue((out / "merge_receipt.json").is_file())
                with uproot.open(out / "sample_partial.root") as stream:
                    self.assertEqual(stream["mass"].values().tolist(), [1.0, 1.0])

    def test_raw_histograms_to_cards_then_existing_card_to_fit(self):
        import numpy as np
        import uproot

        with TemporaryDirectory() as directory:
            root = Path(directory)
            hist = root / "hist.root"
            with uproot.recreate(hist) as stream:
                stream["mass"] = (np.array([2., 3.]), np.array([0., 1., 2.]))
            cards = {"input_files": {"bkg": {"path": str(hist)}, "data_obs": {"path": str(hist)}},
                     "processes": {"bkg": {"samples": ["bkg"]}},
                     "control_regions": {"SR": {"observable": "mass", "processes": ["bkg"],
                                               "data_process": "data_obs"}}}
            (root / "cards.yaml").write_text(yaml.safe_dump(cards))
            config = {"output_root": "from-histograms", "downstream": {
                "SR": {"datacards": {"config": "cards.yaml", "raw_inputs": True}}}}
            self.run_workflow(root, config, "datacards")
            base = root / "from-histograms/SR/downstream"
            receipt = verify_artifact(base / "datacards.json")
            card, = (base / "datacards").glob("*.txt")
            self.assertIn(str(card), receipt["files"])
            self.assertFalse((base / "merged").exists())

            config = {"output_root": "from-card", "downstream": {"SR": {
                "input_card": str(card), "fit": {"fitting_backend": "analysis"}}}}
            self.run_workflow(root, config, "fits")
            fit = root / "from-card/SR/downstream"
            verify_artifact(fit / "fit.json")
            results = json.loads((fit / "fit/analysis_fit_results.json").read_text())
            self.assertEqual(results[0]["status"], "ok")
            self.assertFalse((fit / "datacards").exists())
            mtime = (fit / "fit.json").stat().st_mtime_ns
            self.run_workflow(root, config, "fits")
            self.assertEqual((fit / "fit.json").stat().st_mtime_ns, mtime)
