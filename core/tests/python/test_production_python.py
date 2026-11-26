import io
from importlib.machinery import EXTENSION_SUFFIXES
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from production_jobs import ProductionRun
from production_python import inspect_python, materialize_python, stage_python_bindings
from run_bundle import file_digest
from submission_backend import generate_condor_runscript


class PythonWorkerTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.script = self.root / "worker.py"
        self.script.write_text(
            "from pathlib import Path\n"
            "from sys import argv\n"
            "from extras.helper import decorate\n"
            "from env_helper import prefix\n"
            "Path('python-result.txt').write_text(prefix + decorate(Path(argv[1]).read_text()))\n"
        )
        (self.root / "functions.py").write_text("def decorate(text):\n    return text.strip() + '!'\n")
        self.record = inspect_python({
            "entrypoint": "worker.py",
            "modules": {"extras/helper.py": "functions.py"},
            "interpreter": sys.executable,
        }, self.root)
        self.config = self.root / "submit_config.txt"
        self.config.write_text("enableSkim=false\ntreeList=Events\n")
        self.archive = self.root / "python-env.tar.gz"
        self.write_environment("packaged: ")

    def write_environment(self, value):
        content = f"prefix = {value!r}\n".encode()
        with tarfile.open(self.archive, "w:gz") as archive:
            info = tarfile.TarInfo("env_helper.py")
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))

    def test_launcher_reuses_bytes_and_rejects_changes(self):
        first = materialize_python(self.record, self.root / "staged")
        before = first.stat()
        again = materialize_python(self.record, self.root / "staged")
        self.assertEqual(first, again)
        self.assertEqual(before.st_ino, again.stat().st_ino)
        other = materialize_python(self.record, self.root / "elsewhere")
        self.assertEqual(file_digest(first), file_digest(other))
        self.assertTrue(os.access(first, os.X_OK))
        self.script.write_text("print('new')\n")
        with self.assertRaisesRegex(ValueError, "source changed"):
            materialize_python(self.record, self.root / "staged")

    def test_rejects_invalid_launcher_destinations_and_interpreters(self):
        for path in ("__main__.py", "../helper.py", "/helper.py", "helper.so"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "module destination"):
                inspect_python({
                    "entrypoint": "worker.py", "modules": {path: "functions.py"},
                }, self.root)
        for command in ("python3 -I", "bin/python", "python3\nextra"):
            with self.subTest(command=command), self.assertRaisesRegex(ValueError, "interpreter"):
                inspect_python({"entrypoint": "worker.py", "interpreter": command}, self.root)

    def test_materialize_checks_even_unimported_source_syntax(self):
        source = self.root / "unused.py"
        source.write_text("def broken(\n")
        record = inspect_python({
            "entrypoint": self.script.name, "modules": {"unused.py": source.name},
        }, self.root)
        staged = self.root / "staged"
        with self.assertRaisesRegex(ValueError, r"invalid Python source .*unused\.py:1:"):
            materialize_python(record, staged)
        self.assertFalse(staged.exists())

    def test_generated_condor_script_executes_relocated_launcher(self):
        local = materialize_python(self.record, self.root / "controller")
        worker = self.root / "worker"
        worker.mkdir()
        shutil.copy2(local, worker)
        shutil.copy2(self.archive, worker)
        shutil.copy2(self.config, worker)
        from submission_backend import _package_worker_yaml
        _package_worker_yaml(worker)
        self.script.unlink()
        (self.root / "functions.py").unlink()
        wrapper = worker / "condor_runscript.sh"
        wrapper.write_text(generate_condor_runscript(
            local.name, stage_inputs=False, stage_outputs=False, root_setup="",
            python_env_tarball=str(self.archive), runtime_config_relpath="cfg/submit_config.txt",
            site_ranking_file="none", config_file="submit_config.txt",
        ))
        result = subprocess.run(["bash", str(wrapper)], cwd=worker, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        observed = (worker / "python-result.txt").read_text()
        self.assertTrue(observed.startswith("packaged: ") and observed.endswith("!"))
        import yaml
        self.assertEqual(yaml.safe_load(observed[len("packaged: "):-1]), {"enableSkim": "false", "treeList": "Events"})
        self.assertEqual((worker / "submit_config.txt").read_bytes(), self.config.read_bytes())

    def test_generated_condor_script_requires_requested_environment(self):
        worker = self.root / "worker"
        executable = materialize_python(self.record, worker)
        shutil.copy2(self.config, worker)
        wrapper = worker / "condor_runscript.sh"
        wrapper.write_text(generate_condor_runscript(
            executable.name, stage_inputs=False, stage_outputs=False, root_setup="",
            python_env_tarball=str(self.archive), site_ranking_file="none", config_file="submit_config.txt",
        ))
        result = subprocess.run(["bash", str(wrapper)], cwd=worker, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(f"Python environment tarball {self.archive.name} not found", result.stderr)
        self.assertFalse((worker / "python-result.txt").exists())

    def test_stage_bindings_includes_matching_api_and_native_dependencies(self):
        repo = self.root / "repository"
        build = self.root / "configured-project"
        modules = build / "python"
        modules.mkdir(parents=True)
        native = build / "libNative.so.2.1"
        native.write_bytes(b"native dependency")
        binding = modules / f"rdfanalyzer{EXTENSION_SUFFIXES[0]}"
        binding.write_bytes(b"extension matching the interpreter ABI")
        (modules / "rdfanalysis.py").write_text("# Python authoring API\n")
        (modules / "unrelated.py").write_text("# external module\n")
        (modules / "rdfkinematics.py").write_text("# test fixture\n")
        include = build / "include" / "rdfanalyzer"
        include.mkdir(parents=True)
        for name in ("AnalysisMath.h", "ArrayValidation.h"):
            (include / name).write_text(name)
        with patch("ingestion_utils.collect_local_shared_libs", return_value={
            "libNative.so.2": str(native),
        }) as collect:
            staged = self.root / "packages"
            self.assertEqual(stage_python_bindings(modules, staged, repo), binding)
        collect.assert_called_once_with(str(binding), str(build))
        self.assertEqual((staged / "libNative.so.2").read_bytes(), native.read_bytes())
        self.assertEqual(set(p.name for p in staged.iterdir()), {
            binding.name, "rdfanalysis.py", "libNative.so.2", "rdfkinematics.py", "AnalysisMath.h", "ArrayValidation.h",
        })

    def test_local_preflight_uses_archive_and_invalidates_changed_environment(self):
        executable = materialize_python(self.record, self.root / "controller")
        inputs = self.root / "input.root"
        inputs.write_bytes(b"fixture")
        manifest = self.root / "datasets.yaml"
        manifest.write_text(
            f"lumi: 1.0\ndatasets:\n- name: fixture\n  files: ['{inputs}']\n  dtype: data\n"
        )
        run = ProductionRun(
            name="python-local-preflight", exe=str(executable), workspace=str(self.root),
            submit_config=str(self.config), dataset_manifest=str(manifest), python_env=str(self.archive),
        )
        run.smoke()
        result = Path(run._test_job_dir) / "python-result.txt"
        self.assertTrue(result.read_text().startswith("packaged: "))
        self.assertTrue(run.smoke_complete())
        self.assertEqual(list(Path(run._test_job_dir).glob(".preflight-python-*")), [])
        self.write_environment("new: ")
        self.assertFalse(run.smoke_complete())

    def test_submission_requires_the_same_staged_environment_as_preparation(self):
        executable = materialize_python(self.record, self.root / "controller")
        inputs = self.root / "input.root"
        inputs.write_bytes(b"fixture")
        manifest = self.root / "datasets.yaml"
        manifest.write_text(
            f"lumi: 1.0\ndatasets:\n- name: fixture\n  files: ['{inputs}']\n  dtype: data\n"
        )
        params = {
            "name": "python-env-submission", "exe": str(executable), "partition": "file",
            "submit_config": str(self.config), "dataset_manifest": str(manifest),
            "python_env": str(self.archive),
        }
        run = ProductionRun(**params, workspace=str(self.root))
        run.prepare()
        self.assertTrue(run.preparation_complete())
        run.build_submission()
        self.assertTrue(run.submission_manifest_path.is_file())
        self.write_environment("new: ")
        self.assertFalse(run.preparation_complete())
        with self.assertRaisesRegex(RuntimeError, "Staged Python environment is missing or out of date"):
            run.build_submission()


if __name__ == "__main__":
    unittest.main()
