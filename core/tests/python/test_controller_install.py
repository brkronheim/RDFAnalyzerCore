"""Language-free install, relocation and IDE contracts without native execution."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[3]


@unittest.skipUnless(shutil.which('cmake'), 'CMake installation tests')
class ControllerInstallTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='rdf controller install ')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.directory = Path(cls.temporary.name)
        cls.build = cls.directory/'controller build'
        cls.prefix = cls.directory/'installed prefix'
        cls.env = dict(os.environ, CC='/unavailable-compiler', CXX='/unavailable-compiler')
        cls.cmake = shutil.which('cmake')
        cls.configure = subprocess.run([cls.cmake, '-S', str(REPO/'core/python/controller'),
            '-B', str(cls.build)], check=True, capture_output=True, text=True, env=cls.env)
        cls.install = subprocess.run([cls.cmake, '--install', str(cls.build),
            '--prefix', str(cls.prefix), '--component', 'PythonController'],
            check=True, capture_output=True, text=True, env=cls.env)

    def child(self, code, *, prefix=None):
        return subprocess.run([sys.executable, '-S', '-c', code],
            env=dict(os.environ, PYTHONPATH=str((prefix or self.prefix)/'python')),
            cwd=self.directory, capture_output=True, text=True, check=True)

    def test_only_controller_closure_and_stubs_are_installed_without_compiler(self):
        cache = (self.build/'CMakeCache.txt').read_text()
        self.assertNotIn('CMAKE_CXX_COMPILER:', cache)
        self.assertNotIn('CMAKE_C_COMPILER:', cache)
        expected = {'_rdfjson.py', 'rdfworkflow.py', 'rdfworkflow.pyi', 'rdfresults.py', 'rdfresults.pyi',
            'rdfschema.py', 'rdfschema.pyi',
            'run_bundle.py', 'run_collection.py'}
        self.assertEqual({p.name for p in (self.prefix/'python').iterdir()
            if p.is_file()}, expected)
        for name in expected:
            self.assertEqual((self.prefix/'python'/name).read_bytes(), (REPO/'core/python'/name).read_bytes())
        self.assertFalse(list(self.prefix.rglob('*.so')))
        self.assertFalse(list(self.prefix.rglob('*.a')))
        self.assertTrue((self.prefix/'share/rdfanalyzer/examples/compiled-controller/steer.py').is_file())

    def test_installed_cli_and_result_imports_need_no_checkout_or_event_runtime(self):
        code = '''
import json,sys
from pathlib import Path
import rdfworkflow,rdfresults,rdfschema,run_bundle,run_collection
prefix=Path(sys.path[1]).resolve()
assert all(Path(m.__file__).parent.resolve()==prefix for m in (rdfworkflow,rdfresults,rdfschema,run_bundle,run_collection))
spec=rdfworkflow.CppAnalysisSpec('not-built')
assert spec.validate().passed and not spec.validate('assets').passed
assert rdfresults.RunResult('test','failed','test scope').success is False
document=rdfresults.RunResult('native','succeeded','caller reported').as_dict()
assert rdfresults.RunResult.from_dict(document).as_dict()==document
assert rdfschema.compare_schema([rdfschema.ColumnRequirement('event')],{'event':'ULong64_t'}).passed
assert not {'ROOT','rdfanalyzer','rdfanalysis','numpy','numba','yaml'}.intersection(sys.modules)
print('installed closure verified')
'''
        self.assertEqual(self.child(code).stdout.strip(), 'installed closure verified')
        help_result = subprocess.run([sys.executable, '-S', '-m', 'rdfworkflow', '--help'],
            env=dict(os.environ, PYTHONPATH=str(self.prefix/'python')), cwd=self.directory,
            capture_output=True, text=True, check=True)
        self.assertIn('explain', help_result.stdout)

    def test_relocated_controller_preserves_imports_and_collection_helper(self):
        relocated = self.directory/'relocated prefix'
        shutil.copytree(self.prefix, relocated)
        code = '''
from pathlib import Path
import sys
import rdfworkflow,run_bundle
assert Path(rdfworkflow.__file__).parent.resolve()==Path(sys.path[1]).resolve()
root=Path('collection');root.mkdir();child=root/'job';child.mkdir()
(child/'config.txt').write_text('test config')
run_bundle.seal_bundle(child)
record=run_bundle.seal_collection(root,{'job':'job'})
assert run_bundle.verify_collection(root)['collection_id']==record['collection_id']
print('relocated collection verified')
'''
        self.assertEqual(self.child(code, prefix=relocated).stdout.strip(), 'relocated collection verified')

    def test_shared_rules_install_same_closure_for_full_python_component(self):
        source = self.directory/'consumer source'; source.mkdir()
        module = (REPO/'cmake/InstallPythonController.cmake').as_posix()
        (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.19)\n'
            'project(ControllerConsumer LANGUAGES NONE)\ninclude("'+module+'")\n'
            'rdf_install_python_controller(COMPONENT Python)\n'
            'rdf_install_python_controller(COMPONENT PythonController)\n')
        build = self.directory/'consumer build'; prefix = self.directory/'full-component prefix'
        subprocess.run([self.cmake, '-S', str(source), '-B', str(build)],
            check=True, capture_output=True, text=True, env=self.env)
        subprocess.run([self.cmake, '--install', str(build), '--prefix', str(prefix), '--component', 'Python'],
            check=True, capture_output=True, text=True, env=self.env)
        for original in self.prefix.rglob('*'):
            if original.is_file() and '__pycache__' not in original.parts:
                self.assertEqual(original.read_bytes(), (prefix/original.relative_to(self.prefix)).read_bytes())

    def test_installed_tutorial_inspection_and_failure_scopes(self):
        example = self.prefix/'share/rdfanalyzer/examples/compiled-controller/steer.py'
        spec = self.directory/'tutorial-spec.json'
        work = self.directory/'tutorial work'; work.mkdir()
        document = {'executable': sys.executable, 'working_directory': str(work),
            'arguments': ['-S', '-c', "from pathlib import Path;Path('summary.txt').write_text('opaque output')"],
            'outputs': {'summary': 'summary.txt'}}
        spec.write_text(json.dumps(document))
        env = dict(os.environ, PYTHONPATH=str(self.prefix/'python'))
        def run(*arguments):
            return subprocess.run([sys.executable, '-S', str(example), str(spec), *arguments],
                env=env, cwd=self.directory, capture_output=True, text=True)
        inspected = run()
        self.assertEqual(inspected.returncode, 0, inspected.stderr)
        self.assertEqual(json.loads(inspected.stdout)['backend'], 'compiled-executable')
        self.assertFalse((work/'summary.txt').exists())
        launched = run('--mode', 'run', '--logs', str(self.directory/'tutorial logs'),
            '--progress-interval', '.5')
        self.assertEqual(launched.returncode, 0, launched.stderr)
        result = json.loads(launched.stdout)
        self.assertEqual(result['schema'], 'rdf-run-result-v1')
        self.assertEqual(result['details']['operation'], 'analysis')
        self.assertTrue(result['provenance']['artifact_identity'])
        self.assertTrue(result['details']['progress']['enabled'])
        status = subprocess.run([sys.executable, '-S', '-m', 'rdfworkflow', 'status',
            str(self.directory/'tutorial logs/run')], env=env, cwd=self.directory,
            capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(status.stdout)['status'], 'succeeded')
        document['arguments'] = ['-S', '-c', 'pass']
        document['outputs']['summary'] = 'missing.txt'
        spec.write_text(json.dumps(document))
        failed = run('--mode', 'run', '--logs', str(self.directory/'failed logs'))
        self.assertEqual(failed.returncode, 1, failed.stderr)
        self.assertEqual(json.loads(failed.stdout)['issues'][0]['code'], 'missing_output')
        rejected = run('--build')
        self.assertEqual(rejected.returncode, 2)
        self.assertIn('--build requires', rejected.stderr)
        rejected = run('--progress-interval', '1')
        self.assertEqual(rejected.returncode, 2)
        self.assertIn('--progress-interval requires --mode run', rejected.stderr)

    @unittest.skipUnless(hasattr(os, 'sched_getaffinity') and shutil.which('taskset'),
                         'Linux local affinity preflight')
    def test_installed_tutorial_explicit_resource_validation_preserves_scope(self):
        example = self.prefix/'share/rdfanalyzer/examples/compiled-controller/steer.py'
        spec = self.directory/'resources-tutorial-spec.json'
        available = min(os.sched_getaffinity(0))
        document = {'executable': 'missing-analysis', 'working_directory': str(self.directory),
                    'assets': {'config': 'missing-config'}, 'resources': {'cpus': [available]}}
        spec.write_text(json.dumps(document))
        def run(*arguments):
            return subprocess.run([sys.executable, '-S', str(example), str(spec), *arguments],
                env=dict(os.environ, PYTHONPATH=str(self.prefix/'python')),
                cwd=self.directory, capture_output=True, text=True)
        inspected = run()
        self.assertEqual(inspected.returncode, 0, inspected.stderr)
        checked = run('--mode', 'validate', '--validation-level', 'local-resources')
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertTrue(json.loads(checked.stdout)['passed'])
        assets = run('--mode', 'validate')
        self.assertEqual(assets.returncode, 1, assets.stderr)
        self.assertIn('missing_file', {item['code'] for item in json.loads(assets.stdout)['issues']})
        rejected = run('--validation-level', 'local-resources')
        self.assertEqual(rejected.returncode, 2)
        self.assertIn('--validation-level requires --mode validate', rejected.stderr)
        del document['resources']
        spec.write_text(json.dumps(document))
        unsupported = run('--mode', 'validate', '--validation-level', 'local-resources')
        self.assertEqual(unsupported.returncode, 1, unsupported.stderr)
        self.assertEqual(json.loads(unsupported.stdout)['issues'][0]['code'], 'unsupported_level')
        self.assertFalse((self.directory/'missing-analysis').exists())
        self.assertFalse((self.directory/'missing-config').exists())


class ControllerTypingTests(unittest.TestCase):
    def test_stub_public_api_dataclass_fields_and_method_signatures_match_runtime(self):
        from _stub_contracts import assert_stub_contract
        for module_name in ('rdfworkflow', 'rdfresults', 'rdfschema'):
            assert_stub_contract(self, module_name)



if __name__ == '__main__':
    unittest.main()
