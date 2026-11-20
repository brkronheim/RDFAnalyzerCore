"""Opt-in compiled-child affinity without native libraries or datasets."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from rdfworkflow import CppAnalysisSpec, LocalExecutionResources
from run_bundle import derived_artifact, file_digest, write_json


_REAL_AFFINITY = sys.platform.startswith('linux') and hasattr(os, 'sched_getaffinity') and bool(shutil.which('taskset'))


class LocalResourceTests(unittest.TestCase):
    def test_explicit_resource_level_is_advertised_only_when_requested(self):
        with patch('rdfworkflow._local_resource_tool', side_effect=AssertionError('no implicit host probe')):
            plain = CppAnalysisSpec('missing')
            report = plain.validate('local-resources')
            self.assertFalse(report.passed)
            self.assertEqual(report.supported_levels, ('configuration', 'assets'))
            self.assertEqual(report.issues[0].code, 'unsupported_level')
            self.assertNotIn('local-resources', plain.describe().capabilities)
            pinned = CppAnalysisSpec('missing', resources=LocalExecutionResources((0,)))
            self.assertTrue(pinned.validate().passed)
            self.assertIn('local-resources', pinned.describe().capabilities)

    def test_explicit_resource_report_catches_host_failures_without_asset_validation(self):
        spec = CppAnalysisSpec('missing', working_directory='missing-directory',
            assets={'config': 'missing-config'}, outputs={'result': 'missing-result'},
            libraries={'physics': 'missing-library'}, build_manifest='missing-build',
            resources=LocalExecutionResources((2,)))
        for error in (ValueError('CPU unavailable'), ValueError('taskset unavailable'), OSError('cannot hash tool')):
            with self.subTest(error=str(error)), patch('rdfworkflow._local_resource_tool', side_effect=error) as helper, \
                    patch('rdfworkflow.Path.open', side_effect=AssertionError('no asset opens')), \
                    patch('rdfworkflow._execute', side_effect=AssertionError('no child')), \
                    patch('rdfworkflow._read_build_manifest', side_effect=AssertionError('no build check')):
                report = spec.validate('local-resources')
                self.assertFalse(report.passed)
                self.assertEqual(len(report.issues), 1)
                self.assertEqual(report.issues[0].location, 'resources.cpus')
                self.assertEqual(report.issues[0].code, 'local_resources_unavailable')
                self.assertIn(str(error), report.issues[0].message)
                helper.assert_called_once_with(spec.resources, spec.environment, spec.working_directory)

    @unittest.skipUnless(_REAL_AFFINITY, 'Linux affinity and taskset required')
    def test_actual_resource_validation_only_reads_tool_and_preserves_parent(self):
        before = (set(os.sched_getaffinity(0)), os.getcwd(), dict(os.environ))
        cpu = 83 if 83 in before[0] else min(before[0])
        spec = CppAnalysisSpec('missing-executable', working_directory='missing-directory',
            assets={'config': 'missing-config'}, outputs={'result': 'missing-result'},
            libraries={'physics': 'missing-library'}, build_manifest='missing-build',
            resources=LocalExecutionResources((cpu,)))
        tool = Path(shutil.which('taskset')).resolve()
        original_open, original_is_file = Path.open, Path.is_file
        def open_tool(path, *args, **kwargs):
            if path != tool:
                raise AssertionError('unexpected file read '+str(path))
            return original_open(path, *args, **kwargs)
        def is_tool(path):
            if path != tool:
                raise AssertionError('unexpected asset stat '+str(path))
            return original_is_file(path)
        with patch.object(Path, 'open', open_tool), patch.object(Path, 'is_file', is_tool), \
                patch('rdfworkflow.subprocess.Popen', side_effect=AssertionError('no child')), \
                patch('rdfworkflow.write_json', side_effect=AssertionError('no writes')), \
                patch('rdfworkflow._read_build_manifest', side_effect=AssertionError('no build check')):
            report = spec.validate('local-resources')
        self.assertTrue(report.passed)
        self.assertEqual((set(os.sched_getaffinity(0)), os.getcwd(), dict(os.environ)), before)

    def test_structural_validation_never_probes_host(self):
        with patch('rdfworkflow._local_resource_tool', side_effect=AssertionError('no probe')):
            resources = LocalExecutionResources([0, 2])
            self.assertEqual(resources.cpus, (0, 2))
            spec = CppAnalysisSpec('missing', resources=resources)
            self.assertTrue(spec.validate().passed)
            self.assertEqual(spec.command(), (spec.executable,))
            self.assertEqual(spec.describe().as_dict()['details']['local_resources']['cpus'], [0, 2])
            for cpus in ((), [], (True,), (1.0,), (-1,), (2, 2), '0', None, ({},)):
                with self.subTest(cpus=cpus), self.assertRaises(ValueError):
                    LocalExecutionResources(cpus)
            with self.assertRaisesRegex(ValueError, 'LocalExecutionResources'):
                CppAnalysisSpec('missing', resources={'cpus': [0]})

    def test_default_description_and_launch_keep_legacy_shape_without_probes(self):
        with tempfile.TemporaryDirectory() as temporary, patch('rdfworkflow._local_resource_tool', side_effect=AssertionError('no probe')):
            spec = CppAnalysisSpec(sys.executable, ('-S', '-c', 'pass'), working_directory=temporary)
            details = spec.describe().as_dict()['details']
            self.assertEqual(set(details), {'command', 'working_directory', 'assets', 'outputs', 'libraries',
                'environment_override_names', 'timeout_s', 'build', 'build_manifest', 'build_validation', 'graph', 'library_validation'})
            self.assertEqual(details['command'], list(spec.command()))
            prepared = spec.prepare()
            self.assertIsNone(prepared.resource_tool)
            self.assertEqual(set(prepared.artifacts), {'executable'})
            result = prepared.run(Path(temporary)/'logs')
            self.assertEqual(result.command, spec.command())
            record = json.loads((Path(temporary)/'logs/run.json').read_text())
            self.assertNotIn('local_resources', record['artifacts'])
            self.assertEqual(set(record['artifacts']), {'files', 'artifact_identity', 'preparation_s', 'verification_s',
                'verification_policy', 'build_identity', 'environment_override_names'})

    def test_json_policy_is_explicit_and_validated(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'spec.json'
            base = dict(executable=sys.executable)
            for value in ({'cpus': [0, 2]}, None):
                path.write_text(json.dumps(dict(base, resources=value)))
                spec = CppAnalysisSpec.from_file(path)
                self.assertEqual(spec.resources, None if value is None else LocalExecutionResources((0, 2)))
            for value in ({'cpus': [0], 'threads': 2}, {}, [0], {'cpus': [False]}):
                path.write_text(json.dumps(dict(base, resources=value)))
                with self.subTest(value=value), self.assertRaises(ValueError):
                    CppAnalysisSpec.from_file(path)

    def test_unavailable_cpu_or_tool_rejected_before_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = CppAnalysisSpec(sys.executable, ('-S', '-c', 'raise AssertionError("child launched")'),
                working_directory=root, resources=LocalExecutionResources((2,)))
            for available, tool, message in (({1}, '/unused/taskset', 'CPUs unavailable'), ({2}, None, 'taskset tool is unavailable')):
                with self.subTest(message=message), patch('rdfworkflow.os.sched_getaffinity', return_value=available, create=True), \
                        patch('rdfworkflow.sys.platform', 'linux'), patch('rdfworkflow.shutil.which', return_value=tool), \
                        patch('rdfworkflow.subprocess.Popen', side_effect=AssertionError('no child')):
                    with self.assertRaisesRegex(ValueError, message):
                        spec.run(root/'logs')
                    self.assertFalse((root/'logs').exists())
                    report = spec.validate('local-resources')
                    self.assertFalse(report.passed)
                    self.assertEqual(report.issues[0].location, 'resources.cpus')
                    self.assertIn(message, report.issues[0].message)
            with patch('rdfworkflow.sys.platform', 'win32'):
                with self.assertRaisesRegex(ValueError, 'requires Linux'):
                    spec.prepare()

    def test_prepared_tool_path_and_bytes_and_cpu_availability_rechecked(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); tool = root/'taskset'; tool.write_text('original'); tool.chmod(0o700)
            spec = CppAnalysisSpec(sys.executable, ('-S', '-c', 'pass'), working_directory=root,
                resources=LocalExecutionResources((2,)))
            with patch('rdfworkflow.sys.platform', 'linux'), patch('rdfworkflow.os.sched_getaffinity', return_value={2}, create=True), \
                    patch('rdfworkflow.shutil.which', return_value=str(tool)):
                prepared = spec.prepare()
                with self.assertRaises(TypeError):
                    prepared.resource_tool['path'] = 'other'
                tool.write_text('changed')
                with self.assertRaisesRegex(ValueError, 'taskset tool changed'):
                    prepared.run(root/'changed')
                tool.write_text('original')
                other = root/'other'; shutil.copyfile(tool, other); other.chmod(0o700)
                with patch('rdfworkflow.shutil.which', return_value=str(other)):
                    with self.assertRaisesRegex(ValueError, 'taskset tool changed'):
                        prepared.run(root/'moved')
                with patch('rdfworkflow.os.sched_getaffinity', return_value={1}):
                    with self.assertRaisesRegex(ValueError, 'CPUs unavailable'):
                        prepared.run(root/'unavailable')
            for name in ('changed', 'moved', 'unavailable'):
                self.assertFalse((root/name).exists())

    def test_build_identity_still_names_analysis_products(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); asset = root/'input'; asset.write_text('declared')
            library = root/'library.so'; library.write_bytes(b'fixture, not a native library')
            exe = str(Path(sys.executable).resolve()); products = dict(analysis=exe, library=str(library))
            item = dict(path=str(asset), sha256=file_digest(asset))
            manifest = derived_artifact(kind='compiled-analysis-build', parents={},
                configuration=dict(products=products, declared_inputs={'source': item}, tools={'tool': item}), files=products.values())
            path = root/'build.json'; write_json(path, manifest)
            spec = CppAnalysisSpec(exe, working_directory=root, libraries={'physics': str(library)},
                build_manifest=path, resources=LocalExecutionResources((2,)))
            tool = dict(path='/explicit/taskset', sha256='0'*64)
            with patch('rdfworkflow._local_resource_tool', return_value=tool):
                prepared = spec.prepare()
            self.assertEqual(prepared.artifacts['executable']['path'], exe)
            self.assertEqual(prepared.build_identity, manifest['artifact_id'])
            self.assertEqual(prepared.resource_tool, tool)
            self.assertNotIn('taskset', prepared.artifacts)

    def test_child_relative_path_is_resolved_without_parent_environment_change(self):
        before = dict(os.environ)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); tools = root/'tools'; tools.mkdir()
            tool = tools/'taskset'; tool.write_text('not invoked'); tool.chmod(0o700)
            spec = CppAnalysisSpec(sys.executable, working_directory=root,
                environment={'PATH': 'tools'}, resources=LocalExecutionResources((2,)))
            with patch('rdfworkflow.sys.platform', 'linux'), patch('rdfworkflow.os.sched_getaffinity', return_value={2}, create=True):
                prepared = spec.prepare()
            self.assertEqual(prepared.resource_tool['path'], str(tool))
            self.assertEqual(prepared.resource_tool['sha256'], file_digest(tool))
        self.assertEqual(dict(os.environ), before)

    @unittest.skipUnless(_REAL_AFFINITY, 'Linux affinity and taskset required')
    def test_actual_child_affinity_parent_state_and_actual_argv(self):
        before = (set(os.sched_getaffinity(0)), os.getcwd(), dict(os.environ))
        cpu = 83 if 83 in before[0] else min(before[0])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            code = "import json,os,sys;open(sys.argv[1],'w').write(json.dumps({'cpus':sorted(os.sched_getaffinity(0)),'cwd':os.getcwd(),'value':os.environ['LOCAL_RESOURCE_TEST']}))"
            spec = CppAnalysisSpec(sys.executable, ('-S', '-c', code, '{output:observation}'),
                working_directory=root, outputs={'observation': 'child.json'}, environment={'LOCAL_RESOURCE_TEST': 'child only'},
                resources=LocalExecutionResources((cpu,)))
            prepared = spec.prepare(); result = prepared.run(root/'logs')
            self.assertEqual(json.loads((root/'child.json').read_text()), {'cpus': [cpu], 'cwd': str(root), 'value': 'child only'})
            self.assertEqual(result.command, (prepared.resource_tool['path'], '--cpu-list', '--', str(cpu), *spec.command()))
            record = json.loads((root/'logs/run.json').read_text())
            self.assertEqual(record['artifacts']['local_resources']['tool'], dict(prepared.resource_tool))
            self.assertEqual(record['artifacts']['files']['executable']['path'], spec.executable)
        self.assertEqual((set(os.sched_getaffinity(0)), os.getcwd(), dict(os.environ)), before)

    @unittest.skipUnless(_REAL_AFFINITY, 'Linux affinity and taskset required')
    def test_actual_affinity_child_cancellation_and_progress(self):
        available = set(os.sched_getaffinity(0)); cpu = 83 if 83 in available else min(available)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); ready = root/'ready'; cancel = threading.Event()
            code = "import os,pathlib,sys,time;pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(30)"
            spec = CppAnalysisSpec(sys.executable, ('-S', '-c', code, str(ready)), working_directory=root,
                resources=LocalExecutionResources((cpu,)), terminate_grace_s=.1, timeout_s=5)
            def request_cancel():
                deadline = time.monotonic()+3
                while not ready.exists() and time.monotonic()<deadline:
                    time.sleep(.01)
                time.sleep(.6); cancel.set()
            thread = threading.Thread(target=request_cancel); thread.start()
            try:
                result = spec.run(root/'logs', check=False, cancel_event=cancel, progress_interval_s=.5)
            finally:
                cancel.set(); thread.join(timeout=4)
            self.assertFalse(thread.is_alive()); self.assertTrue(ready.exists())
            self.assertEqual(result.status, 'cancelled'); self.assertIsNotNone(result.returncode)
            pid = int(ready.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
            progress = json.loads((root/'logs/progress.json').read_text())
            self.assertEqual((progress['phase'], progress['status']), ('finished', 'cancelled'))
            self.assertGreaterEqual(progress['sequence'], 5)
        self.assertEqual(set(os.sched_getaffinity(0)), available)


if __name__ == '__main__':
    unittest.main()
