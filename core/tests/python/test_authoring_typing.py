"""Authoring IDE and installed tutorial checks without ROOT/Numba/event runs."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from _stub_contracts import assert_stub_contract

REPO = Path(__file__).resolve().parents[3]


class AuthoringStubTests(unittest.TestCase):
    def test_public_classes_functions_fields_and_signatures_match_runtime(self):
        for name in ('rdfanalysis', 'rdfcolumns', 'rdfdeclarations', 'rdfsettings'):
            with self.subTest(module=name):
                assert_stub_contract(self, name, private_classes=('_Settings',) if name == 'rdfsettings' else ())


@unittest.skipUnless(shutil.which('cmake'), 'CMake source-component install')
class InstalledAuthoringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='rdf authoring signatures ')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.directory = Path(cls.temporary.name)
        source = cls.directory/'source'; source.mkdir()
        module = (REPO/'cmake/InstallPythonAuthoring.cmake').as_posix()
        (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.19)\n'
            'project(AuthoringFiles LANGUAGES NONE)\ninclude("'+module+'")\n'
            'rdf_install_python_authoring(COMPONENT Python)\n')
        build = cls.directory/'build'; cls.prefix = cls.directory/'prefix'
        env = dict(os.environ, CC='/missing-compiler', CXX='/missing-compiler')
        subprocess.run(['cmake', '-S', str(source), '-B', str(build)],
            env=env, check=True, capture_output=True, text=True)
        subprocess.run(['cmake', '--install', str(build), '--prefix', str(cls.prefix), '--component', 'Python'],
            env=env, check=True, capture_output=True, text=True)
        cls.example = cls.prefix/'share/rdfanalyzer/examples/python-authoring/analysis.py'

    def child(self, code):
        result = subprocess.run([sys.executable, '-S', '-c', code], cwd=self.directory,
            env=dict(os.environ, PYTHONPATH=str(self.prefix/'python')), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_installed_stubs_and_frontend_closure_match_without_native_dependencies(self):
        for name in ('rdfanalysis', 'rdfsettings', 'rdfcolumns', 'rdfdeclarations'):
            for suffix in ('.py', '.pyi'):
                self.assertEqual((self.prefix/'python'/(name+suffix)).read_bytes(),
                    (REPO/'core/python'/(name+suffix)).read_bytes())
        self.assertFalse(list(self.prefix.rglob('*.so')))
        self.child('''
import json,sys
from pathlib import Path
import rdfanalysis,rdfsettings,rdfcolumns,rdfdeclarations,rdfworkflow,rdfresults
modules=(rdfanalysis,rdfsettings,rdfcolumns,rdfdeclarations,rdfworkflow,rdfresults)
assert all(Path(module.__file__).parent.resolve()==Path(sys.path[1]).resolve() for module in modules)
config=rdfanalysis.AnalysisConfig(files=['missing.root'],output='unused.root',skim=False,
 input_settings=rdfsettings.InputSettings(cache_enabled=False))
assert config.native_config()['treeCacheEnabled']=='false'
assert config.validate().passed
json.dumps(config.describe().as_dict())
jets=rdfanalysis.CollectionRef('jets',{'pt':'Jet_pt'},
 metadata={'pt':rdfanalysis.FieldMetadata(dtype='RVec<float>',unit='GeV')})
assert jets.describe()['metadata']['pt']['unit']=='GeV'
json.dumps(jets.describe())
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
''')

    def test_installed_tutorial_description_and_schema_never_open_input_or_import_runtime(self):
        program = '''
import contextlib,io,json,runpy,sys
from pathlib import Path
functions=runpy.run_path(EXAMPLE)
for args in ([],['--mode','schema']):
 output=io.StringIO()
 with contextlib.redirect_stdout(output):assert functions['main'](args)==0
 json.loads(output.getvalue())
assert not Path('tutorial.root').exists()
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''.replace('EXAMPLE', repr(str(self.example)))
        self.child(program)

    def test_tutorial_kernel_semantics_and_interleaved_declarations_are_preserved(self):
        program = '''
import runpy,sys
functions=runpy.run_path(EXAMPLE)
assert functions['has_jets']([]) is False
assert functions['has_jets']([10.]) is True
ht=functions['selected_ht']
assert ht([],[])==0.
assert ht([50.,40.],[.2,-.2])==90.
assert ht([30.,31.,80.],[0.,2.,1.99])==80.
assert functions['twice'](90.)==180.
class Recorder:
 def __init__(self):self.calls=[]
 def __getattr__(self,name):
  assert name!='run','builder must not execute events'
  def record(*args,**kwargs):self.calls.append((name,args,kwargs));return self
  return record
analysis=Recorder()
assert functions['build'](analysis) is analysis
assert [name for name,args,kwargs in analysis.calls]==[
 'count','define_numba','filter','define_numba','filter','define_numba','count','sum','histogram','histogram']
assert analysis.calls[1][1][1] is functions['has_jets']
assert analysis.calls[3][1][1] is functions['selected_ht']
assert analysis.calls[5][1][1] is functions['twice']
assert [analysis.calls[i][2]['return_type'] for i in (1,3,5)]==['bool','double','double']
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''.replace('EXAMPLE', repr(str(self.example)))
        self.child(program)

    def test_missing_input_fails_before_native_import_with_actionable_message(self):
        program = '''
import contextlib,io,json,runpy,sys
functions=runpy.run_path(EXAMPLE);output=io.StringIO()
with contextlib.redirect_stdout(output):
 assert functions['main'](['--mode','run','--input','missing.root'])==1
error=json.loads(output.getvalue())
assert error['success'] is False and '--mode make-input' in error['error']
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''.replace('EXAMPLE', repr(str(self.example)))
        self.child(program)


if __name__ == '__main__':
    unittest.main()
